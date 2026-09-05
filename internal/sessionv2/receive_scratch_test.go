package sessionv2

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"
	"unsafe"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/fecv2"
	"github.com/mofelee/mpudp/internal/handshakev2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/recvwindow"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func TestPrepaidReceiveScratchSurvivesOutboundQueuePressure(t *testing.T) {
	for _, outgoing := range []bool{false, true} {
		t.Run(fmt.Sprintf("outgoing=%v", outgoing), func(t *testing.T) {
			const count = 32
			var limit uint64
			p := newPairWithCreditLimits(t, 1, 1, false, 5000, func(role negotiationv2.Role, cfg Config, limits *creditv2.Limits) {
				if role != negotiationv2.Responder {
					return
				}
				claims, err := RequiredInitialClaims(cfg)
				if err != nil {
					t.Fatal(err)
				}
				for _, claim := range claims {
					limit += claim.Bytes
				}
				n := uint64(cfg.LocalProfile.DataShards + cfg.LocalProfile.ParityShards)
				shard := uint64(cfg.FixedPayloadBudget - 94)
				peak := 2*n*shard + uint64(cfg.LocalProfile.DataShards)*shard + 2*n*uint64(unsafe.Sizeof([]byte{})) + uint64(unsafe.Sizeof(fecv2.Fragment{})) + uint64(unsafe.Sizeof(pendingGroup{})) + 64
				scratch := uint64(cfg.FixedPayloadBudget) + wirev2.MaxFECRecords*uint64(unsafe.Sizeof(wirev2.FECRecord{}))
				limit += count*peak + scratch
				limits.MaxPeerBytes, limits.MaxSessionBytes = limit, limit
			})
			t.Cleanup(func() { p.close(t) })
			p.pump(t, p.ready)
			c := p.server.controller
			floor, initial := c.controlLease.Snapshot(), c.setup.Scope.Snapshot()
			payload := bytes.Repeat([]byte("x"), 1400)
			var target []packet
			for id := 1; id <= count; id++ {
				target = groupPackets(t, p, payload)
				for _, pk := range target[:2] {
					if result, err := receivePacket(p, pk); err != nil || len(result.Deliveries) != 0 {
						t.Fatalf("incomplete group admission: %+v, %v", result, err)
					}
				}
			}
			group := c.groups[count]
			deadline := group.admitted.Add(c.cfg.GroupTimeout)
			scratch := uint64(len(target[2].data)) + wirev2.MaxFECRecords*uint64(unsafe.Sizeof(wirev2.FECRecord{}))
			if free := limit - c.setup.Scope.Snapshot().Bytes; free != scratch || len(c.groups) != count {
				t.Fatalf("incomplete groups left %d bytes, want %d", free, scratch)
			}
			var receipt Receipt
			if outgoing {
				var result Result
				var err error
				// The prepaid send workspace can seal the first original even
				// at this pressure. Queue another while its paced group is live.
				if _, _, err = c.Write(p.now, payload); err != nil || c.out == nil {
					t.Fatalf("prepaid output did not start: %v", err)
				}
				receipt, result, err = c.Write(p.now, payload)
				if err != nil || receipt == 0 || result.CompletedThrough != 0 || len(result.Sends) != 0 || c.queue.Snapshot().QueuedDatagrams != 1 {
					t.Fatalf("outbound pressure setup: receipt %d, result %+v, %v", receipt, result, err)
				}
				before := c.setup.Scope.Snapshot()
				if output, err := c.queue.Seal(p.now, true); output != nil || !errors.Is(err, creditv2.ErrResourceLimit) || c.setup.Scope.Snapshot() != before {
					t.Fatal("ordinary output cannot borrow the protected send workspace")
				}
				if free := limit - before.Bytes; free != scratch-1400 || free >= scratch {
					t.Fatal("outbound payload did not consume transient receive headroom")
				}
			}
			result, err := receivePacket(p, target[2])
			p.server.deliveries = append(p.server.deliveries, result.Deliveries...)
			if err != nil || len(result.Deliveries) != 1 || !bytes.Equal(result.Deliveries[0].Payload(), payload) {
				t.Fatalf("owned group could not finish under outbound pressure: %+v, %v", result, err)
			}
			if !p.now.Before(deadline) || len(c.groups) != count-1 || c.groupWindow.State(count) != recvwindow.Completed || c.controlLease.Snapshot() != floor {
				t.Fatal("completion waited for expiry or changed prepaid workspace ownership")
			}
			for id := uint64(1); id < count; id++ {
				if c.groupWindow.State(id) != recvwindow.Unseen || c.groups[id].present != 2 {
					t.Fatal("completion changed another incomplete group")
				}
			}
			reservations := initial.Reservations + count
			if outgoing {
				reservations++
			}
			if c.setup.Scope.Snapshot().Reservations != reservations {
				t.Fatal("receive retained a transient workspace lease")
			}
			if outgoing {
				p.pump(t, func() bool { return c.Snapshot().CompletedThrough >= uint64(receipt) })
				if c.queue.Snapshot().QueuedDatagrams != 0 || c.controlLease.Snapshot() != floor {
					t.Fatal("released group credit did not unblock queued output")
				}
			}
		})
	}
}

func scratchSetup(t *testing.T, cfg Config, reduce uint64) (handshakev2.Setup, Config, *creditv2.Peer) {
	t.Helper()
	remote := cfg.LocalProfile
	remote.Payload.SendHardCap = 65507
	_, contract, err := negotiationv2.Select(negotiationv2.Advertisement{Profile: remote, BootstrapPathID: 1}, cfg.LocalProfile)
	if err != nil {
		t.Fatal(err)
	}
	binding := opposite(clientBinding(1), true)
	cfg.BootstrapPath = &testPath{binding}
	cfg.Emit = func(transport.ReplyPath, []byte) error { return nil }
	claims, err := RequiredInitialClaims(cfg)
	if err != nil {
		t.Fatal(err)
	}
	claims[InitialControl].Bytes -= reduce
	var total uint64
	for _, claim := range claims {
		total += claim.Bytes
	}
	peer, err := creditv2.New(creditv2.Limits{MaxPeerBytes: total, MaxSessionBytes: total, MaxSessions: 1, MaxPendingHandshakes: 1, MaxPendingAccepts: 1, MaxStreamsPerSession: 1, MaxPeerStreams: 1, MaxReservations: InitialCount})
	if err != nil {
		t.Fatal(err)
	}
	scope, _, err := peer.BeginSession(creditv2.Claim{})
	if err != nil {
		t.Fatal(err)
	}
	setup := handshakev2.Setup{ID: wirev2.SessionID{1}, Role: negotiationv2.Responder, PathID: 1, Binding: binding, Contract: contract, Scope: scope, Keys: wirev2.DirectionalKeys{ClientToServer: wirev2.Key{1}, ServerToClient: wirev2.Key{2}}}
	for _, claim := range claims {
		lease, err := scope.Reserve(claim)
		if err != nil {
			t.Fatal(err)
		}
		setup.Initial = append(setup.Initial, lease)
	}
	t.Cleanup(func() {
		for _, lease := range setup.Initial {
			lease.Release()
		}
		scope.Close()
		if got := peer.Snapshot(); got.Bytes != 0 || got.Reservations != 0 || got.SessionSlots != 0 {
			t.Errorf("prepaid setup leaked: %+v", got)
		}
	})
	return setup, cfg, peer
}

func TestReceiveScratchUsesLocalReceiveHardCapAndPrepaidOwnership(t *testing.T) {
	local := profile(1)
	local.Payload.SendHardCap = 512
	var previous uint64
	for _, receive := range []uint16{512, 65507} {
		t.Run(fmt.Sprint(receive), func(t *testing.T) {
			local.Payload.ReceiveHardCap = receive
			cfg := configFor(local, false)
			cfg.FixedPayloadBudget = 512
			claims, err := RequiredInitialClaims(cfg)
			if err != nil || len(claims) != InitialCount {
				t.Fatalf("initial claims: %+v, %v", claims, err)
			}
			if receive != 512 && claims[InitialControl].Bytes-previous != uint64(receive-512) {
				t.Fatal("receive workspace did not grow with the independent receive hard cap")
			}
			previous = claims[InitialControl].Bytes
			widerSend := cfg
			widerSend.LocalProfile.Payload.SendHardCap = 65507
			widerClaims, err := RequiredInitialClaims(widerSend)
			if err != nil || !slices.Equal(claims, widerClaims) {
				t.Fatal("outbound hard cap changed the receive workspace reservation")
			}
			setup, cfg, peer := scratchSetup(t, cfg, 0)
			before := peer.Snapshot()
			c, err := New(setup, cfg)
			if err != nil || peer.Snapshot() != before {
				t.Fatalf("full-capacity installation reserved again: %v", err)
			}
			t.Cleanup(c.Close)
			alias := *setup.Initial[InitialControl]
			if _, err := setup.Scope.BindBytes(&alias, 1); !errors.Is(err, creditv2.ErrInvalid) {
				t.Fatal("controller workspace could be rebound through a retained handle")
			}
			setup.Scope.Close()
			if peer.Snapshot().Bytes != before.Bytes || setup.Scope.Snapshot().Retired {
				t.Fatal("scope Close revoked prepaid workspace")
			}
			c.Close()
			c.Close()
			alias.Release()
			if peer.Snapshot().Usage != (creditv2.Usage{}) || !setup.Scope.Snapshot().Retired {
				t.Fatal("controller Close did not return each initial obligation once")
			}
		})
	}
	for _, receive := range []uint16{511, 65508} {
		local.Payload.ReceiveHardCap = receive
		cfg := configFor(local, false)
		cfg.FixedPayloadBudget = 512
		if claims, err := RequiredInitialClaims(cfg); err == nil || claims != nil {
			t.Fatal("invalid receive hard cap produced a prepaid reservation")
		}
	}
}

func TestInsufficientReceiveScratchPrepaymentDoesNotBindAnyComponent(t *testing.T) {
	cfg := configFor(profile(1), false)
	for _, missing := range []uint64{1, uint64(cfg.LocalProfile.Payload.ReceiveHardCap) + wirev2.MaxFECRecords*uint64(unsafe.Sizeof(wirev2.FECRecord{}))} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			setup, cfg, peer := scratchSetup(t, cfg, missing)
			before := peer.Snapshot()
			if c, err := New(setup, cfg); c != nil || !errors.Is(err, creditv2.ErrInvalid) || peer.Snapshot() != before {
				t.Fatalf("insufficient prepaid workspace was consumed: %v", err)
			}
			for _, lease := range setup.Initial {
				if _, err := setup.Scope.BindBytes(lease, lease.Snapshot().Bytes); err != nil {
					t.Fatalf("failed construction bound another caller obligation: %v", err)
				}
			}
		})
	}
}

func TestReceiveScratchRejectsReentrantReceiveAndOutlivesMalformedPackets(t *testing.T) {
	p := newPair(t, 1, 1, false, 5000)
	t.Cleanup(func() { p.close(t) })
	p.pump(t, p.ready)
	packets := groupPackets(t, p, []byte("incoming"))
	c := p.server.controller
	floor := c.controlLease.Snapshot()
	before := p.server.peer.Snapshot()
	malformed := slices.Clone(packets[0].data)
	malformed[len(malformed)-1] ^= 1
	pk := packets[0]
	pk.data = malformed
	if _, err := receivePacket(p, pk); err == nil || p.server.peer.Snapshot() != before || c.controlLease.Snapshot() != floor {
		t.Fatal("invalid authentication changed prepaid workspace ownership")
	}
	emit := c.cfg.Emit
	calls := 0
	c.cfg.Emit = func(path transport.ReplyPath, data []byte) error {
		calls++
		before := p.server.peer.Snapshot()
		if _, err := receivePacket(p, packets[0]); !errors.Is(err, ErrReentrant) || p.server.peer.Snapshot() != before || c.controlLease.Snapshot() != floor {
			t.Fatal("reentrant receive borrowed concurrent workspace")
		}
		return emit(path, data)
	}
	id, result, err := c.Write(p.now, []byte("outgoing"))
	if err != nil {
		t.Fatal(err)
	}
	p.server.deliveries = append(p.server.deliveries, result.Deliveries...)
	p.pump(t, func() bool { return c.Snapshot().CompletedThrough >= uint64(id) })
	if calls == 0 || c.controlLease.Snapshot() != floor {
		t.Fatal("send callback did not exercise workspace reentrancy guard")
	}
	c.cfg.Emit = emit
	for _, pk := range packets[:3] {
		result, err := receivePacket(p, pk)
		if err != nil {
			t.Fatal(err)
		}
		p.server.deliveries = append(p.server.deliveries, result.Deliveries...)
	}
	if len(p.server.deliveries) != 1 || string(p.server.deliveries[0].Payload()) != "incoming" || c.controlLease.Snapshot() != floor {
		t.Fatal("later receive failed to reuse prepaid workspace")
	}
}

func receiveScratchBundle(t *testing.T, c *Controller, now time.Time, records []wirev2.FECRecord) (Result, error) {
	t.Helper()
	packet, err := wirev2.AppendFECBundle(nil, wirev2.FECBundle{
		Header: wirev2.Header{Type: wirev2.TypeFECBundle, SessionID: wirev2.SessionID{1}},
		Route:  wirev2.Route{PathID: 1, Generation: 1, BudgetEpoch: 1}, Records: records,
	}, c.receiveLookup, wirev2.Key{1}, 512)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(packet)
	envelope, err := wirev2.ParseEnvelope(packet)
	if err != nil {
		t.Fatal(err)
	}
	authenticated, err := envelope.Authenticate(wirev2.Key{1})
	if err != nil {
		t.Fatal(err)
	}
	var result Result
	err = c.receiveBundle(now, authenticated, 512, false, &result)
	for _, delivery := range result.Deliveries {
		t.Cleanup(delivery.Release)
	}
	return result, err
}

func TestPrepaidReceiveScratchHandlesLargerBundleButPreservesNewGroupLimits(t *testing.T) {
	context := wirev2.EncodingContext{Epoch: 1, LayoutID: 1, ProtectionID: 1, DataShards: 3, ParityShards: 2, ShardBytes: 128, MaxDescriptors: 1, MaxLogicalBytes: 384}
	c, scope, limit, _ := creditProgressReceiver(t, context, 2)
	initial, floor := scope.Snapshot(), c.controlLease.Snapshot()
	start := time.Unix(1700000000, 0)
	var records []wirev2.FECRecord
	for id := uint64(1); id <= 2; id++ {
		encoded, err := c.receiveCodec.Encode([]fecv2.Fragment{{DatagramID: id, TotalBytes: 20, Payload: bytes.Repeat([]byte{byte(id)}, 20)}})
		if err != nil {
			t.Fatal(err)
		}
		for shard := range 2 {
			receiveEncodedShard(t, c, start, id, encoded, shard)
		}
		records = append(records, wirev2.FECRecord{GroupID: id, EncodingEpoch: 1, LogicalBytes: encoded.LogicalBytes, ShardIndex: 2, Payload: encoded.Shards[2]})
	}
	if free := limit - scope.Snapshot().Bytes; free != 1000 {
		t.Fatalf("expected 1000 free bytes before 1136-byte bundle scratch, got %d", free)
	}
	newGroup := records[0]
	newGroup.GroupID = 3
	before := scope.Snapshot()
	if result, err := receiveScratchBundle(t, c, start, []wirev2.FECRecord{newGroup, records[0]}); !errors.Is(err, creditv2.ErrResourceLimit) || len(result.Deliveries) != 0 || scope.Snapshot() != before || c.groups[1].present != 2 || c.controlLease.Snapshot() != floor {
		t.Fatal("new-group pressure changed existing ownership or bypassed its limit")
	}
	result, err := receiveScratchBundle(t, c, start, records)
	if err != nil || len(result.Deliveries) != 2 || len(c.groups) != 0 || c.controlLease.Snapshot() != floor {
		t.Fatalf("larger completion bundle could not use prepaid scratch: %+v, %v", result, err)
	}
	for i, delivery := range result.Deliveries {
		if !bytes.Equal(delivery.Payload(), bytes.Repeat([]byte{byte(i + 1)}, 20)) {
			t.Fatal("bundle cleanup modified an independently owned original")
		}
	}
	if scope.Snapshot().Bytes != initial.Bytes+40 || scope.Snapshot().Reservations != initial.Reservations+2 {
		t.Fatal("completion retained bundle scratch or lost delivery credit")
	}
}
