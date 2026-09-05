package sessionv2

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func TestAcceptedOriginalProgressWithNoSendCreditRemaining(t *testing.T) {
	for _, pressure := range []string{"session", "peer"} {
		t.Run(pressure, func(t *testing.T) {
			payload := bytes.Repeat([]byte{37}, 10000)
			var initial, sessionLimit uint64
			p := newPairWithCreditLimits(t, 1, 1, false, 5000, func(role negotiationv2.Role, cfg Config, limits *creditv2.Limits) {
				if role != negotiationv2.Initiator {
					return
				}
				claims, err := RequiredInitialClaims(cfg)
				if err != nil {
					t.Fatal(err)
				}
				for _, claim := range claims {
					initial += claim.Bytes
				}
				sessionLimit = initial + uint64(len(payload))
				limits.MaxSessionBytes, limits.MaxPeerBytes = sessionLimit, sessionLimit+1024
				if pressure == "peer" {
					limits.MaxSessionBytes = limits.MaxPeerBytes
				}
			})
			defer p.close(t)
			p.pump(t, p.ready)
			c := p.client.controller
			if pressure == "peer" {
				other, lease, err := p.client.peer.BeginSession(creditv2.Claim{Bytes: 1024})
				if err != nil {
					t.Fatal(err)
				}
				defer other.Close()
				defer lease.Release()
			}
			var held []*creditv2.Lease
			defer func() {
				for _, lease := range held {
					lease.Release()
				}
			}()
			emit := c.cfg.Emit
			c.cfg.Emit = func(path transport.ReplyPath, packet []byte) error {
				if wirev2.PacketType(packet[5]) == wirev2.TypeFECBundle {
					// Take every byte returned by fully consumed originals, so
					// pending shards must use their standing assembly allowance.
					free := sessionLimit - c.setup.Scope.Snapshot().Bytes
					if free > 0 {
						lease, err := c.setup.Scope.Reserve(creditv2.Claim{Bytes: free})
						if err != nil {
							t.Fatal(err)
						}
						held = append(held, lease)
					}
				}
				return emit(path, packet)
			}
			receipt, result, err := c.Write(p.now, payload)
			if err != nil || receipt != 1 || result.CompletedThrough != 0 || c.queue.Snapshot().RetainedBytes != uint64(len(payload)) {
				t.Fatalf("whole-original admission/partial frontier: %d %+v %v", receipt, result, err)
			}
			if c.setup.Scope.Snapshot().Bytes != sessionLimit {
				t.Fatal("fixture did not consume all available sending credit")
			}
			p.pump(t, func() bool { return c.completed == 1 && len(p.server.deliveries) == 1 })
			if !bytes.Equal(p.server.deliveries[0].Payload(), payload) || c.sticky != nil || c.out != nil || c.queue.Snapshot().QueuedDatagrams != 0 {
				t.Fatal("accepted partial original did not finish under sustained credit pressure")
			}
			if c.setup.Scope.Snapshot().Bytes != sessionLimit || c.assemblyLease.Snapshot().Released {
				t.Fatal("send completion returned the protected allowance")
			}
		})
	}
}

func TestSendWorkspaceInstallationAndRollbackAtFullCapacity(t *testing.T) {
	setup, cfg, peer := scratchSetup(t, configFor(profile(1), false), 0)
	before := peer.Snapshot()
	c, err := New(setup, cfg)
	if err != nil || peer.Snapshot() != before {
		t.Fatalf("full-capacity send workspace installation reserved again: %v", err)
	}
	if c.outputWorkspace == nil || c.assemblyLease == nil || c.assemblyLease.Snapshot().Bytes != 2*uint64(cfg.FixedPayloadBudget) {
		t.Fatal("missing independent output/assembly prepayment")
	}
	setup.Scope.Close()
	c.Close()
	c.Close()
	if c.outputWorkspace != nil || c.assemblyLease != nil || peer.Snapshot().Usage != (creditv2.Usage{}) {
		t.Fatal("Close retained send workspace credit")
	}
	for _, failed := range []int{InitialOutput, InitialAssembly, InitialOriginalWindow} {
		t.Run(strconv.Itoa(failed), func(t *testing.T) {
			setup, cfg, _ := scratchSetup(t, configFor(profile(1), false), 0)
			original := setup.Initial[failed]
			t.Cleanup(original.Release)
			setup.Initial[failed] = nil
			if c, err := New(setup, cfg); c != nil || !errors.Is(err, creditv2.ErrInvalid) {
				t.Fatalf("invalid prepaid owner accepted: %v", err)
			}
			if _, err := setup.Scope.BindBytes(original, original.Snapshot().Bytes); err != nil {
				t.Fatal("constructor consumed the missing caller lease")
			}
			boundOrder := []int{InitialControl, InitialGroupWindow, InitialQueue, InitialOutput, InitialAssembly, InitialOriginalWindow}
			for _, index := range boundOrder {
				if index == failed {
					break
				}
				if !setup.Initial[index].Snapshot().Released {
					t.Fatalf("constructor rollback retained owner %d", index)
				}
			}
		})
	}
}

func TestControllerCloseClearsOutstandingSendGroup(t *testing.T) {
	p := newPair(t, 1, 1, false, 5000)
	p.pump(t, p.ready)
	c := p.client.controller
	if _, _, err := c.Write(p.now, bytes.Repeat([]byte{4}, 10000)); err != nil || c.out == nil {
		t.Fatalf("expected partly sent group: %v", err)
	}
	out := *c.out
	view, _ := out.View()
	workspace, assembly := c.outputWorkspace, c.assemblyLease
	p.close(t)
	if _, live := out.View(); live || !assembly.Snapshot().Released {
		t.Fatal("Close retained output or assembly ownership")
	}
	for _, shard := range view.Group.Shards {
		if !bytes.Equal(shard, make([]byte, len(shard))) {
			t.Fatal("Close released group credit before clearing backing")
		}
	}
	out.Release()
	workspace.Close()
}

func TestSendWorkspaceCoversNegotiatedSmallerBudget(t *testing.T) {
	client, server := profile(1), profile(1)
	server.Payload.ReceiveHardCap = 512
	p := newPairWithProfiles(t, client, server, 1, false, nil)
	defer p.close(t)
	p.pump(t, p.ready)
	c := p.client.controller
	if c.sendContext.ShardBytes+94 != 512 || c.assemblyLease.Snapshot().Bytes != 2400 {
		t.Fatal("effective budget exceeded or replaced local-offer prepayment")
	}
	before := c.setup.Scope.Snapshot()
	payload := bytes.Repeat([]byte{9}, 2000)
	if _, _, err := c.Write(p.now, payload); err != nil {
		t.Fatal(err)
	}
	p.pump(t, func() bool { return c.completed == 1 && len(p.server.deliveries) == 1 })
	if !bytes.Equal(p.server.deliveries[0].Payload(), payload) || c.setup.Scope.Snapshot() != before {
		t.Fatal("negotiated send changed payload or standing credit")
	}
	for _, sent := range p.client.sent {
		if wirev2.PacketType(sent.data[5]) == wirev2.TypeFECBundle && len(sent.data) != 512 {
			t.Fatal("send workspace changed negotiated packet length")
		}
	}
}
