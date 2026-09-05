package sessionv2

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/aggregationv2"
	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/handshakev2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/reassemblyv2"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
)

type testPath struct{ binding handshakev2.Binding }

func (p *testPath) PathID() string                     { return strconv.FormatUint(p.binding.SocketID, 10) }
func (p *testPath) Available() bool                    { return true }
func (p *testPath) Generation() uint64                 { return p.binding.SocketID }
func (p *testPath) LocalAddr() net.Addr                { return net.UDPAddrFromAddrPort(p.binding.Local) }
func (p *testPath) RemoteAddr() net.Addr               { return net.UDPAddrFromAddrPort(p.binding.Remote) }
func (p *testPath) Send(context.Context, []byte) error { return nil }

type packet struct {
	binding handshakev2.Binding
	data    []byte
	at      time.Time
}

type endpoint struct {
	controller *Controller
	peer       *creditv2.Peer
	base       *creditv2.Lease
	out, sent  []packet
	deliveries []*reassemblyv2.Datagram
	now        *time.Time
	failData   bool
}

type pair struct {
	client, server *endpoint
	now            time.Time
	drop           func(bool, packet) bool
}

func profile(paths int) negotiationv2.Profile {
	return negotiationv2.Profile{Protocol: negotiationv2.Datagram, OfferedCaps: negotiationv2.FragmentManifest | negotiationv2.Aggregation, RequiredCaps: negotiationv2.FragmentManifest, LayoutID: 1, DataShards: 3, ParityShards: 2, Payload: negotiationv2.PayloadLimits{SendHardCap: 1200, ReceiveHardCap: 1472, BootstrapBytes: 512}, Datagram: negotiationv2.DatagramLimits{DatagramWindow: 64, GroupWindow: 64, MaxDatagramBytes: 65536, MaxFragments: 256, MaxDescriptors: 32, MaxDatagramAssemblies: 32}, Epochs: negotiationv2.EpochLimits{MaxOldEpochs: 2, GraceMS: 5000}, MaxPaths: uint16(paths)}
}

func configFor(p negotiationv2.Profile, aggregate bool) Config {
	return Config{LocalProfile: p, SendLimits: negotiationv2.SendLimits{Datagram: p.Datagram}, FixedPayloadBudget: 1200, Aggregation: aggregate, MaxGroupBytes: 65536, Queue: aggregationv2.Limits{MaxQueuedDatagrams: 32, MaxQueuedBytes: 1 << 20, MaxDatagramBytes: p.Datagram.MaxDatagramBytes, MaxFragmentsPerDatagram: int(p.Datagram.MaxFragments), MaxDelay: 250 * time.Microsecond}, Reassembly: reassemblyv2.Limits{MaxDatagrams: 32, MaxDatagramBytes: int(p.Datagram.MaxDatagramBytes), MaxFragments: int(p.Datagram.MaxFragments), Span: p.Datagram.DatagramWindow, Timeout: 10 * time.Second}, MaxPendingGroups: 32, GroupTimeout: 10 * time.Second, Entropy: rand.New(rand.NewSource(84))}
}

func clientBinding(path int) handshakev2.Binding {
	return handshakev2.Binding{SocketID: uint64(path), Local: netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(30000+path)), Remote: netip.MustParseAddrPort("127.0.0.1:40000")}
}

func opposite(binding handshakev2.Binding, toServer bool) handshakev2.Binding {
	id := uint64(100)
	if !toServer {
		id = uint64(binding.Remote.Port() - 30000)
	}
	return handshakev2.Binding{SocketID: id, Local: binding.Remote, Remote: binding.Local}
}

func newPair(t testing.TB, paths, bootstrap int, aggregate bool, grace uint32) *pair {
	return newPairWithCreditLimits(t, paths, bootstrap, aggregate, grace, nil)
}

func newPairWithCreditLimits(t testing.TB, paths, bootstrap int, aggregate bool, grace uint32, adjust func(negotiationv2.Role, Config, *creditv2.Limits)) *pair {
	t.Helper()
	p := profile(paths)
	p.Epochs.GraceMS = grace
	return newPairWithProfiles(t, p, p, bootstrap, aggregate, adjust)
}

func newPairWithProfiles(t testing.TB, client, server negotiationv2.Profile, bootstrap int, aggregate bool, adjust func(negotiationv2.Role, Config, *creditv2.Limits)) *pair {
	return newPairWithConfig(t, client, server, bootstrap, aggregate, nil, adjust)
}

func newPairWithConfig(t testing.TB, client, server negotiationv2.Profile, bootstrap int, aggregate bool, configure func(*Config), adjust func(negotiationv2.Role, Config, *creditv2.Limits)) *pair {
	t.Helper()
	_, contract, err := negotiationv2.Select(negotiationv2.Advertisement{Profile: client, BootstrapPathID: uint16(bootstrap)}, server)
	if err != nil {
		t.Fatal(err)
	}
	pair := &pair{now: time.Unix(1000, 0)}
	for _, role := range []negotiationv2.Role{negotiationv2.Initiator, negotiationv2.Responder} {
		e := &endpoint{now: &pair.now}
		local, _, err := contract.Profiles(role)
		if err != nil {
			t.Fatal(err)
		}
		cfg := configFor(local, aggregate)
		if configure != nil {
			configure(&cfg)
		}
		binding := clientBinding(bootstrap)
		if role == negotiationv2.Responder {
			binding = opposite(binding, true)
		}
		cfg.BootstrapPath = &testPath{binding}
		for i := 1; i <= int(client.MaxPaths); i++ {
			cfg.Carriers = append(cfg.Carriers, Carrier{Carrier: handshakev2.Carrier{PathID: uint16(i), Binding: clientBinding(i)}, Sender: &testPath{clientBinding(i)}})
		}
		cfg.Emit = func(sender transport.ReplyPath, data []byte) error {
			pk := packet{binding: sender.(*testPath).binding, data: slices.Clone(data), at: *e.now}
			e.sent = append(e.sent, pk)
			if e.failData && wirev2.PacketType(data[5]) == wirev2.TypeFECBundle {
				return io.ErrClosedPipe
			}
			e.out = append(e.out, pk)
			return nil
		}
		limits := creditv2.Limits{MaxPeerBytes: 64 << 20, MaxSessionBytes: 64 << 20, MaxSessions: 8, MaxPendingHandshakes: 8, MaxPendingAccepts: 64, MaxStreamsPerSession: 8, MaxPeerStreams: 64, MaxReservations: 1024}
		if adjust != nil {
			adjust(role, cfg, &limits)
		}
		e.peer, err = creditv2.New(limits)
		if err != nil {
			t.Fatal(err)
		}
		scope, base, err := e.peer.BeginHandshake(creditv2.Claim{PendingAccept: role == negotiationv2.Responder})
		if err != nil {
			t.Fatal(err)
		}
		e.base = base
		claims, err := RequiredInitialClaims(cfg)
		if err != nil {
			t.Fatal(err)
		}
		setup := handshakev2.Setup{ID: wirev2.SessionID{1}, Role: role, PathID: uint16(bootstrap), Binding: binding, Contract: contract, Scope: scope, Keys: wirev2.DirectionalKeys{ClientToServer: wirev2.Key{1}, ServerToClient: wirev2.Key{2}}}
		for _, claim := range claims {
			lease, err := scope.Reserve(claim)
			if err != nil {
				t.Fatal(err)
			}
			setup.Initial = append(setup.Initial, lease)
		}
		if err := scope.Promote(); err != nil {
			t.Fatal(err)
		}
		e.controller, err = New(setup, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(e.out) != 0 {
			t.Fatal("constructor emitted before handshake READY")
		}
		if role == negotiationv2.Initiator {
			pair.client = e
		} else {
			pair.server = e
		}
	}
	for _, e := range []*endpoint{pair.client, pair.server} {
		if _, err := e.controller.Start(pair.now); err != nil {
			t.Fatal(err)
		}
	}
	return pair
}

func (p *pair) pump(t testing.TB, done func() bool) {
	t.Helper()
	for step := 0; step < 20000; step++ {
		for _, e := range []*endpoint{p.client, p.server} {
			if e.controller.cfg.OwnedSends {
				p.sendOwned(t, e)
			}
		}
		for _, toServer := range []bool{true, false} {
			from, to := p.client, p.server
			if !toServer {
				from, to = p.server, p.client
			}
			out := from.out
			from.out = nil
			for _, pk := range out {
				if p.drop != nil && p.drop(toServer, pk) {
					continue
				}
				binding := opposite(pk.binding, toServer)
				result, err := to.controller.Receive(p.now, binding, &testPath{binding}, pk.data)
				if err != nil {
					t.Fatalf("receive type%d: %v", pk.data[5], err)
				}
				to.deliveries = append(to.deliveries, result.Deliveries...)
			}
		}
		if done() {
			return
		}
		if len(p.client.out)+len(p.server.out) > 0 {
			continue
		}
		due := p.client.controller.NextDeadline()
		other := p.server.controller.NextDeadline()
		if due.IsZero() || (!other.IsZero() && other.Before(due)) {
			due = other
		}
		if due.IsZero() {
			t.Fatal("no deadline before completion")
		}
		if !due.After(p.now) {
			due = p.now.Add(time.Nanosecond)
		}
		p.now = due
		for _, e := range []*endpoint{p.client, p.server} {
			result, err := e.controller.Advance(p.now)
			if err != nil {
				t.Fatal(err)
			}
			e.deliveries = append(e.deliveries, result.Deliveries...)
		}
	}
	t.Fatal("driver failed to make bounded progress")
}

func (p *pair) ready() bool {
	for _, e := range []*endpoint{p.client, p.server} {
		if !e.controller.Snapshot().Ready || !e.controller.receiveAckSent {
			return false
		}
		for _, path := range e.controller.paths[:e.controller.setup.Contract.MaxPaths] {
			if !path.active || path.sendEpoch != 2 || path.receiveEpoch != 2 {
				return false
			}
		}
	}
	return true
}

func (p *pair) close(t testing.TB) {
	t.Helper()
	for _, e := range []*endpoint{p.client, p.server} {
		scope := e.controller.setup.Scope
		e.controller.Close()
		e.controller.Close()
		for _, delivery := range e.deliveries {
			delivery.Release()
		}
		e.base.Release()
		scope.Close()
		if e.controller.setup.Keys != (wirev2.DirectionalKeys{}) || e.controller.cfg.BootstrapPath != nil || e.controller.cfg.Emit != nil || e.controller.cfg.Entropy != nil || len(e.controller.paths) != 0 {
			t.Fatal("closed controller retained keys or transport callbacks")
		}
		state := e.peer.Snapshot()
		if state.Bytes != 0 || state.Reservations != 0 || state.SessionSlots != 0 {
			t.Fatalf("leaked ownership: %+v", state)
		}
	}
}

func TestFixedDatagramRoundTripPreservesBootstrapAndJoins(t *testing.T) {
	p := newPair(t, 3, 2, false, 5000)
	p.pump(t, p.ready)
	payload := bytes.Repeat([]byte("0123456789abcdef"), 400)
	receipt, result, err := p.client.controller.Write(p.now, payload)
	if err != nil || receipt != 1 || result.CompletedThrough != 0 {
		t.Fatalf("write/fence boundary: %d %+v %v", receipt, result, err)
	}
	p.pump(t, func() bool {
		return p.client.controller.Snapshot().CompletedThrough == 1 && len(p.server.deliveries) == 1
	})
	if !bytes.Equal(p.server.deliveries[0].Payload(), payload) {
		t.Fatal("fragmented original changed")
	}
	var fecPacket packet
	paths := map[uint16]bool{}
	for _, pk := range p.client.sent {
		if wirev2.PacketType(pk.data[5]) == wirev2.TypeFECBundle {
			fecPacket = pk
			envelope, _ := wirev2.ParseEnvelope(pk.data)
			auth, _ := envelope.Authenticate(p.client.controller.sendKey)
			msg, _ := wirev2.DecodeEstablished(auth)
			if msg.Route.BudgetEpoch != 2 {
				t.Fatal("DATA sent before fixed epoch2")
			}
			paths[uint16(msg.Route.PathID)] = true
		}
	}
	if len(paths) != 3 || p.client.controller.setup.PathID != 2 {
		t.Fatal("joined paths or bootstrap configured index lost")
	}
	binding := opposite(fecPacket.binding, true)
	result, err = p.server.controller.Receive(p.now, binding, &testPath{binding}, fecPacket.data)
	if err != nil || len(result.Deliveries) != 0 {
		t.Fatal("completed group replay delivered twice")
	}
	p.close(t)
}

func TestAggregationFenceAndStickyFailure(t *testing.T) {
	p := newPair(t, 1, 1, true, 5000)
	p.pump(t, p.ready)
	for _, payload := range [][]byte{[]byte("alpha"), nil, []byte("omega")} {
		if _, result, err := p.client.controller.Write(p.now, payload); err != nil || len(result.Sends) != 0 {
			t.Fatalf("premature aggregation seal: %+v %v", result, err)
		}
	}
	fence, _, err := p.client.controller.Flush(p.now)
	if err != nil || fence != 3 {
		t.Fatalf("flush frontier: %d %v", fence, err)
	}
	p.pump(t, func() bool {
		return p.client.controller.Snapshot().CompletedThrough == 3 && len(p.server.deliveries) == 3
	})
	for i, want := range [][]byte{[]byte("alpha"), nil, []byte("omega")} {
		if !bytes.Equal(p.server.deliveries[i].Payload(), want) {
			t.Fatal("aggregated original mismatch")
		}
	}
	p.client.failData = true
	if _, _, err := p.client.controller.Write(p.now, []byte("failure")); err != nil {
		t.Fatal(err)
	}
	_, _, _ = p.client.controller.Flush(p.now)
	p.pump(t, func() bool { return p.client.controller.Snapshot().CompletedThrough == 4 })
	if state := p.client.controller.Snapshot(); !errors.Is(state.SendError, io.ErrClosedPipe) || state.FailedFrom != 4 {
		t.Fatalf("sticky failure frontier: %+v", state)
	}
	if _, _, err := p.client.controller.Flush(p.now); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("later Flush forgot asynchronous failure")
	}
	p.close(t)
}

func TestBudgetAckLossWithMinimumReceiveGrace(t *testing.T) {
	p := newPair(t, 1, 1, false, 100)
	dropped := false
	p.drop = func(toServer bool, pk packet) bool {
		if !toServer && !dropped && wirev2.PacketType(pk.data[5]) == wirev2.TypePathBudgetAck {
			dropped = true
			return true
		}
		return false
	}
	p.pump(t, p.ready)
	if !dropped || p.now.Before(time.Unix(1000, 0).Add(ControlRetry)) {
		t.Fatal("ACK loss did not cross short DATA grace")
	}
	p.close(t)
}
