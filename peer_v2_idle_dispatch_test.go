//go:build linux

package mpudp

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/handshakev2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/sessionv2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

type v2IdlePath struct {
	binding handshakev2.Binding
	checks  uint64
}

func (p *v2IdlePath) Available() bool                  { p.checks++; return true }
func (*v2IdlePath) Generation() uint64                 { return 1 }
func (*v2IdlePath) PathID() string                     { return "idle-test" }
func (p *v2IdlePath) LocalAddr() net.Addr              { return net.UDPAddrFromAddrPort(p.binding.Local) }
func (p *v2IdlePath) RemoteAddr() net.Addr             { return net.UDPAddrFromAddrPort(p.binding.Remote) }
func (*v2IdlePath) Send(context.Context, []byte) error { return nil }

func newV2IdleSlots(count int) []v2SendSlot {
	slots := make([]v2SendSlot, count)
	for i := range slots {
		slots[i].jobs = make(chan v2SendJob, 1)
		slots[i].results = make(chan v2SendCompletion, 1)
	}
	return slots
}

// Controllers exchange setup packets in memory. No driver or socket worker can
// change readiness while the test inspects one dispatch rotation.
func newV2IdleRuntime(t testing.TB, workers, count int) (*v2Peer, []*v2Session, []*v2IdlePath) {
	t.Helper()
	cfg := v2LoopbackConfig(true)
	cfg.Carriers = []string{"127.0.0.1:40000"}
	cfg.Limits.MaxSendWorkers = workers
	p := &Peer{config: cfg, ctx: context.Background(), wake: make(chan struct{}, 1), diagnostics: make(chan error, 1)}
	p.initStatistics()
	r := &v2Peer{peer: p, sessions: make(map[*v2Session]struct{}), sendSlots: newV2IdleSlots(workers)}
	controllerConfig, err := v2ControllerConfig(cfg, false)
	if err != nil {
		t.Fatal(err)
	}
	controllerConfig.Entropy = rand.Reader
	controllerConfig.PathRatesBPS[1] = 1_000_000_000_000
	_, contract, err := negotiationv2.Select(negotiationv2.Advertisement{Profile: controllerConfig.LocalProfile, BootstrapPathID: 1}, controllerConfig.LocalProfile)
	if err != nil {
		t.Fatal(err)
	}
	var sessions []*v2Session
	var paths []*v2IdlePath
	var controllers []*sessionv2.Controller
	var releases []func()
	t.Cleanup(func() {
		for i := range r.sendSlots {
			if r.sendSlots[i].busy {
				v2CompleteIdleSend(t, r, i, context.Canceled)
			}
		}
		for _, s := range sessions {
			s.cancel()
		}
		for _, controller := range controllers {
			controller.Close()
		}
		for _, release := range releases {
			release()
		}
	})
	for i := 0; i < count; i++ {
		binding := handshakev2.Binding{SocketID: uint64(i + 2), Local: netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(30001+i)), Remote: netip.MustParseAddrPort("127.0.0.1:40000")}
		pairPaths := [2]*v2IdlePath{{binding: binding}, {binding: handshakev2.Binding{SocketID: 1, Local: binding.Remote, Remote: binding.Local}}}
		var pair [2]*sessionv2.Controller
		for side, role := range []negotiationv2.Role{negotiationv2.Initiator, negotiationv2.Responder} {
			ledger, err := creditv2.New(creditv2.Limits{MaxPeerBytes: 64 << 20, MaxSessionBytes: 64 << 20, MaxSessions: 8, MaxPendingHandshakes: 8, MaxPendingAccepts: 8, MaxStreamsPerSession: 8, MaxPeerStreams: 8, MaxReservations: 1024})
			if err != nil {
				t.Fatal(err)
			}
			scope, base, err := ledger.BeginHandshake(creditv2.Claim{PendingAccept: role == negotiationv2.Responder})
			if err != nil {
				t.Fatal(err)
			}
			releases = append(releases, func() { scope.Close(); base.Release(); ledger.Close() })
			local := controllerConfig
			local.BootstrapPath = pairPaths[side]
			local.Carriers = []sessionv2.Carrier{{Carrier: handshakev2.Carrier{PathID: 1, Binding: binding}, Sender: pairPaths[0]}}
			setup := handshakev2.Setup{ID: wirev2.SessionID{byte(i + 1)}, Role: role, PathID: 1, Binding: pairPaths[side].binding, Contract: contract, Scope: scope,
				Keys: wirev2.DirectionalKeys{ClientToServer: wirev2.Key{1}, ServerToClient: wirev2.Key{2}}}
			claims, err := sessionv2.RequiredInitialClaims(local)
			if err != nil {
				t.Fatal(err)
			}
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
			pair[side], err = sessionv2.New(setup, local)
			if err != nil {
				t.Fatal(err)
			}
			controllers = append(controllers, pair[side])
		}
		v2ReadyIdlePair(t, pair, pairPaths)
		s := r.newWrapper(false)
		s.controller = pair[0]
		sessions = append(sessions, s)
		paths = append(paths, pairPaths[0])
	}
	for _, path := range paths {
		path.checks = 0
	}
	return r, sessions, paths
}

func v2ReadyIdlePair(t testing.TB, pair [2]*sessionv2.Controller, paths [2]*v2IdlePath) {
	t.Helper()
	now := time.Now().Add(-time.Second)
	for _, controller := range pair {
		if _, err := controller.Start(now); err != nil {
			t.Fatal(err)
		}
	}
	for step := 0; step < 128; step++ {
		idle := true
		for side, controller := range pair {
			intent, _, err := controller.TakeSend(now)
			if err != nil {
				t.Fatal(err)
			}
			if intent == nil {
				continue
			}
			idle = false
			other := 1 - side
			_, receiveErr := pair[other].Receive(now, paths[other].binding, paths[other], intent.Packet)
			intent.Release()
			_, completeErr := controller.CompleteSend(now, sessionv2.SendOutcome{Token: intent.Token, Invoked: true, FinishedAt: now})
			if err := errors.Join(receiveErr, completeErr); err != nil {
				t.Fatal(err)
			}
		}
		if idle && pair[0].Snapshot().Ready && pair[1].Snapshot().Ready {
			return
		}
		now = now.Add(time.Millisecond)
	}
	t.Fatal("in-memory controllers did not become ready")
}

func v2CompleteIdleSend(t testing.TB, r *v2Peer, slot int, failure error) *v2Session {
	t.Helper()
	select {
	case job := <-r.sendSlots[slot].jobs:
		token := job.intent.Token
		job.intent.Release()
		r.sendSlots[slot].results <- v2SendCompletion{session: job.session, outcome: sessionv2.SendOutcome{Token: token, Invoked: true, FinishedAt: time.Now(), Err: failure}}
		r.consumeSendCompletions()
		return job.session
	default:
		t.Fatal("expected a dispatched send")
		return nil
	}
}

func TestV2IdleDispatchStopsAfterOneEmptyRotation(t *testing.T) {
	r, _, paths := newV2IdleRuntime(t, 8, 3)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sendSlots = newV2IdleSlots(1)
	r.dispatchSends()
	checks := make([]uint64, len(paths))
	for i, path := range paths {
		checks[i], path.checks = path.checks, 0
		if checks[i] == 0 {
			t.Fatal("idle scan did not inspect the ready Session")
		}
	}
	r.sendSlots = newV2IdleSlots(8)
	r.dispatchSends()
	for i, path := range paths {
		if path.checks != checks[i] {
			t.Fatalf("Session %d was rescanned for idle slots: one worker=%d eight workers=%d", i, checks[i], path.checks)
		}
	}
}

func TestV2IdleDispatchResumesAndPreservesSessionRotation(t *testing.T) {
	r, sessions, _ := newV2IdleRuntime(t, 2, 3)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dispatchSends()
	for _, s := range sessions {
		if _, _, err := s.controller.Write(time.Now(), []byte("ready after idle")); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.controller.Flush(time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	r.dispatchSends()
	if got := v2CompleteIdleSend(t, r, 0, nil); got != sessions[0] {
		t.Fatal("first ready Session lost its turn")
	}
	if got := v2CompleteIdleSend(t, r, 1, nil); got != sessions[1] {
		t.Fatal("second ready Session lost its turn")
	}
	r.dispatchSends()
	if got := v2CompleteIdleSend(t, r, 0, nil); got != sessions[2] {
		t.Fatal("next dispatch skipped the Session behind occupied workers")
	}
}

func waitV2IdleRegistration(t *testing.T, s *v2Session) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.owner.mu.Lock()
		waiting := s.changed != nil
		s.owner.mu.Unlock()
		if waiting {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("public operation did not register its waiter")
}

// Keep the fixture compatible with the eager-channel baseline so both test
// executables use identical registration and benchmark code.
func v2IdleRegisterChange(s *v2Session) <-chan struct{} {
	if s.changed == nil {
		s.changed = make(chan struct{})
	}
	return s.changed
}

func TestV2LazyChangeBroadcastAndReuse(t *testing.T) {
	r := &v2Peer{peer: &Peer{ctx: context.Background()}, sessions: make(map[*v2Session]struct{})}
	s := r.newWrapper(false)
	defer s.cancel()
	if s.changed != nil {
		t.Fatal("Session allocated a change channel before a waiter")
	}
	for round := 0; round < 4; round++ {
		const waiters = 8
		registered, woke := make(chan struct{}, waiters), make(chan struct{}, waiters)
		var waiting sync.WaitGroup
		for range waiters {
			waiting.Add(1)
			go func() {
				defer waiting.Done()
				r.mu.Lock()
				changed := v2IdleRegisterChange(s)
				r.mu.Unlock()
				registered <- struct{}{}
				<-changed
				woke <- struct{}{}
			}()
		}
		for range waiters {
			<-registered
		}
		r.mu.Lock()
		s.notify()
		s.notify()
		if s.changed != nil {
			t.Fatal("notification retained an unused change channel")
		}
		r.mu.Unlock()
		for range waiters {
			select {
			case <-woke:
			case <-time.After(time.Second):
				t.Fatal("notification lost a registered waiter")
			}
		}
		waiting.Wait()
	}
}

func TestV2LazyChangePublicCloseWakesReadAndFence(t *testing.T) {
	for _, operation := range []string{"read", "fence"} {
		t.Run(operation, func(t *testing.T) {
			r, sessions, _ := newV2IdleRuntime(t, 1, 1)
			s := sessions[0]
			result := make(chan error, 1)
			go func() {
				if operation == "read" {
					_, err := s.ReadPacket()
					result <- err
				} else {
					result <- s.waitFence(context.Background(), 1)
				}
			}()
			waitV2IdleRegistration(t, s)
			r.mu.Lock()
			r.dispose(s)
			r.mu.Unlock()
			select {
			case err := <-result:
				if !errors.Is(err, ErrClosed) {
					t.Fatalf("closed %s = %v", operation, err)
				}
			case <-time.After(time.Second):
				t.Fatalf("Close did not wake %s", operation)
			}
		})
	}
}

func TestV2LazyChangeFenceWaitsForAllShardsAndRetainsFailure(t *testing.T) {
	for _, failure := range []error{nil, io.ErrClosedPipe} {
		name := "success"
		if failure != nil {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			r, sessions, _ := newV2IdleRuntime(t, 1, 1)
			s := sessions[0]
			if err := s.WritePacket([]byte("one whole original")); err != nil {
				t.Fatal(err)
			}
			r.mu.Lock()
			fence, _, err := s.controller.Flush(time.Now())
			r.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- s.waitFence(ctx, uint64(fence)) }()
			waitV2IdleRegistration(t, s)
			shards := r.peer.config.FEC.DataShards + r.peer.config.FEC.ParityShards
			for i := 0; i < shards; i++ {
				r.mu.Lock()
				r.dispatchSends()
				var sendErr error
				if i == 0 {
					sendErr = failure
				}
				v2CompleteIdleSend(t, r, 0, sendErr)
				r.mu.Unlock()
				if i+1 < shards {
					select {
					case err := <-result:
						t.Fatalf("fence returned before every shard: %v", err)
					default:
					}
				}
			}
			select {
			case err := <-result:
				if !errors.Is(err, failure) {
					t.Fatalf("fence error=%v, want %v", err, failure)
				}
			case <-ctx.Done():
				t.Fatal("terminal shard did not wake the fence")
			}
		})
	}
}

func BenchmarkV2WakeNotification(b *testing.B) {
	for _, mode := range []string{"no-waiter", "registered-waiter", "eager-baseline"} {
		b.Run(mode, func(b *testing.B) {
			r := &v2Peer{}
			s := &v2Session{owner: r, changed: make(chan struct{})}
			if mode != "eager-baseline" {
				s.notify()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r.mu.Lock()
				switch mode {
				case "registered-waiter":
					_ = v2IdleRegisterChange(s)
					s.notify()
				case "eager-baseline":
					close(s.changed)
					s.changed = make(chan struct{})
				default:
					s.notify()
				}
				r.mu.Unlock()
			}
		})
	}
}

func BenchmarkV2IdleDispatch(b *testing.B) {
	for _, sessions := range []int{1, 8} {
		for _, workers := range []int{1, 8} {
			b.Run("sessions-"+strconv.Itoa(sessions)+"/workers-"+strconv.Itoa(workers), func(b *testing.B) {
				r, _, paths := newV2IdleRuntime(b, workers, sessions)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					r.mu.Lock()
					r.dispatchSends()
					r.mu.Unlock()
				}
				b.StopTimer()
				var checks uint64
				for _, path := range paths {
					checks += path.checks
				}
				b.ReportMetric(float64(checks)/float64(b.N), "path-checks/op")
			})
		}
	}
}
