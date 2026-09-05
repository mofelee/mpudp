package handshakev2

import (
	"bytes"
	"errors"
	"io"
	"math"
	"slices"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

const preparedTestBytes = testReceiveBytes + PacketReservationBytes + DeferredDisposalBytes + PreparedDialBytes

type pendingCleanup struct {
	calls   int
	scope   *creditv2.Session
	release func()
}

func prepareTestSide(t *testing.T, s *side) (*PreparedDial, *pendingCleanup) {
	t.Helper()
	cleanup := new(pendingCleanup)
	prepared, err := s.engine.PrepareDial(startTime, s.policy, func(release func()) {
		cleanup.calls++
		if cleanup.scope == nil || !cleanup.scope.Snapshot().Closed {
			t.Fatal("pending disposal began before closing its scope")
		}
		if _, err := s.engine.Advance(s.engine.lastNow); !errors.Is(err, ErrReentrant) {
			t.Fatal("pending disposal callback reentered Engine")
		}
		cleanup.release = release
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanup.scope = prepared.state.scope
	return prepared, cleanup
}

func finishPending(t *testing.T, cleanup *pendingCleanup) {
	t.Helper()
	if cleanup.calls != 1 || cleanup.release == nil {
		t.Fatalf("pending cleanup calls=%d release=%v", cleanup.calls, cleanup.release != nil)
	}
	var workers sync.WaitGroup
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			cleanup.release()
		}()
	}
	workers.Wait()
	cleanup.release()
}

func sixInitialClaims(s *side) {
	s.policy.Receive.Bytes = 1024
	s.policy.Initial = make([]creditv2.Claim, 6)
	for i := range s.policy.Initial {
		s.policy.Initial[i].Bytes = 4096
	}
	s.policy.Initial[5].Bytes = testReceiveBytes - 1024 - 5*4096
}

func TestPreparedDialConstructionAbortKeepsExactCredits(t *testing.T) {
	for _, cause := range []string{"abort", "engine-close", "ledger-close"} {
		t.Run(cause, func(t *testing.T) {
			bound := limits()
			bound.MaxSessions, bound.MaxPendingHandshakes, bound.MaxReservations = 1, 1, 10
			bound.MaxPeerBytes, bound.MaxSessionBytes = preparedTestBytes, preparedTestBytes
			s := newSide(t, false, 2, &bound)
			configureDeferred(t, s)
			sixInitialClaims(s)
			prepared, cleanup := prepareTestSide(t, s)
			copyHandle := *prepared
			initial := slices.Clone(prepared.state.initial)
			before := s.ledger.Snapshot()
			if before.Bytes != preparedTestBytes || before.Reservations != 10 || before.SessionSlots != 1 || before.PendingHandshakes != 1 ||
				s.engine.Snapshot().Prepared != 1 || s.engine.Snapshot().Pending != 0 || s.engine.Snapshot().PacketBytes != PacketReservationBytes ||
				!s.engine.NextDeadline().IsZero() || len(s.out) != 0 {
				t.Fatalf("preparation admission = %+v engine=%+v", before, s.engine.Snapshot())
			}
			if lease, err := cleanup.scope.Reserve(creditv2.Claim{Bytes: 1}); lease != nil || !errors.Is(err, creditv2.ErrResourceLimit) {
				t.Fatal("fixture did not reserve the exact one-session capacity")
			}
			switch cause {
			case "abort":
				if err := s.engine.AbortPreparedDial(startTime, prepared); err != nil {
					t.Fatal(err)
				}
			case "engine-close":
				if _, err := s.engine.Close(startTime); err != nil {
					t.Fatal(err)
				}
			case "ledger-close":
				s.ledger.Close()
				if err := s.engine.AbortPreparedDial(startTime, prepared); err != nil {
					t.Fatal(err)
				}
			}
			after := s.ledger.Snapshot()
			if cleanup.calls != 1 || after.Bytes != preparedTestBytes-PacketReservationBytes || after.Reservations != 9 || after.SessionSlots != 1 || s.engine.Snapshot().Prepared != 0 {
				t.Fatalf("construction cleanup released live owner: %+v calls=%d", after, cleanup.calls)
			}
			for i, lease := range initial {
				if lease.Snapshot().Released || lease.Snapshot().Bytes != s.policy.Initial[i].Bytes {
					t.Fatal("preparation changed or released an Initial index")
				}
			}
			if scope, lease, err := s.ledger.BeginHandshake(creditv2.Claim{Bytes: 1}); err == nil || scope != nil || lease != nil {
				t.Fatal("construction cleanup made its Session slot reusable")
			}
			if err := s.engine.AbortPreparedDial(startTime, &copyHandle); err != nil || cleanup.calls != 1 {
				t.Fatal("copied/repeated abort changed terminal ownership")
			}
			// Construction completion is an adapter barrier; Engine.Close cannot
			// prove a concurrent Carrier open has returned and been joined.
			constructed, finished := make(chan struct{}), make(chan struct{})
			go func() {
				<-constructed
				cleanup.release()
				close(finished)
			}()
			if s.ledger.Snapshot().SessionSlots != 1 {
				t.Fatal("unjoined construction lost its admission")
			}
			close(constructed)
			<-finished
			finishPending(t, cleanup)
			closeSide(t, s, startTime)
		})
	}
}

func TestPreparedDialRejectsAdmissionWithoutDisposal(t *testing.T) {
	for _, cause := range []string{"bytes", "reservations", "policy", "nil-hook", "synchronous", "closed"} {
		t.Run(cause, func(t *testing.T) {
			bound := limits()
			bound.MaxPeerBytes, bound.MaxSessionBytes = preparedTestBytes, preparedTestBytes
			if cause == "bytes" {
				bound.MaxPeerBytes--
				bound.MaxSessionBytes--
			}
			if cause == "reservations" {
				bound.MaxReservations = 3
			}
			s := newSide(t, false, 1, &bound)
			if cause != "synchronous" {
				configureDeferred(t, s)
			}
			policy := s.policy
			if cause == "policy" {
				policy.Receive.PendingAccept = true
			}
			calls := 0
			hook := func(func()) { calls++ }
			if cause == "nil-hook" {
				hook = nil
			}
			if cause == "closed" {
				_, _ = s.engine.Close(startTime)
			}
			prepared, err := s.engine.PrepareDial(startTime, policy, hook)
			if prepared != nil || err == nil || calls != 0 || len(s.out) != 0 || s.engine.Snapshot().Prepared != 0 || s.ledger.Snapshot().SessionSlots != 0 {
				t.Fatalf("failed preparation=%v err=%v calls=%d", prepared, err, calls)
			}
			closeSide(t, s, startTime)
		})
	}
}

func TestPreparedDialAdoptionValidationAndCopiedHandles(t *testing.T) {
	s, other := newSide(t, false, 2, nil), newSide(t, false, 2, nil)
	configureDeferred(t, s)
	configureDeferred(t, other)
	prepared, cleanup := prepareTestSide(t, s)
	copyHandle := *prepared
	before := s.ledger.Snapshot()
	for _, invalid := range [][]Carrier{nil, carriers(1), {{PathID: 1}, {PathID: 2}}} {
		if id, result, err := s.engine.BeginPreparedDial(startTime, prepared, invalid, time.Time{}); id != 0 || err == nil || len(result.Sends) != 0 || cleanup.calls != 0 || s.ledger.Snapshot() != before {
			t.Fatal("invalid start consumed preparation or emitted traffic")
		}
	}
	if _, _, err := s.engine.BeginPreparedDial(startTime, prepared, carriers(2), startTime); !errors.Is(err, ErrExpired) || cleanup.calls != 0 {
		t.Fatal("invalid deadline consumed preparation")
	}
	if err := other.engine.AbortPreparedDial(startTime, prepared); !errors.Is(err, ErrInvalid) {
		t.Fatal("wrong engine aborted preparation")
	}
	if _, _, err := other.engine.BeginPreparedDial(startTime, prepared, carriers(2), time.Time{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("wrong engine adopted preparation")
	}
	s.policy.Profile.MaxPaths = 1
	paths := carriers(2)
	id, result, err := s.engine.BeginPreparedDial(startTime, &copyHandle, paths, time.Time{})
	if err != nil || id == 0 || len(result.Sends) != 1 || s.engine.Snapshot().Prepared != 0 || s.ledger.Snapshot() != before {
		t.Fatalf("valid adoption failed: id=%d result=%+v err=%v", id, result, err)
	}
	paths[1].Binding.SocketID = 999
	if s.engine.dials[id].request.Carriers[1].Binding.SocketID != 2 {
		t.Fatal("caller changed copied Carrier request")
	}
	if _, _, err := s.engine.BeginPreparedDial(startTime, prepared, carriers(2), time.Time{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("copied handle adopted twice")
	}
	if err := s.engine.AbortPreparedDial(startTime, prepared); !errors.Is(err, ErrInvalid) || cleanup.calls != 0 {
		t.Fatal("stale preparation handle aborted the adopted dial")
	}
	if _, err := s.engine.CancelDial(startTime, id); err != nil {
		t.Fatal(err)
	}
	finishPending(t, cleanup)
	closeSide(t, s, startTime)
	closeSide(t, other, startTime)
}

func TestPreparedDialPostAdoptionFailureRetiresSynchronously(t *testing.T) {
	for _, cause := range []string{"entropy", "identifier", "closed-during-entropy"} {
		t.Run(cause, func(t *testing.T) {
			s := newSide(t, false, 2, nil)
			configureDeferred(t, s)
			prepared, cleanup := prepareTestSide(t, s)
			expected := ErrEntropy
			switch cause {
			case "entropy":
				s.engine.config.Entropy = &io.LimitedReader{R: s.engine.config.Entropy, N: 0}
			case "identifier":
				s.engine.nextDial = DialID(math.MaxUint64)
				expected = ErrExhausted
			case "closed-during-entropy":
				s.engine.config.Entropy = preparedClosingReader{s.engine.config.Entropy, s.ledger}
				expected = ErrClosed
			}
			id, result, err := s.engine.BeginPreparedDial(startTime, prepared, carriers(2), time.Time{})
			if id != 0 || !errors.Is(err, expected) || len(result.Sends) != 0 || cleanup.calls != 1 || cleanup.release == nil ||
				s.engine.Snapshot().Dials != 0 || s.engine.Snapshot().Pending != 0 || s.ledger.Snapshot().SessionSlots != 1 ||
				s.ledger.Snapshot().Bytes != preparedTestBytes-PacketReservationBytes {
				t.Fatalf("adopted failure lost cleanup: id=%d result=%+v err=%v ledger=%+v", id, result, err, s.ledger.Snapshot())
			}
			if err := s.engine.AbortPreparedDial(startTime, prepared); !errors.Is(err, ErrInvalid) || cleanup.calls != 1 {
				t.Fatal("adopted failure handle ran a stale disposer")
			}
			finishPending(t, cleanup)
			closeSide(t, s, startTime)
		})
	}
}

type preparedClosingReader struct {
	io.Reader
	ledger *creditv2.Peer
}

func (r preparedClosingReader) Read(p []byte) (int, error) {
	r.ledger.Close()
	return r.Reader.Read(p)
}

func TestPreparedSerialFallbackReusesOneExactAdmissionAndInstalls(t *testing.T) {
	bound := limits()
	bound.MaxSessions, bound.MaxPendingHandshakes, bound.MaxReservations = 1, 1, 10
	bound.MaxPeerBytes, bound.MaxSessionBytes = preparedTestBytes, preparedTestBytes
	client, server := newSide(t, false, 2, &bound), newSide(t, true, 2, nil)
	installed, finish, installedCalls := configureDeferred(t, client)
	sixInitialClaims(client)
	prepared, pending := prepareTestSide(t, client)
	initial := slices.Clone(prepared.state.initial)
	scope := prepared.state.scope
	id, _, err := client.engine.BeginPreparedDial(startTime, prepared, carriers(2), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	first := pop(t, client, wirev2.TypeHello)
	deliver(t, first, server, true, startTime)
	staleChallenge := pop(t, server, wirev2.TypeChallenge)
	oldID := client.engine.orderedIDs()[0]
	old := client.engine.sessions[oldID]
	packetBacking := old.packets
	before := client.ledger.Snapshot()
	now := startTime.Add(Lifetime)
	result := advance(t, client, now)
	second := pop(t, client, wirev2.TypeHello)
	current := client.engine.sessions[client.engine.orderedIDs()[0]]
	if len(result.Failures) != 1 || len(result.Sends) != 1 || result.Sends[0].PathID != 2 || pending.calls != 0 ||
		client.ledger.Snapshot() != before || current.setup.Scope != scope || current.packets != packetBacking ||
		!slices.Equal(current.setup.Initial, initial) || bytes.Equal(first.packet[8:24], second.packet[8:24]) ||
		bytes.Equal(first.packet[wirev2.PrefixSize:wirev2.PrefixSize+16], second.packet[wirev2.PrefixSize:wirev2.PrefixSize+16]) {
		t.Fatal("fallback changed admission, invoked cleanup or reused protocol identity")
	}
	if old.packets != nil || old.setup.Scope != nil || old.setup.Keys != (wirev2.DirectionalKeys{}) || old.transcript != (wirev2.Transcript{}) {
		t.Fatal("old attempt retained authority after fallback")
	}
	for _, lease := range initial {
		if lease.Snapshot().Released {
			t.Fatal("fallback released an Initial component lease")
		}
	}
	deliver(t, staleChallenge, client, false, now)
	if stale, err := client.engine.CloseSession(now, oldID); err != nil || len(stale.Sends) != 0 || pending.calls != 0 {
		t.Fatal("old attempt result disposed the new attempt")
	}
	deliver(t, second, server, true, now)
	deliver(t, pop(t, server, wirev2.TypeChallenge), client, false, now)
	deliver(t, pop(t, client, wirev2.TypeFinish), server, true, now)
	result = deliver(t, pop(t, server, wirev2.TypeReady), client, false, now)
	if len(result.Established) != 1 || result.Established[0].PathID != 2 || pending.calls != 0 ||
		installed.Scope != scope || !slices.Equal(installed.Initial, initial) || client.engine.Snapshot().Dials != 0 {
		t.Fatal("winner did not inherit exactly the prepared admission")
	}
	if err := client.engine.AbortPreparedDial(now, prepared); !errors.Is(err, ErrInvalid) {
		t.Fatal("stale preparation aborted the winner")
	}
	if _, err := client.engine.CancelDial(now, id); err != nil || pending.calls != 0 {
		t.Fatal("old Dial cancellation invoked pending cleanup after installation")
	}
	if _, err := client.engine.CloseSession(now, result.Established[0].ID); err != nil {
		t.Fatal(err)
	}
	if *installedCalls != 1 || *finish == nil || pending.calls != 0 || client.ledger.Snapshot().Bytes != preparedTestBytes-PacketReservationBytes {
		t.Fatal("installed retirement did not own prepared metadata/storage")
	}
	(*finish)()
	closeSide(t, client, now)
	closeSide(t, server, now)
}

func TestPreparedDialTerminalPendingCausesAndUnrelatedProgress(t *testing.T) {
	for _, cause := range []string{"expiry", "deadline", "cancel", "engine", "ledger", "fallback-entropy", "reject"} {
		t.Run(cause, func(t *testing.T) {
			paths := 1
			if cause == "deadline" || cause == "fallback-entropy" {
				paths = 2
			}
			s := newSide(t, false, paths, nil)
			configureDeferred(t, s)
			prepared, cleanup := prepareTestSide(t, s)
			deadline := time.Time{}
			if cause == "deadline" {
				deadline = startTime.Add(time.Second)
			}
			id, _, err := s.engine.BeginPreparedDial(startTime, prepared, carriers(paths), deadline)
			if err != nil {
				t.Fatal(err)
			}
			hello := pop(t, s, wirev2.TypeHello)
			now := startTime
			switch cause {
			case "expiry", "fallback-entropy":
				if cause == "fallback-entropy" {
					s.engine.config.Entropy = &io.LimitedReader{R: s.engine.config.Entropy, N: 0}
				}
				now = now.Add(Lifetime)
				advance(t, s, now)
			case "deadline":
				now = deadline
				advance(t, s, now)
			case "cancel":
				_, err = s.engine.CancelDial(now, id)
			case "engine":
				_, err = s.engine.Close(now)
			case "ledger":
				s.ledger.Close()
				advance(t, s, now)
			case "reject":
				server := newSide(t, true, paths, nil)
				server.engine.config.Listener.Profile.DataShards++
				if _, rejectErr := server.engine.Receive(now, reverse(hello, true), hello.packet); rejectErr == nil {
					t.Fatal("incompatible listener accepted HELLO")
				}
				deliver(t, pop(t, server, wirev2.TypeReject), s, false, now)
				closeSide(t, server, now)
			}
			if err != nil || cleanup.calls != 1 || cleanup.release == nil || s.engine.Snapshot().Pending != 0 ||
				s.engine.Snapshot().Dials != 0 || s.ledger.Snapshot().SessionSlots != 1 || len(s.out) != 0 {
				t.Fatalf("terminal pending cause retained protocol state: %+v err=%v", s.engine.Snapshot(), err)
			}
			if cause == "cancel" {
				other := begin(t, s, 1, 1, now)
				if other == id || s.ledger.Snapshot().SessionSlots != 2 {
					t.Fatal("held cleanup prevented independent admission with spare capacity")
				}
			}
			finishPending(t, cleanup)
			closeSide(t, s, now)
		})
	}
}

func TestPreparedDialPromotedInstallationFailureIsTerminal(t *testing.T) {
	for _, cause := range []string{"error-disposer", "nil-disposer", "closed-scope"} {
		t.Run(cause, func(t *testing.T) {
			client, server := newSide(t, false, 2, nil), newSide(t, true, 2, nil)
			_, finish, installedCalls := configureDeferred(t, client)
			sixInitialClaims(client)
			install := client.engine.config.InstallDeferred
			client.engine.config.InstallDeferred = func(setup Setup) (func(func()), error) {
				if _, err := setup.Scope.BindBytes(setup.Initial[0], setup.Initial[0].Snapshot().Bytes); err != nil {
					t.Fatal(err)
				}
				if cause == "nil-disposer" {
					return nil, io.ErrClosedPipe
				}
				dispose, err := install(setup)
				if cause == "closed-scope" {
					setup.Scope.Close()
					return dispose, err
				}
				return dispose, io.ErrClosedPipe
			}
			prepared, pending := prepareTestSide(t, client)
			if _, _, err := client.engine.BeginPreparedDial(startTime, prepared, carriers(2), time.Time{}); err != nil {
				t.Fatal(err)
			}
			deliver(t, pop(t, client, wirev2.TypeHello), server, true, startTime)
			deliver(t, pop(t, server, wirev2.TypeChallenge), client, false, startTime)
			deliver(t, pop(t, client, wirev2.TypeFinish), server, true, startTime)
			ready := pop(t, server, wirev2.TypeReady)
			result, err := client.engine.Receive(startTime, reverse(ready, false), ready.packet)
			if !errors.Is(err, ErrInstallation) || len(result.Established) != 0 || client.engine.Snapshot().Dials != 0 ||
				client.engine.Snapshot().Pending != 0 || client.ledger.Snapshot().SessionSlots != 1 ||
				client.ledger.Snapshot().EstablishedSessions != 1 || len(result.Sends) != 1 || result.Sends[0].Type != wirev2.TypeClose {
				t.Fatalf("promoted failure restarted fallback: %+v err=%v", result, err)
			}
			if cause == "nil-disposer" {
				if *installedCalls != 0 {
					t.Fatal("nil installer disposer invoked installed cleanup")
				}
				finishPending(t, pending)
			} else {
				if pending.calls != 0 || *installedCalls != 1 || *finish == nil {
					t.Fatal("installer and pending cleanup did not transfer exactly once")
				}
				(*finish)()
			}
			closeSide(t, client, startTime)
			closeSide(t, server, startTime)
		})
	}
}

func TestPreparedDialCountsAgainstSharedEngineAdmission(t *testing.T) {
	bound := limits()
	bound.MaxPendingHandshakes = 1024
	s := newSide(t, true, 1, &bound)
	configureDeferred(t, s)
	policy := s.policy
	policy.Receive.PendingAccept = false
	for range MaxPending {
		if _, err := s.engine.PrepareDial(startTime, policy, func(release func()) { release() }); err != nil {
			t.Fatal(err)
		}
	}
	before := s.ledger.Snapshot()
	if snapshot := s.engine.Snapshot(); snapshot.Prepared != MaxPending || snapshot.Pending != 0 || snapshot.PacketBytes != MaxPending*PacketReservationBytes {
		t.Fatalf("unstarted preparation snapshot=%+v", snapshot)
	}
	if _, err := s.engine.PrepareDial(startTime, policy, func(func()) { t.Fatal("failed prepare invoked cleanup") }); !errors.Is(err, creditv2.ErrResourceLimit) {
		t.Fatal("preparation bypassed engine pending cap")
	}
	if _, _, err := s.engine.BeginDial(startTime, DialRequest{Policy: policy, Carriers: carriers(1)}); !errors.Is(err, creditv2.ErrResourceLimit) {
		t.Fatal("legacy dial bypassed occupied preparation capacity")
	}
	client := newSide(t, false, 1, nil)
	begin(t, client, 1, 1, startTime)
	hello := pop(t, client, wirev2.TypeHello)
	if _, err := s.engine.Receive(startTime, reverse(hello, true), hello.packet); !errors.Is(err, creditv2.ErrResourceLimit) || s.ledger.Snapshot() != before {
		t.Fatal("listener bypassed occupied preparation capacity")
	}
	closeSide(t, s, startTime)
	closeSide(t, client, startTime)
}

func TestPreparedDialMetadataBound(t *testing.T) {
	bytes := uint64(unsafe.Sizeof(preparedDialState{})) + uint64(unsafe.Sizeof(PreparedDial{})) +
		uint64(unsafe.Sizeof(dial{})) + uint64(unsafe.Sizeof(attempt{})) +
		MaxPending*uint64(unsafe.Sizeof(Carrier{})) + MaxInitialReservations*uint64(unsafe.Sizeof(creditv2.Claim{})) +
		4*MaxInitialReservations*uint64(unsafe.Sizeof((*creditv2.Lease)(nil))) + 1024
	if bytes > PreparedDialBytes {
		t.Fatalf("prepared metadata requires %d bytes, prepaid %d", bytes, PreparedDialBytes)
	}
}

func TestPreparedDialMaximumInitialIndexesAndEmptyReceive(t *testing.T) {
	bound := limits()
	bound.MaxSessions, bound.MaxPendingHandshakes = 1, 1
	bound.MaxPeerBytes, bound.MaxSessionBytes = preparedTestBytes, preparedTestBytes
	bound.MaxReservations = MaxInitialReservations + 3
	s := newSide(t, false, 1, &bound)
	configureDeferred(t, s)
	s.policy.Receive = creditv2.Claim{}
	s.policy.Initial = make([]creditv2.Claim, MaxInitialReservations)
	for i := range s.policy.Initial {
		s.policy.Initial[i].Bytes = testReceiveBytes / MaxInitialReservations
	}
	prepared, cleanup := prepareTestSide(t, s)
	if prepared.state.receive != nil || len(prepared.state.initial) != MaxInitialReservations ||
		s.ledger.Snapshot().Reservations != MaxInitialReservations+3 || s.ledger.Snapshot().Bytes != preparedTestBytes {
		t.Fatal("maximum Initial indexes or count-only receive admission changed")
	}
	initial := slices.Clone(prepared.state.initial)
	for i := range s.policy.Initial {
		s.policy.Initial[i].Bytes = 1
	}
	if _, _, err := s.engine.BeginPreparedDial(startTime, prepared, carriers(1), time.Time{}); err != nil {
		t.Fatal(err)
	}
	for i, lease := range s.engine.sessions[s.engine.orderedIDs()[0]].setup.Initial {
		if lease != initial[i] || lease.Snapshot().Bytes != testReceiveBytes/MaxInitialReservations {
			t.Fatal("caller policy mutation changed a prepared Initial mapping")
		}
	}
	if _, err := s.engine.Close(startTime); err != nil {
		t.Fatal(err)
	}
	finishPending(t, cleanup)
	closeSide(t, s, startTime)
}
