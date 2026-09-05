package creditv2

import (
	"errors"
	"math"
	"sync"
	"testing"
)

func testLimits() Limits {
	return Limits{MaxPeerBytes: 100, MaxSessionBytes: 60, MaxSessions: 3, MaxPendingHandshakes: 2, MaxPendingAccepts: 2, MaxStreamsPerSession: 2, MaxPeerStreams: 3, MaxReservations: 8}
}

func testPeer(t testing.TB, limits Limits) *Peer {
	t.Helper()
	p, err := New(limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func testSession(t testing.TB, p *Peer, claim Claim) (*Session, *Lease) {
	t.Helper()
	s, lease, err := p.BeginSession(claim)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	t.Cleanup(lease.Release)
	return s, lease
}

func assertAccounting(t testing.TB, p *Peer) {
	t.Helper()
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	var total Usage
	pending, established := 0, 0
	for s := range p.state.sessions {
		if s.retired || (s.closed && s.usage.Reservations == 0) {
			t.Fatal("retired scope retained by Peer")
		}
		total.Bytes += s.usage.Bytes
		total.Reservations += s.usage.Reservations
		total.BusinessStreams += s.usage.BusinessStreams
		total.PendingAccepts += s.usage.PendingAccepts
		if s.pendingHandshake {
			pending++
		} else {
			established++
		}
		if s.usage.Bytes > p.state.limits.MaxSessionBytes || s.usage.BusinessStreams > p.state.limits.MaxStreamsPerSession {
			t.Fatal("Session ceiling exceeded")
		}
	}
	u := p.state.usage
	if total != u.Usage || pending != u.PendingHandshakes || established != u.EstablishedSessions || pending+established != u.SessionSlots {
		t.Fatalf("Peer/Session accounting differs: total=%+v peer=%+v", total, u)
	}
	l := p.state.limits
	if u.Bytes > l.MaxPeerBytes || u.SessionSlots > l.MaxSessions || u.PendingHandshakes > l.MaxPendingHandshakes || u.PendingAccepts > l.MaxPendingAccepts || u.BusinessStreams > l.MaxPeerStreams || u.Reservations > l.MaxReservations {
		t.Fatal("Peer ceiling exceeded")
	}
}

func TestHandshakeReservesFutureCapacityAndPromotesWithoutCompetition(t *testing.T) {
	l := testLimits()
	l.MaxSessions, l.MaxPendingHandshakes = 1, 1
	l.MaxPendingAccepts, l.MaxPeerStreams, l.MaxStreamsPerSession = 1, 1, 1
	l.MaxPeerBytes, l.MaxSessionBytes, l.MaxReservations = 60, 60, 1
	p := testPeer(t, l)
	claim := Claim{Bytes: 60, BusinessStream: true, PendingAccept: true}
	s, lease, err := p.BeginHandshake(claim)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer lease.Release()
	before := p.Snapshot()
	if before.SessionSlots != 1 || before.PendingHandshakes != 1 || before.EstablishedSessions != 0 || before.Usage != (Usage{Bytes: 60, Reservations: 1, BusinessStreams: 1, PendingAccepts: 1}) {
		t.Fatalf("initial provisional reservation: %+v", before)
	}
	if s2, l2, err := p.BeginSession(Claim{}); !errors.Is(err, ErrResourceLimit) || s2 != nil || l2 != nil || p.Snapshot() != before {
		t.Fatal("future Session slot was not reserved atomically")
	}
	if err := lease.MarkAccepted(); !errors.Is(err, ErrInvalid) || p.Snapshot() != before {
		t.Fatal("pending handshake became accepted")
	}
	for range 2 {
		if err := s.Promote(); err != nil {
			t.Fatalf("pre-reserved promotion competed again: %v", err)
		}
	}
	after := p.Snapshot()
	if after.Usage != before.Usage || after.SessionSlots != 1 || after.PendingHandshakes != 0 || after.EstablishedSessions != 1 {
		t.Fatalf("promotion changed ownership: %+v", after)
	}
	for range 2 {
		if err := lease.MarkAccepted(); err != nil {
			t.Fatal(err)
		}
	}
	if u := p.Snapshot(); u.Bytes != 60 || u.BusinessStreams != 1 || u.PendingAccepts != 0 || u.Reservations != 1 {
		t.Fatalf("Accept prematurely reclaimed bytes/stream: %+v", u)
	}
	assertAccounting(t, p)
}

func TestAdmissionAndClaimsFailWithoutPartialCharge(t *testing.T) {
	for _, test := range []struct {
		name   string
		limits func(*Limits)
		claim  Claim
	}{
		{"Session bytes", func(l *Limits) { l.MaxSessionBytes = 10 }, Claim{Bytes: 11, BusinessStream: true, PendingAccept: true}},
		{"overflow request", func(*Limits) {}, Claim{Bytes: math.MaxUint64, BusinessStream: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			limits := testLimits()
			test.limits(&limits)
			p := testPeer(t, limits)
			before := p.Snapshot()
			for _, begin := range []func(Claim) (*Session, *Lease, error){p.BeginHandshake, p.BeginSession} {
				if s, lease, err := begin(test.claim); !errors.Is(err, ErrResourceLimit) || s != nil || lease != nil || p.Snapshot() != before {
					t.Fatal("failed initial Claim charged admission")
				}
			}
		})
	}
	for _, limit := range []string{"Peer bytes", "Session bytes", "Peer streams", "Session streams", "pending accepts", "reservations"} {
		t.Run(limit, func(t *testing.T) {
			limits := testLimits()
			switch limit {
			case "Peer bytes":
				limits.MaxPeerBytes, limits.MaxSessionBytes = 10, 10
			case "Session bytes":
				limits.MaxSessionBytes = 10
			case "Peer streams":
				limits.MaxPeerStreams = 1
			case "Session streams":
				limits.MaxStreamsPerSession = 1
			case "pending accepts":
				limits.MaxPendingAccepts = 1
			case "reservations":
				limits.MaxReservations = 1
			}
			p := testPeer(t, limits)
			s, _ := testSession(t, p, Claim{Bytes: 10, BusinessStream: true, PendingAccept: true})
			target := s
			if limit == "Peer bytes" || limit == "Peer streams" {
				target, _ = testSession(t, p, Claim{})
			}
			before, beforeSession := p.Snapshot(), target.Snapshot()
			if lease, err := target.Reserve(Claim{Bytes: 1, BusinessStream: true, PendingAccept: true}); !errors.Is(err, ErrResourceLimit) || lease != nil || p.Snapshot() != before || target.Snapshot() != beforeSession {
				t.Fatal("failed multiresource reservation left a partial charge")
			}
			assertAccounting(t, p)
		})
	}
}

func TestPendingHandshakeLimitIsIndependent(t *testing.T) {
	l := testLimits()
	l.MaxPendingHandshakes = 1
	p := testPeer(t, l)
	s, lease, err := p.BeginHandshake(Claim{})
	if err != nil || lease != nil {
		t.Fatalf("empty initial admission: %v", err)
	}
	defer s.Close()
	before := p.Snapshot()
	if _, _, err := p.BeginHandshake(Claim{}); !errors.Is(err, ErrResourceLimit) || p.Snapshot() != before {
		t.Fatal("pending-handshake cap bypassed")
	}
	testSession(t, p, Claim{})
	if err := s.Promote(); err != nil {
		t.Fatal(err)
	}
	second, _, err := p.BeginHandshake(Claim{})
	if err != nil {
		t.Fatal("promotion failed to return pending-handshake capacity")
	}
	second.Close()
	assertAccounting(t, p)
}

func TestTransferPreservesPeerChargeAndRejectsBeforeDebit(t *testing.T) {
	l := testLimits()
	l.MaxReservations, l.MaxPeerBytes = 2, 60
	p := testPeer(t, l)
	source, moving := testSession(t, p, Claim{Bytes: 40, BusinessStream: true, PendingAccept: true})
	target, held := testSession(t, p, Claim{Bytes: 20, BusinessStream: true})
	before := p.Snapshot()
	if _, err := target.Reserve(Claim{Bytes: 40}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal("simultaneous copy was not separately charged")
	}
	if err := moving.Transfer(target); err != nil || p.Snapshot() != before {
		t.Fatalf("same backing transfer competed for Peer capacity: %v", err)
	}
	if source.Snapshot().Usage != (Usage{}) || target.Snapshot().Bytes != 60 || target.Snapshot().BusinessStreams != 2 {
		t.Fatal("transfer lost its per-Session charge")
	}
	if err := moving.Transfer(target); err != nil || p.Snapshot() != before {
		t.Fatal("same-owner transfer was not idempotent")
	}
	if err := moving.Transfer(source); err != nil {
		t.Fatal(err)
	}
	held.Release()
	moving.Release()
	p = testPeer(t, testLimits())
	source, moving = testSession(t, p, Claim{Bytes: 40, BusinessStream: true, PendingAccept: true})
	target, _ = testSession(t, p, Claim{Bytes: 30})
	before, beforeSource, beforeTarget := p.Snapshot(), source.Snapshot(), target.Snapshot()
	if err := moving.Transfer(target); !errors.Is(err, ErrResourceLimit) || p.Snapshot() != before || source.Snapshot() != beforeSource || target.Snapshot() != beforeTarget {
		t.Fatal("destination failure debited source")
	}
	other := testPeer(t, testLimits())
	foreign, _ := testSession(t, other, Claim{})
	if err := moving.Transfer(foreign); !errors.Is(err, ErrInvalid) || p.Snapshot() != before {
		t.Fatal("cross-Peer transfer accepted")
	}
	assertAccounting(t, p)
}

func TestCloseRetainsOwnedBytesAndCountsUntilLastRelease(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "established", true: "pending"}[pending], func(t *testing.T) {
			p := testPeer(t, testLimits())
			begin := p.BeginSession
			if pending {
				begin = p.BeginHandshake
			}
			s, lease, err := begin(Claim{Bytes: 12, BusinessStream: true, PendingAccept: true})
			if err != nil {
				t.Fatal(err)
			}
			countOnly, err := s.Reserve(Claim{PendingAccept: true})
			if err != nil {
				t.Fatal(err)
			}
			before := p.Snapshot()
			copySession, copyLease := *s, *lease
			s.Close()
			copySession.Close()
			if p.Snapshot() != before || !s.Snapshot().Closed || s.Snapshot().Retired {
				t.Fatal("Close revoked live ownership")
			}
			if _, err := s.Reserve(Claim{Bytes: 1}); !errors.Is(err, ErrClosed) {
				t.Fatal("closed Session admitted bytes")
			}
			if err := s.Promote(); !errors.Is(err, ErrClosed) {
				t.Fatal("closed scope promoted")
			}
			if err := lease.MarkAccepted(); !errors.Is(err, ErrClosed) {
				t.Fatal("closed scope accepted")
			}
			lease.Release()
			copyLease.Release()
			if u := p.Snapshot(); u.Bytes != 0 || u.BusinessStreams != 0 || u.PendingAccepts != 1 || u.SessionSlots != 1 || u.Reservations != 1 {
				t.Fatalf("count-only lease lost or copy double-released: %+v", u)
			}
			countOnly.Release()
			if p.Snapshot() != (PeerSnapshot{}) || !s.Snapshot().Retired {
				t.Fatal("final lease did not retire closed scope")
			}
			assertAccounting(t, p)
		})
	}
}

func TestClosedSourceCanTransferButClosedDestinationCannot(t *testing.T) {
	p := testPeer(t, testLimits())
	source, lease := testSession(t, p, Claim{Bytes: 20, PendingAccept: true})
	target, _ := testSession(t, p, Claim{})
	source.Close()
	if err := lease.Transfer(target); err != nil || !source.Snapshot().Retired || p.Snapshot().SessionSlots != 1 || target.Snapshot().Bytes != 20 {
		t.Fatalf("closed-source transfer failed: %v", err)
	}
	before := p.Snapshot()
	if err := lease.Transfer(source); !errors.Is(err, ErrClosed) || p.Snapshot() != before {
		t.Fatal("closed destination revived")
	}
	lease.Release()
	if err := lease.Transfer(target); !errors.Is(err, ErrReleased) {
		t.Fatal("released lease transferred")
	}
	if err := lease.MarkAccepted(); !errors.Is(err, ErrReleased) {
		t.Fatal("released lease accepted")
	}
}

func TestTransferChecksDestinationBusinessCount(t *testing.T) {
	l := testLimits()
	l.MaxStreamsPerSession = 1
	p := testPeer(t, l)
	source, moving := testSession(t, p, Claim{BusinessStream: true})
	target, held := testSession(t, p, Claim{BusinessStream: true})
	before, beforeSource, beforeTarget := p.Snapshot(), source.Snapshot(), target.Snapshot()
	if err := moving.Transfer(target); !errors.Is(err, ErrResourceLimit) || p.Snapshot() != before || source.Snapshot() != beforeSource || target.Snapshot() != beforeTarget {
		t.Fatal("destination stream ceiling did not fail atomically")
	}
	held.Release()
	if err := moving.Transfer(target); err != nil || target.Snapshot().BusinessStreams != 1 || source.Snapshot().BusinessStreams != 0 {
		t.Fatal("count-only ownership did not transfer after capacity returned")
	}
}

func TestAcceptedCountOnlyLeaseStillBoundsMetadata(t *testing.T) {
	l := testLimits()
	l.MaxReservations = 1
	p := testPeer(t, l)
	s, lease := testSession(t, p, Claim{PendingAccept: true})
	if err := lease.MarkAccepted(); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if u := p.Snapshot(); u.Reservations != 1 || u.PendingAccepts != 0 || u.SessionSlots != 1 {
		t.Fatal("accepted count-only lease lost its metadata reservation")
	}
	lease.Release()
	if p.Snapshot() != (PeerSnapshot{}) {
		t.Fatal("accepted count-only lease did not retire")
	}
}

func TestMaximumByteBoundary(t *testing.T) {
	l := testLimits()
	l.MaxPeerBytes, l.MaxSessionBytes = MaxRetainedBytes, MaxRetainedBytes
	p := testPeer(t, l)
	source, lease := testSession(t, p, Claim{Bytes: MaxRetainedBytes})
	target, _ := testSession(t, p, Claim{})
	before := p.Snapshot()
	for _, amount := range []uint64{1, MaxRetainedBytes, math.MaxUint64} {
		if lease, err := source.Reserve(Claim{Bytes: amount}); !errors.Is(err, ErrResourceLimit) || lease != nil || p.Snapshot() != before {
			t.Fatal("full byte ceiling wrapped or partially charged")
		}
	}
	if err := lease.Transfer(target); err != nil || p.Snapshot() != before {
		t.Fatal("full byte credit could not move without double charge")
	}
	lease.Release()
	if p.Snapshot().Bytes != 0 {
		t.Fatal("maximum byte credit did not release exactly")
	}
}

func TestPeerCloseRetiresEmptyScopesAndPreservesLiveClaims(t *testing.T) {
	p := testPeer(t, testLimits())
	empty, _ := testSession(t, p, Claim{})
	live, lease := testSession(t, p, Claim{Bytes: 35, BusinessStream: true})
	pending, _, err := p.BeginHandshake(Claim{})
	if err != nil {
		t.Fatal(err)
	}
	copyPeer := *p
	p.Close()
	copyPeer.Close()
	if !empty.Snapshot().Retired || !pending.Snapshot().Retired || live.Snapshot().Retired || !live.Snapshot().Closed {
		t.Fatal("Peer Close did not distinguish empty/live scopes")
	}
	u := p.Snapshot()
	if !u.Closed || u.SessionSlots != 1 || u.EstablishedSessions != 1 || u.Bytes != 35 || u.BusinessStreams != 1 {
		t.Fatalf("Peer Close erased live ownership: %+v", u)
	}
	if _, _, err := p.BeginHandshake(Claim{}); !errors.Is(err, ErrClosed) {
		t.Fatal("closed Peer admitted handshake")
	}
	if err := lease.Transfer(live); !errors.Is(err, ErrClosed) {
		t.Fatal("closed Peer permitted transfer admission")
	}
	lease.Release()
	if p.Snapshot() != (PeerSnapshot{Closed: true}) {
		t.Fatal("last live claim not released after Peer Close")
	}
	assertAccounting(t, p)
}

func TestControlFloorAndReservationMetadataBound(t *testing.T) {
	l := testLimits()
	l.MaxReservations = 2
	p := testPeer(t, l)
	s, control := testSession(t, p, Claim{Bytes: 40})
	business, err := s.Reserve(Claim{Bytes: 20, BusinessStream: true})
	if err != nil {
		t.Fatal(err)
	}
	defer business.Release()
	if _, err := s.Reserve(Claim{Bytes: 1, BusinessStream: true}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal("business admission consumed the held control floor")
	}
	if control.Snapshot().BusinessStream || p.Snapshot().BusinessStreams != 1 {
		t.Fatal("control lease consumed business count")
	}
	business.Release()
	countOnly, err := s.Reserve(Claim{BusinessStream: true})
	if err != nil {
		t.Fatal(err)
	}
	defer countOnly.Release()
	before := p.Snapshot()
	if _, err := s.Reserve(Claim{PendingAccept: true}); !errors.Is(err, ErrResourceLimit) || p.Snapshot() != before {
		t.Fatal("zero-byte reservations bypassed metadata bound")
	}
	if _, err := s.Reserve(Claim{}); !errors.Is(err, ErrInvalid) || p.Snapshot() != before {
		t.Fatal("empty reservation created metadata")
	}
	if err := control.MarkAccepted(); !errors.Is(err, ErrInvalid) {
		t.Fatal("unreserved accept released a slot")
	}
}

func TestConcurrentReserveTransferAcceptReleaseAndClose(t *testing.T) {
	for range 20 {
		p := testPeer(t, testLimits())
		a, _ := testSession(t, p, Claim{})
		b, _ := testSession(t, p, Claim{})
		var workers sync.WaitGroup
		start := make(chan struct{})
		firstClaim := make(chan struct{}, 1)
		for i := range 8 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				for range 30 {
					source, destination := a, b
					if i%2 != 0 {
						source, destination = b, a
					}
					lease, err := source.Reserve(Claim{Bytes: 7, BusinessStream: true, PendingAccept: true})
					if err != nil {
						if !errors.Is(err, ErrResourceLimit) && !errors.Is(err, ErrClosed) {
							t.Error(err)
						}
						continue
					}
					select {
					case firstClaim <- struct{}{}:
					default:
					}
					copyLease := *lease
					var operations sync.WaitGroup
					operations.Add(3)
					go func() { defer operations.Done(); _ = lease.Transfer(destination) }()
					go func() { defer operations.Done(); _ = lease.MarkAccepted() }()
					go func() { defer operations.Done(); copyLease.Release() }()
					operations.Wait()
					lease.Release()
					assertAccounting(t, p)
				}
			}()
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			<-firstClaim
			a.Close()
			p.Close()
		}()
		close(start)
		workers.Wait()
		if p.Snapshot() != (PeerSnapshot{Closed: true}) {
			t.Fatalf("concurrent lifecycle leaked charge: %+v", p.Snapshot())
		}
		assertAccounting(t, p)
	}
}

func TestConcurrentPromotionCloseAndRelease(t *testing.T) {
	p := testPeer(t, testLimits())
	for range 100 {
		s, lease, err := p.BeginHandshake(Claim{Bytes: 8, PendingAccept: true})
		if err != nil {
			t.Fatal(err)
		}
		var workers sync.WaitGroup
		start := make(chan struct{})
		workers.Add(3)
		go func() {
			defer workers.Done()
			<-start
			if err := s.Promote(); err != nil && !errors.Is(err, ErrClosed) {
				t.Error(err)
			}
		}()
		go func() { defer workers.Done(); <-start; s.Close() }()
		go func() { defer workers.Done(); <-start; lease.Release() }()
		close(start)
		workers.Wait()
		if p.Snapshot() != (PeerSnapshot{}) || !s.Snapshot().Retired {
			t.Fatal("promotion/close/release raced into a retained Session slot")
		}
		if err := s.Promote(); !errors.Is(err, ErrClosed) {
			t.Fatal("retired handshake revived")
		}
		assertAccounting(t, p)
	}
}

func TestInvalidLimitsAndZeroHandles(t *testing.T) {
	for _, mutate := range []func(*Limits){
		func(l *Limits) { l.MaxPeerBytes = 0 }, func(l *Limits) { l.MaxPeerBytes = math.MaxUint64 },
		func(l *Limits) { l.MaxSessionBytes = 0 }, func(l *Limits) { l.MaxSessionBytes = l.MaxPeerBytes + 1 },
		func(l *Limits) { l.MaxSessions = 0 }, func(l *Limits) { l.MaxSessions = 65537 },
		func(l *Limits) { l.MaxPendingHandshakes = 0 }, func(l *Limits) { l.MaxPendingHandshakes = 4097 },
		func(l *Limits) { l.MaxPendingAccepts = 0 }, func(l *Limits) { l.MaxPendingAccepts = 65537 },
		func(l *Limits) { l.MaxStreamsPerSession = 0 }, func(l *Limits) { l.MaxStreamsPerSession = 4097 },
		func(l *Limits) { l.MaxPeerStreams = 0 }, func(l *Limits) { l.MaxPeerStreams = 65537 },
		func(l *Limits) { l.MaxReservations = 0 }, func(l *Limits) { l.MaxReservations = MaxReservations + 1 },
	} {
		l := testLimits()
		mutate(&l)
		if p, err := New(l); !errors.Is(err, ErrInvalid) || p != nil {
			t.Fatalf("invalid limits accepted: %+v %v", l, err)
		}
	}
	for _, p := range []*Peer{nil, {}} {
		p.Close()
		if _, _, err := p.BeginHandshake(Claim{}); !errors.Is(err, ErrInvalid) || !p.Snapshot().Closed {
			t.Fatal("invalid Peer was usable")
		}
	}
	for _, s := range []*Session{nil, {}} {
		s.Close()
		if err := s.Promote(); !errors.Is(err, ErrInvalid) || !s.Snapshot().Closed {
			t.Fatal("invalid Session was usable")
		}
		if _, err := s.Reserve(Claim{Bytes: 1}); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid Session reserved")
		}
	}
	for _, lease := range []*Lease{nil, {}} {
		lease.Release()
		if err := lease.Transfer(nil); !errors.Is(err, ErrInvalid) || !lease.Snapshot().Released {
			t.Fatal("invalid lease was usable")
		}
		if err := lease.MarkAccepted(); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid lease accepted")
		}
	}
}
