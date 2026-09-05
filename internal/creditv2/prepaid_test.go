package creditv2

import (
	"errors"
	"math"
	"sync"
	"testing"
)

func TestBindBytesRejectsWithoutTakingOwnership(t *testing.T) {
	for _, name := range []string{"nil Session", "zero Session", "nil lease", "zero lease", "released", "zero required", "overflow required", "insufficient", "other Session", "other Peer", "business stream", "pending accept", "count only", "pending Session", "closed Session", "closed Peer"} {
		t.Run(name, func(t *testing.T) {
			p := testPeer(t, testLimits())
			claim := Claim{Bytes: 20}
			switch name {
			case "business stream":
				claim.BusinessStream = true
			case "pending accept":
				claim.PendingAccept = true
			case "count only":
				claim = Claim{PendingAccept: true}
			}
			s, lease := testSession(t, p, claim)
			target, supplied, required, want := s, lease, uint64(20), ErrInvalid
			switch name {
			case "nil Session":
				target = nil
			case "zero Session":
				target = &Session{}
			case "nil lease":
				supplied = nil
			case "zero lease":
				supplied = &Lease{}
			case "released":
				lease.Release()
				want = ErrReleased
			case "zero required":
				required = 0
			case "overflow required":
				required = math.MaxUint64
			case "insufficient":
				required = 21
			case "other Session":
				target, _ = testSession(t, p, Claim{})
			case "other Peer":
				target, _ = testSession(t, testPeer(t, testLimits()), Claim{})
			case "pending Session":
				s.Close()
				lease.Release()
				var err error
				s, lease, err = p.BeginHandshake(claim)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(s.Close)
				t.Cleanup(lease.Release)
				target, supplied = s, lease
			case "closed Session":
				s.Close()
				want = ErrClosed
			case "closed Peer":
				p.Close()
				want = ErrClosed
			}
			beforePeer, beforeSession, beforeLease := p.Snapshot(), s.Snapshot(), lease.Snapshot()
			if bound, err := target.BindBytes(supplied, required); !errors.Is(err, want) || bound != nil {
				t.Fatalf("BindBytes = %v, %v; want nil, %v", bound, err, want)
			}
			if p.Snapshot() != beforePeer || s.Snapshot() != beforeSession || lease.Snapshot() != beforeLease || lease.state.bound {
				t.Fatal("failed binding changed accounting or caller ownership")
			}
			if !beforeSession.Closed && !beforeLease.Released && !claim.BusinessStream && !claim.PendingAccept {
				if err := s.Promote(); err != nil {
					t.Fatal(err)
				}
				bound, err := s.BindBytes(lease, 1)
				if err != nil {
					t.Fatalf("rejected caller lease could not be reused: %v", err)
				}
				bound.Release()
			}
			lease.Release()
			assertAccounting(t, p)
		})
	}
}

func TestBindBytesSharesReleaseButCannotBindOrMoveAgain(t *testing.T) {
	p := testPeer(t, testLimits())
	s, original := testSession(t, p, Claim{Bytes: 20})
	target, _ := testSession(t, p, Claim{})
	copyOfLease, copyOfSession := *original, *s
	before := p.Snapshot()
	bound, err := copyOfSession.BindBytes(&copyOfLease, 10)
	if err != nil || bound == original || bound == &copyOfLease || p.Snapshot() != before {
		t.Fatalf("binding reserved again or failed: %v", err)
	}
	for _, handle := range []*Lease{original, &copyOfLease, bound} {
		if second, err := s.BindBytes(handle, 10); !errors.Is(err, ErrInvalid) || second != nil {
			t.Fatal("copied handle reused backing credit")
		}
		if err := handle.Transfer(target); !errors.Is(err, ErrInvalid) {
			t.Fatal("bound backing moved to another Session")
		}
		if err := handle.Transfer(&copyOfSession); err != nil {
			t.Fatal("same-owner transfer stopped being idempotent")
		}
	}
	if p.Snapshot() != before || s.Snapshot().Bytes != 20 || target.Snapshot().Bytes != 0 {
		t.Fatal("bound handle misuse changed accounting")
	}
	s.Close()
	if s.Snapshot().Retired {
		t.Fatal("Close released bound storage")
	}
	bound.Release()
	original.Release()
	copyOfLease.Release()
	if p.Snapshot().Usage != (Usage{}) || !s.Snapshot().Retired {
		t.Fatal("shared Release did not reclaim exactly once")
	}
	assertAccounting(t, p)
}

func TestConcurrentBindBytesAndTransfer(t *testing.T) {
	t.Run("copied bindings", func(t *testing.T) {
		p := testPeer(t, testLimits())
		s, lease := testSession(t, p, Claim{Bytes: 20})
		before := p.Snapshot()
		var wg sync.WaitGroup
		results := make(chan error, 32)
		for range cap(results) {
			wg.Add(1)
			go func(copyOfLease Lease) {
				defer wg.Done()
				_, err := s.BindBytes(&copyOfLease, 20)
				results <- err
			}(*lease)
		}
		wg.Wait()
		close(results)
		successes := 0
		for err := range results {
			if err == nil {
				successes++
			} else if !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
		}
		if successes != 1 || p.Snapshot() != before {
			t.Fatalf("binding successes = %d; accounting = %+v", successes, p.Snapshot())
		}
	})
	t.Run("binding versus transfer", func(t *testing.T) {
		for range 64 {
			p := testPeer(t, testLimits())
			s, lease := testSession(t, p, Claim{Bytes: 20})
			target, _ := testSession(t, p, Claim{})
			var bindErr, transferErr error
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); _, bindErr = s.BindBytes(lease, 20) }()
			go func() { defer wg.Done(); transferErr = lease.Transfer(target) }()
			wg.Wait()
			if bindErr == nil {
				if !errors.Is(transferErr, ErrInvalid) || s.Snapshot().Bytes != 20 || target.Snapshot().Bytes != 0 {
					t.Fatal("successful binding did not freeze its owner")
				}
			} else if !errors.Is(bindErr, ErrInvalid) || transferErr != nil || target.Snapshot().Bytes != 20 || s.Snapshot().Bytes != 0 {
				t.Fatal("successful transfer did not invalidate old-owner binding")
			}
			lease.Release()
			assertAccounting(t, p)
		}
	})
}
