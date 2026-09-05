package creditv2

import (
	"errors"
	"math"
	"sync"
	"testing"
)

func TestShrinkBytesReturnsOnlyDisposedByteCredit(t *testing.T) {
	p := testPeer(t, testLimits())
	s, lease := testSession(t, p, Claim{Bytes: 60, BusinessStream: true, PendingAccept: true})
	copyOfLease := *lease
	for _, size := range []uint64{40, 40, 0} {
		if err := copyOfLease.ShrinkBytes(size); err != nil {
			t.Fatal(err)
		}
		want := Usage{Bytes: size, Reservations: 1, BusinessStreams: 1, PendingAccepts: 1}
		if p.Snapshot().Usage != want || s.Snapshot().Usage != want || lease.Snapshot().Bytes != size {
			t.Fatal("shrink changed counts or disagreed across copied handles")
		}
		assertAccounting(t, p)
	}
	other, err := s.Reserve(Claim{Bytes: 60})
	if err != nil {
		t.Fatalf("returned byte capacity was unavailable: %v", err)
	}
	other.Release()
	s.Close()
	if s.Snapshot().Retired {
		t.Fatal("zero-byte lease lost its remaining obligations")
	}
	lease.Release()
	copyOfLease.Release()
	if p.Snapshot().Usage != (Usage{}) || !s.Snapshot().Retired {
		t.Fatal("final release did not reclaim each remaining obligation once")
	}
}

func TestShrinkBytesRejectsGrowthAndReleasedHandles(t *testing.T) {
	p := testPeer(t, testLimits())
	s, lease := testSession(t, p, Claim{Bytes: 20})
	for _, size := range []uint64{21, math.MaxUint64} {
		beforePeer, beforeSession, beforeLease := p.Snapshot(), s.Snapshot(), lease.Snapshot()
		if err := lease.ShrinkBytes(size); !errors.Is(err, ErrInvalid) {
			t.Fatalf("growth to %d returned %v", size, err)
		}
		if p.Snapshot() != beforePeer || s.Snapshot() != beforeSession || lease.Snapshot() != beforeLease {
			t.Fatal("rejected growth changed accounting")
		}
	}
	for _, invalid := range []*Lease{nil, {}} {
		if err := invalid.ShrinkBytes(0); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid handle returned %v", err)
		}
	}
	lease.Release()
	before := p.Snapshot()
	if err := lease.ShrinkBytes(0); !errors.Is(err, ErrReleased) || p.Snapshot() != before {
		t.Fatal("released lease accepted a reduction or changed usage")
	}
}

func TestShrinkBytesBoundAndTransferredOwnershipAfterClose(t *testing.T) {
	for _, bound := range []bool{false, true} {
		p := testPeer(t, testLimits())
		s, lease := testSession(t, p, Claim{Bytes: 60})
		target, _ := testSession(t, p, Claim{})
		if bound {
			var err error
			lease, err = s.BindBytes(lease, 60)
			if err != nil {
				t.Fatal(err)
			}
			target = s
		} else {
			s.Close()
			if err := lease.Transfer(target); err != nil {
				t.Fatal(err)
			}
		}
		p.Close()
		if err := lease.ShrinkBytes(10); err != nil {
			t.Fatalf("closed storage owner could not return bytes: %v", err)
		}
		if target.Snapshot().Bytes != 10 || p.Snapshot().Bytes != 10 {
			t.Fatal("reduction did not debit the current owner")
		}
		if err := lease.ShrinkBytes(0); err != nil || target.Snapshot().Retired || p.Snapshot().Reservations != 1 {
			t.Fatal("zero-byte reduction prematurely retired live lease metadata")
		}
		lease.Release()
		if !target.Snapshot().Retired || p.Snapshot().Usage != (Usage{}) {
			t.Fatal("closed storage was not fully reclaimed")
		}
		assertAccounting(t, p)
	}
}

func TestShrinkBytesRacesTransferReleaseAndClose(t *testing.T) {
	for range 64 {
		p := testPeer(t, testLimits())
		s, lease := testSession(t, p, Claim{Bytes: 60})
		target, _ := testSession(t, p, Claim{})
		var wg sync.WaitGroup
		wg.Add(4)
		go func(copyOfLease Lease) {
			defer wg.Done()
			for _, size := range []uint64{40, 10, 0} {
				if err := copyOfLease.ShrinkBytes(size); err != nil && !errors.Is(err, ErrReleased) {
					t.Errorf("concurrent shrink: %v", err)
				}
			}
		}(*lease)
		go func() {
			defer wg.Done()
			if err := lease.Transfer(target); err != nil && !errors.Is(err, ErrReleased) {
				t.Errorf("concurrent transfer: %v", err)
			}
		}()
		go func() { defer wg.Done(); lease.Release() }()
		go func() { defer wg.Done(); s.Close() }()
		wg.Wait()
		if p.Snapshot().Usage != (Usage{}) || !s.Snapshot().Retired {
			t.Fatal("concurrent ownership operations lost byte/count conservation")
		}
		assertAccounting(t, p)
	}
}
