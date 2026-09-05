package aggregationv2

import (
	"errors"
	"testing"
	"time"
	"unsafe"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/reassemblyv2"
)

func receiverLimits() reassemblyv2.Limits {
	return reassemblyv2.Limits{MaxDatagrams: 8, MaxDatagramBytes: 1024, MaxFragments: 16, Span: 64, Timeout: time.Second}
}

func TestPrepaidComponentsInstallAtFullHandshakeCapacity(t *testing.T) {
	l, rl := testLimits(), receiverLimits()
	ringBytes, err := RequiredInitialBytes(l)
	if err != nil {
		t.Fatal(err)
	}
	bitmapBytes, err := reassemblyv2.RequiredInitialBytes(rl)
	if err != nil {
		t.Fatal(err)
	}
	total := ringBytes + bitmapBytes
	p, err := creditv2.New(creditv2.Limits{MaxPeerBytes: total, MaxSessionBytes: total, MaxSessions: 1, MaxPendingHandshakes: 1, MaxPendingAccepts: 1, MaxStreamsPerSession: 1, MaxPeerStreams: 1, MaxReservations: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	s, ring, err := p.BeginHandshake(creditv2.Claim{Bytes: ringBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer ring.Release()
	bitmap, err := s.Reserve(creditv2.Claim{Bytes: bitmapBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer bitmap.Release()
	if err := s.Promote(); err != nil {
		t.Fatal(err)
	}
	beforePeer, beforeSession := p.Snapshot(), s.Snapshot()
	if beforePeer.Bytes != total || beforePeer.Reservations != 2 {
		t.Fatal("fixture did not exhaust initial capacity")
	}
	if q, err := New(s, l, testEpoch()); q != nil || !errors.Is(err, creditv2.ErrResourceLimit) {
		t.Fatal("ordinary queue construction unexpectedly fits beside prepaid storage")
	}
	if r, err := reassemblyv2.New(s, rl); r != nil || !errors.Is(err, creditv2.ErrResourceLimit) {
		t.Fatal("ordinary reassembly construction unexpectedly fits beside prepaid storage")
	}
	q, err := NewPrepaid(s, l, testEpoch(), ring)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	r, err := reassemblyv2.NewPrepaid(s, rl, bitmap)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if p.Snapshot() != beforePeer || s.Snapshot() != beforeSession || uint64(cap(q.ring))*uint64(unsafe.Sizeof(entry{})) != ringBytes {
		t.Fatal("installation changed prepaid accounting or allocated the wrong ring")
	}
	s.Close()
	if p.Snapshot().Bytes != total || s.Snapshot().Retired {
		t.Fatal("Session closure released component storage")
	}
	q.Close()
	if q.ring != nil || q.ringLease != nil || p.Snapshot().Bytes != bitmapBytes || !ring.Snapshot().Released {
		t.Fatal("queue disposal did not clear storage and release its dedicated lease")
	}
	r.Close()
	if !r.Snapshot().History.Closed || !bitmap.Snapshot().Released || p.Snapshot().Usage != (creditv2.Usage{}) || !s.Snapshot().Retired {
		t.Fatal("receiver disposal did not finish prepaid ownership")
	}
	ring.Release()
	bitmap.Release()
	if p.Snapshot().Usage != (creditv2.Usage{}) {
		t.Fatal("handshake disposer double-released prepaid handles")
	}
}

func TestPrepaidConstructorsRejectUnownedLeases(t *testing.T) {
	ringBytes, _ := RequiredInitialBytes(testLimits())
	bitmapBytes, _ := reassemblyv2.RequiredInitialBytes(receiverLimits())
	for _, component := range []struct {
		name  string
		bytes uint64
		new   func(*creditv2.Session, *creditv2.Lease) (func(), error)
	}{
		{"queue", ringBytes, func(s *creditv2.Session, lease *creditv2.Lease) (func(), error) {
			q, err := NewPrepaid(s, testLimits(), testEpoch(), lease)
			if err != nil {
				return nil, err
			}
			return q.Close, nil
		}},
		{"receiver", bitmapBytes, func(s *creditv2.Session, lease *creditv2.Lease) (func(), error) {
			r, err := reassemblyv2.NewPrepaid(s, receiverLimits(), lease)
			if err != nil {
				return nil, err
			}
			return r.Close, nil
		}},
	} {
		for _, name := range []string{"nil lease", "zero lease", "released", "insufficient", "other Session", "other Peer", "business stream", "pending accept", "pending Session", "closed Session", "duplicate copy"} {
			t.Run(component.name+"/"+name, func(t *testing.T) {
				p, s := testCredits(t)
				claim := creditv2.Claim{Bytes: component.bytes}
				switch name {
				case "insufficient":
					claim.Bytes--
				case "business stream":
					claim.BusinessStream = true
				case "pending accept":
					claim.PendingAccept = true
				case "pending Session":
					s.Close()
					var err error
					s, _, err = p.BeginHandshake(creditv2.Claim{})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(s.Close)
				}
				lease, err := s.Reserve(claim)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(lease.Release)
				target, supplied := s, lease
				switch name {
				case "nil lease":
					supplied = nil
				case "zero lease":
					supplied = &creditv2.Lease{}
				case "released":
					lease.Release()
				case "other Session":
					target, _, err = p.BeginSession(creditv2.Claim{})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(target.Close)
				case "other Peer":
					_, target = testCredits(t)
				case "closed Session":
					s.Close()
				case "duplicate copy":
					closeComponent, err := component.new(s, lease)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(closeComponent)
					copyOfLease := *lease
					supplied = &copyOfLease
				}
				beforePeer, beforeSession, beforeLease := p.Snapshot(), s.Snapshot(), lease.Snapshot()
				if dispose, err := component.new(target, supplied); err == nil || dispose != nil {
					t.Fatal("constructor accepted an unavailable lease")
				}
				if p.Snapshot() != beforePeer || s.Snapshot() != beforeSession || lease.Snapshot() != beforeLease {
					t.Fatal("rejected constructor changed ownership")
				}
				if !beforeSession.Closed && !beforeLease.Released && !claim.BusinessStream && !claim.PendingAccept && claim.Bytes == component.bytes && name != "duplicate copy" {
					if err := s.Promote(); err != nil {
						t.Fatal(err)
					}
					dispose, err := component.new(s, lease)
					if err != nil {
						t.Fatalf("failed construction consumed caller lease: %v", err)
					}
					dispose()
				}
			})
		}
	}
}

func TestRequiredInitialBytesIsAllocationFree(t *testing.T) {
	for _, count := range []int{1, 8, 65536} {
		l := testLimits()
		l.MaxQueuedDatagrams = count
		got, err := RequiredInitialBytes(l)
		if err != nil || got != uint64(count)*uint64(unsafe.Sizeof(entry{})) {
			t.Fatalf("ring size for %d: %d, %v", count, got, err)
		}
	}
	for _, span := range []uint32{1, 63, 64, 65, 65536} {
		l := receiverLimits()
		l.Span, l.MaxDatagrams = span, 1
		got, err := reassemblyv2.RequiredInitialBytes(l)
		if err != nil || got != uint64((span+63)/64)*16 {
			t.Fatalf("bitmap size for %d: %d, %v", span, got, err)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() {
		_, _ = RequiredInitialBytes(testLimits())
		_, _ = reassemblyv2.RequiredInitialBytes(receiverLimits())
	}); allocs != 0 {
		t.Fatalf("pre-handshake sizing allocated %v times", allocs)
	}
}
