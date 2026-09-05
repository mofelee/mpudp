package reassemblyv2

import (
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/fecv2"
	"github.com/mofelee/mpudp/internal/recvwindow"
)

func scanDeadline(r *Receiver) time.Time {
	var next time.Time
	for _, a := range r.pending {
		deadline := a.admitted.Add(r.limits.Timeout)
		if next.IsZero() || deadline.Before(next) {
			next = deadline
		}
	}
	return next
}

func checkDeadline(t testing.TB, r *Receiver) {
	t.Helper()
	if got, want := r.NextDeadline(), scanDeadline(r); !got.Equal(want) {
		t.Fatalf("NextDeadline = %s, scan = %s", got, want)
	}
	count, previous := 0, uint64(0)
	var admitted time.Time
	for id := r.head; id != 0; {
		a := r.pending[id]
		if a == nil || a.previous != previous || count == len(r.pending) {
			t.Fatalf("broken deadline links at %d", id)
		}
		if count > 0 && a.admitted.Before(admitted) {
			t.Fatal("deadline links reordered admissions")
		}
		admitted = a.admitted
		previous, id = id, a.next
		count++
	}
	if count != len(r.pending) || previous != r.tail {
		t.Fatal("deadline links disagree with pending originals")
	}
}

func TestDeadlineCompletionExpiryAndClose(t *testing.T) {
	r, _, _ := fixture(t, nil)
	checkDeadline(t, r)
	for _, admission := range []struct {
		id uint64
		at time.Duration
	}{{9, 0}, {3, 100 * time.Millisecond}, {7, 200 * time.Millisecond}, {5, 200 * time.Millisecond}} {
		add(t, r, start.Add(admission.at), fragment(admission.id, 2, 0, "a"))
		checkDeadline(t, r)
	}
	for i, id := range []uint64{7, 9, 5} {
		done := add(t, r, start.Add(time.Duration(300+i*100)*time.Millisecond), fragment(id, 2, 1, "b"))
		if len(done) != 1 || string(done[0].Payload()) != "ab" {
			t.Fatalf("completion of %d lost payload", id)
		}
		done[0].Release()
		checkDeadline(t, r)
	}
	add(t, r, start.Add(600*time.Millisecond), fragment(3, 2, 0, "a"))
	for _, d := range add(t, r, start.Add(700*time.Millisecond), fragment(10, 1, 0, "x"), fragment(11, 0, 0, "")) {
		d.Release()
	}
	deadline := start.Add(1100 * time.Millisecond)
	if got := r.NextDeadline(); !got.Equal(deadline) {
		t.Fatalf("duplicate or immediate completion moved deadline: %s", got)
	}
	if expired, err := r.Expire(deadline.Add(-time.Nanosecond)); err != nil || len(expired) != 0 {
		t.Fatalf("early expiry: %v, %v", expired, err)
	}
	if expired, err := r.Expire(deadline); err != nil || !reflect.DeepEqual(expired, []uint64{3}) {
		t.Fatalf("exact expiry: %v, %v", expired, err)
	}
	checkDeadline(t, r)
	add(t, r, deadline.Add(time.Millisecond), fragment(12, 2, 0, "a"))
	pending := r.pending[12]
	r.Close()
	r.Close()
	if !r.NextDeadline().IsZero() || len(r.pending) != 0 || r.head != 0 || r.tail != 0 || pending.data != nil || pending.ranges != nil || pending.lease != nil || pending.previous != 0 || pending.next != 0 {
		t.Fatal("Close retained pending expiry ownership")
	}
}

func TestDeadlineLinkStorageCharged(t *testing.T) {
	for _, size := range []int{0, 2} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			r, p, _ := fixture(t, nil)
			before := p.Snapshot().Bytes
			payload := ""
			if size != 0 {
				payload = "a"
			}
			done := add(t, r, start, fragment(1, uint32(size), 0, payload))
			if got, want := p.Snapshot().Bytes-before, uint64(size+r.limits.MaxFragments*8+16); got != want {
				t.Fatalf("original charge = %d, want %d including deadline links", got, want)
			}
			if size != 0 {
				done = add(t, r, start, fragment(1, uint32(size), 1, "b"))
			}
			checkDeadline(t, r)
			for _, d := range done {
				d.Release()
			}
			if p.Snapshot().Bytes != before {
				t.Fatal("completed original retained deadline link credit")
			}
		})
	}
}

func TestDeadlineMatchesPendingScan(t *testing.T) {
	r, _, _ := fixture(t, func(l *Limits) { l.MaxDatagrams = 32; l.Span = recvwindow.MaxSpan })
	random := rand.New(rand.NewSource(51))
	now, next := start, 0
	for step := 0; step < 2000; step++ {
		now = now.Add(time.Duration(random.Intn(30)) * time.Millisecond)
		ids := make([]uint64, 0, len(r.pending))
		for id := range r.pending {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		switch choice := random.Intn(5); {
		case (choice < 2 || len(ids) == 0) && len(ids) < r.limits.MaxDatagrams:
			id := uint64((next*313)%32768 + 1)
			next++
			add(t, r, now, fragment(id, 2, 0, "a"))
		case choice < 4 && len(ids) > 0:
			id := ids[random.Intn(len(ids))]
			f := fragment(id, 2, 0, "a")
			if choice == 3 {
				f = fragment(id, 2, 1, "b")
			}
			for _, d := range add(t, r, now, f) {
				d.Release()
			}
		default:
			var want []uint64
			for _, id := range ids {
				if r.due(r.pending[id], now) {
					want = append(want, id)
				}
			}
			got, err := r.Expire(now)
			slices.Sort(got)
			if err != nil || !slices.Equal(got, want) {
				t.Fatalf("step %d expiry = %v, %v; scan = %v", step, got, err, want)
			}
			for _, id := range got {
				if r.State(id) != recvwindow.Expired {
					t.Fatalf("expiry lost terminal status of %d", id)
				}
			}
		}
		checkDeadline(t, r)
	}
}

func benchmarkDeadlineReceiver(b *testing.B, count int) *Receiver {
	b.Helper()
	p, err := creditv2.New(creditv2.Limits{MaxPeerBytes: 64 << 20, MaxSessionBytes: 64 << 20,
		MaxSessions: 1, MaxPendingHandshakes: 1, MaxPendingAccepts: 1,
		MaxStreamsPerSession: 1, MaxPeerStreams: 1, MaxReservations: count + 8})
	if err != nil {
		b.Fatal(err)
	}
	s, _, err := p.BeginSession(creditv2.Claim{})
	if err != nil {
		b.Fatal(err)
	}
	r, err := New(s, Limits{MaxDatagrams: max(1, count), MaxDatagramBytes: 2, MaxFragments: 2,
		Span: recvwindow.MaxSpan, Timeout: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		r.Close()
		s.Close()
		if usage := p.Snapshot(); usage.Bytes != 0 || usage.Reservations != 0 {
			b.Fatalf("deadline fixture leaked: %+v", usage)
		}
	})
	for i := 0; i < count; i++ {
		_, err := r.AddGroup(start.Add(time.Duration(i)*time.Microsecond), []fecv2.Fragment{fragment(uint64(i+1), 2, 0, "a")})
		if err != nil {
			b.Fatal(err)
		}
	}
	checkDeadline(b, r)
	return r
}

var benchmarkDeadline time.Time

func BenchmarkReceiverNextDeadline(b *testing.B) {
	for _, count := range []int{0, 128, 512, 1024, 8192} {
		b.Run(fmt.Sprintf("pending-%d", count), func(b *testing.B) {
			r := benchmarkDeadlineReceiver(b, count)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkDeadline = r.NextDeadline()
			}
		})
	}
}

func BenchmarkReceiverExpireNotDue(b *testing.B) {
	for _, count := range []int{0, 128, 512, 1024, 8192} {
		b.Run(fmt.Sprintf("pending-%d", count), func(b *testing.B) {
			r := benchmarkDeadlineReceiver(b, count)
			now := start.Add(100 * time.Millisecond)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if ids, err := r.Expire(now); err != nil || len(ids) != 0 {
					b.Fatal("unexpected expiry", ids, err)
				}
			}
		})
	}
}
