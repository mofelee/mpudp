package aggregationv2

import (
	"bytes"
	"errors"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/fecv2"
)

func testLimits() Limits {
	return Limits{MaxQueuedDatagrams: 8, MaxQueuedBytes: 4096, MaxDatagramBytes: 1024, MaxFragmentsPerDatagram: 16, MaxDelay: 250 * time.Microsecond}
}

func testEpoch() Epoch {
	return Epoch{ID: 7, Parameters: fecv2.Parameters{DataShards: 3, ParityShards: 2, ShardBytes: 32, MaxDescriptors: 4, MaxLogicalBytes: 96, MaxDatagramBytes: 1024}}
}

func testCredits(t testing.TB) (*creditv2.Peer, *creditv2.Session) {
	t.Helper()
	p, err := creditv2.New(creditv2.Limits{MaxPeerBytes: 1 << 20, MaxSessionBytes: 1 << 20, MaxSessions: 2, MaxPendingHandshakes: 2, MaxPendingAccepts: 2, MaxStreamsPerSession: 2, MaxPeerStreams: 2, MaxReservations: 128})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	s, _, err := p.BeginSession(creditv2.Claim{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return p, s
}

func testQueue(t testing.TB, limits Limits, epoch Epoch) (*Queue, *creditv2.Peer, *creditv2.Session) {
	t.Helper()
	p, s := testCredits(t)
	q, err := New(s, limits, epoch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Close)
	return q, p, s
}

func admit(t testing.TB, q *Queue, data []byte, now time.Time) uint64 {
	t.Helper()
	id, err := q.Admit(data, now)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func seal(t testing.TB, q *Queue, now time.Time, force bool) *Output {
	t.Helper()
	out, err := q.Seal(now, force)
	if err != nil || out == nil {
		t.Fatalf("seal: output=%v error=%v", out, err)
	}
	t.Cleanup(out.Release)
	return out
}

func decode(t testing.TB, epoch Epoch, out *Output) []fecv2.Fragment {
	t.Helper()
	view, ok := out.View()
	if !ok || view.EncodingEpoch != epoch.ID {
		t.Fatal("output not live in frozen epoch")
	}
	codec, err := fecv2.New(epoch.Parameters)
	if err != nil {
		t.Fatal(err)
	}
	shards := append([][]byte(nil), view.Group.Shards...)
	shards[0], shards[len(shards)-1] = nil, nil
	fragments, err := codec.Decode(view.Group.LogicalBytes, shards)
	if err != nil {
		t.Fatal(err)
	}
	return fragments
}

func TestAdmissionCopiesAndEmptyDatagramsConsumeBoundedSlots(t *testing.T) {
	l := testLimits()
	l.MaxQueuedDatagrams, l.MaxQueuedBytes = 2, 3
	q, p, _ := testQueue(t, l, testEpoch())
	ringBytes := uint64(l.MaxQueuedDatagrams) * uint64(unsafe.Sizeof(entry{}))
	if p.Snapshot().Bytes != ringBytes || p.Snapshot().Reservations != 1 {
		t.Fatal("ring metadata was not reserved")
	}
	now := time.Unix(1, 0)
	input := []byte("abc")
	first := admit(t, q, input, now)
	clear(input)
	empty := admit(t, q, nil, now)
	before, creditBefore := q.Snapshot(), p.Snapshot()
	if id, err := q.Admit(nil, now); !errors.Is(err, ErrResourceLimit) || id != 0 || q.Snapshot() != before || p.Snapshot() != creditBefore {
		t.Fatal("empty Datagram bypassed bounded whole-admission count")
	}
	out := seal(t, q, now, true)
	got := decode(t, testEpoch(), out)
	if len(got) != 2 || got[0].DatagramID != first || string(got[0].Payload) != "abc" || got[1].DatagramID != empty || got[1].Payload == nil || len(got[1].Payload) != 0 {
		t.Fatal("caller mutation or empty identity changed admitted originals")
	}
	if q.Snapshot().QueuedDatagrams != 0 || p.Snapshot().Bytes != ringBytes+q.outputBytes {
		t.Fatal("encoded output and original ownership did not transition")
	}
}

func TestOldestDeadlineDoesNotExtendAcrossArrivalsOrPartialSeal(t *testing.T) {
	q, _, _ := testQueue(t, testLimits(), testEpoch())
	start := time.Unix(1, 0)
	id := admit(t, q, bytes.Repeat([]byte{1}, 100), start)
	first := seal(t, q, start, false)
	got := decode(t, testEpoch(), first)
	if len(got) != 1 || len(got[0].Payload) != 72 || got[0].DatagramID != id {
		t.Fatal("first fragment did not fill context")
	}
	first.Release()
	admit(t, q, []byte("later"), start.Add(100*time.Microsecond))
	deadline := start.Add(250 * time.Microsecond)
	if snapshot := q.Snapshot(); snapshot.OldestDeadline != deadline || snapshot.RetainedBytes != 105 {
		t.Fatalf("partial seal reset time or reclaimed a retained prefix: %+v", snapshot)
	}
	if ready, err := q.Ready(deadline.Add(-time.Nanosecond)); err != nil || ready {
		t.Fatal("sparse tail sealed before its fixed deadline")
	}
	if out, err := q.Seal(deadline.Add(-time.Nanosecond), false); err != nil || out != nil {
		t.Fatal("sparse tail ignored max delay")
	}
	last := seal(t, q, deadline, false)
	got = decode(t, testEpoch(), last)
	if len(got) != 2 || got[0].Offset != 72 || len(got[0].Payload) != 28 || string(got[1].Payload) != "later" {
		t.Fatal("tail continuation changed original range")
	}
}

func TestEmptyOnlyContextAndDescriptorReadiness(t *testing.T) {
	e := testEpoch()
	e.Parameters.MaxLogicalBytes = 24
	q, p, _ := testQueue(t, testLimits(), e)
	now := time.Unix(1, 0)
	before, creditBefore := q.Snapshot(), p.Snapshot()
	if id, err := q.Admit([]byte{1}, now); !errors.Is(err, ErrMessageTooLarge) || id != 0 || q.Snapshot() != before || p.Snapshot() != creditBefore {
		t.Fatal("24-byte context admitted an original that cannot progress")
	}
	admit(t, q, nil, now)
	out := seal(t, q, now, false)
	if got := decode(t, e, out); len(got) != 1 || len(got[0].Payload) != 0 {
		t.Fatal("empty-only group failed")
	}
	e = testEpoch()
	e.Parameters.MaxDescriptors = 2
	q2, _, _ := testQueue(t, testLimits(), e)
	admit(t, q2, nil, now)
	if ready, _ := q2.Ready(now); ready {
		t.Fatal("one empty descriptor prematurely filled two-descriptor context")
	}
	admit(t, q2, nil, now)
	if ready, err := q2.Ready(now); err != nil || !ready {
		t.Fatal("descriptor bound did not trigger readiness")
	}
	seal(t, q2, now, false)
}

func TestFragmentLimitStartsFreshGroupAtExactBoundary(t *testing.T) {
	for _, size := range []int{104, 105, 144} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			l := testLimits()
			l.MaxFragmentsPerDatagram = 2
			q, _, _ := testQueue(t, l, testEpoch())
			now := time.Unix(1, 0)
			admit(t, q, bytes.Repeat([]byte{1}, 20), now)
			id := admit(t, q, bytes.Repeat([]byte{2}, size), now)
			first := seal(t, q, now, false)
			parts := decode(t, testEpoch(), first)
			if size == 104 && (len(parts) != 2 || len(parts[1].Payload) != 32) {
				t.Fatal("exact two-fragment boundary did not use current tail")
			}
			if size > 104 && len(parts) != 1 {
				t.Fatal("short current tail exceeded the original fragment reservation")
			}
			var original []byte
			fragments := 0
			collect := func(parts []fecv2.Fragment) {
				for _, part := range parts {
					if part.DatagramID != id {
						continue
					}
					if int(part.Offset) != len(original) {
						t.Fatal("fresh-group planning changed fragment offsets")
					}
					original = append(original, part.Payload...)
					fragments++
				}
			}
			collect(parts)
			first.Release()
			for q.Snapshot().QueuedDatagrams > 0 {
				out := seal(t, q, now, true)
				collect(decode(t, testEpoch(), out))
				out.Release()
			}
			if fragments != 2 || !bytes.Equal(original, bytes.Repeat([]byte{2}, size)) {
				t.Fatalf("fragment bound or original changed: count=%d size=%d", fragments, len(original))
			}
		})
	}
}

func TestResourceFailuresLeaveIDsCursorsAndOwnershipUnchanged(t *testing.T) {
	q, p, s := testQueue(t, testLimits(), testEpoch())
	now := time.Unix(1, 0)
	admit(t, q, bytes.Repeat([]byte{3}, 100), now)
	held, err := s.Reserve(creditv2.Claim{Bytes: (1 << 20) - p.Snapshot().Bytes - q.outputBytes + 1})
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	before, creditBefore := q.Snapshot(), p.Snapshot()
	if out, err := q.Seal(now, true); !errors.Is(err, ErrResourceLimit) || out != nil || q.Snapshot() != before || p.Snapshot() != creditBefore || q.ring[q.head].offset != 0 || q.ring[q.head].fragments != 0 {
		t.Fatal("output reservation failure consumed queue state")
	}
	held.Release()
	first := seal(t, q, now, true)
	first.Release()
	held, err = s.Reserve(creditv2.Claim{Bytes: (1 << 20) - p.Snapshot().Bytes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(held.Release)
	before, creditBefore = q.Snapshot(), p.Snapshot()
	if id, err := q.Admit([]byte("whole"), now); !errors.Is(err, ErrResourceLimit) || id != 0 || q.Snapshot() != before || p.Snapshot() != creditBefore {
		t.Fatal("payload admission failure consumed an ID or partial credit")
	}
	held.Release()
	last := seal(t, q, now, true)
	if got := decode(t, testEpoch(), last); len(got) != 1 || got[0].Offset != 72 {
		t.Fatal("failed admission/encoding changed existing cursor")
	}
}

func TestCancellationCompactsRingAndClosePreservesOutput(t *testing.T) {
	l := testLimits()
	l.MaxQueuedDatagrams = 3
	q, p, s := testQueue(t, l, testEpoch())
	now := time.Unix(1, 0)
	admit(t, q, nil, now)
	seal(t, q, now, true).Release()
	for range 100 {
		ids := []uint64{admit(t, q, nil, now), admit(t, q, []byte{1}, now), admit(t, q, nil, now)}
		if !q.Cancel(ids[1]) || q.Cancel(ids[1]) {
			t.Fatal("middle cancellation not terminal")
		}
		admit(t, q, nil, now)
		out := seal(t, q, now, true)
		got := decode(t, testEpoch(), out)
		if len(got) != 3 || got[0].DatagramID != ids[0] || got[1].DatagramID != ids[2] || got[2].DatagramID <= ids[2] {
			t.Fatal("cancelled ring holes reordered or consumed queue capacity")
		}
		out.Release()
	}
	id := admit(t, q, bytes.Repeat([]byte{4}, 100), now)
	out := seal(t, q, now, false)
	copyOutput := *out
	if !q.Cancel(id) || q.Snapshot().RetainedBytes != 0 {
		t.Fatal("partial original remainder was not cancelled")
	}
	q.Close()
	s.Close()
	if u := p.Snapshot(); u.Bytes != q.outputBytes || u.Reservations != 1 || u.SessionSlots != 1 {
		t.Fatalf("queue Close revoked output or retained ring: %+v", u)
	}
	if got := decode(t, testEpoch(), out); len(got) != 1 || len(got[0].Payload) != 72 {
		t.Fatal("queue Close cleared a returned output")
	}
	out.Release()
	copyOutput.Release()
	if _, ok := out.View(); ok || p.Snapshot().Bytes != 0 || p.Snapshot().SessionSlots != 0 {
		t.Fatal("copied output double-released or retained ownership")
	}
}

func TestClockAndIDExhaustionAreFailureAtomic(t *testing.T) {
	q, p, _ := testQueue(t, testLimits(), testEpoch())
	now := time.Unix(1, 0)
	q.nextDatagramID = math.MaxUint64
	if id := admit(t, q, []byte{1}, now); id != math.MaxUint64 {
		t.Fatal("last valid DatagramID rejected")
	}
	before, creditBefore := q.Snapshot(), p.Snapshot()
	if id, err := q.Admit(nil, now); !errors.Is(err, ErrIDExhausted) || id != 0 || q.Snapshot() != before || p.Snapshot() != creditBefore {
		t.Fatal("DatagramID exhaustion wrapped or mutated")
	}
	for _, operation := range []func() error{
		func() error { _, err := q.Admit(nil, now.Add(-time.Nanosecond)); return err },
		func() error { _, err := q.Ready(now.Add(-time.Nanosecond)); return err },
		func() error { _, err := q.Seal(now.Add(-time.Nanosecond), true); return err },
	} {
		if err := operation(); !errors.Is(err, ErrClockRegression) || q.Snapshot() != before || p.Snapshot() != creditBefore {
			t.Fatal("clock regression mutated queue")
		}
	}
	seal(t, q, now, true)
	q2, p2, _ := testQueue(t, testLimits(), testEpoch())
	admit(t, q2, bytes.Repeat([]byte{2}, 100), now)
	q2.nextGroupID = math.MaxUint64
	last := seal(t, q2, now, false)
	if view, _ := last.View(); view.GroupID != math.MaxUint64 {
		t.Fatal("last valid GroupID rejected")
	}
	before, creditBefore = q2.Snapshot(), p2.Snapshot()
	if out, err := q2.Seal(now, true); !errors.Is(err, ErrIDExhausted) || out != nil || q2.Snapshot() != before || p2.Snapshot() != creditBefore {
		t.Fatal("GroupID exhaustion consumed a queued remainder")
	}
	if id, err := q2.Admit(nil, now); !errors.Is(err, ErrIDExhausted) || id != 0 || q2.Snapshot() != before {
		t.Fatal("queue admitted after terminal GroupID exhaustion")
	}
}

func TestConcurrentAdmissionAndCloseHaveBoundedOwnership(t *testing.T) {
	q, p, _ := testQueue(t, testLimits(), testEpoch())
	now := time.Unix(1, 0)
	var workers sync.WaitGroup
	start := make(chan struct{})
	firstAdmit := make(chan struct{}, 1)
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 20 {
				_, err := q.Admit([]byte{1}, now)
				if err != nil && !errors.Is(err, ErrResourceLimit) && !errors.Is(err, ErrClosed) {
					t.Error(err)
				}
				if err == nil {
					select {
					case firstAdmit <- struct{}{}:
					default:
					}
				}
			}
		}()
	}
	workers.Add(1)
	go func() { defer workers.Done(); <-start; <-firstAdmit; q.Close() }()
	close(start)
	workers.Wait()
	if q.Snapshot().QueuedDatagrams != 0 || !q.Snapshot().Closed || p.Snapshot().Bytes != 0 || p.Snapshot().Reservations != 0 {
		t.Fatal("Close raced into retained admitted ownership")
	}
}

func TestLowRateEmptyAndCancellationKeepOriginalTimes(t *testing.T) {
	q, _, _ := testQueue(t, testLimits(), testEpoch())
	now := time.Unix(1, 0)
	first := admit(t, q, nil, now)
	admit(t, q, nil, now.Add(100*time.Microsecond))
	if !q.Cancel(first) || q.Cancel(0) || q.Cancel(math.MaxUint64) {
		t.Fatal("cancellation did not identify the queued original")
	}
	deadline := now.Add(350 * time.Microsecond)
	if q.Snapshot().OldestDeadline != deadline {
		t.Fatal("cancellation reset the next original's admission time")
	}
	if out, err := q.Seal(deadline.Add(-time.Nanosecond), false); err != nil || out != nil {
		t.Fatal("low-rate empty admission did not wait for its deadline")
	}
	out := seal(t, q, deadline, false)
	if got := decode(t, testEpoch(), out); len(got) != 1 || got[0].DatagramID != first+1 || len(got[0].Payload) != 0 {
		t.Fatal("low-rate empty admission lost its distinct descriptor")
	}
}

func TestInvalidAdmissionAndConstructionHaveNoCharge(t *testing.T) {
	p, s := testCredits(t)
	for _, mutate := range []func(*Limits, *Epoch){
		func(l *Limits, _ *Epoch) { l.MaxQueuedDatagrams = 0 },
		func(l *Limits, _ *Epoch) { l.MaxQueuedDatagrams = 65537 },
		func(l *Limits, _ *Epoch) { l.MaxQueuedBytes = 0 },
		func(l *Limits, _ *Epoch) { l.MaxQueuedBytes = math.MaxUint64 },
		func(l *Limits, _ *Epoch) { l.MaxDatagramBytes = 0 },
		func(l *Limits, _ *Epoch) { l.MaxDatagramBytes = math.MaxUint32 },
		func(l *Limits, _ *Epoch) { l.MaxFragmentsPerDatagram = 0 },
		func(l *Limits, _ *Epoch) { l.MaxFragmentsPerDatagram = 4097 },
		func(l *Limits, _ *Epoch) { l.MaxDelay = 0 },
		func(l *Limits, _ *Epoch) { l.MaxDelay = 11 * time.Millisecond },
		func(_ *Limits, e *Epoch) { e.ID = 0 },
		func(_ *Limits, e *Epoch) { e.Parameters.MaxLogicalBytes = 23 },
		func(_ *Limits, e *Epoch) { e.Parameters.MaxDatagramBytes = 1 },
	} {
		l, e := testLimits(), testEpoch()
		mutate(&l, &e)
		before := p.Snapshot()
		if q, err := New(s, l, e); err == nil || q != nil || p.Snapshot() != before {
			t.Fatal("invalid construction retained charge")
		}
	}
	l := testLimits()
	l.MaxQueuedDatagrams = 65536
	before := p.Snapshot()
	if q, err := New(s, l, testEpoch()); !errors.Is(err, ErrResourceLimit) || q != nil || p.Snapshot() != before {
		t.Fatal("ring allocation was not reserved before construction")
	}
	l = testLimits()
	l.MaxFragmentsPerDatagram, l.MaxDatagramBytes, l.MaxQueuedBytes = 1, 80, 70
	q, p2, _ := testQueue(t, l, testEpoch())
	now := time.Unix(1, 0)
	for _, size := range []int{73, 81, 1024} {
		before, creditBefore := q.Snapshot(), p2.Snapshot()
		if id, err := q.Admit(make([]byte, size), now); !errors.Is(err, ErrMessageTooLarge) || id != 0 || q.Snapshot() != before || p2.Snapshot() != creditBefore {
			t.Fatal("oversized whole original consumed state")
		}
	}
	beforeQueue, beforeCredit := q.Snapshot(), p2.Snapshot()
	if id, err := q.Admit(make([]byte, 71), now); !errors.Is(err, ErrResourceLimit) || id != 0 || q.Snapshot() != beforeQueue || p2.Snapshot() != beforeCredit {
		t.Fatal("whole queue-byte bound admitted a prefix")
	}
	for _, q := range []*Queue{nil, {}} {
		if _, err := q.Admit(nil, now); !errors.Is(err, ErrClosed) {
			t.Fatal("uninitialized queue admitted")
		}
		if _, err := q.Ready(now); !errors.Is(err, ErrClosed) {
			t.Fatal("uninitialized queue ready")
		}
		if _, err := q.Seal(now, true); !errors.Is(err, ErrClosed) {
			t.Fatal("uninitialized queue sealed")
		}
		q.Cancel(1)
		q.Close()
		q.Close()
	}
}
