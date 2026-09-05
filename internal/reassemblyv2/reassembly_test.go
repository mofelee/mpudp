package reassemblyv2

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/fecv2"
	"github.com/mofelee/mpudp/internal/recvwindow"
)

var start = time.Unix(1700000000, 0)

func fixture(t *testing.T, mutate func(*Limits)) (*Receiver, *creditv2.Peer, *creditv2.Session) {
	t.Helper()
	p, err := creditv2.New(creditv2.Limits{MaxPeerBytes: 1 << 20, MaxSessionBytes: 1 << 20, MaxSessions: 4, MaxPendingHandshakes: 4, MaxPendingAccepts: 4, MaxStreamsPerSession: 4, MaxPeerStreams: 8, MaxReservations: 64})
	if err != nil {
		t.Fatal(err)
	}
	s, _, err := p.BeginSession(creditv2.Claim{})
	if err != nil {
		t.Fatal(err)
	}
	l := Limits{MaxDatagrams: 8, MaxDatagramBytes: 2048, MaxFragments: 4, Span: 64, Timeout: time.Second}
	if mutate != nil {
		mutate(&l)
	}
	r, err := New(s, l)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		r.Close()
		s.Close()
		if got := p.Snapshot(); got.Bytes != 0 || got.Reservations != 0 || got.SessionSlots != 0 {
			t.Errorf("leaked ownership: %+v", got)
		}
	})
	return r, p, s
}

func fragment(id uint64, total, offset uint32, payload string) fecv2.Fragment {
	return fecv2.Fragment{DatagramID: id, TotalBytes: total, Offset: offset, Payload: []byte(payload)}
}

func add(t *testing.T, r *Receiver, now time.Time, fragments ...fecv2.Fragment) []*Datagram {
	t.Helper()
	result, err := r.AddGroup(now, fragments)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestReorderingOriginalOwnershipAndAtMostOnce(t *testing.T) {
	r, p, _ := fixture(t, nil)
	f := fragment(1, 6, 3, "def")
	if got := add(t, r, start, f); len(got) != 0 {
		t.Fatal("delivered a prefix")
	}
	clear(f.Payload)
	if got := add(t, r, start.Add(time.Millisecond), fragment(1, 6, 3, "def")); len(got) != 0 || len(r.pending[1].ranges) != 1 {
		t.Fatal("duplicate grew metadata")
	}
	done := add(t, r, start.Add(2*time.Millisecond), fragment(1, 6, 0, "abc"), fragment(2, 0, 0, ""))
	if len(done) != 2 || done[0].ID() != 1 || string(done[0].Payload()) != "abcdef" || done[1].ID() != 2 || len(done[1].Payload()) != 0 {
		t.Fatal("boundaries, empty original or payload ownership lost")
	}
	if r.State(1) != recvwindow.Completed || r.State(2) != recvwindow.Completed || r.Snapshot().Pending != 0 {
		t.Fatal("completion did not atomically retire originals")
	}
	if got := add(t, r, start.Add(3*time.Millisecond), fragment(1, 6, 3, "def"), fragment(2, 0, 0, "")); len(got) != 0 {
		t.Fatal("original delivered twice")
	}
	owned := done[0].Payload()
	copyHandle := *done[0]
	r.Close()
	if p.Snapshot().Bytes == 0 || string(done[0].Payload()) != "abcdef" {
		t.Fatal("receiver Close revoked transferred payload")
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() { defer wg.Done(); copyHandle.Release() }()
	}
	wg.Wait()
	if done[0].Payload() != nil || !bytes.Equal(owned, make([]byte, len(owned))) {
		t.Fatal("Release retained payload or copied handle state")
	}
	done[1].Release()
	if p.Snapshot().Bytes != 0 {
		t.Fatal("transferred bytes remained charged")
	}
}

func TestConstructorCannotInstallBeforePromotion(t *testing.T) {
	r, p, _ := fixture(t, nil)
	s, lease, err := p.BeginHandshake(creditv2.Claim{Bytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer lease.Release()
	before := p.Snapshot()
	if candidate, err := New(s, r.limits); candidate != nil || !errors.Is(err, ErrInvalid) {
		t.Fatal("pending scope installed reassembly")
	}
	if p.Snapshot() != before {
		t.Fatal("rejected constructor retained ownership")
	}
	bytes, _ := RequiredInitialBytes(r.limits)
	prepaid, err := r.scope.Reserve(creditv2.Claim{Bytes: bytes})
	if err != nil {
		t.Fatal(err)
	}
	defer prepaid.Release()
	before, beforeLease := p.Snapshot(), prepaid.Snapshot()
	for _, change := range []func(*Limits){
		func(l *Limits) { l.MaxDatagrams = 0 }, func(l *Limits) { l.MaxDatagrams = int(l.Span) + 1 },
		func(l *Limits) { l.MaxDatagramBytes = fecv2.MaxDatagramBytes + 1 }, func(l *Limits) { l.MaxFragments = 4097 },
		func(l *Limits) { l.Span = 0 }, func(l *Limits) { l.Span = 65537 }, func(l *Limits) { l.Timeout = 99 * time.Millisecond },
	} {
		limits := r.limits
		change(&limits)
		if candidate, err := New(r.scope, limits); candidate != nil || !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid constructor bounds accepted")
		}
		if bytes, err := RequiredInitialBytes(limits); err == nil || bytes != 0 {
			t.Fatal("invalid limits produced an initial byte requirement")
		}
		if candidate, err := NewPrepaid(r.scope, limits, prepaid); candidate != nil || !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid prepaid constructor bounds accepted")
		}
		if p.Snapshot() != before || prepaid.Snapshot() != beforeLease {
			t.Fatal("invalid constructor leaked credits")
		}
	}
	candidate, err := NewPrepaid(r.scope, r.limits, prepaid)
	if err != nil {
		t.Fatalf("invalid prepaid construction bound the caller lease: %v", err)
	}
	candidate.Close()
}

func TestGroupFailureRollsBackAllOriginalsAndCredits(t *testing.T) {
	r, p, s := fixture(t, nil)
	add(t, r, start, fragment(1, 4, 0, "ab"))
	base := r.Snapshot()
	usage := p.Snapshot()
	// Leave room for one original's payload, range slots and deadline links.
	pressure, err := s.Reserve(creditv2.Claim{Bytes: (1 << 20) - usage.Bytes - (40 + deadlineLinkBytes)})
	if err != nil {
		t.Fatal(err)
	}
	charged := p.Snapshot()
	batch := []fecv2.Fragment{fragment(1, 4, 2, "cd"), fragment(2, 4, 0, "ef"), fragment(3, 4, 0, "gh")}
	if got, err := r.AddGroup(start.Add(time.Millisecond), batch); got != nil || !errors.Is(err, creditv2.ErrResourceLimit) {
		t.Fatalf("resource failure: %v", err)
	}
	if r.Snapshot() != base || p.Snapshot() != charged || string(r.pending[1].data) != "ab\x00\x00" || !r.last.Equal(start) {
		t.Fatal("failed group partially committed or leaked reservation")
	}
	checkDeadline(t, r)
	pressure.Release()
	for _, d := range add(t, r, start.Add(time.Millisecond), batch...) {
		d.Release()
	}
	if r.State(1) != recvwindow.Completed || r.State(2) != recvwindow.Unseen || r.Snapshot().Pending != 2 {
		t.Fatal("retry did not commit whole group")
	}
	checkDeadline(t, r)
}

func TestDeadlineDuplicatesAndPendingBelowFloor(t *testing.T) {
	r, _, _ := fixture(t, func(l *Limits) { l.Span = 3; l.MaxDatagrams = 3 })
	add(t, r, start, fragment(1, 4, 0, "ab"))
	for id := uint64(2); id <= 9; id++ {
		for _, d := range add(t, r, start, fragment(id, 1, 0, "x")) {
			d.Release()
		}
	}
	if r.Snapshot().History.Floor != 7 || r.State(1) != recvwindow.Retired {
		t.Fatal("window did not retire old new admissions")
	}
	done := add(t, r, start.Add(900*time.Millisecond), fragment(1, 4, 2, "cd"))
	if len(done) != 1 || string(done[0].Payload()) != "abcd" {
		t.Fatal("already admitted work below floor was lost")
	}
	done[0].Release()
	if len(add(t, r, start.Add(900*time.Millisecond), fragment(1, 4, 0, "ab"))) != 0 {
		t.Fatal("retired original reopened")
	}
	add(t, r, start.Add(time.Second), fragment(10, 4, 0, "ab"))
	add(t, r, start.Add(1900*time.Millisecond), fragment(10, 4, 0, "ab"))
	if got := add(t, r, start.Add(2*time.Second), fragment(10, 4, 2, "cd")); len(got) != 0 || r.State(10) != recvwindow.Expired {
		t.Fatal("duplicate or final fragment extended deadline")
	}
	if r.Snapshot().History.Expired != 1 {
		t.Fatal("expiry was recorded as completion")
	}
}

func TestInvalidFragmentsNeverPolluteState(t *testing.T) {
	r, p, _ := fixture(t, nil)
	add(t, r, start, fragment(1, 6, 2, "cd"))
	before, credit := r.Snapshot(), p.Snapshot()
	for _, batch := range [][]fecv2.Fragment{
		nil, {fragment(0, 1, 0, "x")}, {fragment(2, 1, 0, "x"), fragment(1, 1, 0, "x")},
		{fragment(1, 6, 2, "XX")}, {fragment(1, 7, 0, "a")}, {fragment(1, 6, 1, "bc")},
		{fragment(1, 6, 3, "de")}, {fragment(1, 6, 0, "abcde")}, {fragment(1, 6, 2, "c")},
		{fragment(2, 1, 2, "")}, {fragment(2, 1, 0, "ab")}, {fragment(2, 1, 0, "")},
		{fragment(2, 2049, 0, "x")}, {fragment(2, 0, 1, "")},
	} {
		if _, err := r.AddGroup(start.Add(time.Millisecond), batch); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid group: %v", err)
		}
		if r.Snapshot() != before || p.Snapshot() != credit || !r.last.Equal(start) || string(r.pending[1].data) != "\x00\x00cd\x00\x00" {
			t.Fatal("invalid group changed owned state")
		}
	}
	if _, err := r.AddGroup(start.Add(-time.Millisecond), []fecv2.Fragment{fragment(2, 1, 0, "x")}); !errors.Is(err, ErrInvalid) {
		t.Fatal("clock regression accepted")
	}
}

func TestFragmentAndPendingLimits(t *testing.T) {
	r, p, _ := fixture(t, func(l *Limits) { l.MaxFragments = 2; l.MaxDatagrams = 2 })
	add(t, r, start, fragment(1, 6, 0, "ab"))
	add(t, r, start, fragment(1, 6, 2, "cd"), fragment(2, 2, 0, "x"))
	before, credit := r.Snapshot(), p.Snapshot()
	for _, f := range []fecv2.Fragment{fragment(1, 6, 4, "ef"), fragment(3, 1, 0, "x")} {
		if _, err := r.AddGroup(start, []fecv2.Fragment{f}); !errors.Is(err, creditv2.ErrResourceLimit) {
			t.Fatal("fragment/pending limit ignored")
		}
		if r.Snapshot() != before || p.Snapshot() != credit {
			t.Fatal("limit failure mutated state")
		}
	}
	ids, err := r.Expire(start.Add(time.Second))
	if err != nil || len(ids) != 2 || r.Snapshot().Pending != 0 {
		t.Fatal("expiry did not reclaim bounded assemblies")
	}
	for _, id := range ids {
		if r.State(id) != recvwindow.Expired {
			t.Fatal("expiry lost terminal identity")
		}
	}
	for _, d := range add(t, r, start.Add(time.Second), fragment(3, 1, 0, "x")) {
		d.Release()
	}
}

func TestExpiryAndSessionClose(t *testing.T) {
	r, p, s := fixture(t, nil)
	base := p.Snapshot().Bytes
	add(t, r, start, fragment(math.MaxUint64, 4, 0, "ab"))
	ids, err := r.Expire(start.Add(time.Second - time.Nanosecond))
	if err != nil || len(ids) != 0 {
		t.Fatal("expired too early")
	}
	ids, err = r.Expire(start.Add(time.Second))
	if err != nil || !reflect.DeepEqual(ids, []uint64{math.MaxUint64}) || r.State(math.MaxUint64) != recvwindow.Expired || p.Snapshot().Bytes != base {
		t.Fatal("uint64-boundary expiry failed")
	}
	if _, err = r.Expire(start); !errors.Is(err, ErrInvalid) {
		t.Fatal("expiry clock moved backwards")
	}
	s.Close()
	if _, err = r.AddGroup(start.Add(2*time.Second), []fecv2.Fragment{fragment(1, 1, 0, "x")}); !errors.Is(err, creditv2.ErrClosed) {
		t.Fatal("closed scope accepted group")
	}
	r.Close()
	r.Close()
	if r.State(1) != recvwindow.Closed || p.Snapshot().Bytes != 0 {
		t.Fatal("Close retained bitmap ownership")
	}
	for _, r := range []*Receiver{nil, {}} {
		r.Close()
		if _, err := r.AddGroup(start, nil); !errors.Is(err, ErrClosed) {
			t.Fatal("zero receiver accepted")
		}
		if !r.Snapshot().History.Closed {
			t.Fatal("zero history open")
		}
	}
}

func TestRealRSRecoveredGroupsPreserveOriginals(t *testing.T) {
	parameters := fecv2.Parameters{DataShards: 3, ParityShards: 2, ShardBytes: 64, MaxDescriptors: 8, MaxLogicalBytes: 192, MaxDatagramBytes: 2048}
	codec, err := fecv2.New(parameters)
	if err != nil {
		t.Fatal(err)
	}
	one, err := codec.Encode([]fecv2.Fragment{fragment(1, 40, 0, "abcdefghijklmnopqrst"), fragment(2, 0, 0, "")})
	if err != nil {
		t.Fatal(err)
	}
	two, err := codec.Encode([]fecv2.Fragment{fragment(1, 40, 20, "uvwxyzABCDEFGHIJKLMN"), fragment(3, 3, 0, "xyz")})
	if err != nil {
		t.Fatal(err)
	}
	for mask := 0; mask < 32; mask++ {
		var selected []int
		for i := range 5 {
			if mask&(1<<i) != 0 {
				selected = append(selected, i)
			}
		}
		if len(selected) != 3 {
			continue
		}
		t.Run(string(rune('A'+mask)), func(t *testing.T) {
			r, _, _ := fixture(t, nil)
			seen := map[uint64]string{}
			for _, g := range []fecv2.Group{two, one} {
				shards := make([][]byte, 5)
				for _, i := range selected {
					shards[i] = g.Shards[i]
				}
				fragments, err := codec.Decode(g.LogicalBytes, shards)
				if err != nil {
					t.Fatal(err)
				}
				for _, d := range add(t, r, start, fragments...) {
					if _, duplicate := seen[d.ID()]; duplicate {
						t.Fatal("duplicate original")
					}
					seen[d.ID()] = string(d.Payload())
					d.Release()
				}
				if got := add(t, r, start, fragments...); len(got) != 0 {
					t.Fatal("group repair delivered duplicate")
				}
			}
			want := map[uint64]string{1: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN", 2: "", 3: "xyz"}
			if !reflect.DeepEqual(seen, want) {
				t.Fatalf("originals=%v", seen)
			}
		})
	}
}
