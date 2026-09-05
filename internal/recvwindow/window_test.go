package recvwindow

import (
	"errors"
	"math"
	"math/rand/v2"
	"testing"
)

func TestUninitializedWindowIsClosed(t *testing.T) {
	for _, w := range []*Window{nil, {}} {
		for _, id := range []uint64{0, 1, math.MaxUint64} {
			if w.State(id) != Closed || w.Admit(id) != Closed || w.Finish(id, Completed) || w.Finish(id, Expired) {
				t.Fatal("uninitialized window accepted work")
			}
		}
		w.Close()
		w.Close()
		if w.Floor() != 1 || w.Snapshot() != (Snapshot{Floor: 1, Closed: true}) {
			t.Fatal("uninitialized window reported live state")
		}
	}
}

func TestTerminalStatesAndPendingBelowFloor(t *testing.T) {
	w, err := New(3)
	if err != nil {
		t.Fatal(err)
	}
	if w.Admit(0) != InvalidID || w.Snapshot().Highest != 0 {
		t.Fatal("zero ID polluted history")
	}
	if w.Admit(1) != Unseen || w.Admit(3) != Unseen {
		t.Fatal("admission failed")
	}
	if !w.Finish(3, Completed) || !w.Finish(3, Completed) || w.Finish(3, Expired) {
		t.Fatal("completion is not immutable/idempotent")
	}
	if w.Admit(2) != Unseen || !w.Finish(2, Expired) || w.Finish(2, Completed) {
		t.Fatal("out-of-order expiry changed completion")
	}
	if w.Admit(3) != Completed || w.Admit(2) != Expired {
		t.Fatal("terminal IDs reopened")
	}
	if w.Admit(5) != Unseen || w.Floor() != 3 {
		t.Fatal("window did not advance")
	}
	if !w.Finish(1, Completed) || w.State(1) != Retired {
		t.Fatal("admitted pending below floor could not finish")
	}
	if w.Admit(1) != Retired || w.Finish(6, Completed) {
		t.Fatal("retired/future ID polluted history")
	}
	if snapshot := w.Snapshot(); snapshot.Completed != 1 || snapshot.Expired != 0 {
		t.Fatalf("terminal counts=%+v", snapshot)
	}
	w.Close()
	w.Close()
	if w.Admit(6) != Closed || w.Finish(5, Completed) || len(w.completed) != 0 || len(w.expired) != 0 {
		t.Fatal("Close retained or recreated bitmap state")
	}
}

func TestBoundarySpansAndUint64Exhaustion(t *testing.T) {
	for _, span := range []uint32{1, 2, 3, 63, 64, 65, 127, 128, 129, 65535, 65536} {
		w, err := New(span)
		if err != nil {
			t.Fatal(err)
		}
		for id := uint64(1); id <= uint64(span)+3; id++ {
			if w.Admit(id) != Unseen || !w.Finish(id, Completed) {
				t.Fatalf("span%d id%d", span, id)
			}
		}
		if w.Snapshot().Completed != int(span) || w.State(w.Floor()-1) != Retired {
			t.Fatalf("span%d bad wrap: %+v", span, w.Snapshot())
		}
		if w.Admit(math.MaxUint64) != Unseen || !w.Finish(math.MaxUint64, Expired) {
			t.Fatal("max ID cannot finish")
		}
		if w.Floor() != math.MaxUint64-uint64(span)+1 || w.Snapshot().Expired != 1 || w.Snapshot().Completed != 0 {
			t.Fatalf("max jump: %+v", w.Snapshot())
		}
		if w.Admit(math.MaxUint64) != Expired || w.Admit(1) != Retired {
			t.Fatal("exhaustion wrapped or reopened")
		}
	}
	for _, span := range []uint32{0, 65537, math.MaxUint32} {
		if w, err := New(span); w != nil || !errors.Is(err, ErrInvalidSpan) {
			t.Fatal("invalid span accepted")
		}
	}
}

func TestSeparateNamespaces(t *testing.T) {
	datagrams, _ := New(64)
	groups, _ := New(64)
	datagrams.Admit(7)
	datagrams.Finish(7, Completed)
	groups.Admit(7)
	groups.Finish(7, Expired)
	groups.Admit(1000)
	if datagrams.State(7) != Completed || groups.State(7) != Retired {
		t.Fatal("group progress polluted original Datagram namespace")
	}
}

func TestWindowMatchesBoundedReference(t *testing.T) {
	for _, span := range []uint32{1, 3, 63, 64, 65, 127, 65535, 65536} {
		w, _ := New(span)
		model := make(map[uint64]Status)
		rng := rand.New(rand.NewPCG(uint64(span), 19))
		var highest uint64
		for range 10000 {
			var id uint64
			switch rng.IntN(3) {
			case 0:
				id = highest + 1 + uint64(rng.IntN(100))
			case 1:
				if highest > 0 {
					id = 1 + rng.Uint64N(highest)
				}
			case 2:
				id = highest
			}
			want := Unseen
			floor := uint64(1)
			if highest >= uint64(span) {
				floor = highest - uint64(span) + 1
			}
			if id == 0 {
				want = InvalidID
			} else if id < floor {
				want = Retired
			} else if state, ok := model[id]; ok {
				want = state
			}
			if got := w.Admit(id); got != want {
				t.Fatalf("span%d id%d got%d want%d %+v", span, id, got, want, w.Snapshot())
			}
			if want == Unseen {
				if id > highest {
					highest = id
				}
				floor = 1
				if highest >= uint64(span) {
					floor = highest - uint64(span) + 1
				}
				for old := range model {
					if old < floor {
						delete(model, old)
					}
				}
				terminal := Completed
				if rng.IntN(2) == 0 {
					terminal = Expired
				}
				if !w.Finish(id, terminal) {
					t.Fatal("valid terminal transition failed")
				}
				model[id] = terminal
			}
			completed, expired := 0, 0
			for id, status := range model {
				if w.State(id) != status {
					t.Fatal("model state mismatch")
				}
				if status == Completed {
					completed++
				} else {
					expired++
				}
			}
			if got := w.Snapshot(); got.Highest != highest || got.Completed != completed || got.Expired != expired {
				t.Fatalf("span%d reference count mismatch %+v", span, got)
			}
		}
	}
}
