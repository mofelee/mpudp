// Package recvwindow provides bounded terminal-ID history for one v2 Session
// direction. Datagram and encoding-group IDs need separate windows. This is
// not a packet replay filter: callers check already admitted pending work
// before consulting the window, and retain its original deadline below Floor.
package recvwindow

import (
	"errors"
	"math/bits"
)

const MaxSpan = 65536

var ErrInvalidSpan = errors.New("receive ID span must be 1..65536")

type Status uint8

const (
	Unseen Status = iota
	Completed
	Expired
	Retired
	InvalidID
	Closed
)

// Window has no pending map or timer. Its owner must serialize calls with its
// admission/state lock and must not copy it. The zero value is closed. A new
// incarnation requires New; Close cannot reopen it. Storage is at most two
// 8 KiB bitmaps, independent of pending work.
type Window struct {
	span           uint64
	highest        uint64
	completed      []uint64
	expired        []uint64
	completedCount int
	expiredCount   int
	closed         bool
}

func New(span uint32) (*Window, error) {
	if span < 1 || span > MaxSpan {
		return nil, ErrInvalidSpan
	}
	words := (int(span) + 63) / 64
	return &Window{span: uint64(span), completed: make([]uint64, words), expired: make([]uint64, words)}, nil
}

// Floor is the first admissible new ID. Admitted pending IDs below it remain
// the caller's responsibility and may finish without reopening their IDs.
func (w *Window) Floor() uint64 {
	if w == nil || w.span == 0 || w.highest < w.span {
		return 1
	}
	return w.highest - w.span + 1
}

func (w *Window) State(id uint64) Status {
	if w == nil || w.span == 0 || w.closed {
		return Closed
	}
	if id == 0 {
		return InvalidID
	}
	if id < w.Floor() {
		return Retired
	}
	if id > w.highest {
		return Unseen
	}
	index := id % w.span
	mask := uint64(1) << (index % 64)
	if w.completed[index/64]&mask != 0 {
		return Completed
	}
	if w.expired[index/64]&mask != 0 {
		return Expired
	}
	return Unseen
}

// Admit advances history only when the ID is Unseen and returns its prior
// status. Call only after authenticating metadata and reserving all initial
// ownership, under the same lock that commits pending state. Failed resource
// admission must not call Admit and cannot move the retirement floor.
func (w *Window) Admit(id uint64) Status {
	status := w.State(id)
	if status != Unseen || id <= w.highest {
		return status
	}
	distance := id - w.highest
	if distance >= w.span {
		clear(w.completed)
		clear(w.expired)
		w.completedCount, w.expiredCount = 0, 0
	} else {
		position := (w.highest + 1) % w.span
		for remaining := distance; remaining > 0; {
			width := min(remaining, 64-position%64, w.span-position)
			mask := (^uint64(0) >> (64 - width)) << (position % 64)
			word := position / 64
			w.completedCount -= bits.OnesCount64(w.completed[word] & mask)
			w.expiredCount -= bits.OnesCount64(w.expired[word] & mask)
			w.completed[word] &^= mask
			w.expired[word] &^= mask
			position = (position + width) % w.span
			remaining -= width
		}
	}
	w.highest = id
	return Unseen
}

// Finish records the caller's admitted ID as Completed or Expired, never both.
// Retained terminal results are idempotent; retained conflicts fail. It does
// not advance the floor or admit state. Callers must prove the ID was admitted,
// since a bitmap cannot distinguish an unadmitted hole from pending work.
// Admitted IDs below Floor finish successfully without a retained bit: all
// later attempts to recreate them are already rejected by the monotonic floor.
// Their owner must prevent conflicting terminal transitions in pending state.
func (w *Window) Finish(id uint64, terminal Status) bool {
	if w == nil || w.span == 0 || (terminal != Completed && terminal != Expired) || w.closed || id == 0 || id > w.highest {
		return false
	}
	status := w.State(id)
	if status == Retired || status == terminal {
		return true
	}
	if status != Unseen {
		return false
	}
	index := id % w.span
	mask := uint64(1) << (index % 64)
	if terminal == Completed {
		w.completed[index/64] |= mask
		w.completedCount++
	} else {
		w.expired[index/64] |= mask
		w.expiredCount++
	}
	return true
}

type Snapshot struct {
	Span      uint32
	Highest   uint64
	Floor     uint64
	Completed int
	Expired   int
	Closed    bool
}

func (w *Window) Snapshot() Snapshot {
	if w == nil || w.span == 0 {
		return Snapshot{Floor: 1, Closed: true}
	}
	return Snapshot{Span: uint32(w.span), Highest: w.highest, Floor: w.Floor(), Completed: w.completedCount, Expired: w.expiredCount, Closed: w.closed}
}

// Close releases bitmap storage and prevents any future admission/finish.
// It does not release the caller's pending payloads or resource reservations.
func (w *Window) Close() {
	if w == nil || w.closed {
		return
	}
	w.closed = true
	w.completed, w.expired = nil, nil
	w.completedCount, w.expiredCount = 0, 0
}
