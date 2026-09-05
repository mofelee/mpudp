// Package reassemblyv2 admits decoded, authenticated FEC group fragments into
// bounded original-Datagram ownership. It does not authenticate, decode groups,
// acknowledge packets, admit migrations or enqueue application deliveries.
package reassemblyv2

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
	"unsafe"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/fecv2"
	"github.com/mofelee/mpudp/internal/recvwindow"
)

var (
	ErrInvalid = errors.New("invalid v2 Datagram reassembly")
	ErrClosed  = errors.New("v2 Datagram reassembly closed")
)

type Limits struct {
	MaxDatagrams     int
	MaxDatagramBytes int
	MaxFragments     int
	Span             uint32
	Timeout          time.Duration
}

type interval struct{ start, end uint32 }

type assembly struct {
	total    uint32
	admitted time.Time
	data     []byte
	ranges   []interval
	received uint32
	lease    *creditv2.Lease
}

// Receiver is serialized by its owner's receive/admission lock. Do not copy
// it. Callers keep the source decoded-group lease until AddGroup succeeds;
// resource pressure must leave that group pending under its original deadline.
type Receiver struct {
	scope       *creditv2.Session
	limits      Limits
	window      *recvwindow.Window
	windowLease *creditv2.Lease
	pending     map[uint64]*assembly
	last        time.Time
	haveTime    bool
	closed      bool
}

// New reserves the bitmap storage before constructing terminal history.
// The Session scope remains owned by the caller and is never closed here.
func New(scope *creditv2.Session, limits Limits) (*Receiver, error) {
	if scope == nil || limits.MaxDatagrams < 1 || limits.MaxDatagrams > 65536 || limits.MaxDatagramBytes < 1 || limits.MaxDatagramBytes > fecv2.MaxDatagramBytes || limits.MaxFragments < 1 || limits.MaxFragments > 4096 || limits.Span < 1 || limits.Span > recvwindow.MaxSpan || limits.MaxDatagrams > int(limits.Span) || limits.Timeout < 100*time.Millisecond || limits.Timeout > time.Minute {
		return nil, invalid("limits outside bounds")
	}
	if state := scope.Snapshot(); state.Closed || state.PendingHandshake {
		return nil, invalid("an open established credit scope is required")
	}
	lease, err := scope.Reserve(creditv2.Claim{Bytes: uint64((limits.Span+63)/64) * 16})
	if err != nil {
		return nil, err
	}
	window, err := recvwindow.New(limits.Span)
	if err != nil {
		lease.Release()
		return nil, err
	}
	return &Receiver{scope: scope, limits: limits, window: window, windowLease: lease, pending: make(map[uint64]*assembly)}, nil
}

func invalid(reason string) error { return fmt.Errorf("%w: %s", ErrInvalid, reason) }

func (r *Receiver) checkTime(now time.Time) error {
	if r == nil || r.window == nil || r.closed {
		return ErrClosed
	}
	if r.haveTime && now.Before(r.last) {
		return invalid("clock moved backwards")
	}
	return nil
}

func (r *Receiver) validate(fragments []fecv2.Fragment) error {
	if len(fragments) < 1 || len(fragments) > fecv2.MaxDescriptors {
		return invalid("fragment count outside bounds")
	}
	var previous uint64
	for _, f := range fragments {
		if f.DatagramID == 0 || f.DatagramID <= previous || f.TotalBytes > uint32(r.limits.MaxDatagramBytes) || f.Offset > f.TotalBytes || uint64(len(f.Payload)) > uint64(f.TotalBytes-f.Offset) || (len(f.Payload) == 0 && f.TotalBytes != 0) {
			return invalid("noncanonical fragment metadata")
		}
		previous = f.DatagramID
	}
	return nil
}

func (r *Receiver) due(a *assembly, now time.Time) bool {
	return now.Sub(a.admitted) >= r.limits.Timeout
}

type planned struct {
	a         *assembly
	new       bool
	expire    bool
	duplicate bool
	insert    int
}

// AddGroup atomically admits all nonterminal fragments of one reconstructed
// canonical group. Inputs remain borrowed until return and are never retained.
// A migration must first reconstruct and verify its original canonical group.
// An error leaves IDs, pending state, payloads, clocks and credits unchanged.
// The caller may mark its GroupID complete only after this method succeeds.
// Returned Datagrams own their leases independently of Receiver.Close.
func (r *Receiver) AddGroup(now time.Time, fragments []fecv2.Fragment) ([]*Datagram, error) {
	if err := r.checkTime(now); err != nil {
		return nil, err
	}
	if r.scope.Snapshot().Closed {
		return nil, creditv2.ErrClosed
	}
	if err := r.validate(fragments); err != nil {
		return nil, err
	}
	var plan [fecv2.MaxDescriptors]planned
	count := len(r.pending)
	for i, f := range fragments {
		a := r.pending[f.DatagramID]
		if a != nil {
			if a.total != f.TotalBytes {
				return nil, invalid("conflicting original length")
			}
			if r.due(a, now) {
				plan[i] = planned{a: a, expire: true}
				count--
				continue
			}
			at, duplicate, err := inspect(a, f)
			if err != nil {
				return nil, err
			}
			if !duplicate && len(a.ranges) == r.limits.MaxFragments {
				return nil, creditv2.ErrResourceLimit
			}
			plan[i] = planned{a: a, duplicate: duplicate, insert: at}
			continue
		}
		if r.window.State(f.DatagramID) != recvwindow.Unseen {
			continue
		}
		count++
		plan[i].new = true
	}
	if count > r.limits.MaxDatagrams {
		return nil, creditv2.ErrResourceLimit
	}

	// Reserve the entire original payload plus its bounded range table before
	// copying any payload or moving the terminal floor. Roll back every lease
	// if any Session/Peer reservation fails.
	for i, f := range fragments {
		if !plan[i].new {
			continue
		}
		charge := uint64(f.TotalBytes) + uint64(r.limits.MaxFragments)*uint64(unsafe.Sizeof(interval{}))
		lease, err := r.scope.Reserve(creditv2.Claim{Bytes: charge})
		if err != nil {
			for j := range i {
				if plan[j].new && plan[j].a != nil {
					plan[j].a.lease.Release()
				}
			}
			return nil, err
		}
		plan[i].a = &assembly{total: f.TotalBytes, admitted: now, lease: lease}
	}
	for i := range fragments {
		if plan[i].new {
			a := plan[i].a
			a.data = make([]byte, int(a.total))
			a.ranges = make([]interval, 0, r.limits.MaxFragments)
		}
	}
	r.last, r.haveTime = now, true
	var completed []*Datagram
	for i, f := range fragments {
		p := plan[i]
		if p.expire {
			r.expire(f.DatagramID, p.a)
			continue
		}
		if p.a == nil || p.duplicate {
			continue
		}
		a := p.a
		if p.new {
			r.window.Admit(f.DatagramID)
			r.pending[f.DatagramID] = a
		}
		copy(a.data[int(f.Offset):], f.Payload)
		a.ranges = slices.Insert(a.ranges, p.insert, interval{f.Offset, f.Offset + uint32(len(f.Payload))})
		a.received += uint32(len(f.Payload))
		if a.received == a.total {
			r.window.Finish(f.DatagramID, recvwindow.Completed)
			delete(r.pending, f.DatagramID)
			a.ranges = nil
			completed = append(completed, &Datagram{state: &datagramState{id: f.DatagramID, data: a.data, lease: a.lease}})
			a.data, a.lease = nil, nil
		}
	}
	return completed, nil
}

func inspect(a *assembly, f fecv2.Fragment) (int, bool, error) {
	end := f.Offset + uint32(len(f.Payload))
	at, found := slices.BinarySearchFunc(a.ranges, f.Offset, func(i interval, offset uint32) int {
		if i.start < offset {
			return -1
		}
		if i.start > offset {
			return 1
		}
		return 0
	})
	if found && a.ranges[at].end == end {
		if !bytes.Equal(a.data[int(f.Offset):int(end)], f.Payload) {
			return 0, false, invalid("conflicting duplicate bytes")
		}
		return at, true, nil
	}
	if (at > 0 && a.ranges[at-1].end > f.Offset) || (at < len(a.ranges) && a.ranges[at].start < end) {
		return 0, false, invalid("overlapping fragment ranges")
	}
	return at, false, nil
}

func (r *Receiver) expire(id uint64, a *assembly) {
	r.window.Finish(id, recvwindow.Expired)
	delete(r.pending, id)
	clear(a.data)
	a.data, a.ranges = nil, nil
	a.lease.Release()
	a.lease = nil
}

// Expire retires all elapsed pending originals without extending deadlines.
// Return order is unspecified. The list is bounded by MaxDatagrams; a caller
// may generate bounded expiry feedback but must not emit completion ACKs.
func (r *Receiver) Expire(now time.Time) ([]uint64, error) {
	if err := r.checkTime(now); err != nil {
		return nil, err
	}
	r.last, r.haveTime = now, true
	var ids []uint64
	for id, a := range r.pending {
		if r.due(a, now) {
			ids = append(ids, id)
			r.expire(id, a)
		}
	}
	return ids, nil
}

// State reports terminal history only. A live pending ID may be below Floor;
// AddGroup checks that admitted state before consulting terminal history.
func (r *Receiver) State(id uint64) recvwindow.Status {
	if r == nil || r.closed {
		return recvwindow.Closed
	}
	return r.window.State(id)
}

type Snapshot struct {
	Pending int
	History recvwindow.Snapshot
}

func (r *Receiver) Snapshot() Snapshot {
	if r == nil {
		return Snapshot{History: (*recvwindow.Window)(nil).Snapshot()}
	}
	return Snapshot{Pending: len(r.pending), History: r.window.Snapshot()}
}

// Close clears receiver-owned storage before releasing credits. It never
// releases an already returned Datagram or closes the shared Session scope.
func (r *Receiver) Close() {
	if r == nil || r.closed {
		return
	}
	r.closed = true
	for id, a := range r.pending {
		r.expire(id, a)
	}
	r.pending = nil
	r.window.Close()
	r.windowLease.Release()
	r.windowLease = nil
}

// Datagram is an immutable, owned completion. Copies share release state.
// Payload borrows bytes until Release; the owner must finish all borrowed
// reads before Release. Retaining or enqueueing it retains its charged lease.
type Datagram struct{ state *datagramState }
type datagramState struct {
	mu    sync.Mutex
	id    uint64
	data  []byte
	lease *creditv2.Lease
}

func (d *Datagram) ID() uint64 {
	if d == nil || d.state == nil {
		return 0
	}
	return d.state.id
}
func (d *Datagram) Payload() []byte {
	if d == nil || d.state == nil {
		return nil
	}
	d.state.mu.Lock()
	defer d.state.mu.Unlock()
	return d.state.data[:len(d.state.data):len(d.state.data)]
}
func (d *Datagram) Release() {
	if d == nil || d.state == nil {
		return
	}
	d.state.mu.Lock()
	defer d.state.mu.Unlock()
	clear(d.state.data)
	d.state.data = nil
	d.state.lease.Release()
	d.state.lease = nil
}
