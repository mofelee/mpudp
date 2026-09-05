// Package aggregationv2 implements an isolated caller-clock-driven v2 queue.
// It performs no socket work, timers, goroutines, repair or public Flush fences.
package aggregationv2

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
	"unsafe"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/fecv2"
)

var (
	ErrInvalid         = errors.New("invalid v2 aggregation queue")
	ErrClosed          = errors.New("v2 aggregation queue closed")
	ErrResourceLimit   = creditv2.ErrResourceLimit
	ErrMessageTooLarge = errors.New("v2 Datagram exceeds aggregation bounds")
	ErrIDExhausted     = errors.New("v2 aggregation ID exhausted")
	ErrClockRegression = errors.New("v2 aggregation clock regressed")
)

type Limits struct {
	MaxQueuedDatagrams      int
	MaxQueuedBytes          uint64
	MaxDatagramBytes        uint32
	MaxFragmentsPerDatagram int
	MaxDelay                time.Duration
}

// Epoch is frozen for the queue lifetime, including the padded tail size.
type Epoch struct {
	ID         uint32
	Parameters fecv2.Parameters
}

type entry struct {
	id         uint64
	payload    []byte
	offset     uint32
	fragments  int
	admittedAt time.Time
	lease      *creditv2.Lease
}

// Queue must not be copied. Methods serialize commit order; callers may reuse
// an input slice after Admit returns. Session teardown must close its queues
// as well as its credit scope; ledger Close does not dispose of queue buffers.
type Queue struct {
	mu              sync.Mutex
	session         *creditv2.Session
	limits          Limits
	epoch           Epoch
	codec           *fecv2.Codec
	ring            []entry
	ringLease       *creditv2.Lease
	head, count     int
	retainedBytes   uint64
	nextDatagramID  uint64
	nextGroupID     uint64
	outputBytes     uint64
	lastNow         time.Time
	haveNow, closed bool
}

func New(session *creditv2.Session, limits Limits, epoch Epoch) (*Queue, error) {
	state := session.Snapshot()
	if state.Closed || state.PendingHandshake {
		return nil, invalid("an open established credit scope is required")
	}
	if limits.MaxQueuedDatagrams < 1 || limits.MaxQueuedDatagrams > 65536 || limits.MaxQueuedBytes == 0 || limits.MaxQueuedBytes > creditv2.MaxRetainedBytes || limits.MaxDatagramBytes == 0 || limits.MaxDatagramBytes > fecv2.MaxDatagramBytes || limits.MaxFragmentsPerDatagram < 1 || limits.MaxFragmentsPerDatagram > 4096 || limits.MaxDelay < time.Microsecond || limits.MaxDelay > 10*time.Millisecond || epoch.ID == 0 {
		return nil, invalid("limits or epoch outside bounds")
	}
	codec, err := fecv2.New(epoch.Parameters)
	if err != nil {
		return nil, err
	}
	if limits.MaxDatagramBytes > uint32(epoch.Parameters.MaxDatagramBytes) {
		return nil, invalid("Datagram limit exceeds encoding context")
	}
	// Sizeof measures the ring's actual fixed backing storage, not allocator
	// overhead. Payloads and encoded output have separate concurrent leases.
	slotBytes := uint64(unsafe.Sizeof(entry{}))
	if uint64(limits.MaxQueuedDatagrams) > math.MaxUint64/slotBytes {
		return nil, invalid("ring size overflow")
	}
	ringBytes := uint64(limits.MaxQueuedDatagrams) * slotBytes
	ringLease, err := session.Reserve(creditv2.Claim{Bytes: ringBytes})
	if err != nil {
		return nil, err
	}
	n := uint64(epoch.Parameters.DataShards + epoch.Parameters.ParityShards)
	outputBytes := n * (uint64(epoch.Parameters.ShardBytes) + uint64(unsafe.Sizeof([]byte{})))
	return &Queue{session: session, limits: limits, epoch: epoch, codec: codec, ring: make([]entry, limits.MaxQueuedDatagrams), ringLease: ringLease, nextDatagramID: 1, nextGroupID: 1, outputBytes: outputBytes}, nil
}

func invalid(reason string) error { return fmt.Errorf("%w: %s", ErrInvalid, reason) }

func (q *Queue) checkLocked(now time.Time) error {
	if q.closed || q.session == nil || q.session.Snapshot().Closed {
		return ErrClosed
	}
	if q.haveNow && now.Before(q.lastNow) {
		return ErrClockRegression
	}
	return nil
}

func (q *Queue) clockLocked(now time.Time) {
	q.lastNow, q.haveNow = now, true
}

// Admit reserves and copies the complete Datagram before committing its ID.
// Empty Datagrams consume ring slots whose metadata was reserved by New.
// Queue bytes count full retained backing slices, including consumed prefixes.
func (q *Queue) Admit(payload []byte, now time.Time) (uint64, error) {
	if q == nil {
		return 0, ErrClosed
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.checkLocked(now); err != nil {
		return 0, err
	}
	if q.nextDatagramID == 0 || q.nextGroupID == 0 {
		return 0, ErrIDExhausted
	}
	capacity := q.epoch.Parameters.MaxLogicalBytes - fecv2.ManifestBytes - fecv2.DescriptorBytes
	if uint64(len(payload)) > uint64(q.limits.MaxDatagramBytes) || (len(payload) > 0 && (capacity == 0 || fragmentCount(len(payload), capacity) > q.limits.MaxFragmentsPerDatagram)) {
		return 0, ErrMessageTooLarge
	}
	if q.count == len(q.ring) || uint64(len(payload)) > q.limits.MaxQueuedBytes-q.retainedBytes {
		return 0, ErrResourceLimit
	}
	var lease *creditv2.Lease
	if len(payload) > 0 {
		var err error
		lease, err = q.session.Reserve(creditv2.Claim{Bytes: uint64(len(payload))})
		if err != nil {
			return 0, err
		}
	}
	copyOfPayload := make([]byte, len(payload))
	copy(copyOfPayload, payload)
	id := q.nextDatagramID
	q.ring[(q.head+q.count)%len(q.ring)] = entry{id: id, payload: copyOfPayload, admittedAt: now, lease: lease}
	q.count++
	q.retainedBytes += uint64(len(payload))
	q.nextDatagramID++
	q.clockLocked(now)
	return id, nil
}

func fragmentCount(bytes, capacity int) int {
	if bytes == 0 {
		return 0
	}
	return 1 + (bytes-1)/capacity
}

type plan struct {
	fragments [fecv2.MaxDescriptors]fecv2.Fragment
	count     int
	cursor    fecv2.Cursor
	full      bool
}

func (q *Queue) planLocked() plan {
	var result plan
	p := q.epoch.Parameters
	used := fecv2.ManifestBytes
	freshCapacity := p.MaxLogicalBytes - fecv2.ManifestBytes - fecv2.DescriptorBytes
	for i := 0; i < q.count; i++ {
		e := &q.ring[(q.head+i)%len(q.ring)]
		remaining := len(e.payload) - int(e.offset)
		room := p.MaxLogicalBytes - used - fecv2.DescriptorBytes
		if result.count == p.MaxDescriptors || room < 0 || (room == 0 && remaining > 0) {
			result.full = true
			break
		}
		consumed := min(room, remaining)
		// A short current tail must not spend an extra fragment beyond the
		// original admission's limit. Start this original in a fresh group.
		if consumed < remaining && e.fragments+1+fragmentCount(remaining-consumed, freshCapacity) > q.limits.MaxFragmentsPerDatagram {
			result.full = true
			break
		}
		result.fragments[result.count] = fecv2.Fragment{DatagramID: e.id, TotalBytes: uint32(len(e.payload)), Offset: e.offset, Payload: e.payload[e.offset:]}
		result.count++
		used += fecv2.DescriptorBytes + consumed
		if consumed < remaining {
			result.cursor = fecv2.Cursor{Next: i, Bytes: consumed}
			result.full = true
			break
		}
		result.cursor = fecv2.Cursor{Next: i + 1}
	}
	if used == p.MaxLogicalBytes || result.count == p.MaxDescriptors {
		result.full = true
	}
	return result
}

func (q *Queue) dueLocked(now time.Time, planned plan) bool {
	return q.count > 0 && (planned.full || !now.Before(q.ring[q.head].admittedAt.Add(q.limits.MaxDelay)))
}

// Ready reports capacity/descriptor/oldest-admission deadline readiness.
// Successful clock-bearing calls establish a monotonic caller-clock floor.
func (q *Queue) Ready(now time.Time) (bool, error) {
	if q == nil {
		return false, ErrClosed
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.checkLocked(now); err != nil {
		return false, err
	}
	q.clockLocked(now)
	return q.dueLocked(now, q.planLocked()), nil
}

// Seal emits one immutable group when ready, or a tail when force is true.
// A nil output with nil error means empty/not ready. It reserves all encoded
// backing plus shard-slice storage before encoding; errors retain queue IDs,
// payloads and cursors. This is not a Flush/socket-completion fence.
func (q *Queue) Seal(now time.Time, force bool) (*Output, error) {
	if q == nil {
		return nil, ErrClosed
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.checkLocked(now); err != nil {
		return nil, err
	}
	if q.count == 0 {
		q.clockLocked(now)
		return nil, nil
	}
	if q.nextGroupID == 0 {
		return nil, ErrIDExhausted
	}
	planned := q.planLocked()
	if !force && !q.dueLocked(now, planned) {
		q.clockLocked(now)
		return nil, nil
	}
	if planned.count == 0 {
		return nil, invalid("admitted Datagram cannot make encoding progress")
	}
	lease, err := q.session.Reserve(creditv2.Claim{Bytes: q.outputBytes})
	if err != nil {
		return nil, err
	}
	group, cursor, err := q.codec.EncodePrefix(planned.fragments[:planned.count])
	if err != nil || cursor != planned.cursor {
		group.Shards = nil
		lease.Release()
		if err != nil {
			return nil, err
		}
		return nil, invalid("packing cursor differs from admission plan")
	}
	output := &Output{state: &outputState{group: SealedGroup{EncodingEpoch: q.epoch.ID, GroupID: q.nextGroupID, Group: group}, lease: lease}}
	for range cursor.Next {
		q.dropHeadLocked()
	}
	if cursor.Bytes > 0 {
		e := &q.ring[q.head]
		e.offset += uint32(cursor.Bytes)
		e.fragments++
	}
	q.nextGroupID++
	q.clockLocked(now)
	return output, nil
}

func (q *Queue) dropHeadLocked() {
	e := &q.ring[q.head]
	lease := e.lease
	q.retainedBytes -= uint64(len(e.payload))
	*e = entry{}
	lease.Release()
	q.head = (q.head + 1) % len(q.ring)
	q.count--
}

// Cancel drops only the still-queued remainder of an admitted original. It
// does not revoke already-returned groups or reuse IDs. Public Flush waiter
// cancellation must not call this: admitted Datagram ownership is separate.
func (q *Queue) Cancel(id uint64) bool {
	if q == nil {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.session == nil {
		return false
	}
	for i := 0; i < q.count; i++ {
		index := (q.head + i) % len(q.ring)
		if q.ring[index].id != id {
			continue
		}
		lease := q.ring[index].lease
		q.retainedBytes -= uint64(len(q.ring[index].payload))
		for j := i; j < q.count-1; j++ {
			q.ring[(q.head+j)%len(q.ring)] = q.ring[(q.head+j+1)%len(q.ring)]
		}
		q.ring[(q.head+q.count-1)%len(q.ring)] = entry{}
		q.count--
		lease.Release()
		return true
	}
	return false
}

// Close discards all unsealed remainders, clears ring references, then returns
// their leases. Returned outputs remain independently owned by their callers.
func (q *Queue) Close() {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	for q.count > 0 {
		q.dropHeadLocked()
	}
	q.ring = nil
	q.ringLease.Release()
	q.ringLease = nil
	q.codec = nil
}

type Snapshot struct {
	QueuedDatagrams int
	RetainedBytes   uint64
	NextDatagramID  uint64
	NextGroupID     uint64
	OldestDeadline  time.Time
	Closed          bool
}

func (q *Queue) Snapshot() Snapshot {
	if q == nil {
		return Snapshot{Closed: true}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	result := Snapshot{QueuedDatagrams: q.count, RetainedBytes: q.retainedBytes, NextDatagramID: q.nextDatagramID, NextGroupID: q.nextGroupID, Closed: q.closed || q.session == nil || q.session.Snapshot().Closed}
	if q.count > 0 {
		result.OldestDeadline = q.ring[q.head].admittedAt.Add(q.limits.MaxDelay)
	}
	return result
}

type SealedGroup struct {
	EncodingEpoch uint32
	GroupID       uint64
	Group         fecv2.Group
}

// Output copies share one release state. View borrows immutable shard slices;
// callers must finish using every borrowed view before Release.
type Output struct{ state *outputState }

type outputState struct {
	mu       sync.Mutex
	group    SealedGroup
	lease    *creditv2.Lease
	released bool
}

func (o *Output) View() (SealedGroup, bool) {
	if o == nil || o.state == nil {
		return SealedGroup{}, false
	}
	o.state.mu.Lock()
	defer o.state.mu.Unlock()
	return o.state.group, !o.state.released
}

func (o *Output) Release() {
	if o == nil || o.state == nil {
		return
	}
	o.state.mu.Lock()
	defer o.state.mu.Unlock()
	if o.state.released {
		return
	}
	o.state.group = SealedGroup{}
	o.state.released = true
	o.state.lease.Release()
	o.state.lease = nil
}
