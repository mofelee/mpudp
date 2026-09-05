package aggregationv2

import (
	"sync"
	"unsafe"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/fecv2"
)

// OutputWorkspace prepays one live group's output. Copies share ownership.
// Close prevents reuse and retains its lease until any live Output is released.
type OutputWorkspace struct{ state *outputWorkspaceState }

type outputWorkspaceState struct {
	mu           sync.Mutex
	queue        *Queue
	lease        *creditv2.Lease
	busy, closed bool
}

// RequiredOutputWorkspaceBytes sizes one group's backing, shard slices and
// workspace/output owners without constructing a codec or allocating buffers.
func RequiredOutputWorkspaceBytes(shards, shardBytes int) (uint64, error) {
	if shards < 2 || shards > 256 || shardBytes < 1 || shardBytes > fecv2.MaxShardBytes {
		return 0, invalid("output workspace dimensions outside bounds")
	}
	return uint64(shards)*(uint64(shardBytes)+uint64(unsafe.Sizeof([]byte{}))) +
		uint64(unsafe.Sizeof(OutputWorkspace{})) + uint64(unsafe.Sizeof(outputWorkspaceState{})) +
		uint64(unsafe.Sizeof(Output{})) + uint64(unsafe.Sizeof(outputState{})), nil
}

// NewPrepaidOutputWorkspace binds a dedicated byte-only lease for this queue's
// frozen output dimensions. Failure leaves prepaid unchanged. The caller must
// Close both the queue and workspace; neither invalidates a returned Output.
func (q *Queue) NewPrepaidOutputWorkspace(prepaid *creditv2.Lease) (*OutputWorkspace, error) {
	if q == nil {
		return nil, ErrClosed
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.session == nil || q.session.Snapshot().Closed {
		return nil, ErrClosed
	}
	p := q.epoch.Parameters
	bytes, err := RequiredOutputWorkspaceBytes(p.DataShards+p.ParityShards, p.ShardBytes)
	if err != nil {
		return nil, err
	}
	lease, err := q.session.BindBytes(prepaid, bytes)
	if err != nil {
		return nil, err
	}
	return &OutputWorkspace{state: &outputWorkspaceState{queue: q, lease: lease}}, nil
}

func (s *outputWorkspaceState) acquire(q *Queue) error {
	if s == nil {
		return invalid("nil output workspace")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.queue != q {
		return invalid("output workspace belongs to another queue")
	}
	if s.busy {
		return ErrResourceLimit
	}
	s.busy = true
	return nil
}

func (s *outputWorkspaceState) releaseOutput() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.busy = false
	if s.closed {
		s.lease.Release()
		s.lease = nil
	}
}

func (w *OutputWorkspace) Close() {
	if w == nil || w.state == nil {
		return
	}
	s := w.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed, s.queue = true, nil
	if !s.busy {
		s.lease.Release()
		s.lease = nil
	}
}
