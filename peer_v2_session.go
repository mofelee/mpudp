package mpudp

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/handshakev2"
	"github.com/mofelee/mpudp/internal/reassemblyv2"
	"github.com/mofelee/mpudp/internal/sessionv2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

type v2Session struct {
	owner        *v2Peer
	ctx          context.Context
	cancel       context.CancelFunc
	id           wirev2.SessionID
	dial         handshakev2.DialID
	inbound      bool
	controller   *sessionv2.Controller
	carriers     []runtimeCarrier
	paths        []sessionv2.Carrier
	delivery     chan *reassemblyv2.Datagram
	done         chan struct{}
	changed      chan struct{}
	closed       bool
	draining     bool
	closeErr     error
	terminalErr  error
	startupScope *creditv2.Session
	startupLease *creditv2.Lease
	graceOnce    sync.Once
	graceErr     error
}

func (r *v2Peer) newWrapper(inbound bool) *v2Session {
	parent := r.peer.ctx
	if inbound {
		parent = r.listener.ctx
	}
	ctx, cancel := context.WithCancel(parent)
	s := &v2Session{owner: r, ctx: ctx, cancel: cancel, inbound: inbound,
		delivery: make(chan *reassemblyv2.Datagram, r.peer.config.Limits.DeliveryQueueCapacity),
		done:     make(chan struct{}), changed: make(chan struct{})}
	r.sessions[s] = struct{}{}
	return s
}

func (s *v2Session) notify() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *v2Session) WritePacket(payload []byte) error {
	r := s.owner
	r.mu.Lock()
	if s.closed || s.draining || s.ctx.Err() != nil {
		r.mu.Unlock()
		return ErrClosed
	}
	if len(payload) > r.peer.config.Limits.MaxDatagramSize {
		r.mu.Unlock()
		return ErrMessageTooLarge
	}
	if s.controller == nil {
		r.mu.Unlock()
		return ErrNotReady
	}
	start := time.Now()
	receipt, result, err := s.controller.Write(start, payload)
	r.handleSession(s, result, err)
	if receipt != 0 {
		r.peer.statistics.sentDatagrams.Add(1)
		r.peer.statistics.sentDatagramBytes.Add(uint64(len(payload)))
	}
	async := r.peer.config.Aggregation.Enabled
	r.mu.Unlock()
	r.peer.wakeDriver()
	if receipt == 0 {
		return mapV2Error(err)
	}
	if async {
		return nil
	}
	err = s.waitFence(context.Background(), uint64(receipt))
	if r.peer.statistics.enabled.Load() {
		r.peer.statistics.sendLatency.Observe(time.Since(start))
	}
	return err
}

func (s *v2Session) ReadPacket() ([]byte, error) {
	for {
		r := s.owner
		r.mu.Lock()
		if s.closed || s.ctx.Err() != nil {
			r.mu.Unlock()
			return nil, ErrClosed
		}
		select {
		case packet := <-s.delivery:
			// Public []byte ownership outlives Session.Close. The retained
			// completion stays charged until the caller's copy is ready.
			payload := append([]byte{}, packet.Payload()...)
			packet.Release()
			r.peer.statistics.deliveredPackets.Add(1)
			r.peer.statistics.deliveredBytes.Add(uint64(len(payload)))
			r.mu.Unlock()
			r.peer.wakeDriver()
			return payload, nil
		default:
		}
		changed := s.changed
		r.mu.Unlock()
		select {
		case <-s.ctx.Done():
			return nil, ErrClosed
		case <-s.done:
			return nil, ErrClosed
		case <-changed:
		}
	}
}

func (s *v2Session) waitFence(ctx context.Context, frontier uint64) error {
	for {
		r := s.owner
		r.mu.Lock()
		if s.closed || s.controller == nil || s.ctx.Err() != nil {
			err := errors.Join(ErrClosed, s.terminalErr)
			r.mu.Unlock()
			return err
		}
		snapshot := s.controller.Snapshot()
		var failure error
		if snapshot.FailedFrom != 0 && snapshot.FailedFrom <= frontier {
			failure = mapV2Error(snapshot.SendError)
		}
		if snapshot.CompletedThrough >= frontier {
			r.mu.Unlock()
			return failure
		}
		changed := s.changed
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), failure)
		case <-s.ctx.Done():
			continue
		case <-s.done:
			continue
		case <-changed:
		}
	}
}

func (s *v2Session) Flush(ctx context.Context) error {
	return s.flush(ctx, false)
}

func (s *v2Session) flush(ctx context.Context, stop bool) error {
	if ctx == nil {
		return ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r := s.owner
	r.mu.Lock()
	if s.closed || s.ctx.Err() != nil {
		r.mu.Unlock()
		return ErrClosed
	}
	if stop {
		s.draining = true
	}
	if s.controller == nil {
		r.mu.Unlock()
		return ErrNotReady
	}
	if stop {
		s.controller.StopAdmissions()
	}
	fence, result, err := s.controller.Flush(time.Now())
	r.handleSession(s, result, err)
	r.mu.Unlock()
	r.peer.wakeDriver()
	return s.waitFence(ctx, uint64(fence))
}

func (s *v2Session) CloseGracefully(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidConfig
	}
	s.graceOnce.Do(func() {
		s.owner.mu.Lock()
		closed, closeErr := s.closed, s.closeErr
		s.owner.mu.Unlock()
		if closed {
			s.graceErr = closeErr
			return
		}
		err := s.flush(ctx, true)
		s.graceErr = errors.Join(err, s.Close())
	})
	return s.graceErr
}

func (s *v2Session) Close() error {
	s.cancel()
	r := s.owner
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closeSession(s)
	r.peer.wakeDriver()
	return s.closeErr
}

func (s *v2Session) String() string {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	fingerprint := sha256.Sum256(s.id[:])
	role := "initiator"
	if s.inbound {
		role = "listener"
	}
	return fmt.Sprintf("Session{%x role:%s closed:%t}", fingerprint[:6], role, s.closed)
}

func (s *v2Session) GoString() string               { return s.String() }
func (s *v2Session) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, s.String()) }

var _ DatagramSession = (*v2Session)(nil)
