package mpudp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func (r *v2Peer) enqueue(event v2Ingress) {
	select {
	case <-r.peer.ctx.Done():
		return
	default:
	}
	select {
	case r.ingress <- event:
		if event.err == nil {
			r.peer.statistics.ingressAccepted.Add(1)
		}
	default:
		if event.err == nil {
			r.peer.statistics.ingressDrops.Add(1)
		}
	}
}

func (r *v2Peer) nextDeadline() time.Time {
	next := r.engine.NextDeadline()
	for s := range r.sessions {
		if s.controller != nil && !s.closed {
			candidate := s.controller.NextDeadline()
			if !candidate.IsZero() && (next.IsZero() || candidate.Before(next)) {
				next = candidate
			}
		}
	}
	return next
}

func (r *v2Peer) run() {
	defer close(r.peer.workerDone)
	timer := r.peer.deps.newTimer()
	if !timer.Stop() {
		<-timer.Channel()
	}
	defer timer.Stop()
	for {
		r.mu.Lock()
		deadline := r.nextDeadline()
		r.mu.Unlock()
		var timerC <-chan time.Time
		if !deadline.IsZero() {
			timer.Reset(max(0, time.Until(deadline)))
			timerC = timer.Channel()
		}
		select {
		case <-r.peer.ctx.Done():
			return
		case err := <-r.peer.listenerFailure:
			r.mu.Lock()
			r.report("MPUDP v2 listener failed", err)
			r.closeListener()
			r.mu.Unlock()
		case event := <-r.ingress:
			r.mu.Lock()
			if !r.closed {
				r.receive(event)
			}
			r.mu.Unlock()
		case <-r.peer.wake:
		case <-timerC:
			r.mu.Lock()
			if !r.closed {
				result, err := r.engine.Advance(time.Now())
				r.handleHandshake(result)
				r.report("MPUDP v2 handshake deadline failed", err)
				for s := range r.sessions {
					if s.controller != nil && !s.closed {
						result, err := s.controller.Advance(time.Now())
						r.handleSession(s, result, err)
					}
				}
			}
			r.mu.Unlock()
		}
		if timerC != nil && !timer.Stop() {
			select {
			case <-timer.Channel():
			default:
			}
		}
	}
}

func (r *v2Peer) receive(event v2Ingress) {
	if event.err != nil {
		r.report("MPUDP v2 transport failed", event.err)
		if !isRecoverableRuntimeTransportError(event.err) && event.target != nil {
			r.closeSession(event.target)
		}
		return
	}
	if event.socketID == 1 && (r.listener == nil || r.listener.closed) {
		return
	}
	if event.target != nil && event.target.closed {
		return
	}
	packet := event.packet
	if packet.Reply == nil || packet.Context == nil || packet.Context.Err() != nil || packet.Generation != packet.Reply.Generation() {
		return
	}
	binding, err := v2Binding(event.socketID, packet.LocalAddr, packet.RemoteAddr)
	if err != nil {
		return
	}
	envelope, err := wirev2.ParseEnvelope(packet.Payload)
	if err != nil {
		return
	}
	header := envelope.Header()
	if header.Type.IsHandshake() || header.Type == wirev2.TypeClose {
		r.current = v2Bootstrap{binding: binding, path: packet.Reply, owner: event.target}
		result, err := r.engine.Receive(time.Now(), binding, packet.Payload)
		r.current = v2Bootstrap{}
		r.handleHandshake(result)
		r.report("MPUDP v2 handshake packet rejected", err)
		return
	}
	s := r.established[header.SessionID]
	if s == nil || s.closed || (event.target != nil && s != event.target) || (event.socketID == 1 && !s.inbound) {
		return
	}
	result, err := s.controller.Receive(time.Now(), binding, packet.Reply, packet.Payload)
	r.handleSession(s, result, err)
}

func (r *v2Peer) closeSession(s *v2Session) {
	if s == nil || s.closed {
		return
	}
	if s.controller != nil {
		result, err := r.engine.CloseSession(time.Now(), s.id)
		r.handleHandshake(result)
		r.report("MPUDP v2 close failed", err)
	} else if s.dial != 0 {
		result, err := r.engine.CancelDial(time.Now(), s.dial)
		r.handleHandshake(result)
		r.report("MPUDP v2 cancellation failed", err)
	}
	r.dispose(s)
}

func (r *v2Peer) dispose(s *v2Session) {
	if s.closed {
		return
	}
	s.closed = true
	s.cancel()
	close(s.done)
	s.notify()
	if s.controller != nil {
		s.controller.Close()
	}
	for len(s.delivery) > 0 {
		(<-s.delivery).Release()
	}
	var failures []error
	for _, carrier := range s.carriers {
		if err := carrier.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	s.closeErr = mapV2Error(errors.Join(failures...))
	for _, path := range s.paths {
		delete(r.sockets, path.Binding.SocketID)
	}
	s.carriers, s.paths = nil, nil
	delete(r.sessions, s)
	delete(r.established, s.id)
	delete(r.dials, s.dial)
	for id, route := range r.routes {
		if route.owner == s || id == s.id {
			delete(r.routes, id)
		}
	}
	if s.inbound && r.listener != nil {
		// Closed unaccepted Sessions must not consume bounded accept slots.
		count := len(r.listener.accept)
		for i := 0; i < count; i++ {
			select {
			case queued := <-r.listener.accept:
				if queued != s {
					r.listener.accept <- queued
				}
			default:
			}
		}
	}
}

func (r *v2Peer) closeListener() error {
	l := r.listener
	if l == nil || l.closed {
		if l != nil {
			return l.closeErr
		}
		return nil
	}
	l.closed = true
	close(l.done)
	var failures []error
	for s := range r.sessions {
		if s.inbound {
			r.closeSession(s)
			failures = append(failures, s.closeErr)
		}
	}
	for id, route := range r.routes {
		if route.binding.SocketID == 1 {
			result, err := r.engine.CloseSession(time.Now(), id)
			r.handleHandshake(result)
			r.report("MPUDP v2 pending listener close failed", err)
		}
	}
	if r.peer.listenerSocket != nil {
		failures = append(failures, r.peer.listenerSocket.Close())
	}
	l.closeErr = mapV2Error(errors.Join(failures...))
	return l.closeErr
}

func (r *v2Peer) closePeer() {
	p := r.peer
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.cancel()
	r.mu.Lock()
	r.closed = true
	var failures []error
	sessions := make([]*v2Session, 0, len(r.sessions))
	for s := range r.sessions {
		sessions = append(sessions, s)
	}
	_, err := r.engine.Close(time.Now())
	failures = append(failures, err)
	for _, s := range sessions {
		r.dispose(s)
		failures = append(failures, s.closeErr)
	}
	failures = append(failures, r.closeListener())
	r.credits.Close()
	clear(r.routes)
	r.mu.Unlock()
	<-p.workerDone
	for len(r.ingress) > 0 {
		<-r.ingress
	}
	p.closeErr = mapV2Error(errors.Join(failures...))
	close(p.closeDone)
}

func (r *v2Peer) publicListener() (Listener, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.peer.ctx.Err() != nil || (r.listener != nil && r.listener.closed) {
		return nil, ErrClosed
	}
	if r.listener == nil {
		return nil, ErrModeUnavailable
	}
	return r.listener, nil
}

func (r *v2Peer) string() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fmt.Sprintf("Peer{mode:%s sessions:%d closed:%t}", modeOf(r.peer.config), len(r.sessions), r.closed)
}

type v2Listener struct {
	owner    *v2Peer
	accept   chan *v2Session
	done     chan struct{}
	closed   bool
	closeErr error
}

func (l *v2Listener) Accept(ctx context.Context) (Session, error) {
	if ctx == nil {
		return nil, ErrInvalidConfig
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		select {
		case <-l.done:
			return nil, ErrClosed
		case <-l.owner.peer.ctx.Done():
			return nil, ErrClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		case s := <-l.accept:
			r := l.owner
			r.mu.Lock()
			if l.closed || r.closed {
				r.mu.Unlock()
				return nil, ErrClosed
			}
			if s.closed {
				r.mu.Unlock()
				continue
			}
			err := r.engine.MarkAccepted(time.Now(), s.id)
			r.mu.Unlock()
			if err != nil {
				return nil, mapV2Error(err)
			}
			return s, nil
		}
	}
}

func (l *v2Listener) Close() error {
	l.owner.mu.Lock()
	defer l.owner.mu.Unlock()
	return l.owner.closeListener()
}

func (l *v2Listener) String() string {
	l.owner.mu.Lock()
	defer l.owner.mu.Unlock()
	return fmt.Sprintf("Listener{queued:%d closed:%t}", len(l.accept), l.closed)
}

var _ transport.ReplyPath = (*transport.Carrier)(nil)
