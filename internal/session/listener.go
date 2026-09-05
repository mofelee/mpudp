package session

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/mofelee/mpudp/internal/fec"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wire"
)

// Listener owns the bounded SessionID registry for one authenticated listen
// surface. It does not own or close the transport Listener socket.
type Listener struct {
	mu          sync.Mutex
	settings    *settings
	maxSessions int
	sessions    map[wire.SessionID]*Session
	closed      bool
	closeOnce   sync.Once
	closeErr    error
}

// ListenerStats contains bounded retained-state counts.
type ListenerStats struct {
	Sessions          int
	Endpoints         int
	OutstandingProbes int
	LogicalDeadlines  int
}

// SessionAdvance identifies one Session's due work.
type SessionAdvance struct {
	SessionID wire.SessionID
	Result    AdvanceResult
	Err       error
}

// NewListener constructs an empty authenticated Session registry.
func NewListener(config ListenerConfig) (*Listener, error) {
	settings, err := normalizeConfig(config.Session)
	if err != nil {
		return nil, err
	}
	if config.MaxSessions <= 0 || config.MaxSessions > 65536 {
		return nil, invalidConfig("max Sessions must be in [1, 65536]")
	}
	return &Listener{
		settings: settings, maxSessions: config.MaxSessions,
		sessions: make(map[wire.SessionID]*Session),
	}, nil
}

// HandleTransportPacket adapts a transport receive callback.
func (l *Listener) HandleTransportPacket(ctx context.Context, packet transport.ReceivedPacket) (*Session, HandleResult, error) {
	return l.HandlePacket(ctx, NewReceivedPacket(packet))
}

// HandlePacket authenticates before Session lookup. Only a compatible HELLO
// may create a Session; all other unknown Session IDs are rejected without a
// retained path, timer deadline, FEC codec, or response.
func (l *Listener) HandlePacket(ctx context.Context, packet ReceivedPacket) (*Session, HandleResult, error) {
	if ctx == nil {
		return nil, HandleResult{}, invalidConfig("nil Listener HandlePacket context")
	}
	if err := ctx.Err(); err != nil {
		return nil, HandleResult{}, err
	}
	message, err := l.settings.authenticator.Decode(packet.Payload, l.settings.localMaxUDPPayload)
	if err != nil {
		return nil, HandleResult{}, err
	}
	key, err := replyIdentity(packet.Reply)
	if err != nil {
		return nil, HandleResult{Message: message}, err
	}

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, HandleResult{Message: message}, ErrClosed
	}
	if existing := l.sessions[message.Header.SessionID]; existing != nil {
		l.mu.Unlock()
		result, handleErr := existing.handleAuthenticated(ctx, packet, message)
		return existing, result, handleErr
	}
	if message.Header.Type != wire.TypeHello {
		l.mu.Unlock()
		return nil, HandleResult{Message: message}, ErrUnknownSession
	}
	if err := validateHandshakeSettings(l.settings, message.Handshake); err != nil {
		l.mu.Unlock()
		return nil, HandleResult{Message: message}, err
	}
	if len(l.sessions) >= l.maxSessions {
		l.mu.Unlock()
		return nil, HandleResult{Message: message}, ErrSessionLimit
	}

	session, err := newListenerSession(message.Header.SessionID, l.settings, message.Handshake, key, packet.Reply)
	if err != nil {
		l.mu.Unlock()
		return nil, HandleResult{Message: message}, err
	}
	session.onClose = l.removeSession
	l.sessions[message.Header.SessionID] = session
	session.mu.Lock()
	session.receiveEndpointLocked(key, len(packet.Payload))
	reply := session.endpoints[key].path
	session.mu.Unlock()
	l.mu.Unlock()

	result := HandleResult{Message: message, Created: true, Established: true, EndpointAdded: true}
	ack, err := wire.NewHelloAck(session.id, uint8(l.settings.params.DataShards), uint8(l.settings.params.ParityShards), uint16(l.settings.localMaxUDPPayload))
	if err != nil {
		return session, result, err
	}
	session.mu.Lock()
	if session.state == StateClosed {
		session.mu.Unlock()
		return session, result, ErrClosed
	}
	session.active.Add(1)
	session.mu.Unlock()
	attempts := session.executePlans(ctx, []sendPlan{{message: ack, path: reply, budget: session.sendMaxUDPPayload}})
	session.active.Done()
	result.Response = &attempts[0]
	return session, result, nil
}

func newListenerSession(id wire.SessionID, settings *settings, handshake wire.Handshake, endpointKey string, path ReplyPath) (*Session, error) {
	lifetime, cancel := context.WithCancel(context.Background())
	session := &Session{
		id: id, role: RoleListener, settings: settings, state: StateHandshaking, started: true,
		endpoints: make(map[string]*endpointState), outstanding: make(map[string]outstandingProbe),
		nextToken: initialToken(id), lifetime: lifetime, cancel: cancel,
	}
	now := settings.clock.Now()
	session.mu.Lock()
	if err := session.establishLocked(handshake, now); err != nil {
		session.mu.Unlock()
		cancel()
		return nil, err
	}
	if _, _, err := session.learnEndpointLocked(endpointKey, path, now); err != nil {
		decoder := session.decoder
		session.decoder = nil
		session.encoder = nil
		session.markClosedLocked()
		session.mu.Unlock()
		if decoder != nil {
			_ = decoder.Close()
		}
		return nil, err
	}
	session.mu.Unlock()
	return session, nil
}

func validateHandshakeSettings(settings *settings, handshake wire.Handshake) error {
	if int(handshake.DataShards) != settings.params.DataShards || int(handshake.ParityShards) != settings.params.ParityShards {
		return ErrHandshakeIncompatible
	}
	if int(handshake.MaxUDPPayload) < wire.MinUDPPayload || int(handshake.MaxUDPPayload) > wire.MaxUDPPayload {
		return ErrHandshakeIncompatible
	}
	negotiated := min(settings.localMaxUDPPayload, int(handshake.MaxUDPPayload))
	_, err := fec.DeriveLimits(settings.params, fec.Budget{
		MaxUDPPayload: negotiated, DataShardWireOverhead: wire.DataShardOverhead,
		MaxDatagramSize: settings.maxDatagramSize,
	})
	if err != nil {
		return ErrHandshakeIncompatible
	}
	return nil
}

// Session returns a current Session without changing its lifetime.
func (l *Listener) Session(id wire.SessionID) (*Session, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	session, ok := l.sessions[id]
	return session, ok
}

// Advance drives all current Sessions in SessionID order.
func (l *Listener) Advance(ctx context.Context) []SessionAdvance {
	if ctx == nil {
		return []SessionAdvance{{Err: invalidConfig("nil Listener Advance context")}}
	}
	l.mu.Lock()
	ids := make([]wire.SessionID, 0, len(l.sessions))
	byID := make(map[wire.SessionID]*Session, len(l.sessions))
	for id, session := range l.sessions {
		ids = append(ids, id)
		byID[id] = session
	}
	l.mu.Unlock()
	sort.Slice(ids, func(i, j int) bool { return string(ids[i][:]) < string(ids[j][:]) })
	results := make([]SessionAdvance, 0, len(ids))
	for _, id := range ids {
		result, err := byID[id].Advance(ctx)
		results = append(results, SessionAdvance{SessionID: id, Result: result, Err: err})
	}
	return results
}

// Stats reports retained state without expiring it.
func (l *Listener) Stats() ListenerStats {
	l.mu.Lock()
	sessions := make([]*Session, 0, len(l.sessions))
	for _, session := range l.sessions {
		sessions = append(sessions, session)
	}
	l.mu.Unlock()
	stats := ListenerStats{Sessions: len(sessions)}
	for _, session := range sessions {
		snapshot := session.Snapshot()
		stats.Endpoints += snapshot.Endpoints
		stats.OutstandingProbes += snapshot.OutstandingProbes
		if !snapshot.NextDeadline.IsZero() {
			stats.LogicalDeadlines++
		}
	}
	return stats
}

// Close closes every Session and prevents future authenticated creation. It
// does not close the transport Listener socket owned by the integration layer.
func (l *Listener) Close(ctx context.Context) error {
	if ctx == nil {
		return invalidConfig("nil Listener Close context")
	}
	l.closeOnce.Do(func() {
		l.mu.Lock()
		l.closed = true
		sessions := make([]*Session, 0, len(l.sessions))
		for _, session := range l.sessions {
			sessions = append(sessions, session)
		}
		clear(l.sessions)
		l.mu.Unlock()
		var failures []error
		for _, session := range sessions {
			if err := session.Close(ctx); err != nil {
				failures = append(failures, err)
			}
		}
		l.closeErr = errors.Join(failures...)
	})
	return l.closeErr
}

func (l *Listener) removeSession(session *Session) {
	l.mu.Lock()
	if l.sessions[session.id] == session {
		delete(l.sessions, session.id)
	}
	l.mu.Unlock()
}
