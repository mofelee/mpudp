package mpudp

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"io"
	"sync"

	"github.com/mofelee/mpudp/config"
)

// Mode describes which bootstrap roles a Peer configuration enables.
type Mode string

const (
	ModeInitiator Mode = "initiator"
	ModeListener  Mode = "listener"
	ModeDual      Mode = "dual"
)

// Session is a bidirectional Datagram tunnel. Methods are safe for concurrent
// use. Concurrent writes remain distinct Datagrams; their send order is not
// specified. Close is idempotent and unblocks future runtime reads.
type Session interface {
	WritePacket(payload []byte) error
	ReadPacket() ([]byte, error)
	Close() error
}

// Listener accepts authenticated inbound Sessions. Accept is context-aware,
// safe for concurrent use, and will eventually unblock with ErrClosed after
// Close. The issue #2 skeleton returns ErrNotReady immediately because it has
// no socket or handshake implementation.
type Listener interface {
	Accept(ctx context.Context) (Session, error)
	Close() error
}

// Peer owns initiator and/or listener lifecycle state. Construction validates
// and copies configuration but performs no network or background activity.
type Peer struct {
	mu       sync.Mutex
	config   config.Config
	random   io.Reader
	closed   bool
	listener *listener
	sessions map[*session]struct{}
}

// NewPeer validates cfg before allocating lifecycle handles and always uses
// crypto/rand.Reader for Session IDs. Invalid configuration cannot open a
// socket or start a goroutine or timer.
func NewPeer(cfg config.Config) (*Peer, error) {
	return newPeer(cfg, cryptorand.Reader)
}

func newPeer(cfg config.Config, random io.Reader) (*Peer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if random == nil {
		return nil, fmt.Errorf("%w: random reader must not be nil", ErrInvalidConfig)
	}
	return &Peer{
		config:   cfg.Clone(),
		random:   random,
		sessions: make(map[*session]struct{}),
	}, nil
}

// Mode reports the roles enabled by the validated configuration.
func (p *Peer) Mode() Mode {
	p.mu.Lock()
	defer p.mu.Unlock()
	return modeOf(p.config)
}

// Config returns a deep copy of the validated configuration.
func (p *Peer) Config() config.Config {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.config.Clone()
}

// NewSession creates inert initiator lifecycle state with a fresh SessionID.
// It does not create Carrier sockets or perform a handshake.
func (p *Peer) NewSession() (Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	if !p.config.InitiatorEnabled() {
		return nil, ErrModeUnavailable
	}
	if len(p.sessions) >= p.config.Limits.MaxSessions {
		return nil, ErrResourceLimit
	}
	id, err := newSessionID(p.random)
	if err != nil {
		return nil, err
	}
	s := &session{id: id, maxDatagramSize: p.config.Limits.MaxDatagramSize, owner: p}
	p.sessions[s] = struct{}{}
	return s, nil
}

// Listener returns the single inert listener lifecycle handle for this Peer.
// It does not bind the configured address.
func (p *Peer) Listener() (Listener, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	if !p.config.ListenerEnabled() {
		return nil, ErrModeUnavailable
	}
	if p.listener == nil {
		p.listener = &listener{}
	}
	return p.listener, nil
}

// Close idempotently closes every lifecycle handle without waiting on
// background work; this skeleton never starts any.
func (p *Peer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	listener := p.listener
	sessions := make([]*session, 0, len(p.sessions))
	for session := range p.sessions {
		sessions = append(sessions, session)
	}
	p.mu.Unlock()

	if listener != nil {
		_ = listener.Close()
	}
	for _, session := range sessions {
		_ = session.Close()
	}
	return nil
}

func (p *Peer) removeSession(s *session) {
	p.mu.Lock()
	delete(p.sessions, s)
	p.mu.Unlock()
}

func modeOf(cfg config.Config) Mode {
	if cfg.InitiatorEnabled() && cfg.ListenerEnabled() {
		return ModeDual
	}
	if cfg.InitiatorEnabled() {
		return ModeInitiator
	}
	return ModeListener
}

type session struct {
	mu              sync.RWMutex
	id              SessionID
	maxDatagramSize int
	owner           *Peer
	closed          bool
}

func (s *session) WritePacket(payload []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosed
	}
	if len(payload) > s.maxDatagramSize {
		return ErrMessageTooLarge
	}
	return ErrNotReady
}

func (s *session) ReadPacket() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	return nil, ErrNotReady
}

func (s *session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	owner := s.owner
	s.owner = nil
	s.mu.Unlock()
	if owner != nil {
		owner.removeSession(s)
	}
	return nil
}

type listener struct {
	mu     sync.RWMutex
	closed bool
}

func (l *listener) Accept(ctx context.Context) (Session, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil accept context", ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, ErrNotReady
}

func (l *listener) Close() error {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	return nil
}
