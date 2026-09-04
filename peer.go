package mpudp

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"io"
	"sync"

	"github.com/mofelee/mpudp/config"
	internalsession "github.com/mofelee/mpudp/internal/session"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wire"
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
// specified. Close is idempotent and unblocks ReadPacket.
type Session interface {
	WritePacket(payload []byte) error
	ReadPacket() ([]byte, error)
	Close() error
}

// Listener accepts authenticated inbound Sessions. Accept is context-aware
// and safe for concurrent use. Close is idempotent, stops inbound admission,
// closes accepted Sessions, and unblocks pending Accept calls.
type Listener interface {
	Accept(ctx context.Context) (Session, error)
	Close() error
}

// Peer owns all runtime sockets, Sessions, bounded queues, and the single
// logical-deadline driver for one validated configuration.
type Peer struct {
	mu       sync.RWMutex
	config   config.Config
	random   io.Reader
	closed   bool
	sessions map[*session]struct{}
	inbound  map[wire.SessionID]*session
	outbound map[wire.SessionID]*session
	listener *listener

	ctx     context.Context
	cancel  context.CancelFunc
	ingress chan ingressEvent
	// listenerFailure is a one-shot terminal-error latch kept separate from
	// packet ingress so a full packet queue cannot hide a dead listener socket.
	listenerFailure chan error
	wake            chan struct{}
	diagnostics     chan error
	workerDone      chan struct{}

	listenerState  *internalsession.Listener
	listenerSocket runtimePacketListener
	deps           runtimeDependencies

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

// NewPeer validates cfg, binds the configured listener when present, and
// starts one bounded runtime dispatcher. Invalid configuration has no socket,
// goroutine, or timer side effects.
func NewPeer(cfg config.Config) (*Peer, error) {
	return newPeerWithContextAndDependencies(context.Background(), cfg, cryptorand.Reader, defaultRuntimeDependencies())
}

// NewPeerContext is NewPeer with a context that can cancel listener binding,
// Carrier startup, and runtime operations. Callers must still call Close to
// synchronously release sockets and wait for background work.
func NewPeerContext(ctx context.Context, cfg config.Config) (*Peer, error) {
	return newPeerWithContextAndDependencies(ctx, cfg, cryptorand.Reader, defaultRuntimeDependencies())
}

func newPeer(cfg config.Config, random io.Reader) (*Peer, error) {
	return newPeerWithContextAndDependencies(context.Background(), cfg, random, defaultRuntimeDependencies())
}

func newPeerWithDependencies(cfg config.Config, random io.Reader, deps runtimeDependencies) (*Peer, error) {
	return newPeerWithContextAndDependencies(context.Background(), cfg, random, deps)
}

func newPeerWithContextAndDependencies(parent context.Context, cfg config.Config, random io.Reader, deps runtimeDependencies) (*Peer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if parent == nil {
		return nil, fmt.Errorf("%w: nil Peer context", ErrInvalidConfig)
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if random == nil {
		return nil, fmt.Errorf("%w: random reader must not be nil", ErrInvalidConfig)
	}
	if err := deps.validate(); err != nil {
		return nil, err
	}

	runtimeContext, cancel := context.WithCancel(parent)
	p := &Peer{
		config:          cfg.Clone(),
		random:          random,
		sessions:        make(map[*session]struct{}),
		inbound:         make(map[wire.SessionID]*session),
		outbound:        make(map[wire.SessionID]*session),
		ctx:             runtimeContext,
		cancel:          cancel,
		ingress:         make(chan ingressEvent, cfg.Limits.ReceiveQueueCapacity),
		listenerFailure: make(chan error, 1),
		wake:            make(chan struct{}, 1),
		diagnostics:     make(chan error, 1),
		workerDone:      make(chan struct{}),
		deps:            deps,
		closeDone:       make(chan struct{}),
	}

	if cfg.ListenerEnabled() {
		state, err := internalsession.NewListener(internalsession.ListenerConfig{
			Session:     mapSessionConfig(cfg),
			MaxSessions: cfg.Limits.MaxSessions,
		})
		if err != nil {
			cancel()
			return nil, mapRuntimeError(err)
		}
		publicListener := &listener{
			owner:  p,
			accept: make(chan Session, cfg.Limits.ReceiveQueueCapacity),
			done:   make(chan struct{}),
		}
		p.listenerState = state
		p.listener = publicListener
		socket, err := deps.openListener(runtimeContext, "udp", cfg.Listen, transport.ListenerOptions{
			PathID:     "listener",
			MaxPayload: cfg.Transport.MaxUDPPayload,
			OnPacket: func(packet transport.ReceivedPacket) {
				p.enqueue(ingressEvent{packet: packet, listener: true})
			},
			OnError: func(err error) {
				if isRecoverableRuntimeTransportError(err) {
					p.enqueue(ingressEvent{listener: true, transportErr: err})
					return
				}
				p.enqueueListenerFailure(err)
			},
		})
		if err != nil {
			cancel()
			_ = state.Close(context.Background())
			return nil, mapRuntimeError(err)
		}
		p.listenerSocket = socket
	}

	go p.run()
	return p, nil
}

// Mode reports the roles enabled by the validated configuration.
func (p *Peer) Mode() Mode {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return modeOf(p.config)
}

// Config returns a deep copy of the validated configuration.
func (p *Peer) Config() config.Config {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.config.Clone()
}

// NewSession opens one long-lived Carrier per configured remote, starts the
// authenticated initiator handshake, and returns immediately. WritePacket
// reports ErrNotReady until a compatible listener acknowledges the Session.
func (p *Peer) NewSession() (Session, error) {
	return p.newInitiatorSession()
}

// Listener returns the single active listener handle for this Peer.
func (p *Peer) Listener() (Listener, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrClosed
	}
	publicListener := p.listener
	p.mu.RUnlock()
	if publicListener == nil {
		return nil, ErrModeUnavailable
	}
	select {
	case <-publicListener.done:
		return nil, ErrClosed
	default:
		return publicListener, nil
	}
}

// Errors returns the bounded asynchronous diagnostic stream. Producers never
// block; when the single slot is full, the newest diagnostic is dropped. The
// channel is not closed by Peer.Close, so consumers should select with their
// own lifecycle context.
func (p *Peer) Errors() <-chan error { return p.diagnostics }

// Close idempotently prevents new work, closes all Sessions and sockets, wakes
// blocked reads and accepts, and waits for the dispatcher to exit. Concurrent
// callers receive the same aggregate close result.
func (p *Peer) Close() error {
	p.closeOnce.Do(p.close)
	<-p.closeDone
	return p.closeErr
}

// String reports bounded lifecycle metadata without formatting configuration
// secrets, Session IDs, packet contents, or transport error causes.
func (p *Peer) String() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return fmt.Sprintf("Peer{mode:%s sessions:%d closed:%t}", modeOf(p.config), len(p.sessions), p.closed)
}

func (p *Peer) GoString() string { return p.String() }

func (p *Peer) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, p.String()) }

func modeOf(cfg config.Config) Mode {
	if cfg.InitiatorEnabled() && cfg.ListenerEnabled() {
		return ModeDual
	}
	if cfg.InitiatorEnabled() {
		return ModeInitiator
	}
	return ModeListener
}
