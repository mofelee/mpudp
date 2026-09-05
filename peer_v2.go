package mpudp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/netip"
	"sync"
	"time"
	"unsafe"

	"github.com/mofelee/mpudp/config"
	"github.com/mofelee/mpudp/internal/aggregationv2"
	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/handshakev2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/reassemblyv2"
	"github.com/mofelee/mpudp/internal/sessionv2"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
)

const v2SocketAttemptTimeout = 20 * time.Millisecond

type v2Ingress struct {
	packet   transport.ReceivedPacket
	socketID uint64
	target   *v2Session
	err      error
}

type v2Bootstrap struct {
	binding handshakev2.Binding
	path    transport.ReplyPath
	owner   *v2Session
}

// The mutex serializes the socket-free engines, admission and public fences.
// Socket callbacks only enqueue; they never acquire this mutex.
type v2Peer struct {
	mu             sync.Mutex
	peer           *Peer
	credits        *creditv2.Peer
	creditLimit    creditv2.Limits
	engine         *handshakev2.Engine
	ingress        chan v2Ingress
	listener       *v2Listener
	sessions       map[*v2Session]struct{}
	established    map[wirev2.SessionID]*v2Session
	dials          map[handshakev2.DialID]*v2Session
	routes         map[wirev2.SessionID]v2Bootstrap
	sockets        map[uint64]*v2Session
	nextSocket     uint64
	constructing   sync.WaitGroup
	current        v2Bootstrap
	retiredReceive sessionv2.ReceiveCounters
	closed         bool
}

func newV2Peer(parent context.Context, cfg config.Config, random io.Reader, deps runtimeDependencies) (*Peer, error) {
	// Charge the bounded ingress backing, retained packets, listener read
	// buffer, accept backing and engine metadata before allocating or binding.
	runtimeBytes := uint64(cfg.Limits.ReceiveQueueCapacity+1)*(uint64(cfg.Transport.MaxReceiveUDPPayload)+uint64(unsafe.Sizeof(v2Ingress{}))+256) + 128<<10
	if cfg.ListenerEnabled() {
		runtimeBytes += uint64(cfg.Limits.MaxPendingAccepts)*uint64(unsafe.Sizeof((*v2Session)(nil))) + uint64(cfg.Transport.MaxReceiveUDPPayload+1)
	}
	limits, err := v2CreditLimits(cfg, runtimeBytes)
	if err != nil {
		return nil, err
	}
	for _, responder := range []bool{false, true} {
		if (responder && !cfg.ListenerEnabled()) || (!responder && !cfg.InitiatorEnabled()) {
			continue
		}
		controllerConfig, err := v2ControllerConfig(cfg, responder)
		if err != nil {
			return nil, err
		}
		claims, err := sessionv2.RequiredInitialClaims(controllerConfig)
		if err != nil {
			return nil, classifyRuntimeError(ErrInvalidConfig, err)
		}
		bytes := v2SessionBytes(cfg, responder) + handshakev2.PacketReservationBytes
		for _, claim := range claims {
			bytes += claim.Bytes
		}
		if bytes > limits.MaxSessionBytes || bytes > limits.MaxPeerBytes {
			return nil, ErrResourceLimit
		}
	}
	credits, err := creditv2.New(limits)
	if err != nil {
		return nil, mapV2Error(err)
	}
	ctx, cancel := context.WithCancel(parent)
	p := &Peer{config: cfg.Clone(), random: random, ctx: ctx, cancel: cancel, deps: deps,
		wake: make(chan struct{}, 1), listenerFailure: make(chan error, 1),
		diagnostics: make(chan error, 1), workerDone: make(chan struct{}), closeDone: make(chan struct{})}
	p.initStatistics()
	r := &v2Peer{peer: p, credits: credits, creditLimit: limits, ingress: make(chan v2Ingress, cfg.Limits.ReceiveQueueCapacity),
		sessions: make(map[*v2Session]struct{}), established: make(map[wirev2.SessionID]*v2Session),
		dials: make(map[handshakev2.DialID]*v2Session), routes: make(map[wirev2.SessionID]v2Bootstrap), sockets: make(map[uint64]*v2Session), nextSocket: 1}
	p.v2 = r
	engineConfig := handshakev2.Config{Credits: credits, Entropy: random, Emit: r.emitBootstrap, Install: r.install}
	if cfg.ListenerEnabled() {
		policy, err := r.policy(true)
		if err != nil {
			cancel()
			credits.Close()
			return nil, err
		}
		engineConfig.Listener = &policy
		listenerContext, listenerCancel := context.WithCancel(ctx)
		r.listener = &v2Listener{owner: r, ctx: listenerContext, cancel: listenerCancel,
			accept: make(chan *v2Session, cfg.Limits.MaxPendingAccepts), done: make(chan struct{}), changed: make(chan struct{})}
	}
	r.engine, err = handshakev2.New(cfg.PSK.Bytes(), engineConfig)
	if err != nil {
		cancel()
		credits.Close()
		return nil, mapV2Error(err)
	}
	if r.listener != nil {
		p.listenerSocket, err = deps.openListener(ctx, "udp", cfg.Listen, transport.ListenerOptions{
			PathID: "listener", MaxPayload: cfg.Transport.MaxUDPPayload, MaxReceivePayload: cfg.Transport.MaxReceiveUDPPayload,
			RequirePMTU: true, RequireDestination: true, Statistics: p.statistics.listener,
			OnPacket: func(packet transport.ReceivedPacket) { r.enqueue(v2Ingress{packet: packet, socketID: 1}) },
			OnError: func(err error) {
				if isRecoverableRuntimeTransportError(err) {
					r.enqueue(v2Ingress{err: err, socketID: 1})
				} else {
					p.enqueueListenerFailure(err)
				}
			},
		})
		if err != nil {
			cancel()
			_, _ = r.engine.Close(time.Now())
			credits.Close()
			return nil, mapV2Error(err)
		}
	}
	go r.run()
	return p, nil
}

func v2SessionBytes(cfg config.Config, responder bool) uint64 {
	bytes := uint64(unsafe.Sizeof(v2Session{})) + uint64(cfg.Limits.DeliveryQueueCapacity)*uint64(unsafe.Sizeof((*reassemblyv2.Datagram)(nil))) + 4096
	if !responder {
		bytes += uint64(len(cfg.Carriers)) * (uint64(cfg.Transport.MaxReceiveUDPPayload+1) + uint64(unsafe.Sizeof(sessionv2.Carrier{})) + 512)
	}
	return bytes
}

func (r *v2Peer) policy(responder bool) (handshakev2.Policy, error) {
	cfg, err := v2ControllerConfig(r.peer.config, responder)
	if err != nil {
		return handshakev2.Policy{}, err
	}
	initial, err := sessionv2.RequiredInitialClaims(cfg)
	return handshakev2.Policy{Profile: cfg.LocalProfile, Initial: initial,
		Receive: creditv2.Claim{Bytes: v2SessionBytes(r.peer.config, responder), PendingAccept: responder}}, mapV2Error(err)
}

func (r *v2Peer) newSession() (Session, error) {
	r.mu.Lock()
	if r.closed || r.peer.ctx.Err() != nil {
		r.mu.Unlock()
		return nil, ErrClosed
	}
	cfg := r.peer.config
	if !cfg.InitiatorEnabled() {
		r.mu.Unlock()
		return nil, ErrModeUnavailable
	}
	policy, err := r.policy(false)
	if err != nil {
		r.mu.Unlock()
		return nil, err
	}
	bytes := policy.Receive.Bytes + handshakev2.PacketReservationBytes
	for _, claim := range policy.Initial {
		bytes += claim.Bytes
	}
	usage := r.credits.Snapshot()
	if usage.SessionSlots >= r.creditLimit.MaxSessions || usage.PendingHandshakes >= r.creditLimit.MaxPendingHandshakes ||
		usage.Bytes+bytes > r.creditLimit.MaxPeerBytes || bytes > r.creditLimit.MaxSessionBytes || usage.Reservations+len(policy.Initial)+2 > r.creditLimit.MaxReservations {
		r.mu.Unlock()
		return nil, ErrResourceLimit
	}
	if uint64(len(cfg.Carriers)) > math.MaxUint64-r.nextSocket {
		r.mu.Unlock()
		return nil, ErrResourceLimit
	}
	// Reserve the entire construction obligation while sockets open outside
	// the dispatcher lock. BeginDial replaces this reservation under the lock.
	scope, lease, err := r.credits.BeginHandshake(creditv2.Claim{Bytes: bytes})
	if err != nil {
		r.mu.Unlock()
		return nil, mapV2Error(err)
	}
	s := r.newWrapper(false)
	s.startupScope, s.startupLease = scope, lease
	firstSocket := r.nextSocket + 1
	r.nextSocket += uint64(len(cfg.Carriers))
	r.constructing.Add(1)
	r.mu.Unlock()
	failed := true
	defer func() {
		r.mu.Lock()
		if failed {
			r.dispose(s)
		}
		r.mu.Unlock()
		r.constructing.Done()
	}()
	carriers := make([]handshakev2.Carrier, 0, len(cfg.Carriers))
	for index, remote := range cfg.Carriers {
		socketID := firstSocket + uint64(index)
		openContext, cancel := context.WithTimeout(s.ctx, 5*time.Second)
		carrier, err := r.peer.deps.openCarrier(openContext, fmt.Sprintf("carrier-%d", index), remote, transport.CarrierOptions{
			MaxPayload: cfg.Transport.MaxUDPPayload, MaxReceivePayload: cfg.Transport.MaxReceiveUDPPayload,
			RequirePMTU: true, Statistics: r.peer.statistics.carriers[index],
			OnPacket: func(packet transport.ReceivedPacket) {
				r.enqueue(v2Ingress{packet: packet, target: s, socketID: socketID})
			},
			OnError: func(err error) { r.enqueue(v2Ingress{target: s, socketID: socketID, err: err}) },
		})
		cancel()
		if err != nil {
			return nil, mapV2Error(err)
		}
		r.mu.Lock()
		if s.closed || r.closed || s.ctx.Err() != nil {
			r.mu.Unlock()
			_ = carrier.Close()
			return nil, ErrClosed
		}
		s.carriers = append(s.carriers, carrier)
		sender, ok := carrier.(transport.ReplyPath)
		if !ok {
			r.mu.Unlock()
			return nil, classifyRuntimeError(ErrInvalidConfig, transport.ErrInvalidArgument)
		}
		binding, err := v2Binding(socketID, sender.LocalAddr(), sender.RemoteAddr())
		if err != nil {
			r.mu.Unlock()
			return nil, err
		}
		path := handshakev2.Carrier{PathID: uint16(index + 1), Binding: binding}
		s.paths = append(s.paths, sessionv2.Carrier{Carrier: path, Sender: sender})
		r.sockets[socketID] = s
		r.mu.Unlock()
		carriers = append(carriers, path)
	}
	r.mu.Lock()
	if s.closed || r.closed || s.ctx.Err() != nil {
		r.mu.Unlock()
		return nil, ErrClosed
	}
	s.startupScope.Close()
	s.startupLease.Release()
	s.startupScope, s.startupLease = nil, nil
	id, result, err := r.engine.BeginDial(time.Now(), handshakev2.DialRequest{Policy: policy, Carriers: carriers})
	if err != nil {
		r.handleHandshake(result)
		r.mu.Unlock()
		return nil, mapV2Error(err)
	}
	s.dial = id
	r.dials[id] = s
	r.handleHandshake(result)
	failed = false
	r.mu.Unlock()
	r.peer.wakeDriver()
	return s, nil
}

func v2Binding(socketID uint64, local, remote net.Addr) (handshakev2.Binding, error) {
	toAddrPort := func(address net.Addr) (netip.AddrPort, bool) {
		udp, ok := address.(*net.UDPAddr)
		if !ok || udp == nil {
			return netip.AddrPort{}, false
		}
		value := udp.AddrPort()
		value = netip.AddrPortFrom(value.Addr().Unmap(), value.Port())
		return value, value.IsValid() && value.Port() != 0 && !value.Addr().IsUnspecified() && !value.Addr().IsMulticast()
	}
	l, lok := toAddrPort(local)
	remoteValue, rok := toAddrPort(remote)
	if socketID == 0 || !lok || !rok {
		return handshakev2.Binding{}, classifyRuntimeError(ErrInvalidConfig, transport.ErrInvalidArgument)
	}
	return handshakev2.Binding{SocketID: socketID, Local: l, Remote: remoteValue}, nil
}

func (r *v2Peer) emit(path transport.ReplyPath, packet []byte) error {
	return r.emitContext(r.peer.ctx, path, packet)
}

func (r *v2Peer) emitContext(parent context.Context, path transport.ReplyPath, packet []byte) error {
	if path == nil {
		return ErrNoAvailablePaths
	}
	ctx, cancel := context.WithTimeout(parent, v2SocketAttemptTimeout)
	defer cancel()
	return path.Send(ctx, packet)
}

func (r *v2Peer) emitBootstrap(binding handshakev2.Binding, packet []byte) error {
	envelope, err := wirev2.ParseEnvelope(packet)
	if err != nil {
		return err
	}
	header := envelope.Header()
	route, found := r.routes[header.SessionID]
	if !found {
		if r.current.binding == binding {
			route = r.current
		} else if owner := r.sockets[binding.SocketID]; owner != nil {
			for _, carrier := range owner.paths {
				if carrier.Binding == binding {
					route = v2Bootstrap{binding: binding, path: carrier.Sender, owner: owner}
					break
				}
			}
		}
		// Only Engine-admitted HELLO/CHALLENGE creates a retry route. Rejected
		// or unauthenticated ingress never retains its captured reply handle.
		if route.path != nil && (header.Type == wirev2.TypeHello || header.Type == wirev2.TypeChallenge) {
			r.routes[header.SessionID] = route
		}
	}
	if route.path == nil || route.binding != binding {
		return ErrNoAvailablePaths
	}
	if route.owner != nil && header.Type != wirev2.TypeClose {
		return r.emitContext(route.owner.ctx, route.path, packet)
	}
	if binding.SocketID == 1 && r.listener != nil && header.Type != wirev2.TypeClose {
		return r.emitContext(r.listener.ctx, route.path, packet)
	}
	return r.emit(route.path, packet)
}

func (r *v2Peer) install(setup handshakev2.Setup) (func(), error) {
	responder := setup.Role == negotiationv2.Responder
	var s *v2Session
	if responder {
		if r.listener == nil || r.listener.closed || r.listener.ctx.Err() != nil {
			return nil, ErrClosed
		}
		s = r.newWrapper(true)
	} else {
		s = r.dials[setup.DialID]
		if s == nil || s.closed {
			return nil, ErrClosed
		}
	}
	var failedInstall func()
	if responder {
		failedInstall = func() { r.dispose(s) }
	}
	cfg, err := v2ControllerConfig(r.peer.config, responder)
	if err != nil {
		return failedInstall, err
	}
	route := r.routes[setup.ID]
	cfg.LocalProfile, _, err = setup.Contract.Profiles(setup.Role)
	if err != nil {
		return failedInstall, err
	}
	cfg.BootstrapPath, cfg.Carriers, cfg.Entropy = route.path, s.paths, r.peer.random
	cfg.Emit = func(path transport.ReplyPath, packet []byte) error {
		return r.emitContext(s.ctx, path, packet)
	}
	controller, err := sessionv2.New(setup, cfg)
	if err != nil {
		return failedInstall, err
	}
	s.id, s.controller = setup.ID, controller
	r.established[setup.ID] = s
	return func() { r.dispose(s) }, nil
}

func (r *v2Peer) handleHandshake(result handshakev2.Result) {
	for _, attempt := range result.Sends {
		r.report("MPUDP v2 handshake send failed", attempt.Err)
		if attempt.Type == wirev2.TypeHello && attempt.Err != nil && !isRecoverableRuntimeTransportError(attempt.Err) {
			if route, ok := r.routes[attempt.ID]; ok && route.owner != nil {
				r.failCarrier(route.owner, route.binding.SocketID)
			}
		}
	}
	for _, setup := range result.Established {
		s := r.established[setup.ID]
		if s == nil || s.closed {
			continue
		}
		started, err := s.controller.Start(time.Now())
		r.handleSession(s, started, err)
		if s.inbound && !s.closed {
			select {
			case r.listener.accept <- s:
				close(r.listener.changed)
				r.listener.changed = make(chan struct{})
			default:
				r.closeSession(s)
			}
		}
	}
	for _, id := range result.Closed {
		delete(r.routes, id)
	}
	for _, failure := range result.Failures {
		delete(r.routes, failure.ID)
		r.report("MPUDP v2 handshake failed", failure.Err)
		if s := r.dials[failure.DialID]; s != nil && s.controller == nil {
			pending := false
			for _, route := range r.routes {
				if route.owner == s {
					pending = true
					break
				}
			}
			if !pending {
				r.dispose(s)
			}
		}
	}
}

func (r *v2Peer) handleSession(s *v2Session, result sessionv2.Result, err error) {
	for _, packet := range result.Deliveries {
		if s.closed {
			packet.Release()
			continue
		}
		select {
		case s.delivery <- packet:
			r.peer.statistics.deliveryAccepted.Add(1)
		default:
			packet.Release()
			r.peer.statistics.deliveryDrops.Add(1)
		}
	}
	for _, attempt := range result.Sends {
		r.report("MPUDP v2 send failed", attempt.Err)
	}
	r.report("MPUDP v2 Session operation failed", err)
	s.notify()
	if errors.Is(err, sessionv2.ErrExpired) || errors.Is(err, sessionv2.ErrExhausted) || errors.Is(err, sessionv2.ErrEntropy) || errors.Is(err, transport.ErrNoAvailablePaths) {
		s.terminalErr = mapV2Error(err)
		r.closeSession(s)
	}
}

func (r *v2Peer) report(operation string, err error) {
	if err != nil {
		r.peer.reportRuntimeError(operation, mapV2Error(err))
	}
}

func mapV2Error(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, creditv2.ErrResourceLimit):
		return classifyRuntimeError(ErrResourceLimit, err)
	case errors.Is(err, sessionv2.ErrNotReady):
		return classifyRuntimeError(ErrNotReady, err)
	case errors.Is(err, sessionv2.ErrClosed), errors.Is(err, creditv2.ErrClosed), errors.Is(err, handshakev2.ErrClosed):
		return classifyRuntimeError(ErrClosed, err)
	case errors.Is(err, aggregationv2.ErrMessageTooLarge):
		return classifyRuntimeError(ErrMessageTooLarge, err)
	case errors.Is(err, wirev2.ErrAuthentication):
		return classifyRuntimeError(ErrAuthentication, err)
	case errors.Is(err, negotiationv2.ErrIncompatible), errors.Is(err, handshakev2.ErrRejected):
		return classifyRuntimeError(ErrHandshakeIncompatible, err)
	default:
		return mapRuntimeError(err)
	}
}
