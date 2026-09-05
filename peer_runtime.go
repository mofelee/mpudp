package mpudp

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/mofelee/mpudp/config"
	"github.com/mofelee/mpudp/internal/fec"
	internalsession "github.com/mofelee/mpudp/internal/session"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wire"
)

const runtimeCloseTimeout = time.Second

type runtimeCarrier interface {
	internalsession.Path
	Close() error
}

type runtimePacketListener interface {
	Close() error
	LocalAddr() net.Addr
}

type runtimeDeadlineTimer interface {
	Channel() <-chan time.Time
	Reset(time.Duration) bool
	Stop() bool
}

type realDeadlineTimer struct{ timer *time.Timer }

func (t realDeadlineTimer) Channel() <-chan time.Time { return t.timer.C }
func (t realDeadlineTimer) Reset(delay time.Duration) bool {
	return t.timer.Reset(delay)
}
func (t realDeadlineTimer) Stop() bool { return t.timer.Stop() }

type runtimeDependencies struct {
	openCarrier  func(context.Context, string, string, transport.CarrierOptions) (runtimeCarrier, error)
	openListener func(context.Context, string, string, transport.ListenerOptions) (runtimePacketListener, error)
	newTimer     func() runtimeDeadlineTimer
}

func defaultRuntimeDependencies() runtimeDependencies {
	return runtimeDependencies{
		openCarrier: func(ctx context.Context, id, remote string, options transport.CarrierOptions) (runtimeCarrier, error) {
			return transport.OpenCarrier(ctx, id, remote, options)
		},
		openListener: func(ctx context.Context, network, address string, options transport.ListenerOptions) (runtimePacketListener, error) {
			return transport.OpenListener(ctx, network, address, options)
		},
		newTimer: func() runtimeDeadlineTimer {
			return realDeadlineTimer{timer: time.NewTimer(time.Hour)}
		},
	}
}

func (d runtimeDependencies) validate() error {
	if d.openCarrier == nil || d.openListener == nil || d.newTimer == nil {
		return fmt.Errorf("%w: incomplete runtime dependencies", ErrInvalidConfig)
	}
	return nil
}

type ingressEvent struct {
	packet       transport.ReceivedPacket
	target       *session
	listener     bool
	transportErr error
	queuedAt     time.Time
}

// session wraps the socket-free state machine with bounded delivery and public
// lifecycle state. initDone lets Peer.Close safely join partial construction.
type session struct {
	mu         sync.Mutex
	active     sync.WaitGroup
	id         SessionID
	wireID     wire.SessionID
	owner      *Peer
	inbound    bool
	controller *internalsession.Session
	carriers   []runtimeCarrier
	delivery   chan []byte
	done       chan struct{}
	doneOnce   sync.Once
	initDone   chan struct{}
	closed     bool
	closeOnce  sync.Once
	closeErr   error
}

type listener struct {
	owner     *Peer
	accept    chan Session
	done      chan struct{}
	doneOnce  sync.Once
	closeOnce sync.Once
	closeErr  error
}

func mapSessionConfig(cfg config.Config) internalsession.Config {
	return internalsession.Config{
		PSK:                       cfg.PSK.Bytes(),
		FEC:                       fec.Params{DataShards: cfg.FEC.DataShards, ParityShards: cfg.FEC.ParityShards},
		LocalMaxUDPPayload:        cfg.Transport.MaxUDPPayload,
		MaxDatagramSize:           cfg.Limits.MaxDatagramSize,
		MaxEndpoints:              cfg.Limits.MaxEndpointsPerSession,
		MaxHandshakeAttempts:      cfg.Limits.MaxHandshakeAttempts,
		MaxPendingFECBlocks:       cfg.Limits.MaxPendingFECBlocks,
		MaxCompletedFECBlocks:     cfg.Limits.MaxPendingFECBlocks,
		DecodeTimeout:             cfg.Timers.DecodeTimeout,
		CompletionTTL:             cfg.Timers.EndpointTTL,
		EndpointTTL:               cfg.Timers.EndpointTTL,
		KeepaliveInterval:         cfg.Timers.KeepaliveInterval,
		HandshakeRetryInterval:    cfg.Timers.HandshakeRetryInterval,
		HandshakeRetryJitterLimit: 0,
	}
}

func publicSessionID(id wire.SessionID) SessionID { return SessionID(id) }

func internalSessionID(id SessionID) wire.SessionID { return wire.SessionID(id) }

func (p *Peer) newInitiatorSession() (Session, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrClosed
	}
	if !p.config.InitiatorEnabled() {
		p.mu.Unlock()
		return nil, ErrModeUnavailable
	}
	if len(p.sessions) >= p.config.Limits.MaxSessions {
		p.mu.Unlock()
		return nil, ErrResourceLimit
	}

	var id SessionID
	var wireID wire.SessionID
	for attempt := 0; attempt < maxSessionIDAttempts; attempt++ {
		candidate, err := newSessionID(p.random)
		if err != nil {
			p.mu.Unlock()
			return nil, err
		}
		candidateWire := internalSessionID(candidate)
		if _, exists := p.outbound[candidateWire]; !exists {
			id, wireID = candidate, candidateWire
			break
		}
	}
	if id == (SessionID{}) {
		p.mu.Unlock()
		return nil, fmt.Errorf("generate MPUDP session ID: random source returned repeated identifiers")
	}

	s := &session{
		id:       id,
		wireID:   wireID,
		owner:    p,
		delivery: make(chan []byte, p.config.Limits.DeliveryQueueCapacity),
		done:     make(chan struct{}),
		initDone: make(chan struct{}),
	}
	p.sessions[s] = struct{}{}
	p.outbound[wireID] = s
	carrierAddresses := append([]string(nil), p.config.Carriers...)
	maxPayload := p.config.Transport.MaxUDPPayload
	stateConfig := mapSessionConfig(p.config)
	stateConfig.FECStatistics = &p.statistics.fec
	p.mu.Unlock()

	succeeded := false
	defer func() {
		close(s.initDone)
		if !succeeded {
			s.failConstruction()
		}
	}()

	paths := make([]internalsession.Path, 0, len(carrierAddresses))
	for index, remote := range carrierAddresses {
		pathID := fmt.Sprintf("carrier-%d", index)
		carrier, err := p.deps.openCarrier(p.ctx, pathID, remote, transport.CarrierOptions{
			MaxPayload: maxPayload,
			Statistics: p.statistics.carriers[index],
			OnPacket: func(packet transport.ReceivedPacket) {
				p.enqueue(ingressEvent{packet: packet, target: s})
			},
			OnError: func(err error) {
				p.enqueue(ingressEvent{target: s, transportErr: err})
			},
		})
		if err != nil {
			return nil, mapRuntimeError(err)
		}
		if !s.addCarrier(carrier) {
			_ = carrier.Close()
			return nil, ErrClosed
		}
		paths = append(paths, carrier)
	}

	controller, err := internalsession.NewInitiator(wireID, stateConfig, paths)
	if err != nil {
		return nil, mapRuntimeError(err)
	}
	if !s.setController(controller) {
		closeContext, cancel := context.WithTimeout(context.Background(), runtimeCloseTimeout)
		_ = controller.Close(closeContext)
		cancel()
		return nil, ErrClosed
	}
	attempts, err := controller.Start(p.ctx)
	for _, attempt := range attempts {
		if attempt.Err != nil {
			p.reportRuntimeError("MPUDP handshake send failed", attempt.Err)
		}
	}
	if err != nil {
		return nil, mapRuntimeError(err)
	}
	succeeded = true
	p.wakeDriver()
	return s, nil
}

func (s *session) addCarrier(carrier runtimeCarrier) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.carriers = append(s.carriers, carrier)
	return true
}

func (s *session) setController(controller *internalsession.Session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.controller = controller
	return true
}

func (s *session) failConstruction() {
	s.markClosed()
	s.mu.Lock()
	controller := s.controller
	carriers := append([]runtimeCarrier(nil), s.carriers...)
	owner := s.owner
	s.owner = nil
	s.mu.Unlock()
	if controller != nil {
		closeContext, cancel := context.WithTimeout(context.Background(), runtimeCloseTimeout)
		_ = controller.Close(closeContext)
		cancel()
	}
	for _, carrier := range carriers {
		_ = carrier.Close()
	}
	if owner != nil {
		owner.removeSession(s)
	}
}

func (s *session) beginOperation() (*internalsession.Session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.controller == nil {
		return nil, false
	}
	s.active.Add(1)
	return s.controller, true
}

func (s *session) endOperation() { s.active.Done() }

func (s *session) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	fingerprint := sha256.Sum256(s.wireID[:])
	role := "initiator"
	if s.inbound {
		role = "listener"
	}
	return fmt.Sprintf("Session{%x role:%s closed:%t}", fingerprint[:6], role, s.closed)
}

func (s *session) GoString() string { return s.String() }

func (s *session) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, s.String()) }

func (s *session) WritePacket(payload []byte) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	owner := s.owner
	if owner != nil && len(payload) > owner.config.Limits.MaxDatagramSize {
		s.mu.Unlock()
		return ErrMessageTooLarge
	}
	if s.controller == nil {
		s.mu.Unlock()
		return ErrNotReady
	}
	controller := s.controller
	s.active.Add(1)
	s.mu.Unlock()
	defer s.active.Done()

	var sendStarted time.Time
	if owner != nil && owner.statistics.enabled.Load() {
		sendStarted = time.Now()
	}
	_, err := controller.WritePacket(context.Background(), payload)
	if owner != nil {
		if err == nil {
			owner.statistics.sentDatagrams.Add(1)
			owner.statistics.sentDatagramBytes.Add(uint64(len(payload)))
		}
		if !sendStarted.IsZero() {
			owner.statistics.sendLatency.Observe(time.Since(sendStarted))
		}
		owner.wakeDriver()
	}
	return mapRuntimeError(err)
}

func (s *session) ReadPacket() ([]byte, error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, ErrClosed
		}
		delivery := s.delivery
		done := s.done
		owner := s.owner
		s.mu.Unlock()

		select {
		case packet := <-delivery:
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil, ErrClosed
			}
			if owner != nil {
				owner.statistics.deliveredPackets.Add(1)
				owner.statistics.deliveredBytes.Add(uint64(len(packet)))
			}
			return packet, nil
		case <-done:
			return nil, ErrClosed
		}
	}
}

func (s *session) Close() error {
	closeContext, cancel := context.WithTimeout(context.Background(), runtimeCloseTimeout)
	defer cancel()
	return s.closeWithContext(closeContext)
}

func (s *session) closeWithContext(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.markClosed()
		var failures []error
		select {
		case <-s.initDone:
		case <-ctx.Done():
			failures = append(failures, ctx.Err())
		}

		s.mu.Lock()
		controller := s.controller
		carriers := append([]runtimeCarrier(nil), s.carriers...)
		owner := s.owner
		s.owner = nil
		s.mu.Unlock()

		if controller != nil {
			if err := controller.Close(ctx); err != nil {
				failures = append(failures, err)
			}
		}
		s.active.Wait()
		for _, carrier := range carriers {
			if err := carrier.Close(); err != nil {
				failures = append(failures, err)
			}
		}
		if owner != nil {
			owner.removeSession(s)
		}
		s.closeErr = mapRuntimeError(errors.Join(failures...))
	})
	return s.closeErr
}

func (s *session) markClosed() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.doneOnce.Do(func() { close(s.done) })
	}
	s.mu.Unlock()
}

func (s *session) deliver(packet []byte) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	delivery := s.delivery
	owner := s.owner
	s.mu.Unlock()
	select {
	case delivery <- packet:
		if owner != nil {
			owner.statistics.deliveryAccepted.Add(1)
		}
	default:
		// Deterministic drop-newest keeps a slow consumer bounded.
		if owner != nil {
			owner.statistics.deliveryDrops.Add(1)
		}
	}
}

func (l *listener) Accept(ctx context.Context) (Session, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil accept context", ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-l.done:
		return nil, ErrClosed
	default:
	}
	select {
	case accepted := <-l.accept:
		select {
		case <-l.done:
			return nil, ErrClosed
		default:
			return accepted, nil
		}
	case <-l.done:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *listener) offer(accepted Session) bool {
	select {
	case <-l.done:
		return false
	default:
	}
	select {
	case l.accept <- accepted:
		return true
	default:
		return false
	}
}

func (l *listener) signalClosed() { l.doneOnce.Do(func() { close(l.done) }) }

func (l *listener) String() string {
	closed := false
	select {
	case <-l.done:
		closed = true
	default:
	}
	return fmt.Sprintf("Listener{queued:%d closed:%t}", len(l.accept), closed)
}

func (l *listener) GoString() string { return l.String() }

func (l *listener) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, l.String()) }

func (l *listener) Close() error {
	l.closeOnce.Do(func() {
		l.signalClosed()
		closeContext, cancel := context.WithTimeout(context.Background(), runtimeCloseTimeout)
		defer cancel()
		l.closeErr = l.owner.closeListener(closeContext)
	})
	return l.closeErr
}

func (p *Peer) enqueue(event ingressEvent) {
	select {
	case <-p.ctx.Done():
		return
	default:
	}
	if event.transportErr == nil && p.statistics.enabled.Load() {
		event.queuedAt = time.Now()
	}
	select {
	case p.ingress <- event:
		if event.transportErr == nil {
			p.statistics.ingressAccepted.Add(1)
		}
	default:
		// Transport callbacks never block. Full ingress drops the newest packet.
		if event.transportErr == nil {
			p.statistics.ingressDrops.Add(1)
		}
	}
}

func (p *Peer) enqueueListenerFailure(err error) {
	if err == nil {
		return
	}
	select {
	case <-p.ctx.Done():
		return
	default:
	}
	select {
	case p.listenerFailure <- err:
	default:
		// A real listener has one terminal read failure. Retain the first if an
		// injected transport reports more than one.
	}
}

func (p *Peer) wakeDriver() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

type runtimeDiagnostic struct {
	operation string
	cause     error
}

func (e *runtimeDiagnostic) Error() string { return e.operation }

func (e *runtimeDiagnostic) Unwrap() error { return e.cause }

func (p *Peer) reportRuntimeError(operation string, err error) {
	if err == nil {
		return
	}
	p.mu.RLock()
	closed := p.closed
	p.mu.RUnlock()
	if closed || (errors.Is(err, context.Canceled) && p.ctx != nil && p.ctx.Err() != nil) {
		return
	}
	diagnostic := &runtimeDiagnostic{operation: operation, cause: mapRuntimeError(err)}
	select {
	case p.diagnostics <- diagnostic:
	default:
	}
}

func (p *Peer) run() {
	defer close(p.workerDone)
	timer := p.deps.newTimer()
	if !timer.Stop() {
		<-timer.Channel()
	}
	defer timer.Stop()

	for {
		select {
		case err := <-p.listenerFailure:
			p.handleIngress(ingressEvent{listener: true, transportErr: err})
			continue
		default:
		}

		deadline := p.nextDeadline()
		var timerC <-chan time.Time
		if !deadline.IsZero() {
			delay := time.Until(deadline)
			if delay < 0 {
				delay = 0
			}
			timer.Reset(delay)
			timerC = timer.Channel()
		}

		select {
		case <-p.ctx.Done():
			return
		case err := <-p.listenerFailure:
			p.handleIngress(ingressEvent{listener: true, transportErr: err})
		case event := <-p.ingress:
			p.handleIngress(event)
		case <-p.wake:
		case <-timerC:
			p.advanceSessions()
		}

		if timerC != nil && !timer.Stop() {
			select {
			case <-timer.Channel():
			default:
			}
		}
	}
}

func (p *Peer) handleIngress(event ingressEvent) {
	if !event.queuedAt.IsZero() {
		p.statistics.ingressQueue.Observe(time.Since(event.queuedAt))
	}
	if event.transportErr != nil {
		p.reportRuntimeError("MPUDP transport failed", event.transportErr)
		if isRecoverableRuntimeTransportError(event.transportErr) {
			return
		}
		if event.listener {
			if p.listener != nil {
				_ = p.listener.Close()
			}
			return
		}
		if event.target != nil {
			controller, ok := event.target.beginOperation()
			if ok {
				var pathError *transport.PathError
				if errors.As(event.transportErr, &pathError) {
					_ = controller.SetPathHealthy(pathError.PathID, false)
				}
				event.target.endOperation()
			}
		}
		return
	}
	if event.listener {
		p.handleListenerPacket(event.packet)
		return
	}
	if event.target == nil {
		return
	}
	controller, ok := event.target.beginOperation()
	if !ok {
		return
	}
	result, err := controller.HandleTransportPacket(p.ctx, event.packet)
	event.target.endOperation()
	p.reportRuntimeError("MPUDP packet rejected", err)
	if result.Response != nil {
		p.reportRuntimeError("MPUDP control response failed", result.Response.Err)
	}
	if result.Datagram != nil {
		event.target.deliver(result.Datagram)
	}
	if controller.Snapshot().State == internalsession.StateClosed {
		closeContext, cancel := context.WithTimeout(context.Background(), runtimeCloseTimeout)
		_ = event.target.closeWithContext(closeContext)
		cancel()
	}
	p.wakeDriver()
}

func isRecoverableRuntimeTransportError(err error) bool {
	if errors.Is(err, transport.ErrPayloadTooLarge) || errors.Is(err, transport.ErrInvalidArgument) {
		return true
	}
	var networkError net.Error
	return errors.As(err, &networkError) && networkError.Timeout()
}

func (p *Peer) handleListenerPacket(packet transport.ReceivedPacket) {
	p.mu.RLock()
	state := p.listenerState
	publicListener := p.listener
	closed := p.closed
	p.mu.RUnlock()
	if state == nil || publicListener == nil || closed {
		return
	}

	controller, result, err := state.HandleTransportPacket(p.ctx, packet)
	p.reportRuntimeError("MPUDP listener packet rejected", err)
	if controller == nil {
		return
	}
	if result.Response != nil {
		p.reportRuntimeError("MPUDP listener response failed", result.Response.Err)
	}
	wireID := controller.ID()
	p.mu.RLock()
	wrapper := p.inbound[wireID]
	p.mu.RUnlock()
	if result.Created {
		wrapper = p.addInboundSession(controller)
		if wrapper == nil {
			closeContext, cancel := context.WithTimeout(context.Background(), runtimeCloseTimeout)
			_ = controller.Close(closeContext)
			cancel()
			return
		}
		if !publicListener.offer(wrapper) {
			closeContext, cancel := context.WithTimeout(context.Background(), runtimeCloseTimeout)
			_ = wrapper.closeWithContext(closeContext)
			cancel()
			return
		}
	}
	if wrapper == nil {
		return
	}
	if result.Datagram != nil {
		wrapper.deliver(result.Datagram)
	}
	if controller.Snapshot().State == internalsession.StateClosed {
		closeContext, cancel := context.WithTimeout(context.Background(), runtimeCloseTimeout)
		_ = wrapper.closeWithContext(closeContext)
		cancel()
	}
	p.wakeDriver()
}

func (p *Peer) addInboundSession(controller *internalsession.Session) *session {
	id := controller.ID()
	publicID := publicSessionID(id)
	initDone := make(chan struct{})
	close(initDone)
	wrapper := &session{
		id:         publicID,
		wireID:     id,
		owner:      p,
		inbound:    true,
		controller: controller,
		delivery:   make(chan []byte, p.config.Limits.DeliveryQueueCapacity),
		done:       make(chan struct{}),
		initDone:   initDone,
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.listener == nil || len(p.sessions) >= p.config.Limits.MaxSessions {
		return nil
	}
	select {
	case <-p.listener.done:
		return nil
	default:
	}
	if existing := p.inbound[id]; existing != nil {
		return existing
	}
	p.sessions[wrapper] = struct{}{}
	p.inbound[id] = wrapper
	return wrapper
}

func (p *Peer) removeSession(s *session) {
	p.mu.Lock()
	delete(p.sessions, s)
	if s.inbound {
		if p.inbound[s.wireID] == s {
			delete(p.inbound, s.wireID)
		}
	} else if p.outbound[s.wireID] == s {
		delete(p.outbound, s.wireID)
	}
	p.mu.Unlock()
	p.wakeDriver()
}

func (p *Peer) snapshotSessions() []*session {
	p.mu.RLock()
	sessions := make([]*session, 0, len(p.sessions))
	for current := range p.sessions {
		sessions = append(sessions, current)
	}
	p.mu.RUnlock()
	sort.Slice(sessions, func(i, j int) bool {
		left, right := string(sessions[i].wireID[:]), string(sessions[j].wireID[:])
		if left == right {
			return !sessions[i].inbound && sessions[j].inbound
		}
		return left < right
	})
	return sessions
}

func (p *Peer) nextDeadline() time.Time {
	var next time.Time
	for _, current := range p.snapshotSessions() {
		controller, ok := current.beginOperation()
		if !ok {
			continue
		}
		candidate := controller.NextDeadline()
		current.endOperation()
		if !candidate.IsZero() && (next.IsZero() || candidate.Before(next)) {
			next = candidate
		}
	}
	return next
}

func (p *Peer) advanceSessions() {
	for _, current := range p.snapshotSessions() {
		controller, ok := current.beginOperation()
		if !ok {
			continue
		}
		result, err := controller.Advance(p.ctx)
		state := controller.Snapshot().State
		current.endOperation()
		for _, attempt := range result.Sends {
			p.reportRuntimeError("MPUDP deadline send failed", attempt.Err)
		}
		p.reportRuntimeError("MPUDP Session advance failed", err)
		if errors.Is(err, internalsession.ErrHandshakeFailed) || state == internalsession.StateClosed {
			closeContext, cancel := context.WithTimeout(context.Background(), runtimeCloseTimeout)
			_ = current.closeWithContext(closeContext)
			cancel()
		}
	}
}

func (p *Peer) closeListener(ctx context.Context) error {
	p.mu.RLock()
	state := p.listenerState
	socket := p.listenerSocket
	inbound := make([]*session, 0, len(p.inbound))
	for _, current := range p.inbound {
		inbound = append(inbound, current)
	}
	p.mu.RUnlock()

	var failures []error
	for _, current := range inbound {
		if err := current.closeWithContext(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	if state != nil {
		if err := state.Close(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	if socket != nil {
		if err := socket.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	return mapRuntimeError(errors.Join(failures...))
}

func (p *Peer) close() {
	if p.v2 != nil {
		p.v2.closePeer()
		return
	}
	p.mu.Lock()
	p.closed = true
	publicListener := p.listener
	sessions := make([]*session, 0, len(p.sessions))
	for current := range p.sessions {
		sessions = append(sessions, current)
	}
	p.mu.Unlock()

	if publicListener != nil {
		publicListener.signalClosed()
	}
	p.cancel()
	closeContext, cancel := context.WithTimeout(context.Background(), runtimeCloseTimeout)
	defer cancel()

	var failures []error
	for _, current := range sessions {
		if err := current.closeWithContext(closeContext); err != nil {
			failures = append(failures, err)
		}
	}
	if publicListener != nil {
		if err := publicListener.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	<-p.workerDone
	p.closeErr = mapRuntimeError(errors.Join(failures...))
	close(p.closeDone)
}

type classifiedRuntimeError struct {
	kind  error
	cause error
}

func (e *classifiedRuntimeError) Error() string { return e.kind.Error() }

func (e *classifiedRuntimeError) Unwrap() []error { return []error{e.kind, e.cause} }

func classifyRuntimeError(kind, cause error) error {
	if cause == nil {
		return nil
	}
	if errors.Is(cause, kind) {
		return cause
	}
	return &classifiedRuntimeError{kind: kind, cause: cause}
}

func mapRuntimeError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, internalsession.ErrClosed), errors.Is(err, transport.ErrClosed), errors.Is(err, fec.ErrClosed):
		return classifyRuntimeError(ErrClosed, err)
	case errors.Is(err, fec.ErrMessageTooLarge), errors.Is(err, transport.ErrPayloadTooLarge), errors.Is(err, wire.ErrPacketTooLarge):
		return classifyRuntimeError(ErrMessageTooLarge, err)
	case errors.Is(err, transport.ErrNoAvailablePaths):
		return classifyRuntimeError(ErrNoAvailablePaths, err)
	case errors.Is(err, transport.ErrAllSendsFailed):
		if errors.Is(err, transport.ErrPathMTUExceeded) {
			err = classifyRuntimeError(ErrPathMTUExceeded, err)
		}
		return classifyRuntimeError(ErrAllSendsFailed, err)
	case errors.Is(err, transport.ErrPartialSend):
		if errors.Is(err, transport.ErrPathMTUExceeded) {
			err = classifyRuntimeError(ErrPathMTUExceeded, err)
		}
		return classifyRuntimeError(ErrPartialSend, err)
	case errors.Is(err, transport.ErrPathMTUExceeded):
		return classifyRuntimeError(ErrPathMTUExceeded, err)
	case errors.Is(err, internalsession.ErrHandshakeIncompatible):
		return classifyRuntimeError(ErrHandshakeIncompatible, err)
	case errors.Is(err, wire.ErrAuthentication):
		return classifyRuntimeError(ErrAuthentication, err)
	case errors.Is(err, internalsession.ErrEndpointLimit), errors.Is(err, internalsession.ErrSessionLimit), errors.Is(err, fec.ErrDecoderFull):
		return classifyRuntimeError(ErrResourceLimit, err)
	case errors.Is(err, internalsession.ErrNotEstablished), errors.Is(err, internalsession.ErrHandshakeFailed):
		return classifyRuntimeError(ErrNotReady, err)
	case errors.Is(err, internalsession.ErrInvalidConfig):
		return classifyRuntimeError(ErrInvalidConfig, err)
	default:
		return err
	}
}
