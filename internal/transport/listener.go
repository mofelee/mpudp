package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"syscall"
	"time"
)

type ListenerOptions struct {
	PathID     string
	MaxPayload int
	// MaxReceivePayload defaults to MaxPayload when zero, independently of sends.
	MaxReceivePayload int
	OnPacket          PacketHandler
	OnError           ErrorHandler
	RequirePMTU       bool
	// RequireDestination captures the actual packet destination and pins replies
	// to that source. It requires a native Linux UDP socket.
	RequireDestination bool
	Statistics         *Counters
}

// Listener owns one unconnected UDP socket. Every ReplyPath created by its
// read loop writes back through this same socket, preserving the captured
// destination as the reply source when RequireDestination is enabled.
type Listener struct {
	id                string
	conn              net.PacketConn
	maxPayload        int
	maxReceivePayload int
	destination       *destinationCapture
	onPacket          PacketHandler
	onError           ErrorHandler
	generation        uint64
	ctx               context.Context
	cancel            context.CancelFunc
	done              chan struct{}
	pmtuEnabled       bool
	statistics        *Counters

	mu        sync.RWMutex
	writeMu   sync.Mutex
	active    sync.WaitGroup
	closed    bool
	closeDone chan struct{}
	closeErr  error
}

// OpenListener binds one UDP socket and starts its receive loop.
func OpenListener(ctx context.Context, network, address string, options ListenerOptions) (*Listener, error) {
	if ctx == nil {
		return nil, invalidArgument("nil listen context")
	}
	if network != "udp" && network != "udp4" && network != "udp6" {
		return nil, invalidArgument("listener network must be udp, udp4, or udp6")
	}
	packetConn, err := (&net.ListenConfig{}).ListenPacket(ctx, network, address)
	if err != nil {
		return nil, err
	}
	pmtuEnabled := false
	if syscallConn, ok := packetConn.(syscall.Conn); ok {
		family := network
		if family == "udp" {
			family = addressNetwork(packetConn.LocalAddr())
		}
		pmtuEnabled, err = configurePMTU(syscallConn, family)
		if err != nil {
			_ = packetConn.Close()
			return nil, err
		}
	}
	listener, err := newListener(packetConn, options, pmtuEnabled)
	if err != nil {
		_ = packetConn.Close()
		return nil, err
	}
	return listener, nil
}

// ServePacketConn starts a Listener around an injected connection. Ownership
// transfers to Listener on success. A fake can advertise PMTU status by
// implementing interface{ PMTUEnabled() bool }.
func ServePacketConn(conn net.PacketConn, options ListenerOptions) (*Listener, error) {
	if conn == nil {
		return nil, invalidArgument("nil listener packet connection")
	}
	pmtuEnabled := false
	if status, ok := conn.(pmtuConnection); ok {
		pmtuEnabled = status.PMTUEnabled()
	}
	return newListener(conn, options, pmtuEnabled)
}

func newListener(conn net.PacketConn, options ListenerOptions, pmtuEnabled bool) (*Listener, error) {
	if options.PathID == "" {
		return nil, invalidArgument("empty listener path ID")
	}
	maxPayload := options.MaxPayload
	if maxPayload == 0 {
		maxPayload = MaxUDPPayload
	}
	if maxPayload < 1 || maxPayload > MaxUDPPayload {
		return nil, &PayloadSizeError{Size: maxPayload, Limit: MaxUDPPayload}
	}
	maxReceivePayload := options.MaxReceivePayload
	if maxReceivePayload == 0 {
		maxReceivePayload = maxPayload
	}
	if maxReceivePayload < 1 || maxReceivePayload > MaxUDPPayload {
		return nil, &PayloadSizeError{Size: maxReceivePayload, Limit: MaxUDPPayload}
	}
	if options.RequirePMTU && !pmtuEnabled {
		return nil, ErrPMTUUnsupported
	}
	var destination *destinationCapture
	if options.RequireDestination {
		var err error
		destination, err = newDestinationCapture(conn)
		if err != nil {
			return nil, err
		}
	}
	lifetime, cancel := context.WithCancel(context.Background())
	listener := &Listener{
		id:                options.PathID,
		conn:              conn,
		maxPayload:        maxPayload,
		maxReceivePayload: maxReceivePayload,
		destination:       destination,
		onPacket:          options.OnPacket,
		onError:           options.OnError,
		generation:        1,
		ctx:               lifetime,
		cancel:            cancel,
		done:              make(chan struct{}),
		closeDone:         make(chan struct{}),
		pmtuEnabled:       pmtuEnabled,
		statistics:        options.Statistics,
	}
	go listener.readLoop()
	return listener, nil
}

func (l *Listener) PathID() string      { return l.id }
func (l *Listener) Generation() uint64  { return l.generation }
func (l *Listener) PMTUEnabled() bool   { return l.pmtuEnabled }
func (l *Listener) LocalAddr() net.Addr { return cloneAddr(l.conn.LocalAddr()) }
func (l *Listener) Available() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return !l.closed
}

func (l *Listener) readLoop() {
	defer close(l.done)
	buffer := make([]byte, l.maxReceivePayload+1)
	var oob []byte
	if l.destination != nil {
		oob = make([]byte, l.destination.controlSize())
	}
	// Only native UDP sockets promise the value-based receive path. Injected
	// connections keep their ReadFrom overrides and address ownership contract.
	udp, nativeUDP := l.conn.(*net.UDPConn)
	for {
		var n int
		var remote net.Addr
		var remoteValue netip.AddrPort
		var oobn, flags int
		var err error
		if l.destination != nil {
			n, oobn, flags, remoteValue, err = udp.ReadMsgUDPAddrPort(buffer, oob)
		} else if nativeUDP {
			n, remoteValue, err = udp.ReadFromUDPAddrPort(buffer)
		} else {
			n, remote, err = l.conn.ReadFrom(buffer)
		}
		if err != nil {
			if isTemporary(err) {
				continue
			}
			if !errors.Is(err, net.ErrClosed) && l.Available() {
				l.reportError(&PathError{PathID: l.id, Generation: l.generation, Operation: "read", Err: err})
			}
			return
		}
		l.statistics.receive(n, l.maxReceivePayload)
		if n > l.maxReceivePayload {
			l.reportError(&PathError{
				PathID: l.id, Generation: l.generation, Operation: "read",
				Err: &PayloadSizeError{Size: n, Limit: l.maxReceivePayload},
			})
			continue
		}
		if (nativeUDP && !remoteValue.IsValid()) || (!nativeUDP && remote == nil) {
			l.reportError(&PathError{PathID: l.id, Generation: l.generation, Operation: "read", Err: invalidArgument("nil remote endpoint")})
			continue
		}
		var local net.Addr
		var sourceControl []byte
		if l.destination != nil {
			local, sourceControl, err = l.destination.parseControl(oob[:oobn], flags, remoteValue)
			if err != nil {
				l.reportError(&PathError{PathID: l.id, Generation: l.generation, Operation: "read destination", Err: err})
				continue
			}
		}
		if l.onPacket == nil {
			continue
		}
		if nativeUDP {
			remote = net.UDPAddrFromAddrPort(remoteValue)
		} else {
			remote = cloneAddr(remote)
		}
		if local == nil {
			local = cloneAddr(l.conn.LocalAddr())
		}
		reply := listenerReplyPath{
			listener:      l,
			generation:    l.generation,
			local:         local,
			remote:        remote,
			pathID:        fmt.Sprintf("%s/%s", l.id, remote.String()),
			sourceControl: sourceControl,
		}
		if l.destination != nil {
			reply.local, reply.remote = cloneAddr(local), cloneAddr(remote)
		}
		packet := ReceivedPacket{
			Payload:    append([]byte(nil), buffer[:n]...),
			PathID:     l.id,
			Generation: l.generation,
			LocalAddr:  local,
			RemoteAddr: remote,
			Reply:      reply,
			Context:    l.ctx,
		}

		l.mu.RLock()
		if l.closed {
			l.mu.RUnlock()
			return
		}
		l.active.Add(1)
		l.mu.RUnlock()
		l.onPacket(packet)
		l.active.Done()
	}
}

func (l *Listener) reportError(err error) {
	if l.onError != nil {
		l.onError(err)
	}
}

func (l *Listener) sendTo(ctx context.Context, generation uint64, remote net.Addr, payload []byte, statistics *Counters) error {
	return l.sendToWithControl(ctx, generation, remote, payload, statistics, nil)
}

func (l *Listener) sendToWithControl(ctx context.Context, generation uint64, remote net.Addr, payload []byte, statistics *Counters, sourceControl []byte) error {
	if ctx == nil {
		return invalidArgument("nil send context")
	}
	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return ErrClosed
	}
	if generation != l.generation {
		l.mu.RUnlock()
		return ErrGenerationReplaced
	}
	l.active.Add(1)
	l.mu.RUnlock()
	defer l.active.Done()

	if len(payload) > l.maxPayload {
		return &PayloadSizeError{Size: len(payload), Limit: l.maxPayload}
	}
	queuedAt := l.statistics.start()
	pathQueuedAt := statistics.start()
	l.writeMu.Lock()
	var acquired time.Time
	if !queuedAt.IsZero() || !pathQueuedAt.IsZero() {
		acquired = time.Now()
	}
	defer l.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := l.conn.SetWriteDeadline(deadline); err != nil {
			return &PathError{PathID: l.id, Generation: generation, Operation: "set write deadline", Err: err}
		}
	}
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = l.conn.SetWriteDeadline(time.Now())
		close(callbackDone)
	})
	var writeStarted time.Time
	if !acquired.IsZero() {
		writeStarted = time.Now()
	}
	var n int
	var err error
	if sourceControl == nil {
		n, err = l.conn.WriteTo(payload, remote)
	} else {
		udp, socketOK := l.conn.(*net.UDPConn)
		address, addressOK := remote.(*net.UDPAddr)
		if !socketOK || !addressOK || address == nil {
			err = ErrDestinationUnsupported
		} else {
			n, _, err = udp.WriteMsgUDPAddrPort(payload, sourceControl, address.AddrPort())
		}
	}
	var elapsed time.Duration
	if !writeStarted.IsZero() {
		elapsed = time.Since(writeStarted)
	}
	if !queuedAt.IsZero() {
		l.statistics.WriteQueue.Observe(acquired.Sub(queuedAt))
	}
	if !pathQueuedAt.IsZero() {
		statistics.WriteQueue.Observe(acquired.Sub(pathQueuedAt))
	}
	l.statistics.wroteElapsed(n, err == nil && n == len(payload), err, !queuedAt.IsZero(), elapsed)
	statistics.wroteElapsed(n, err == nil && n == len(payload), err, !pathQueuedAt.IsZero(), elapsed)
	if !stop() {
		<-callbackDone
	}
	resetErr := l.conn.SetWriteDeadline(time.Time{})
	if err == nil {
		err = resetErr
	}
	if err == nil && n != len(payload) {
		err = io.ErrShortWrite
	}
	if err != nil {
		if isPathMTUError(err) {
			err = errors.Join(ErrPathMTUExceeded, err)
		}
		return &PathError{PathID: l.id, Generation: generation, Operation: "write", Err: err}
	}
	return nil
}

// Close is idempotent and waits for the read loop and active writes/callbacks.
func (l *Listener) Close() error {
	l.mu.Lock()
	if l.closed {
		done := l.closeDone
		l.mu.Unlock()
		<-done
		l.mu.RLock()
		defer l.mu.RUnlock()
		return l.closeErr
	}
	l.closed = true
	l.cancel()
	err := l.conn.Close()
	l.mu.Unlock()
	<-l.done
	l.active.Wait()
	if err != nil && !errors.Is(err, net.ErrClosed) {
		err = &PathError{PathID: l.id, Generation: l.generation, Operation: "close", Err: err}
	} else {
		err = nil
	}
	l.mu.Lock()
	l.closeErr = err
	close(l.closeDone)
	l.mu.Unlock()
	return err
}

type listenerReplyPath struct {
	listener      *Listener
	generation    uint64
	pathID        string
	local         net.Addr
	remote        net.Addr
	statistics    *Counters
	sourceControl []byte
}

func (r listenerReplyPath) PathID() string       { return r.pathID }
func (r listenerReplyPath) Generation() uint64   { return r.generation }
func (r listenerReplyPath) LocalAddr() net.Addr  { return cloneAddr(r.local) }
func (r listenerReplyPath) RemoteAddr() net.Addr { return cloneAddr(r.remote) }
func (r listenerReplyPath) Available() bool      { return r.listener.Available() }
func (r listenerReplyPath) Send(ctx context.Context, payload []byte) error {
	if r.sourceControl != nil {
		return r.listener.sendToWithControl(ctx, r.generation, r.remote, payload, r.statistics, r.sourceControl)
	}
	return r.listener.sendTo(ctx, r.generation, r.remote, payload, r.statistics)
}

// WithReplyStatistics attaches an accepted listener path's collector at the
// actual socket boundary. Other ReplyPath implementations are left unchanged.
func WithReplyStatistics(path ReplyPath, counters *Counters) ReplyPath {
	if counters == nil {
		return path
	}
	if reply, ok := path.(listenerReplyPath); ok {
		reply.statistics = counters
		return reply
	}
	return path
}
