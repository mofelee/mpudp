package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// DialFunc creates one connected UDP socket for one remote Carrier. A custom
// function is the injection point used by deterministic tests.
type DialFunc func(context.Context, string) (net.Conn, error)

type CarrierOptions struct {
	Dial       DialFunc
	MaxPayload int
	// MaxReceivePayload defaults to MaxPayload when zero, independently of sends.
	MaxReceivePayload int
	OnPacket          PacketHandler
	OnError           ErrorHandler
	RequirePMTU       bool
	Statistics        *Counters
}

type carrierGeneration struct {
	number      uint64
	conn        net.Conn
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	active      sync.WaitGroup
	writeMu     sync.Mutex
	alive       atomic.Bool
	pmtuEnabled bool
}

// Carrier owns a stable identity and at most one current connected UDP socket.
// Rebuild replaces the socket without changing the Carrier identity.
type Carrier struct {
	id                string
	remote            string
	dial              DialFunc
	maxPayload        int
	maxReceivePayload int
	onPacket          PacketHandler
	onError           ErrorHandler

	lifecycleMu sync.Mutex
	mu          sync.RWMutex
	current     *carrierGeneration
	nextGen     uint64
	closed      bool
	closeDone   chan struct{}
	closeErr    error
	lifetime    context.Context
	cancelLife  context.CancelFunc
	requirePMTU bool
	statistics  *Counters
}

// OpenCarrier validates options, creates one connected socket, and starts its
// receive loop. The socket remains open until Rebuild or Close.
func OpenCarrier(ctx context.Context, id, remote string, options CarrierOptions) (*Carrier, error) {
	if ctx == nil {
		return nil, invalidArgument("nil open context")
	}
	if id == "" {
		return nil, invalidArgument("empty carrier ID")
	}
	if remote == "" {
		return nil, invalidArgument("empty carrier remote address")
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
	dial := options.Dial
	if dial == nil {
		dial = dialUDP
	}
	lifetime, cancelLife := context.WithCancel(context.Background())
	c := &Carrier{
		id:                id,
		remote:            remote,
		dial:              dial,
		maxPayload:        maxPayload,
		maxReceivePayload: maxReceivePayload,
		onPacket:          options.OnPacket,
		onError:           options.OnError,
		closeDone:         make(chan struct{}),
		lifetime:          lifetime,
		cancelLife:        cancelLife,
		requirePMTU:       options.RequirePMTU,
		statistics:        options.Statistics,
	}
	if err := c.Rebuild(ctx); err != nil {
		cancelLife()
		return nil, err
	}
	return c, nil
}

func (c *Carrier) PathID() string { return c.id }

func (c *Carrier) Available() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !c.closed && c.current != nil && c.current.alive.Load()
}

func (c *Carrier) Generation() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.current == nil {
		return c.nextGen
	}
	return c.current.number
}

func (c *Carrier) LocalAddr() net.Addr {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.current == nil {
		return nil
	}
	return cloneAddr(c.current.conn.LocalAddr())
}

func (c *Carrier) RemoteAddr() net.Addr {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.current == nil {
		return nil
	}
	return cloneAddr(c.current.conn.RemoteAddr())
}

func (c *Carrier) PMTUEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.current != nil && c.current.pmtuEnabled
}

// Rebuild creates and installs a new socket before retiring the old generation.
// A failed dial leaves the old generation untouched.
func (c *Carrier) Rebuild(ctx context.Context) error {
	if ctx == nil {
		return invalidArgument("nil rebuild context")
	}
	c.lifecycleMu.Lock()

	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		c.lifecycleMu.Unlock()
		return ErrClosed
	}

	dialCtx, cancelDial := context.WithCancel(ctx)
	stopLifetimeCancel := context.AfterFunc(c.lifetime, cancelDial)
	conn, err := c.dial(dialCtx, c.remote)
	stopLifetimeCancel()
	cancelDial()
	if err != nil {
		c.lifecycleMu.Unlock()
		return &PathError{PathID: c.id, Generation: c.Generation(), Operation: "dial", Err: err}
	}
	if conn == nil {
		c.lifecycleMu.Unlock()
		return &PathError{PathID: c.id, Generation: c.Generation(), Operation: "dial", Err: invalidArgument("dialer returned nil connection")}
	}
	pmtuEnabled := connectionPMTUEnabled(conn)
	if c.requirePMTU && !pmtuEnabled {
		_ = conn.Close()
		c.lifecycleMu.Unlock()
		return &PathError{PathID: c.id, Generation: c.Generation(), Operation: "enable PMTU discovery", Err: ErrPMTUUnsupported}
	}

	genCtx, cancel := context.WithCancel(c.lifetime)
	generation := &carrierGeneration{
		conn:        conn,
		ctx:         genCtx,
		cancel:      cancel,
		done:        make(chan struct{}),
		pmtuEnabled: pmtuEnabled,
	}
	generation.alive.Store(true)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		_ = conn.Close()
		c.lifecycleMu.Unlock()
		return ErrClosed
	}
	c.nextGen++
	generation.number = c.nextGen
	old := c.current
	c.current = generation
	go c.readLoop(generation)
	c.mu.Unlock()

	retireErr := retireCarrierGeneration(old)
	c.lifecycleMu.Unlock()
	if retireErr != nil {
		return &PathError{PathID: c.id, Generation: old.number, Operation: "retire", Err: retireErr}
	}
	return nil
}

func (c *Carrier) Send(ctx context.Context, payload []byte) error {
	return c.sendOnGeneration(ctx, 0, payload)
}

func (c *Carrier) sendOnGeneration(ctx context.Context, expected uint64, payload []byte) error {
	if ctx == nil {
		return invalidArgument("nil send context")
	}
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrClosed
	}
	generation := c.current
	if generation == nil || !generation.alive.Load() {
		c.mu.RUnlock()
		return ErrPathUnavailable
	}
	if expected != 0 && generation.number != expected {
		c.mu.RUnlock()
		return ErrGenerationReplaced
	}
	generation.active.Add(1)
	c.mu.RUnlock()
	defer generation.active.Done()

	if len(payload) > c.maxPayload {
		return &PayloadSizeError{Size: len(payload), Limit: c.maxPayload}
	}
	if err := writeConnected(ctx, generation, payload, c.statistics); err != nil {
		if isPathMTUError(err) {
			err = errors.Join(ErrPathMTUExceeded, err)
		}
		return &PathError{PathID: c.id, Generation: generation.number, Operation: "write", Err: err}
	}
	return nil
}

func writeConnected(ctx context.Context, generation *carrierGeneration, payload []byte, statistics *Counters) error {
	queuedAt := statistics.start()
	generation.writeMu.Lock()
	var acquired time.Time
	if !queuedAt.IsZero() {
		acquired = time.Now()
	}
	defer generation.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	if deadline, ok := ctx.Deadline(); ok {
		if err := generation.conn.SetWriteDeadline(deadline); err != nil {
			return err
		}
	}
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = generation.conn.SetWriteDeadline(time.Now())
		close(callbackDone)
	})
	var writeStarted time.Time
	if !queuedAt.IsZero() {
		writeStarted = time.Now()
	}
	n, err := generation.conn.Write(payload)
	statistics.wrote(n, err == nil && n == len(payload), err, writeStarted)
	if !queuedAt.IsZero() {
		statistics.WriteQueue.Observe(acquired.Sub(queuedAt))
	}
	if !stop() {
		<-callbackDone
	}
	resetErr := generation.conn.SetWriteDeadline(time.Time{})
	if err != nil {
		return err
	}
	if resetErr != nil {
		return resetErr
	}
	if n != len(payload) {
		return io.ErrShortWrite
	}
	return nil
}

func (c *Carrier) readLoop(generation *carrierGeneration) {
	defer close(generation.done)
	buffer := make([]byte, c.maxReceivePayload+1)
	for {
		n, err := generation.conn.Read(buffer)
		if err != nil {
			if isTemporary(err) {
				continue
			}
			generation.alive.Store(false)
			if !errors.Is(err, net.ErrClosed) && c.isCurrent(generation) {
				c.reportError(&PathError{PathID: c.id, Generation: generation.number, Operation: "read", Err: err})
			}
			return
		}
		c.statistics.receive(n, c.maxReceivePayload)
		if n > c.maxReceivePayload {
			c.reportIfCurrent(generation, &PathError{
				PathID: c.id, Generation: generation.number, Operation: "read",
				Err: &PayloadSizeError{Size: n, Limit: c.maxReceivePayload},
			})
			continue
		}
		if c.onPacket == nil {
			continue
		}
		payload := append([]byte(nil), buffer[:n]...)
		local := cloneAddr(generation.conn.LocalAddr())
		remote := cloneAddr(generation.conn.RemoteAddr())
		c.deliverIfCurrent(generation, ReceivedPacket{
			Payload:    payload,
			PathID:     c.id,
			Generation: generation.number,
			LocalAddr:  local,
			RemoteAddr: remote,
			Reply: carrierReplyPath{
				carrier:    c,
				generation: generation.number,
				local:      local,
				remote:     remote,
			},
			Context: generation.ctx,
		})
	}
}

func (c *Carrier) isCurrent(generation *carrierGeneration) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !c.closed && c.current == generation
}

func (c *Carrier) deliverIfCurrent(generation *carrierGeneration, packet ReceivedPacket) {
	c.mu.RLock()
	if c.closed || c.current != generation {
		c.mu.RUnlock()
		return
	}
	generation.active.Add(1)
	c.mu.RUnlock()
	c.onPacket(packet)
	generation.active.Done()
}

func (c *Carrier) reportIfCurrent(generation *carrierGeneration, err error) {
	if c.isCurrent(generation) {
		c.reportError(err)
	}
}

func (c *Carrier) reportError(err error) {
	if c.onError != nil {
		c.onError(err)
	}
}

// Close is idempotent and waits for the current socket read loop and writes to
// finish. No read callback from that generation can begin after Close returns.
func (c *Carrier) Close() error {
	c.mu.Lock()
	if c.closed {
		done := c.closeDone
		c.mu.Unlock()
		<-done
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.closeErr
	}
	c.closed = true
	old := c.current
	c.current = nil
	c.mu.Unlock()
	c.cancelLife()

	// A rebuild may still be dialing or retiring an older generation. The
	// closed flag prevents it from publishing a socket after this point.
	c.lifecycleMu.Lock()
	c.lifecycleMu.Unlock()

	retireErr := retireCarrierGeneration(old)
	var closeErr error
	if retireErr != nil {
		closeErr = &PathError{PathID: c.id, Generation: old.number, Operation: "close", Err: retireErr}
	}
	c.mu.Lock()
	c.closeErr = closeErr
	close(c.closeDone)
	c.mu.Unlock()
	return closeErr
}

func retireCarrierGeneration(generation *carrierGeneration) error {
	if generation == nil {
		return nil
	}
	generation.alive.Store(false)
	generation.cancel()
	err := generation.conn.Close()
	<-generation.done
	generation.active.Wait()
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

type carrierReplyPath struct {
	carrier    *Carrier
	generation uint64
	local      net.Addr
	remote     net.Addr
}

func (r carrierReplyPath) PathID() string       { return r.carrier.PathID() }
func (r carrierReplyPath) Generation() uint64   { return r.generation }
func (r carrierReplyPath) LocalAddr() net.Addr  { return cloneAddr(r.local) }
func (r carrierReplyPath) RemoteAddr() net.Addr { return cloneAddr(r.remote) }
func (r carrierReplyPath) Available() bool {
	return r.carrier.Available() && r.carrier.Generation() == r.generation
}
func (r carrierReplyPath) Send(ctx context.Context, payload []byte) error {
	return r.carrier.sendOnGeneration(ctx, r.generation, payload)
}

func isTemporary(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func (c *Carrier) String() string {
	return fmt.Sprintf("Carrier{%q generation:%d available:%t}", c.id, c.Generation(), c.Available())
}
