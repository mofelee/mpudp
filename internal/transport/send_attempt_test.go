package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type attemptTestConn struct {
	closed    chan struct{}
	closeOnce sync.Once
	writes    atomic.Int64
	write     func([]byte) (int, error)
	deadline  func(time.Time) error
	mu        sync.Mutex
	writtenAt time.Time
}

func (c *attemptTestConn) Read([]byte) (int, error) {
	<-c.closed
	return 0, net.ErrClosed
}
func (c *attemptTestConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, err := c.Read(p)
	return n, nil, err
}
func (c *attemptTestConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writtenAt = time.Now()
	c.mu.Unlock()
	c.writes.Add(1)
	if c.write != nil {
		return c.write(p)
	}
	return len(p), nil
}
func (c *attemptTestConn) WriteTo(p []byte, _ net.Addr) (int, error) { return c.Write(p) }
func (c *attemptTestConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}
func (*attemptTestConn) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 10001}
}
func (*attemptTestConn) RemoteAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 10002}
}
func (*attemptTestConn) SetDeadline(time.Time) error     { return nil }
func (*attemptTestConn) SetReadDeadline(time.Time) error { return nil }
func (c *attemptTestConn) SetWriteDeadline(at time.Time) error {
	if c.deadline != nil {
		return c.deadline(at)
	}
	return nil
}
func (c *attemptTestConn) writeTime() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writtenAt
}

type attemptFixture struct {
	path     ReplyPath
	conn     *attemptTestConn
	writeMu  sync.Locker
	counters *Counters
	close    func() error
}

func newAttemptFixture(t *testing.T, kind string) attemptFixture {
	t.Helper()
	conn := &attemptTestConn{closed: make(chan struct{})}
	counters := new(Counters)
	fixture := attemptFixture{conn: conn, counters: counters}
	if kind == "listener" {
		listener, err := ServePacketConn(conn, ListenerOptions{PathID: "listener", MaxPayload: 8, Statistics: counters})
		if err != nil {
			t.Fatal(err)
		}
		fixture.path = listenerReplyPath{listener: listener, generation: listener.Generation(), local: conn.LocalAddr(), remote: conn.RemoteAddr()}
		fixture.writeMu, fixture.close = &listener.writeMu, listener.Close
	} else {
		carrier, err := OpenCarrier(context.Background(), "carrier", "remote", CarrierOptions{
			MaxPayload: 8, Statistics: counters,
			Dial: func(context.Context, string) (net.Conn, error) { return conn, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		fixture.path = carrier
		fixture.writeMu, fixture.close = &carrier.current.writeMu, carrier.Close
		if kind == "captured-carrier" {
			fixture.path = carrierReplyPath{carrier: carrier, generation: carrier.Generation(), local: conn.LocalAddr(), remote: conn.RemoteAddr()}
		}
	}
	t.Cleanup(func() { _ = fixture.close() })
	return fixture
}

func TestSendWithAttemptExcludesWriteMutexAndDeadlineSetup(t *testing.T) {
	for _, kind := range []string{"carrier", "captured-carrier", "listener"} {
		t.Run(kind, func(t *testing.T) {
			f := newAttemptFixture(t, kind)
			deadlineEntered, releaseDeadline := make(chan struct{}), make(chan struct{})
			var releaseOnce, enteredOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseDeadline) }) }
			defer release()
			f.conn.deadline = func(at time.Time) error {
				if !at.IsZero() {
					enteredOnce.Do(func() { close(deadlineEntered) })
					<-releaseDeadline
				}
				return nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			type outcome struct {
				at  time.Time
				err error
			}
			result, calling := make(chan outcome, 1), make(chan struct{})
			f.writeMu.Lock()
			var unlockOnce sync.Once
			unlock := func() { unlockOnce.Do(f.writeMu.Unlock) }
			defer unlock()
			go func() {
				close(calling)
				at, err := SendWithAttempt(ctx, f.path, []byte("packet"))
				result <- outcome{at, err}
			}()
			<-calling
			select {
			case <-deadlineEntered:
				t.Fatal("deadline setup bypassed the held transport write mutex")
			case <-time.After(10 * time.Millisecond):
			}
			unlockedAt := time.Now()
			unlock()
			select {
			case <-deadlineEntered:
			case <-time.After(time.Second):
				t.Fatal("send did not reach deadline setup")
			}
			if f.conn.writes.Load() != 0 {
				t.Fatal("write bypassed blocked deadline setup")
			}
			setupFinishedAt := time.Now()
			release()
			select {
			case got := <-result:
				if got.err != nil || got.at.IsZero() || got.at.Before(unlockedAt) || got.at.Before(setupFinishedAt) || got.at.After(f.conn.writeTime()) {
					t.Fatalf("attempt time includes pre-write waiting or follows Write: time=%v err=%v", got.at, got.err)
				}
			case <-time.After(time.Second):
				t.Fatal("timed send did not finish")
			}
			if f.counters.SocketWrite.Snapshot().Count != 0 || f.counters.WriteQueue.Snapshot().Count != 0 {
				t.Fatal("requesting an attempt time enabled diagnostics")
			}
		})
	}
}

func TestSendWithAttemptPreflightHasNoTimestamp(t *testing.T) {
	for _, kind := range []string{"carrier", "captured-carrier", "listener"} {
		for _, cause := range []string{"nil-context", "cancelled", "oversize", "deadline", "closed"} {
			t.Run(kind+"/"+cause, func(t *testing.T) {
				f := newAttemptFixture(t, kind)
				ctx, payload := context.Background(), []byte("packet")
				var expected error
				switch cause {
				case "nil-context":
					ctx, expected = nil, ErrInvalidArgument
				case "cancelled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
					expected = context.Canceled
				case "oversize":
					payload, expected = make([]byte, 9), ErrPayloadTooLarge
				case "deadline":
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, time.Second)
					defer cancel()
					expected = errors.New("deadline setup failed")
					f.conn.deadline = func(time.Time) error { return expected }
				case "closed":
					if err := f.close(); err != nil {
						t.Fatal(err)
					}
					expected = ErrClosed
				}
				at, err := SendWithAttempt(ctx, f.path, payload)
				if !at.IsZero() || !errors.Is(err, expected) || f.conn.writes.Load() != 0 {
					t.Fatalf("preflight time=%v err=%v writes=%d", at, err, f.conn.writes.Load())
				}
			})
		}
	}
}

func TestSendWithAttemptRetainsTimestampForWriteOutcomes(t *testing.T) {
	for _, kind := range []string{"carrier", "captured-carrier", "listener"} {
		for _, cause := range []string{"success", "empty", "short", "write-error", "mtu-error", "reset-error", "write-before-reset-error"} {
			t.Run(kind+"/"+cause, func(t *testing.T) {
				f := newAttemptFixture(t, kind)
				payload := []byte("packet")
				var expected error
				switch cause {
				case "empty":
					payload = nil
				case "short":
					expected = io.ErrShortWrite
					f.conn.write = func([]byte) (int, error) { return 2, nil }
				case "write-error", "write-before-reset-error":
					expected = io.ErrUnexpectedEOF
					f.conn.write = func([]byte) (int, error) { return 0, expected }
				case "mtu-error":
					expected = syscall.EMSGSIZE
					f.conn.write = func([]byte) (int, error) { return 0, expected }
				case "reset-error":
					expected = errors.New("deadline reset failed")
				}
				var reset atomic.Bool
				f.conn.deadline = func(at time.Time) error {
					if at.IsZero() {
						reset.Store(true)
						if cause == "reset-error" {
							return expected
						}
						if cause == "write-before-reset-error" {
							return errors.New("lower priority reset failure")
						}
					}
					return nil
				}
				before := time.Now()
				at, err := SendWithAttempt(context.Background(), f.path, payload)
				if at.IsZero() || at.Before(before) || at.After(f.conn.writeTime()) || !errors.Is(err, expected) || f.conn.writes.Load() != 1 || !reset.Load() {
					t.Fatalf("write outcome time=%v err=%v writes=%d reset=%t", at, err, f.conn.writes.Load(), reset.Load())
				}
				if cause == "mtu-error" && !errors.Is(err, ErrPathMTUExceeded) {
					t.Fatal("timestamp path lost MTU classification")
				}
				rejectedAt, rejectedErr := SendWithAttempt(context.Background(), f.path, make([]byte, 9))
				if !rejectedAt.IsZero() || !errors.Is(rejectedErr, ErrPayloadTooLarge) {
					t.Fatal("rejected call retained an earlier attempt timestamp")
				}
			})
		}
	}
}

func TestSendWithAttemptRejectsCapturedGenerationAndUnsupportedSource(t *testing.T) {
	for _, kind := range []string{"captured-carrier", "listener"} {
		t.Run(kind, func(t *testing.T) {
			f := newAttemptFixture(t, kind)
			switch path := f.path.(type) {
			case carrierReplyPath:
				path.generation++
				f.path = path
			case listenerReplyPath:
				path.generation++
				f.path = path
			}
			at, err := SendWithAttempt(context.Background(), f.path, []byte("packet"))
			if !at.IsZero() || !errors.Is(err, ErrGenerationReplaced) || f.conn.writes.Load() != 0 {
				t.Fatalf("stale captured generation time=%v err=%v", at, err)
			}
		})
	}
	t.Run("unsupported-source-control", func(t *testing.T) {
		f := newAttemptFixture(t, "listener")
		path := f.path.(listenerReplyPath)
		path.sourceControl = []byte{1}
		at, err := SendWithAttempt(context.Background(), path, []byte("packet"))
		if !at.IsZero() || !errors.Is(err, ErrDestinationUnsupported) || f.conn.writes.Load() != 0 {
			t.Fatalf("unsupported source control time=%v err=%v", at, err)
		}
	})
}

type untimedAttemptReply struct {
	ReplyPath
	called int
	err    error
}

func (p *untimedAttemptReply) Send(context.Context, []byte) error {
	p.called++
	return p.err
}

type adaptedAttemptCarrier struct {
	*Carrier
	called int
}

func (p *adaptedAttemptCarrier) Send(context.Context, []byte) error {
	p.called++
	return io.ErrUnexpectedEOF
}

func TestSendWithAttemptCustomFallbackIsExplicitlyUnknown(t *testing.T) {
	for _, expected := range []error{nil, io.ErrUnexpectedEOF} {
		path := &untimedAttemptReply{err: expected}
		at, err := SendWithAttempt(context.Background(), path, []byte("packet"))
		if !at.IsZero() || err != expected || path.called != 1 {
			t.Fatalf("custom path time=%v err=%v calls=%d", at, err, path.called)
		}
	}
	at, err := SendWithAttempt(context.Background(), nil, nil)
	if !at.IsZero() || !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil path time=%v err=%v", at, err)
	}
	f := newAttemptFixture(t, "carrier")
	adapter := &adaptedAttemptCarrier{Carrier: f.path.(*Carrier)}
	at, err = SendWithAttempt(context.Background(), adapter, []byte("packet"))
	if !at.IsZero() || err != io.ErrUnexpectedEOF || adapter.called != 1 || f.conn.writes.Load() != 0 {
		t.Fatalf("inherited timing method bypassed custom Send: time=%v err=%v calls=%d", at, err, adapter.called)
	}
}

func TestSendWithAttemptCancellationAfterWriteRetainsTimestamp(t *testing.T) {
	for _, kind := range []string{"carrier", "captured-carrier", "listener"} {
		t.Run(kind, func(t *testing.T) {
			f := newAttemptFixture(t, kind)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			interrupted := make(chan struct{})
			f.conn.deadline = func(at time.Time) error {
				if !at.IsZero() {
					close(interrupted)
				}
				return nil
			}
			f.conn.write = func([]byte) (int, error) {
				cancel()
				select {
				case <-interrupted:
					return 0, context.Canceled
				case <-time.After(time.Second):
					return 0, errors.New("cancellation did not interrupt write")
				}
			}
			at, err := SendWithAttempt(ctx, f.path, []byte("packet"))
			if at.IsZero() || at.After(f.conn.writeTime()) || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled active write time=%v err=%v", at, err)
			}
		})
	}
}

func TestSendWithAttemptCloseJoinsActiveWrite(t *testing.T) {
	for _, kind := range []string{"carrier", "captured-carrier", "listener"} {
		t.Run(kind, func(t *testing.T) {
			f := newAttemptFixture(t, kind)
			writing := make(chan struct{})
			f.conn.write = func([]byte) (int, error) {
				close(writing)
				<-f.conn.closed
				return 0, net.ErrClosed
			}
			done := make(chan struct{})
			var at time.Time
			var err error
			go func() {
				at, err = SendWithAttempt(context.Background(), f.path, []byte("packet"))
				close(done)
			}()
			select {
			case <-writing:
			case <-time.After(time.Second):
				t.Fatal("send did not start")
			}
			if closeErr := f.close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			<-done
			if at.IsZero() || at.After(f.conn.writeTime()) || !errors.Is(err, net.ErrClosed) {
				t.Fatalf("closed active write time=%v err=%v", at, err)
			}
		})
	}
}
