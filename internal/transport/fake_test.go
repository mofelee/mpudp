package transport_test

import (
	"net"
	"sync"
	"time"
)

type fakeAddr string

func (a fakeAddr) Network() string { return "udp" }
func (a fakeAddr) String() string  { return string(a) }

type streamRead struct {
	payload []byte
	err     error
}

type fakeConnectedConn struct {
	local  net.Addr
	remote net.Addr
	reads  chan streamRead
	closed chan struct{}
	once   sync.Once
	pmtu   bool

	mu       sync.Mutex
	writes   [][]byte
	writeErr error
}

func newFakeConnectedConn(local, remote string) *fakeConnectedConn {
	return &fakeConnectedConn{
		local:  fakeAddr(local),
		remote: fakeAddr(remote),
		reads:  make(chan streamRead, 16),
		closed: make(chan struct{}),
	}
}

func (c *fakeConnectedConn) Read(dst []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	case read := <-c.reads:
		if read.err != nil {
			return 0, read.err
		}
		return copy(dst, read.payload), nil
	}
}

func (c *fakeConnectedConn) Write(payload []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	c.writes = append(c.writes, append([]byte(nil), payload...))
	return len(payload), nil
}

func (c *fakeConnectedConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *fakeConnectedConn) LocalAddr() net.Addr              { return c.local }
func (c *fakeConnectedConn) RemoteAddr() net.Addr             { return c.remote }
func (c *fakeConnectedConn) SetDeadline(time.Time) error      { return nil }
func (c *fakeConnectedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakeConnectedConn) SetWriteDeadline(time.Time) error { return nil }
func (c *fakeConnectedConn) PMTUEnabled() bool                { return c.pmtu }

func (c *fakeConnectedConn) written() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([][]byte, len(c.writes))
	for i := range c.writes {
		result[i] = append([]byte(nil), c.writes[i]...)
	}
	return result
}

func (c *fakeConnectedConn) setWriteError(err error) {
	c.mu.Lock()
	c.writeErr = err
	c.mu.Unlock()
}

type packetRead struct {
	payload []byte
	remote  net.Addr
	err     error
}

type packetWrite struct {
	payload []byte
	remote  net.Addr
}

type fakePacketConn struct {
	local  net.Addr
	reads  chan packetRead
	closed chan struct{}
	once   sync.Once
	pmtu   bool

	mu       sync.Mutex
	writes   []packetWrite
	writeErr error
}

func newFakePacketConn(local string) *fakePacketConn {
	return &fakePacketConn{
		local:  fakeAddr(local),
		reads:  make(chan packetRead, 16),
		closed: make(chan struct{}),
	}
}

func (c *fakePacketConn) ReadFrom(dst []byte) (int, net.Addr, error) {
	select {
	case <-c.closed:
		return 0, nil, net.ErrClosed
	case read := <-c.reads:
		if read.err != nil {
			return 0, nil, read.err
		}
		return copy(dst, read.payload), read.remote, nil
	}
}

func (c *fakePacketConn) WriteTo(payload []byte, remote net.Addr) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	c.writes = append(c.writes, packetWrite{payload: append([]byte(nil), payload...), remote: remote})
	return len(payload), nil
}

func (c *fakePacketConn) Close() error                     { c.once.Do(func() { close(c.closed) }); return nil }
func (c *fakePacketConn) LocalAddr() net.Addr              { return c.local }
func (c *fakePacketConn) SetDeadline(time.Time) error      { return nil }
func (c *fakePacketConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakePacketConn) SetWriteDeadline(time.Time) error { return nil }
func (c *fakePacketConn) PMTUEnabled() bool                { return c.pmtu }

func (c *fakePacketConn) written() []packetWrite {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]packetWrite, len(c.writes))
	copy(result, c.writes)
	return result
}

var _ net.Conn = (*fakeConnectedConn)(nil)
var _ net.PacketConn = (*fakePacketConn)(nil)
