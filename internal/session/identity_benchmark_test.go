package session

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wire"
)

// One injected receive obtains the same immutable ReplyPath implementation
// used by a real transport Listener, including its clone-returning UDP getters.
type identityBenchmarkConn struct {
	local    *net.UDPAddr
	remote   *net.UDPAddr
	received bool
	closed   chan struct{}
	once     sync.Once
}

func (c *identityBenchmarkConn) ReadFrom(payload []byte) (int, net.Addr, error) {
	if !c.received {
		c.received = true
		payload[0] = 0
		return 1, c.remote, nil
	}
	<-c.closed
	return 0, nil, net.ErrClosed
}

func (*identityBenchmarkConn) WriteTo(payload []byte, _ net.Addr) (int, error) {
	return len(payload), nil
}

func (c *identityBenchmarkConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *identityBenchmarkConn) LocalAddr() net.Addr            { return c.local }
func (*identityBenchmarkConn) SetDeadline(time.Time) error      { return nil }
func (*identityBenchmarkConn) SetReadDeadline(time.Time) error  { return nil }
func (*identityBenchmarkConn) SetWriteDeadline(time.Time) error { return nil }

func nativeIdentityBenchmarkPath(b *testing.B, local, remote *net.UDPAddr) ReplyPath {
	b.Helper()
	conn := &identityBenchmarkConn{local: local, remote: remote, closed: make(chan struct{})}
	received := make(chan ReplyPath, 1)
	listener, err := transport.ServePacketConn(conn, transport.ListenerOptions{
		PathID: "listener", MaxPayload: 1200,
		OnPacket: func(packet transport.ReceivedPacket) { received <- packet.Reply },
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = listener.Close() })
	select {
	case path := <-received:
		return path
	case <-time.After(5 * time.Second):
		b.Fatal("transport did not deliver the injected receive")
		return nil
	}
}

func BenchmarkListenerReplyIdentity(b *testing.B) {
	for _, test := range []struct {
		name   string
		local  *net.UDPAddr
		remote *net.UDPAddr
	}{
		{
			name:   "native_ipv4",
			local:  &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1).To4(), Port: 9000},
			remote: &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1).To4(), Port: 4000},
		},
		{
			name:   "native_ipv6_zone",
			local:  &net.UDPAddr{IP: net.ParseIP("fe80::1"), Port: 9000, Zone: "eth0"},
			remote: &net.UDPAddr{IP: net.ParseIP("fe80::2"), Port: 4000, Zone: "eth0"},
		},
		{name: "generic"},
	} {
		b.Run(test.name, func(b *testing.B) {
			var path ReplyPath
			if test.local == nil {
				path = newFakePath("listener/client", "198.51.100.1:4000")
			} else {
				path = nativeIdentityBenchmarkPath(b, test.local, test.remote)
			}
			listener, err := NewListener(ListenerConfig{Session: testConfig(newFakeClock(), 1200), MaxSessions: 1})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = listener.Close(context.Background()) })
			hello, err := wire.NewHello(testSessionID, 3, 2, 1200)
			if err != nil {
				b.Fatal(err)
			}
			payload, err := wire.AppendAuthenticated(nil, hello, []byte(testPSK), 1200)
			if err != nil {
				b.Fatal(err)
			}
			if _, _, err := listener.HandlePacket(context.Background(), ReceivedPacket{Payload: payload, Reply: path}); err != nil {
				b.Fatal(err)
			}
			message, err := wire.NewDataShard(testSessionID, 1, 3, 2, 0, 1440, make([]byte, 480))
			if err != nil {
				b.Fatal(err)
			}
			payload, err = wire.AppendAuthenticated(nil, message, []byte(testPSK), 1200)
			if err != nil {
				b.Fatal(err)
			}
			packet := ReceivedPacket{Payload: payload, Reply: path}
			// Warm one pending block. Repeated shards keep FEC state bounded and
			// still exercise authenticated dispatch and Endpoint refresh.
			if _, _, err := listener.HandlePacket(context.Background(), packet); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, _, err := listener.HandlePacket(context.Background(), packet); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
