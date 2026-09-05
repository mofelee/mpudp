package transport_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/netip"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/transport"
)

func TestNativeListenerPreservesReadFromAddressesAndReplyRoute(t *testing.T) {
	for _, test := range []struct {
		name    string
		network string
		bind    string
		client  string
	}{
		{"IPv4", "udp4", "127.0.0.1:0", "127.0.0.1"},
		{"IPv6", "udp6", "[::1]:0", "::1"},
		{"dual stack IPv4", "udp", "[::]:0", "127.0.0.1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			bind, err := net.ResolveUDPAddr(test.network, test.bind)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := net.ListenUDP(test.network, bind)
			if err != nil {
				if test.network != "udp4" {
					t.Skipf("IPv6 socket unavailable: %v", err)
				}
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			local := conn.LocalAddr().(*net.UDPAddr)
			expectedLocal := *local
			expectedLocal.IP = append(net.IP(nil), local.IP...)
			remote, err := net.ResolveUDPAddr("udp", net.JoinHostPort(test.client, strconv.Itoa(local.Port)))
			if err != nil {
				t.Fatal(err)
			}
			client, err := net.DialUDP("udp", nil, remote)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := client.Write([]byte("original address")); err != nil {
				t.Fatal(err)
			}
			buffer := make([]byte, 64)
			_, expectedRemote, err := conn.ReadFrom(buffer)
			if err != nil {
				t.Fatal(err)
			}
			if err := conn.SetReadDeadline(time.Time{}); err != nil {
				t.Fatal(err)
			}
			packets := make(chan transport.ReceivedPacket, 1)
			listener, err := transport.ServePacketConn(conn, transport.ListenerOptions{
				PathID: "native", MaxPayload: 64,
				OnPacket: func(packet transport.ReceivedPacket) { packets <- packet },
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			payloads := [][]byte{[]byte("first"), nil, []byte("last")}
			var retained []transport.ReceivedPacket
			for _, payload := range payloads {
				if _, err := client.Write(payload); err != nil {
					t.Fatal(err)
				}
				packet := receivePacket(t, packets)
				retained = append(retained, packet)
				if !bytes.Equal(packet.Payload, payload) || !reflect.DeepEqual(packet.LocalAddr, &expectedLocal) || !reflect.DeepEqual(packet.RemoteAddr, expectedRemote) {
					t.Fatalf("native receive changed owned payload or ReadFrom address representation: %+v", packet)
				}
				if packet.Reply.PathID() != "native/"+expectedRemote.String() || packet.Reply.Generation() != 1 {
					t.Fatal("native receive changed reply identity")
				}
				localCopy := packet.Reply.LocalAddr().(*net.UDPAddr)
				remoteCopy := packet.Reply.RemoteAddr().(*net.UDPAddr)
				localCopy.IP[0], remoteCopy.IP[0] = 0, 0
				if !reflect.DeepEqual(packet.Reply.LocalAddr(), &expectedLocal) || !reflect.DeepEqual(packet.Reply.RemoteAddr(), expectedRemote) {
					t.Fatal("mutating a getter result changed the reply route")
				}
				if err := packet.Reply.Send(context.Background(), []byte("reply")); err != nil {
					t.Fatal(err)
				}
				if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
					t.Fatal(err)
				}
				n, source, err := client.ReadFromUDP(buffer)
				if err != nil || string(buffer[:n]) != "reply" || source.Port != local.Port || !source.IP.Equal(remote.IP) {
					t.Fatalf("reply changed receiving socket/source: %v", err)
				}
				packet.LocalAddr.(*net.UDPAddr).IP[0] = 0
				packet.RemoteAddr.(*net.UDPAddr).IP[0] = 0
			}
			for i, packet := range retained {
				if !bytes.Equal(packet.Payload, payloads[i]) {
					t.Fatal("later native receive overwrote a retained packet")
				}
			}
			if !reflect.DeepEqual(conn.LocalAddr(), &expectedLocal) {
				t.Fatal("packet address mutation changed the socket address")
			}
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type addrPortCapablePacketConn struct {
	*fakePacketConn
	addrPortCalls atomic.Int64
}

func (c *addrPortCapablePacketConn) ReadFromUDPAddrPort([]byte) (int, netip.AddrPort, error) {
	c.addrPortCalls.Add(1)
	return 0, netip.AddrPort{}, errors.New("injected AddrPort method must not bypass ReadFrom")
}

func TestInjectedAddrPortMethodKeepsReadFromFallbackAndErrorOrder(t *testing.T) {
	conn := &addrPortCapablePacketConn{fakePacketConn: newFakePacketConn("listener")}
	errorsCh := make(chan error, 2)
	listener, err := transport.ServePacketConn(conn, transport.ListenerOptions{
		PathID: "injected", MaxPayload: 4, OnError: func(err error) { errorsCh <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	for _, test := range []struct {
		payload []byte
		want    error
	}{
		{[]byte("12345"), transport.ErrPayloadTooLarge},
		{[]byte("1234"), transport.ErrInvalidArgument},
	} {
		conn.reads <- packetRead{payload: test.payload}
		select {
		case err := <-errorsCh:
			if !errors.Is(err, test.want) {
				t.Fatalf("read error = %v, want %v", err, test.want)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("missing ordered receive error")
		}
	}
	if conn.addrPortCalls.Load() != 0 {
		t.Fatal("injected connection's ReadFrom override was bypassed")
	}
}

func TestNativeListenerOversizeThenValidAndConcurrentClose(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := net.DialUDP("udp4", nil, conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	packets := make(chan transport.ReceivedPacket, 1)
	errorsCh := make(chan error, 1)
	listener, err := transport.ServePacketConn(conn, transport.ListenerOptions{
		PathID: "native", MaxPayload: 4,
		OnPacket: func(packet transport.ReceivedPacket) { packets <- packet },
		OnError:  func(err error) { errorsCh <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	if _, err := client.Write([]byte("oversize")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errorsCh:
		if !errors.Is(err, transport.ErrPayloadTooLarge) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("native read did not reject oversize")
	}
	if _, err := client.Write([]byte("1234")); err != nil {
		t.Fatal(err)
	}
	packet := receivePacket(t, packets)
	if string(packet.Payload) != "1234" {
		t.Fatal("valid packet after oversized receive was lost")
	}
	results := make(chan error, 2)
	for range 2 {
		go func() { results <- listener.Close() }()
	}
	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("native read did not unblock on Close")
		}
	}
	if packet.Reply.Available() || packet.Context.Err() == nil {
		t.Fatal("native read retained an active route after Close")
	}
}
