package transport_test

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/transport"
)

func TestLoopbackCarriersUseDistinctLongLivedSourcePorts(t *testing.T) {
	const carrierCount = 3
	type endpoint struct {
		server  *net.UDPConn
		carrier *transport.Carrier
	}
	endpoints := make([]endpoint, 0, carrierCount)
	for i := 0; i < carrierCount; i++ {
		server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		carrier, err := transport.OpenCarrier(context.Background(), "carrier-"+strconv.Itoa(i), server.LocalAddr().String(), transport.CarrierOptions{})
		if err != nil {
			_ = server.Close()
			t.Fatal(err)
		}
		endpoints = append(endpoints, endpoint{server: server, carrier: carrier})
	}
	t.Cleanup(func() {
		for _, endpoint := range endpoints {
			_ = endpoint.carrier.Close()
			_ = endpoint.server.Close()
		}
	})

	sourcePorts := make(map[int]struct{}, carrierCount)
	for i, endpoint := range endpoints {
		local, ok := endpoint.carrier.LocalAddr().(*net.UDPAddr)
		if !ok {
			t.Fatalf("carrier %d local address type = %T", i, endpoint.carrier.LocalAddr())
		}
		if local.Port == 0 {
			t.Fatalf("carrier %d has zero local port", i)
		}
		if _, duplicate := sourcePorts[local.Port]; duplicate {
			t.Fatalf("carrier %d reused source port %d from another Carrier", i, local.Port)
		}
		sourcePorts[local.Port] = struct{}{}

		for _, payload := range [][]byte{[]byte("first"), []byte("second")} {
			if err := endpoint.carrier.Send(context.Background(), payload); err != nil {
				t.Fatalf("carrier %d Send() error = %v", i, err)
			}
			got, remote := readLoopbackUDP(t, endpoint.server)
			if string(got) != string(payload) {
				t.Fatalf("carrier %d payload = %q, want %q", i, got, payload)
			}
			if remote.Port != local.Port {
				t.Fatalf("carrier %d source port changed: got %d, want %d", i, remote.Port, local.Port)
			}
		}
		if transport.PMTUDiscoverySupported() && !endpoint.carrier.PMTUEnabled() {
			t.Fatalf("carrier %d did not verify Linux PMTU discovery", i)
		}
	}
}

func TestLoopbackListenerReplyKeepsListeningSource(t *testing.T) {
	packets := make(chan transport.ReceivedPacket, 1)
	listener, err := transport.OpenListener(context.Background(), "udp4", "127.0.0.1:0", transport.ListenerOptions{
		PathID:   "listener-loopback",
		OnPacket: func(packet transport.ReceivedPacket) { packets <- packet },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	client, err := net.DialUDP("udp4", nil, listener.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.Write([]byte("request")); err != nil {
		t.Fatal(err)
	}
	packet := receivePacket(t, packets)
	if packet.LocalAddr.String() != listener.LocalAddr().String() {
		t.Fatalf("packet local address = %v, listener = %v", packet.LocalAddr, listener.LocalAddr())
	}
	if packet.RemoteAddr.String() != client.LocalAddr().String() {
		t.Fatalf("packet remote address = %v, client = %v", packet.RemoteAddr, client.LocalAddr())
	}
	if err := packet.Reply.Send(context.Background(), []byte("response")); err != nil {
		t.Fatal(err)
	}
	if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 64)
	n, remote, err := client.ReadFromUDP(buffer)
	if err != nil {
		t.Fatal(err)
	}
	if string(buffer[:n]) != "response" {
		t.Fatalf("reply payload = %q", buffer[:n])
	}
	if remote.String() != listener.LocalAddr().String() {
		t.Fatalf("reply source = %v, want listening socket %v", remote, listener.LocalAddr())
	}
	if transport.PMTUDiscoverySupported() && !listener.PMTUEnabled() {
		t.Fatal("listener did not verify Linux PMTU discovery")
	}
}

func readLoopbackUDP(t *testing.T, conn *net.UDPConn) ([]byte, *net.UDPAddr) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1024)
	n, remote, err := conn.ReadFromUDP(buffer)
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), buffer[:n]...), remote
}
