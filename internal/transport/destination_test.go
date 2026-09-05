package transport_test

import (
	"errors"
	"net"
	"testing"

	"github.com/mofelee/mpudp/internal/transport"
)

type wrappedUDPConn struct{ *net.UDPConn }

func TestRequiredDestinationRejectsInjectedConnectionsBeforeOwnership(t *testing.T) {
	conn := newFakePacketConn("local")
	defer conn.Close()
	listener, err := transport.ServePacketConn(conn, transport.ListenerOptions{PathID: "fake", RequireDestination: true})
	if listener != nil || !errors.Is(err, transport.ErrDestinationUnsupported) {
		t.Fatalf("fake strict constructor = %v, %v", listener, err)
	}
	select {
	case <-conn.closed:
		t.Fatal("failed constructor acquired fake connection")
	default:
	}
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	listener, err = transport.ServePacketConn(&wrappedUDPConn{udp}, transport.ListenerOptions{PathID: "wrapped", RequireDestination: true})
	if listener != nil || !errors.Is(err, transport.ErrDestinationUnsupported) {
		t.Fatalf("wrapped strict constructor = %v, %v", listener, err)
	}
	if err := udp.SetReadBuffer(1024); err != nil {
		t.Fatalf("failed constructor acquired native wrapped connection: %v", err)
	}
}
