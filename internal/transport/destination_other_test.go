//go:build !linux

package transport_test

import (
	"errors"
	"net"
	"testing"

	"github.com/mofelee/mpudp/internal/transport"
)

func TestRequiredDestinationUnsupportedPlatform(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	listener, err := transport.ServePacketConn(conn, transport.ListenerOptions{PathID: "native", RequireDestination: true})
	if listener != nil || !errors.Is(err, transport.ErrDestinationUnsupported) {
		t.Fatalf("non-Linux native constructor = %v, %v", listener, err)
	}
	if err := conn.SetReadBuffer(1024); err != nil {
		t.Fatalf("failed constructor acquired native connection: %v", err)
	}
}
