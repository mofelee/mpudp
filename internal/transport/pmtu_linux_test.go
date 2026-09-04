//go:build linux

package transport_test

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/transport"
)

func TestLinuxIPv6PMTURejectsPacketBeyondLoopbackMTU(t *testing.T) {
	server, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })

	carrier, err := transport.OpenCarrier(context.Background(), "ipv6-pmtu", server.LocalAddr().String(), transport.CarrierOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = carrier.Close() })
	if !carrier.PMTUEnabled() {
		t.Fatal("Linux carrier did not enable and verify PMTU discovery")
	}

	// Linux loopback normally has MTU 65536. This is a valid UDP/IPv4 payload
	// size, but with IPv6 and DF it exceeds that path after IPv6+UDP headers.
	err = carrier.Send(context.Background(), make([]byte, transport.MaxUDPPayload))
	if !errors.Is(err, transport.ErrPathMTUExceeded) || !errors.Is(err, syscall.EMSGSIZE) {
		t.Fatalf("oversize IPv6 Send() error = %v, want PMTU/EMSGSIZE", err)
	}
	if !carrier.Available() {
		t.Fatal("PMTU error disabled Carrier")
	}

	if err := carrier.Send(context.Background(), []byte("small")); err != nil {
		t.Fatalf("small Send() after PMTU error = %v", err)
	}
	if err := server.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 64)
	n, _, err := server.ReadFromUDP(buffer)
	if err != nil {
		t.Fatal(err)
	}
	if string(buffer[:n]) != "small" {
		t.Fatalf("server received %d-byte datagram instead of later small packet", n)
	}
}
