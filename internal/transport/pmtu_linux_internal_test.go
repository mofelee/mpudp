//go:build linux

package transport

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func TestLinuxDualStackListenerConfiguresIPv4AndIPv6PMTU(t *testing.T) {
	listener, err := OpenListener(context.Background(), "udp", ":0", ListenerOptions{PathID: "dual-stack-pmtu"})
	if err != nil {
		if errors.Is(err, unix.EAFNOSUPPORT) || errors.Is(err, unix.EPROTONOSUPPORT) {
			t.Skipf("IPv6 sockets unavailable: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	conn, ok := listener.conn.(syscall.Conn)
	if !ok {
		t.Fatalf("listener connection type %T does not expose syscall.Conn", listener.conn)
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}

	var domain, ipv6Only, ipv6PMTU, ipv4PMTU int
	var socketErr error
	err = raw.Control(func(fd uintptr) {
		domain, socketErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_DOMAIN)
		if socketErr != nil || domain != unix.AF_INET6 {
			return
		}
		ipv6Only, socketErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_V6ONLY)
		if socketErr != nil || ipv6Only != 0 {
			return
		}
		ipv6PMTU, socketErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_MTU_DISCOVER)
		if socketErr != nil {
			return
		}
		ipv4PMTU, socketErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_MTU_DISCOVER)
	})
	if err != nil {
		t.Fatalf("inspect listener socket: %v", err)
	}
	if socketErr != nil {
		t.Fatalf("inspect listener socket options: %v", socketErr)
	}
	if domain != unix.AF_INET6 {
		t.Skipf("dual-stack IPv6 wildcard unavailable: socket domain = %d", domain)
	}
	if ipv6Only != 0 {
		t.Skip("dual-stack IPv6 wildcard unavailable: IPV6_V6ONLY is enabled")
	}
	if ipv6PMTU != unix.IPV6_PMTUDISC_DO {
		t.Errorf("IPV6_MTU_DISCOVER = %d, want %d", ipv6PMTU, unix.IPV6_PMTUDISC_DO)
	}
	if ipv4PMTU != unix.IP_PMTUDISC_DO {
		t.Errorf("IP_MTU_DISCOVER = %d, want %d", ipv4PMTU, unix.IP_PMTUDISC_DO)
	}
}
