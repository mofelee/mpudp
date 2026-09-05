//go:build linux

package main

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestNativePMTUOptions(t *testing.T) {
	for _, address := range []string{"127.0.0.1:0", "[::1]:0", "[::]:0"} {
		t.Run(address, func(t *testing.T) {
			a, err := net.ResolveUDPAddr("udp", address)
			if err != nil {
				t.Fatal(err)
			}
			c, err := net.ListenUDP("udp", a)
			if err != nil {
				t.Skipf("address unavailable: %v", err)
			}
			defer c.Close()
			if err := configureNativePMTU(c); err != nil {
				t.Fatal(err)
			}
			raw, err := c.SyscallConn()
			if err != nil {
				t.Fatal(err)
			}
			if err := raw.Control(func(fd uintptr) {
				level, option, want := unix.IPPROTO_IP, unix.IP_MTU_DISCOVER, unix.IP_PMTUDISC_DO
				if a.IP.To4() == nil {
					level, option, want = unix.IPPROTO_IPV6, unix.IPV6_MTU_DISCOVER, unix.IPV6_PMTUDISC_DO
				}
				got, err := unix.GetsockoptInt(int(fd), level, option)
				if err != nil || got != want {
					t.Errorf("PMTU mode=%d, error=%v, expected %d", got, err, want)
				}
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func assertNoFragment(t *testing.T, network, address string, oversize int) {
	t.Helper()
	a, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		t.Fatal(err)
	}
	server, err := net.ListenUDP(network, a)
	if err != nil {
		t.Skipf("network unavailable: %v", err)
	}
	defer server.Close()
	client, err := net.DialUDP(network, nil, server.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := configureNativePMTU(client); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(make([]byte, oversize)); !errors.Is(err, unix.EMSGSIZE) {
		t.Fatalf("oversize error=%v, expected EMSGSIZE", err)
	}
	if _, err := client.Write([]byte("small")); err != nil {
		t.Fatal(err)
	}
	if err := server.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, oversize)
	n, _, err := server.ReadFromUDP(b)
	if err != nil || string(b[:n]) != "small" {
		t.Fatalf("received fragmented oversize payload: n=%d err=%v", n, err)
	}
}

func TestNativeIPv6PMTURejectsOversize(t *testing.T) {
	assertNoFragment(t, "udp6", "[::1]:0", 65507)
}

func TestNativeIPv4LowMTUNamespace(t *testing.T) {
	if os.Getenv("MPUDP_PERF_MTU_CHILD") == "1" {
		if b, err := exec.Command("ip", "link", "set", "lo", "up", "mtu", "1280").CombinedOutput(); err != nil {
			t.Fatalf("configure isolated loopback: %v: %s", err, b)
		}
		assertNoFragment(t, "udp4", "127.0.0.1:0", 1400)
		return
	}
	if os.Getenv("MPUDP_PERF_PRIVILEGED_TESTS") != "1" {
		t.Skip("set MPUDP_PERF_PRIVILEGED_TESTS=1 for isolated low-MTU network namespace test")
	}
	command := exec.Command("unshare", "--net", "--", os.Args[0], "-test.run=^TestNativeIPv4LowMTUNamespace$")
	command.Env = append(os.Environ(), "MPUDP_PERF_MTU_CHILD=1")
	if b, err := command.CombinedOutput(); err != nil {
		t.Fatalf("isolated low-MTU test: %v: %s", err, b)
	}
}
