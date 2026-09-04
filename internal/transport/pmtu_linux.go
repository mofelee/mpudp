//go:build linux

package transport

import (
	"errors"
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

// PMTUDiscoverySupported reports whether this build can enforce Linux's
// do-not-fragment/path-MTU-discovery socket mode.
func PMTUDiscoverySupported() bool { return true }

func configurePMTU(conn syscall.Conn, network string) (bool, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return false, fmt.Errorf("configure MPUDP PMTU discovery: %w", err)
	}
	var socketErr error
	err = raw.Control(func(fd uintptr) {
		switch network {
		case "udp4":
			socketErr = setAndVerifySocketOption(int(fd), unix.IPPROTO_IP, unix.IP_MTU_DISCOVER, unix.IP_PMTUDISC_DO)
		case "udp6":
			socketErr = setAndVerifySocketOption(int(fd), unix.IPPROTO_IPV6, unix.IPV6_MTU_DISCOVER, unix.IPV6_PMTUDISC_DO)
			if socketErr != nil {
				break
			}
			ipv6Only, getErr := unix.GetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_V6ONLY)
			if getErr != nil {
				socketErr = getErr
				break
			}
			if ipv6Only == 0 {
				socketErr = setAndVerifySocketOption(int(fd), unix.IPPROTO_IP, unix.IP_MTU_DISCOVER, unix.IP_PMTUDISC_DO)
			}
		default:
			socketErr = fmt.Errorf("%w: cannot determine UDP address family", ErrPMTUUnsupported)
		}
	})
	if err != nil {
		return false, fmt.Errorf("configure MPUDP PMTU discovery: %w", err)
	}
	if socketErr != nil {
		return false, fmt.Errorf("configure MPUDP PMTU discovery: %w", socketErr)
	}
	return true, nil
}

func setAndVerifySocketOption(fd, level, option, value int) error {
	if err := unix.SetsockoptInt(fd, level, option, value); err != nil {
		return err
	}
	got, err := unix.GetsockoptInt(fd, level, option)
	if err != nil {
		return err
	}
	if got != value {
		return fmt.Errorf("socket option verification returned %d, want %d", got, value)
	}
	return nil
}

func isPathMTUError(err error) bool { return errors.Is(err, unix.EMSGSIZE) }
