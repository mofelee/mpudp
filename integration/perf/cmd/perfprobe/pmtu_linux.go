//go:build linux

package main

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

func configureNativePMTU(c *net.UDPConn) error {
	raw, err := c.SyscallConn()
	if err != nil {
		return err
	}
	var socketErr error
	err = raw.Control(func(fd uintptr) {
		address, err := unix.Getsockname(int(fd))
		if err != nil {
			socketErr = err
			return
		}
		set := func(level, option, value int) error {
			if err := unix.SetsockoptInt(int(fd), level, option, value); err != nil {
				return err
			}
			got, err := unix.GetsockoptInt(int(fd), level, option)
			if err != nil {
				return err
			}
			if got != value {
				return fmt.Errorf("PMTU socket option %d = %d, expected %d", option, got, value)
			}
			return nil
		}
		switch address.(type) {
		case *unix.SockaddrInet4:
			socketErr = set(unix.IPPROTO_IP, unix.IP_MTU_DISCOVER, unix.IP_PMTUDISC_DO)
		case *unix.SockaddrInet6:
			if socketErr = set(unix.IPPROTO_IPV6, unix.IPV6_MTU_DISCOVER, unix.IPV6_PMTUDISC_DO); socketErr != nil {
				return
			}
			v6Only, err := unix.GetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_V6ONLY)
			if err != nil {
				socketErr = err
				return
			}
			if v6Only == 0 {
				socketErr = set(unix.IPPROTO_IP, unix.IP_MTU_DISCOVER, unix.IP_PMTUDISC_DO)
			}
		default:
			socketErr = fmt.Errorf("unsupported native UDP address family %T", address)
		}
	})
	if err != nil {
		return err
	}
	return socketErr
}
