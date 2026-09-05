//go:build linux

package transport

import (
	"encoding/binary"
	"errors"
	"math"
	"net"
	"net/netip"

	"golang.org/x/sys/unix"
)

type destinationCapture struct{ port int }

func newDestinationCapture(conn net.PacketConn) (*destinationCapture, error) {
	udp, ok := conn.(*net.UDPConn)
	if !ok || udp == nil {
		return nil, ErrDestinationUnsupported
	}
	local, ok := udp.LocalAddr().(*net.UDPAddr)
	if !ok || local == nil {
		return nil, ErrDestinationUnsupported
	}
	raw, err := udp.SyscallConn()
	if err != nil {
		return nil, errors.Join(ErrDestinationUnsupported, err)
	}
	var socketErr error
	err = raw.Control(func(fd uintptr) {
		if addressNetwork(local) == "udp4" {
			socketErr = setAndVerifySocketOption(int(fd), unix.IPPROTO_IP, unix.IP_PKTINFO, 1)
			return
		}
		socketErr = setAndVerifySocketOption(int(fd), unix.IPPROTO_IPV6, unix.IPV6_RECVPKTINFO, 1)
		if socketErr != nil {
			return
		}
		onlyIPv6, err := unix.GetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_V6ONLY)
		if err != nil {
			socketErr = err
		} else if onlyIPv6 == 0 {
			socketErr = setAndVerifySocketOption(int(fd), unix.IPPROTO_IP, unix.IP_PKTINFO, 1)
		}
	})
	if err != nil || socketErr != nil {
		return nil, errors.Join(ErrDestinationUnsupported, err, socketErr)
	}
	return &destinationCapture{port: local.Port}, nil
}

func (*destinationCapture) controlSize() int {
	return unix.CmsgSpace(unix.SizeofInet4Pktinfo) + unix.CmsgSpace(unix.SizeofInet6Pktinfo)
}

func (c *destinationCapture) parseControl(oob []byte, flags int, remote netip.AddrPort) (net.Addr, []byte, error) {
	if flags&(unix.MSG_CTRUNC|unix.MSG_TRUNC) != 0 {
		return nil, nil, invalidArgument("truncated destination or payload")
	}
	messages, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, nil, invalidArgument("invalid destination control messages")
	}
	var ipv4 *unix.Inet4Pktinfo
	var ipv6 *unix.Inet6Pktinfo
	controlBytes := 0
	for _, message := range messages {
		controlBytes += unix.CmsgSpace(len(message.Data))
		switch {
		case message.Header.Level == unix.IPPROTO_IP && message.Header.Type == unix.IP_PKTINFO:
			if ipv4 != nil || len(message.Data) != unix.SizeofInet4Pktinfo {
				return nil, nil, invalidArgument("duplicate or malformed IPv4 destination")
			}
			ipv4 = new(unix.Inet4Pktinfo)
			if _, err := binary.Decode(message.Data, binary.NativeEndian, ipv4); err != nil || ipv4.Ifindex < 1 {
				return nil, nil, invalidArgument("invalid IPv4 destination interface")
			}
		case message.Header.Level == unix.IPPROTO_IPV6 && message.Header.Type == unix.IPV6_PKTINFO:
			if ipv6 != nil || len(message.Data) != unix.SizeofInet6Pktinfo {
				return nil, nil, invalidArgument("duplicate or malformed IPv6 destination")
			}
			ipv6 = new(unix.Inet6Pktinfo)
			if _, err := binary.Decode(message.Data, binary.NativeEndian, ipv6); err != nil || ipv6.Ifindex < 1 || ipv6.Ifindex > math.MaxInt32 {
				return nil, nil, invalidArgument("invalid IPv6 destination interface")
			}
		}
	}
	if len(oob) > controlBytes {
		return nil, nil, invalidArgument("incomplete destination control message")
	}
	remoteIP := remote.Addr()
	if remoteIP.Is4() || remoteIP.Is4In6() {
		if ipv4 != nil {
			ip := netip.AddrFrom4(ipv4.Addr)
			if !validDestination(ip) {
				return nil, nil, invalidArgument("invalid IPv4 destination address")
			}
			source := unix.Inet4Pktinfo{Ifindex: ipv4.Ifindex, Spec_dst: ipv4.Addr}
			return &net.UDPAddr{IP: ip.AsSlice(), Port: c.port}, unix.PktInfo4(&source), nil
		}
		if ipv6 == nil || !netip.AddrFrom16(ipv6.Addr).Is4In6() {
			return nil, nil, invalidArgument("missing IPv4 destination")
		}
	} else if !remoteIP.Is6() || ipv6 == nil || netip.AddrFrom16(ipv6.Addr).Is4In6() {
		return nil, nil, invalidArgument("missing IPv6 destination")
	}
	ip := netip.AddrFrom16(ipv6.Addr)
	if !validDestination(ip) {
		return nil, nil, invalidArgument("invalid IPv6 destination address")
	}
	zone := ""
	if ip.IsLinkLocalUnicast() {
		device, err := net.InterfaceByIndex(int(ipv6.Ifindex))
		if err != nil {
			return nil, nil, invalidArgument("unknown IPv6 destination interface")
		}
		zone = device.Name
	}
	return &net.UDPAddr{IP: ip.AsSlice(), Port: c.port, Zone: zone}, unix.PktInfo6(ipv6), nil
}

func validDestination(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || ip.IsUnspecified() || ip.IsMulticast() {
		return false
	}
	return ip != netip.AddrFrom4([4]byte{255, 255, 255, 255})
}
