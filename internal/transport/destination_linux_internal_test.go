//go:build linux

package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"testing"

	"golang.org/x/sys/unix"
)

func TestDestinationControlRejectsIncompleteOrInvalidMetadata(t *testing.T) {
	ipv4 := unix.PktInfo4(&unix.Inet4Pktinfo{Ifindex: 1, Addr: [4]byte{127, 0, 0, 2}})
	ipv6 := unix.PktInfo6(&unix.Inet6Pktinfo{Ifindex: 1, Addr: netip.MustParseAddr("::1").As16()})
	shortInfo := make([]byte, unix.CmsgSpace(4))
	header := unix.Cmsghdr{Level: unix.IPPROTO_IP, Type: unix.IP_PKTINFO}
	header.SetLen(unix.CmsgLen(4))
	if _, err := binary.Encode(shortInfo, binary.NativeEndian, header); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, remote string
		oob          []byte
		flags        int
	}{
		{"missing", "127.0.0.1:9", nil, 0},
		{"short header", "127.0.0.1:9", []byte{1}, 0},
		{"invalid header", "127.0.0.1:9", make([]byte, unix.CmsgLen(0)), 0},
		{"short pktinfo", "127.0.0.1:9", shortInfo, 0},
		{"incomplete trailing header", "127.0.0.1:9", append(bytes.Clone(ipv4), 1), 0},
		{"duplicate IPv4", "127.0.0.1:9", append(bytes.Clone(ipv4), ipv4...), 0},
		{"duplicate IPv6", "[::1]:9", append(bytes.Clone(ipv6), ipv6...), 0},
		{"control truncated", "127.0.0.1:9", ipv4, unix.MSG_CTRUNC},
		{"payload truncated", "127.0.0.1:9", ipv4, unix.MSG_TRUNC},
		{"IPv6 only for IPv4", "127.0.0.1:9", ipv6, 0},
		{"IPv4 only for IPv6", "[::1]:9", ipv4, 0},
		{"zero IPv4 interface", "127.0.0.1:9", unix.PktInfo4(&unix.Inet4Pktinfo{Addr: [4]byte{127, 0, 0, 1}}), 0},
		{"negative IPv4 interface", "127.0.0.1:9", unix.PktInfo4(&unix.Inet4Pktinfo{Ifindex: -1, Addr: [4]byte{127, 0, 0, 1}}), 0},
		{"zero IPv6 interface", "[::1]:9", unix.PktInfo6(&unix.Inet6Pktinfo{Addr: netip.MustParseAddr("::1").As16()}), 0},
		{"overflow IPv6 interface", "[::1]:9", unix.PktInfo6(&unix.Inet6Pktinfo{Ifindex: 1 << 31, Addr: netip.MustParseAddr("::1").As16()}), 0},
		{"unspecified IPv4", "127.0.0.1:9", unix.PktInfo4(&unix.Inet4Pktinfo{Ifindex: 1}), 0},
		{"multicast IPv4", "127.0.0.1:9", unix.PktInfo4(&unix.Inet4Pktinfo{Ifindex: 1, Addr: [4]byte{224, 0, 0, 1}}), 0},
		{"broadcast IPv4", "127.0.0.1:9", unix.PktInfo4(&unix.Inet4Pktinfo{Ifindex: 1, Addr: [4]byte{255, 255, 255, 255}}), 0},
		{"unspecified IPv6", "[::1]:9", unix.PktInfo6(&unix.Inet6Pktinfo{Ifindex: 1}), 0},
		{"multicast IPv6", "[::1]:9", unix.PktInfo6(&unix.Inet6Pktinfo{Ifindex: 1, Addr: netip.MustParseAddr("ff02::1").As16()}), 0},
		{"mapped unspecified", "127.0.0.1:9", unix.PktInfo6(&unix.Inet6Pktinfo{Ifindex: 1, Addr: netip.MustParseAddr("::ffff:0.0.0.0").As16()}), 0},
		{"mapped broadcast", "127.0.0.1:9", unix.PktInfo6(&unix.Inet6Pktinfo{Ifindex: 1, Addr: netip.MustParseAddr("::ffff:255.255.255.255").As16()}), 0},
		{"mapped multicast", "127.0.0.1:9", unix.PktInfo6(&unix.Inet6Pktinfo{Ifindex: 1, Addr: netip.MustParseAddr("::ffff:224.0.0.1").As16()}), 0},
		{"mapped destination for IPv6", "[::1]:9", unix.PktInfo6(&unix.Inet6Pktinfo{Ifindex: 1, Addr: netip.MustParseAddr("::ffff:127.0.0.1").As16()}), 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			capture := destinationCapture{port: 4000}
			local, source, err := capture.parseControl(test.oob, test.flags, netip.MustParseAddrPort(test.remote))
			if !errors.Is(err, ErrInvalidArgument) || local != nil || source != nil {
				t.Fatalf("invalid metadata result = %v, %x, %v", local, source, err)
			}
		})
	}
}

func TestDestinationControlOwnsActualDestinationAndReplySource(t *testing.T) {
	for _, test := range []struct {
		name, remote, local string
		input, source       []byte
	}{
		{
			"IPv4 actual destination", "127.0.0.1:9", "127.0.0.2:4000",
			unix.PktInfo4(&unix.Inet4Pktinfo{Ifindex: 1, Addr: [4]byte{127, 0, 0, 2}, Spec_dst: [4]byte{127, 0, 0, 1}}),
			unix.PktInfo4(&unix.Inet4Pktinfo{Ifindex: 1, Spec_dst: [4]byte{127, 0, 0, 2}}),
		},
		{
			"dual stack both metadata", "[::ffff:127.0.0.1]:9", "127.0.0.2:4000",
			append(unix.PktInfo6(&unix.Inet6Pktinfo{Ifindex: 1, Addr: netip.MustParseAddr("::ffff:127.0.0.2").As16()}), unix.PktInfo4(&unix.Inet4Pktinfo{Ifindex: 1, Addr: [4]byte{127, 0, 0, 2}})...),
			unix.PktInfo4(&unix.Inet4Pktinfo{Ifindex: 1, Spec_dst: [4]byte{127, 0, 0, 2}}),
		},
		{
			"mapped fallback", "[::ffff:127.0.0.1]:9", "127.0.0.2:4000",
			unix.PktInfo6(&unix.Inet6Pktinfo{Ifindex: 1, Addr: netip.MustParseAddr("::ffff:127.0.0.2").As16()}),
			unix.PktInfo6(&unix.Inet6Pktinfo{Ifindex: 1, Addr: netip.MustParseAddr("::ffff:127.0.0.2").As16()}),
		},
		{
			"IPv6", "[::1]:9", "[::1]:4000",
			unix.PktInfo6(&unix.Inet6Pktinfo{Ifindex: 1, Addr: netip.MustParseAddr("::1").As16()}),
			unix.PktInfo6(&unix.Inet6Pktinfo{Ifindex: 1, Addr: netip.MustParseAddr("::1").As16()}),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			capture := destinationCapture{port: 4000}
			local, source, err := capture.parseControl(test.input, 0, netip.MustParseAddrPort(test.remote))
			if err != nil || local.String() != test.local || !bytes.Equal(source, test.source) {
				t.Fatalf("destination/source = %v / %x, error %v", local, source, err)
			}
			clear(test.input)
			if local.String() != test.local || !bytes.Equal(source, test.source) {
				t.Fatal("retained destination or source control aliases receive buffer")
			}
			clear(local.(*net.UDPAddr).IP)
			if !bytes.Equal(source, test.source) {
				t.Fatal("source control aliases returned address")
			}
		})
	}
}
