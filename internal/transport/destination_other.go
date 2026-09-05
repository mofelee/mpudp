//go:build !linux

package transport

import (
	"net"
	"net/netip"
)

type destinationCapture struct{}

func newDestinationCapture(net.PacketConn) (*destinationCapture, error) {
	return nil, ErrDestinationUnsupported
}

func (*destinationCapture) controlSize() int { return 0 }

func (*destinationCapture) parseControl([]byte, int, netip.AddrPort) (net.Addr, []byte, error) {
	return nil, nil, ErrDestinationUnsupported
}
