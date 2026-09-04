package transport

import (
	"context"
	"fmt"
	"net"
)

type pmtuConnection interface {
	PMTUEnabled() bool
}

type udpConn struct {
	*net.UDPConn
	pmtuEnabled bool
}

func (c *udpConn) PMTUEnabled() bool { return c.pmtuEnabled }

func connectionPMTUEnabled(conn net.Conn) bool {
	status, ok := conn.(pmtuConnection)
	return ok && status.PMTUEnabled()
}

func dialUDP(ctx context.Context, remote string) (net.Conn, error) {
	connection, err := (&net.Dialer{}).DialContext(ctx, "udp", remote)
	if err != nil {
		return nil, err
	}
	udp, ok := connection.(*net.UDPConn)
	if !ok {
		_ = connection.Close()
		return nil, fmt.Errorf("%w: UDP dial returned %T", ErrInvalidArgument, connection)
	}
	enabled, err := configurePMTU(udp, addressNetwork(udp.RemoteAddr()))
	if err != nil {
		_ = udp.Close()
		return nil, err
	}
	return &udpConn{UDPConn: udp, pmtuEnabled: enabled}, nil
}

func addressNetwork(addr net.Addr) string {
	udp, ok := addr.(*net.UDPAddr)
	if !ok || udp == nil {
		return "udp"
	}
	if udp.IP.To4() != nil {
		return "udp4"
	}
	return "udp6"
}
