// Package transport owns UDP socket lifecycles and sends already encoded MPUDP
// packets. It deliberately does not parse wire packets or create FEC shards.
package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
)

const (
	// MaxUDPPayload is the protocol-independent UDP payload hard limit.
	MaxUDPPayload = 65507
)

var (
	ErrClosed             = errors.New("MPUDP transport closed")
	ErrInvalidArgument    = errors.New("MPUDP transport invalid argument")
	ErrPathUnavailable    = errors.New("MPUDP path unavailable")
	ErrGenerationReplaced = errors.New("MPUDP path generation replaced")
	ErrPayloadTooLarge    = errors.New("MPUDP transport payload too large")
	ErrPathMTUExceeded    = errors.New("MPUDP path MTU exceeded")
	ErrPMTUUnsupported    = errors.New("MPUDP path MTU discovery unsupported")
	ErrNoAvailablePaths   = errors.New("MPUDP has no available send path")
	ErrPartialSend        = errors.New("MPUDP block partially sent")
	ErrAllSendsFailed     = errors.New("MPUDP block send failed on every shard")
)

// Path is one current Carrier or learned listener Endpoint. Send must make one
// datagram attempt and must never split payload into smaller datagrams.
type Path interface {
	PathID() string
	Available() bool
	Send(context.Context, []byte) error
}

// ReplyPath preserves the exact local socket and remote Endpoint associated
// with a received packet.
type ReplyPath interface {
	Path
	Generation() uint64
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

// ReceivedPacket is delivered by a Carrier or Listener read loop. Payload is
// an owned copy and may be retained by the handler. Context is canceled when
// the socket generation is replaced or closed.
type ReceivedPacket struct {
	Payload    []byte
	PathID     string
	Generation uint64
	LocalAddr  net.Addr
	RemoteAddr net.Addr
	Reply      ReplyPath
	Context    context.Context
}

type PacketHandler func(ReceivedPacket)
type ErrorHandler func(error)

// PayloadSizeError reports lengths only; it never formats packet contents.
type PayloadSizeError struct {
	Size  int
	Limit int
}

func (e *PayloadSizeError) Error() string {
	return fmt.Sprintf("MPUDP UDP payload size %d exceeds limit %d", e.Size, e.Limit)
}

func (e *PayloadSizeError) Unwrap() error { return ErrPayloadTooLarge }

// PathError intentionally omits its underlying error text. Callers can still
// inspect it with errors.Is/errors.As without accidentally logging payloads
// embedded by an injected connection implementation.
type PathError struct {
	PathID     string
	Generation uint64
	Operation  string
	Err        error
}

func (e *PathError) Error() string {
	return fmt.Sprintf("MPUDP path %q generation %d %s failed", e.PathID, e.Generation, e.Operation)
}

func (e *PathError) Unwrap() error { return e.Err }

func invalidArgument(name string) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, name)
}

func cloneAddr(addr net.Addr) net.Addr {
	switch value := addr.(type) {
	case *net.UDPAddr:
		if value == nil {
			return nil
		}
		clone := *value
		clone.IP = append(net.IP(nil), value.IP...)
		return &clone
	case *net.IPAddr:
		if value == nil {
			return nil
		}
		clone := *value
		clone.IP = append(net.IP(nil), value.IP...)
		return &clone
	default:
		return addr
	}
}
