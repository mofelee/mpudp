// Package sessionv2 drives established fixed-budget Datagram Sessions. Its
// owner serializes calls and supplies explicit nondecreasing time. It owns no
// workers, timers, transport sockets or public API waiters.
package sessionv2

import (
	"errors"
	"io"
	"time"

	"github.com/mofelee/mpudp/internal/aggregationv2"
	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/handshakev2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/reassemblyv2"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
)

const (
	InitialQueue = iota
	InitialOriginalWindow
	InitialGroupWindow
	InitialControl
	InitialCount
)

const (
	DefaultPathRateBPS = 100000000
	ControlLifetime    = 5 * time.Second
	ControlRetry       = 250 * time.Millisecond
	ControlSends       = 8
	MaxSendsPerStep    = 256
)

var (
	ErrInvalid     = errors.New("invalid v2 Datagram controller operation")
	ErrUnsupported = errors.New("v2 Datagram controller policy unsupported")
	ErrClosed      = errors.New("v2 Datagram controller closed")
	ErrNotReady    = errors.New("v2 Datagram context is not ready")
	ErrReentrant   = errors.New("v2 Datagram callback reentered controller")
	ErrTime        = errors.New("v2 Datagram clock moved backwards")
	ErrExpired     = errors.New("v2 Datagram control deadline expired")
	ErrEntropy     = errors.New("v2 Datagram path entropy unavailable")
	ErrProtocol    = errors.New("invalid authenticated v2 Datagram transition")
	ErrExhausted   = errors.New("v2 Datagram identifier exhausted")
)

type Carrier struct {
	handshakev2.Carrier
	Sender transport.ReplyPath
}

type Config struct {
	LocalProfile       negotiationv2.Profile
	SendLimits         negotiationv2.SendLimits
	FixedPayloadBudget uint16
	Aggregation        bool
	MaxGroupBytes      uint32
	Queue              aggregationv2.Limits
	Reassembly         reassemblyv2.Limits
	MaxPendingGroups   int
	GroupTimeout       time.Duration
	Carriers           []Carrier
	BootstrapPath      transport.ReplyPath
	Entropy            io.Reader
	// Rates are local outbound operator settings. Missing paths use the
	// documented100Mbps default; every emitted packet pays full IP+UDP bytes.
	PathRatesBPS map[uint16]uint64
	// Emit makes one synchronous bounded socket attempt and borrows packet
	// until return. Queued adapters need separate ownership and completion.
	Emit func(transport.ReplyPath, []byte) error
}

type Receipt uint64
type Fence uint64

type SendAttempt struct {
	Type   wirev2.PacketType
	PathID uint16
	Bytes  int
	Err    error
}

// Result transfers owned deliveries to the caller. A caller that cannot queue
// a delivery must release it. CompletedThrough means every original shard
// through that admitted DatagramID finished a local socket attempt.
type Result struct {
	Sends            []SendAttempt
	Deliveries       []*reassemblyv2.Datagram
	CompletedThrough uint64
	SendError        error
	FailedFrom       uint64
	PathsChanged     bool
	Ready            bool
}

type PathSnapshot struct {
	PathID        uint16
	Generation    uint64
	Binding       handshakev2.Binding
	Active        bool
	SendBudget    uint16
	ReceiveBudget uint16
	SendEpoch     uint32
	ReceiveEpoch  uint32
	Pending       bool
}

type Snapshot struct {
	Started, Ready, Accepting, Closed bool
	AcceptedThrough, CompletedThrough uint64
	QueuedDatagrams, PendingGroups    int
	SendError                         error
	FailedFrom                        uint64
	NextDeadline                      time.Time
	Paths                             []PathSnapshot
}

// RequiredInitialClaims is called before handshake admission. Its four
// dedicated byte-only claims map to the Initial* indexes above. New consumes
// matching prepaid handles after handshake promotion, without re-reserving.
func RequiredInitialClaims(cfg Config) ([]creditv2.Claim, error) {
	return requiredInitialClaims(cfg)
}
