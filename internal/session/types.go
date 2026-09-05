package session

import (
	"context"
	"net"
	"time"

	"github.com/mofelee/mpudp/internal/fec"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wire"
)

// Role fixes which handshake message establishes a Session.
type Role uint8

const (
	RoleInitiator Role = iota + 1
	RoleListener
)

// State is the externally observable Session lifecycle state.
type State uint8

const (
	StateHandshaking State = iota + 1
	StateEstablished
	StateHandshakeFailed
	StateClosed
)

// Clock supplies deterministic time to the state machine.
type Clock interface {
	Now() time.Time
}

// Path is implemented by transport Carrier and ReplyPath values and by tests.
type Path interface {
	PathID() string
	Available() bool
	Send(context.Context, []byte) error
}

// ReplyPath retains the exact socket generation and source Endpoint associated
// with a received packet. transport.ReplyPath satisfies this interface.
type ReplyPath interface {
	Path
	Generation() uint64
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

// Config contains immutable state-machine and FEC resource bounds. The public
// config package is mapped to this structure by the integration layer.
type Config struct {
	PSK                       []byte
	FEC                       fec.Params
	LocalMaxUDPPayload        int
	MaxDatagramSize           int
	MaxEndpoints              int
	MaxHandshakeAttempts      int
	MaxPendingFECBlocks       int
	MaxCompletedFECBlocks     int
	DecodeTimeout             time.Duration
	CompletionTTL             time.Duration
	EndpointTTL               time.Duration
	KeepaliveInterval         time.Duration
	HandshakeRetryInterval    time.Duration
	HandshakeRetryJitterLimit time.Duration
	Clock                     Clock
	FECStatistics             *fec.Counters
}

// ListenerConfig adds the global authenticated Session bound.
type ListenerConfig struct {
	Session     Config
	MaxSessions int
}

// ReceivedPacket is the subset of transport.ReceivedPacket required by the
// state machine. NewReceivedPacket provides a zero-copy adapter.
type ReceivedPacket struct {
	Payload []byte
	Reply   ReplyPath
}

// NewReceivedPacket adapts a transport packet without copying its owned bytes.
func NewReceivedPacket(packet transport.ReceivedPacket) ReceivedPacket {
	return ReceivedPacket{Payload: packet.Payload, Reply: packet.Reply}
}

// SendAttempt is one control-packet send. It intentionally contains no packet bytes.
type SendAttempt struct {
	Type   wire.PacketType
	PathID string
	Err    error
}

// AdvanceResult reports due actions performed by Advance.
type AdvanceResult struct {
	Sends            []SendAttempt
	ExpiredEndpoints int
	ExpiredFEC       fec.ExpireStats
	HandshakeFailed  bool
}

// HandleResult reports one authenticated packet's state-machine effects.
// Message.DataShard.Payload aliases the ReceivedPacket payload.
type HandleResult struct {
	Message           wire.Message
	Created           bool
	Established       bool
	EndpointAdded     bool
	EndpointRefreshed bool
	RTTUpdated        bool
	RTT               time.Duration
	Datagram          []byte
	Response          *SendAttempt
}

// EndpointSnapshot is immutable diagnostic and scheduler state.
type EndpointSnapshot struct {
	Key          string
	PathID       string
	Generation   uint64
	LocalAddr    string
	RemoteAddr   string
	LastActivity time.Time
	RTT          time.Duration
	HasRTT       bool
	Available    bool
}

// Snapshot is immutable Session state. Send and receive budgets are named
// separately even though v0.1 computes both as min(local, peer).
type Snapshot struct {
	ID                     wire.SessionID
	Role                   Role
	State                  State
	PeerMaxUDPPayload      int
	SendMaxUDPPayload      int
	ReceiveMaxUDPPayload   int
	HandshakeAttempts      int
	AcknowledgedCarriers   int
	Endpoints              int
	OutstandingProbes      int
	HasRetryDeadline       bool
	HasKeepaliveDeadline   bool
	HasDecodeSweepDeadline bool
	NextDeadline           time.Time
}

// WriteResult describes a one-shot FEC block send. DATA is never retried.
type WriteResult struct {
	PacketID uint64
	Send     transport.BlockSendResult
}
