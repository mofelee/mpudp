// Package wire implements the authenticated MPUDP v0.1 wire format.
package wire

import "crypto/sha256"

const (
	// Magic is the fixed four-byte ASCII protocol discriminator.
	Magic = "MPUD"

	// Version is the only protocol version understood by this package.
	Version uint8 = 1

	// PrefixSize is the fixed packet prefix before the type-specific body.
	PrefixSize = 24
	// AuthenticationTagSize is the full HMAC-SHA-256 output size.
	AuthenticationTagSize = sha256.Size
	// MinimumPacketSize is a CLOSE packet with an empty body.
	MinimumPacketSize = PrefixSize + AuthenticationTagSize
	// HelloPacketSize is the fixed HELLO and HELLO_ACK wire size.
	HelloPacketSize = PrefixSize + helloBodySize + AuthenticationTagSize
	// ProbePacketSize is the fixed PING and PONG wire size.
	ProbePacketSize = PrefixSize + probeBodySize + AuthenticationTagSize

	// DataShardMetadataSize is the fixed DATA_SHARD body before shard bytes.
	DataShardMetadataSize = 15
	// DataShardOverhead includes the prefix, DATA_SHARD metadata, and tag.
	DataShardOverhead = PrefixSize + DataShardMetadataSize + AuthenticationTagSize

	// MinUDPPayload fits PING/PONG and a DATA_SHARD with one shard byte.
	MinUDPPayload = 72
	// MaxUDPPayload is the portable non-jumbogram IPv4 UDP payload maximum.
	MaxUDPPayload = 65507
)

const (
	magic0 = 'M'
	magic1 = 'P'
	magic2 = 'U'
	magic3 = 'D'
)

// PacketType identifies an MPUDP v0.1 packet body.
type PacketType uint8

const (
	TypeHello PacketType = iota + 1
	TypeHelloAck
	TypeDataShard
	TypePing
	TypePong
	TypeClose
)

const (
	helloBodySize = 4
	probeBodySize = 16
)
