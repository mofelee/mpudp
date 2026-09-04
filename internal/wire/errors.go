package wire

import "errors"

var (
	// ErrMalformed identifies an invalid envelope, body length, or body shape.
	ErrMalformed = errors.New("malformed MPUDP packet")
	// ErrUnsupportedVersion identifies a packet for another protocol version.
	ErrUnsupportedVersion = errors.New("unsupported MPUDP protocol version")
	// ErrUnknownPacketType identifies a packet type outside the v0.1 set.
	ErrUnknownPacketType = errors.New("unknown MPUDP packet type")
	// ErrAuthentication identifies a failed HMAC verification.
	ErrAuthentication = errors.New("MPUDP packet authentication failed")
	// ErrInvalidSessionID identifies the reserved all-zero SessionID.
	ErrInvalidSessionID = errors.New("invalid MPUDP session ID")
	// ErrInvalidFEC identifies invalid shard counts or indexes.
	ErrInvalidFEC = errors.New("invalid MPUDP FEC metadata")
	// ErrInvalidCapability identifies an invalid UDP payload capability or limit.
	ErrInvalidCapability = errors.New("invalid MPUDP UDP payload capability")
	// ErrPacketTooLarge identifies a packet exceeding its UDP payload budget.
	ErrPacketTooLarge = errors.New("MPUDP wire packet too large")
	// ErrInvalidKey identifies an empty authentication key.
	ErrInvalidKey = errors.New("invalid MPUDP authentication key")
	// ErrInvalidHandler identifies a nil authenticated-message callback.
	ErrInvalidHandler = errors.New("invalid MPUDP authenticated handler")
)
