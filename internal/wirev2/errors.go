package wirev2

import "errors"

var (
	ErrMalformed          = errors.New("malformed MPUDP v2 packet")
	ErrUnsupportedVersion = errors.New("unsupported MPUDP protocol version")
	ErrUnknownPacketType  = errors.New("unknown MPUDP v2 packet type")
	ErrAuthentication     = errors.New("MPUDP v2 packet authentication failed")
	ErrInvalidSessionID   = errors.New("invalid MPUDP session ID")
	ErrPacketTooLarge     = errors.New("MPUDP v2 packet too large")
	ErrInvalidKey         = errors.New("invalid MPUDP v2 key")
	ErrInvalidTLV         = errors.New("invalid MPUDP v2 handshake TLV")
	ErrRequiredTLV        = errors.New("unsupported required MPUDP v2 handshake TLV")
	ErrTranscript         = errors.New("MPUDP v2 handshake transcript mismatch")
)
