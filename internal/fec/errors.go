package fec

import "errors"

var (
	// ErrInvalidParameters means the Reed-Solomon data/parity counts are invalid.
	ErrInvalidParameters = errors.New("invalid FEC parameters")
	// ErrInvalidBudget means a UDP payload budget or resource limit is invalid.
	ErrInvalidBudget = errors.New("invalid FEC payload budget")
	// ErrInvalidDecoderConfig means a decoder timeout or capacity is invalid.
	ErrInvalidDecoderConfig = errors.New("invalid FEC decoder configuration")
	// ErrMessageTooLarge means a Datagram exceeds the effective sending limit.
	ErrMessageTooLarge = errors.New("FEC message too large")
	// ErrPacketIDExhausted means every uint64 PacketID has been consumed.
	ErrPacketIDExhausted = errors.New("FEC packet ID exhausted")
	// ErrInvalidShard means a shard is outside the negotiated protocol bounds.
	ErrInvalidShard = errors.New("invalid FEC shard")
	// ErrInconsistentBlock means shards for one block disagree on metadata.
	ErrInconsistentBlock = errors.New("inconsistent FEC block metadata")
	// ErrConflictingShard means an index was repeated with different payload.
	ErrConflictingShard = errors.New("conflicting FEC shard")
	// ErrDecoderFull means accepting a new block would exceed the pending bound.
	ErrDecoderFull = errors.New("FEC decoder pending block limit reached")
	// ErrClosed means an operation targeted a closed Decoder.
	ErrClosed = errors.New("FEC decoder closed")
	// ErrEncode classifies an unexpected Reed-Solomon encoding failure.
	ErrEncode = errors.New("FEC encode failed")
	// ErrDecode classifies an unexpected Reed-Solomon reconstruction failure.
	ErrDecode = errors.New("FEC decode failed")
)
