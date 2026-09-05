package wirev2

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

const (
	EncodingContextBodySize    = 24
	EncodingContextAckBodySize = 36
	FECBundlePrefixSize        = 4
	FECRecordHeaderSize        = 18
	MaxFECRecords              = 16
	MaxFECDescriptors          = 256
	MinimumFECLogicalBytes     = 24
	MaxFECLogicalBytes         = 16 * 1024 * 1024
	MaxFECShardBytes           = MaxUDPPayload - TypedBodyOverhead - FECBundlePrefixSize - FECRecordHeaderSize
)

var (
	ErrInvalidContext     = errors.New("invalid MPUDP v2 encoding context")
	ErrContextUnavailable = errors.New("MPUDP v2 encoding context unavailable")
	ErrContextAck         = errors.New("MPUDP v2 encoding context acknowledgement mismatch")
)

// EncodingContext is the immutable equal-length RS profile for an epoch.
// Receiving a valid record does not admit or acknowledge it. Every context,
// including epoch one, needs caller-owned admission and exact ACK state.
type EncodingContext struct {
	Epoch           uint32
	LayoutID        uint16
	ProtectionID    uint16
	DataShards      uint8
	ParityShards    uint8
	ShardBytes      uint16
	MaxDescriptors  uint16
	MaxLogicalBytes uint32
}

// Validate checks shared wire/codec maxima only. Negotiated resources and the
// eligible paths' safe budgets must further constrain context admission.
func (c EncodingContext) Validate() error {
	if c.Epoch == 0 || c.LayoutID != 1 || c.ProtectionID != 1 || c.DataShards == 0 || c.ParityShards == 0 || int(c.DataShards)+int(c.ParityShards) > 256 {
		return ErrInvalidContext
	}
	if c.ShardBytes == 0 || int(c.ShardBytes) > MaxFECShardBytes || c.MaxDescriptors == 0 || c.MaxDescriptors > MaxFECDescriptors {
		return ErrInvalidContext
	}
	if c.MaxLogicalBytes < MinimumFECLogicalBytes || c.MaxLogicalBytes > MaxFECLogicalBytes || uint64(c.MaxLogicalBytes) > uint64(c.DataShards)*uint64(c.ShardBytes) {
		return ErrInvalidContext
	}
	return nil
}

func encodeContext(context EncodingContext) ([EncodingContextBodySize]byte, error) {
	var body [EncodingContextBodySize]byte
	if err := context.Validate(); err != nil {
		return body, err
	}
	binary.BigEndian.PutUint32(body[:4], context.Epoch)
	binary.BigEndian.PutUint16(body[4:6], context.LayoutID)
	binary.BigEndian.PutUint16(body[6:8], context.ProtectionID)
	body[8], body[9] = context.DataShards, context.ParityShards
	binary.BigEndian.PutUint16(body[10:12], context.ShardBytes)
	binary.BigEndian.PutUint16(body[12:14], context.MaxDescriptors)
	binary.BigEndian.PutUint32(body[16:20], context.MaxLogicalBytes)
	return body, nil
}

// AppendEncodingContext appends the exact 24-byte context plus route/envelope.
func AppendEncodingContext(dst []byte, sessionID SessionID, route Route, context EncodingContext, key Key) ([]byte, error) {
	body, err := encodeContext(context)
	if err != nil {
		return dst, err
	}
	return AppendEstablished(dst, Header{Type: TypeEncodingContext, SessionID: sessionID}, route, body[:], key)
}

func DecodeEncodingContext(envelope AuthenticatedEnvelope) (Route, EncodingContext, error) {
	message, err := DecodeEstablished(envelope)
	if err != nil {
		return Route{}, EncodingContext{}, err
	}
	if message.Header.Type != TypeEncodingContext {
		return Route{}, EncodingContext{}, ErrUnknownPacketType
	}
	body := message.Body
	if len(body) != EncodingContextBodySize || !allZero(body[14:16]) || !allZero(body[20:24]) {
		return Route{}, EncodingContext{}, ErrMalformed
	}
	context := EncodingContext{
		Epoch: binary.BigEndian.Uint32(body[:4]), LayoutID: binary.BigEndian.Uint16(body[4:6]), ProtectionID: binary.BigEndian.Uint16(body[6:8]),
		DataShards: body[8], ParityShards: body[9], ShardBytes: binary.BigEndian.Uint16(body[10:12]), MaxDescriptors: binary.BigEndian.Uint16(body[12:14]), MaxLogicalBytes: binary.BigEndian.Uint32(body[16:20]),
	}
	if err := context.Validate(); err != nil {
		return Route{}, EncodingContext{}, err
	}
	return message.Route, context, nil
}

type EncodingContextAck struct {
	Epoch  uint32
	Digest Digest
}

// NewEncodingContextAck binds the exact canonical 24-byte typed body, excluding
// route, outer envelope and tag. It does not record or authorize admission.
func NewEncodingContextAck(context EncodingContext) (EncodingContextAck, error) {
	body, err := encodeContext(context)
	if err != nil {
		return EncodingContextAck{}, err
	}
	return EncodingContextAck{Epoch: context.Epoch, Digest: sha256.Sum256(body[:])}, nil
}

func (ack EncodingContextAck) ValidateContext(context EncodingContext) error {
	expected, err := NewEncodingContextAck(context)
	if err != nil {
		return err
	}
	if ack != expected {
		return ErrContextAck
	}
	return nil
}

func AppendEncodingContextAck(dst []byte, sessionID SessionID, route Route, ack EncodingContextAck, key Key) ([]byte, error) {
	if ack.Epoch == 0 {
		return dst, ErrInvalidContext
	}
	var body [EncodingContextAckBodySize]byte
	binary.BigEndian.PutUint32(body[:4], ack.Epoch)
	copy(body[4:], ack.Digest[:])
	return AppendEstablished(dst, Header{Type: TypeEncodingContextAck, SessionID: sessionID}, route, body[:], key)
}

func DecodeEncodingContextAck(envelope AuthenticatedEnvelope) (Route, EncodingContextAck, error) {
	message, err := DecodeEstablished(envelope)
	if err != nil {
		return Route{}, EncodingContextAck{}, err
	}
	if message.Header.Type != TypeEncodingContextAck {
		return Route{}, EncodingContextAck{}, ErrUnknownPacketType
	}
	if len(message.Body) != EncodingContextAckBodySize {
		return Route{}, EncodingContextAck{}, ErrMalformed
	}
	ack := EncodingContextAck{Epoch: binary.BigEndian.Uint32(message.Body[:4])}
	copy(ack.Digest[:], message.Body[4:])
	if ack.Epoch == 0 {
		return Route{}, EncodingContextAck{}, ErrInvalidContext
	}
	return message.Route, ack, nil
}
