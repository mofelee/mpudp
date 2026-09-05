package wirev2

import (
	"encoding/binary"
	"errors"
)

var ErrInvalidFECBundle = errors.New("invalid MPUDP v2 FEC bundle")

// ContextLookup must be read-only and return only contexts usable under the
// caller's negotiated admission/ACK state and receive-grace rules. It must not
// learn epochs or reserve storage from a lookup. Each distinct referenced
// epoch is queried at most once per codec call and cached by value.
type ContextLookup func(epoch uint32) (EncodingContext, bool)

// FECRecord borrows immutable payload bytes when encoding. Decoded records own
// their payloads in one shared allocation, with capacities capped per record.
type FECRecord struct {
	GroupID       uint64
	EncodingEpoch uint32
	LogicalBytes  uint32
	ShardIndex    uint8
	Payload       []byte
}

type FECBundle struct {
	Header  Header
	Route   Route
	Records []FECRecord
}

type contextCache struct {
	lookup   ContextLookup
	contexts [MaxFECRecords]EncodingContext
	count    int
}

func (c *contextCache) get(epoch uint32) (EncodingContext, error) {
	if epoch == 0 {
		return EncodingContext{}, ErrInvalidContext
	}
	for i := 0; i < c.count; i++ {
		if c.contexts[i].Epoch == epoch {
			return c.contexts[i], nil
		}
	}
	if c.lookup == nil {
		return EncodingContext{}, ErrContextUnavailable
	}
	context, ok := c.lookup(epoch)
	if !ok {
		return EncodingContext{}, ErrContextUnavailable
	}
	if context.Epoch != epoch {
		return EncodingContext{}, ErrInvalidContext
	}
	if err := context.Validate(); err != nil {
		return EncodingContext{}, err
	}
	c.contexts[c.count] = context
	c.count++
	return context, nil
}

func validateFECRecord(record FECRecord, context EncodingContext) error {
	if record.GroupID == 0 || record.LogicalBytes < MinimumFECLogicalBytes || record.LogicalBytes > context.MaxLogicalBytes ||
		int(record.ShardIndex) >= int(context.DataShards)+int(context.ParityShards) || len(record.Payload) != int(context.ShardBytes) {
		return ErrInvalidFECBundle
	}
	if record.ShardIndex < context.DataShards {
		start := uint64(record.ShardIndex) * uint64(context.ShardBytes)
		used := 0
		if uint64(record.LogicalBytes) > start {
			used = int(min(uint64(record.LogicalBytes)-start, uint64(context.ShardBytes)))
		}
		if !allZero(record.Payload[used:]) {
			return ErrInvalidFECBundle
		}
	}
	return nil
}

func duplicateGroup(records []FECRecord, groupID uint64) bool {
	for _, record := range records {
		if record.GroupID == groupID {
			return true
		}
	}
	return false
}

// AppendFECBundle validates the complete bundle before allocating its wire
// body. All inputs must remain immutable until return. maxPayload is the
// caller-validated current send budget for this route, within512..65507.
// This does not admit groups, reserve credits, or mutate context/ACK state.
func AppendFECBundle(dst []byte, bundle FECBundle, lookup ContextLookup, key Key, maxPayload int) ([]byte, error) {
	if bundle.Header.Type != TypeFECBundle {
		return dst, ErrUnknownPacketType
	}
	if bundle.Header.SessionID == (SessionID{}) {
		return dst, ErrInvalidSessionID
	}
	if key == (Key{}) {
		return dst, ErrInvalidKey
	}
	if err := validateRoute(bundle.Header.Type, bundle.Route); err != nil {
		return dst, err
	}
	if err := validatePayloadLimit(maxPayload); err != nil {
		return dst, err
	}
	if len(bundle.Records) < 1 || len(bundle.Records) > MaxFECRecords {
		return dst, ErrInvalidFECBundle
	}
	cache := contextCache{lookup: lookup}
	typedBytes := FECBundlePrefixSize
	for i, record := range bundle.Records {
		if duplicateGroup(bundle.Records[:i], record.GroupID) {
			return dst, ErrInvalidFECBundle
		}
		context, err := cache.get(record.EncodingEpoch)
		if err != nil {
			return dst, err
		}
		if err := validateFECRecord(record, context); err != nil {
			return dst, err
		}
		recordBytes := FECRecordHeaderSize + len(record.Payload)
		if recordBytes > maxPayload-TypedBodyOverhead-typedBytes {
			return dst, ErrPacketTooLarge
		}
		typedBytes += recordBytes
	}
	bodyBytes := RouteSize + typedBytes
	var body []byte
	if dst == nil {
		// Place the body at its final offset so AppendEnvelope can frame and
		// authenticate this allocation without moving it or growing dst.
		packet := make([]byte, EnvelopeOverhead+bodyBytes)
		dst, body = packet[:0], packet[PrefixSize:PrefixSize+bodyBytes]
	} else {
		// Caller-owned destination capacity may overlap any input record.
		body = make([]byte, bodyBytes)
	}
	encodeRoute(body, bundle.Route)
	binary.BigEndian.PutUint16(body[RouteSize:RouteSize+2], uint16(len(bundle.Records)))
	offset := RouteSize + FECBundlePrefixSize
	for _, record := range bundle.Records {
		binary.BigEndian.PutUint64(body[offset:offset+8], record.GroupID)
		binary.BigEndian.PutUint32(body[offset+8:offset+12], record.EncodingEpoch)
		binary.BigEndian.PutUint32(body[offset+12:offset+16], record.LogicalBytes)
		body[offset+16] = record.ShardIndex
		copy(body[offset+FECRecordHeaderSize:], record.Payload)
		offset += FECRecordHeaderSize + len(record.Payload)
	}
	return AppendEnvelope(dst, bundle.Header, body, key)
}

// DecodeFECBundle requires an immutable authenticated packet and the actual
// receive budget applicable to its route (including any caller-approved old
// route grace). It validates every record and exact full-shard boundary before
// allocating owned payloads. Unknown contexts reject the entire bundle.
// Data-shard padding is checked here; parity consistency and reconstructed
// padding/manifest validation belong to fecv2. Runtime ledgers remain untouched.
func DecodeFECBundle(envelope AuthenticatedEnvelope, lookup ContextLookup, maxPayload int) (FECBundle, error) {
	message, err := DecodeEstablished(envelope)
	if err != nil {
		return FECBundle{}, err
	}
	if message.Header.Type != TypeFECBundle {
		return FECBundle{}, ErrUnknownPacketType
	}
	if err := validatePayloadLimit(maxPayload); err != nil {
		return FECBundle{}, err
	}
	if len(envelope.envelope.packet) > maxPayload {
		return FECBundle{}, ErrPacketTooLarge
	}
	body := message.Body
	if len(body) < FECBundlePrefixSize || !allZero(body[2:4]) {
		return FECBundle{}, ErrMalformed
	}
	count := int(binary.BigEndian.Uint16(body[:2]))
	if count < 1 || count > MaxFECRecords {
		return FECBundle{}, ErrInvalidFECBundle
	}
	var records [MaxFECRecords]FECRecord
	cache := contextCache{lookup: lookup}
	offset := FECBundlePrefixSize
	payloadBytes := 0
	for i := 0; i < count; i++ {
		if len(body)-offset < FECRecordHeaderSize {
			return FECBundle{}, ErrMalformed
		}
		record := FECRecord{
			GroupID: binary.BigEndian.Uint64(body[offset : offset+8]), EncodingEpoch: binary.BigEndian.Uint32(body[offset+8 : offset+12]),
			LogicalBytes: binary.BigEndian.Uint32(body[offset+12 : offset+16]), ShardIndex: body[offset+16],
		}
		if body[offset+17] != 0 || duplicateGroup(records[:i], record.GroupID) {
			return FECBundle{}, ErrInvalidFECBundle
		}
		context, err := cache.get(record.EncodingEpoch)
		if err != nil {
			return FECBundle{}, err
		}
		offset += FECRecordHeaderSize
		if int(context.ShardBytes) > len(body)-offset {
			return FECBundle{}, ErrMalformed
		}
		end := offset + int(context.ShardBytes)
		record.Payload = body[offset:end:end]
		if err := validateFECRecord(record, context); err != nil {
			return FECBundle{}, err
		}
		records[i] = record
		payloadBytes += len(record.Payload)
		offset = end
	}
	if offset != len(body) {
		return FECBundle{}, ErrMalformed
	}
	owned := make([]byte, payloadBytes)
	result := FECBundle{Header: message.Header, Route: message.Route, Records: make([]FECRecord, count)}
	offset = 0
	for i, record := range records[:count] {
		end := offset + len(record.Payload)
		copy(owned[offset:end], record.Payload)
		record.Payload = owned[offset:end:end]
		result.Records[i] = record
		offset = end
	}
	return result, nil
}
