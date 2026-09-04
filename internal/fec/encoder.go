package fec

import (
	"fmt"
	"math"
	"sync"

	"github.com/klauspost/reedsolomon"
)

// EncodedBlock owns all returned shard bytes. Shards are equal length and
// share one backing allocation; callers must treat them as immutable.
type EncodedBlock struct {
	PacketID       uint64
	OriginalLength int
	Params         Params
	Shards         [][]byte
}

// Encoder assigns PacketIDs and encodes one sending direction. Its methods are
// safe for concurrent use. A Session must use a distinct Encoder per direction.
type Encoder struct {
	mu        sync.Mutex
	params    Params
	limits    Limits
	codec     reedsolomon.Encoder
	total     int
	next      uint64
	exhausted bool
}

// NewEncoder constructs a bounded encoder whose first PacketID is zero.
func NewEncoder(params Params, budget Budget) (*Encoder, error) {
	return newEncoderAt(params, budget, 0, false)
}

func newEncoderAt(params Params, budget Budget, next uint64, exhausted bool) (*Encoder, error) {
	limits, err := DeriveLimits(params, budget)
	if err != nil {
		return nil, err
	}
	codec, total, err := newCodec(params)
	if err != nil {
		return nil, err
	}
	return &Encoder{
		params:    params,
		limits:    limits,
		codec:     codec,
		total:     total,
		next:      next,
		exhausted: exhausted,
	}, nil
}

// Limits returns the immutable, checked limits used by this Encoder.
func (e *Encoder) Limits() Limits {
	return e.limits
}

// Encode turns exactly one Datagram into exactly one FEC block. Size rejection
// happens before taking a PacketID or allocating shard storage.
func (e *Encoder) Encode(payload []byte) (EncodedBlock, error) {
	if len(payload) > e.limits.EffectiveDatagramLimit {
		return EncodedBlock{}, ErrMessageTooLarge
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.exhausted {
		return EncodedBlock{}, ErrPacketIDExhausted
	}

	shardSize := shardSizeForDatagram(len(payload), e.params.DataShards)
	if shardSize > e.limits.ShardCapacity {
		return EncodedBlock{}, ErrMessageTooLarge
	}
	maxInt := int(^uint(0) >> 1)
	if shardSize > maxInt/e.total {
		return EncodedBlock{}, fmt.Errorf("%w: shard allocation overflows int", ErrEncode)
	}

	backing := make([]byte, e.total*shardSize)
	shards := make([][]byte, e.total)
	for index := range shards {
		start := index * shardSize
		end := start + shardSize
		shards[index] = backing[start:end:end]
	}
	copy(backing[:e.params.DataShards*shardSize], payload)
	if err := e.codec.Encode(shards); err != nil {
		return EncodedBlock{}, fmt.Errorf("%w: %v", ErrEncode, err)
	}

	packetID := e.next
	if packetID == math.MaxUint64 {
		e.exhausted = true
	} else {
		e.next++
	}
	return EncodedBlock{
		PacketID:       packetID,
		OriginalLength: len(payload),
		Params:         e.params,
		Shards:         shards,
	}, nil
}
