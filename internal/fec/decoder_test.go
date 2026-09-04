package fec

import (
	"bytes"
	"errors"
	"fmt"
	"math/bits"
	"sync"
	"testing"
	"time"
)

type decoderTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func newDecoderTestClock() *decoderTestClock {
	return &decoderTestClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *decoderTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *decoderTestClock) Advance(delta time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delta)
	c.mu.Unlock()
}

func decoderTestBudget() Budget {
	return Budget{
		MaxUDPPayload:         512,
		DataShardWireOverhead: 72,
		MaxDatagramSize:       1_320,
	}
}

func decoderTestConfig(clock Clock) DecoderConfig {
	return DecoderConfig{
		Params:             Params{DataShards: 3, ParityShards: 2},
		Budget:             decoderTestBudget(),
		DecodeTimeout:      10 * time.Second,
		CompletionTTL:      30 * time.Second,
		MaxPendingBlocks:   8,
		MaxCompletedBlocks: 8,
		Clock:              clock,
	}
}

func newDecoderForTest(t *testing.T, config DecoderConfig) *Decoder {
	t.Helper()
	decoder, err := NewDecoder(config)
	if err != nil {
		t.Fatalf("NewDecoder() error = %v", err)
	}
	t.Cleanup(func() {
		if err := decoder.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return decoder
}

func encodeDecoderTestBlock(t *testing.T, params Params, payload []byte) EncodedBlock {
	t.Helper()
	encoder, err := NewEncoder(params, decoderTestBudget())
	if err != nil {
		t.Fatalf("NewEncoder() error = %v", err)
	}
	block, err := encoder.Encode(payload)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	return block
}

func decoderTestShard(block EncodedBlock, key BlockKey, index int) IncomingShard {
	return IncomingShard{
		Key:            key,
		Params:         block.Params,
		Index:          index,
		OriginalLength: block.OriginalLength,
		Payload:        block.Shards[index],
	}
}

func TestDecoderRS5_3RecoversEveryZeroOneOrTwoShardLoss(t *testing.T) {
	params := Params{DataShards: 3, ParityShards: 2}
	payload := make([]byte, 1_019)
	for index := range payload {
		payload[index] = byte(index*31 + 7)
	}
	block := encodeDecoderTestBlock(t, params, payload)

	for missing := 0; missing < 1<<len(block.Shards); missing++ {
		losses := bits.OnesCount(uint(missing))
		if losses > params.ParityShards {
			continue
		}
		t.Run(fmt.Sprintf("losses_%02b", missing), func(t *testing.T) {
			clock := newDecoderTestClock()
			decoder := newDecoderForTest(t, decoderTestConfig(clock))
			key := BlockKey{SessionID: [16]byte{1}, PacketID: uint64(missing + 1)}
			completions := 0
			arrivals := 0
			for index := len(block.Shards) - 1; index >= 0; index-- {
				if missing&(1<<index) != 0 {
					continue
				}
				result, err := decoder.AddVerifiedShard(decoderTestShard(block, key, index))
				if err != nil {
					t.Fatalf("AddVerifiedShard(index=%d) error = %v", index, err)
				}
				arrivals++
				if arrivals < params.DataShards && result.Outcome != OutcomePending {
					t.Fatalf("arrival %d outcome = %v, want pending", arrivals, result.Outcome)
				}
				if arrivals == params.DataShards {
					if result.Outcome != OutcomeComplete {
						t.Fatalf("kth arrival outcome = %v, want complete", result.Outcome)
					}
					if !bytes.Equal(result.Datagram, payload) {
						t.Fatal("recovered Datagram differs from input")
					}
					completions++
				}
				if arrivals > params.DataShards && result.Outcome != OutcomeDuplicate {
					t.Fatalf("late arrival outcome = %v, want duplicate", result.Outcome)
				}
			}
			if completions != 1 {
				t.Fatalf("completion count = %d, want 1", completions)
			}
			stats := decoder.Stats()
			if stats.PendingBlocks != 0 || stats.CompletedBlocks != 1 {
				t.Fatalf("Stats() = %+v, want no pending and one completed", stats)
			}
		})
	}
}

func TestDecoderInsufficientShardsExpireWithoutDelivery(t *testing.T) {
	clock := newDecoderTestClock()
	decoder := newDecoderForTest(t, decoderTestConfig(clock))
	payload := bytes.Repeat([]byte("timeout"), 17)
	block := encodeDecoderTestBlock(t, decoder.params, payload)
	key := BlockKey{SessionID: [16]byte{2}, PacketID: 20}

	for _, index := range []int{0, 4} {
		result, err := decoder.AddVerifiedShard(decoderTestShard(block, key, index))
		if err != nil {
			t.Fatalf("AddVerifiedShard(index=%d) error = %v", index, err)
		}
		if result.Outcome != OutcomePending || result.Datagram != nil {
			t.Fatalf("AddVerifiedShard(index=%d) = %+v, want pending without delivery", index, result)
		}
	}
	stats := decoder.Stats()
	if stats.PendingBlocks != 1 || stats.PendingShards != 2 || stats.PendingBytes != 2*len(block.Shards[0]) {
		t.Fatalf("Stats() = %+v, want one two-shard block", stats)
	}

	clock.Advance(10 * time.Second)
	if expired := decoder.Sweep(); expired != (ExpireStats{PendingBlocks: 1}) {
		t.Fatalf("Sweep() = %+v, want one pending expiry", expired)
	}
	if stats := decoder.Stats(); stats != (Stats{}) {
		t.Fatalf("Stats() after expiry = %+v, want zero", stats)
	}

	result, err := decoder.AddVerifiedShard(decoderTestShard(block, key, 2))
	if err != nil {
		t.Fatalf("AddVerifiedShard() after expiry error = %v", err)
	}
	if result.Outcome != OutcomePending {
		t.Fatalf("AddVerifiedShard() after expiry outcome = %v, want pending", result.Outcome)
	}
}

func TestDecoderKthDistinctShardCompletesSynchronously(t *testing.T) {
	decoder := newDecoderForTest(t, decoderTestConfig(newDecoderTestClock()))
	payload := bytes.Repeat([]byte{0xa5}, 401)
	block := encodeDecoderTestBlock(t, decoder.params, payload)
	key := BlockKey{SessionID: [16]byte{3}, PacketID: 30}

	for _, index := range []int{4, 1} {
		result, err := decoder.AddVerifiedShard(decoderTestShard(block, key, index))
		if err != nil || result.Outcome != OutcomePending {
			t.Fatalf("AddVerifiedShard(index=%d) = %+v, %v; want pending", index, result, err)
		}
	}
	result, err := decoder.AddVerifiedShard(decoderTestShard(block, key, 3))
	if err != nil {
		t.Fatalf("kth AddVerifiedShard() error = %v", err)
	}
	if result.Outcome != OutcomeComplete || !bytes.Equal(result.Datagram, payload) {
		t.Fatalf("kth AddVerifiedShard() = %+v, want immediate original Datagram", result)
	}
}

func TestDecoderRecoversCanonicalEmptyDatagram(t *testing.T) {
	decoder := newDecoderForTest(t, decoderTestConfig(newDecoderTestClock()))
	block := encodeDecoderTestBlock(t, decoder.params, []byte{})
	key := BlockKey{SessionID: [16]byte{0x03}, PacketID: 31}
	var result Result
	for _, index := range []int{4, 2, 0} {
		var err error
		result, err = decoder.AddVerifiedShard(decoderTestShard(block, key, index))
		if err != nil {
			t.Fatalf("AddVerifiedShard(index=%d) error = %v", index, err)
		}
	}
	if result.Outcome != OutcomeComplete {
		t.Fatalf("empty recovery outcome = %v, want complete", result.Outcome)
	}
	if result.Datagram == nil || len(result.Datagram) != 0 {
		t.Fatalf("empty recovery Datagram = %#v, want non-nil zero-length slice", result.Datagram)
	}
}

func TestDecoderDuplicateConflictAndOutOfOrder(t *testing.T) {
	decoder := newDecoderForTest(t, decoderTestConfig(newDecoderTestClock()))
	payload := bytes.Repeat([]byte("out-of-order"), 29)
	block := encodeDecoderTestBlock(t, decoder.params, payload)
	key := BlockKey{SessionID: [16]byte{4}, PacketID: 40}
	first := decoderTestShard(block, key, 4)

	result, err := decoder.AddVerifiedShard(first)
	if err != nil || result.Outcome != OutcomePending {
		t.Fatalf("first AddVerifiedShard() = %+v, %v; want pending", result, err)
	}
	result, err = decoder.AddVerifiedShard(first)
	if err != nil || result.Outcome != OutcomeDuplicate {
		t.Fatalf("identical AddVerifiedShard() = %+v, %v; want duplicate", result, err)
	}
	conflict := first
	conflict.Payload = bytes.Clone(first.Payload)
	conflict.Payload[0] ^= 0xff
	if _, err := decoder.AddVerifiedShard(conflict); !errors.Is(err, ErrConflictingShard) {
		t.Fatalf("conflicting AddVerifiedShard() error = %v, want ErrConflictingShard", err)
	}
	if stats := decoder.Stats(); stats.PendingShards != 1 || stats.PendingBytes != len(first.Payload) {
		t.Fatalf("Stats() after duplicates = %+v, want one retained shard", stats)
	}

	for _, index := range []int{2, 0} {
		result, err = decoder.AddVerifiedShard(decoderTestShard(block, key, index))
		if err != nil {
			t.Fatalf("AddVerifiedShard(index=%d) error = %v", index, err)
		}
	}
	if result.Outcome != OutcomeComplete || !bytes.Equal(result.Datagram, payload) {
		t.Fatalf("out-of-order recovery = %+v, want original Datagram", result)
	}
}

func TestDecoderRejectsInvalidAndInconsistentShardMetadata(t *testing.T) {
	clock := newDecoderTestClock()
	decoder := newDecoderForTest(t, decoderTestConfig(clock))
	payload := bytes.Repeat([]byte{0x5c}, 100)
	block := encodeDecoderTestBlock(t, decoder.params, payload)
	key := BlockKey{SessionID: [16]byte{5}, PacketID: 50}
	valid := decoderTestShard(block, key, 0)

	tests := []struct {
		name   string
		mutate func(*IncomingShard)
	}{
		{name: "FEC mismatch", mutate: func(shard *IncomingShard) { shard.Params = Params{DataShards: 2, ParityShards: 3} }},
		{name: "negative index", mutate: func(shard *IncomingShard) { shard.Index = -1 }},
		{name: "index at total", mutate: func(shard *IncomingShard) { shard.Index = 5 }},
		{name: "negative original length", mutate: func(shard *IncomingShard) { shard.OriginalLength = -1 }},
		{name: "original length above limit", mutate: func(shard *IncomingShard) { shard.OriginalLength = decoder.limits.EffectiveDatagramLimit + 1 }},
		{name: "short payload", mutate: func(shard *IncomingShard) { shard.Payload = shard.Payload[:len(shard.Payload)-1] }},
		{name: "long payload", mutate: func(shard *IncomingShard) { shard.Payload = append(bytes.Clone(shard.Payload), 0) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := valid
			test.mutate(&input)
			if _, err := decoder.AddVerifiedShard(input); !errors.Is(err, ErrInvalidShard) {
				t.Fatalf("AddVerifiedShard() error = %v, want ErrInvalidShard", err)
			}
			if stats := decoder.Stats(); stats != (Stats{}) {
				t.Fatalf("invalid input retained state: %+v", stats)
			}
		})
	}

	if _, err := decoder.AddVerifiedShard(valid); err != nil {
		t.Fatalf("valid AddVerifiedShard() error = %v", err)
	}
	inconsistent := decoderTestShard(block, key, 1)
	inconsistent.OriginalLength++
	if _, err := decoder.AddVerifiedShard(inconsistent); !errors.Is(err, ErrInconsistentBlock) {
		t.Fatalf("inconsistent AddVerifiedShard() error = %v, want ErrInconsistentBlock", err)
	}
	if stats := decoder.Stats(); stats.PendingBlocks != 1 || stats.PendingShards != 1 {
		t.Fatalf("Stats() after inconsistent input = %+v, want original state only", stats)
	}
}

func TestDecoderKeysIsolateSessionsAndPacketIDs(t *testing.T) {
	decoder := newDecoderForTest(t, decoderTestConfig(newDecoderTestClock()))
	params := decoder.params
	payloadA := bytes.Repeat([]byte{'A'}, 91)
	payloadB := bytes.Repeat([]byte{'B'}, 91)
	payloadC := bytes.Repeat([]byte{'C'}, 91)
	blockA := encodeDecoderTestBlock(t, params, payloadA)
	blockB := encodeDecoderTestBlock(t, params, payloadB)
	blockC := encodeDecoderTestBlock(t, params, payloadC)
	keyA := BlockKey{SessionID: [16]byte{0x10}, PacketID: 7}
	keyB := BlockKey{SessionID: [16]byte{0x20}, PacketID: 7}
	keyC := BlockKey{SessionID: [16]byte{0x10}, PacketID: 8}

	for _, pair := range []struct {
		block EncodedBlock
		key   BlockKey
		index int
	}{
		{blockA, keyA, 0},
		{blockA, keyA, 1},
		{blockB, keyB, 2},
		{blockC, keyC, 2},
	} {
		result, err := decoder.AddVerifiedShard(decoderTestShard(pair.block, pair.key, pair.index))
		if err != nil || result.Outcome != OutcomePending {
			t.Fatalf("isolated AddVerifiedShard(%+v, %d) = %+v, %v; want pending", pair.key, pair.index, result, err)
		}
	}

	for _, test := range []struct {
		name    string
		block   EncodedBlock
		key     BlockKey
		indexes []int
		want    []byte
	}{
		{name: "first session", block: blockA, key: keyA, indexes: []int{2}, want: payloadA},
		{name: "second session", block: blockB, key: keyB, indexes: []int{0, 1}, want: payloadB},
		{name: "second packet", block: blockC, key: keyC, indexes: []int{0, 1}, want: payloadC},
	} {
		t.Run(test.name, func(t *testing.T) {
			var result Result
			for _, index := range test.indexes {
				var err error
				result, err = decoder.AddVerifiedShard(decoderTestShard(test.block, test.key, index))
				if err != nil {
					t.Fatalf("AddVerifiedShard(index=%d) error = %v", index, err)
				}
			}
			if result.Outcome != OutcomeComplete || !bytes.Equal(result.Datagram, test.want) {
				t.Fatalf("recovery = %+v, want isolated payload", result)
			}
		})
	}
}

func TestDecoderLateShardsDoNotRedeliverWithinCompletionTTL(t *testing.T) {
	clock := newDecoderTestClock()
	decoder := newDecoderForTest(t, decoderTestConfig(clock))
	payload := bytes.Repeat([]byte("late"), 67)
	block := encodeDecoderTestBlock(t, decoder.params, payload)
	key := BlockKey{SessionID: [16]byte{6}, PacketID: 60}

	for _, index := range []int{0, 3, 4} {
		result, err := decoder.AddVerifiedShard(decoderTestShard(block, key, index))
		if err != nil {
			t.Fatalf("initial AddVerifiedShard(index=%d) error = %v", index, err)
		}
		if index == 4 && result.Outcome != OutcomeComplete {
			t.Fatalf("third initial shard outcome = %v, want complete", result.Outcome)
		}
	}
	for _, index := range []int{1, 2, 0, 4} {
		result, err := decoder.AddVerifiedShard(decoderTestShard(block, key, index))
		if err != nil {
			t.Fatalf("late AddVerifiedShard(index=%d) error = %v", index, err)
		}
		if result.Outcome != OutcomeDuplicate || result.Datagram != nil {
			t.Fatalf("late AddVerifiedShard(index=%d) = %+v, want duplicate without delivery", index, result)
		}
	}
}

func TestDecoderCopiesRetainedShardPayload(t *testing.T) {
	decoder := newDecoderForTest(t, decoderTestConfig(newDecoderTestClock()))
	payload := make([]byte, 307)
	for index := range payload {
		payload[index] = byte(index)
	}
	block := encodeDecoderTestBlock(t, decoder.params, payload)
	key := BlockKey{SessionID: [16]byte{7}, PacketID: 70}
	input := decoderTestShard(block, key, 0)
	callerBuffer := bytes.Clone(input.Payload)
	input.Payload = callerBuffer

	if result, err := decoder.AddVerifiedShard(input); err != nil || result.Outcome != OutcomePending {
		t.Fatalf("first AddVerifiedShard() = %+v, %v; want pending", result, err)
	}
	for index := range callerBuffer {
		callerBuffer[index] ^= 0xff
	}
	for _, index := range []int{3, 4} {
		result, err := decoder.AddVerifiedShard(decoderTestShard(block, key, index))
		if err != nil {
			t.Fatalf("AddVerifiedShard(index=%d) error = %v", index, err)
		}
		if index == 4 && (result.Outcome != OutcomeComplete || !bytes.Equal(result.Datagram, payload)) {
			t.Fatalf("recovery after caller mutation = %+v, want original Datagram", result)
		}
	}
}

func TestDecoderPendingDeadlineIsFixedFromFirstShard(t *testing.T) {
	clock := newDecoderTestClock()
	decoder := newDecoderForTest(t, decoderTestConfig(clock))
	block := encodeDecoderTestBlock(t, decoder.params, bytes.Repeat([]byte{1}, 100))
	key := BlockKey{SessionID: [16]byte{8}, PacketID: 80}

	if _, err := decoder.AddVerifiedShard(decoderTestShard(block, key, 0)); err != nil {
		t.Fatalf("first AddVerifiedShard() error = %v", err)
	}
	clock.Advance(9 * time.Second)
	if _, err := decoder.AddVerifiedShard(decoderTestShard(block, key, 1)); err != nil {
		t.Fatalf("second AddVerifiedShard() error = %v", err)
	}
	clock.Advance(time.Second)
	if expired := decoder.Sweep(); expired.PendingBlocks != 1 {
		t.Fatalf("Sweep() = %+v, want original deadline to expire block", expired)
	}
}

func TestDecoderPendingCapacityRejectsThenRecoversAfterExpiry(t *testing.T) {
	clock := newDecoderTestClock()
	config := decoderTestConfig(clock)
	config.MaxPendingBlocks = 1
	decoder := newDecoderForTest(t, config)
	block := encodeDecoderTestBlock(t, decoder.params, bytes.Repeat([]byte{2}, 100))
	firstKey := BlockKey{SessionID: [16]byte{9}, PacketID: 1}
	secondKey := BlockKey{SessionID: [16]byte{9}, PacketID: 2}

	if _, err := decoder.AddVerifiedShard(decoderTestShard(block, firstKey, 0)); err != nil {
		t.Fatalf("first block AddVerifiedShard() error = %v", err)
	}
	if _, err := decoder.AddVerifiedShard(decoderTestShard(block, secondKey, 0)); !errors.Is(err, ErrDecoderFull) {
		t.Fatalf("second block AddVerifiedShard() error = %v, want ErrDecoderFull", err)
	}
	if stats := decoder.Stats(); stats.PendingBlocks != 1 || stats.PendingShards != 1 {
		t.Fatalf("Stats() at capacity = %+v, want only first block", stats)
	}

	clock.Advance(config.DecodeTimeout)
	result, err := decoder.AddVerifiedShard(decoderTestShard(block, secondKey, 0))
	if err != nil || result.Outcome != OutcomePending {
		t.Fatalf("AddVerifiedShard() after opportunistic expiry = %+v, %v; want pending", result, err)
	}
	if _, exists := decoder.pending[firstKey]; exists {
		t.Fatal("expired first key is still pending")
	}
}

func TestDecoderCompletionCacheTTLAndDeterministicCapacity(t *testing.T) {
	params := Params{DataShards: 1, ParityShards: 1}

	t.Run("equal deadlines evict lexical key", func(t *testing.T) {
		clock := newDecoderTestClock()
		config := decoderTestConfig(clock)
		config.Params = params
		config.MaxCompletedBlocks = 2
		decoder := newDecoderForTest(t, config)
		keys := []BlockKey{
			{SessionID: [16]byte{0x22}, PacketID: 2},
			{SessionID: [16]byte{0x22}, PacketID: 1},
			{SessionID: [16]byte{0x22}, PacketID: 3},
		}
		blocks := make([]EncodedBlock, len(keys))
		for index := range keys {
			blocks[index] = encodeDecoderTestBlock(t, params, []byte{byte(index + 1)})
			result, err := decoder.AddVerifiedShard(decoderTestShard(blocks[index], keys[index], 0))
			if err != nil || result.Outcome != OutcomeComplete {
				t.Fatalf("completion %d = %+v, %v; want complete", index, result, err)
			}
		}
		if stats := decoder.Stats(); stats.CompletedBlocks != 2 {
			t.Fatalf("Stats() = %+v, want completion capacity 2", stats)
		}
		if _, exists := decoder.completed[keys[1]]; exists {
			t.Fatalf("lexically earliest key %+v was not evicted", keys[1])
		}
		if _, exists := decoder.completed[keys[0]]; !exists {
			t.Fatalf("key %+v unexpectedly evicted", keys[0])
		}
	})

	t.Run("duplicates do not refresh TTL", func(t *testing.T) {
		clock := newDecoderTestClock()
		config := decoderTestConfig(clock)
		config.Params = params
		config.CompletionTTL = 10 * time.Second
		decoder := newDecoderForTest(t, config)
		block := encodeDecoderTestBlock(t, params, []byte("ttl"))
		key := BlockKey{SessionID: [16]byte{0x23}, PacketID: 1}
		input := decoderTestShard(block, key, 0)
		if result, err := decoder.AddVerifiedShard(input); err != nil || result.Outcome != OutcomeComplete {
			t.Fatalf("initial completion = %+v, %v", result, err)
		}
		clock.Advance(9 * time.Second)
		if result, err := decoder.AddVerifiedShard(input); err != nil || result.Outcome != OutcomeDuplicate {
			t.Fatalf("pre-expiry duplicate = %+v, %v", result, err)
		}
		clock.Advance(time.Second)
		result, err := decoder.AddVerifiedShard(input)
		if err != nil || result.Outcome != OutcomeComplete {
			t.Fatalf("post-expiry AddVerifiedShard() = %+v, %v; want a new completion", result, err)
		}
	})
}

func TestDecoderHighCardinalityStateRemainsBounded(t *testing.T) {
	t.Run("pending", func(t *testing.T) {
		clock := newDecoderTestClock()
		config := decoderTestConfig(clock)
		config.MaxPendingBlocks = 8
		decoder := newDecoderForTest(t, config)
		block := encodeDecoderTestBlock(t, decoder.params, []byte("pending-cardinality"))
		for packetID := uint64(0); packetID < 100; packetID++ {
			key := BlockKey{SessionID: [16]byte{0x30}, PacketID: packetID}
			_, err := decoder.AddVerifiedShard(decoderTestShard(block, key, 0))
			if packetID < uint64(config.MaxPendingBlocks) && err != nil {
				t.Fatalf("AddVerifiedShard(packet=%d) error = %v", packetID, err)
			}
			if packetID >= uint64(config.MaxPendingBlocks) && !errors.Is(err, ErrDecoderFull) {
				t.Fatalf("AddVerifiedShard(packet=%d) error = %v, want ErrDecoderFull", packetID, err)
			}
			if stats := decoder.Stats(); stats.PendingBlocks > config.MaxPendingBlocks {
				t.Fatalf("pending state exceeded capacity: %+v", stats)
			}
		}
		if decoder.pendingExpiry.Len() != config.MaxPendingBlocks {
			t.Fatalf("pending expiry heap length = %d, want %d", decoder.pendingExpiry.Len(), config.MaxPendingBlocks)
		}
		clock.Advance(config.DecodeTimeout)
		decoder.Sweep()
		if stats := decoder.Stats(); stats != (Stats{}) {
			t.Fatalf("Stats() after pending sweep = %+v, want zero", stats)
		}
	})

	t.Run("completed", func(t *testing.T) {
		clock := newDecoderTestClock()
		config := decoderTestConfig(clock)
		config.Params = Params{DataShards: 1, ParityShards: 1}
		config.MaxCompletedBlocks = 7
		decoder := newDecoderForTest(t, config)
		block := encodeDecoderTestBlock(t, config.Params, []byte("completed-cardinality"))
		for packetID := uint64(0); packetID < 100; packetID++ {
			key := BlockKey{SessionID: [16]byte{0x31}, PacketID: packetID}
			result, err := decoder.AddVerifiedShard(decoderTestShard(block, key, 0))
			if err != nil || result.Outcome != OutcomeComplete {
				t.Fatalf("AddVerifiedShard(packet=%d) = %+v, %v; want complete", packetID, result, err)
			}
			if stats := decoder.Stats(); stats.CompletedBlocks > config.MaxCompletedBlocks {
				t.Fatalf("completion state exceeded capacity: %+v", stats)
			}
		}
		if decoder.completedExpiry.Len() != config.MaxCompletedBlocks {
			t.Fatalf("completion expiry heap length = %d, want %d", decoder.completedExpiry.Len(), config.MaxCompletedBlocks)
		}
	})
}

func TestDecoderConcurrentArrivalDeliversExactlyOnce(t *testing.T) {
	decoder := newDecoderForTest(t, decoderTestConfig(newDecoderTestClock()))
	payload := bytes.Repeat([]byte("concurrent"), 101)
	block := encodeDecoderTestBlock(t, decoder.params, payload)
	key := BlockKey{SessionID: [16]byte{0x40}, PacketID: 400}
	const repeats = 20
	type response struct {
		result Result
		err    error
	}
	responses := make(chan response, repeats*len(block.Shards))
	var wait sync.WaitGroup
	for repeat := 0; repeat < repeats; repeat++ {
		for index := range block.Shards {
			wait.Add(1)
			go func(index int) {
				defer wait.Done()
				result, err := decoder.AddVerifiedShard(decoderTestShard(block, key, index))
				responses <- response{result: result, err: err}
			}(index)
		}
	}
	wait.Wait()
	close(responses)

	completions := 0
	for response := range responses {
		if response.err != nil {
			t.Fatalf("concurrent AddVerifiedShard() error = %v", response.err)
		}
		if response.result.Outcome == OutcomeComplete {
			completions++
			if !bytes.Equal(response.result.Datagram, payload) {
				t.Fatal("concurrent recovery differs from input")
			}
		}
	}
	if completions != 1 {
		t.Fatalf("concurrent completion count = %d, want 1", completions)
	}
	if stats := decoder.Stats(); stats.PendingBlocks != 0 || stats.CompletedBlocks != 1 {
		t.Fatalf("Stats() = %+v, want one completed block only", stats)
	}
}

func TestDecoderCloseIsIdempotentDropsStateAndTakesPrecedence(t *testing.T) {
	decoder, err := NewDecoder(decoderTestConfig(newDecoderTestClock()))
	if err != nil {
		t.Fatalf("NewDecoder() error = %v", err)
	}
	block := encodeDecoderTestBlock(t, decoder.params, []byte("close"))
	completedKey := BlockKey{SessionID: [16]byte{0x50}, PacketID: 1}
	for _, index := range []int{0, 1, 2} {
		if _, err := decoder.AddVerifiedShard(decoderTestShard(block, completedKey, index)); err != nil {
			t.Fatalf("completed block AddVerifiedShard(index=%d) error = %v", index, err)
		}
	}
	pendingKey := BlockKey{SessionID: [16]byte{0x50}, PacketID: 2}
	if _, err := decoder.AddVerifiedShard(decoderTestShard(block, pendingKey, 0)); err != nil {
		t.Fatalf("pending block AddVerifiedShard() error = %v", err)
	}

	const closers = 8
	errorsByCloser := make(chan error, closers)
	var wait sync.WaitGroup
	for range closers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errorsByCloser <- decoder.Close()
		}()
	}
	wait.Wait()
	close(errorsByCloser)
	for err := range errorsByCloser {
		if err != nil {
			t.Fatalf("concurrent Close() error = %v", err)
		}
	}
	if err := decoder.Close(); err != nil {
		t.Fatalf("repeated Close() error = %v", err)
	}
	if stats := decoder.Stats(); stats != (Stats{}) {
		t.Fatalf("Stats() after Close = %+v, want zero", stats)
	}
	if len(decoder.pendingExpiry) != 0 || len(decoder.completedExpiry) != 0 {
		t.Fatal("Close() retained expiry heap entries")
	}
	if _, err := decoder.AddVerifiedShard(IncomingShard{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("invalid AddVerifiedShard() after Close error = %v, want ErrClosed", err)
	}
	if expired := decoder.Sweep(); expired != (ExpireStats{}) {
		t.Fatalf("Sweep() after Close = %+v, want zero", expired)
	}
}

func TestNewDecoderRejectsInvalidConfig(t *testing.T) {
	clock := newDecoderTestClock()
	tests := []struct {
		name   string
		mutate func(*DecoderConfig)
		want   error
	}{
		{name: "invalid params", mutate: func(config *DecoderConfig) { config.Params.DataShards = 0 }, want: ErrInvalidParameters},
		{name: "invalid budget", mutate: func(config *DecoderConfig) { config.Budget.MaxUDPPayload = config.Budget.DataShardWireOverhead }, want: ErrInvalidBudget},
		{name: "zero decode timeout", mutate: func(config *DecoderConfig) { config.DecodeTimeout = 0 }, want: ErrInvalidDecoderConfig},
		{name: "negative decode timeout", mutate: func(config *DecoderConfig) { config.DecodeTimeout = -1 }, want: ErrInvalidDecoderConfig},
		{name: "zero completion TTL", mutate: func(config *DecoderConfig) { config.CompletionTTL = 0 }, want: ErrInvalidDecoderConfig},
		{name: "negative completion TTL", mutate: func(config *DecoderConfig) { config.CompletionTTL = -1 }, want: ErrInvalidDecoderConfig},
		{name: "zero pending capacity", mutate: func(config *DecoderConfig) { config.MaxPendingBlocks = 0 }, want: ErrInvalidDecoderConfig},
		{name: "negative pending capacity", mutate: func(config *DecoderConfig) { config.MaxPendingBlocks = -1 }, want: ErrInvalidDecoderConfig},
		{name: "zero completion capacity", mutate: func(config *DecoderConfig) { config.MaxCompletedBlocks = 0 }, want: ErrInvalidDecoderConfig},
		{name: "negative completion capacity", mutate: func(config *DecoderConfig) { config.MaxCompletedBlocks = -1 }, want: ErrInvalidDecoderConfig},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := decoderTestConfig(clock)
			test.mutate(&config)
			decoder, err := NewDecoder(config)
			if decoder != nil {
				decoder.Close()
				t.Fatal("NewDecoder() returned a decoder for invalid config")
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("NewDecoder() error = %v, want %v", err, test.want)
			}
		})
	}

	config := decoderTestConfig(nil)
	decoder, err := NewDecoder(config)
	if err != nil {
		t.Fatalf("NewDecoder() with default clock error = %v", err)
	}
	if _, ok := decoder.clock.(realClock); !ok {
		t.Fatalf("default clock type = %T, want realClock", decoder.clock)
	}
	if err := decoder.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}
