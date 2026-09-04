package fec

import (
	"bytes"
	"errors"
	"math"
	"sort"
	"sync"
	"testing"
	"unsafe"

	"github.com/mofelee/mpudp/internal/wire"
)

var encoderTestBudget = Budget{
	MaxUDPPayload:         96,
	DataShardWireOverhead: 32,
	MaxDatagramSize:       320,
}

func TestEncoderEncodesCanonicalShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		payload   []byte
		shardSize int
	}{
		{name: "empty", payload: []byte{}, shardSize: 1},
		{name: "one byte", payload: []byte{0x7f}, shardSize: 1},
		{name: "divisible", payload: []byte("abcdefghijklmno"), shardSize: 3},
		{name: "nondivisible", payload: []byte("abcdefghijklmnop"), shardSize: 4},
	}

	params := Params{DataShards: 5, ParityShards: 3}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			encoder, err := NewEncoder(params, encoderTestBudget)
			if err != nil {
				t.Fatalf("NewEncoder() returned error: %v", err)
			}
			inputBefore := bytes.Clone(test.payload)
			block, err := encoder.Encode(test.payload)
			if err != nil {
				t.Fatalf("Encode() returned error: %v", err)
			}

			if block.PacketID != 0 {
				t.Errorf("PacketID = %d, want 0", block.PacketID)
			}
			if block.OriginalLength != len(test.payload) {
				t.Errorf("OriginalLength = %d, want %d", block.OriginalLength, len(test.payload))
			}
			if block.Params != params {
				t.Errorf("Params = %+v, want %+v", block.Params, params)
			}
			if len(block.Shards) != params.DataShards+params.ParityShards {
				t.Fatalf("len(Shards) = %d, want %d", len(block.Shards), params.DataShards+params.ParityShards)
			}

			base := unsafe.Pointer(&block.Shards[0][0])
			for index, shard := range block.Shards {
				if len(shard) != test.shardSize {
					t.Errorf("len(Shards[%d]) = %d, want %d", index, len(shard), test.shardSize)
				}
				if cap(shard) != len(shard) {
					t.Errorf("cap(Shards[%d]) = %d, want capped length %d", index, cap(shard), len(shard))
				}
				wantAddress := unsafe.Add(base, index*test.shardSize)
				if unsafe.Pointer(&shard[0]) != wantAddress {
					t.Errorf("Shards[%d] is not contiguous with the shared backing allocation", index)
				}
			}

			dataRegion := bytes.Join(block.Shards[:params.DataShards], nil)
			if got := dataRegion[:len(test.payload)]; !bytes.Equal(got, test.payload) {
				t.Errorf("encoded data prefix = %x, want %x", got, test.payload)
			}
			if padding := dataRegion[len(test.payload):]; !encoderBytesAreZero(padding) {
				t.Errorf("data shard padding = %x, want zero bytes", padding)
			}
			if !bytes.Equal(test.payload, inputBefore) {
				t.Errorf("Encode() mutated input: got %x, want %x", test.payload, inputBefore)
			}
			valid, err := encoder.codec.Verify(block.Shards)
			if err != nil {
				t.Fatalf("Verify() returned error: %v", err)
			}
			if !valid {
				t.Fatal("encoded Reed-Solomon block failed verification")
			}

			if len(test.payload) == 0 {
				for index, shard := range block.Shards {
					if !bytes.Equal(shard, []byte{0}) {
						t.Errorf("empty Datagram shard %d = %x, want one zero byte", index, shard)
					}
				}
			}
		})
	}
}

func TestEncoderOwnsShardBytes(t *testing.T) {
	t.Parallel()

	encoder, err := NewEncoder(Params{DataShards: 5, ParityShards: 3}, encoderTestBudget)
	if err != nil {
		t.Fatalf("NewEncoder() returned error: %v", err)
	}
	payload := []byte("owned input")
	want := bytes.Clone(payload)
	block, err := encoder.Encode(payload)
	if err != nil {
		t.Fatalf("Encode() returned error: %v", err)
	}

	for index := range payload {
		payload[index] ^= 0xff
	}
	dataRegion := bytes.Join(block.Shards[:block.Params.DataShards], nil)
	if got := dataRegion[:block.OriginalLength]; !bytes.Equal(got, want) {
		t.Fatalf("encoded bytes changed with caller input: got %x, want %x", got, want)
	}
}

func TestEncoderEffectiveLimitBoundary(t *testing.T) {
	t.Parallel()

	params := Params{DataShards: 3, ParityShards: 2}
	encoder, err := NewEncoder(params, Budget{
		MaxUDPPayload:         100,
		DataShardWireOverhead: 96,
		MaxDatagramSize:       100,
	})
	if err != nil {
		t.Fatalf("NewEncoder() returned error: %v", err)
	}
	if got := encoder.Limits().EffectiveDatagramLimit; got != 12 {
		t.Fatalf("EffectiveDatagramLimit = %d, want 12", got)
	}

	block, err := encoder.Encode(make([]byte, 12))
	if err != nil {
		t.Fatalf("Encode(exact limit) returned error: %v", err)
	}
	if block.PacketID != 0 {
		t.Errorf("exact-limit PacketID = %d, want 0", block.PacketID)
	}
	if len(block.Shards[0]) != 4 {
		t.Errorf("exact-limit shard size = %d, want 4", len(block.Shards[0]))
	}

	block, err = encoder.Encode(make([]byte, 13))
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("Encode(limit + 1) error = %v, want ErrMessageTooLarge", err)
	}
	if !encoderBlockIsZero(block) {
		t.Fatalf("Encode(limit + 1) block = %+v, want zero value", block)
	}

	next, err := encoder.Encode([]byte("ok"))
	if err != nil {
		t.Fatalf("Encode() after size rejection returned error: %v", err)
	}
	if next.PacketID != 1 {
		t.Fatalf("PacketID after size rejection = %d, want 1", next.PacketID)
	}
}

func TestEncoderUsesExactWireOverheadAndConfiguredLimit(t *testing.T) {
	const (
		maxUDPPayload   = 1_200
		maxDatagramSize = 1_000
	)
	if wire.DataShardOverhead != 71 {
		t.Fatalf("wire.DataShardOverhead = %d, want 71", wire.DataShardOverhead)
	}
	params := Params{DataShards: 3, ParityShards: 2}
	encoder, err := NewEncoder(params, Budget{
		MaxUDPPayload:         maxUDPPayload,
		DataShardWireOverhead: wire.DataShardOverhead,
		MaxDatagramSize:       maxDatagramSize,
	})
	if err != nil {
		t.Fatalf("NewEncoder() error = %v", err)
	}
	limits := encoder.Limits()
	if limits.ShardCapacity != maxUDPPayload-wire.DataShardOverhead || limits.EffectiveDatagramLimit != maxDatagramSize {
		t.Fatalf("Limits() = %+v, want wire capacity %d and effective limit %d", limits, maxUDPPayload-wire.DataShardOverhead, maxDatagramSize)
	}

	block, err := encoder.Encode(make([]byte, maxDatagramSize))
	if err != nil {
		t.Fatalf("Encode(exact configured limit) error = %v", err)
	}
	sessionID := wire.SessionID{1}
	for index, shard := range block.Shards {
		message, err := wire.NewDataShard(
			sessionID,
			block.PacketID,
			uint8(params.DataShards),
			uint8(params.ParityShards),
			uint8(index),
			uint32(block.OriginalLength),
			shard,
		)
		if err != nil {
			t.Fatalf("wire.NewDataShard(index=%d) error = %v", index, err)
		}
		encodedLength, err := wire.EncodedLen(message)
		if err != nil {
			t.Fatalf("wire.EncodedLen(index=%d) error = %v", index, err)
		}
		if encodedLength != wire.DataShardOverhead+len(shard) || encodedLength > maxUDPPayload {
			t.Fatalf("wire shard %d length = %d, want %d and <= %d", index, encodedLength, wire.DataShardOverhead+len(shard), maxUDPPayload)
		}
	}

	if rejected, err := encoder.Encode(make([]byte, maxDatagramSize+1)); !errors.Is(err, ErrMessageTooLarge) || !encoderBlockIsZero(rejected) {
		t.Fatalf("Encode(limit+1) = %+v, %v; want zero block and ErrMessageTooLarge", rejected, err)
	}
	accepted, err := encoder.Encode([]byte("next"))
	if err != nil {
		t.Fatalf("Encode() after limit rejection error = %v", err)
	}
	if accepted.PacketID != block.PacketID+1 {
		t.Fatalf("PacketID after limit rejection = %d, want %d", accepted.PacketID, block.PacketID+1)
	}
}

func TestEncoderOversizeDoesNotAllocateOrConsumePacketID(t *testing.T) {
	params := Params{DataShards: 5, ParityShards: 3}
	encoder, err := NewEncoder(params, encoderTestBudget)
	if err != nil {
		t.Fatalf("NewEncoder() returned error: %v", err)
	}
	oversize := make([]byte, encoder.Limits().EffectiveDatagramLimit+1)

	allocations := testing.AllocsPerRun(100, func() {
		block, encodeErr := encoder.Encode(oversize)
		if encodeErr != ErrMessageTooLarge {
			t.Fatalf("Encode(oversize) error = %v, want ErrMessageTooLarge", encodeErr)
		}
		if !encoderBlockIsZero(block) {
			t.Fatalf("Encode(oversize) block = %+v, want zero value", block)
		}
	})
	if allocations != 0 {
		t.Fatalf("Encode(oversize) allocations = %v, want 0", allocations)
	}

	block, err := encoder.Encode([]byte("accepted"))
	if err != nil {
		t.Fatalf("Encode() after size rejection returned error: %v", err)
	}
	if block.PacketID != 0 {
		t.Fatalf("PacketID after size rejection = %d, want 0", block.PacketID)
	}
}

func TestEncoderConcurrentPacketIDs(t *testing.T) {
	t.Parallel()

	encoder, err := NewEncoder(Params{DataShards: 5, ParityShards: 3}, encoderTestBudget)
	if err != nil {
		t.Fatalf("NewEncoder() returned error: %v", err)
	}

	const count = 256
	results := make(chan uint64, count)
	errorsCh := make(chan error, count)
	var workers sync.WaitGroup
	workers.Add(count)
	for index := 0; index < count; index++ {
		go func(value byte) {
			defer workers.Done()
			block, encodeErr := encoder.Encode([]byte{value})
			if encodeErr != nil {
				errorsCh <- encodeErr
				return
			}
			results <- block.PacketID
		}(byte(index))
	}
	workers.Wait()
	close(results)
	close(errorsCh)

	for encodeErr := range errorsCh {
		t.Errorf("concurrent Encode() returned error: %v", encodeErr)
	}
	ids := make([]uint64, 0, count)
	for id := range results {
		ids = append(ids, id)
	}
	if len(ids) != count {
		t.Fatalf("successful Encode() count = %d, want %d", len(ids), count)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for index, id := range ids {
		if id != uint64(index) {
			t.Fatalf("sorted PacketID[%d] = %d, want %d", index, id, index)
		}
	}
}

func TestEncoderPacketIDExhaustionDoesNotWrap(t *testing.T) {
	t.Parallel()

	encoder, err := newEncoderAt(
		Params{DataShards: 5, ParityShards: 3},
		encoderTestBudget,
		math.MaxUint64-1,
		false,
	)
	if err != nil {
		t.Fatalf("newEncoderAt() returned error: %v", err)
	}

	for _, want := range []uint64{math.MaxUint64 - 1, math.MaxUint64} {
		block, encodeErr := encoder.Encode([]byte("payload"))
		if encodeErr != nil {
			t.Fatalf("Encode() for PacketID %d returned error: %v", want, encodeErr)
		}
		if block.PacketID != want {
			t.Fatalf("PacketID = %d, want %d", block.PacketID, want)
		}
	}

	for attempt := 0; attempt < 2; attempt++ {
		block, encodeErr := encoder.Encode([]byte("payload"))
		if !errors.Is(encodeErr, ErrPacketIDExhausted) {
			t.Fatalf("Encode() after exhaustion error = %v, want ErrPacketIDExhausted", encodeErr)
		}
		if !encoderBlockIsZero(block) {
			t.Fatalf("Encode() after exhaustion block = %+v, want zero value", block)
		}
	}
}

func encoderBytesAreZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}

func encoderBlockIsZero(block EncodedBlock) bool {
	return block.PacketID == 0 &&
		block.OriginalLength == 0 &&
		block.Params == (Params{}) &&
		block.Shards == nil
}
