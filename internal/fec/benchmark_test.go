package fec

import (
	"bytes"
	"testing"
	"time"
)

var (
	benchmarkEncodedBlock EncodedBlock
	benchmarkDecodeResult Result
)

func BenchmarkEncodeRS5_3(b *testing.B) {
	params := Params{DataShards: 3, ParityShards: 2}
	budget := Budget{
		MaxUDPPayload:         1_500,
		DataShardWireOverhead: 72,
		MaxDatagramSize:       4_284,
	}
	encoder, err := NewEncoder(params, budget)
	if err != nil {
		b.Fatal(err)
	}
	payload := bytes.Repeat([]byte("representative FEC payload "), 48)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		benchmarkEncodedBlock, err = encoder.Encode(payload)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRecoverRS5_3(b *testing.B) {
	params := Params{DataShards: 3, ParityShards: 2}
	budget := Budget{
		MaxUDPPayload:         1_500,
		DataShardWireOverhead: 72,
		MaxDatagramSize:       4_284,
	}
	payload := bytes.Repeat([]byte("representative FEC payload "), 48)
	encoder, err := NewEncoder(params, budget)
	if err != nil {
		b.Fatal(err)
	}
	block, err := encoder.Encode(payload)
	if err != nil {
		b.Fatal(err)
	}
	clock := &benchmarkClock{now: time.Unix(1_700_000_000, 0)}
	decoder, err := NewDecoder(DecoderConfig{
		Params:             params,
		Budget:             budget,
		DecodeTimeout:      time.Second,
		CompletionTTL:      time.Second,
		MaxPendingBlocks:   1,
		MaxCompletedBlocks: 1,
		Clock:              clock,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer decoder.Close()

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		key := BlockKey{SessionID: [16]byte{1}, PacketID: uint64(index)}
		for _, shardIndex := range []int{0, 3, 4} {
			benchmarkDecodeResult, err = decoder.AddVerifiedShard(IncomingShard{
				Key:            key,
				Params:         params,
				Index:          shardIndex,
				OriginalLength: len(payload),
				Payload:        block.Shards[shardIndex],
			})
			if err != nil {
				b.Fatal(err)
			}
		}
		if benchmarkDecodeResult.Outcome != OutcomeComplete {
			b.Fatalf("outcome = %v, want complete", benchmarkDecodeResult.Outcome)
		}
	}
}

type benchmarkClock struct {
	now time.Time
}

func (c *benchmarkClock) Now() time.Time { return c.now }
