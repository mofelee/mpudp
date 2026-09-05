package fec

import (
	"errors"
	"testing"
)

func TestDecoderCumulativeStatistics(t *testing.T) {
	clock := newDecoderTestClock()
	config := decoderTestConfig(clock)
	config.MaxPendingBlocks = 1
	counters := &Counters{}
	config.Statistics = counters
	decoder := newDecoderForTest(t, config)
	block := encodeDecoderTestBlock(t, config.Params, []byte("payload"))
	key := BlockKey{PacketID: 1}
	add := func(key BlockKey, index int) {
		t.Helper()
		if _, err := decoder.AddVerifiedShard(decoderTestShard(block, key, index)); err != nil {
			t.Fatal(err)
		}
	}
	add(key, 0)
	add(key, 0)
	if _, err := decoder.AddVerifiedShard(decoderTestShard(block, BlockKey{PacketID: 2}, 0)); !errors.Is(err, ErrDecoderFull) {
		t.Fatalf("full decoder error = %v", err)
	}
	add(key, 3)
	add(key, 4)
	add(key, 1)
	add(BlockKey{PacketID: 2}, 0)
	clock.Advance(config.DecodeTimeout)
	decoder.Sweep()
	for i := 0; i < config.Params.DataShards; i++ {
		add(BlockKey{PacketID: 3}, i)
	}
	if err := decoder.Close(); err != nil {
		t.Fatal(err)
	}
	if counters.CompletedBlocks.Load() != 2 || counters.RecoveredBlocks.Load() != 1 || counters.RecoveredShards.Load() != 2 || counters.ExpiredBlocks.Load() != 1 || counters.DecoderFull.Load() != 1 || counters.LateShards.Load() != 1 || counters.DuplicateShards.Load() != 1 {
		t.Fatalf("unexpected decoder counters: completed=%d recovered=%d shards=%d expired=%d full=%d late=%d duplicate=%d", counters.CompletedBlocks.Load(), counters.RecoveredBlocks.Load(), counters.RecoveredShards.Load(), counters.ExpiredBlocks.Load(), counters.DecoderFull.Load(), counters.LateShards.Load(), counters.DuplicateShards.Load())
	}
}
