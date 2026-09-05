package fec

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
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

func TestPendingGaugesAcrossDecoderLifetimes(t *testing.T) {
	counters := &Counters{}
	const decoders = 8
	ready := make(chan struct{}, decoders)
	release := make(chan struct{})
	var workers sync.WaitGroup
	for i := 0; i < decoders; i++ {
		cfg := decoderTestConfig(newDecoderTestClock())
		cfg.Statistics = counters
		d := newDecoderForTest(t, cfg)
		block := encodeDecoderTestBlock(t, cfg.Params, []byte("retained"))
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := d.AddVerifiedShard(decoderTestShard(block, BlockKey{PacketID: 1}, 0))
			if err != nil {
				t.Error(err)
			}
			ready <- struct{}{}
			<-release
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	for i := 0; i < decoders; i++ {
		<-ready
	}
	if counters.PendingBlocks.Load() != decoders || counters.PendingShards.Load() != decoders || counters.PendingBytes.Load() != decoders*3 {
		t.Errorf("shared pending gauges = %d/%d/%d", counters.PendingBlocks.Load(), counters.PendingShards.Load(), counters.PendingBytes.Load())
	}
	close(release)
	workers.Wait()
	if counters.PendingBlocks.Load() != 0 || counters.PendingShards.Load() != 0 || counters.PendingBytes.Load() != 0 {
		t.Fatal("Close did not remove each decoder's retained state exactly once")
	}
	if counters.PendingBlocksHighWater.Load() != decoders || counters.PendingShardsHighWater.Load() != decoders || counters.PendingBytesHighWater.Load() != decoders*3 {
		t.Fatal("high-water marks lost the simultaneous retained state")
	}
}

func TestCapacityEvictionsExcludeCompletionTTLExpiry(t *testing.T) {
	clock := newDecoderTestClock()
	cfg := decoderTestConfig(clock)
	cfg.MaxCompletedBlocks = 1
	cfg.Statistics = &Counters{}
	d := newDecoderForTest(t, cfg)
	block := encodeDecoderTestBlock(t, cfg.Params, []byte("payload"))
	for id := uint64(0); id < 2; id++ {
		for shard := 0; shard < cfg.Params.DataShards; shard++ {
			if _, err := d.AddVerifiedShard(decoderTestShard(block, BlockKey{PacketID: id}, shard)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if cfg.Statistics.CompletedCapacityEvictions.Load() != 1 || cfg.Statistics.PendingBlocks.Load() != 0 {
		t.Fatal("completion failed to release pending state or count capacity eviction")
	}
	clock.Advance(cfg.CompletionTTL)
	if expired := d.Sweep(); expired.CompletedBlocks != 1 {
		t.Fatalf("completion TTL expiry = %+v", expired)
	}
	if cfg.Statistics.CompletedCapacityEvictions.Load() != 1 {
		t.Fatal("TTL expiry was mislabeled as a capacity eviction")
	}
}

type delayedParityResult struct {
	Evictions, LateShards, TooOldShards, Full uint64
	Pending                                   Stats
	NewBlockRejected                          bool
}

// All delayed keys are known to have completed before parity is released.
// The legacy cache and production replay window receive the identical workload.
func delayedParityWorkload(tb testing.TB, capacity int, window bool) delayedParityResult {
	tb.Helper()
	clock := newDecoderTestClock()
	cfg := decoderTestConfig(clock)
	cfg.MaxPendingBlocks = 16
	cfg.MaxCompletedBlocks = capacity
	cfg.Statistics = &Counters{}
	if window {
		cfg.ReplayWindow = &ReplayWindowConfig{}
	}
	d, err := NewDecoder(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	defer d.Close()
	encoder, err := NewEncoder(cfg.Params, cfg.Budget)
	if err != nil {
		tb.Fatal(err)
	}
	data := bytes.Repeat([]byte{0x5a}, 1200)
	block, err := encoder.Encode(data)
	if err != nil {
		tb.Fatal(err)
	}
	for id := uint64(0); id < 32; id++ {
		for shard := 0; shard < cfg.Params.DataShards; shard++ {
			result, err := d.AddVerifiedShard(decoderTestShard(block, BlockKey{PacketID: id}, shard))
			if err != nil {
				tb.Fatal(err)
			}
			if shard == cfg.Params.DataShards-1 && (result.Outcome != OutcomeComplete || !bytes.Equal(result.Datagram, data)) {
				tb.Fatal("original block did not complete correctly")
			}
		}
	}
	result := delayedParityResult{Evictions: cfg.Statistics.CompletedCapacityEvictions.Load()}
	for id := uint64(0); id < 32; id++ {
		for shard := cfg.Params.DataShards; shard < len(block.Shards); shard++ {
			late, err := d.AddVerifiedShard(decoderTestShard(block, BlockKey{PacketID: id}, shard))
			if err != nil && !errors.Is(err, ErrDecoderFull) {
				tb.Fatal(err)
			}
			if late.Outcome == OutcomeComplete {
				tb.Fatal("delayed parity unexpectedly redelivered the Datagram")
			}
		}
	}
	result.Pending = d.Stats()
	if cfg.Statistics.PendingBlocks.Load() != int64(result.Pending.PendingBlocks) ||
		cfg.Statistics.PendingShards.Load() != int64(result.Pending.PendingShards) ||
		cfg.Statistics.PendingBytes.Load() != int64(result.Pending.PendingBytes) {
		tb.Fatal("public collector differs from exact retained decoder state")
	}
	_, err = d.AddVerifiedShard(decoderTestShard(block, BlockKey{PacketID: 32}, 0))
	result.NewBlockRejected = errors.Is(err, ErrDecoderFull)
	if err != nil && !result.NewBlockRejected {
		tb.Fatal(err)
	}
	result.LateShards, result.Full = cfg.Statistics.LateShards.Load(), cfg.Statistics.DecoderFull.Load()
	result.TooOldShards = cfg.Statistics.TooOldShards.Load()
	clock.Advance(cfg.DecodeTimeout)
	d.Sweep()
	if cfg.Statistics.PendingBlocks.Load() != 0 || cfg.Statistics.PendingShards.Load() != 0 || cfg.Statistics.PendingBytes.Load() != 0 {
		tb.Fatal("expiry did not release retained pending gauges")
	}
	return result
}

func TestDelayedParityCapacityDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		capacity int
		pending  int
		full     uint64
	}{{8, 16, 17}, {16, 16, 1}, {32, 0, 0}} {
		t.Run(fmt.Sprintf("completed_capacity_%d", tc.capacity), func(t *testing.T) {
			got := delayedParityWorkload(t, tc.capacity, false)
			if got.Evictions != uint64(32-tc.capacity) || got.Pending.PendingBlocks != tc.pending ||
				got.Pending.PendingShards != tc.pending*2 || got.Pending.PendingBytes != tc.pending*800 ||
				got.Full != tc.full || got.LateShards != uint64(tc.capacity*2) || got.NewBlockRejected != (tc.pending == 16) {
				t.Fatalf("delayed parity evidence = %+v", got)
			}
			t.Logf("capacity=%d evictions=%d reopened_pending=%d shards=%d bytes=%d full=%d new_block_rejected=%t",
				tc.capacity, got.Evictions, got.Pending.PendingBlocks, got.Pending.PendingShards,
				got.Pending.PendingBytes, got.Full, got.NewBlockRejected)
		})
	}
}

func TestDelayedParityWindowDiagnostics(t *testing.T) {
	for _, capacity := range []int{8, 16, 32} {
		t.Run(fmt.Sprintf("legacy_capacity_%d", capacity), func(t *testing.T) {
			got := delayedParityWorkload(t, capacity, true)
			if got.Evictions != 0 || got.Pending.PendingBlocks != 0 || got.Pending.PendingShards != 0 ||
				got.Pending.PendingBytes != 0 || got.Full != 0 || got.LateShards != 64 || got.TooOldShards != 0 || got.NewBlockRejected {
				t.Fatalf("window delayed parity evidence = %+v", got)
			}
			t.Logf("window=%d legacy_capacity=%d evictions=%d reopened_pending=%d late=%d too_old=%d full=%d new_block_rejected=%t",
				ReplayWindowIDs, capacity, got.Evictions, got.Pending.PendingBlocks, got.LateShards, got.TooOldShards, got.Full, got.NewBlockRejected)
		})
	}
}

func TestReplayWindowHighBlockRateDelayedParityDoesNotReopen(t *testing.T) {
	cfg := windowDecoderTestConfig(newDecoderTestClock())
	cfg.MaxCompletedBlocks = 8
	cfg.MaxPendingBlocks = 16
	d := newDecoderForTest(t, cfg)
	block := encodeDecoderTestBlock(t, cfg.Params, bytes.Repeat([]byte{0x5a}, 1200))
	const blocks = ReplayWindowIDs + 32
	for id := uint64(0); id < blocks; id++ {
		completeWindowBlock(t, d, cfg, block, id)
	}
	for id := uint64(0); id < blocks; id++ {
		for shard := cfg.Params.DataShards; shard < len(block.Shards); shard++ {
			result, err := d.AddVerifiedShard(windowShard(block, cfg, id, shard))
			want := OutcomeDuplicate
			if id < 32 {
				want = OutcomeTooOld
			}
			if err != nil || result.Outcome != want {
				t.Fatalf("late high-rate parity %d: %+v %v", id, result, err)
			}
		}
	}
	if got := d.Stats(); got.PendingBlocks != 0 || got.PendingBytes != 0 || got.CompletedBlocks != ReplayWindowIDs {
		t.Fatalf("high-rate late parity reopened state: %+v", got)
	}
	if cfg.Statistics.CompletedBlocks.Load() != blocks || cfg.Statistics.DecoderFull.Load() != 0 || cfg.Statistics.CompletedCapacityEvictions.Load() != 0 ||
		cfg.Statistics.LateShards.Load() != ReplayWindowIDs*2 || cfg.Statistics.TooOldShards.Load() != 64 {
		t.Fatal("high-rate late parity changed completions or mislabeled drop reasons")
	}
	completeWindowBlock(t, d, cfg, block, blocks)
	t.Logf("completed=%d late=%d too_old=%d pending=%d full=%d", blocks,
		cfg.Statistics.LateShards.Load(), cfg.Statistics.TooOldShards.Load(), d.Stats().PendingBlocks, cfg.Statistics.DecoderFull.Load())
}
