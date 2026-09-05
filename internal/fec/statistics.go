package fec

import "sync/atomic"

// Counters are optional decoder diagnostics. Sharing a collector aggregates
// concurrent retained state and cumulative events without retaining block keys
// or shard content. PendingBytes includes owned shards, not map/codec overhead.
type Counters struct {
	CompletedBlocks            atomic.Uint64
	CompletedCapacityEvictions atomic.Uint64
	RecoveredBlocks            atomic.Uint64
	RecoveredShards            atomic.Uint64
	ExpiredBlocks              atomic.Uint64
	DecoderFull                atomic.Uint64
	LateShards                 atomic.Uint64
	DuplicateShards            atomic.Uint64
	PendingBlocks              atomic.Int64
	PendingShards              atomic.Int64
	PendingBytes               atomic.Int64
	PendingBlocksHighWater     atomic.Uint64
	PendingShardsHighWater     atomic.Uint64
	PendingBytesHighWater      atomic.Uint64
}

func (c *Counters) changePending(blocks, shards, bytes int64) {
	if c == nil {
		return
	}
	changeGauge(&c.PendingBlocks, &c.PendingBlocksHighWater, blocks)
	changeGauge(&c.PendingShards, &c.PendingShardsHighWater, shards)
	changeGauge(&c.PendingBytes, &c.PendingBytesHighWater, bytes)
}

func changeGauge(current *atomic.Int64, peak *atomic.Uint64, delta int64) {
	if delta == 0 {
		return
	}
	now := current.Add(delta)
	if delta < 0 {
		return
	}
	value := uint64(now)
	for old := peak.Load(); value > old; old = peak.Load() {
		if peak.CompareAndSwap(old, value) {
			return
		}
	}
}
