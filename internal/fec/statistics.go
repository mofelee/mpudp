package fec

import "sync/atomic"

// Counters are optional cumulative decoder diagnostics. Sharing a collector
// aggregates multiple decoders without retaining their keys or shard content.
type Counters struct {
	CompletedBlocks atomic.Uint64
	RecoveredBlocks atomic.Uint64
	RecoveredShards atomic.Uint64
	ExpiredBlocks   atomic.Uint64
	DecoderFull     atomic.Uint64
	LateShards      atomic.Uint64
	DuplicateShards atomic.Uint64
}
