package fec

import (
	"bytes"
	"container/heap"
	"fmt"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
)

// BlockKey is the complete decode aggregation key.
type BlockKey struct {
	SessionID [16]byte
	PacketID  uint64
}

// IncomingShard is authenticated DATA_SHARD content. The caller must verify
// the wire format, SessionID, and HMAC before calling AddVerifiedShard.
type IncomingShard struct {
	Key            BlockKey
	Params         Params
	Index          int
	OriginalLength int
	Payload        []byte
}

// Outcome describes what AddVerifiedShard did with a valid shard.
type Outcome uint8

const (
	// OutcomePending means a new shard was retained but k have not arrived.
	OutcomePending Outcome = iota
	// OutcomeDuplicate means the shard or completed block was already seen.
	OutcomeDuplicate
	// OutcomeComplete means Datagram contains the one recovered delivery.
	OutcomeComplete
)

// Result is returned for every valid shard. Datagram is set only for
// OutcomeComplete; it is non-nil even for an empty Datagram.
type Result struct {
	Outcome  Outcome
	Datagram []byte
}

// Clock provides deterministic decoder expiry tests without creating timers.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// DecoderConfig defines hard state and time bounds for one Decoder.
type DecoderConfig struct {
	Params             Params
	Budget             Budget
	DecodeTimeout      time.Duration
	CompletionTTL      time.Duration
	MaxPendingBlocks   int
	MaxCompletedBlocks int
	Clock              Clock
	Statistics         *Counters
}

// Stats reports retained state; it does not mutate or expire entries.
type Stats struct {
	PendingBlocks   int
	PendingShards   int
	PendingBytes    int
	CompletedBlocks int
}

// ExpireStats reports entries removed by one Sweep or opportunistic sweep.
type ExpireStats struct {
	PendingBlocks   int
	CompletedBlocks int
}

type pendingBlock struct {
	originalLength int
	shardSize      int
	shards         [][]byte
	present        int
	bytes          int
	expiry         *expiryEntry
}

// Decoder aggregates verified shards. It owns copied shard bytes and is safe
// for concurrent use. It creates no goroutine or timer.
type Decoder struct {
	mu                 sync.Mutex
	params             Params
	limits             Limits
	codec              reedsolomon.Encoder
	total              int
	clock              Clock
	decodeTimeout      time.Duration
	completionTTL      time.Duration
	maxPendingBlocks   int
	maxCompletedBlocks int
	pending            map[BlockKey]*pendingBlock
	pendingExpiry      expiryHeap
	completed          map[BlockKey]*expiryEntry
	completedExpiry    expiryHeap
	pendingShards      int
	pendingBytes       int
	closed             bool
	statistics         *Counters
}

// NewDecoder constructs an empty bounded Decoder.
func NewDecoder(config DecoderConfig) (*Decoder, error) {
	limits, err := DeriveLimits(config.Params, config.Budget)
	if err != nil {
		return nil, err
	}
	if config.DecodeTimeout <= 0 {
		return nil, fmt.Errorf("%w: decode timeout must be greater than zero", ErrInvalidDecoderConfig)
	}
	if config.CompletionTTL <= 0 {
		return nil, fmt.Errorf("%w: completion TTL must be greater than zero", ErrInvalidDecoderConfig)
	}
	if config.MaxPendingBlocks <= 0 {
		return nil, fmt.Errorf("%w: max pending blocks must be greater than zero", ErrInvalidDecoderConfig)
	}
	if config.MaxCompletedBlocks <= 0 {
		return nil, fmt.Errorf("%w: max completed blocks must be greater than zero", ErrInvalidDecoderConfig)
	}
	codec, total, err := newCodec(config.Params)
	if err != nil {
		return nil, err
	}
	clock := config.Clock
	if clock == nil {
		clock = realClock{}
	}
	return &Decoder{
		params:             config.Params,
		limits:             limits,
		codec:              codec,
		total:              total,
		clock:              clock,
		decodeTimeout:      config.DecodeTimeout,
		completionTTL:      config.CompletionTTL,
		maxPendingBlocks:   config.MaxPendingBlocks,
		maxCompletedBlocks: config.MaxCompletedBlocks,
		pending:            make(map[BlockKey]*pendingBlock, config.MaxPendingBlocks),
		completed:          make(map[BlockKey]*expiryEntry, config.MaxCompletedBlocks),
		statistics:         config.Statistics,
	}, nil
}

// Limits returns the immutable, checked limits used by this Decoder.
func (d *Decoder) Limits() Limits {
	return d.limits
}

// AddVerifiedShard retains a distinct shard and reconstructs synchronously in
// the same call that reaches k. Invalid input creates no state.
func (d *Decoder) AddVerifiedShard(input IncomingShard) (Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return Result{}, ErrClosed
	}
	shardSize, err := d.validateShard(input)
	if err != nil {
		return Result{}, err
	}
	now := d.clock.Now()
	d.sweepLocked(now)
	if _, ok := d.completed[input.Key]; ok {
		if d.statistics != nil {
			d.statistics.LateShards.Add(1)
		}
		return Result{Outcome: OutcomeDuplicate}, nil
	}

	block := d.pending[input.Key]
	if block == nil {
		if len(d.pending) >= d.maxPendingBlocks {
			if d.statistics != nil {
				d.statistics.DecoderFull.Add(1)
			}
			return Result{}, ErrDecoderFull
		}
		expiry := &expiryEntry{key: input.Key, deadline: now.Add(d.decodeTimeout), index: -1}
		block = &pendingBlock{
			originalLength: input.OriginalLength,
			shardSize:      shardSize,
			shards:         make([][]byte, d.total),
			expiry:         expiry,
		}
		d.pending[input.Key] = block
		heap.Push(&d.pendingExpiry, expiry)
		d.statistics.changePending(1, 0, 0)
	} else if block.originalLength != input.OriginalLength || block.shardSize != shardSize {
		return Result{}, ErrInconsistentBlock
	}

	if existing := block.shards[input.Index]; existing != nil {
		if bytes.Equal(existing, input.Payload) {
			if d.statistics != nil {
				d.statistics.DuplicateShards.Add(1)
			}
			return Result{Outcome: OutcomeDuplicate}, nil
		}
		return Result{}, ErrConflictingShard
	}
	owned := bytes.Clone(input.Payload)
	block.shards[input.Index] = owned
	block.present++
	block.bytes += len(owned)
	d.pendingShards++
	d.pendingBytes += len(owned)
	d.statistics.changePending(0, 1, int64(len(owned)))
	if block.present < d.params.DataShards {
		return Result{Outcome: OutcomePending}, nil
	}

	missingData := 0
	if d.statistics != nil {
		for _, shard := range block.shards[:d.params.DataShards] {
			if shard == nil {
				missingData++
			}
		}
	}
	working := append([][]byte(nil), block.shards...)
	if err := d.codec.ReconstructData(working); err != nil {
		d.removePendingLocked(input.Key, block)
		return Result{}, fmt.Errorf("%w: %v", ErrDecode, err)
	}
	datagram := make([]byte, input.OriginalLength)
	written := 0
	for _, shard := range working[:d.params.DataShards] {
		if written == len(datagram) {
			break
		}
		written += copy(datagram[written:], shard)
	}
	if written != len(datagram) {
		d.removePendingLocked(input.Key, block)
		return Result{}, fmt.Errorf("%w: reconstructed data shorter than original length", ErrDecode)
	}

	d.removePendingLocked(input.Key, block)
	d.rememberCompletedLocked(input.Key, now)
	if d.statistics != nil {
		d.statistics.CompletedBlocks.Add(1)
		if missingData != 0 {
			d.statistics.RecoveredBlocks.Add(1)
			d.statistics.RecoveredShards.Add(uint64(missingData))
		}
	}
	return Result{Outcome: OutcomeComplete, Datagram: datagram}, nil
}

func (d *Decoder) validateShard(input IncomingShard) (int, error) {
	if input.Params != d.params {
		return 0, fmt.Errorf("%w: Reed-Solomon parameters do not match the decoder", ErrInvalidShard)
	}
	if input.Index < 0 || input.Index >= d.total {
		return 0, fmt.Errorf("%w: shard index is outside the configured block", ErrInvalidShard)
	}
	if input.OriginalLength < 0 || input.OriginalLength > d.limits.EffectiveDatagramLimit {
		return 0, fmt.Errorf("%w: original Datagram length is outside the effective limit", ErrInvalidShard)
	}
	shardSize := shardSizeForDatagram(input.OriginalLength, d.params.DataShards)
	if shardSize > d.limits.ShardCapacity || len(input.Payload) != shardSize {
		return 0, fmt.Errorf("%w: shard payload length is not canonical for the original Datagram", ErrInvalidShard)
	}
	return shardSize, nil
}

// Sweep expires entries whose fixed deadline is at or before Clock.Now.
func (d *Decoder) Sweep() ExpireStats {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ExpireStats{}
	}
	return d.sweepLocked(d.clock.Now())
}

func (d *Decoder) sweepLocked(now time.Time) ExpireStats {
	var expired ExpireStats
	for d.pendingExpiry.Len() != 0 && !d.pendingExpiry[0].deadline.After(now) {
		entry := heap.Pop(&d.pendingExpiry).(*expiryEntry)
		block, ok := d.pending[entry.key]
		if !ok || block.expiry != entry {
			continue
		}
		delete(d.pending, entry.key)
		d.pendingShards -= block.present
		d.pendingBytes -= block.bytes
		d.statistics.changePending(-1, -int64(block.present), -int64(block.bytes))
		expired.PendingBlocks++
		if d.statistics != nil {
			d.statistics.ExpiredBlocks.Add(1)
		}
	}
	for d.completedExpiry.Len() != 0 && !d.completedExpiry[0].deadline.After(now) {
		entry := heap.Pop(&d.completedExpiry).(*expiryEntry)
		current, ok := d.completed[entry.key]
		if !ok || current != entry {
			continue
		}
		delete(d.completed, entry.key)
		expired.CompletedBlocks++
	}
	return expired
}

func (d *Decoder) removePendingLocked(key BlockKey, block *pendingBlock) {
	delete(d.pending, key)
	if block.expiry.index >= 0 {
		heap.Remove(&d.pendingExpiry, block.expiry.index)
	}
	d.pendingShards -= block.present
	d.pendingBytes -= block.bytes
	d.statistics.changePending(-1, -int64(block.present), -int64(block.bytes))
}

func (d *Decoder) rememberCompletedLocked(key BlockKey, now time.Time) {
	if len(d.completed) >= d.maxCompletedBlocks {
		oldest := heap.Pop(&d.completedExpiry).(*expiryEntry)
		delete(d.completed, oldest.key)
		if d.statistics != nil {
			d.statistics.CompletedCapacityEvictions.Add(1)
		}
	}
	entry := &expiryEntry{key: key, deadline: now.Add(d.completionTTL), index: -1}
	d.completed[key] = entry
	heap.Push(&d.completedExpiry, entry)
}

// Stats returns exact retained counts without exposing shard content.
func (d *Decoder) Stats() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return Stats{
		PendingBlocks:   len(d.pending),
		PendingShards:   d.pendingShards,
		PendingBytes:    d.pendingBytes,
		CompletedBlocks: len(d.completed),
	}
}

// Close idempotently drops all retained state. Decoder owns no background work.
func (d *Decoder) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	d.statistics.changePending(-int64(len(d.pending)), -int64(d.pendingShards), -int64(d.pendingBytes))
	clear(d.pending)
	clear(d.completed)
	for index := range d.pendingExpiry {
		d.pendingExpiry[index] = nil
	}
	for index := range d.completedExpiry {
		d.completedExpiry[index] = nil
	}
	d.pendingExpiry = nil
	d.completedExpiry = nil
	d.pendingShards = 0
	d.pendingBytes = 0
	return nil
}
