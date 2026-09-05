package fec

import (
	"bytes"
	"errors"
	"math"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"
)

func windowDecoderTestConfig(clock Clock) DecoderConfig {
	cfg := decoderTestConfig(clock)
	cfg.ReplayWindow = &ReplayWindowConfig{SessionID: [16]byte{1}}
	cfg.Statistics = &Counters{}
	return cfg
}

func windowShard(block EncodedBlock, cfg DecoderConfig, id uint64, index int) IncomingShard {
	return decoderTestShard(block, BlockKey{SessionID: cfg.ReplayWindow.SessionID, PacketID: id}, index)
}

func completeWindowBlock(t *testing.T, d *Decoder, cfg DecoderConfig, block EncodedBlock, id uint64) {
	t.Helper()
	for index := range cfg.Params.DataShards {
		result, err := d.AddVerifiedShard(windowShard(block, cfg, id, index))
		if err != nil {
			t.Fatal(err)
		}
		want := OutcomePending
		if index == cfg.Params.DataShards-1 {
			want = OutcomeComplete
		}
		if result.Outcome != want {
			t.Fatalf("ID %d shard %d: outcome=%v, want %v", id, index, result.Outcome, want)
		}
	}
}

func TestReplayWindowBoundariesAndMaxUint64(t *testing.T) {
	cfg := windowDecoderTestConfig(newDecoderTestClock())
	d := newDecoderForTest(t, cfg)
	block := encodeDecoderTestBlock(t, cfg.Params, []byte("bounded delivery"))
	completeWindowBlock(t, d, cfg, block, 0)
	completeWindowBlock(t, d, cfg, block, ReplayWindowIDs-1)
	if result, err := d.AddVerifiedShard(windowShard(block, cfg, 0, 3)); err != nil || result.Outcome != OutcomeDuplicate {
		t.Fatalf("W-1 span lost completed bit: %+v %v", result, err)
	}
	completeWindowBlock(t, d, cfg, block, ReplayWindowIDs)
	if result, err := d.AddVerifiedShard(windowShard(block, cfg, 0, 3)); err != nil || result.Outcome != OutcomeTooOld {
		t.Fatalf("W span did not retire ID 0: %+v %v", result, err)
	}
	// An unknown ID at the inclusive lower edge still has its full chance to decode.
	completeWindowBlock(t, d, cfg, block, 1)
	completeWindowBlock(t, d, cfg, block, math.MaxUint64)
	if d.replay.highest != math.MaxUint64 || d.Stats().CompletedBlocks != 1 {
		t.Fatal("large jump retained old ring bits or wrapped the high watermark")
	}
	completeWindowBlock(t, d, cfg, block, math.MaxUint64-ReplayWindowIDs+1)
	for _, id := range []uint64{0, ReplayWindowIDs, math.MaxUint64 - ReplayWindowIDs} {
		if result, err := d.AddVerifiedShard(windowShard(block, cfg, id, 0)); err != nil || result.Outcome != OutcomeTooOld {
			t.Fatalf("retired ID %d: %+v %v", id, result, err)
		}
	}
	if result, err := d.AddVerifiedShard(windowShard(block, cfg, math.MaxUint64, 4)); err != nil || result.Outcome != OutcomeDuplicate {
		t.Fatalf("MaxUint64 lost completed identity: %+v %v", result, err)
	}
	if cfg.Statistics.TooOldShards.Load() != 4 || cfg.Statistics.LateShards.Load() != 2 || cfg.Statistics.DecoderFull.Load() != 0 {
		t.Fatal("retired and completed late shards were not counted separately")
	}
	before := d.Stats()
	allocs := testing.AllocsPerRun(100, func() {
		result, err := d.AddVerifiedShard(windowShard(block, cfg, 0, 0))
		if err != nil || result.Outcome != OutcomeTooOld {
			t.Fatal("retired packet changed outcome")
		}
	})
	if allocs != 0 || d.Stats() != before {
		t.Fatalf("retired shard allocated state: allocations=%v before=%+v after=%+v", allocs, before, d.Stats())
	}
}

func TestReplayWindowPinsPendingUntilOriginalDeadline(t *testing.T) {
	clock := newDecoderTestClock()
	cfg := windowDecoderTestConfig(clock)
	d := newDecoderForTest(t, cfg)
	block := encodeDecoderTestBlock(t, cfg.Params, []byte("pinned below the floor"))
	for _, id := range []uint64{0, 1} {
		if _, err := d.AddVerifiedShard(windowShard(block, cfg, id, 0)); err != nil {
			t.Fatal(err)
		}
	}
	deadline := d.pending[windowShard(block, cfg, 1, 0).Key].expiry.deadline
	completeWindowBlock(t, d, cfg, block, ReplayWindowIDs+1)
	clock.Advance(cfg.DecodeTimeout / 2)
	for _, index := range []int{3, 4} {
		result, err := d.AddVerifiedShard(windowShard(block, cfg, 0, index))
		if err != nil || (index == 4 && (result.Outcome != OutcomeComplete || !bytes.Equal(result.Datagram, []byte("pinned below the floor")))) {
			t.Fatalf("admitted ID below floor could not complete: %+v %v", result, err)
		}
	}
	if result, err := d.AddVerifiedShard(windowShard(block, cfg, 0, 1)); err != nil || result.Outcome != OutcomeTooOld {
		t.Fatalf("completed pinned ID reopened: %+v %v", result, err)
	}
	if _, err := d.AddVerifiedShard(windowShard(block, cfg, 1, 3)); err != nil {
		t.Fatal(err)
	}
	if got := d.pending[windowShard(block, cfg, 1, 0).Key].expiry.deadline; !got.Equal(deadline) {
		t.Fatal("a shard below the floor refreshed the original pending deadline")
	}
	clock.Advance(cfg.DecodeTimeout / 2)
	if result, err := d.AddVerifiedShard(windowShard(block, cfg, 1, 4)); err != nil || result.Outcome != OutcomeTooOld {
		t.Fatalf("expired pinned ID was readmitted: %+v %v", result, err)
	}
	if d.Stats().PendingBlocks != 0 || cfg.Statistics.ExpiredBlocks.Load() != 1 || cfg.Statistics.CompletedBlocks.Load() != 2 {
		t.Fatal("pinned completion/expiry changed pending state or delivery count")
	}
}

func TestReplayWindowRejectedInputDoesNotAdvance(t *testing.T) {
	clock := newDecoderTestClock()
	cfg := windowDecoderTestConfig(clock)
	cfg.MaxPendingBlocks = 1
	d := newDecoderForTest(t, cfg)
	block := encodeDecoderTestBlock(t, cfg.Params, []byte("admission"))
	completeWindowBlock(t, d, cfg, block, 0)
	if _, err := d.AddVerifiedShard(windowShard(block, cfg, 1, 0)); err != nil {
		t.Fatal(err)
	}
	before := *d.replay
	stats := d.Stats()
	valid := windowShard(block, cfg, math.MaxUint64, 0)
	for _, mutate := range []func(*IncomingShard){
		func(shard *IncomingShard) { shard.Key.SessionID[0]++ },
		func(shard *IncomingShard) { shard.Params.DataShards++ },
		func(shard *IncomingShard) { shard.Index = len(block.Shards) },
		func(shard *IncomingShard) { shard.OriginalLength = math.MaxInt },
		func(shard *IncomingShard) { shard.Payload = shard.Payload[:1] },
	} {
		invalid := valid
		mutate(&invalid)
		if _, err := d.AddVerifiedShard(invalid); !errors.Is(err, ErrInvalidShard) {
			t.Fatalf("invalid shard rejection = %v", err)
		}
		if *d.replay != before || d.Stats() != stats {
			t.Fatal("invalid shard advanced or mutated the replay window")
		}
	}
	if _, err := d.AddVerifiedShard(valid); !errors.Is(err, ErrDecoderFull) {
		t.Fatalf("capacity rejection = %v", err)
	}
	if *d.replay != before || d.Stats() != stats {
		t.Fatal("decoder-full admission advanced the replay window")
	}
	if result, err := d.AddVerifiedShard(windowShard(block, cfg, 0, 4)); err != nil || result.Outcome != OutcomeDuplicate {
		t.Fatalf("rejected high ID erased completed low ID: %+v %v", result, err)
	}
}

func TestReplayWindowCompletedBitsSurviveLowRateLongDelays(t *testing.T) {
	clock := newDecoderTestClock()
	cfg := windowDecoderTestConfig(clock)
	cfg.MaxCompletedBlocks = 1
	d := newDecoderForTest(t, cfg)
	block := encodeDecoderTestBlock(t, cfg.Params, []byte("slow source"))
	for id := uint64(0); id < 16; id++ {
		completeWindowBlock(t, d, cfg, block, id)
		clock.Advance(10 * cfg.CompletionTTL)
		if expired := d.Sweep(); expired != (ExpireStats{}) {
			t.Fatalf("time alone expired completed window bits: %+v", expired)
		}
	}
	for id := uint64(0); id < 16; id++ {
		for index := range len(block.Shards) {
			if result, err := d.AddVerifiedShard(windowShard(block, cfg, id, index)); err != nil || result.Outcome != OutcomeDuplicate {
				t.Fatalf("delayed completed ID %d reopened: %+v %v", id, result, err)
			}
		}
	}
	if d.Stats().CompletedBlocks != 16 || d.Stats().PendingBlocks != 0 || cfg.Statistics.CompletedBlocks.Load() != 16 || cfg.Statistics.CompletedCapacityEvictions.Load() != 0 {
		t.Fatal("window remained tied to legacy completed capacity or TTL")
	}
}

func TestReplayWindowSessionAndDirectionIsolationAndClose(t *testing.T) {
	cfg := windowDecoderTestConfig(newDecoderTestClock())
	block := encodeDecoderTestBlock(t, cfg.Params, []byte("independent directions"))
	first := newDecoderForTest(t, cfg)
	secondConfig := cfg
	secondConfig.ReplayWindow = &ReplayWindowConfig{SessionID: [16]byte{2}}
	second := newDecoderForTest(t, secondConfig)
	opposite := newDecoderForTest(t, cfg)
	completeWindowBlock(t, first, cfg, block, math.MaxUint64)
	completeWindowBlock(t, second, secondConfig, block, 0)
	completeWindowBlock(t, opposite, cfg, block, 0)
	if _, err := second.AddVerifiedShard(windowShard(block, cfg, math.MaxUint64, 0)); !errors.Is(err, ErrInvalidShard) {
		t.Fatalf("another Session poisoned the bound decoder: %v", err)
	}
	if second.replay.highest != 0 || opposite.replay.highest != 0 {
		t.Fatal("one Session or direction advanced another's receive window")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if first.replay != nil || first.Stats() != (Stats{}) {
		t.Fatal("Close retained the bitmap or pending state")
	}
	if _, err := first.AddVerifiedShard(windowShard(block, cfg, 0, 0)); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed decoder accepted a shard: %v", err)
	}
	completeWindowBlock(t, second, secondConfig, block, 1)
	completeWindowBlock(t, opposite, cfg, block, 1)
}

func TestReplayWindowRingMatchesReferenceAcrossWrapsAndJumps(t *testing.T) {
	w := &replayWindow{}
	completed := make(map[uint64]bool)
	random := rand.New(rand.NewSource(18))
	highest := uint64(0)
	for step := 0; step < 4000; step++ {
		if step%100 == 0 {
			highest += 2 * ReplayWindowIDs
		} else {
			highest += uint64(random.Intn(200))
		}
		w.admit(highest)
		for id := range completed {
			if highest-id >= ReplayWindowIDs {
				delete(completed, id)
			}
		}
		id := highest - uint64(random.Intn(ReplayWindowIDs))
		w.complete(id)
		completed[id] = true
		if w.count != len(completed) {
			t.Fatalf("ring count=%d reference=%d step=%d", w.count, len(completed), step)
		}
		for id := range completed {
			if !w.contains(id) {
				t.Fatalf("ring lost completed ID %d at highest %d", id, highest)
			}
		}
		if w.contains(highest-ReplayWindowIDs) || !w.tooOld(highest-ReplayWindowIDs) {
			t.Fatal("ring boundary accepted a retired ID")
		}
	}
}

func TestReplayWindowConcurrentShardsDoNotRedeliver(t *testing.T) {
	cfg := windowDecoderTestConfig(newDecoderTestClock())
	cfg.MaxPendingBlocks = 128
	d := newDecoderForTest(t, cfg)
	block := encodeDecoderTestBlock(t, cfg.Params, []byte("concurrent delivery"))
	const ids = 128
	deliveries := make([]int, ids)
	var resultsMu sync.Mutex
	var workers sync.WaitGroup
	for worker := range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for id := range ids {
				for offset := range len(block.Shards) {
					result, err := d.AddVerifiedShard(windowShard(block, cfg, uint64(id), (offset+worker)%len(block.Shards)))
					if err != nil {
						t.Error(err)
						return
					}
					if result.Outcome == OutcomeComplete {
						resultsMu.Lock()
						deliveries[id]++
						resultsMu.Unlock()
					}
				}
			}
		}()
	}
	workers.Wait()
	want := make([]int, ids)
	for id := range want {
		want[id] = 1
	}
	if !reflect.DeepEqual(deliveries, want) || d.Stats().PendingBlocks != 0 {
		t.Fatalf("concurrent shard replay changed deliveries: %v", deliveries)
	}
}

func TestReplayWindowUsesFixedStorageInsteadOfCompletedCache(t *testing.T) {
	cfg := windowDecoderTestConfig(newDecoderTestClock())
	cfg.CompletionTTL = 0
	cfg.MaxCompletedBlocks = 0
	d := newDecoderForTest(t, cfg)
	if len(d.replay.completed)*8 != 8192 || d.completed != nil || d.completedExpiry != nil {
		t.Fatal("window mode allocated a legacy completed cache or changed the fixed bitmap budget")
	}
	block := encodeDecoderTestBlock(t, cfg.Params, []byte("fixed budget"))
	completeWindowBlock(t, d, cfg, block, 0)
	if d.Stats().CompletedBlocks != 1 {
		t.Fatal("window mode did not expose retained completed bits")
	}
}

func TestReplayWindowExpiredPendingCannotBePinnedAgain(t *testing.T) {
	clock := newDecoderTestClock()
	cfg := windowDecoderTestConfig(clock)
	d := newDecoderForTest(t, cfg)
	block := encodeDecoderTestBlock(t, cfg.Params, []byte("expiry"))
	if _, err := d.AddVerifiedShard(windowShard(block, cfg, 1, 0)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(cfg.DecodeTimeout)
	completeWindowBlock(t, d, cfg, block, ReplayWindowIDs+1)
	clock.Advance(time.Nanosecond)
	if result, err := d.AddVerifiedShard(windowShard(block, cfg, 1, 1)); err != nil || result.Outcome != OutcomeTooOld {
		t.Fatalf("expired ID regained a pin: %+v %v", result, err)
	}
}
