package fec

import "math/bits"

// ReplayWindowIDs is the fixed v1 receive reordering span, independent of the
// pending block bound. Each Session receive direction owns an 8 KiB bitmap.
const ReplayWindowIDs = 65536

// ReplayWindowConfig enables bounded lifetime deduplication for one Session's
// receive direction. Nil DecoderConfig.ReplayWindow selects the legacy cache.
type ReplayWindowConfig struct {
	SessionID [16]byte
}

type replayWindow struct {
	sessionID [16]byte
	completed [ReplayWindowIDs / 64]uint64
	highest   uint64
	started   bool
	count     int
}

func (w *replayWindow) tooOld(id uint64) bool {
	return w.started && id < w.highest && w.highest-id >= ReplayWindowIDs
}

func (w *replayWindow) contains(id uint64) bool {
	if !w.started || id > w.highest || w.tooOld(id) {
		return false
	}
	index := id % ReplayWindowIDs
	return w.completed[index/64]&(uint64(1)<<(index%64)) != 0
}

// admit advances only after a new pending block has passed every admission
// check. Clearing ring positions costs at most the bitmap size, even at MaxUint64.
func (w *replayWindow) admit(id uint64) {
	if !w.started {
		w.started, w.highest = true, id
		return
	}
	if id <= w.highest {
		return
	}
	distance := id - w.highest
	if distance >= ReplayWindowIDs {
		clear(w.completed[:])
		w.count = 0
	} else {
		position := (w.highest + 1) % ReplayWindowIDs
		for remaining := distance; remaining != 0; {
			width := min(remaining, 64-position%64)
			mask := (^uint64(0) >> (64 - width)) << (position % 64)
			word := &w.completed[position/64]
			w.count -= bits.OnesCount64(*word & mask)
			*word &^= mask
			position = (position + width) % ReplayWindowIDs
			remaining -= width
		}
	}
	w.highest = id
}

func (w *replayWindow) complete(id uint64) {
	// Already admitted pending blocks remain completable below the floor. Their
	// later shards are rejected by that monotonic floor without needing a bit.
	if w.tooOld(id) {
		return
	}
	index := id % ReplayWindowIDs
	mask := uint64(1) << (index % 64)
	if w.completed[index/64]&mask == 0 {
		w.completed[index/64] |= mask
		w.count++
	}
}
