// Package metrics provides bounded counters without packet content or identity.
package metrics

import (
	"sync/atomic"
	"time"
)

// LatencySnapshot uses non-cumulative buckets: <= 1us, <= 2us, ...,
// <= 4194304us, and an overflow bucket. Durations are nanoseconds.
type LatencySnapshot struct {
	Count   uint64     `json:"count"`
	TotalNS uint64     `json:"total_ns"`
	MaxNS   uint64     `json:"max_ns"`
	Buckets [24]uint64 `json:"buckets"`
}

type Latency struct {
	count   atomic.Uint64
	total   atomic.Uint64
	max     atomic.Uint64
	buckets [24]atomic.Uint64
}

func (l *Latency) Observe(duration time.Duration) {
	if duration < 0 {
		duration = 0
	}
	ns := uint64(duration)
	l.count.Add(1)
	l.total.Add(ns)
	for old := l.max.Load(); ns > old; old = l.max.Load() {
		if l.max.CompareAndSwap(old, ns) {
			break
		}
	}
	bucket, upper := 0, uint64(time.Microsecond)
	for bucket < len(l.buckets)-1 && ns > upper {
		bucket++
		upper *= 2
	}
	l.buckets[bucket].Add(1)
}

func (l *Latency) Snapshot() LatencySnapshot {
	s := LatencySnapshot{Count: l.count.Load(), TotalNS: l.total.Load(), MaxNS: l.max.Load()}
	for i := range s.Buckets {
		s.Buckets[i] = l.buckets[i].Load()
	}
	return s
}

// PacketSizeBounds are inclusive upper bounds for non-cumulative buckets.
var PacketSizeBounds = [16]uint64{64, 128, 256, 512, 768, 1024, 1200, 1280, 1400, 1472, 1500, 2048, 4096, 8192, 16384, 65535}

type PacketSizeSnapshot struct {
	UpperBounds [16]uint64 `json:"upper_bounds"`
	Counts      [16]uint64 `json:"counts"`
}

type PacketSizes struct{ buckets [16]atomic.Uint64 }

func (p *PacketSizes) Observe(size int) {
	for i, upper := range PacketSizeBounds {
		if uint64(size) <= upper {
			p.buckets[i].Add(1)
			return
		}
	}
}

func (p *PacketSizes) Snapshot() PacketSizeSnapshot {
	s := PacketSizeSnapshot{UpperBounds: PacketSizeBounds}
	for i := range s.Counts {
		s.Counts[i] = p.buckets[i].Load()
	}
	return s
}
