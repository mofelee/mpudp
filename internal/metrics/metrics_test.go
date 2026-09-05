package metrics

import (
	"testing"
	"time"
)

func TestLatencyBoundaryBuckets(t *testing.T) {
	var l Latency
	for _, duration := range []time.Duration{0, time.Microsecond, time.Microsecond + 1, 2 * time.Microsecond, 4194304 * time.Microsecond, 5 * time.Second} {
		l.Observe(duration)
	}
	s := l.Snapshot()
	if s.Count != 6 || s.MaxNS != uint64(5*time.Second) || s.Buckets[0] != 2 || s.Buckets[1] != 2 || s.Buckets[22] != 1 || s.Buckets[23] != 1 {
		t.Fatalf("latency = %+v", s)
	}
}

func TestPacketSizeBoundaryBuckets(t *testing.T) {
	var p PacketSizes
	for _, size := range []int{0, 64, 65, 1200, 1201, 65507} {
		p.Observe(size)
	}
	s := p.Snapshot()
	if s.Counts[0] != 2 || s.Counts[1] != 1 || s.Counts[6] != 1 || s.Counts[7] != 1 || s.Counts[15] != 1 {
		t.Fatalf("packet lengths = %+v", s)
	}
}
