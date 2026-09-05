package fec

import (
	"fmt"
	"testing"
)

func BenchmarkDelayedParityCapacity(b *testing.B) {
	for _, window := range []bool{false, true} {
		for _, capacity := range []int{8, 16, 32} {
			b.Run(fmt.Sprintf("window_%t/completed_%d", window, capacity), func(b *testing.B) {
				var result delayedParityResult
				b.ReportAllocs()
				for range b.N {
					result = delayedParityWorkload(b, capacity, window)
				}
				b.ReportMetric(float64(result.Evictions), "evictions/workload")
				b.ReportMetric(float64(result.Pending.PendingBlocks), "reopened-blocks/workload")
				b.ReportMetric(float64(result.Pending.PendingBytes), "pending-bytes/workload")
				b.ReportMetric(float64(result.Full), "decoder-full/workload")
			})
		}
	}
}
