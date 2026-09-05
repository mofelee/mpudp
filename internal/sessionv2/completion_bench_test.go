package sessionv2

import (
	"fmt"
	"testing"
)

var benchmarkCompletion struct {
	through, failedFrom uint64
	sendError           error
}

func BenchmarkCompletionPolling(b *testing.B) {
	for _, paths := range []int{1, 5, 256} {
		b.Run(fmt.Sprintf("paths_%d", paths), func(b *testing.B) {
			p := newPair(b, paths, 1, false, 5000)
			p.pump(b, p.ready)
			b.Cleanup(func() { p.close(b) })
			c := p.client.controller
			for _, groups := range []int{0, 1057} {
				for id := 1; id <= groups; id++ {
					c.insertGroup(uint64(id), &pendingGroup{admitted: p.now})
				}
				b.Run(fmt.Sprintf("groups_%d", groups), func(b *testing.B) {
					b.Run("Snapshot", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							s := c.Snapshot()
							benchmarkCompletion.through, benchmarkCompletion.failedFrom, benchmarkCompletion.sendError = s.CompletedThrough, s.FailedFrom, s.SendError
						}
					})
					b.Run("Completion", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							benchmarkCompletion.through, benchmarkCompletion.failedFrom, benchmarkCompletion.sendError = c.Completion()
						}
					})
				})
			}
		})
	}
}
