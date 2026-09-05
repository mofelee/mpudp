package sessionv2

import (
	"fmt"
	"testing"
	"time"
)

var benchmarkGroupDeadline time.Time

func BenchmarkPendingGroupDeadlines(b *testing.B) {
	for _, count := range []int{0, 128, 512, 1024, 8192} {
		b.Run(fmt.Sprintf("groups_%d", count), func(b *testing.B) {
			now := time.Unix(1700000000, 0)
			c := &Controller{started: true, paths: make([]pathState, 5), cfg: Config{GroupTimeout: 10 * time.Second}, groups: make(map[uint64]*pendingGroup)}
			for id := 1; id <= count; id++ {
				c.insertGroup(uint64(id), &pendingGroup{admitted: now.Add(time.Duration(id) * time.Nanosecond)})
			}
			b.Run("NextDeadline", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					benchmarkGroupDeadline = c.NextDeadline()
				}
			})
			b.Run("expireGroups", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					c.expireGroups(now)
				}
			})
			b.Run("retryGroups", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if err := c.retryGroups(now, &Result{}); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
