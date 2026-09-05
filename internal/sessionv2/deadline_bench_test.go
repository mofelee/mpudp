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

func BenchmarkNegotiatedPathCapacity(b *testing.B) {
	for _, counts := range [][2]int{{5, 5}, {5, 256}, {256, 5}, {256, 256}} {
		b.Run(fmt.Sprintf("client_%d/server_%d", counts[0], counts[1]), func(b *testing.B) {
			p := newPairWithProfiles(b, profile(counts[0]), profile(counts[1]), 5, false, nil)
			p.pump(b, p.ready)
			b.Cleanup(func() { p.close(b) })
			for _, side := range []struct {
				name string
				e    *endpoint
			}{{"client", p.client}, {"server", p.server}} {
				b.Run(side.name, func(b *testing.B) {
					c := side.e.controller
					for _, count := range []int{0, 1057} {
						for id := 1; id <= count; id++ {
							c.insertGroup(uint64(id), &pendingGroup{admitted: p.now})
						}
						b.Run(fmt.Sprintf("groups_%d/NextDeadline", count), func(b *testing.B) {
							b.ReportAllocs()
							for b.Loop() {
								benchmarkGroupDeadline = c.NextDeadline()
							}
						})
					}
					b.Run("driveControl", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							c.driveControl(p.now, &Result{})
						}
					})
					b.Run("expireControl", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							if err := c.expireControl(p.now); err != nil {
								b.Fatal(err)
							}
						}
					})
				})
			}
		})
	}
}
