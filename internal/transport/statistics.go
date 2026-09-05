package transport

import (
	"sync/atomic"
	"time"

	"github.com/mofelee/mpudp/internal/metrics"
)

// Counters may be shared by multiple sockets with the same configured path
// index. Its storage remains fixed across socket rebuilds and Session churn.
type Counters struct {
	DiagnosticsEnabled   *atomic.Bool
	SentPackets          atomic.Uint64
	SentBytes            atomic.Uint64
	SendErrors           atomic.Uint64
	ReceivedPackets      atomic.Uint64
	ReceivedBytes        atomic.Uint64
	ReceiveOversizeDrops atomic.Uint64
	WriteQueue           metrics.Latency
	SocketWrite          metrics.Latency
	SentPacketSizes      metrics.PacketSizes
	ReceivedPacketSizes  metrics.PacketSizes
}

func (c *Counters) start() time.Time {
	if c != nil && c.DiagnosticsEnabled != nil && c.DiagnosticsEnabled.Load() {
		return time.Now()
	}
	return time.Time{}
}

func (c *Counters) receive(size, limit int) {
	if c == nil {
		return
	}
	c.ReceivedPackets.Add(1)
	c.ReceivedBytes.Add(uint64(size))
	if size > limit {
		c.ReceiveOversizeDrops.Add(1)
	}
	if c.DiagnosticsEnabled != nil && c.DiagnosticsEnabled.Load() {
		c.ReceivedPacketSizes.Observe(size)
	}
}

func (c *Counters) wrote(size int, complete bool, err error, started time.Time) {
	if c == nil {
		return
	}
	if size > 0 {
		c.SentBytes.Add(uint64(size))
	}
	if complete {
		c.SentPackets.Add(1)
		if !started.IsZero() {
			c.SentPacketSizes.Observe(size)
		}
	}
	if err != nil || !complete {
		c.SendErrors.Add(1)
	}
	if !started.IsZero() {
		c.SocketWrite.Observe(time.Since(started))
	}
}
