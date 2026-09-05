package transport

import (
	"crypto/sha256"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mofelee/mpudp/internal/metrics"
)

// MaxListenerStatisticsPaths bounds named listener slots for the Peer lifetime.
const MaxListenerStatisticsPaths = 256

// ListenerPathCounters retains anonymous slots across Session and Endpoint
// churn. Only authenticated, semantically accepted endpoint learning may call
// Learn. Excess identities share one overflow collector and are not retained.
type ListenerPathCounters struct {
	mu       sync.Mutex
	enabled  *atomic.Bool
	byKey    map[[32]byte]*Counters
	paths    []*Counters
	overflow *Counters
}

func NewListenerPathCounters(enabled *atomic.Bool) *ListenerPathCounters {
	return &ListenerPathCounters{enabled: enabled, byKey: make(map[[32]byte]*Counters)}
}

func (c *ListenerPathCounters) Learn(key string) *Counters {
	if c == nil {
		return nil
	}
	digest := sha256.Sum256([]byte(key))
	c.mu.Lock()
	defer c.mu.Unlock()
	if counters := c.byKey[digest]; counters != nil {
		return counters
	}
	if len(c.paths) == MaxListenerStatisticsPaths {
		if c.overflow == nil {
			c.overflow = &Counters{DiagnosticsEnabled: c.enabled}
		}
		return c.overflow
	}
	counters := &Counters{DiagnosticsEnabled: c.enabled}
	c.byKey[digest] = counters
	c.paths = append(c.paths, counters)
	return counters
}

// Snapshot returns a detached list in lifetime allocation order. Counters use
// atomic fields and may continue changing while the caller samples them.
func (c *ListenerPathCounters) Snapshot() (paths []*Counters, overflow *Counters) {
	if c == nil {
		return nil, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*Counters(nil), c.paths...), c.overflow
}

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

// ReceiveAccepted records a full authenticated packet after protocol acceptance.
func (c *Counters) ReceiveAccepted(size int) { c.receive(size, size) }

func (c *Counters) wrote(size int, complete bool, err error, started time.Time) {
	var elapsed time.Duration
	if !started.IsZero() {
		elapsed = time.Since(started)
	}
	c.wroteElapsed(size, complete, err, !started.IsZero(), elapsed)
}

func (c *Counters) wroteElapsed(size int, complete bool, err error, timed bool, elapsed time.Duration) {
	if c == nil {
		return
	}
	if size > 0 {
		c.SentBytes.Add(uint64(size))
	}
	if complete {
		c.SentPackets.Add(1)
		if timed {
			c.SentPacketSizes.Observe(size)
		}
	}
	if err != nil || !complete {
		c.SendErrors.Add(1)
	}
	if timed {
		c.SocketWrite.Observe(elapsed)
	}
}
