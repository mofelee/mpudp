package main

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"
	"sync"
	"time"
)

const (
	headerSize = 40
	frameMagic = 0x4d505031
	kindData   = 1
	kindProbe  = 2
	kindHello  = 3
	kindReady  = 4
	kindEcho   = 5
	dedupSize  = 65536
)

var checksumTable = crc32.MakeTable(crc32.Castagnoli)

func payload(size int, nonce uint64, flow int) []byte {
	b := make([]byte, size)
	binary.BigEndian.PutUint32(b, frameMagic)
	binary.BigEndian.PutUint64(b[8:], nonce)
	binary.BigEndian.PutUint32(b[16:], uint32(flow))
	x := uint32(nonce) ^ uint32(nonce>>32) ^ uint32(flow)
	for i := headerSize; i < len(b); i++ {
		x = x*1664525 + 1013904223
		b[i] = byte(x >> 24)
	}
	return b
}

func stamp(b []byte, kind uint32, seq uint64, sent int64) {
	binary.BigEndian.PutUint32(b[20:], kind)
	binary.BigEndian.PutUint64(b[24:], seq)
	binary.BigEndian.PutUint64(b[32:], uint64(sent))
	binary.BigEndian.PutUint32(b[4:], crc32.Checksum(b[8:], checksumTable))
}

func validPayload(b, want []byte) bool {
	return len(b) == len(want) && len(b) >= headerSize &&
		binary.BigEndian.Uint32(b) == frameMagic &&
		bytes.Equal(b[8:20], want[8:20]) && bytes.Equal(b[headerSize:], want[headerSize:]) &&
		binary.BigEndian.Uint32(b[4:]) == crc32.Checksum(b[8:], checksumTable)
}

// The fixed sequence window rejects replays without retaining every packet in
// a long run. Packets reordered beyond the window are reported, never counted.
type sequenceWindow struct {
	slots [dedupSize]uint64
	max   uint64
}

func (w *sequenceWindow) accept(seq uint64) (unique, tooOld bool) {
	if seq == math.MaxUint64 || (w.max >= dedupSize && seq <= w.max-dedupSize) {
		return false, true
	}
	if w.slots[seq%dedupSize] == seq+1 {
		return false, false
	}
	w.slots[seq%dedupSize] = seq + 1
	if seq > w.max {
		w.max = seq
	}
	return true, false
}

type bucket struct {
	VerifiedBytes   uint64 `json:"verified_bytes"`
	VerifiedPackets uint64 `json:"verified_packets"`
	CorruptFrames   uint64 `json:"corrupt_frames"`
	DuplicateFrames uint64 `json:"duplicate_frames"`
	TooOldFrames    uint64 `json:"too_old_frames"`
}

type receiverCounters struct {
	mu      sync.Mutex
	start   time.Time
	buckets []bucket
}

func (c *receiverCounters) record(size int, valid, unique, old bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := int(time.Since(c.start) / time.Second)
	if time.Now().Before(c.start) || i >= len(c.buckets) {
		return
	}
	b := &c.buckets[i]
	switch {
	case !valid:
		b.CorruptFrames++
	case old:
		b.TooOldFrames++
	case !unique:
		b.DuplicateFrames++
	default:
		b.VerifiedBytes += uint64(size - headerSize)
		b.VerifiedPackets++
	}
}

func (c *receiverCounters) snapshot(i int) bucket {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buckets[i]
}

// RTT uses a bounded histogram. Quantiles are upper bucket bounds at 1 ms
// resolution; overflow is explicit instead of silently clipping slow replies.
type latencyCounters struct {
	mu                                          sync.Mutex
	buckets                                     [10001]uint64
	sent, received, overflow                    uint64
	submitted, queueMissed, writeFailed, onTime uint64
}

func (l *latencyCounters) markSent()        { l.mu.Lock(); l.sent++; l.mu.Unlock() }
func (l *latencyCounters) markSubmitted()   { l.mu.Lock(); l.submitted++; l.mu.Unlock() }
func (l *latencyCounters) markQueueMissed() { l.mu.Lock(); l.queueMissed++; l.mu.Unlock() }
func (l *latencyCounters) markWriteFailed() { l.mu.Lock(); l.writeFailed++; l.mu.Unlock() }
func (l *latencyCounters) observe(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.received++
	if d <= time.Second {
		l.onTime++
	}
	i := int((d + time.Millisecond - 1) / time.Millisecond)
	if i < 0 || i >= len(l.buckets) {
		l.overflow++
		return
	}
	l.buckets[i]++
}

type latencySummary struct {
	Sent           uint64   `json:"sent"`
	Scheduled      uint64   `json:"scheduled"`
	Submitted      uint64   `json:"submitted"`
	QueueMissed    uint64   `json:"queue_missed"`
	WriteFailed    uint64   `json:"write_failed"`
	Received       uint64   `json:"received"`
	Unanswered     uint64   `json:"unanswered"`
	OnTime         uint64   `json:"on_time"`
	DeadlineMissed uint64   `json:"deadline_missed"`
	DeadlineMS     int      `json:"deadline_ms"`
	Overflow       uint64   `json:"over_10000_ms"`
	P50MS          *float64 `json:"p50_ms"`
	P95MS          *float64 `json:"p95_ms"`
	P99MS          *float64 `json:"p99_ms"`
	ResolutionMS   int      `json:"resolution_ms"`
}

func (l *latencyCounters) snapshot() latencySummary {
	l.mu.Lock()
	defer l.mu.Unlock()
	quantile := func(p float64) *float64 {
		if l.sent == 0 {
			return nil
		}
		// Unanswered opportunities remain in the quantile population. A null
		// quantile means its rank reached an unanswered/overflow observation.
		target := uint64(math.Ceil(float64(l.sent) * p))
		var total uint64
		for i, n := range l.buckets {
			total += n
			if total >= target {
				v := float64(i)
				return &v
			}
		}
		return nil
	}
	return latencySummary{Sent: l.submitted, Scheduled: l.sent, Submitted: l.submitted, QueueMissed: l.queueMissed,
		WriteFailed: l.writeFailed, Received: l.received, DeadlineMissed: l.sent - l.onTime, DeadlineMS: 1000,
		Unanswered: l.sent - l.received, OnTime: l.onTime,
		Overflow: l.overflow, P50MS: quantile(.5), P95MS: quantile(.95), P99MS: quantile(.99), ResolutionMS: 1}
}
