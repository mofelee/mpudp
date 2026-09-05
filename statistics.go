package mpudp

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/mofelee/mpudp/internal/fec"
	"github.com/mofelee/mpudp/internal/metrics"
	"github.com/mofelee/mpudp/internal/transport"
)

// LatencyStatistics is a fixed-size distribution. Buckets are non-cumulative:
// <= 1us, <= 2us, ..., <= 4194304us, then overflow. Times are nanoseconds.
type LatencyStatistics metrics.LatencySnapshot

// PacketSizeStatistics counts complete UDP payload lengths in fixed buckets.
// UpperBounds are inclusive and Counts are non-cumulative.
type PacketSizeStatistics metrics.PacketSizeSnapshot

// FECStatistics contains cumulative decoder events and retained-state gauges.
// Recovered counts require parity reconstruction of missing data shards.
// LateShards only counts blocks still in the completed cache. Capacity evictions
// exclude TTL expiry. Neither event alone proves network loss or state reopening.
// Pending gauges aggregate all live decoders; bytes count owned shard payloads,
// excluding map/codec overhead. High-water marks persist after state is released.
type FECStatistics struct {
	CompletedBlocks            uint64 `json:"completed_blocks"`
	CompletedCapacityEvictions uint64 `json:"completed_capacity_evictions"`
	RecoveredBlocks            uint64 `json:"recovered_blocks"`
	RecoveredShards            uint64 `json:"recovered_shards"`
	ExpiredBlocks              uint64 `json:"expired_blocks"`
	DecoderFull                uint64 `json:"decoder_full"`
	LateShards                 uint64 `json:"late_shards"`
	DuplicateShards            uint64 `json:"duplicate_shards"`
	PendingBlocks              int64  `json:"pending_blocks"`
	PendingShards              int64  `json:"pending_shards"`
	PendingBytes               int64  `json:"pending_bytes"`
	PendingBlocksHighWater     uint64 `json:"pending_blocks_high_water"`
	PendingShardsHighWater     uint64 `json:"pending_shards_high_water"`
	PendingBytesHighWater      uint64 `json:"pending_bytes_high_water"`
}

// PathStatistics aggregates sockets at one configured Carrier index across
// Sessions. The listener row aggregates its single shared socket. Path contains
// only "carrier-N" or "listener", never a remote address or Session ID.
type PathStatistics struct {
	Path                 string               `json:"path"`
	SentPackets          uint64               `json:"sent_packets"`
	SentBytes            uint64               `json:"sent_bytes"`
	SendErrors           uint64               `json:"send_errors"`
	ReceivedPackets      uint64               `json:"received_packets"`
	ReceivedBytes        uint64               `json:"received_bytes"`
	ReceiveOversizeDrops uint64               `json:"receive_oversize_drops"`
	WriteQueue           LatencyStatistics    `json:"write_queue"`
	SocketWrite          LatencyStatistics    `json:"socket_write"`
	SentPacketSizes      PacketSizeStatistics `json:"sent_packet_sizes"`
	ReceivedPacketSizes  PacketSizeStatistics `json:"received_packet_sizes"`
}

// Statistics is a bounded, non-secret Peer lifetime snapshot. Counters are
// monotonic, including after Session/Peer closure, except pending-state gauges
// which decrease on completion, expiry, and Close. Fields are sampled independently
// and need not describe the same instant during concurrent I/O.
// Subtract successive snapshots and divide by elapsed time for bytes/s or PPS.
type Statistics struct {
	CapturedAt         time.Time         `json:"captured_at"`
	DiagnosticsEnabled bool              `json:"diagnostics_enabled"`
	IngressAccepted    uint64            `json:"ingress_accepted"`
	IngressDrops       uint64            `json:"ingress_drops"`
	DeliveryAccepted   uint64            `json:"delivery_accepted"`
	DeliveryDrops      uint64            `json:"delivery_drops"`
	DeliveredPackets   uint64            `json:"delivered_packets"`
	DeliveredBytes     uint64            `json:"delivered_bytes"`
	SentDatagrams      uint64            `json:"sent_datagrams"`
	SentDatagramBytes  uint64            `json:"sent_datagram_bytes"`
	IngressQueue       LatencyStatistics `json:"ingress_queue"`
	SendLatency        LatencyStatistics `json:"send_latency"`
	FEC                FECStatistics     `json:"fec"`
	Paths              []PathStatistics  `json:"paths"`
}

type peerCounters struct {
	enabled           atomic.Bool
	ingressAccepted   atomic.Uint64
	ingressDrops      atomic.Uint64
	deliveryAccepted  atomic.Uint64
	deliveryDrops     atomic.Uint64
	deliveredPackets  atomic.Uint64
	deliveredBytes    atomic.Uint64
	sentDatagrams     atomic.Uint64
	sentDatagramBytes atomic.Uint64
	ingressQueue      metrics.Latency
	sendLatency       metrics.Latency
	fec               fec.Counters
	carriers          []*transport.Counters
	listener          *transport.Counters
}

func (p *Peer) initStatistics() {
	p.statistics.carriers = make([]*transport.Counters, len(p.config.Carriers))
	for i := range p.statistics.carriers {
		p.statistics.carriers[i] = &transport.Counters{DiagnosticsEnabled: &p.statistics.enabled}
	}
	if p.config.ListenerEnabled() {
		p.statistics.listener = &transport.Counters{DiagnosticsEnabled: &p.statistics.enabled}
	}
}

// SetDiagnosticsEnabled enables optional queue/socket timing and packet-size
// histograms, disabled by default. Basic counters remain enabled. Disabling
// diagnostics retains prior observations; operations in flight may finish an
// observation after this call. This method is safe during concurrent I/O.
func (p *Peer) SetDiagnosticsEnabled(enabled bool) { p.statistics.enabled.Store(enabled) }

// Statistics returns aggregate diagnostics without configuration secrets, packet
// content, Session IDs, endpoint addresses, or unbounded per-packet records.
func (p *Peer) Statistics() Statistics {
	c := &p.statistics
	s := Statistics{
		CapturedAt: time.Now(), DiagnosticsEnabled: c.enabled.Load(),
		IngressAccepted: c.ingressAccepted.Load(), IngressDrops: c.ingressDrops.Load(),
		DeliveryAccepted: c.deliveryAccepted.Load(), DeliveryDrops: c.deliveryDrops.Load(),
		DeliveredPackets: c.deliveredPackets.Load(), DeliveredBytes: c.deliveredBytes.Load(),
		SentDatagrams: c.sentDatagrams.Load(), SentDatagramBytes: c.sentDatagramBytes.Load(),
		IngressQueue: LatencyStatistics(c.ingressQueue.Snapshot()),
		SendLatency:  LatencyStatistics(c.sendLatency.Snapshot()),
		FEC: FECStatistics{
			CompletedBlocks: c.fec.CompletedBlocks.Load(), RecoveredBlocks: c.fec.RecoveredBlocks.Load(),
			CompletedCapacityEvictions: c.fec.CompletedCapacityEvictions.Load(),
			RecoveredShards:            c.fec.RecoveredShards.Load(), ExpiredBlocks: c.fec.ExpiredBlocks.Load(),
			DecoderFull: c.fec.DecoderFull.Load(), LateShards: c.fec.LateShards.Load(),
			DuplicateShards: c.fec.DuplicateShards.Load(),
			PendingBlocks:   c.fec.PendingBlocks.Load(), PendingShards: c.fec.PendingShards.Load(),
			PendingBytes: c.fec.PendingBytes.Load(), PendingBlocksHighWater: c.fec.PendingBlocksHighWater.Load(),
			PendingShardsHighWater: c.fec.PendingShardsHighWater.Load(), PendingBytesHighWater: c.fec.PendingBytesHighWater.Load(),
		},
		Paths: make([]PathStatistics, 0, len(c.carriers)+1),
	}
	for i, counters := range c.carriers {
		s.Paths = append(s.Paths, pathStatistics(fmt.Sprintf("carrier-%d", i), counters))
	}
	if c.listener != nil {
		s.Paths = append(s.Paths, pathStatistics("listener", c.listener))
	}
	return s
}

func pathStatistics(path string, c *transport.Counters) PathStatistics {
	return PathStatistics{
		Path: path, SentPackets: c.SentPackets.Load(), SentBytes: c.SentBytes.Load(),
		SendErrors: c.SendErrors.Load(), ReceivedPackets: c.ReceivedPackets.Load(),
		ReceivedBytes: c.ReceivedBytes.Load(), ReceiveOversizeDrops: c.ReceiveOversizeDrops.Load(),
		WriteQueue: LatencyStatistics(c.WriteQueue.Snapshot()), SocketWrite: LatencyStatistics(c.SocketWrite.Snapshot()),
		SentPacketSizes:     PacketSizeStatistics(c.SentPacketSizes.Snapshot()),
		ReceivedPacketSizes: PacketSizeStatistics(c.ReceivedPacketSizes.Snapshot()),
	}
}
