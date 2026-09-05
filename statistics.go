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
// LateShards counts known completed blocks within the replay window. TooOldShards
// counts unknown or previously completed IDs below its floor, excluding admitted
// pending blocks. Neither event alone proves network loss. Capacity evictions
// apply only to legacy internal decoder caches, excluding their TTL expiry.
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
	TooOldShards               uint64 `json:"too_old_shards"`
	DuplicateShards            uint64 `json:"duplicate_shards"`
	PendingBlocks              int64  `json:"pending_blocks"`
	PendingShards              int64  `json:"pending_shards"`
	PendingBytes               int64  `json:"pending_bytes"`
	PendingBlocksHighWater     uint64 `json:"pending_blocks_high_water"`
	PendingShardsHighWater     uint64 `json:"pending_shards_high_water"`
	PendingBytesHighWater      uint64 `json:"pending_bytes_high_water"`
}

// V2ReceiveStatistics contains authenticated FEC handler attempts, resource
// rejection attempts and group lifecycle counters. Original admission retries
// count separately. ExpiredGroups includes terminal expiry/error events, not
// Close. Pending and credit fields are current aggregate gauges, not totals.
type V2ReceiveStatistics struct {
	ReceivedFECBundles          uint64 `json:"received_fec_bundles"`
	PacketScratchRejections     uint64 `json:"packet_scratch_rejections"`
	NewGroupRejections          uint64 `json:"new_group_rejections"`
	OriginalAdmissionRejections uint64 `json:"original_admission_rejections"`
	DecodedGroups               uint64 `json:"decoded_groups"`
	CompletedGroups             uint64 `json:"completed_groups"`
	ExpiredGroups               uint64 `json:"expired_groups"`
	PendingGroups               int64  `json:"pending_groups"`
	DecodedPendingGroups        int64  `json:"decoded_pending_groups"`
	PendingOriginals            int64  `json:"pending_originals"`
	CreditBytes                 uint64 `json:"credit_bytes"`
	CreditReservations          int64  `json:"credit_reservations"`
}

// PathStatistics counts UDP traffic for an anonymous path or socket aggregate.
// Path never contains a remote address or Session ID.
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
	CapturedAt         time.Time            `json:"captured_at"`
	DiagnosticsEnabled bool                 `json:"diagnostics_enabled"`
	IngressAccepted    uint64               `json:"ingress_accepted"`
	IngressDrops       uint64               `json:"ingress_drops"`
	DeliveryAccepted   uint64               `json:"delivery_accepted"`
	DeliveryDrops      uint64               `json:"delivery_drops"`
	DeliveredPackets   uint64               `json:"delivered_packets"`
	DeliveredBytes     uint64               `json:"delivered_bytes"`
	SentDatagrams      uint64               `json:"sent_datagrams"`
	SentDatagramBytes  uint64               `json:"sent_datagram_bytes"`
	IngressQueue       LatencyStatistics    `json:"ingress_queue"`
	SendLatency        LatencyStatistics    `json:"send_latency"`
	FEC                FECStatistics        `json:"fec"`
	V2Receive          *V2ReceiveStatistics `json:"v2_receive,omitempty"`
	Paths              []PathStatistics     `json:"paths"`
	// ListenerPaths counts authenticated, protocol-accepted traffic in at most
	// 256 lifetime slots plus listener-overflow. Paths retains the raw socket
	// aggregate, including rejected packets. An accepted CLOSE is attributed
	// only when its source is already an Endpoint of that Session.
	ListenerPaths []PathStatistics `json:"listener_paths"`
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
	listenerPaths     *transport.ListenerPathCounters
}

func (p *Peer) initStatistics() {
	p.statistics.carriers = make([]*transport.Counters, len(p.config.Carriers))
	for i := range p.statistics.carriers {
		p.statistics.carriers[i] = &transport.Counters{DiagnosticsEnabled: &p.statistics.enabled}
	}
	if p.config.ListenerEnabled() {
		p.statistics.listener = &transport.Counters{DiagnosticsEnabled: &p.statistics.enabled}
		p.statistics.listenerPaths = transport.NewListenerPathCounters(&p.statistics.enabled)
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
			TooOldShards:    c.fec.TooOldShards.Load(),
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
	paths, overflow := c.listenerPaths.Snapshot()
	s.ListenerPaths = make([]PathStatistics, 0, len(paths)+1)
	for i, counters := range paths {
		s.ListenerPaths = append(s.ListenerPaths, pathStatistics(fmt.Sprintf("listener-path-%d", i), counters))
	}
	if overflow != nil {
		s.ListenerPaths = append(s.ListenerPaths, pathStatistics("listener-overflow", overflow))
	}
	if p.v2 != nil {
		s.V2Receive = p.v2.receiveStatistics()
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
