package main

import (
	"encoding/binary"
	"math/bits"
	"sync"
	"time"
)

const (
	kcpHeaderBytes   = 24
	kcpTraceSlots    = 1024
	kcpTraceAttempts = 4
)

// Buckets are <=1us, <=2us, ... <=8388608us, then overflow. No packet
// identifiers or content are retained in exported diagnostics.
type durationDistribution struct {
	Count   uint64     `json:"count"`
	SumNS   uint64     `json:"sum_ns"`
	MaxNS   uint64     `json:"max_ns"`
	Buckets [25]uint64 `json:"buckets"`
}

func (d *durationDistribution) observe(elapsed time.Duration) {
	if elapsed < 0 {
		return
	}
	ns := uint64(elapsed)
	d.Count++
	d.SumNS += ns
	if ns > d.MaxNS {
		d.MaxNS = ns
	}
	micros := (ns + 999) / 1000
	index := 0
	if micros > 1 {
		index = bits.Len64(micros - 1)
	}
	if index >= len(d.Buckets) {
		index = len(d.Buckets) - 1
	}
	d.Buckets[index]++
}

type kcpCorrelationStats struct {
	Flow                       int                  `json:"flow"`
	PacketCorrelationAvailable bool                 `json:"packet_correlation_available"`
	Boundary                   string               `json:"boundary"`
	RetransmitReasonAvailable  bool                 `json:"retransmit_reason_available"`
	SlotLimit                  int                  `json:"slot_limit"`
	AttemptsPerSlot            int                  `json:"attempts_per_slot"`
	MalformedPackets           uint64               `json:"malformed_packets"`
	OutboundPackets            uint64               `json:"outbound_packets"`
	OutboundPushSegments       uint64               `json:"outbound_push_segments"`
	FirstObservedPushSegments  uint64               `json:"first_observed_push_segments"`
	RepeatedPushSegments       uint64               `json:"repeated_push_segments"`
	UnclassifiedPushSegments   uint64               `json:"unclassified_push_segments"`
	OutboundPushPayloadBytes   uint64               `json:"outbound_push_payload_bytes"`
	OutboundHeaderBytes        uint64               `json:"outbound_header_bytes"`
	OutboundACKSegments        uint64               `json:"outbound_ack_segments"`
	InboundPackets             uint64               `json:"inbound_packets"`
	InboundPushSegments        uint64               `json:"inbound_push_segments"`
	InboundACKSegments         uint64               `json:"inbound_ack_segments"`
	IncomingUNAAdvances        uint64               `json:"incoming_una_advances"`
	MatchedACKs                uint64               `json:"matched_acks"`
	UnmatchedACKs              uint64               `json:"unmatched_acks"`
	AmbiguousACKs              uint64               `json:"ambiguous_acks"`
	IncompleteHistoryACKs      uint64               `json:"incomplete_history_acks"`
	DuplicateACKs              uint64               `json:"duplicate_acks"`
	ACKBeforeAdapterReturn     uint64               `json:"ack_before_adapter_return"`
	SlotEvictions              uint64               `json:"slot_evictions"`
	AttemptEvictions           uint64               `json:"attempt_evictions"`
	AdapterErrors              uint64               `json:"adapter_errors"`
	ApplicationWrite           durationDistribution `json:"application_write"`
	AdapterCall                durationDistribution `json:"adapter_call"`
	EntryToACK                 durationDistribution `json:"entry_to_ack"`
	ReturnToACK                durationDistribution `json:"return_to_ack"`
}

type tracedAttempt struct {
	timestamp                       uint32
	writeID                         uint64
	entered, returned, acknowledged time.Time
}

type tracedSegment struct {
	valid             bool
	historyIncomplete bool
	sequence          uint32
	next              int
	attempts          [kcpTraceAttempts]tracedAttempt
}

type kcpTrace struct {
	mu                    sync.Mutex
	stats                 kcpCorrelationStats
	slots                 []tracedSegment
	conv                  uint32
	writeID               uint64
	highestSN, highestUNA uint32
	haveSN, haveUNA       bool
}

func newKCPTrace(packetCorrelation bool) *kcpTrace {
	t := &kcpTrace{conv: 42, stats: kcpCorrelationStats{PacketCorrelationAvailable: packetCorrelation,
		Boundary: "application_write_only; native_batch_socket_correlation_unavailable"}}
	if packetCorrelation {
		t.slots = make([]tracedSegment, kcpTraceSlots)
		t.stats.Boundary = "mpudp_datagram_adapter_call; not_individual_socket_write"
		t.stats.SlotLimit, t.stats.AttemptsPerSlot = kcpTraceSlots, kcpTraceAttempts
	}
	return t
}

type kcpHeader struct {
	command                          byte
	timestamp, sequence, una, length uint32
}

// Validate the complete concatenated KCP packet before recording anything.
// Payload bytes are skipped, not copied, hashed, formatted, or exported.
func validKCPPacket(packet []byte, conv uint32) bool {
	if len(packet) < kcpHeaderBytes || len(packet) > 1500 {
		return false
	}
	for len(packet) > 0 {
		if len(packet) < kcpHeaderBytes || binary.LittleEndian.Uint32(packet) != conv {
			return false
		}
		command := packet[4]
		length := binary.LittleEndian.Uint32(packet[20:])
		if command < 81 || command > 84 || uint64(length) > uint64(len(packet)-kcpHeaderBytes) {
			return false
		}
		if command != 81 && length != 0 {
			return false
		}
		packet = packet[kcpHeaderBytes+int(length):]
	}
	return true
}

func walkKCP(packet []byte, visit func(kcpHeader)) {
	for len(packet) > 0 {
		h := kcpHeader{packet[4], binary.LittleEndian.Uint32(packet[8:]), binary.LittleEndian.Uint32(packet[12:]), binary.LittleEndian.Uint32(packet[16:]), binary.LittleEndian.Uint32(packet[20:])}
		visit(h)
		packet = packet[kcpHeaderBytes+int(h.length):]
	}
}

func (t *kcpTrace) outgoing(packet []byte, at time.Time) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !validKCPPacket(packet, t.conv) {
		t.stats.MalformedPackets++
		return 0
	}
	t.writeID++
	writeID := t.writeID
	t.stats.OutboundPackets++
	walkKCP(packet, func(h kcpHeader) {
		t.stats.OutboundHeaderBytes += kcpHeaderBytes
		if h.command == 82 {
			t.stats.OutboundACKSegments++
		}
		if h.command != 81 {
			return
		}
		t.stats.OutboundPushSegments++
		t.stats.OutboundPushPayloadBytes += uint64(h.length)
		slot := &t.slots[int(h.sequence)%len(t.slots)]
		if slot.valid && slot.sequence == h.sequence {
			t.stats.RepeatedPushSegments++
		} else {
			if slot.valid {
				t.stats.SlotEvictions++
			}
			*slot = tracedSegment{valid: true, sequence: h.sequence}
			if !t.haveSN || int32(h.sequence-t.highestSN) > 0 {
				t.stats.FirstObservedPushSegments++
				t.highestSN, t.haveSN = h.sequence, true
			} else {
				t.stats.UnclassifiedPushSegments++
				slot.historyIncomplete = true
			}
		}
		attempt := &slot.attempts[slot.next]
		if attempt.writeID != 0 {
			t.stats.AttemptEvictions++
			slot.historyIncomplete = true
		}
		*attempt = tracedAttempt{timestamp: h.timestamp, writeID: writeID, entered: at}
		slot.next = (slot.next + 1) % len(slot.attempts)
	})
	return writeID
}

func (t *kcpTrace) returned(packet []byte, writeID uint64, start, at time.Time, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stats.AdapterCall.observe(at.Sub(start))
	if err != nil {
		t.stats.AdapterErrors++
	}
	if writeID == 0 {
		return
	}
	walkKCP(packet, func(h kcpHeader) {
		if h.command != 81 {
			return
		}
		slot := &t.slots[int(h.sequence)%len(t.slots)]
		if !slot.valid || slot.sequence != h.sequence {
			return
		}
		for i := range slot.attempts {
			a := &slot.attempts[i]
			if a.writeID != writeID {
				continue
			}
			a.returned = at
			if !a.acknowledged.IsZero() {
				if a.acknowledged.Before(at) {
					t.stats.ACKBeforeAdapterReturn++
				} else {
					t.stats.ReturnToACK.observe(a.acknowledged.Sub(at))
				}
			}
		}
	})
}

func (t *kcpTrace) incoming(packet []byte, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !validKCPPacket(packet, t.conv) {
		t.stats.MalformedPackets++
		return
	}
	t.stats.InboundPackets++
	walkKCP(packet, func(h kcpHeader) {
		if !t.haveUNA || int32(h.una-t.highestUNA) > 0 {
			if t.haveUNA {
				t.stats.IncomingUNAAdvances++
			}
			t.highestUNA, t.haveUNA = h.una, true
		}
		if h.command == 81 {
			t.stats.InboundPushSegments++
		}
		if h.command != 82 {
			return
		}
		t.stats.InboundACKSegments++
		slot := &t.slots[int(h.sequence)%len(t.slots)]
		if !slot.valid || slot.sequence != h.sequence {
			t.stats.UnmatchedACKs++
			return
		}
		// An evicted attempt may share the echoed millisecond timestamp.
		// Uniqueness among retained entries cannot prove a causal match.
		if slot.historyIncomplete {
			t.stats.IncompleteHistoryACKs++
			return
		}
		var match *tracedAttempt
		for i := range slot.attempts {
			a := &slot.attempts[i]
			if a.writeID == 0 || a.timestamp != h.timestamp {
				continue
			}
			if match != nil {
				t.stats.AmbiguousACKs++
				return
			}
			match = a
		}
		if match == nil {
			t.stats.UnmatchedACKs++
			return
		}
		if !match.acknowledged.IsZero() {
			t.stats.DuplicateACKs++
			return
		}
		match.acknowledged = at
		t.stats.MatchedACKs++
		t.stats.EntryToACK.observe(at.Sub(match.entered))
		if !match.returned.IsZero() {
			if at.Before(match.returned) {
				t.stats.ACKBeforeAdapterReturn++
			} else {
				t.stats.ReturnToACK.observe(at.Sub(match.returned))
			}
		}
	})
}

func (t *kcpTrace) applicationWrite(elapsed time.Duration) {
	t.mu.Lock()
	t.stats.ApplicationWrite.observe(elapsed)
	t.mu.Unlock()
}

func (t *kcpTrace) snapshot(flow int) kcpCorrelationStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.stats
	s.Flow = flow
	return s
}
