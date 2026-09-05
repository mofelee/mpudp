package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

func tracePacket(command byte, sequence, timestamp uint32, body []byte) []byte {
	b := make([]byte, kcpHeaderBytes+len(body))
	binary.LittleEndian.PutUint32(b, 42)
	b[4] = command
	binary.LittleEndian.PutUint32(b[8:], timestamp)
	binary.LittleEndian.PutUint32(b[12:], sequence)
	binary.LittleEndian.PutUint32(b[20:], uint32(len(body)))
	copy(b[kcpHeaderBytes:], body)
	return b
}

func TestKCPTraceMatchesACKToTimestampedAttempt(t *testing.T) {
	trace := newKCPTrace(true)
	start := time.Now()
	first := tracePacket(81, 7, 100, []byte("synthetic-private-body-marker"))
	id := trace.outgoing(first, start)
	trace.returned(first, id, start, start.Add(2*time.Millisecond), nil)
	second := tracePacket(81, 7, 200, []byte("synthetic-private-body-marker"))
	id = trace.outgoing(second, start.Add(100*time.Millisecond))
	trace.returned(second, id, start.Add(100*time.Millisecond), start.Add(103*time.Millisecond), nil)
	trace.incoming(tracePacket(82, 7, 100, nil), start.Add(120*time.Millisecond))
	trace.incoming(tracePacket(82, 7, 200, nil), start.Add(125*time.Millisecond))
	trace.incoming(tracePacket(82, 7, 200, nil), start.Add(126*time.Millisecond))
	s := trace.snapshot(0)
	if s.FirstObservedPushSegments != 1 || s.RepeatedPushSegments != 1 || s.MatchedACKs != 2 || s.DuplicateACKs != 1 {
		t.Fatalf("incorrect association: %+v", s)
	}
	if s.EntryToACK.Count != 2 || s.EntryToACK.SumNS != uint64(145*time.Millisecond) || s.ReturnToACK.SumNS != uint64(140*time.Millisecond) {
		t.Fatalf("incorrect timing: %+v", s)
	}
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(b, []byte("synthetic-private-body-marker")) || bytes.Contains(b, []byte("timestamp")) || bytes.Contains(b, []byte("sequence")) {
		t.Fatal("snapshot exposed packet content or correlating identifiers")
	}
}

func TestKCPTraceACKBeforeReturnIsNotSocketRTT(t *testing.T) {
	trace := newKCPTrace(true)
	start := time.Now()
	b := tracePacket(81, 1, 12, nil)
	id := trace.outgoing(b, start)
	trace.incoming(tracePacket(82, 1, 12, nil), start.Add(time.Millisecond))
	trace.returned(b, id, start, start.Add(2*time.Millisecond), errors.New("partial send"))
	s := trace.snapshot(0)
	if s.ACKBeforeAdapterReturn != 1 || s.ReturnToACK.Count != 0 || s.EntryToACK.Count != 1 || s.AdapterErrors != 1 {
		t.Fatalf("overlapping socket attempts were misreported: %+v", s)
	}
}

func TestKCPTraceAmbiguityAndEvictionRemainExplicit(t *testing.T) {
	trace := newKCPTrace(true)
	start := time.Now()
	b := tracePacket(81, 1, 12, nil)
	trace.outgoing(b, start)
	trace.outgoing(b, start.Add(time.Millisecond))
	trace.incoming(tracePacket(82, 1, 12, nil), start.Add(2*time.Millisecond))
	for i := 0; i < kcpTraceAttempts; i++ {
		trace.outgoing(tracePacket(81, 1, uint32(20+i), nil), start)
	}
	trace.incoming(tracePacket(82, 1, 12, nil), start)
	trace.outgoing(tracePacket(81, 1+kcpTraceSlots, 40, nil), start)
	trace.incoming(tracePacket(82, 1, 20, nil), start)
	trace.outgoing(tracePacket(81, 1, 50, nil), start)
	s := trace.snapshot(0)
	if s.AmbiguousACKs != 1 || s.UnmatchedACKs != 1 || s.IncompleteHistoryACKs != 1 || s.AttemptEvictions != 2 || s.SlotEvictions != 2 || s.UnclassifiedPushSegments != 1 || s.EntryToACK.Count != 0 {
		t.Fatalf("evicted/ambiguous evidence became a match: %+v", s)
	}
}

func TestKCPTraceEvictedSameTimestampCannotBecomeUniqueMatch(t *testing.T) {
	for _, timestamps := range [][]uint32{{10, 10, 10, 10, 10}, {10, 11, 12, 13, 10}} {
		trace := newKCPTrace(true)
		start := time.Now()
		// In the second case, timestamp 10 appears unique after eviction,
		// but the evicted first attempt had that same echoed timestamp.
		for _, ts := range timestamps {
			trace.outgoing(tracePacket(81, 1, ts, nil), start)
		}
		trace.incoming(tracePacket(82, 1, 10, nil), start.Add(time.Millisecond))
		s := trace.snapshot(0)
		if s.IncompleteHistoryACKs != 1 || s.MatchedACKs != 0 || s.EntryToACK.Count != 0 {
			t.Fatalf("evicted timestamp produced a false match: %+v", s)
		}
	}
}

func TestKCPTraceRecreatedOldSequenceCannotClaimCompleteHistory(t *testing.T) {
	trace := newKCPTrace(true)
	start := time.Now()
	trace.outgoing(tracePacket(81, 1, 10, nil), start)
	trace.outgoing(tracePacket(81, 1+kcpTraceSlots, 11, nil), start)
	trace.outgoing(tracePacket(81, 1, 10, nil), start.Add(time.Millisecond))
	trace.incoming(tracePacket(82, 1, 10, nil), start.Add(2*time.Millisecond))
	s := trace.snapshot(0)
	if s.IncompleteHistoryACKs != 1 || s.UnclassifiedPushSegments != 1 || s.MatchedACKs != 0 {
		t.Fatalf("recreated sequence hid lost history: %+v", s)
	}
}

func TestKCPTraceConcurrentACKReturnAndSnapshot(t *testing.T) {
	trace := newKCPTrace(true)
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for attempt := 0; attempt < 20; attempt++ {
				start := time.Now()
				packet := tracePacket(81, uint32(worker), uint32(attempt), nil)
				id := trace.outgoing(packet, start)
				var pair sync.WaitGroup
				pair.Add(2)
				go func() { defer pair.Done(); trace.returned(packet, id, start, start.Add(time.Millisecond), nil) }()
				go func() {
					defer pair.Done()
					trace.incoming(tracePacket(82, uint32(worker), uint32(attempt), nil), start.Add(2*time.Millisecond))
				}()
				_ = trace.snapshot(worker)
				pair.Wait()
			}
		}(worker)
	}
	workers.Wait()
	s := trace.snapshot(0)
	if s.OutboundPackets != 160 || s.InboundACKSegments != 160 || s.AdapterCall.Count != 160 {
		t.Fatalf("concurrent observations were lost: %+v", s)
	}
	accounted := s.MatchedACKs + s.UnmatchedACKs + s.AmbiguousACKs + s.IncompleteHistoryACKs + s.DuplicateACKs
	if accounted != s.InboundACKSegments || s.EntryToACK.Count != s.MatchedACKs {
		t.Fatalf("concurrent ACK accounting inconsistent: %+v", s)
	}
}

func TestKCPTraceRejectsEntireMalformedPacket(t *testing.T) {
	valid := tracePacket(81, 1, 2, []byte("body"))
	for _, packet := range [][]byte{nil, valid[:23], append(append([]byte(nil), valid...), 0), tracePacket(80, 1, 2, nil), tracePacket(82, 1, 2, []byte("invalid ACK body"))} {
		trace := newKCPTrace(true)
		if id := trace.outgoing(packet, time.Now()); id != 0 {
			t.Fatal("malformed packet got a correlation ID")
		}
		if s := trace.snapshot(0); s.MalformedPackets != 1 || s.OutboundPushSegments != 0 {
			t.Fatalf("partially counted malformed packet: %+v", s)
		}
	}
	trace := newKCPTrace(true)
	packet := append(valid, tracePacket(82, 3, 4, nil)...)
	trace.outgoing(packet, time.Now())
	if s := trace.snapshot(0); s.OutboundPushSegments != 1 || s.OutboundACKSegments != 1 || s.OutboundHeaderBytes != 48 {
		t.Fatalf("concatenated KCP segments lost: %+v", s)
	}
}

func TestNativeKCPTraceDoesNotClaimPacketCorrelation(t *testing.T) {
	trace := newKCPTrace(false)
	trace.applicationWrite(5 * time.Millisecond)
	s := trace.snapshot(0)
	if s.PacketCorrelationAvailable || s.RetransmitReasonAvailable || len(trace.slots) != 0 || s.ApplicationWrite.Count != 1 {
		t.Fatalf("native timing was presented as packet correlation: %+v", s)
	}
}

func TestKCPTraceBoundedAcrossSequenceWrap(t *testing.T) {
	trace := newKCPTrace(true)
	for _, sn := range []uint32{0xfffffffe, 0xffffffff, 0, 1} {
		trace.outgoing(tracePacket(81, sn, 1, nil), time.Now())
	}
	if s := trace.snapshot(0); s.FirstObservedPushSegments != 4 || s.UnclassifiedPushSegments != 0 || len(trace.slots) != kcpTraceSlots {
		t.Fatalf("sequence wrap failed: %+v", s)
	}
}
