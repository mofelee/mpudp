package session

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/fec"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wire"
)

func TestListenerAuthenticatesBeforeCreatingSessionOrEndpoint(t *testing.T) {
	clock := newFakeClock()
	listener, err := NewListener(ListenerConfig{Session: testConfig(clock, 1000), MaxSessions: 2})
	if err != nil {
		t.Fatal(err)
	}
	path := newFakePath("listener/198.51.100.1:4000", "198.51.100.1:4000")
	valid := helloPacket(t, testSessionID, 3, 2, 1200, []byte(testPSK))

	badTag := append([]byte(nil), valid...)
	badTag[len(badTag)-1] ^= 1
	invalid := [][]byte{
		badTag,
		helloPacket(t, testSessionID, 3, 2, 1200, []byte("wrong PSK")),
		[]byte("not a wire packet"),
	}
	for index, packet := range invalid {
		if _, _, err := listener.HandlePacket(context.Background(), received(path, packet)); err == nil {
			t.Fatalf("invalid packet %d was accepted", index)
		}
		if stats := listener.Stats(); stats != (ListenerStats{}) {
			t.Fatalf("invalid packet %d retained state: %+v", index, stats)
		}
	}
	if path.PacketCount() != 0 {
		t.Fatalf("invalid packets triggered %d responses", path.PacketCount())
	}

	mismatchedFEC := helloPacket(t, testSessionID, 4, 1, 1200, []byte(testPSK))
	if _, _, err := listener.HandlePacket(context.Background(), received(path, mismatchedFEC)); !errors.Is(err, ErrHandshakeIncompatible) {
		t.Fatalf("FEC mismatch error = %v", err)
	}
	if stats := listener.Stats(); stats != (ListenerStats{}) {
		t.Fatalf("FEC mismatch retained partial Session: %+v", stats)
	}

	invalidCapability := rawAuthenticatedHandshake(wire.TypeHello, testSessionID, 3, 2, 0xffff, []byte(testPSK))
	if _, _, err := listener.HandlePacket(context.Background(), received(path, invalidCapability)); !errors.Is(err, wire.ErrInvalidCapability) {
		t.Fatalf("0xffff capability error = %v", err)
	}
	if stats := listener.Stats(); stats != (ListenerStats{}) {
		t.Fatalf("0xffff capability retained partial Session: %+v", stats)
	}
	if path.PacketCount() != 0 {
		t.Fatal("invalid authenticated handshake triggered a response")
	}
}

func TestListenerCreatesOnceNegotiatesAndBoundsEndpointsAndSessions(t *testing.T) {
	clock := newFakeClock()
	listener, err := NewListener(ListenerConfig{Session: testConfig(clock, 1000), MaxSessions: 1})
	if err != nil {
		t.Fatal(err)
	}
	first := newFakePath("listener/198.51.100.1:4000", "198.51.100.1:4000")
	second := newFakePath("listener/198.51.100.2:4000", "198.51.100.2:4000")
	third := newFakePath("listener/198.51.100.3:4000", "198.51.100.3:4000")
	hello := helloPacket(t, testSessionID, 3, 2, 1200, []byte(testPSK))

	session, result, err := listener.HandlePacket(context.Background(), received(first, hello))
	if err != nil {
		t.Fatal(err)
	}
	if session == nil || !result.Created || !result.Established || !result.EndpointAdded || result.Response == nil {
		t.Fatalf("first HELLO result session=%v result=%+v", session, result)
	}
	if first.PacketCount() != 1 {
		t.Fatalf("first Endpoint ACK count=%d", first.PacketCount())
	}
	ack := decodeSent(t, first.Packets()[0], 1000)
	if ack.Header.Type != wire.TypeHelloAck || ack.Handshake.MaxUDPPayload != 1000 {
		t.Fatalf("listener response = %+v", ack)
	}
	snapshot := session.Snapshot()
	if snapshot.SendMaxUDPPayload != 1000 || snapshot.ReceiveMaxUDPPayload != 1000 || snapshot.Endpoints != 1 {
		t.Fatalf("listener negotiation = %+v", snapshot)
	}

	clock.Advance(time.Second)
	same, duplicate, err := listener.HandlePacket(context.Background(), received(first, hello))
	if err != nil {
		t.Fatal(err)
	}
	if same != session || duplicate.Created || !duplicate.EndpointRefreshed || first.PacketCount() != 2 {
		t.Fatalf("duplicate HELLO created/failed refresh: result=%+v sends=%d", duplicate, first.PacketCount())
	}
	if got := listener.Stats().Sessions; got != 1 {
		t.Fatalf("duplicate HELLO Session count=%d", got)
	}

	same, added, err := listener.HandlePacket(context.Background(), received(second, hello))
	if err != nil || same != session || !added.EndpointAdded {
		t.Fatalf("second Endpoint result=%+v err=%v", added, err)
	}
	if session.Snapshot().Endpoints != 2 || second.PacketCount() != 1 {
		t.Fatalf("second Endpoint was not learned/ACKed: %+v sends=%d", session.Snapshot(), second.PacketCount())
	}
	if _, _, err := listener.HandlePacket(context.Background(), received(third, hello)); !errors.Is(err, ErrEndpointLimit) {
		t.Fatalf("third Endpoint error = %v", err)
	}
	if third.PacketCount() != 0 || session.Snapshot().Endpoints != 2 {
		t.Fatal("Endpoint-limit rejection changed state or responded")
	}

	other := helloPacket(t, otherSessionID, 3, 2, 1200, []byte(testPSK))
	if _, _, err := listener.HandlePacket(context.Background(), received(third, other)); !errors.Is(err, ErrSessionLimit) {
		t.Fatalf("second Session error = %v", err)
	}
	if third.PacketCount() != 0 || listener.Stats().Sessions != 1 {
		t.Fatal("Session-limit rejection changed state or responded")
	}

	clock.Advance(5 * time.Second)
	advanced := listener.Advance(context.Background())
	if len(advanced) != 1 || advanced[0].Result.ExpiredEndpoints != 2 {
		t.Fatalf("TTL Advance = %+v", advanced)
	}
	if listener.Stats().Sessions != 1 || session.Snapshot().Endpoints != 0 || session.ID() != testSessionID {
		t.Fatalf("Endpoint TTL destroyed or changed Session: stats=%+v snapshot=%+v", listener.Stats(), session.Snapshot())
	}
	if _, refreshed, err := listener.HandlePacket(context.Background(), received(third, hello)); err != nil || !refreshed.EndpointAdded {
		t.Fatalf("Endpoint was not reusable after TTL: result=%+v err=%v", refreshed, err)
	}
}

func TestListenerDATARecoveryBudgetAndReverseWrite(t *testing.T) {
	clock := newFakeClock()
	listenerConfig := testConfig(clock, 1200)
	listener, err := NewListener(ListenerConfig{Session: listenerConfig, MaxSessions: 2})
	if err != nil {
		t.Fatal(err)
	}
	first := newFakePath("listener/198.51.100.1:4000", "198.51.100.1:4000")
	second := newFakePath("listener/198.51.100.2:4000", "198.51.100.2:4000")
	hello := helloPacket(t, testSessionID, 3, 2, 1000, []byte(testPSK))
	session, _, err := listener.HandlePacket(context.Background(), received(first, hello))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := listener.HandlePacket(context.Background(), received(second, hello)); err != nil {
		t.Fatal(err)
	}

	encoder, err := fec.NewEncoder(fec.Params{DataShards: 3, ParityShards: 2}, fec.Budget{
		MaxUDPPayload: 1000, DataShardWireOverhead: wire.DataShardOverhead, MaxDatagramSize: 64 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("authenticated shards recover without ordering")
	block, err := encoder.Encode(payload)
	if err != nil {
		t.Fatal(err)
	}
	packets := make([][]byte, len(block.Shards))
	for index, shard := range block.Shards {
		message, makeErr := wire.NewDataShard(testSessionID, block.PacketID, 3, 2, uint8(index), uint32(len(payload)), shard)
		if makeErr != nil {
			t.Fatal(makeErr)
		}
		packets[index] = encodeMessage(t, message, []byte(testPSK), 1000)
	}
	order := []int{4, 0, 2}
	for position, index := range order {
		path := first
		if position%2 == 1 {
			path = second
		}
		_, result, handleErr := listener.HandlePacket(context.Background(), received(path, packets[index]))
		if handleErr != nil {
			t.Fatalf("DATA index %d: %v", index, handleErr)
		}
		if position < 2 && result.Datagram != nil {
			t.Fatalf("DATA completed before k shards at position %d", position)
		}
		if position == 2 && !bytes.Equal(result.Datagram, payload) {
			t.Fatalf("recovered Datagram = %q, want %q", result.Datagram, payload)
		}
	}
	_, duplicate, err := listener.HandlePacket(context.Background(), received(first, packets[0]))
	if err != nil || duplicate.Datagram != nil {
		t.Fatalf("late duplicate result=%+v err=%v", duplicate, err)
	}

	oversizeShard := bytes.Repeat([]byte{1}, 930)
	overBudget, err := wire.NewDataShard(testSessionID, 99, 3, 2, 0, 2790, oversizeShard)
	if err != nil {
		t.Fatal(err)
	}
	beforeActivity := endpointActivity(t, session, first.PathID())
	clock.Advance(time.Millisecond)
	if _, _, err := listener.HandlePacket(context.Background(), received(first, encodeMessage(t, overBudget, []byte(testPSK), 1200))); !errors.Is(err, ErrPacketOverBudget) {
		t.Fatalf("negotiated receive budget error = %v", err)
	}
	if endpointActivity(t, session, first.PathID()) != beforeActivity {
		t.Fatal("over-budget authenticated DATA refreshed Endpoint")
	}

	firstBefore, secondBefore := first.PacketCount(), second.PacketCount()
	write, err := session.WritePacket(context.Background(), []byte("reverse direction"))
	if err != nil {
		t.Fatal(err)
	}
	if write.Send.Attempted != 5 || write.Send.Succeeded != 5 {
		t.Fatalf("reverse DATA send = %+v", write.Send)
	}
	if first.PacketCount() == firstBefore || second.PacketCount() == secondBefore {
		t.Fatalf("reverse scheduler did not use both Endpoints: deltas=%d/%d", first.PacketCount()-firstBefore, second.PacketCount()-secondBefore)
	}
	for _, path := range []*fakePath{first, second} {
		for _, packet := range path.Packets() {
			if len(packet) > 1000 {
				t.Fatalf("captured control/DATA packet length %d exceeds negotiated budget", len(packet))
			}
		}
	}
}

func TestListenerPathFailureIsolatesEndpointsSharingListenSocket(t *testing.T) {
	clock := newFakeClock()
	listener, err := NewListener(ListenerConfig{Session: testConfig(clock, 1200), MaxSessions: 2})
	if err != nil {
		t.Fatal(err)
	}
	first := newFakePath("listener", "198.51.100.1:4000")
	second := newFakePath("listener", "198.51.100.2:4000")
	hello := helloPacket(t, testSessionID, 3, 2, 1200, []byte(testPSK))
	session, _, err := listener.HandlePacket(context.Background(), received(first, hello))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := listener.HandlePacket(context.Background(), received(second, hello)); err != nil {
		t.Fatal(err)
	}

	pathFailure := errors.Join(transport.ErrPathMTUExceeded, errors.New("injected endpoint PMTU failure"))
	first.SetError(pathFailure)
	ping, err := wire.NewPing(testSessionID, 99, 1234)
	if err != nil {
		t.Fatal(err)
	}
	_, handled, err := listener.HandlePacket(context.Background(), received(first, encodeMessage(t, ping, []byte(testPSK), 1200)))
	if err != nil {
		t.Fatal(err)
	}
	if handled.Response == nil || !errors.Is(handled.Response.Err, transport.ErrPathMTUExceeded) {
		t.Fatalf("PONG response = %+v, want endpoint PMTU failure", handled.Response)
	}
	assertEndpointAvailability(t, session, first.RemoteAddr().String(), false)
	assertEndpointAvailability(t, session, second.RemoteAddr().String(), true)
	paths := session.SendPaths()
	if len(paths) != 1 || paths[0].(ReplyPath).RemoteAddr().String() != second.RemoteAddr().String() {
		t.Fatalf("control failure affected sibling Endpoint: %+v", session.Endpoints())
	}

	first.SetError(nil)
	if _, _, err := listener.HandlePacket(context.Background(), received(first, hello)); err != nil {
		t.Fatal(err)
	}
	first.SetError(pathFailure)
	write, err := session.WritePacket(context.Background(), []byte("isolate one reverse path"))
	if !errors.Is(err, transport.ErrPartialSend) || write.Send.Succeeded == 0 {
		t.Fatalf("reverse DATA send = %+v, %v; want partial success", write.Send, err)
	}
	assertEndpointAvailability(t, session, first.RemoteAddr().String(), false)
	assertEndpointAvailability(t, session, second.RemoteAddr().String(), true)
}

func TestAuthenticatedCloseRemovesListenerSessionWithoutResponseOrLearning(t *testing.T) {
	clock := newFakeClock()
	listener, err := NewListener(ListenerConfig{Session: testConfig(clock, 1200), MaxSessions: 2})
	if err != nil {
		t.Fatal(err)
	}
	known := newFakePath("listener/known", "198.51.100.1:4000")
	unknown := newFakePath("listener/new", "198.51.100.2:4000")
	session, _, err := listener.HandlePacket(context.Background(), received(known, helloPacket(t, testSessionID, 3, 2, 1200, []byte(testPSK))))
	if err != nil {
		t.Fatal(err)
	}
	closeMessage, err := wire.NewClose(testSessionID)
	if err != nil {
		t.Fatal(err)
	}
	before := unknown.PacketCount()
	got, result, err := listener.HandlePacket(context.Background(), received(unknown, encodeMessage(t, closeMessage, []byte(testPSK), 1200)))
	if err != nil || got != session {
		t.Fatalf("authenticated CLOSE result=%+v err=%v", result, err)
	}
	if unknown.PacketCount() != before || result.EndpointAdded || result.EndpointRefreshed || result.Response != nil {
		t.Fatalf("CLOSE learned/responded: %+v", result)
	}
	if listener.Stats().Sessions != 0 || session.Snapshot().State != StateClosed {
		t.Fatalf("CLOSE did not remove terminal Session: listener=%+v session=%+v", listener.Stats(), session.Snapshot())
	}
}

func TestListenerConcurrentCloseReturnsStableError(t *testing.T) {
	clock := newFakeClock()
	listener, err := NewListener(ListenerConfig{Session: testConfig(clock, 1200), MaxSessions: 2})
	if err != nil {
		t.Fatal(err)
	}
	path := newFakePath("listener/path", "198.51.100.1:4000")
	if _, _, err := listener.HandlePacket(context.Background(), received(path, helloPacket(t, testSessionID, 3, 2, 1200, []byte(testPSK)))); err != nil {
		t.Fatal(err)
	}
	want := errors.New("listener close path failed")
	path.SetError(want)
	const callers = 6
	results := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			results <- listener.Close(context.Background())
		}()
	}
	group.Wait()
	close(results)
	for err := range results {
		if !errors.Is(err, want) {
			t.Fatalf("Listener.Close error = %v, want stable send error", err)
		}
	}
	if listener.Stats().Sessions != 0 {
		t.Fatal("Listener.Close retained Sessions")
	}
}

func rawAuthenticatedHandshake(packetType wire.PacketType, id wire.SessionID, data, parity uint8, capability uint16, key []byte) []byte {
	body := []byte{data, parity, byte(capability >> 8), byte(capability)}
	packet := make([]byte, wire.PrefixSize+len(body)+wire.AuthenticationTagSize)
	copy(packet[0:4], wire.Magic)
	packet[4] = wire.Version
	packet[5] = byte(packetType)
	binary.BigEndian.PutUint16(packet[6:8], uint16(len(body)))
	copy(packet[8:24], id[:])
	copy(packet[24:], body)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(packet[:wire.PrefixSize+len(body)])
	copy(packet[wire.PrefixSize+len(body):], mac.Sum(nil))
	return packet
}

func endpointActivity(t *testing.T, session *Session, pathID string) time.Time {
	t.Helper()
	for _, endpoint := range session.Endpoints() {
		if endpoint.PathID == pathID {
			return endpoint.LastActivity
		}
	}
	t.Fatalf("Endpoint %q not found", pathID)
	return time.Time{}
}

func assertEndpointAvailability(t *testing.T, session *Session, remote string, want bool) {
	t.Helper()
	for _, endpoint := range session.Endpoints() {
		if endpoint.RemoteAddr == remote {
			if endpoint.Available != want {
				t.Fatalf("Endpoint %q availability = %t, want %t", remote, endpoint.Available, want)
			}
			return
		}
	}
	t.Fatalf("Endpoint %q not found", remote)
}
