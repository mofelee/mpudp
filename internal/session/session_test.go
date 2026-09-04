package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mofelee/mpudp/config"
	"github.com/mofelee/mpudp/internal/fec"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wire"
)

func TestCrossLayerUDPPayloadLimitsMatch(t *testing.T) {
	if config.MinMaxUDPPayload != wire.MinUDPPayload {
		t.Fatalf("config/wire minimum mismatch: %d != %d", config.MinMaxUDPPayload, wire.MinUDPPayload)
	}
	if config.MaxMaxUDPPayload != wire.MaxUDPPayload || transport.MaxUDPPayload != wire.MaxUDPPayload {
		t.Fatalf("maximum mismatch: config=%d wire=%d transport=%d", config.MaxMaxUDPPayload, wire.MaxUDPPayload, transport.MaxUDPPayload)
	}
}

func TestInitiatorFirstACKEstablishesAndLaterACKAddsEndpoint(t *testing.T) {
	clock := newFakeClock()
	first := newFakePath("carrier-a", "198.51.100.1:9000")
	second := newFakePath("carrier-b", "198.51.100.2:9000")
	sessionConfig := testConfig(clock, 1200)
	session, err := NewInitiator(testSessionID, sessionConfig, []Path{first, second})
	if err != nil {
		t.Fatal(err)
	}
	attempts, err := session.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 || first.PacketCount() != 1 || second.PacketCount() != 1 {
		t.Fatalf("initial HELLO attempts = %d, path counts = %d/%d", len(attempts), first.PacketCount(), second.PacketCount())
	}
	for _, path := range []*fakePath{first, second} {
		message := decodeSent(t, path.Packets()[0], 1200)
		if message.Header.Type != wire.TypeHello || message.Handshake.MaxUDPPayload != 1200 {
			t.Fatalf("initial packet = %+v, want HELLO capability 1200", message)
		}
	}

	bad := ackPacket(t, testSessionID, 3, 2, 1000)
	bad[len(bad)-1] ^= 1
	before := session.Snapshot()
	if _, err := session.HandlePacket(context.Background(), received(second, bad)); err == nil {
		t.Fatal("tampered ACK was accepted")
	}
	after := session.Snapshot()
	if after.State != before.State || after.Endpoints != before.Endpoints || after.AcknowledgedCarriers != before.AcknowledgedCarriers {
		t.Fatalf("tampered ACK changed state: before=%+v after=%+v", before, after)
	}

	result, err := session.HandlePacket(context.Background(), received(second, ackPacket(t, testSessionID, 3, 2, 1000)))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Established || !result.EndpointAdded {
		t.Fatalf("first ACK result = %+v", result)
	}
	snapshot := session.Snapshot()
	if snapshot.State != StateEstablished || snapshot.SendMaxUDPPayload != 1000 || snapshot.ReceiveMaxUDPPayload != 1000 || snapshot.PeerMaxUDPPayload != 1000 {
		t.Fatalf("negotiated snapshot = %+v", snapshot)
	}
	if snapshot.Endpoints != 1 || snapshot.AcknowledgedCarriers != 1 {
		t.Fatalf("first ACK path accounting = %+v", snapshot)
	}

	result, err = session.HandlePacket(context.Background(), received(first, ackPacket(t, testSessionID, 3, 2, 1000)))
	if err != nil {
		t.Fatal(err)
	}
	if result.Established || !result.EndpointAdded {
		t.Fatalf("later ACK result = %+v", result)
	}
	snapshot = session.Snapshot()
	if snapshot.State != StateEstablished || snapshot.Endpoints != 2 || snapshot.AcknowledgedCarriers != 2 {
		t.Fatalf("later ACK did not extend same Session: %+v", snapshot)
	}

	clock.Advance(time.Second)
	result, err = session.HandlePacket(context.Background(), received(first, ackPacket(t, testSessionID, 3, 2, 1000)))
	if err != nil || !result.EndpointRefreshed || result.EndpointAdded {
		t.Fatalf("duplicate ACK result=%+v err=%v", result, err)
	}
	activity := session.Endpoints()[0].LastActivity
	clock.Advance(time.Second)
	_, err = session.HandlePacket(context.Background(), received(first, ackPacket(t, testSessionID, 3, 2, 1100)))
	requireErrorIs(t, err, ErrHandshakeIncompatible)
	if session.Endpoints()[0].LastActivity != activity {
		t.Fatal("changed authenticated capability refreshed Endpoint")
	}
}

func TestHandshakeRetriesAreBoundedIndependentAndCancelable(t *testing.T) {
	clock := newFakeClock()
	paths := []*fakePath{
		newFakePath("carrier-a", "198.51.100.1:9000"),
		newFakePath("carrier-b", "198.51.100.2:9000"),
	}
	configured := []Path{paths[0], paths[1]}
	sessionConfig := testConfig(clock, 1200)
	sessionConfig.MaxHandshakeAttempts = 2
	session, err := NewInitiator(testSessionID, sessionConfig, configured)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	initialDelay := session.NextDeadline().Sub(clock.Now())
	if initialDelay < sessionConfig.HandshakeRetryInterval || initialDelay > sessionConfig.HandshakeRetryInterval+sessionConfig.HandshakeRetryJitterLimit {
		t.Fatalf("initial retry delay %s outside bounded jitter interval", initialDelay)
	}
	for {
		deadline := session.NextDeadline()
		if deadline.IsZero() {
			t.Fatal("handshake lost its retry deadline")
		}
		clock.Set(deadline)
		result, advanceErr := session.Advance(context.Background())
		if errors.Is(advanceErr, ErrHandshakeFailed) {
			if !result.HandshakeFailed {
				t.Fatal("timeout error omitted transition flag")
			}
			break
		}
		if advanceErr != nil {
			t.Fatal(advanceErr)
		}
	}
	for _, path := range paths {
		if path.PacketCount() != 2 {
			t.Fatalf("%s sent %d HELLOs, want 2", path.PathID(), path.PacketCount())
		}
	}
	if snapshot := session.Snapshot(); snapshot.State != StateHandshakeFailed || snapshot.HasRetryDeadline {
		t.Fatalf("failed handshake retained retry state: %+v", snapshot)
	}

	cancelClock := newFakeClock()
	cancelPath := newFakePath("carrier", "198.51.100.3:9000")
	canceled, err := NewInitiator(otherSessionID, sessionConfigWithClock(sessionConfig, cancelClock), []Path{cancelPath})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := canceled.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := canceled.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	cancelClock.Advance(time.Hour)
	if _, err := canceled.Advance(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Advance after Close error = %v", err)
	}
	if cancelPath.PacketCount() != 1 {
		t.Fatalf("Close before establishment sent unexpected packets: %d", cancelPath.PacketCount())
	}
}

func TestEstablishedSessionContinuesRegistrationRetriesOnlyOnUnackedCarrier(t *testing.T) {
	clock := newFakeClock()
	first := newFakePath("carrier-a", "198.51.100.1:9000")
	second := newFakePath("carrier-b", "198.51.100.2:9000")
	sessionConfig := testConfig(clock, 1200)
	sessionConfig.MaxHandshakeAttempts = 2
	session, err := NewInitiator(testSessionID, sessionConfig, []Path{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := session.HandlePacket(context.Background(), received(first, ackPacket(t, testSessionID, 3, 2, 1200))); err != nil {
		t.Fatal(err)
	}
	for !session.NextDeadline().IsZero() && session.Snapshot().HasRetryDeadline {
		clock.Set(session.NextDeadline())
		if _, err := session.Advance(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if first.PacketCount() != 1 {
		t.Fatalf("acknowledged Carrier retried HELLO %d times", first.PacketCount())
	}
	if second.PacketCount() != 2 {
		t.Fatalf("unacknowledged Carrier attempts = %d, want 2", second.PacketCount())
	}
	if session.Snapshot().State != StateEstablished {
		t.Fatal("registration exhaustion closed an established Session")
	}
	if paths := session.SendPaths(); len(paths) != 1 || paths[0].PathID() != first.PathID() {
		t.Fatalf("exhausted unacknowledged Carrier remained healthy: %v", pathIDs(paths))
	}
}

func TestPerCarrierKeepalivePONGMatchingAndRTT(t *testing.T) {
	clock := newFakeClock()
	first := newFakePath("carrier-a", "198.51.100.1:9000")
	second := newFakePath("carrier-b", "198.51.100.2:9000")
	session := establishInitiator(t, clock, 1200, first, second)
	if _, err := session.HandlePacket(context.Background(), received(second, ackPacket(t, testSessionID, 3, 2, 1200))); err != nil {
		t.Fatal(err)
	}
	initialFirst, initialSecond := first.PacketCount(), second.PacketCount()
	clock.Advance(time.Second)
	advance, err := session.Advance(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pingCount := 0
	for _, attempt := range advance.Sends {
		if attempt.Type == wire.TypePing {
			pingCount++
		}
	}
	if pingCount != 2 || first.PacketCount() != initialFirst+1 || second.PacketCount() != initialSecond+1 {
		t.Fatalf("per-Carrier PINGs=%d packet counts=%d/%d", pingCount, first.PacketCount(), second.PacketCount())
	}
	firstPing := decodeSent(t, first.Packets()[first.PacketCount()-1], 1200)
	secondPing := decodeSent(t, second.Packets()[second.PacketCount()-1], 1200)
	if firstPing.Header.Type != wire.TypePing || secondPing.Header.Type != wire.TypePing || firstPing.Probe.Token == secondPing.Probe.Token {
		t.Fatalf("captured probes are not independent: first=%+v second=%+v", firstPing, secondPing)
	}

	wrong, err := wire.NewPong(testSessionID, firstPing.Probe.Token+1, firstPing.Probe.Timestamp)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.HandlePacket(context.Background(), received(first, encodeMessage(t, wrong, []byte(testPSK), 1200))); !errors.Is(err, ErrProbeMismatch) {
		t.Fatalf("mismatched PONG error = %v", err)
	}
	if session.Snapshot().OutstandingProbes != 2 {
		t.Fatal("mismatched PONG consumed a probe")
	}

	clock.Advance(25 * time.Millisecond)
	pong, err := wire.NewPong(testSessionID, firstPing.Probe.Token, firstPing.Probe.Timestamp)
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.HandlePacket(context.Background(), received(first, encodeMessage(t, pong, []byte(testPSK), 1200)))
	if err != nil {
		t.Fatal(err)
	}
	if !result.RTTUpdated || result.RTT != 25*time.Millisecond || session.Snapshot().OutstandingProbes != 1 {
		t.Fatalf("matched PONG result=%+v snapshot=%+v", result, session.Snapshot())
	}
	if endpoints := session.Endpoints(); !endpointHasRTT(endpoints, "carrier-a", 25*time.Millisecond) {
		t.Fatalf("RTT was not retained on carrier-a: %+v", endpoints)
	}

	incomingPing, err := wire.NewPing(testSessionID, 99, 1234)
	if err != nil {
		t.Fatal(err)
	}
	before := second.PacketCount()
	result, err = session.HandlePacket(context.Background(), received(second, encodeMessage(t, incomingPing, []byte(testPSK), 1200)))
	if err != nil {
		t.Fatal(err)
	}
	if result.Response == nil || result.Response.Type != wire.TypePong || second.PacketCount() != before+1 {
		t.Fatalf("PING response result=%+v sends=%d", result, second.PacketCount()-before)
	}
	echo := decodeSent(t, second.Packets()[second.PacketCount()-1], 1200)
	if echo.Probe != incomingPing.Probe {
		t.Fatalf("PONG did not echo probe: got=%+v want=%+v", echo.Probe, incomingPing.Probe)
	}
}

func TestWritePacketExactDerivedLimitAndPMTUPathIsolation(t *testing.T) {
	clock := newFakeClock()
	first := newFakePath("carrier-a", "198.51.100.1:9000")
	second := newFakePath("carrier-b", "198.51.100.2:9000")
	sessionConfig := testConfig(clock, 1200)
	sessionConfig.MaxDatagramSize = 64 * 1024
	session, err := NewInitiator(testSessionID, sessionConfig, []Path{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := session.HandlePacket(context.Background(), received(first, ackPacket(t, testSessionID, 3, 2, 1200))); err != nil {
		t.Fatal(err)
	}
	firstBefore, secondBefore := first.PacketCount(), second.PacketCount()
	exact := bytes.Repeat([]byte{0xa5}, 3*(1200-wire.DataShardOverhead))
	result, err := session.WritePacket(context.Background(), exact)
	if err != nil {
		t.Fatalf("exact-limit WritePacket: %v", err)
	}
	if result.Send.Attempted != 5 || result.Send.Succeeded != 5 {
		t.Fatalf("exact-limit send = %+v", result.Send)
	}
	for _, path := range []*fakePath{first, second} {
		for _, packet := range path.Packets()[map[*fakePath]int{first: firstBefore, second: secondBefore}[path]:] {
			if len(packet) > 1200 {
				t.Fatalf("DATA packet length %d exceeds negotiated budget", len(packet))
			}
			if got := decodeSent(t, packet, 1200).Header.Type; got != wire.TypeDataShard {
				t.Fatalf("WritePacket emitted type %d", got)
			}
		}
	}
	beforeOversize := first.PacketCount() + second.PacketCount()
	if _, err := session.WritePacket(context.Background(), append(exact, 1)); !errors.Is(err, fec.ErrMessageTooLarge) {
		t.Fatalf("limit+1 error = %v", err)
	}
	if got := first.PacketCount() + second.PacketCount(); got != beforeOversize {
		t.Fatalf("limit+1 called Path.Send %d times", got-beforeOversize)
	}

	first.SetError(errors.Join(transport.ErrPathMTUExceeded, fmt.Errorf("fake EMSGSIZE")))
	if _, err := session.WritePacket(context.Background(), []byte("small payload")); !errors.Is(err, transport.ErrPartialSend) {
		t.Fatalf("PMTU block error = %v, want partial send", err)
	}
	paths := session.SendPaths()
	if len(paths) != 1 || paths[0].PathID() != second.PathID() {
		t.Fatalf("PMTU-failed path remained schedulable: %+v", pathIDs(paths))
	}
	if session.Snapshot().State != StateEstablished {
		t.Fatal("one PMTU failure closed Session")
	}
	first.SetError(nil)
	if err := session.SetPathHealthy(first.PathID(), true); err != nil {
		t.Fatal(err)
	}
	if len(session.SendPaths()) != 2 {
		t.Fatal("explicit recovery did not restore PMTU path")
	}
}

func TestCloseIsBestEffortIdempotentAndStableAcrossConcurrentCallers(t *testing.T) {
	clock := newFakeClock()
	first := newFakePath("carrier-a", "198.51.100.1:9000")
	second := newFakePath("carrier-b", "198.51.100.2:9000")
	session := establishInitiator(t, clock, 1200, first, second)
	closeFailure := errors.New("close send failed")
	first.SetError(closeFailure)
	beforeFirst, beforeSecond := first.PacketCount(), second.PacketCount()
	const callers = 8
	errorsSeen := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			errorsSeen <- session.Close(context.Background())
		}()
	}
	group.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if !errors.Is(err, closeFailure) {
			t.Fatalf("concurrent Close error = %v, want stable path error", err)
		}
	}
	if first.PacketCount() != beforeFirst+1 || second.PacketCount() != beforeSecond+1 {
		t.Fatalf("best-effort Close sends = %d/%d, want one on each path", first.PacketCount()-beforeFirst, second.PacketCount()-beforeSecond)
	}
	if got := decodeSent(t, second.Packets()[second.PacketCount()-1], 1200).Header.Type; got != wire.TypeClose {
		t.Fatalf("successful close path type = %d", got)
	}
	snapshot := session.Snapshot()
	if snapshot.State != StateClosed || snapshot.Endpoints != 0 || snapshot.OutstandingProbes != 0 || !snapshot.NextDeadline.IsZero() {
		t.Fatalf("Close retained state: %+v", snapshot)
	}
}

func TestHighFrequencyWriteContextsCleanUpBeforeClose(t *testing.T) {
	clock := newFakeClock()
	path := newFakePath("carrier", "198.51.100.1:9000")
	session := establishInitiator(t, clock, 1200, path)
	for index := 0; index < 500; index++ {
		if _, err := session.WritePacket(context.Background(), []byte{byte(index)}); err != nil {
			t.Fatalf("WritePacket %d: %v", index, err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- session.Close(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked after high-frequency operation contexts")
	}
}

func TestSessionStringUsesShortFingerprint(t *testing.T) {
	clock := newFakeClock()
	path := newFakePath("carrier", "198.51.100.1:9000")
	session, err := NewInitiator(testSessionID, testConfig(clock, 1200), []Path{path})
	if err != nil {
		t.Fatal(err)
	}
	rendered := session.String()
	fullID := fmt.Sprintf("%x", testSessionID)
	if strings.Contains(rendered, fullID) {
		t.Fatalf("Session.String exposed complete SessionID: %s", rendered)
	}
	if strings.Contains(rendered, testPSK) {
		t.Fatalf("Session.String exposed PSK: %s", rendered)
	}
}

func sessionConfigWithClock(config Config, clock Clock) Config {
	config.Clock = clock
	return config
}

func endpointHasRTT(endpoints []EndpointSnapshot, pathID string, want time.Duration) bool {
	for _, endpoint := range endpoints {
		if endpoint.PathID == pathID && endpoint.HasRTT && endpoint.RTT == want {
			return true
		}
	}
	return false
}

func pathIDs(paths []Path) []string {
	result := make([]string, len(paths))
	for index := range paths {
		result[index] = paths[index].PathID()
	}
	return result
}
