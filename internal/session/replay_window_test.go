package session

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/fec"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wire"
)

func sessionWindowShard(t *testing.T, id wire.SessionID, packetID uint64, index uint8) []byte {
	t.Helper()
	message, err := wire.NewDataShard(id, packetID, 3, 2, index, 3, []byte{1})
	if err != nil {
		t.Fatal(err)
	}
	return encodeMessage(t, message, []byte(testPSK), 1200)
}

func TestSessionReplayWindowRejectsOldEndpointLearningAndBadAuth(t *testing.T) {
	clock := newFakeClock()
	cfg := testConfig(clock, 1200)
	cfg.FECStatistics = &fec.Counters{}
	cfg.ListenerPathStatistics = transport.NewListenerPathCounters(nil)
	l, err := NewListener(ListenerConfig{Session: cfg, MaxSessions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close(context.Background())
	known := newFakePath("known", "198.51.100.1:4000")
	unknown := newFakePath("unknown", "198.51.100.2:4000")
	s, _, err := l.HandlePacket(context.Background(), received(known, helloPacket(t, testSessionID, 3, 2, 1200, []byte(testPSK))))
	if err != nil {
		t.Fatal(err)
	}
	complete := func(id uint64) {
		t.Helper()
		for index := uint8(0); index < 3; index++ {
			_, result, err := l.HandlePacket(context.Background(), received(known, sessionWindowShard(t, testSessionID, id, index)))
			if err != nil || (index == 2 && result.Datagram == nil) {
				t.Fatalf("complete ID %d: %+v %v", id, result, err)
			}
		}
	}
	complete(0)
	complete(fec.ReplayWindowIDs)
	before := s.Endpoints()[0].LastActivity
	paths, _ := cfg.ListenerPathStatistics.Snapshot()
	beforeReceives := paths[0].ReceivedPackets.Load()
	clock.Advance(time.Millisecond)
	for _, path := range []*fakePath{known, unknown} {
		_, result, err := l.HandlePacket(context.Background(), received(path, sessionWindowShard(t, testSessionID, 0, 0)))
		if err != nil || result.EndpointAdded || result.EndpointRefreshed || result.Datagram != nil {
			t.Fatalf("too-old DATA learned an Endpoint or delivered: %+v %v", result, err)
		}
	}
	if s.Snapshot().Endpoints != 1 || !s.Endpoints()[0].LastActivity.Equal(before) || paths[0].ReceivedPackets.Load() != beforeReceives {
		t.Fatal("too-old DATA changed Endpoint or accepted path counters")
	}
	if got, overflow := cfg.ListenerPathStatistics.Snapshot(); len(got) != 1 || overflow != nil {
		t.Fatal("too-old source allocated an anonymous path slot")
	}
	badTag := sessionWindowShard(t, testSessionID, math.MaxUint64, 0)
	badTag[len(badTag)-1] ^= 1
	if _, _, err := l.HandlePacket(context.Background(), received(known, badTag)); !errors.Is(err, wire.ErrAuthentication) {
		t.Fatalf("invalid high-ID authentication rejection = %v", err)
	}
	complete(1)
	if cfg.FECStatistics.TooOldShards.Load() != 2 || cfg.FECStatistics.CompletedBlocks.Load() != 3 {
		t.Fatal("bad authentication moved the replay floor or changed completions")
	}
}

func TestInitiatorReplayWindowDoesNotExpireCompletedIDs(t *testing.T) {
	clock := newFakeClock()
	cfg := testConfig(clock, 1200)
	cfg.FECStatistics = &fec.Counters{}
	cfg.MaxCompletedFECBlocks = 1
	path := newFakePath("carrier-0", "198.51.100.1:4000")
	s, err := NewInitiator(testSessionID, cfg, []Path{path})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	if _, err := s.HandlePacket(context.Background(), received(path, ackPacket(t, testSessionID, 3, 2, 1200))); err != nil {
		t.Fatal(err)
	}
	for id := uint64(0); id < 2; id++ {
		for index := uint8(0); index < 3; index++ {
			if _, err := s.HandlePacket(context.Background(), received(path, sessionWindowShard(t, testSessionID, id, index))); err != nil {
				t.Fatal(err)
			}
		}
	}
	clock.Advance(cfg.CompletionTTL * 10)
	if _, err := s.Advance(context.Background()); err != nil {
		t.Fatal(err)
	}
	for index := uint8(0); index < 5; index++ {
		result, err := s.HandlePacket(context.Background(), received(path, sessionWindowShard(t, testSessionID, 0, index)))
		if err != nil || result.Datagram != nil {
			t.Fatalf("initiator forgot completed ID after cache TTL/capacity: %+v %v", result, err)
		}
	}
	if cfg.FECStatistics.LateShards.Load() != 5 || cfg.FECStatistics.CompletedBlocks.Load() != 2 || cfg.FECStatistics.PendingBlocks.Load() != 0 {
		t.Fatal("initiator did not select production replay-window mode")
	}
}

type forwardingWindowPath struct {
	send func(context.Context, []byte) error
}

func (*forwardingWindowPath) PathID() string  { return "carrier-0" }
func (*forwardingWindowPath) Available() bool { return true }
func (p *forwardingWindowPath) Send(ctx context.Context, packet []byte) error {
	return p.send(ctx, packet)
}

func TestConcurrentWritePacketKeepsAdmittedLowIDAcrossReceiveWindow(t *testing.T) {
	clock := newFakeClock()
	cfg := testConfig(clock, 1200)
	receiveCounters := &fec.Counters{}
	receiverConfig := cfg
	receiverConfig.FECStatistics = receiveCounters
	l, err := NewListener(ListenerConfig{Session: receiverConfig, MaxSessions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close(context.Background())
	reply := newFakePath("listener/source", "198.51.100.1:4000")
	receiver, _, err := l.HandlePacket(context.Background(), received(reply, helloPacket(t, testSessionID, 3, 2, 1200, []byte(testPSK))))
	if err != nil {
		t.Fatal(err)
	}
	paused, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	var lowDelivered int
	path := &forwardingWindowPath{send: func(ctx context.Context, packet []byte) error {
		message, err := wire.DecodeAuthenticated(packet, []byte(testPSK), 1200)
		if err != nil || message.Header.Type != wire.TypeDataShard {
			return err
		}
		if message.DataShard.PacketID == 0 && message.DataShard.ShardIndex == 1 {
			close(paused)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		result, err := receiver.handleAuthenticated(ctx, ReceivedPacket{Payload: packet, Reply: reply}, message)
		if result.Datagram != nil && message.DataShard.PacketID == 0 {
			lowDelivered++
		}
		return err
	}}
	sender, err := NewInitiator(testSessionID, cfg, []Path{path})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close(context.Background())
	defer releaseOnce.Do(func() { close(release) })
	ackPath := newFakePath("carrier-0", "198.51.100.1:4000")
	if _, err := sender.HandlePacket(context.Background(), received(ackPath, ackPacket(t, testSessionID, 3, 2, 1200))); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := sender.WritePacket(context.Background(), []byte("pinned"))
		done <- err
	}()
	select {
	case <-paused:
	case <-time.After(5 * time.Second):
		t.Fatal("first WritePacket did not pause after its admitted shard")
	}
	// The encoder lock must permit newer complete writes while PacketID 0 is
	// paused outside it. Deliver a whole receive-window span before releasing 0.
	for id := uint64(1); id <= fec.ReplayWindowIDs; id++ {
		result, err := sender.WritePacket(context.Background(), []byte("newer"))
		if err != nil || result.PacketID != id {
			t.Fatalf("concurrent newer write ID %d: %+v %v", id, result, err)
		}
	}
	if receiveCounters.PendingBlocks.Load() != 1 || receiveCounters.CompletedBlocks.Load() != fec.ReplayWindowIDs {
		t.Fatal("newer writes displaced the already admitted low-ID block")
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if lowDelivered != 1 || receiveCounters.PendingBlocks.Load() != 0 || receiveCounters.CompletedBlocks.Load() != fec.ReplayWindowIDs+1 || receiveCounters.TooOldShards.Load() != 2 {
		t.Fatal("paused WritePacket failed to complete exactly once below the replay floor")
	}
}
