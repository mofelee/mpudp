package session

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/fec"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wire"
)

func TestListenerStatisticsRejectBeforeAllocatingOrCounting(t *testing.T) {
	cfg := testConfig(newFakeClock(), 1200)
	cfg.MaxEndpoints = 1
	cfg.MaxPendingFECBlocks = 1
	cfg.ListenerPathStatistics = transport.NewListenerPathCounters(nil)
	l, err := NewListener(ListenerConfig{Session: cfg, MaxSessions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close(context.Background())
	known := newFakePath("known", "198.51.100.1:4000")
	unknown := newFakePath("unknown", "198.51.100.2:4000")
	hello := helloPacket(t, testSessionID, 3, 2, 1200, []byte(testPSK))
	badTag := append([]byte(nil), hello...)
	badTag[len(badTag)-1] ^= 1
	ping, _ := wire.NewPing(testSessionID, 1, 2)
	for _, packet := range [][]byte{
		badTag,
		helloPacket(t, testSessionID, 4, 1, 1200, []byte(testPSK)),
		encodeMessage(t, ping, []byte(testPSK), 1200),
	} {
		if _, _, err := l.HandlePacket(context.Background(), received(unknown, packet)); err == nil {
			t.Fatal("rejected packet accepted")
		}
		if paths, overflow := cfg.ListenerPathStatistics.Snapshot(); len(paths) != 0 || overflow != nil {
			t.Fatal("rejected packet allocated statistics")
		}
	}
	s, _, err := l.HandlePacket(context.Background(), received(known, hello))
	if err != nil {
		t.Fatal(err)
	}
	paths, _ := cfg.ListenerPathStatistics.Snapshot()
	if len(paths) != 1 || paths[0].ReceivedPackets.Load() != 1 || paths[0].ReceivedBytes.Load() != uint64(len(hello)) {
		t.Fatal("first accepted HELLO was not counted exactly once")
	}
	pong, _ := wire.NewPong(testSessionID, 1, 2)
	for _, test := range []struct {
		path *fakePath
		data []byte
		want error
	}{
		{unknown, hello, ErrEndpointLimit},
		{unknown, helloPacket(t, otherSessionID, 3, 2, 1200, []byte(testPSK)), ErrSessionLimit},
		{known, helloPacket(t, testSessionID, 4, 1, 1200, []byte(testPSK)), ErrHandshakeIncompatible},
		{unknown, encodeMessage(t, pong, []byte(testPSK), 1200), ErrProbeMismatch},
		{known, badTag, wire.ErrAuthentication},
	} {
		if _, _, err := l.HandlePacket(context.Background(), received(test.path, test.data)); !errors.Is(err, test.want) {
			t.Fatalf("rejection = %v, want %v", err, test.want)
		}
		if got, overflow := cfg.ListenerPathStatistics.Snapshot(); len(got) != 1 || overflow != nil || got[0].ReceivedPackets.Load() != 1 {
			t.Fatal("rejection changed path statistics")
		}
	}
	shard := func(id uint64) []byte {
		message, err := wire.NewDataShard(testSessionID, id, 3, 2, 0, 3, []byte{1})
		if err != nil {
			t.Fatal(err)
		}
		return encodeMessage(t, message, []byte(testPSK), 1200)
	}
	if _, _, err := l.HandlePacket(context.Background(), received(known, shard(1))); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.HandlePacket(context.Background(), received(known, shard(2))); !errors.Is(err, fec.ErrDecoderFull) {
		t.Fatalf("decoder capacity rejection = %v", err)
	}
	if paths[0].ReceivedPackets.Load() != 2 {
		t.Fatal("decoder-full rejection increased path receives")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.HandlePacket(context.Background(), received(unknown, hello)); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed Session accepted packet: %v", err)
	}
	if paths[0].ReceivedPackets.Load() != 2 {
		t.Fatal("closed Session changed retained counters")
	}
}

func TestListenerStatisticsReuseAcrossExpirySessionsAndClose(t *testing.T) {
	clock := newFakeClock()
	cfg := testConfig(clock, 1200)
	cfg.ListenerPathStatistics = transport.NewListenerPathCounters(nil)
	l, err := NewListener(ListenerConfig{Session: cfg, MaxSessions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close(context.Background())
	path := newFakePath("listener/source", "198.51.100.1:4000")
	hello := helloPacket(t, testSessionID, 3, 2, 1200, []byte(testPSK))
	for range 2 {
		if _, _, err := l.HandlePacket(context.Background(), received(path, hello)); err != nil {
			t.Fatal(err)
		}
	}
	clock.Advance(cfg.EndpointTTL + time.Second)
	l.Advance(context.Background())
	if _, _, err := l.HandlePacket(context.Background(), received(path, hello)); err != nil {
		t.Fatal(err)
	}
	closeMessage, _ := wire.NewClose(testSessionID)
	closePacket := encodeMessage(t, closeMessage, []byte(testPSK), 1200)
	if _, _, err := l.HandlePacket(context.Background(), received(path, closePacket)); err != nil {
		t.Fatal(err)
	}
	hello = helloPacket(t, otherSessionID, 3, 2, 1200, []byte(testPSK))
	if _, _, err := l.HandlePacket(context.Background(), received(path, hello)); err != nil {
		t.Fatal(err)
	}
	unknown := newFakePath("listener/unknown", "198.51.100.2:4000")
	closeMessage, _ = wire.NewClose(otherSessionID)
	if _, _, err := l.HandlePacket(context.Background(), received(unknown, encodeMessage(t, closeMessage, []byte(testPSK), 1200))); err != nil {
		t.Fatal(err)
	}
	paths, overflow := cfg.ListenerPathStatistics.Snapshot()
	if len(paths) != 1 || overflow != nil || paths[0].ReceivedPackets.Load() != 5 {
		t.Fatal("expiry, Session churn, or unknown CLOSE changed slot identity/counts")
	}
	if err := l.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.HandlePacket(context.Background(), received(unknown, hello)); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed Listener accepted packet: %v", err)
	}
	if got, overflow := cfg.ListenerPathStatistics.Snapshot(); len(got) != 1 || got[0] != paths[0] || overflow != nil || paths[0].ReceivedPackets.Load() != 5 {
		t.Fatal("close lost counters or allocated a new slot")
	}
}

func TestListenerStatisticsDecoderFullDoesNotLearnNewPath(t *testing.T) {
	cfg := testConfig(newFakeClock(), 1200)
	cfg.MaxPendingFECBlocks = 1
	cfg.ListenerPathStatistics = transport.NewListenerPathCounters(nil)
	l, err := NewListener(ListenerConfig{Session: cfg, MaxSessions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close(context.Background())
	known := newFakePath("known", "198.51.100.1:4000")
	unknown := newFakePath("unknown", "198.51.100.2:4000")
	hello := helloPacket(t, testSessionID, 3, 2, 1200, []byte(testPSK))
	if _, _, err := l.HandlePacket(context.Background(), received(known, hello)); err != nil {
		t.Fatal(err)
	}
	first, _ := wire.NewDataShard(testSessionID, 1, 3, 2, 0, 3, []byte{1})
	second, _ := wire.NewDataShard(testSessionID, 2, 3, 2, 0, 3, []byte{1})
	packet := encodeMessage(t, first, []byte(testPSK), 1200)
	if _, _, err := l.HandlePacket(context.Background(), received(known, packet)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.HandlePacket(context.Background(), received(unknown, encodeMessage(t, second, []byte(testPSK), 1200))); !errors.Is(err, fec.ErrDecoderFull) {
		t.Fatalf("new source rejected before decoder capacity check: %v", err)
	}
	paths, overflow := cfg.ListenerPathStatistics.Snapshot()
	if len(paths) != 1 || overflow != nil || paths[0].ReceivedPackets.Load() != 2 {
		t.Fatal("decoder-full new source allocated or incremented path statistics")
	}
	if _, _, err := l.HandlePacket(context.Background(), received(known, packet)); err != nil {
		t.Fatal(err)
	}
	if paths[0].ReceivedPackets.Load() != 3 {
		t.Fatal("accepted duplicate shard was not counted")
	}
}

func TestListenerStatisticsConcurrentAdmissionBoundsLifetimeSlots(t *testing.T) {
	cfg := testConfig(newFakeClock(), 1200)
	cfg.ListenerPathStatistics = transport.NewListenerPathCounters(nil)
	l, err := NewListener(ListenerConfig{Session: cfg, MaxSessions: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close(context.Background())
	const count = transport.MaxListenerStatisticsPaths + 40
	var workers sync.WaitGroup
	errs := make(chan error, count)
	for i := range count {
		id := testSessionID
		binary.BigEndian.PutUint64(id[8:], uint64(i+1))
		hello := helloPacket(t, id, 3, 2, 1200, []byte(testPSK))
		path := newFakePath(fmt.Sprintf("listener/%d", i), fmt.Sprintf("198.51.100.1:%d", 4000+i))
		workers.Add(1)
		go func() {
			defer workers.Done()
			s, _, err := l.HandlePacket(context.Background(), received(path, hello))
			if err == nil {
				_, _, err = l.HandlePacket(context.Background(), received(path, hello))
			}
			if err == nil {
				err = s.Close(context.Background())
			}
			errs <- err
		}()
	}
	for range count {
		paths, overflow := cfg.ListenerPathStatistics.Snapshot()
		if len(paths) > transport.MaxListenerStatisticsPaths {
			t.Fatal("unbounded listener slots")
		}
		for _, path := range paths {
			_ = path.ReceivedPackets.Load()
		}
		if overflow != nil {
			_ = overflow.ReceivedPackets.Load()
		}
	}
	workers.Wait()
	for range count {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	paths, overflow := cfg.ListenerPathStatistics.Snapshot()
	if len(paths) != transport.MaxListenerStatisticsPaths || overflow == nil || overflow.ReceivedPackets.Load() != 80 {
		t.Fatal("overflow did not retain exactly the excess accepted traffic")
	}
	for _, path := range paths {
		if path.ReceivedPackets.Load() != 2 {
			t.Fatal("named slot was recycled across Session closure")
		}
	}
	paths[0] = nil
	if got, _ := cfg.ListenerPathStatistics.Snapshot(); got[0] == nil {
		t.Fatal("snapshot aliases registry slice")
	}
}
