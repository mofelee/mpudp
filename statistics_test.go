package mpudp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/fec"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wire"
)

func TestStatisticsCountQueueDropsAndDeliveredPayload(t *testing.T) {
	p := &Peer{ctx: context.Background(), ingress: make(chan ingressEvent, 1)}
	p.enqueue(ingressEvent{})
	p.enqueue(ingressEvent{})
	s := &session{owner: p, delivery: make(chan []byte, 1), done: make(chan struct{})}
	s.deliver([]byte("accepted"))
	s.deliver([]byte("dropped"))
	got, err := s.ReadPacket()
	if err != nil || string(got) != "accepted" {
		t.Fatalf("ReadPacket() = %q, %v", got, err)
	}
	stats := p.Statistics()
	if stats.IngressAccepted != 1 || stats.IngressDrops != 1 || stats.DeliveryAccepted != 1 || stats.DeliveryDrops != 1 {
		t.Fatalf("queue statistics = %+v", stats)
	}
	if stats.DeliveredPackets != 1 || stats.DeliveredBytes != 8 {
		t.Fatalf("delivery statistics = %+v", stats)
	}
	if stats.DiagnosticsEnabled || stats.IngressQueue.Count != 0 {
		t.Fatal("default diagnostics recorded timing")
	}
	<-p.ingress
	p.SetDiagnosticsEnabled(true)
	p.enqueue(ingressEvent{})
	p.handleIngress(<-p.ingress)
	if stats := p.Statistics(); !stats.DiagnosticsEnabled || stats.IngressQueue.Count != 1 {
		t.Fatalf("enabled timing = %+v", stats)
	}
}

func TestStatisticsLoopbackAndRetention(t *testing.T) {
	cfg := runtimeTestConfig()
	cfg.Listen = reserveUDPAddress(t)
	server, err := NewPeer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	server.SetDiagnosticsEnabled(true)
	listener, _ := server.Listener()
	clientConfig := runtimeTestConfig()
	clientConfig.Carriers = []string{cfg.Listen}
	client, err := NewPeer(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetDiagnosticsEnabled(true)
	send, err := client.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), runtimeTestTimeout)
	defer cancel()
	receive, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("private-payload-marker")
	writeEventually(t, send, payload)
	if got := readWithin(t, receive); !bytes.Equal(got, payload) {
		t.Fatalf("payload = %q", got)
	}
	writeEventually(t, receive, payload)
	if got := readWithin(t, send); !bytes.Equal(got, payload) {
		t.Fatalf("reverse payload = %q", got)
	}
	for _, peer := range []*Peer{client, server} {
		stats := peer.Statistics()
		if stats.SentDatagrams != 1 || stats.SentDatagramBytes != uint64(len(payload)) || stats.DeliveredPackets != 1 || stats.FEC.CompletedBlocks != 1 {
			t.Fatalf("Datagram counts = %+v", stats)
		}
		if stats.FEC.PendingBlocks != 0 || stats.FEC.PendingShards != 0 || stats.FEC.PendingBytes != 0 ||
			stats.FEC.PendingBlocksHighWater == 0 || stats.FEC.PendingShardsHighWater == 0 || stats.FEC.PendingBytesHighWater == 0 {
			t.Fatalf("completed Datagram retained state or lost high-water marks: %+v", stats.FEC)
		}
		if len(stats.Paths) != 1 {
			t.Fatalf("path count = %d", len(stats.Paths))
		}
		if peer == server {
			if len(stats.ListenerPaths) != 1 {
				t.Fatalf("listener path count = %d", len(stats.ListenerPaths))
			}
			path := stats.ListenerPaths[0]
			if path.Path != "listener-path-0" || path.SentPackets < 2 || path.ReceivedPackets < 2 || path.WriteQueue.Count == 0 || path.SocketWrite.Count == 0 {
				t.Fatalf("listener path statistics = %+v", path)
			}
			stats.ListenerPaths[0].Path = "mutated"
			if peer.Statistics().ListenerPaths[0].Path == "mutated" {
				t.Fatal("listener snapshot aliases retained storage")
			}
		} else if len(stats.ListenerPaths) != 0 {
			t.Fatal("initiator retained listener path slots")
		}
		path := stats.Paths[0]
		if path.SentPackets < 1 || path.ReceivedPackets < 1 || path.SentBytes <= uint64(len(payload)) || path.ReceivedBytes <= uint64(len(payload)) {
			t.Fatalf("socket counts = %+v", path)
		}
		if stats.IngressQueue.Count == 0 || stats.SendLatency.Count == 0 || path.WriteQueue.Count == 0 || path.SocketWrite.Count == 0 {
			t.Fatalf("missing diagnostics = %+v", stats)
		}
		encoded, err := json.Marshal(stats)
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range []string{string(payload), string(cfg.PSK.Bytes()), cfg.Listen} {
			if strings.Contains(string(encoded), secret) {
				t.Fatalf("statistics exposed sensitive text %q", secret)
			}
		}
		stats.Paths[0].Path = "mutated"
		if peer.Statistics().Paths[0].Path == "mutated" {
			t.Fatal("snapshot aliases retained storage")
		}
		peer.SetDiagnosticsEnabled(false)
		before := peer.Statistics()
		if err := peer.Close(); err != nil {
			t.Fatal(err)
		}
		after := peer.Statistics()
		if after.DeliveredPackets != before.DeliveredPackets || after.FEC.CompletedBlocks != before.FEC.CompletedBlocks || after.Paths[0].SentBytes < before.Paths[0].SentBytes {
			t.Fatalf("close lost counters: before=%+v after=%+v", before, after)
		}
		if after.FEC.PendingBlocks != 0 || after.FEC.PendingShards != 0 || after.FEC.PendingBytes != 0 ||
			after.FEC.PendingBlocksHighWater != before.FEC.PendingBlocksHighWater ||
			after.FEC.PendingShardsHighWater != before.FEC.PendingShardsHighWater ||
			after.FEC.PendingBytesHighWater != before.FEC.PendingBytesHighWater {
			t.Fatalf("close changed retained-state diagnostics: before=%+v after=%+v", before.FEC, after.FEC)
		}
	}
}

func TestStatisticsListenerMultipleRemotePathsAndRawRejections(t *testing.T) {
	cfg := runtimeTestConfig()
	cfg.Listen = reserveUDPAddress(t)
	p, err := NewPeer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.SetDiagnosticsEnabled(true)
	remote, err := net.ResolveUDPAddr("udp", cfg.Listen)
	if err != nil {
		t.Fatal(err)
	}
	var id wire.SessionID
	id[0] = 1
	hello, err := wire.NewHello(id, uint8(cfg.FEC.DataShards), uint8(cfg.FEC.ParityShards), uint16(cfg.Transport.MaxUDPPayload))
	if err != nil {
		t.Fatal(err)
	}
	packet, err := wire.AppendAuthenticated(nil, hello, cfg.PSK.Bytes(), cfg.Transport.MaxUDPPayload)
	if err != nil {
		t.Fatal(err)
	}
	var privateAddresses []string
	for i := range 2 {
		conn, err := net.DialUDP("udp", nil, remote)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		privateAddresses = append(privateAddresses, conn.LocalAddr().String())
		if err := conn.SetDeadline(time.Now().Add(runtimeTestTimeout)); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write([]byte("invalid-authentication")); err != nil {
			t.Fatal(err)
		}
		waitForCondition(t, func() bool { return p.Statistics().Paths[0].ReceivedPackets >= uint64(2*i+1) }, "raw invalid packet count")
		if got := len(p.Statistics().ListenerPaths); got != i {
			t.Fatalf("invalid packet allocated slot: %d", got)
		}
		if _, err := conn.Write(packet); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, cfg.Transport.MaxUDPPayload)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		ack, err := wire.DecodeAuthenticated(buf[:n], cfg.PSK.Bytes(), cfg.Transport.MaxUDPPayload)
		if err != nil || ack.Header.Type != wire.TypeHelloAck {
			t.Fatalf("handshake response = %+v, %v", ack, err)
		}
	}
	waitForCondition(t, func() bool {
		stats := p.Statistics()
		return len(stats.ListenerPaths) == 2 && stats.ListenerPaths[1].SentPackets == 1
	}, "accepted listener path counters")
	stats := p.Statistics()
	if stats.Paths[0].ReceivedPackets != 4 || stats.Paths[0].SentPackets != 2 {
		t.Fatalf("raw listener counts = %+v", stats.Paths[0])
	}
	for i, path := range stats.ListenerPaths {
		if path.Path != fmt.Sprintf("listener-path-%d", i) || path.ReceivedPackets != 1 || path.SentPackets != 1 || path.ReceivedBytes != uint64(len(packet)) || path.SocketWrite.Count != 1 || path.WriteQueue.Count != 1 {
			t.Fatalf("accepted path = %+v", path)
		}
	}
	encoded, err := json.Marshal(stats)
	if err != nil {
		t.Fatal(err)
	}
	for _, address := range privateAddresses {
		if strings.Contains(string(encoded), address) {
			t.Fatal("listener path snapshot exposed a remote endpoint")
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if got := p.Statistics(); len(got.ListenerPaths) != 2 || got.ListenerPaths[0].ReceivedPackets != 1 || got.ListenerPaths[1].ReceivedPackets != 1 {
		t.Fatal("Peer close discarded listener path statistics")
	}
}

func TestStatisticsListenerOverflowSnapshotIsBoundedAndAnonymous(t *testing.T) {
	p := &Peer{}
	p.statistics.listenerPaths = transport.NewListenerPathCounters(&p.statistics.enabled)
	for i := range transport.MaxListenerStatisticsPaths + 100 {
		p.statistics.listenerPaths.Learn(fmt.Sprintf("private-remote-%d", i)).ReceiveAccepted(100)
	}
	stats := p.Statistics()
	if len(stats.ListenerPaths) != transport.MaxListenerStatisticsPaths+1 {
		t.Fatalf("listener path rows = %d", len(stats.ListenerPaths))
	}
	overflow := stats.ListenerPaths[len(stats.ListenerPaths)-1]
	if overflow.Path != "listener-overflow" || overflow.ReceivedPackets != 100 || overflow.ReceivedBytes != 10000 {
		t.Fatalf("overflow snapshot = %+v", overflow)
	}
	encoded, err := json.Marshal(stats)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "private-remote") {
		t.Fatal("overflow snapshot exposed private identities")
	}
}

func TestStatisticsFECStateAndCapacityEvictions(t *testing.T) {
	p := &Peer{}
	params := fec.Params{DataShards: 3, ParityShards: 2}
	budget := fec.Budget{MaxUDPPayload: 512, DataShardWireOverhead: 72, MaxDatagramSize: 1320}
	d, err := fec.NewDecoder(fec.DecoderConfig{
		Params: params, Budget: budget, DecodeTimeout: time.Minute, CompletionTTL: time.Minute,
		MaxPendingBlocks: 4, MaxCompletedBlocks: 1, Statistics: &p.statistics.fec,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	encoder, err := fec.NewEncoder(params, budget)
	if err != nil {
		t.Fatal(err)
	}
	block, err := encoder.Encode([]byte("retained"))
	if err != nil {
		t.Fatal(err)
	}
	add := func(id uint64, index int) {
		t.Helper()
		_, err := d.AddVerifiedShard(fec.IncomingShard{
			Key: fec.BlockKey{PacketID: id}, Params: params, Index: index,
			OriginalLength: block.OriginalLength, Payload: block.Shards[index],
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for id := uint64(0); id < 2; id++ {
		for index := 0; index < params.DataShards; index++ {
			add(id, index)
		}
	}
	add(2, 0)
	add(3, 0)
	add(3, 1)
	want := FECStatistics{
		CompletedBlocks: 2, CompletedCapacityEvictions: 1,
		PendingBlocks: 2, PendingShards: 3, PendingBytes: 9,
		PendingBlocksHighWater: 2, PendingShardsHighWater: 3, PendingBytesHighWater: 9,
	}
	if got := p.Statistics().FEC; got != want {
		t.Fatalf("FEC snapshot = %+v, want %+v", got, want)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	want.PendingBlocks, want.PendingShards, want.PendingBytes = 0, 0, 0
	if got := p.Statistics().FEC; got != want {
		t.Fatalf("FEC snapshot after decoder close = %+v, want %+v", got, want)
	}
}

func BenchmarkIngressDiagnostics(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		b.Run(name, func(b *testing.B) {
			p := &Peer{ctx: context.Background(), ingress: make(chan ingressEvent, 1)}
			p.SetDiagnosticsEnabled(enabled)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p.enqueue(ingressEvent{})
				p.handleIngress(<-p.ingress)
			}
		})
	}
}

func TestStatisticsConcurrentSnapshotsAndToggle(t *testing.T) {
	p := &Peer{ctx: context.Background(), ingress: make(chan ingressEvent, 1)}
	p.statistics.listenerPaths = transport.NewListenerPathCounters(&p.statistics.enabled)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			p.enqueue(ingressEvent{})
			p.handleIngress(<-p.ingress)
			p.statistics.listenerPaths.Learn(fmt.Sprintf("private-source-%d", i%300)).ReceiveAccepted(100)
		}
	}()
	for i := 0; i < 1000; i++ {
		p.SetDiagnosticsEnabled(i%2 == 0)
		_ = p.Statistics()
	}
	select {
	case <-done:
	case <-time.After(runtimeTestTimeout):
		t.Fatal("concurrent statistics did not finish")
	}
}
