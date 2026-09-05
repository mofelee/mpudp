package mpudp

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/fec"
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
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			p.enqueue(ingressEvent{})
			p.handleIngress(<-p.ingress)
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
