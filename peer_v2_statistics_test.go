//go:build linux

package mpudp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestV2StatisticsRetainsCountersAcrossSessionAndPeerClose(t *testing.T) {
	server, listener := v2LoopbackListener(t, "127.0.0.1:0", false)
	ctx, cancel := context.WithCancel(context.Background())
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		var previous V2ReceiveStatistics
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			current := server.Statistics().V2Receive
			if current.ReceivedFECBundles < previous.ReceivedFECBundles || current.CompletedGroups < previous.CompletedGroups {
				t.Error("concurrent sampling lost cumulative receive events")
				return
			}
			previous = *current
		}
	}()
	t.Cleanup(func() { cancel(); <-sampled })
	var expectedBundles uint64
	for i := range 2 {
		server.SetDiagnosticsEnabled(i != 0)
		client, sender, receiver := v2LoopbackDial(t, server, listener, []string{"127.0.0.1"}, false)
		payload := []byte(fmt.Sprintf("private-receive-statistics-%d", i))
		if err := sender.WritePacket(payload); err != nil {
			t.Fatal(err)
		}
		v2Flush(t, sender)
		if got := readWithin(t, receiver); !bytes.Equal(got, payload) {
			t.Fatal("statistics changed delivered payload")
		}
		expectedBundles += uint64(server.config.FEC.DataShards + server.config.FEC.ParityShards)
		waitForCondition(t, func() bool { return server.Statistics().V2Receive.ReceivedFECBundles == expectedBundles }, "all FEC handler attempts")
		stats := server.Statistics()
		got := stats.V2Receive
		if got.DecodedGroups != uint64(i+1) || got.CompletedGroups != uint64(i+1) || got.ExpiredGroups != 0 || got.PacketScratchRejections != 0 || got.NewGroupRejections != 0 || got.OriginalAdmissionRejections != 0 {
			t.Fatalf("live/retired receive counters = %+v", got)
		}
		if got.PendingGroups != 0 || got.DecodedPendingGroups != 0 || got.PendingOriginals != 0 || got.CreditBytes == 0 || got.CreditReservations == 0 {
			t.Fatalf("current receive gauges = %+v", got)
		}
		encoded, err := json.Marshal(stats)
		if err != nil {
			t.Fatal(err)
		}
		for _, private := range []string{string(payload), string(server.config.PSK.Bytes()), server.listenerSocket.LocalAddr().String()} {
			if strings.Contains(string(encoded), private) {
				t.Fatal("receive statistics exposed private data")
			}
		}
		got.CompletedGroups = 999
		if server.Statistics().V2Receive.CompletedGroups != uint64(i+1) {
			t.Fatal("receive snapshot aliases retained state")
		}
		if err := receiver.Close(); err != nil {
			t.Fatal(err)
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		if retired := server.Statistics().V2Receive; retired.CompletedGroups != uint64(i+1) || retired.ReceivedFECBundles != expectedBundles || retired.PendingGroups != 0 || retired.DecodedPendingGroups != 0 || retired.PendingOriginals != 0 {
			t.Fatalf("disposed session statistics = %+v", retired)
		}
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	closed := server.Statistics().V2Receive
	if closed.CompletedGroups != 2 || closed.ReceivedFECBundles != expectedBundles || closed.ExpiredGroups != 0 || closed.PendingGroups != 0 || closed.DecodedPendingGroups != 0 || closed.PendingOriginals != 0 || closed.CreditBytes != 0 || closed.CreditReservations != 0 {
		t.Fatalf("closed peer statistics = %+v", closed)
	}
}

func TestStatisticsOmitsV2ReceiveForLegacyPeer(t *testing.T) {
	p := &Peer{}
	stats := p.Statistics()
	encoded, err := json.Marshal(stats)
	if err != nil {
		t.Fatal(err)
	}
	if stats.V2Receive != nil || strings.Contains(string(encoded), "v2_receive") {
		t.Fatal("legacy peer claims v2 receive statistics")
	}
}
