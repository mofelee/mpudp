package transport_test

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/transport"
)

func TestCarrierStatisticsCountSocketBoundariesAndRetainAcrossRebuild(t *testing.T) {
	first := newFakeConnectedConn("local-1", "remote")
	second := newFakeConnectedConn("local-2", "remote")
	dialer := &fakeDialer{steps: []dialStep{{conn: first}, {conn: second}}}
	var enabled atomic.Bool
	counters := &transport.Counters{DiagnosticsEnabled: &enabled}
	packets := make(chan transport.ReceivedPacket, 1)
	errorsC := make(chan error, 1)
	carrier, err := transport.OpenCarrier(context.Background(), "carrier-0", "remote", transport.CarrierOptions{
		Dial: dialer.dial, MaxPayload: 4, Statistics: counters,
		OnPacket: func(packet transport.ReceivedPacket) { packets <- packet },
		OnError:  func(err error) { errorsC <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer carrier.Close()
	if err := carrier.Send(context.Background(), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if counters.SentPackets.Load() != 1 || counters.SentBytes.Load() != 3 || counters.WriteQueue.Snapshot().Count != 0 || counters.SocketWrite.Snapshot().Count != 0 {
		t.Fatal("disabled diagnostics changed basic counters or recorded timing")
	}
	for _, count := range counters.SentPacketSizes.Snapshot().Counts {
		if count != 0 {
			t.Fatal("disabled diagnostics recorded packet sizes")
		}
	}
	enabled.Store(true)
	first.setWriteError(syscall.EMSGSIZE)
	if err := carrier.Send(context.Background(), []byte("bad")); !errors.Is(err, syscall.EMSGSIZE) {
		t.Fatalf("write error = %v", err)
	}
	if counters.SentPackets.Load() != 1 || counters.SentBytes.Load() != 3 || counters.SendErrors.Load() != 1 {
		t.Fatal("failed socket write counted bytes or packets")
	}
	if err := carrier.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := carrier.Send(context.Background(), []byte("two")); err != nil {
		t.Fatal(err)
	}
	second.reads <- streamRead{payload: []byte("get")}
	_ = receivePacket(t, packets)
	second.reads <- streamRead{payload: []byte("oversize")}
	select {
	case err := <-errorsC:
		if !errors.Is(err, transport.ErrPayloadTooLarge) {
			t.Fatalf("oversize receive error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("missing oversize receive error")
	}
	if err := carrier.Close(); err != nil {
		t.Fatal(err)
	}
	if counters.SentPackets.Load() != 2 || counters.SentBytes.Load() != 6 || counters.ReceivedPackets.Load() != 2 || counters.ReceivedBytes.Load() != 8 || counters.ReceiveOversizeDrops.Load() != 1 {
		t.Fatal("socket counters were lost across rebuild/close or counted unobserved bytes")
	}
	if counters.WriteQueue.Snapshot().Count != 2 || counters.SocketWrite.Snapshot().Count != 2 || counters.SentPacketSizes.Snapshot().Counts[0] != 1 || counters.ReceivedPacketSizes.Snapshot().Counts[0] != 2 {
		t.Fatal("enabled diagnostics missed socket writes/receives")
	}
}
