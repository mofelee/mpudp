package sessionv2

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/fecv2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func TestReceiveStatisticsSeparatesPrepaidScratchAndGroupPressure(t *testing.T) {
	p := newPair(t, 1, 1, false, 5000)
	p.pump(t, p.ready)
	first := groupPackets(t, p, []byte("first"))
	second := groupPackets(t, p, []byte("second"))
	c := p.server.controller
	corrupt := first[0]
	corrupt.data = bytes.Clone(corrupt.data)
	corrupt.data[len(corrupt.data)-1] ^= 1
	if _, err := receivePacket(p, corrupt); !errors.Is(err, wirev2.ErrAuthentication) || c.ReceiveStatistics() != (ReceiveStatistics{}) {
		t.Fatal("unauthenticated packet entered FEC handler statistics")
	}
	held, err := c.setup.Scope.Reserve(creditv2.Claim{Bytes: (64 << 20) - p.server.peer.Snapshot().Bytes})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receivePacket(p, first[0]); !errors.Is(err, creditv2.ErrResourceLimit) {
		t.Fatalf("new group was not refused after using prepaid scratch: %v", err)
	}
	held.Release()
	c.cfg.MaxPendingGroups = 1
	if _, err := receivePacket(p, first[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := receivePacket(p, second[0]); !errors.Is(err, creditv2.ErrResourceLimit) {
		t.Fatalf("pending-group capacity was not refused: %v", err)
	}
	want := ReceiveStatistics{ReceiveCounters: ReceiveCounters{ReceivedFECBundles: 3, NewGroupRejections: 2}, PendingGroups: 1}
	if got := c.ReceiveStatistics(); got != want {
		t.Fatalf("resource stages = %+v, want %+v", got, want)
	}
	for _, pk := range first[1:] {
		result, err := receivePacket(p, pk)
		if err != nil {
			t.Fatal(err)
		}
		p.server.deliveries = append(p.server.deliveries, result.Deliveries...)
	}
	if _, err := receivePacket(p, second[0]); err != nil {
		t.Fatal(err)
	}
	c.expireGroups(p.now.Add(c.cfg.GroupTimeout))
	want = ReceiveStatistics{ReceiveCounters: ReceiveCounters{ReceivedFECBundles: 8, NewGroupRejections: 2, DecodedGroups: 1, CompletedGroups: 1, ExpiredGroups: 1}}
	if got := c.ReceiveStatistics(); got != want {
		t.Fatalf("lifecycle statistics = %+v, want %+v", got, want)
	}
	p.close(t)
	if got := c.ReceiveStatistics(); got != want {
		t.Fatalf("Close changed cumulative events: %+v", got)
	}
}

func TestReceiveStatisticsCountsBlockedOriginalRetryAttempts(t *testing.T) {
	context := wirev2.EncodingContext{Epoch: 1, LayoutID: 1, ProtectionID: 1, DataShards: 3, ParityShards: 2, ShardBytes: 2048, MaxDescriptors: 1, MaxLogicalBytes: 6144}
	c, _, _, _ := creditProgressReceiver(t, context, 1)
	encoded, err := c.receiveCodec.Encode([]fecv2.Fragment{{DatagramID: 1, TotalBytes: 65536, Payload: []byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1700000000, 0)
	for _, shard := range []int{0, 3, 4} {
		receiveEncodedShard(t, c, now, 1, encoded, shard)
	}
	want := ReceiveStatistics{ReceiveCounters: ReceiveCounters{ReceivedFECBundles: 3, OriginalAdmissionRejections: 1, DecodedGroups: 1}, PendingGroups: 1, DecodedPendingGroups: 1}
	if got := c.ReceiveStatistics(); got != want {
		t.Fatalf("blocked original statistics = %+v, want %+v", got, want)
	}
	if err := c.retryGroups(now.Add(time.Millisecond), &Result{}); err != nil {
		t.Fatal(err)
	}
	want.OriginalAdmissionRejections++
	if got := c.ReceiveStatistics(); got != want {
		t.Fatalf("retry did not count exactly one original admission attempt: %+v", got)
	}
	c.Close()
	want.PendingGroups, want.DecodedPendingGroups = 0, 0
	if got := c.ReceiveStatistics(); got != want {
		t.Fatalf("Close counted expiry or retained gauges: %+v", got)
	}
}

func TestReceiveStatisticsSamplesIncompleteOriginals(t *testing.T) {
	context := wirev2.EncodingContext{Epoch: 1, LayoutID: 1, ProtectionID: 1, DataShards: 3, ParityShards: 2, ShardBytes: 1106, MaxDescriptors: 1, MaxLogicalBytes: 3318}
	c, _, _, _ := creditProgressReceiver(t, context, 2)
	now := time.Unix(1700000000, 0)
	for i := range 2 {
		encoded, err := c.receiveCodec.Encode([]fecv2.Fragment{{DatagramID: 1, TotalBytes: 2, Offset: uint32(i), Payload: []byte{byte('a' + i)}}})
		if err != nil {
			t.Fatal(err)
		}
		for _, shard := range []int{0, 1, 2} {
			receiveEncodedShard(t, c, now, uint64(i+1), encoded, shard)
		}
		want := ReceiveStatistics{ReceiveCounters: ReceiveCounters{ReceivedFECBundles: uint64(3 * (i + 1)), DecodedGroups: uint64(i + 1), CompletedGroups: uint64(i + 1)}, PendingOriginals: 1 - i}
		if got := c.ReceiveStatistics(); got != want {
			t.Fatalf("original gauge after fragment %d = %+v, want %+v", i, got, want)
		}
	}
}

func TestReceiveStatisticsIncludesDecodeErrorExpiry(t *testing.T) {
	p := newPair(t, 1, 1, false, 5000)
	p.pump(t, p.ready)
	packets := groupPackets(t, p, []byte("payload"))
	c := p.server.controller
	envelope, err := wirev2.ParseEnvelope(packets[0].data)
	if err != nil {
		t.Fatal(err)
	}
	authenticated, err := envelope.Authenticate(c.receiveKey)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := wirev2.DecodeFECBundle(authenticated, c.receiveLookup, 1200)
	if err != nil {
		t.Fatal(err)
	}
	bundle.Records[0].Payload[0] ^= 1
	packets[0].data, err = wirev2.AppendFECBundle(nil, bundle, c.receiveLookup, c.receiveKey, 1200)
	if err != nil {
		t.Fatal(err)
	}
	for i, pk := range packets[:3] {
		_, err := receivePacket(p, pk)
		if (i < 2 && err != nil) || (i == 2 && !errors.Is(err, fecv2.ErrInvalid)) {
			t.Fatalf("shard %d decode result = %v", i, err)
		}
	}
	want := ReceiveStatistics{ReceiveCounters: ReceiveCounters{ReceivedFECBundles: 3, ExpiredGroups: 1}}
	if got := c.ReceiveStatistics(); got != want {
		t.Fatalf("decode error was not a terminal expiry: %+v", got)
	}
	p.close(t)
}
