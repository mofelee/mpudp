package sessionv2

import (
	"bytes"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/fecv2"
	"github.com/mofelee/mpudp/internal/handshakev2"
	"github.com/mofelee/mpudp/internal/reassemblyv2"
	"github.com/mofelee/mpudp/internal/recvwindow"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func creditProgressReceiver(t *testing.T, context wirev2.EncodingContext, groups int) (*Controller, *creditv2.Session, uint64, uint64) {
	t.Helper()
	n := uint64(context.DataShards + context.ParityShards)
	peak := 2*n*uint64(context.ShardBytes) + uint64(context.MaxLogicalBytes) + 2*n*uint64(unsafe.Sizeof([]byte{})) + uint64(context.MaxDescriptors)*uint64(unsafe.Sizeof(fecv2.Fragment{})) + uint64(unsafe.Sizeof(pendingGroup{})) + 64
	packetScratch := uint64(max(512, 94+int(context.ShardBytes))) + wirev2.MaxFECRecords*uint64(unsafe.Sizeof(wirev2.FECRecord{}))
	limits := reassemblyv2.Limits{MaxDatagrams: 256, MaxDatagramBytes: 65536, MaxFragments: 256, Span: 512, Timeout: 10 * time.Second}
	initial, err := reassemblyv2.RequiredInitialBytes(limits)
	if err != nil {
		t.Fatal(err)
	}
	initial += 16 + packetScratch // Group window and serialized receive scratch.
	limit := initial + uint64(groups)*peak + 1000
	peer, err := creditv2.New(creditv2.Limits{MaxPeerBytes: limit, MaxSessionBytes: limit, MaxSessions: 1, MaxPendingHandshakes: 1, MaxPendingAccepts: 1, MaxStreamsPerSession: 1, MaxPeerStreams: 1, MaxReservations: 1024})
	if err != nil {
		t.Fatal(err)
	}
	scope, windowLease, err := peer.BeginSession(creditv2.Claim{Bytes: 16})
	if err != nil {
		t.Fatal(err)
	}
	scratch, err := scope.Reserve(creditv2.Claim{Bytes: packetScratch})
	if err != nil {
		t.Fatal(err)
	}
	scratch, err = scope.BindBytes(scratch, packetScratch)
	if err != nil {
		t.Fatal(err)
	}
	originals, err := reassemblyv2.New(scope, limits)
	if err != nil {
		t.Fatal(err)
	}
	window, _ := recvwindow.New(64)
	codec, err := fecv2.New(parameters(context, 65536))
	if err != nil {
		t.Fatal(err)
	}
	c := &Controller{cfg: Config{MaxPendingGroups: groups, GroupTimeout: 10 * time.Second}, setup: handshakev2.Setup{Scope: scope}, receiveContext: context, receiveAckSent: true, receiveCodec: codec, groups: make(map[uint64]*pendingGroup), groupWindow: window, groupWindowLease: windowLease, controlLease: scratch, originals: originals}
	t.Cleanup(func() {
		c.Close()
		scope.Close()
		if got := peer.Snapshot(); got.Bytes != 0 || got.Reservations != 0 || got.SessionSlots != 0 {
			t.Errorf("leaked receive ownership: %+v", got)
		}
	})
	return c, scope, limit, peak
}

func receiveEncodedShard(t *testing.T, c *Controller, at time.Time, id uint64, group fecv2.Group, shard int) Result {
	t.Helper()
	budget := max(512, 94+int(c.receiveContext.ShardBytes))
	bundle := wirev2.FECBundle{
		Header: wirev2.Header{Type: wirev2.TypeFECBundle, SessionID: wirev2.SessionID{1}},
		Route:  wirev2.Route{PathID: 1, Generation: 1, BudgetEpoch: 1},
		Records: []wirev2.FECRecord{{GroupID: id, EncodingEpoch: c.receiveContext.Epoch,
			LogicalBytes: group.LogicalBytes, ShardIndex: uint8(shard), Payload: group.Shards[shard]}},
	}
	packet, err := wirev2.AppendFECBundle(nil, bundle, c.receiveLookup, wirev2.Key{1}, budget)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := wirev2.ParseEnvelope(packet)
	if err != nil {
		t.Fatal(err)
	}
	authenticated, err := envelope.Authenticate(wirev2.Key{1})
	if err != nil {
		t.Fatal(err)
	}
	var result Result
	if err := c.receiveBundle(at, authenticated, budget, false, &result); err != nil {
		t.Fatal(err)
	}
	for _, delivery := range result.Deliveries {
		t.Cleanup(delivery.Release)
	}
	clear(packet)
	return result
}

func TestDecodeReturnsWorkspaceCreditBeforeOriginalAdmission(t *testing.T) {
	context := wirev2.EncodingContext{Epoch: 1, LayoutID: 1, ProtectionID: 1, DataShards: 3, ParityShards: 2, ShardBytes: 1106, MaxDescriptors: 1, MaxLogicalBytes: 3318}
	c, scope, limit, peak := creditProgressReceiver(t, context, 32)
	base := scope.Snapshot()
	payload := bytes.Repeat([]byte("x"), 1400)
	encoded, err := c.receiveCodec.Encode([]fecv2.Fragment{{DatagramID: 32, TotalBytes: 1400, Payload: payload}})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1700000000, 0)
	for id := uint64(1); id <= 32; id++ {
		if result := receiveEncodedShard(t, c, start, id, encoded, 0); len(result.Deliveries) != 0 {
			t.Fatal("incomplete group delivered")
		}
	}
	if got := scope.Snapshot().Bytes; got != base.Bytes+32*peak {
		t.Fatalf("decode peak reservation = %d, want %d", got, base.Bytes+32*peak)
	}
	if free := limit - scope.Snapshot().Bytes; free != 1000 {
		t.Fatalf("expected 1000 bytes free during decode, got %d", free)
	}
	at := start.Add(time.Millisecond)
	receiveEncodedShard(t, c, at, 32, encoded, 3)
	result := receiveEncodedShard(t, c, at, 32, encoded, 4)
	if len(result.Deliveries) != 1 || !bytes.Equal(result.Deliveries[0].Payload(), payload) {
		t.Fatal("discarded reconstruction workspace still blocked the 1400-byte original")
	}
	if len(c.groups) != 31 || c.decodedGroups != 0 || c.groupWindow.State(32) != recvwindow.Completed || c.originals.Snapshot().Pending != 0 {
		t.Fatal("completion did not release only the decoded group")
	}
	for id, group := range c.groups {
		if id == 32 || !group.admitted.Equal(start) || group.lease.Snapshot().Bytes != peak || c.groupWindow.State(id) != recvwindow.Unseen {
			t.Fatal("progress changed an incomplete group's ownership or deadline")
		}
	}
	if got, want := scope.Snapshot().Bytes, base.Bytes+31*peak+uint64(len(payload)); got != want {
		t.Fatalf("retained ownership = %d, want %d", got, want)
	}
	result.Deliveries[0].Release()
	if scope.Snapshot().Bytes != base.Bytes+31*peak {
		t.Fatal("delivery release lost receiver credit")
	}
}

func TestDecodedGroupRetainsCompleteLogicalBackingUnderPressure(t *testing.T) {
	for _, count := range []int{1, fecv2.MaxDescriptors} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			context := wirev2.EncodingContext{Epoch: 1, LayoutID: 1, ProtectionID: 1, DataShards: 3, ParityShards: 2, ShardBytes: 2048, MaxDescriptors: fecv2.MaxDescriptors, MaxLogicalBytes: 6144}
			c, scope, _, peak := creditProgressReceiver(t, context, 1)
			base := scope.Snapshot()
			fragments := make([]fecv2.Fragment, count)
			for i := range fragments {
				fragments[i] = fecv2.Fragment{DatagramID: uint64(i + 1), TotalBytes: 65536, Payload: []byte{byte(i + 1)}}
			}
			encoded, err := c.receiveCodec.Encode(fragments)
			if err != nil {
				t.Fatal(err)
			}
			start := time.Unix(1700000000, 0)
			for _, shard := range []int{0, 3, 4} {
				if result := receiveEncodedShard(t, c, start, 1, encoded, shard); len(result.Deliveries) != 0 {
					t.Fatal("incomplete original delivered")
				}
			}
			group := c.groups[1]
			if group == nil || group.shards != nil || len(group.fragments) != count || cap(group.fragments) != count || c.decodedGroups != 1 || c.originals.Snapshot().Pending != 0 {
				t.Fatal("pressure did not retain only decoded-group ownership")
			}
			// Payload capacities are one byte each, but their shared allocation
			// also retains the full manifest and descriptor prefix.
			want := uint64(fecv2.ManifestBytes+count*(fecv2.DescriptorBytes+1)) + uint64(count)*uint64(unsafe.Sizeof(fecv2.Fragment{})) + uint64(unsafe.Sizeof(pendingGroup{})) + 64
			if want >= peak || group.lease.Snapshot().Bytes != want || scope.Snapshot().Bytes != base.Bytes+want {
				t.Fatalf("decoded charge = %d, want %d", group.lease.Snapshot().Bytes, want)
			}
			for i, fragment := range group.fragments {
				if cap(fragment.Payload) != 1 || !bytes.Equal(fragment.Payload, fragments[i].Payload) {
					t.Fatal("decoded backing was modified or charged by its exposed payload capacity")
				}
			}
			before := scope.Snapshot()
			var result Result
			if err := c.retryGroups(start.Add(time.Millisecond), &result); err != nil || scope.Snapshot() != before || c.groups[1] != group || c.originals.Snapshot().Pending != 0 {
				t.Fatal("failed original retry changed source ownership or partially admitted originals")
			}
			borrowed := group.fragments[0].Payload
			c.expireGroups(start.Add(c.cfg.GroupTimeout))
			if scope.Snapshot() != base || borrowed[0] != 0 || c.decodedGroups != 0 || c.groupWindow.State(1) != recvwindow.Expired {
				t.Fatal("decoded expiry did not clear bytes and return the reduced lease exactly")
			}
		})
	}
}
