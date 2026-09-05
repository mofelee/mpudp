package sessionv2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
	"time"
	"unsafe"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/recvwindow"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func groupPackets(t testing.TB, p *pair, payload []byte) []packet {
	t.Helper()
	start := len(p.client.sent)
	p.drop = func(toServer bool, pk packet) bool {
		return toServer && wirev2.PacketType(pk.data[5]) == wirev2.TypeFECBundle
	}
	id, _, err := p.client.controller.Write(p.now, payload)
	if err != nil {
		t.Fatal(err)
	}
	p.pump(t, func() bool { return p.client.controller.Snapshot().CompletedThrough >= uint64(id) })
	var packets []packet
	for _, pk := range p.client.sent[start:] {
		if wirev2.PacketType(pk.data[5]) == wirev2.TypeFECBundle {
			packets = append(packets, pk)
		}
	}
	return packets
}

func receivePacket(p *pair, pk packet) (Result, error) {
	binding := opposite(pk.binding, true)
	return p.server.controller.Receive(p.now, binding, &testPath{binding}, pk.data)
}

func TestFECRecoveryFromTwoMissingDataShards(t *testing.T) {
	p := newPair(t, 1, 1, false, 5000)
	p.pump(t, p.ready)
	payload := bytes.Repeat([]byte("recover"), 40)
	packets := groupPackets(t, p, payload)
	if len(packets) != 5 {
		t.Fatalf("got%d shards", len(packets))
	}
	for _, pk := range packets[2:] {
		result, err := receivePacket(p, pk)
		if err != nil {
			t.Fatal(err)
		}
		p.server.deliveries = append(p.server.deliveries, result.Deliveries...)
	}
	if len(p.server.deliveries) != 1 || !bytes.Equal(p.server.deliveries[0].Payload(), payload) {
		t.Fatal("any-k reconstruction failed")
	}
	p.close(t)
}

func TestPendingGroupPressureDoesNotMoveHistory(t *testing.T) {
	p := newPair(t, 1, 1, false, 5000)
	p.pump(t, p.ready)
	first := groupPackets(t, p, []byte("first"))
	second := groupPackets(t, p, []byte("second"))
	c := p.server.controller
	c.cfg.MaxPendingGroups = 1
	c.cfg.GroupTimeout = 100 * time.Millisecond
	if _, err := receivePacket(p, first[0]); err != nil {
		t.Fatal(err)
	}
	before, history := p.server.peer.Snapshot(), c.groupWindow.Snapshot()
	if _, err := receivePacket(p, second[0]); !errors.Is(err, creditv2.ErrResourceLimit) {
		t.Fatalf("capacity was not refused: %v", err)
	}
	if p.server.peer.Snapshot() != before || c.groupWindow.Snapshot() != history || len(c.groups) != 1 {
		t.Fatal("failed group admission moved floor or retained bytes")
	}
	deadline := c.groups[1].admitted.Add(c.cfg.GroupTimeout)
	p.now = deadline
	if _, err := c.Advance(p.now); err != nil {
		t.Fatal(err)
	}
	if len(c.groups) != 0 || c.groupWindow.State(1) != recvwindow.Expired {
		t.Fatal("original group deadline was extended")
	}
	if result, err := receivePacket(p, first[1]); err != nil || len(result.Deliveries) != 0 || len(c.groups) != 0 {
		t.Fatal("expired group was recreated")
	}
	p.close(t)
}

func TestDecodedGroupRetainsOwnershipUntilAtomicOriginalAdmission(t *testing.T) {
	p := newPair(t, 1, 1, false, 5000)
	p.pump(t, p.ready)
	// This original still exceeds the credit reclaimed from decode workspace.
	payload := bytes.Repeat([]byte("x"), 50000)
	packets := groupPackets(t, p, payload)
	c := p.server.controller
	for _, pk := range packets[:2] {
		if _, err := receivePacket(p, pk); err != nil {
			t.Fatal(err)
		}
	}
	group := c.groups[1]
	deadline := group.admitted.Add(c.cfg.GroupTimeout)
	scratch := uint64(len(packets[2].data)) + wirev2.MaxFECRecords*uint64(unsafe.Sizeof(wirev2.FECRecord{}))
	held, err := c.setup.Scope.Reserve(creditv2.Claim{Bytes: (64 << 20) - p.server.peer.Snapshot().Bytes - scratch})
	if err != nil {
		t.Fatal(err)
	}
	result, err := receivePacket(p, packets[2])
	if err != nil || len(result.Deliveries) != 0 || c.groups[1] != group || group.fragments == nil || c.decodedGroups != 1 || c.groupWindow.State(1) != recvwindow.Unseen || !group.admitted.Add(c.cfg.GroupTimeout).Equal(deadline) {
		t.Fatalf("decoded group lost pending ownership: %+v %v", result, err)
	}
	held.Release()
	p.now = p.now.Add(time.Millisecond)
	result, err = c.Advance(p.now)
	if err != nil || len(result.Deliveries) != 0 || c.groups[1] != nil || c.decodedGroups != 0 || c.groupHead != 0 || c.groupTail != 0 || c.groupWindow.State(1) != recvwindow.Completed || c.originals.Snapshot().Pending != 1 {
		t.Fatalf("retry did not atomically admit first fragment group: %+v %v", result, err)
	}
	for i, pk := range packets[5:] {
		if i%5 >= 3 {
			continue
		}
		result, err = receivePacket(p, pk)
		if err != nil {
			t.Fatal(err)
		}
		p.server.deliveries = append(p.server.deliveries, result.Deliveries...)
	}
	if len(p.server.deliveries) != 1 || !bytes.Equal(p.server.deliveries[0].Payload(), payload) {
		t.Fatal("owned original did not complete after pressure cleared")
	}
	p.close(t)
}

func TestAuthenticatedBudgetSubstitutionDoesNotMutate(t *testing.T) {
	p := newPair(t, 1, 1, false, 5000)
	p.pump(t, p.ready)
	c, server := p.client.controller, p.server.controller
	path := &c.paths[0]
	before := server.paths[0].receiveBudget
	for _, change := range []func([]byte){func(body []byte) { body[0] = 1 }, func(body []byte) { binary.BigEndian.PutUint16(body[2:4], 1100) }, func(body []byte) { binary.BigEndian.PutUint32(body[4:], 3) }} {
		var body [8]byte
		binary.BigEndian.PutUint16(body[2:4], 1200)
		binary.BigEndian.PutUint32(body[4:], 2)
		change(body[:])
		encoded, err := wirev2.AppendEstablished(nil, wirev2.Header{Type: wirev2.TypePathBudgetUpdate, SessionID: c.setup.ID}, path.route(), body[:], c.sendKey)
		if err != nil {
			t.Fatal(err)
		}
		binding := opposite(path.binding, true)
		if _, err := server.Receive(p.now, binding, &testPath{binding}, encoded); !errors.Is(err, ErrProtocol) {
			t.Fatalf("invalid budget accepted: %v", err)
		}
		if server.paths[0].receiveBudget != before {
			t.Fatal("invalid update changed committed budget")
		}
	}
	p.close(t)
}

func TestFailedCarrierPreservesOtherPathAndAdmittedOriginal(t *testing.T) {
	p := newPair(t, 2, 1, false, 5000)
	p.pump(t, p.ready)
	payload := bytes.Repeat([]byte("surviving path"), 500)
	receipt, _, err := p.client.controller.Write(p.now, payload)
	if err != nil {
		t.Fatal(err)
	}
	result, err := p.client.controller.FailPath(p.now, p.client.controller.paths[0].binding)
	if err != nil || !result.PathsChanged || !result.Ready || p.client.controller.paths[0].active || !p.client.controller.paths[1].active {
		t.Fatalf("one local path failure destroyed sibling authority: %+v %v", result, err)
	}
	first := len(p.client.sent)
	p.pump(t, func() bool {
		return p.client.controller.Snapshot().CompletedThrough == uint64(receipt) && len(p.server.deliveries) == 1
	})
	if !bytes.Equal(p.server.deliveries[0].Payload(), payload) {
		t.Fatal("path failure discarded part of admitted original")
	}
	for _, pk := range p.client.sent[first:] {
		if wirev2.PacketType(pk.data[5]) == wirev2.TypeFECBundle && pk.binding.SocketID != 2 {
			t.Fatal("failed Carrier retained send authority")
		}
	}
	p.close(t)
}

func TestBootstrapFailureDuringJoinKeepsOriginalPathID(t *testing.T) {
	p := newPair(t, 2, 1, false, 5000)
	failed := p.client.controller.paths[0].binding
	result, err := p.client.controller.FailPath(p.now, failed)
	if err != nil || !result.PathsChanged || p.client.controller.NextDeadline().IsZero() {
		t.Fatalf("pending sibling join was abandoned: %+v %v", result, err)
	}
	p.drop = func(toServer bool, pk packet) bool {
		if toServer {
			return pk.binding.SocketID == failed.SocketID
		}
		return pk.binding.Remote == failed.Local
	}
	p.pump(t, func() bool {
		return p.client.controller.ready() && p.server.controller.ready() && p.client.controller.receiveAckSent && p.server.controller.receiveAckSent
	})
	if p.client.controller.setup.PathID != 1 || p.client.controller.contextPath != 2 || p.client.controller.paths[0].active {
		t.Fatal("context fallback renumbered bootstrap or reactivated failed Carrier")
	}
	receipt, _, err := p.client.controller.Write(p.now, []byte("joined after local failure"))
	if err != nil {
		t.Fatal(err)
	}
	p.pump(t, func() bool { return p.client.controller.completed == uint64(receipt) && len(p.server.deliveries) == 1 })
	p.close(t)
}
