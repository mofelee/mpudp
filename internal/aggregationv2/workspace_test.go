package aggregationv2

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/fecv2"
)

func testWorkspace(t *testing.T, q *Queue, scope *creditv2.Session) (*OutputWorkspace, *creditv2.Lease) {
	t.Helper()
	p := q.epoch.Parameters
	charge, err := RequiredOutputWorkspaceBytes(p.DataShards+p.ParityShards, p.ShardBytes)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := scope.Reserve(creditv2.Claim{Bytes: charge})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := q.NewPrepaidOutputWorkspace(lease)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(workspace.Close)
	return workspace, lease
}

func TestOutputWorkspaceProgressAtFullCredit(t *testing.T) {
	q, peer, scope := testQueue(t, testLimits(), testEpoch())
	workspace, lease := testWorkspace(t, q, scope)
	now := time.Unix(100, 0)
	payload := bytes.Repeat([]byte{7}, 200)
	admit(t, q, payload, now)
	held, err := scope.Reserve(creditv2.Claim{Bytes: (1 << 20) - peer.Snapshot().Bytes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(held.Release)
	full := peer.Snapshot()
	if out, err := q.Seal(now, true); out != nil || !errors.Is(err, ErrResourceLimit) {
		t.Fatal("ordinary output unexpectedly fit at the exact byte ceiling")
	}
	var old Output
	var rebuilt []byte
	for group := 0; q.Snapshot().QueuedDatagrams != 0; group++ {
		out, err := q.SealWithWorkspace(now, true, workspace)
		if err != nil || out == nil {
			t.Fatalf("prepaid partial group %d failed: %v", group, err)
		}
		parts := decode(t, q.epoch, out)
		rebuilt = append(rebuilt, parts[0].Payload...)
		if group == 0 {
			old = *out
			before := q.Snapshot()
			if extra, err := q.SealWithWorkspace(now, true, workspace); extra != nil || !errors.Is(err, ErrResourceLimit) || q.Snapshot() != before {
				t.Fatal("busy workspace consumed another partial original")
			}
			if peer.Snapshot() != full || before.RetainedBytes != uint64(len(payload)) {
				t.Fatal("partial group released original backing or reserved output again")
			}
		} else {
			if _, live := old.View(); live {
				t.Fatal("reusing workspace revived an old copied output")
			}
			old.Release()
			if _, live := out.View(); !live {
				t.Fatal("stale copied Release invalidated the current output")
			}
		}
		view, _ := out.View()
		out.Release()
		for _, shard := range view.Group.Shards {
			if !bytes.Equal(shard, make([]byte, len(shard))) {
				t.Fatal("released output backing was not cleared")
			}
		}
		reservations := full.Reservations
		if q.Snapshot().QueuedDatagrams == 0 {
			reservations--
		}
		if lease.Snapshot().Released || peer.Snapshot().Reservations != reservations {
			t.Fatal("output Release returned the standing reservation")
		}
	}
	if !bytes.Equal(rebuilt, payload) {
		t.Fatal("successive full-credit groups changed the original")
	}
}

func TestOutputWorkspaceCloseRetainsLiveOutput(t *testing.T) {
	q, peer, scope := testQueue(t, testLimits(), testEpoch())
	workspace, lease := testWorkspace(t, q, scope)
	admit(t, q, []byte("owned until release"), time.Time{})
	out, err := q.SealWithWorkspace(time.Time{}, true, workspace)
	if err != nil {
		t.Fatal(err)
	}
	copyOfOutput, copyOfWorkspace := *out, *workspace
	view, _ := out.View()
	scope.Close()
	q.Close()
	var join sync.WaitGroup
	join.Add(2)
	go func() { defer join.Done(); workspace.Close() }()
	go func() { defer join.Done(); copyOfWorkspace.Close() }()
	join.Wait()
	if lease.Snapshot().Released || peer.Snapshot().Bytes != lease.Snapshot().Bytes {
		t.Fatal("workspace Close released a live output's claim")
	}
	if _, live := out.View(); !live {
		t.Fatal("workspace Close revoked output ownership")
	}
	join.Add(2)
	go func() { defer join.Done(); out.Release() }()
	go func() { defer join.Done(); copyOfOutput.Release() }()
	join.Wait()
	if !lease.Snapshot().Released || peer.Snapshot().Usage != (creditv2.Usage{}) || !scope.Snapshot().Retired {
		t.Fatal("last output release did not return workspace credit exactly once")
	}
	for _, shard := range view.Group.Shards {
		if !bytes.Equal(shard, make([]byte, len(shard))) {
			t.Fatal("last output release retained bytes")
		}
	}
}

func TestOutputWorkspaceBindingAndFailureAtomicity(t *testing.T) {
	q, _, scope := testQueue(t, testLimits(), testEpoch())
	p := q.epoch.Parameters
	charge, _ := RequiredOutputWorkspaceBytes(p.DataShards+p.ParityShards, p.ShardBytes)
	short, err := scope.Reserve(creditv2.Claim{Bytes: charge - 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(short.Release)
	before := scope.Snapshot()
	if workspace, err := q.NewPrepaidOutputWorkspace(short); workspace != nil || !errors.Is(err, creditv2.ErrInvalid) || scope.Snapshot() != before {
		t.Fatal("insufficient prepayment changed ownership")
	}
	if _, err := scope.BindBytes(short, charge-1); err != nil {
		t.Fatal("failed workspace constructor consumed caller lease")
	}
	workspace, lease := testWorkspace(t, q, scope)
	if second, err := q.NewPrepaidOutputWorkspace(lease); second != nil || !errors.Is(err, creditv2.ErrInvalid) {
		t.Fatal("workspace bound the same credit twice")
	}
	other, err := New(scope, testLimits(), testEpoch())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(other.Close)
	admit(t, other, []byte("wrong queue"), time.Time{})
	beforeQueue := other.Snapshot()
	if out, err := other.SealWithWorkspace(time.Time{}, true, workspace); out != nil || !errors.Is(err, ErrInvalid) || other.Snapshot() != beforeQueue {
		t.Fatal("foreign workspace advanced another queue")
	}
	admit(t, q, []byte("valid"), time.Time{})
	// A codec rejection after slot acquisition must make the slot reusable.
	id := q.ring[q.head].id
	q.ring[q.head].id = 0
	beforeQueue = q.Snapshot()
	if out, err := q.SealWithWorkspace(time.Time{}, true, workspace); out != nil || !errors.Is(err, fecv2.ErrInvalid) || q.Snapshot() != beforeQueue {
		t.Fatal("codec rejection consumed queue state")
	}
	q.ring[q.head].id = id
	out, err := q.SealWithWorkspace(time.Time{}, true, workspace)
	if err != nil {
		t.Fatal(err)
	}
	out.Release()
	workspace.Close()
	admit(t, q, []byte("closed"), time.Time{})
	beforeQueue = q.Snapshot()
	if out, err := q.SealWithWorkspace(time.Time{}, true, workspace); out != nil || !errors.Is(err, ErrClosed) || q.Snapshot() != beforeQueue {
		t.Fatal("closed workspace advanced queue state")
	}
}

func TestOrdinarySealStillAllowsMultipleLiveOutputs(t *testing.T) {
	q, _, scope := testQueue(t, testLimits(), testEpoch())
	workspace, _ := testWorkspace(t, q, scope)
	admit(t, q, bytes.Repeat([]byte{3}, 200), time.Time{})
	prepaid, err := q.SealWithWorkspace(time.Time{}, true, workspace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prepaid.Release)
	first := seal(t, q, time.Time{}, true)
	second := seal(t, q, time.Time{}, true)
	for _, out := range []*Output{prepaid, first, second} {
		if _, live := out.View(); !live {
			t.Fatal("sealing another output revoked an existing owner")
		}
	}
}

func TestOutputWorkspaceSizingCoversAllOwners(t *testing.T) {
	for _, dimensions := range [][2]int{{2, 1}, {5, 1106}, {256, fecv2.MaxShardBytes}} {
		shards, shardBytes := dimensions[0], dimensions[1]
		want := uint64(shards)*(uint64(shardBytes)+uint64(unsafe.Sizeof([]byte{}))) +
			uint64(unsafe.Sizeof(OutputWorkspace{})) + uint64(unsafe.Sizeof(outputWorkspaceState{})) +
			uint64(unsafe.Sizeof(Output{})) + uint64(unsafe.Sizeof(outputState{}))
		if got, err := RequiredOutputWorkspaceBytes(shards, shardBytes); err != nil || got != want {
			t.Fatalf("workspace charge = %d, want %d: %v", got, want, err)
		}
	}
	for _, dimensions := range [][2]int{{1, 1}, {257, 1}, {2, 0}, {2, fecv2.MaxShardBytes + 1}} {
		if _, err := RequiredOutputWorkspaceBytes(dimensions[0], dimensions[1]); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid workspace dimensions accepted")
		}
	}
	if allocs := testing.AllocsPerRun(100, func() { _, _ = RequiredOutputWorkspaceBytes(5, 1106) }); allocs != 0 {
		t.Fatalf("pre-handshake workspace sizing allocated %v times", allocs)
	}
}

func TestOutputRetainsWorkspaceStateAfterCallerReassignsHandle(t *testing.T) {
	q, _, scope := testQueue(t, testLimits(), testEpoch())
	workspace, firstLease := testWorkspace(t, q, scope)
	other, secondLease := testWorkspace(t, q, scope)
	retained := *workspace
	t.Cleanup(retained.Close)
	admit(t, q, bytes.Repeat([]byte{8}, 200), time.Time{})
	first, err := q.SealWithWorkspace(time.Time{}, true, workspace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(first.Release)
	second, err := q.SealWithWorkspace(time.Time{}, true, other)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(second.Release)
	*workspace = *other
	first.Release()
	retained.Close()
	if !firstLease.Snapshot().Released || secondLease.Snapshot().Released {
		t.Fatal("output released the reassigned wrapper's workspace")
	}
	before := q.Snapshot()
	if out, err := q.SealWithWorkspace(time.Time{}, true, other); out != nil || !errors.Is(err, ErrResourceLimit) || q.Snapshot() != before {
		t.Fatal("old output release freed another workspace's live slot")
	}
	*workspace = OutputWorkspace{}
	second.Release()
	other.Close()
	if !secondLease.Snapshot().Released {
		t.Fatal("zeroing a caller wrapper lost an output's retained owner")
	}
}
