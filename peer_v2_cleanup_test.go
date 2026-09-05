package mpudp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/creditv2"
)

type v2CleanupTestCarrier struct {
	runtimeCarrier
	close func() error
}

func (c *v2CleanupTestCarrier) Close() error { return c.close() }

func TestV2CleanupRetainsStorageThroughConstructionAndBlockedCarrierClose(t *testing.T) {
	ledger, err := creditv2.New(creditv2.Limits{MaxPeerBytes: 4096, MaxSessionBytes: 4096, MaxSessions: 1,
		MaxPendingHandshakes: 1, MaxPendingAccepts: 1, MaxStreamsPerSession: 1, MaxPeerStreams: 1, MaxReservations: 8})
	if err != nil {
		t.Fatal(err)
	}
	scope, lease, err := ledger.BeginSession(creditv2.Claim{Bytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	scope.Close()
	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseClose := func() { once.Do(func() { close(unblock) }) }
	var closes, releases atomic.Int32
	carrier := &v2CleanupTestCarrier{close: func() error {
		closes.Add(1)
		close(entered)
		<-unblock
		return nil
	}}
	p := &Peer{ctx: context.Background(), wake: make(chan struct{}, 1)}
	r := &v2Peer{peer: p, credits: ledger, sendWake: make(chan struct{}, 1), sessions: make(map[*v2Session]struct{})}
	s := &v2Session{owner: r, closed: true, constructing: true, cleanupDone: make(chan struct{}),
		carriers: []runtimeCarrier{carrier}, carrierRetiring: []bool{false}, releaseStorage: func() {
			releases.Add(1)
			lease.Release()
		}}
	r.sessions[s] = struct{}{}
	r.startCleanupWorker()
	t.Cleanup(func() {
		releaseClose()
		r.joinCleanupWorker()
		lease.Release()
		ledger.Close()
	})
	r.mu.Lock()
	r.queueSessionCleanup(s)
	r.dispatchCleanup()
	if s.cleanupReady || r.cleanup.busy || releases.Load() != 0 {
		r.mu.Unlock()
		t.Fatal("cleanup started while construction still owned the wrapper")
	}
	s.constructing = false
	r.queueSessionCleanup(s)
	r.dispatchCleanup()
	// Carrier cleanup must enter even while the owner remains locked.
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		r.mu.Unlock()
		t.Fatal("cleanup waited for the owner mutex")
	}
	if usage := ledger.Snapshot(); usage.Bytes != 2048 || usage.SessionSlots != 1 || releases.Load() != 0 {
		r.mu.Unlock()
		t.Fatalf("blocked cleanup lost ownership: %+v releases=%d", usage, releases.Load())
	}
	if _, _, err := ledger.BeginSession(creditv2.Claim{Bytes: 1}); !errors.Is(err, creditv2.ErrResourceLimit) {
		r.mu.Unlock()
		t.Fatalf("admitted replacement before cleanup: %v", err)
	}
	r.mu.Unlock()
	releaseClose()
	select {
	case <-r.sendWake:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup did not publish completion")
	}
	r.mu.Lock()
	r.consumeCleanupCompletion()
	r.mu.Unlock()
	select {
	case <-s.cleanupDone:
	default:
		t.Fatal("cleanup join was not released")
	}
	if usage := ledger.Snapshot(); usage.Bytes != 0 || usage.SessionSlots != 0 || closes.Load() != 1 || releases.Load() != 1 || len(r.sessions) != 0 {
		t.Fatalf("incomplete cleanup: %+v closes=%d releases=%d sessions=%d", usage, closes.Load(), releases.Load(), len(r.sessions))
	}
}

func TestV2CleanupDoesNotReleaseBeforeActiveSendCompletes(t *testing.T) {
	r := &v2Peer{peer: &Peer{wake: make(chan struct{}, 1)}}
	var released bool
	s := &v2Session{closed: true, activeSends: 1, releaseStorage: func() { released = true }}
	r.queueSessionCleanup(s)
	if s.cleanupReady || released {
		t.Fatal("active send did not retain cleanup ownership")
	}
	s.activeSends = 0
	r.queueSessionCleanup(s)
	if !s.cleanupReady || released {
		t.Fatal("terminal send did not make cleanup ready while retaining storage")
	}
}
