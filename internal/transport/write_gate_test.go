package transport

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type writeWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *writeWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

type writeWaitResult struct {
	at  time.Time
	err error
}

func waitWriteSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("send did not reach the write boundary")
	}
}

func waitWriteResult(t *testing.T, results <-chan writeWaitResult) writeWaitResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(time.Second):
		t.Fatal("send did not return")
		return writeWaitResult{}
	}
}

func TestSendWriteWaitCancellationPreservesActiveWriter(t *testing.T) {
	for _, kind := range []string{"carrier", "captured-carrier", "listener"} {
		for _, cause := range []string{"cancel", "deadline"} {
			for _, timed := range []bool{false, true} {
				mode := "ordinary"
				if timed {
					mode = "timed"
				}
				t.Run(kind+"/"+cause+"/"+mode, func(t *testing.T) {
					f := newAttemptFixture(t, kind)
					var enabled atomic.Bool
					enabled.Store(true)
					f.counters.DiagnosticsEnabled = &enabled
					writing, release := make(chan struct{}), make(chan struct{})
					var releaseOnce sync.Once
					unblock := func() { releaseOnce.Do(func() { close(release) }) }
					defer unblock()
					var active, deadlines atomic.Int64
					var overlapping atomic.Bool
					f.conn.write = func(payload []byte) (int, error) {
						if active.Add(1) != 1 {
							overlapping.Store(true)
						}
						defer active.Add(-1)
						if string(payload) == "first" {
							close(writing)
							<-release
						}
						return len(payload), nil
					}
					f.conn.deadline = func(time.Time) error {
						deadlines.Add(1)
						return nil
					}
					first := make(chan writeWaitResult, 1)
					go func() {
						at, err := SendWithAttempt(context.Background(), f.path, []byte("first"))
						first <- writeWaitResult{at, err}
					}()
					waitWriteSignal(t, writing)

					var parent context.Context
					var cancel context.CancelFunc
					expected := context.Canceled
					if cause == "deadline" {
						parent, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
						expected = context.DeadlineExceeded
					} else {
						parent, cancel = context.WithCancel(context.Background())
					}
					defer cancel()
					waiting := &writeWaitContext{Context: parent, waiting: make(chan struct{})}
					second := make(chan writeWaitResult, 1)
					go func() {
						var result writeWaitResult
						if timed {
							result.at, result.err = SendWithAttempt(waiting, f.path, []byte("second"))
						} else {
							result.err = f.path.Send(waiting, []byte("second"))
						}
						second <- result
					}()
					waitWriteSignal(t, waiting.waiting)
					if cause == "cancel" {
						cancel()
					}
					got := waitWriteResult(t, second)
					if !got.at.IsZero() || !errors.Is(got.err, expected) {
						t.Fatalf("waiting send time=%v err=%v, want zero time and %v", got.at, got.err, expected)
					}
					if f.conn.writes.Load() != 1 || deadlines.Load() != 0 || active.Load() != 1 {
						t.Fatalf("waiting cancellation changed active I/O: writes=%d deadlines=%d active=%d", f.conn.writes.Load(), deadlines.Load(), active.Load())
					}
					if f.counters.SentPackets.Load() != 0 || f.counters.SendErrors.Load() != 0 || f.counters.SocketWrite.Snapshot().Count != 0 || f.counters.WriteQueue.Snapshot().Count != 0 {
						t.Fatal("waiting cancellation counted a socket attempt")
					}

					const followers = 4
					reused := make(chan writeWaitResult, followers)
					for range followers {
						ready := &writeWaitContext{Context: context.Background(), waiting: make(chan struct{})}
						go func() {
							at, err := SendWithAttempt(ready, f.path, []byte("reused"))
							reused <- writeWaitResult{at, err}
						}()
						waitWriteSignal(t, ready.waiting)
					}
					if f.conn.writes.Load() != 1 {
						t.Fatal("later send bypassed the active writer")
					}
					unblock()
					if got := waitWriteResult(t, first); got.err != nil || got.at.IsZero() {
						t.Fatalf("active send time=%v err=%v", got.at, got.err)
					}
					for range followers {
						if got := waitWriteResult(t, reused); got.err != nil || got.at.IsZero() {
							t.Fatalf("reused send time=%v err=%v", got.at, got.err)
						}
					}
					if overlapping.Load() || f.conn.writes.Load() != followers+1 || deadlines.Load() != followers+1 {
						t.Fatalf("write serialization lost: overlap=%t writes=%d deadline resets=%d", overlapping.Load(), f.conn.writes.Load(), deadlines.Load())
					}
					if f.counters.SentPackets.Load() != followers+1 || f.counters.SocketWrite.Snapshot().Count != followers+1 || f.counters.WriteQueue.Snapshot().Count != followers+1 {
						t.Fatal("completed sends lost queue or socket statistics")
					}
				})
			}
		}
	}
}
