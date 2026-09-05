package mpudp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/sessionv2"
	"github.com/mofelee/mpudp/internal/transport"
)

type v2WorkerTestPath struct {
	transport.ReplyPath
	send func(context.Context, []byte) error
}

func (p *v2WorkerTestPath) Send(ctx context.Context, packet []byte) error {
	return p.send(ctx, packet)
}

func TestExecuteV2SendCustomSuccessHasUnknownAttemptTime(t *testing.T) {
	payload := []byte("owned packet")
	var calls int
	path := &v2WorkerTestPath{send: func(ctx context.Context, packet []byte) error {
		calls++
		if ctx.Err() != nil || !bytes.Equal(packet, payload) {
			t.Fatalf("custom invocation context=%v packet=%q", ctx.Err(), packet)
		}
		return nil
	}}
	before := time.Now()
	outcome := executeV2Send(context.Background(), &sessionv2.SendIntent{
		Token: 7, Sender: path, Packet: payload,
	})
	if calls != 1 || outcome.Token != 7 || !outcome.Invoked || outcome.AttemptKnown ||
		!outcome.StartedAt.IsZero() || outcome.Err != nil || outcome.FinishedAt.Before(before) {
		t.Fatalf("custom send calls=%d outcome=%+v", calls, outcome)
	}
}

func TestExecuteV2SendCancellationAndExpiryDoNotInvokePath(t *testing.T) {
	for _, reason := range []string{"canceled", "parent-deadline", "queue-expired"} {
		t.Run(reason, func(t *testing.T) {
			ctx := context.Background()
			var cancel context.CancelFunc
			var expires time.Time
			var expected error
			switch reason {
			case "canceled":
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				expected = context.Canceled
			case "parent-deadline":
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				expected = context.DeadlineExceeded
			case "queue-expired":
				expires = time.Now().Add(-time.Second)
				expected = sessionv2.ErrSendExpired
			}
			var calls int
			path := &v2WorkerTestPath{send: func(context.Context, []byte) error {
				calls++
				return nil
			}}
			before := time.Now()
			outcome := executeV2Send(ctx, &sessionv2.SendIntent{
				Token: 9, Sender: path, Packet: []byte("unsent"), ExpiresAt: expires,
			})
			if calls != 0 || outcome.Token != 9 || outcome.Invoked || outcome.AttemptKnown ||
				!outcome.StartedAt.IsZero() || !errors.Is(outcome.Err, expected) || outcome.FinishedAt.Before(before) {
				t.Fatalf("pre-invocation calls=%d outcome=%+v, want %v", calls, outcome, expected)
			}
		})
	}
}

func TestExecuteV2SendActiveAttemptKeepsIndependentTimeout(t *testing.T) {
	var deadline, invoked time.Time
	var hasDeadline bool
	path := &v2WorkerTestPath{send: func(ctx context.Context, _ []byte) error {
		invoked = time.Now()
		deadline, hasDeadline = ctx.Deadline()
		<-ctx.Done()
		return ctx.Err()
	}}
	before := time.Now()
	expires := before.Add(3 * v2SocketAttemptTimeout / 4)
	outcome := executeV2Send(context.Background(), &sessionv2.SendIntent{
		Token: 11, Sender: path, Packet: []byte("active"), ExpiresAt: expires,
	})
	if !outcome.Invoked || !hasDeadline || !errors.Is(outcome.Err, context.DeadlineExceeded) {
		t.Fatalf("active attempt deadline=%v outcome=%+v", deadline, outcome)
	}
	if !invoked.Before(expires) || !deadline.After(expires) ||
		deadline.Before(before.Add(v2SocketAttemptTimeout)) || deadline.After(invoked.Add(v2SocketAttemptTimeout)) {
		t.Fatalf("execution deadline=%v invocation=%v queue expiry=%v start=%v", deadline, invoked, expires, before)
	}
	if outcome.AttemptKnown || !outcome.StartedAt.IsZero() || outcome.FinishedAt.Before(deadline) {
		t.Fatalf("unknown custom timeout outcome=%+v, deadline=%v", outcome, deadline)
	}
}

func newV2WorkerTestPool(t *testing.T, count int) (*v2Peer, context.Context, func() <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	peer := &Peer{ctx: ctx, diagnostics: make(chan error, 1)}
	peer.config.Limits.MaxSendWorkers = count
	r := &v2Peer{peer: peer, ingress: make(chan v2Ingress, 1)}
	r.startSendWorkers()
	joined := make(chan struct{})
	var once sync.Once
	join := func() <-chan struct{} {
		once.Do(func() {
			go func() {
				r.joinSendWorkers()
				close(joined)
			}()
		})
		return joined
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-join():
		case <-time.After(2 * time.Second):
			t.Error("send workers did not stop")
		}
	})
	return r, ctx, join
}

func TestV2SendWorkersProgressIndependentlyOfBlockedPathAndOwner(t *testing.T) {
	r, ctx, _ := newV2WorkerTestPool(t, 2)
	blocked, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	slow := &v2WorkerTestPath{send: func(context.Context, []byte) error {
		close(blocked)
		<-release
		return nil
	}}
	fast := &v2WorkerTestPath{send: func(context.Context, []byte) error { return nil }}
	slowSession, fastSession := &v2Session{ctx: ctx}, &v2Session{ctx: ctx}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.sendSlots[0].busy, r.sendSlots[1].busy = true, true
	r.sendSlots[0].jobs <- v2SendJob{session: slowSession, intent: &sessionv2.SendIntent{Token: 1, Sender: slow}}
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("first worker did not enter its path while owner was locked")
	}
	r.sendSlots[1].jobs <- v2SendJob{session: fastSession, intent: &sessionv2.SendIntent{Token: 2, Sender: fast}}
	select {
	case completion := <-r.sendSlots[1].results:
		if completion.session != fastSession || completion.outcome.Token != 2 ||
			!completion.outcome.Invoked || completion.outcome.Err != nil {
			t.Fatalf("independent completion=%+v", completion)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked path or owner lock prevented another worker from completing")
	}
	select {
	case completion := <-r.sendSlots[0].results:
		t.Fatalf("blocked path completed before release: %+v", completion)
	default:
	}
	unblock()
	select {
	case completion := <-r.sendSlots[0].results:
		if completion.session != slowSession || completion.outcome.Token != 1 || !completion.outcome.Invoked {
			t.Fatalf("released completion=%+v", completion)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("released path did not publish its completion")
	}
}

func TestV2SendWorkerMailboxesRemainReliableWithCoalescedWake(t *testing.T) {
	for _, count := range []int{1, 8} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			r, ctx, join := newV2WorkerTestPool(t, count)
			r.sendWake <- struct{}{}
			r.ingress <- v2Ingress{}
			r.peer.diagnostics <- io.ErrUnexpectedEOF
			var calls atomic.Int32
			sessions := make([]*v2Session, count)
			for i := range r.sendSlots {
				sessions[i] = &v2Session{ctx: ctx}
				var expected error
				if i%2 != 0 {
					expected = io.ErrClosedPipe
				}
				path := &v2WorkerTestPath{send: func(context.Context, []byte) error {
					calls.Add(1)
					return expected
				}}
				slot := &r.sendSlots[i]
				if cap(slot.jobs) != 1 || cap(slot.results) != 1 {
					t.Fatal("worker does not have one bounded job and completion slot")
				}
				slot.busy = true
				slot.jobs <- v2SendJob{session: sessions[i], intent: &sessionv2.SendIntent{Token: 3, Sender: path}}
			}
			// Join before consuming results: every terminal outcome must fit even
			// when shutdown cannot consume another wake or diagnostic event.
			select {
			case <-join():
			case <-time.After(2 * time.Second):
				t.Fatal("full completion or wake mailboxes prevented worker shutdown")
			}
			if calls.Load() != int32(count) || len(r.sendWake) != 1 || len(r.ingress) != 1 || len(r.peer.diagnostics) != 1 {
				t.Fatalf("calls=%d wake=%d ingress=%d diagnostics=%d", calls.Load(), len(r.sendWake), len(r.ingress), len(r.peer.diagnostics))
			}
			for i := range r.sendSlots {
				slot := &r.sendSlots[i]
				if !slot.busy || len(slot.results) != 1 {
					t.Fatalf("slot %d lost its unconsumed obligation: busy=%t results=%d", i, slot.busy, len(slot.results))
				}
				completion := <-slot.results
				var expected error
				if i%2 != 0 {
					expected = io.ErrClosedPipe
				}
				if completion.session != sessions[i] || completion.outcome.Token != 3 || !completion.outcome.Invoked ||
					completion.outcome.AttemptKnown || !completion.outcome.StartedAt.IsZero() ||
					completion.outcome.FinishedAt.IsZero() || !errors.Is(completion.outcome.Err, expected) {
					t.Fatalf("slot %d completion=%+v, want error %v", i, completion, expected)
				}
				select {
				case extra := <-slot.results:
					t.Fatalf("slot %d published a duplicate completion: %+v", i, extra)
				default:
				}
			}
		})
	}
}
