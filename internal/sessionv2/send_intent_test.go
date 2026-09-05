package sessionv2

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func ownedConfig(cfg *Config) {
	cfg.OwnedSends = true
	cfg.MaxInFlightSends = 32
	cfg.MaxPathQueuedPackets = 1
	cfg.MaxPathQueuedBytes = uint64(cfg.FixedPayloadBudget)
	cfg.MaxQueueResidence = 100 * time.Millisecond
}

func newOwnedPair(t *testing.T, paths int, adjust func(negotiationv2.Role, Config, *creditv2.Limits)) *pair {
	t.Helper()
	p := newPairWithConfig(t, profile(paths), profile(paths), 1, false, ownedConfig, adjust)
	t.Cleanup(func() {
		for _, e := range []*endpoint{p.client, p.server} {
			c := e.controller
			c.BeginClose()
			if c.sends != nil {
				for i := range c.sends.slots {
					slot := &c.sends.slots[i]
					if slot.token == 0 {
						continue
					}
					intent := &SendIntent{state: slot.owner}
					intent.Release()
					if _, err := c.CompleteSend(p.now, SendOutcome{Token: slot.token, FinishedAt: p.now, Err: io.ErrClosedPipe}); err != nil {
						t.Error(err)
					}
				}
			}
		}
		p.close(t)
	})
	return p
}

func (p *pair) sendOwned(t testing.TB, e *endpoint) {
	t.Helper()
	for step := 0; step < 256; step++ {
		intent, result, err := e.controller.TakeSend(p.now)
		if err != nil {
			t.Fatal(err)
		}
		e.deliveries = append(e.deliveries, result.Deliveries...)
		if intent == nil {
			return
		}
		err = e.controller.cfg.Emit(intent.Sender, intent.Packet)
		intent.Release()
		result, completeErr := e.controller.CompleteSend(p.now, SendOutcome{Token: intent.Token, Invoked: true, FinishedAt: p.now, Err: err})
		if completeErr != nil {
			t.Fatal(completeErr)
		}
		e.deliveries = append(e.deliveries, result.Deliveries...)
	}
	t.Fatal("unbounded owned send pump")
}

func takeData(t *testing.T, p *pair) *SendIntent {
	t.Helper()
	intent, _, err := p.client.controller.TakeSend(p.now)
	if err != nil || intent == nil || intent.Type != wirev2.TypeFECBundle {
		t.Fatalf("take DATA: %+v %v", intent, err)
	}
	return intent
}

func completeIntent(t *testing.T, p *pair, intent *SendIntent, failure error) Result {
	t.Helper()
	intent.Release()
	result, err := p.client.controller.CompleteSend(p.now, SendOutcome{Token: intent.Token, Invoked: true, FinishedAt: p.now, Err: failure})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestOwnedRoundTripAndNoImplicitEmit(t *testing.T) {
	p := newOwnedPair(t, 3, nil)
	for _, e := range []*endpoint{p.client, p.server} {
		if len(e.sent) != 0 || e.controller.PendingSends() != 0 {
			t.Fatal("Start invoked Emit or admitted a send")
		}
	}
	p.pump(t, p.ready)
	payload := bytes.Repeat([]byte{37}, 10000)
	if _, result, err := p.client.controller.Write(p.now, payload); err != nil || len(result.Sends) != 0 || result.CompletedThrough != 0 {
		t.Fatalf("Write acted as socket completion: %+v %v", result, err)
	}
	p.pump(t, func() bool { return p.client.controller.completed == 1 && len(p.server.deliveries) == 1 })
	if !bytes.Equal(p.server.deliveries[0].Payload(), payload) {
		t.Fatal("owned fragmented original changed")
	}
}

func TestOwnedTokensReleaseAndOutOfOrderGroupCompletion(t *testing.T) {
	p := newOwnedPair(t, 3, nil)
	p.pump(t, p.ready)
	p.now = p.now.Add(time.Millisecond)
	c := p.client.controller
	if _, _, err := c.Write(p.now, []byte("owned")); err != nil {
		t.Fatal(err)
	}
	intents := []*SendIntent{takeData(t, p), takeData(t, p), takeData(t, p)}
	if c.PendingSends() != 3 || c.completed != 0 {
		t.Fatal("dispatch completed the group")
	}
	if extra, _, err := c.TakeSend(p.now); extra != nil || err != nil {
		t.Fatal("busy path admitted another packet")
	}
	before := c.setup.Scope.Snapshot()
	for i, failure := range []error{ErrSendPending, ErrSendToken} {
		token := intents[0].Token
		if i == 1 {
			token += 1000
		}
		if _, err := c.CompleteSend(p.now, SendOutcome{Token: token, Invoked: true, FinishedAt: p.now}); !errors.Is(err, failure) || c.PendingSends() != 3 || c.setup.Scope.Snapshot() != before {
			t.Fatalf("invalid completion changed ownership: %v", err)
		}
	}
	borrowed := intents[2].Packet
	copyHandle := *intents[2]
	var releases sync.WaitGroup
	for range 8 {
		releases.Add(1)
		go func() {
			defer releases.Done()
			intents[2].Release()
		}()
	}
	releases.Wait()
	if c.PendingSends() != 3 || !bytes.Equal(borrowed, make([]byte, len(borrowed))) {
		t.Fatal("Release returned token capacity or retained packet bytes")
	}
	completeIntent(t, p, intents[2], nil)
	copyHandle.Release()
	if _, err := c.CompleteSend(p.now, SendOutcome{Token: copyHandle.Token, Invoked: true, FinishedAt: p.now}); !errors.Is(err, ErrSendToken) {
		t.Fatal("duplicate token was accepted")
	}
	completeIntent(t, p, intents[0], nil)
	completeIntent(t, p, intents[1], nil)
	if c.completed != 0 {
		t.Fatal("completed before waiting shards became terminal")
	}
	p.now = p.now.Add(time.Millisecond)
	last := []*SendIntent{takeData(t, p), takeData(t, p)}
	completeIntent(t, p, last[1], nil)
	if c.completed != 0 {
		t.Fatal("out-of-order outcome skipped live shard")
	}
	if result := completeIntent(t, p, last[0], nil); result.CompletedThrough != 1 || c.out != nil || c.PendingSends() != 0 {
		t.Fatal("last terminal shard did not complete original")
	}
}

func TestOwnedQueueCeilingAndWaitingExpiry(t *testing.T) {
	p := newOwnedPair(t, 1, nil)
	p.pump(t, p.ready)
	p.now = p.now.Add(time.Millisecond)
	c := p.client.controller
	if _, _, err := c.Write(p.now, []byte("expiry")); err != nil {
		t.Fatal(err)
	}
	intent := takeData(t, p)
	if len(intent.Packet) != 1200 || cap(intent.Packet) != 1200 || c.paths[0].queuedPackets != 1 || c.paths[0].queuedBytes != 1200 {
		t.Fatal("packet owner did not obey exact backing limits")
	}
	for shard, state := range c.sends.shards[:5] {
		if state == shardWaiting && c.outPaths[shard] != 0 {
			t.Fatal("waiting descriptor prematurely assigned a path")
		}
	}
	if due := c.NextDeadline(); !due.Equal(intent.ExpiresAt) {
		t.Fatalf("busy send generated spinning deadline: %v want %v", due, intent.ExpiresAt)
	}
	p.now = intent.ExpiresAt
	result, err := c.Advance(p.now)
	if err != nil || len(result.Sends) != 4 || c.sends.remaining != 1 || c.completed != 0 || !errors.Is(c.sticky, ErrSendExpired) {
		t.Fatalf("waiting expiry: %+v %v", result, err)
	}
	if !c.NextDeadline().IsZero() {
		t.Fatal("dispatched-only group retained immediate deadline")
	}
	result = completeIntent(t, p, intent, nil)
	if result.CompletedThrough != 1 || result.FailedFrom != 1 || !errors.Is(result.SendError, ErrSendExpired) {
		t.Fatal("last outcome lost prior queue failure")
	}
}

func TestOwnedCloseRetainsEveryInitialOwnerThroughCompletion(t *testing.T) {
	p := newOwnedPair(t, 2, nil)
	p.pump(t, p.ready)
	p.now = p.now.Add(time.Millisecond)
	c := p.client.controller
	if _, _, err := c.Write(p.now, bytes.Repeat([]byte{1}, 10000)); err != nil {
		t.Fatal(err)
	}
	a, b := takeData(t, p), takeData(t, p)
	scope := c.setup.Scope
	before := scope.Snapshot().Bytes
	borrowed := b.Packet
	c.BeginClose()
	scope.Close()
	if scope.Snapshot().Bytes != before || c.PendingSends() != 2 || !errors.Is(c.FinalizeClose(), ErrSendPending) {
		t.Fatal("BeginClose returned retained owner credit")
	}
	a.Release()
	if c.PendingSends() != 2 || scope.Snapshot().Bytes != before {
		t.Fatal("unconsumed completion returned ownership")
	}
	completeIntent(t, p, a, nil)
	if c.completed != 0 || scope.Snapshot().Bytes != before || borrowed[0] == 0 {
		t.Fatal("closing completion touched fence or another live packet")
	}
	completeIntent(t, p, b, io.ErrClosedPipe)
	if scope.Snapshot().Bytes != before || c.PendingSends() != 0 || c.completed != 0 {
		t.Fatal("last completion finalized implicitly")
	}
	if err := c.FinalizeClose(); err != nil || p.client.peer.Snapshot().Bytes != 0 {
		t.Fatalf("finalization retained owned bytes: %v", err)
	}
	// The shared pair cleanup needs the scope only to make its idempotent close.
	c.setup.Scope = scope
}

func TestOwnedActualAttemptPacingAndReplacement(t *testing.T) {
	for _, mode := range []string{"native", "custom", "preflight", "replacement"} {
		t.Run(mode, func(t *testing.T) {
			p := newOwnedPair(t, 1, nil)
			p.pump(t, p.ready)
			p.now = p.now.Add(time.Millisecond)
			c := p.client.controller
			path := &c.paths[0]
			path.rate = 1000000
			path.nativeTiming = mode != "custom"
			if _, _, err := c.Write(p.now, []byte("pacing")); err != nil {
				t.Fatal(err)
			}
			intent := takeData(t, p)
			start := p.now.Add(time.Millisecond)
			p.now = p.now.Add(20 * time.Millisecond)
			outcome := SendOutcome{Token: intent.Token, Invoked: true, AttemptKnown: intent.NativeTiming, StartedAt: start, FinishedAt: p.now}
			before := path.pacedAt
			if mode == "custom" || mode == "preflight" {
				outcome.StartedAt = time.Time{}
			}
			if mode == "preflight" {
				outcome.Err = transport.ErrGenerationReplaced
			}
			if mode == "replacement" {
				path.transportGeneration++
				binding := path.binding
				binding.SocketID = path.transportGeneration
				path.sender = &testPath{binding}
			}
			intent.Release()
			if _, err := c.CompleteSend(p.now, outcome); err != nil {
				t.Fatal(err)
			}
			want := start.Add(serializationTime(path.rate, intent.Binding, 1200))
			if mode == "custom" {
				want = p.now.Add(serializationTime(path.rate, intent.Binding, 1200))
			}
			if mode == "preflight" || mode == "replacement" {
				want = before
			}
			if path.pacedAt != want {
				t.Fatalf("pacing=%v want=%v", path.pacedAt, want)
			}
		})
	}
}

func TestOwnedIntentWrapperReassignmentCannotRedirectPrivateOwner(t *testing.T) {
	p := newOwnedPair(t, 2, nil)
	p.pump(t, p.ready)
	p.now = p.now.Add(time.Millisecond)
	c := p.client.controller
	if _, _, err := c.Write(p.now, []byte("private owners")); err != nil {
		t.Fatal(err)
	}
	a, b := takeData(t, p), takeData(t, p)
	old, borrowed := *a, a.Packet
	*a = *b
	old.Release()
	if _, err := c.CompleteSend(p.now, SendOutcome{Token: old.Token, Invoked: true, FinishedAt: p.now}); err != nil || c.PendingSends() != 1 || b.Packet[0] == 0 || !bytes.Equal(borrowed, make([]byte, len(borrowed))) {
		t.Fatalf("wrapper reassignment redirected token owner: %v", err)
	}
	completeIntent(t, p, b, nil)
	old.Release()
}

func TestOwnedInvalidOutcomesLeaveTokenAndClockUntouched(t *testing.T) {
	p := newOwnedPair(t, 1, nil)
	p.pump(t, p.ready)
	p.now = p.now.Add(time.Millisecond)
	c := p.client.controller
	if _, _, err := c.Write(p.now, []byte("outcomes")); err != nil {
		t.Fatal(err)
	}
	intent := takeData(t, p)
	for _, bad := range []SendOutcome{
		{Token: intent.Token, FinishedAt: p.now},
		{Token: intent.Token, Invoked: true, FinishedAt: p.now.Add(-time.Second)},
		{Token: intent.Token, Invoked: true, FinishedAt: p.now.Add(time.Second)},
		{Token: intent.Token, Invoked: true, AttemptKnown: true, StartedAt: p.now, FinishedAt: p.now},
		{Token: intent.Token, Invoked: true, StartedAt: p.now, FinishedAt: p.now},
		{Token: intent.Token, Err: io.EOF, StartedAt: p.now, FinishedAt: p.now},
	} {
		before := c.last
		if _, err := c.CompleteSend(p.now, bad); !errors.Is(err, ErrInvalid) || c.last != before || c.PendingSends() != 1 {
			t.Fatalf("invalid observation changed token or clock: %+v %v", bad, err)
		}
	}
	completeIntent(t, p, intent, nil)
}
