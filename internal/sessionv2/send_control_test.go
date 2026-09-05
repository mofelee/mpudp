package sessionv2

import (
	"errors"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func deliverIntent(t *testing.T, p *pair, intent *SendIntent, toServer bool) {
	t.Helper()
	to := p.client
	if toServer {
		to = p.server
	}
	binding := opposite(intent.Binding, toServer)
	result, err := to.controller.Receive(p.now, binding, &testPath{binding}, intent.Packet)
	if err != nil {
		t.Fatal(err)
	}
	to.deliveries = append(to.deliveries, result.Deliveries...)
}

func TestOwnedJoinPhaseKeepsLateAttemptWithoutClearingNewFrame(t *testing.T) {
	p := newOwnedPair(t, 2, nil)
	c := p.client.controller
	c.controlCursor = 1
	join, _, err := c.TakeSend(p.now)
	if err != nil || join == nil || join.Type != wirev2.TypePathJoin {
		t.Fatalf("join: %+v %v", join, err)
	}
	state := &c.paths[1].join
	transaction, oldVersion := state.id, state.frame.version
	deliverIntent(t, p, join, true)
	p.server.controller.controlCursor = 1
	challenge, _, err := p.server.controller.TakeSend(p.now)
	if err != nil || challenge == nil || challenge.Type != wirev2.TypePathChallenge {
		t.Fatalf("challenge: %+v %v", challenge, err)
	}
	deliverIntent(t, p, challenge, false)
	if state.id != transaction || state.frame.version == oldVersion || !state.frame.queued || state.frame.dispatched || state.inflight != 1 {
		t.Fatal("phase transition lost the dispatched transaction")
	}
	version, next := state.frame.version, state.next
	completeIntent(t, p, join, nil)
	if state.sends != 1 || state.inflight != 0 || state.frame.version != version || !state.frame.queued || state.next != next {
		t.Fatal("late Join outcome changed Confirm frame or lost attempt count")
	}
	p.now = p.now.Add(time.Millisecond)
	c.controlCursor = 1
	confirm, _, err := c.TakeSend(p.now)
	if err != nil || confirm == nil || confirm.Type != wirev2.TypePathConfirm || confirm.Token == join.Token {
		t.Fatalf("confirm: %+v %v", confirm, err)
	}
}

func TestOwnedOverwrittenReplyRequiresCurrentCompletionForAckGate(t *testing.T) {
	p := newOwnedPair(t, 1, nil)
	p.pump(t, p.ready)
	p.now = p.now.Add(time.Millisecond)
	c, receiver := p.client.controller, p.server.controller
	if err := c.startEncoding(&c.paths[0], p.now); err != nil {
		t.Fatal(err)
	}
	context, _, err := c.TakeSend(p.now)
	if err != nil || context == nil || context.Type != wirev2.TypeEncodingContext {
		t.Fatalf("context: %+v %v", context, err)
	}
	receiver.receiveAckSent = false
	deliverIntent(t, p, context, true)
	ack, _, err := receiver.TakeSend(p.now)
	if err != nil || ack == nil || ack.Type != wirev2.TypeEncodingContextAck || receiver.receiveAckSent {
		t.Fatalf("ACK dispatch changed gate: %+v %v", ack, err)
	}
	old := receiver.paths[0].replies[1].version
	deliverIntent(t, p, context, true)
	if receiver.paths[0].replies[1].version == old {
		t.Fatal("repeated context reused reply identity")
	}
	ack.Release()
	if _, err := receiver.CompleteSend(p.now, SendOutcome{Token: ack.Token, Invoked: true, FinishedAt: p.now}); err != nil || receiver.receiveAckSent || !receiver.paths[0].replies[1].queued {
		t.Fatalf("old reply cleared current ACK: %v", err)
	}
	p.now = p.now.Add(time.Millisecond)
	ack, _, err = receiver.TakeSend(p.now)
	if err != nil || ack == nil || receiver.receiveAckSent {
		t.Fatal("current ACK was lost or treated as completed")
	}
	ack.Release()
	if _, err := receiver.CompleteSend(p.now, SendOutcome{Token: ack.Token, Invoked: true, FinishedAt: p.now}); err != nil || !receiver.receiveAckSent {
		t.Fatalf("current successful ACK did not open gate: %v", err)
	}
}

func TestOwnedContextMigrationSharesActualAttemptBudget(t *testing.T) {
	p := newOwnedPair(t, 2, nil)
	p.pump(t, p.ready)
	p.now = p.now.Add(time.Millisecond)
	c := p.client.controller
	if err := c.startEncoding(&c.paths[0], p.now); err != nil {
		t.Fatal(err)
	}
	c.controlCursor = 0
	first, _, err := c.TakeSend(p.now)
	if err != nil || first == nil || first.Type != wirev2.TypeEncodingContext {
		t.Fatalf("first context: %+v %v", first, err)
	}
	id, deadline := c.context.id, c.context.deadline
	if err := c.startEncoding(&c.paths[1], p.now); err != nil {
		t.Fatal(err)
	}
	c.controlCursor = 1
	second, _, err := c.TakeSend(p.now)
	if err != nil || second == nil || second.Type != wirev2.TypeEncodingContext || c.context.inflight != 2 {
		t.Fatalf("migrated context: %+v %v", second, err)
	}
	completeIntent(t, p, first, nil)
	if c.context.id != id || c.context.deadline != deadline || !c.context.frame.dispatched || c.context.inflight != 1 || c.context.sends != 1 {
		t.Fatal("old path completion lost migrated transaction accounting")
	}
	completeIntent(t, p, second, nil)
	if c.context.sends != 2 || c.context.inflight != 0 {
		t.Fatal("migrated actual attempt was not counted")
	}
}

func TestOwnedNativePreflightDoesNotConsumeControlAttempts(t *testing.T) {
	p := newOwnedPair(t, 1, nil)
	c := p.client.controller
	c.paths[0].nativeTiming = true
	intent, _, err := c.TakeSend(p.now)
	if err != nil || intent == nil || intent.Type != wirev2.TypePathBudgetUpdate {
		t.Fatalf("budget intent: %+v %v", intent, err)
	}
	intent.Release()
	if _, err := c.CompleteSend(p.now, SendOutcome{Token: intent.Token, Invoked: true, AttemptKnown: true, FinishedAt: p.now, Err: transport.ErrGenerationReplaced}); err != nil {
		t.Fatal(err)
	}
	state := &c.paths[0].budget
	if state.sends != 0 || state.inflight != 0 || state.next != p.now.Add(ControlRetry) {
		t.Fatal("native preflight consumed actual attempt budget or skipped backoff")
	}
	p.now = state.next.Add(c.cfg.MaxQueueResidence)
	result, err := c.Advance(p.now)
	if err != nil || len(result.Sends) != 1 || !errors.Is(result.Sends[0].Err, ErrSendExpired) || state.sends != 0 || state.next != p.now.Add(ControlRetry) {
		t.Fatalf("waiting retry expiry: %+v %v", result, err)
	}
}

func TestOwnedExpiredBudgetDropsInactiveReplyDeadlines(t *testing.T) {
	p := newOwnedPair(t, 2, nil)
	p.pump(t, p.ready)
	c := p.client.controller
	path := &c.paths[1]
	if err := c.startBudget(path, p.now); err != nil {
		t.Fatal(err)
	}
	packet := append([]byte(nil), path.budget.frame.packet[:path.budget.frame.length]...)
	if err := c.frameSet(&path.replies[0], packet, p.now); err != nil {
		t.Fatal(err)
	}
	p.now = path.budget.deadline
	if _, err := c.Advance(p.now); err != nil || path.active || path.replies[0].queued {
		t.Fatalf("expired path retained reply: %v", err)
	}
	if next := c.NextDeadline(); !next.IsZero() && !next.After(p.now) {
		t.Fatalf("inactive reply caused deadline loop: %v", next)
	}
}

func TestOwnedDeadlinesWithNoPeerSendCapacityPreserveMaintenance(t *testing.T) {
	p := newOwnedPair(t, 1, nil)
	c := p.client.controller
	if next := c.NextDeadlineWithSendCapacity(false); next != p.now.Add(c.cfg.MaxQueueResidence) {
		t.Fatalf("busy Peer lost control queue expiry: %v", next)
	}
	p.now = c.NextDeadlineWithSendCapacity(false)
	if _, err := c.Advance(p.now); err != nil {
		t.Fatal(err)
	}
	if next := c.NextDeadlineWithSendCapacity(false); next != p.now.Add(ControlRetry) {
		t.Fatalf("busy Peer lost retry preparation: %v", next)
	}
	p.now = c.NextDeadlineWithSendCapacity(false)
	if _, err := c.Advance(p.now); err != nil {
		t.Fatal(err)
	}
	if next := c.NextDeadlineWithSendCapacity(false); next != p.now.Add(c.cfg.MaxQueueResidence) {
		t.Fatalf("prepared retry spun dispatch deadline: %v", next)
	}
	p.pump(t, p.ready)
	p.now = p.now.Add(time.Millisecond)
	if _, _, err := c.Write(p.now, []byte("Peer capacity")); err != nil {
		t.Fatal(err)
	}
	if c.NextDeadline() != p.now || c.NextDeadlineWithSendCapacity(false) != p.now.Add(c.cfg.MaxQueueResidence) {
		t.Fatal("busy Peer failed to suppress DATA readiness or lost waiting expiry")
	}
}
