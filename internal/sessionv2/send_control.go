package sessionv2

import (
	"time"

	"github.com/mofelee/mpudp/internal/handshakev2"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
)

type ownedControl struct {
	p          *pathState
	frame      *controlFrame
	retry      *controlRetry
	sender     transport.ReplyPath
	binding    handshakev2.Binding
	generation uint64
	native     bool
	source     sendSource
	limit      int
}

func (c *Controller) controlAt(p *pathState, source sendSource) ownedControl {
	value := ownedControl{p: p, sender: p.sender, binding: p.binding, generation: p.transportGeneration, native: p.nativeTiming, source: source, limit: ControlSends}
	if source == sendJoin {
		if !p.join.pending {
			return ownedControl{}
		}
		value.frame, value.retry = &p.join.frame, &p.join.controlRetry
		value.sender, value.binding, value.generation, value.native = p.join.sender, p.join.binding, p.join.transportGeneration, p.join.nativeTiming
		if p.join.kind == wirev2.TypePathJoin || p.join.kind == wirev2.TypePathChallenge {
			value.limit--
		}
		return value
	}
	if !p.active {
		return ownedControl{}
	}
	switch source {
	case sendBudget:
		value.frame, value.retry = &p.budget.frame, &p.budget
	case sendContext:
		if p.id != c.contextPath {
			return ownedControl{}
		}
		value.frame, value.retry = &c.context.frame, &c.context
	case sendReply0:
		value.frame = &p.replies[0]
	case sendReply1:
		value.frame = &p.replies[1]
	}
	return value
}

func (c *Controller) visitOwnedControls(visit func(ownedControl) bool) {
	for offset := range c.paths {
		p := &c.paths[(c.controlCursor+offset)%len(c.paths)]
		for _, source := range [...]sendSource{sendJoin, sendReply0, sendReply1, sendBudget, sendContext} {
			value := c.controlAt(p, source)
			if value.frame != nil && visit(value) {
				return
			}
		}
	}
}

func (c *Controller) prepareOwnedControls(now time.Time, result *Result) error {
	// Rotate a due context retry once, when its next frame becomes queued.
	if state := &c.context; state.pending && !state.frame.queued && !state.frame.dispatched && state.sends+state.inflight < ControlSends && !now.Before(state.next) && now.Before(state.deadline) {
		due := state.next
		for offset := range c.paths {
			p := &c.paths[(int(c.contextPath)+offset)%len(c.paths)]
			if c.eligible(p) {
				if err := c.startEncoding(p, now); err != nil {
					return err
				}
				c.context.frame.queuedAt = due
				break
			}
		}
	}
	c.visitOwnedControls(func(value ownedControl) bool {
		frame, retry := value.frame, value.retry
		if retry != nil {
			if !retry.pending || !now.Before(retry.deadline) || retry.sends+retry.inflight >= value.limit || now.Before(retry.next) {
				return false
			}
			if !frame.queued && !frame.dispatched {
				frame.queued, frame.queuedAt = true, retry.next
			}
		}
		if !frame.queued || frame.dispatched || now.Before(frame.queuedAt.Add(c.cfg.MaxQueueResidence)) {
			return false
		}
		if len(result.Sends) == MaxSendsPerStep {
			return true
		}
		c.failQueuedControl(value, now, ErrSendExpired, result)
		return false
	})
	return nil
}

func (c *Controller) failQueuedControl(value ownedControl, now time.Time, failure error, result *Result) {
	result.Sends = append(result.Sends, SendAttempt{Type: wirev2.PacketType(value.frame.packet[5]), PathID: value.p.id, Bytes: value.frame.length, Err: failure})
	value.frame.queued = false
	if value.retry != nil {
		value.retry.next = now.Add(ControlRetry)
	}
}

func (c *Controller) takeOwnedControl(now time.Time, slot *sendSlot, result *Result) (intent *SendIntent, err error) {
	c.visitOwnedControls(func(value ownedControl) bool {
		frame, retry, p := value.frame, value.retry, value.p
		if frame.dispatched || !frame.queued || frame.length == 0 || !now.Before(frame.queuedAt.Add(c.cfg.MaxQueueResidence)) || !c.pathCanDispatch(p, now, frame.length) {
			return false
		}
		if retry != nil && (!retry.pending || !now.Before(retry.deadline) || now.Before(retry.next) || retry.sends+retry.inflight >= value.limit) {
			return false
		}
		if value.sender == nil || !value.sender.Available() || value.sender.Generation() != value.generation {
			if len(result.Sends) < MaxSendsPerStep {
				c.failQueuedControl(value, now, transport.ErrGenerationReplaced, result)
			}
			return len(result.Sends) == MaxSendsPerStep
		}
		packet := make([]byte, frame.length)
		copy(packet, frame.packet[:frame.length])
		envelope, parseErr := wirev2.ParseEnvelope(packet)
		if parseErr != nil {
			clear(packet)
			err = parseErr
			return true
		}
		authenticated, authErr := c.sendAuth.Authenticate(envelope)
		if authErr != nil {
			clear(packet)
			err = authErr
			return true
		}
		message, decodeErr := wirev2.DecodeEstablished(authenticated)
		if decodeErr != nil {
			clear(packet)
			err = decodeErr
			return true
		}
		*slot = sendSlot{pathID: p.id, pathGeneration: p.generation, transportGeneration: value.generation, binding: value.binding, route: message.Route, native: value.native, source: value.source, version: frame.version, issued: now, expires: frame.queuedAt.Add(c.cfg.MaxQueueResidence)}
		if retry != nil {
			slot.transaction = retry.id
			retry.inflight++
		}
		frame.dispatched, frame.queued = true, false
		intent = c.installIntent(slot, p, value.sender, packet)
		c.controlCursor = int(p.id) % len(c.paths)
		return true
	})
	return intent, err
}

func (c *Controller) completeOwnedControl(slot *sendSlot, outcome SendOutcome) {
	p := &c.paths[slot.pathID-1]
	var frame *controlFrame
	var retry *controlRetry
	switch slot.source {
	case sendJoin:
		retry = &p.join.controlRetry
	case sendBudget:
		retry = &p.budget
	case sendContext:
		retry = &c.context
	case sendReply0:
		frame = &p.replies[0]
	case sendReply1:
		frame = &p.replies[1]
	}
	if retry != nil {
		if retry.id != slot.transaction {
			return
		}
		retry.inflight--
		if outcome.Invoked && (!outcome.AttemptKnown || !outcome.StartedAt.IsZero()) {
			retry.sends++
		}
		frame = &retry.frame
	}
	if frame == nil || frame.version != slot.version {
		return
	}
	frame.dispatched = false
	if retry != nil {
		retry.next = outcome.FinishedAt.Add(ControlRetry)
	}
	if slot.kind == wirev2.TypeEncodingContextAck && outcome.Invoked && outcome.Err == nil && p.generation == slot.route.Generation && p.binding == slot.binding && p.transportGeneration == slot.transportGeneration {
		c.receiveAckSent = true
	}
}
