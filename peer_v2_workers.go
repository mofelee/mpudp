package mpudp

import (
	"context"
	"time"
	"unsafe"

	"github.com/mofelee/mpudp/internal/sessionv2"
	"github.com/mofelee/mpudp/internal/transport"
)

type v2SendJob struct {
	session *v2Session
	intent  *sessionv2.SendIntent
}

type v2SendCompletion struct {
	session *v2Session
	outcome sessionv2.SendOutcome
}

// Each busy slot owns exactly one job or unconsumed completion. A worker never
// takes the owner mutex, and its terminal result cannot compete with ingress.
type v2SendSlot struct {
	jobs    chan v2SendJob
	results chan v2SendCompletion
	done    chan struct{}
	busy    bool
}

func v2SendWorkerBytes(count int) uint64 {
	return uint64(count) * (uint64(unsafe.Sizeof(v2SendSlot{})) + uint64(unsafe.Sizeof(v2SendJob{})) + uint64(unsafe.Sizeof(v2SendCompletion{})) + 4096)
}

func (r *v2Peer) startSendWorkers() {
	r.sendSlots = make([]v2SendSlot, r.peer.config.Limits.MaxSendWorkers)
	r.sendWake = make(chan struct{}, 1)
	for i := range r.sendSlots {
		slot := &r.sendSlots[i]
		slot.jobs = make(chan v2SendJob, 1)
		slot.results = make(chan v2SendCompletion, 1)
		slot.done = make(chan struct{})
		go r.runSendWorker(slot)
	}
}

func (r *v2Peer) runSendWorker(slot *v2SendSlot) {
	defer close(slot.done)
	for job := range slot.jobs {
		outcome := executeV2Send(job.session.ctx, job.intent)
		completion := v2SendCompletion{session: job.session, outcome: outcome}
		job = v2SendJob{}
		slot.results <- completion
		completion = v2SendCompletion{}
		select {
		case r.sendWake <- struct{}{}:
		default:
		}
	}
}

func executeV2Send(parent context.Context, intent *sessionv2.SendIntent) sessionv2.SendOutcome {
	outcome := sessionv2.SendOutcome{Token: intent.Token}
	ctx, cancel := context.WithTimeout(parent, v2SocketAttemptTimeout)
	if err := ctx.Err(); err != nil {
		outcome.Err = err
	} else if !intent.ExpiresAt.IsZero() && !time.Now().Before(intent.ExpiresAt) {
		outcome.Err = sessionv2.ErrSendExpired
	} else {
		outcome.Invoked = true
		outcome.AttemptKnown = intent.NativeTiming
		outcome.StartedAt, outcome.Err = transport.SendWithAttempt(ctx, intent.Sender, intent.Packet)
	}
	outcome.FinishedAt = time.Now()
	cancel()
	intent.Release()
	return outcome
}

func (r *v2Peer) linkSendSession(s *v2Session) {
	if r.sendHead == nil {
		s.sendNext, s.sendPrev = s, s
		r.sendHead = s
		return
	}
	head := r.sendHead
	s.sendNext, s.sendPrev = head, head.sendPrev
	head.sendPrev.sendNext, head.sendPrev = s, s
}

func (r *v2Peer) unlinkSendSession(s *v2Session) {
	if s.sendNext == nil {
		return
	}
	if s.sendNext == s {
		r.sendHead = nil
	} else {
		s.sendPrev.sendNext, s.sendNext.sendPrev = s.sendNext, s.sendPrev
		if r.sendHead == s {
			r.sendHead = s.sendNext
		}
	}
	s.sendNext, s.sendPrev = nil, nil
}

func (r *v2Peer) dispatchSends() {
	if r.closed || r.peer.ctx.Err() != nil {
		return
	}
	for i := range r.sendSlots {
		slot := &r.sendSlots[i]
		if slot.busy {
			continue
		}
		remaining := len(r.sessions)
		for remaining > 0 && r.sendHead != nil {
			remaining--
			s := r.sendHead
			r.sendHead = s.sendNext
			if s.closed || s.controller == nil {
				continue
			}
			intent, result, err := s.controller.TakeSend(time.Now())
			if intent == nil {
				r.handleSession(s, result, err)
				continue
			}
			slot.busy = true
			s.activeSends++
			r.handleSession(s, result, err)
			slot.jobs <- v2SendJob{session: s, intent: intent}
			break
		}
		if !slot.busy {
			return
		}
	}
}

func (r *v2Peer) consumeSendCompletions() {
	for i := range r.sendSlots {
		slot := &r.sendSlots[i]
		select {
		case completion := <-slot.results:
			s := completion.session
			result, err := s.controller.CompleteSend(time.Now(), completion.outcome)
			s.activeSends--
			slot.busy = false
			if !s.closed {
				r.handleSession(s, result, err)
			} else {
				r.report("MPUDP v2 retiring send failed", err)
			}
			r.queueSessionCleanup(s)
		default:
		}
	}
}

func (r *v2Peer) joinSendWorkers() {
	for i := range r.sendSlots {
		close(r.sendSlots[i].jobs)
	}
	for i := range r.sendSlots {
		<-r.sendSlots[i].done
	}
}
