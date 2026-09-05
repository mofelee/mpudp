package handshakev2

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

// NextDeadline returns the earliest retry or original expiry, including bounded
// rejection-cache expiry. Zero means no scheduled handshake work remains. A
// deadline at or before the caller's time needs one Advance call, not catch-up
// sends. Exhausted attempts still retain their original expiry. The caller
// serializes this read with all other Engine methods.
func (e *Engine) NextDeadline() time.Time {
	if e == nil || e.closed {
		return time.Time{}
	}
	var next time.Time
	include := func(deadline time.Time) {
		if next.IsZero() || deadline.Before(next) {
			next = deadline
		}
	}
	for _, a := range e.sessions {
		if a.state == established && a.packets == nil {
			continue
		}
		include(a.deadline)
		if a.sends >= MaxSends || ((a.state == waitChallenge || a.state == waitFinish) && a.sends >= MaxSends-1) {
			continue
		}
		include(a.nextSend)
	}
	for _, entry := range e.rejections {
		if entry.id != (wirev2.SessionID{}) {
			include(entry.deadline)
		}
	}
	return next
}

func (e *Engine) releasePackets(a *attempt) {
	if a.packets != nil {
		clear(a.packets.hello[:])
		clear(a.packets.challenge[:])
		clear(a.packets.finish[:])
		clear(a.packets.ready[:])
		a.packets = nil
	}
	a.packetLease.Release()
	a.packetLease = nil
	a.transcript = wirev2.Transcript{}
}

func (e *Engine) disposeAttempt(a *attempt) {
	a.setup.Scope.Close()
	if a.dispose != nil {
		dispose := a.dispose
		a.dispose = nil
		dispose()
	}
	e.releasePackets(a)
	a.setup.Keys = wirev2.DirectionalKeys{}
	if a.retirement != nil {
		storage, dispose := a.retirement, a.disposeDeferred
		a.retirement, a.disposeDeferred = nil, nil
		a.setup.Initial, a.receiveLease = nil, nil
		if dispose != nil {
			dispose(storage.release)
		} else {
			storage.release()
		}
		return
	}
	for _, lease := range a.setup.Initial {
		lease.Release()
	}
	a.setup.Initial = nil
	a.receiveLease.Release()
	a.receiveLease = nil
}

func (e *Engine) sendClose(a *attempt, cause error, result *Result) {
	if a.closeSent {
		return
	}
	key := a.setup.Keys.ClientToServer
	if a.setup.Role == negotiationv2.Responder {
		key = a.setup.Keys.ServerToClient
	}
	if key == (wirev2.Key{}) {
		return
	}
	a.closeSent = true
	var body [8]byte
	reason := uint16(6)
	if errors.Is(cause, ErrExpired) {
		reason = 5
	}
	binary.BigEndian.PutUint16(body[:2], reason)
	binary.BigEndian.PutUint32(body[4:], 1)
	packet, err := wirev2.AppendEstablished(nil, wirev2.Header{Type: wirev2.TypeClose, SessionID: a.setup.ID}, wirev2.Route{PathID: uint32(a.setup.PathID), Generation: 1, BudgetEpoch: 1}, body[:], key)
	if err == nil {
		err = e.config.Emit(a.setup.Binding, packet)
	}
	result.Sends = append(result.Sends, SendAttempt{ID: a.setup.ID, Type: wirev2.TypeClose, PathID: a.setup.PathID, Err: err})
	clear(packet)
}

func (e *Engine) failAttempt(a *attempt, cause error, now time.Time, result *Result, notify bool) {
	if e.sessions[a.setup.ID] != a {
		return
	}
	if notify {
		e.sendClose(a, cause, result)
	}
	delete(e.sessions, a.setup.ID)
	if a.state == established {
		result.Closed = append(result.Closed, a.setup.ID)
	} else {
		e.pending--
		if d := e.dials[a.setup.DialID]; d != nil {
			d.running--
		}
		result.Failures = append(result.Failures, Failure{ID: a.setup.ID, DialID: a.setup.DialID, Err: cause})
	}
	e.disposeAttempt(a)
}

func (e *Engine) receiveClose(now time.Time, a *attempt, envelope wirev2.AuthenticatedEnvelope, result *Result) (Result, error) {
	message, err := wirev2.DecodeEstablished(envelope)
	if err != nil {
		return *result, err
	}
	if message.Route != (wirev2.Route{PathID: uint32(a.setup.PathID), Generation: 1, BudgetEpoch: 1}) {
		return *result, wirev2.ErrInvalidRoute
	}
	if len(message.Body) != 8 || binary.BigEndian.Uint16(message.Body[2:4]) != 0 || binary.BigEndian.Uint32(message.Body[4:]) != 1 {
		return *result, wirev2.ErrMalformed
	}
	reason := binary.BigEndian.Uint16(message.Body[:2])
	if reason < 1 || reason > 9 {
		return *result, wirev2.ErrMalformed
	}
	e.failAttempt(a, ErrCancelled, now, result, false)
	e.fillDials(now, result)
	return *result, nil
}

// Advance performs due work once per retained attempt, without catch-up bursts.
// An exhausted send budget still permits matching responses before the original
// deadline. The first phase spends at most seven sends to preserve one final
// FINISH/READY transmission. Completed listener retry proofs expire at the same
// original deadline, independently of the installed Session lifetime.
func (e *Engine) Advance(now time.Time) (Result, error) {
	var result Result
	if err := e.enter(now, false); err != nil {
		return result, err
	}
	defer e.leave()
	for i, entry := range e.rejections {
		if !now.Before(entry.deadline) {
			e.rejections[i] = rejected{}
		}
	}
	for _, id := range e.orderedIDs() {
		a := e.sessions[id]
		if a.setup.Scope.Snapshot().Closed {
			e.failAttempt(a, ErrClosed, now, &result, false)
			continue
		}
		if !now.Before(a.deadline) {
			if a.state == established {
				e.releasePackets(a)
			} else {
				e.failAttempt(a, ErrExpired, now, &result, true)
			}
			continue
		}
		if a.packets == nil {
			continue
		}
		switch a.state {
		case waitChallenge:
			e.send(a, wirev2.TypeHello, a.packets.hello[:], now, &result, false)
		case waitFinish:
			e.send(a, wirev2.TypeChallenge, a.packets.challenge[:], now, &result, false)
		case waitReady:
			e.send(a, wirev2.TypeFinish, a.packets.finish[:], now, &result, false)
		case established:
			if a.setup.Role == negotiationv2.Responder {
				e.send(a, wirev2.TypeReady, a.packets.ready[:], now, &result, false)
			}
		}
	}
	e.fillDials(now, &result)
	return result, nil
}

// CancelDial cancels only this Dial and its attempts. At most one CLOSE per
// attempt is sent when directional keys exist. No CLOSE response is generated.
func (e *Engine) CancelDial(now time.Time, id DialID) (Result, error) {
	var result Result
	if err := e.enter(now, false); err != nil {
		return result, err
	}
	defer e.leave()
	if e.dials[id] == nil {
		return result, nil
	}
	delete(e.dials, id)
	for _, sessionID := range e.orderedIDs() {
		a := e.sessions[sessionID]
		if a.setup.DialID == id && a.state != established {
			e.failAttempt(a, ErrCancelled, now, &result, true)
		}
	}
	return result, nil
}

// MarkAccepted releases only the listener's reserved pending-accept count.
// Initial receive bytes remain owned until disposal at CloseSession/Close.
func (e *Engine) MarkAccepted(now time.Time, id wirev2.SessionID) error {
	if err := e.enter(now, false); err != nil {
		return err
	}
	defer e.leave()
	a := e.sessions[id]
	if a == nil || a.state != established || a.setup.Role != negotiationv2.Responder {
		return ErrInvalid
	}
	return a.receiveLease.MarkAccepted()
}

func (e *Engine) CloseSession(now time.Time, id wirev2.SessionID) (Result, error) {
	var result Result
	if err := e.enter(now, false); err != nil {
		return result, err
	}
	defer e.leave()
	if a := e.sessions[id]; a != nil {
		e.failAttempt(a, ErrCancelled, now, &result, true)
	}
	e.fillDials(now, &result)
	return result, nil
}

// Close clears protocol packets/keys and starts installed storage disposal.
// Synchronous installers finish disposal before return. Deferred installers
// retain initial credits until their releaseStorage callback runs; their adapter
// must join cleanup. Close does not close the shared credit Peer or any transport
// socket. It is idempotent at nondecreasing time.
func (e *Engine) Close(now time.Time) (Result, error) {
	var result Result
	if err := e.enter(now, true); err != nil {
		return result, err
	}
	defer e.leave()
	if e.closed {
		return result, nil
	}
	e.closed = true
	clear(e.dials)
	for _, id := range e.orderedIDs() {
		e.failAttempt(e.sessions[id], ErrCancelled, now, &result, true)
	}
	clear(e.rejections[:])
	clear(e.psk)
	e.psk = nil
	e.handshakeKey = wirev2.Key{}
	return result, nil
}
