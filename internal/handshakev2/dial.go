package handshakev2

import (
	"bytes"
	"slices"
	"time"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

// BeginDial tries the configured Carrier order without renumbering any path.
// Each independent attempt gets its own fresh SessionID/client nonce and
// reserved future Session. The first successfully installed READY wins.
func (e *Engine) BeginDial(now time.Time, request DialRequest) (DialID, Result, error) {
	var result Result
	if err := e.enter(now, false); err != nil {
		return 0, result, err
	}
	defer e.leave()
	if err := validatePolicy(request.Policy, false); err != nil {
		return 0, result, err
	}
	if len(request.Carriers) < 1 || len(request.Carriers) > MaxPending || len(request.Carriers) != int(request.Policy.Profile.MaxPaths) {
		return 0, result, ErrInvalid
	}
	if request.Concurrent == 0 {
		request.Concurrent = 1
	}
	if request.Concurrent < 1 || request.Concurrent > len(request.Carriers) {
		return 0, result, ErrInvalid
	}
	if !request.Deadline.IsZero() && !now.Before(request.Deadline) {
		return 0, result, ErrExpired
	}
	for i, carrier := range request.Carriers {
		if carrier.PathID != uint16(i+1) || !validBinding(carrier.Binding) {
			return 0, result, ErrInvalid
		}
	}
	if len(e.dials) >= MaxPending {
		return 0, result, creditv2.ErrResourceLimit
	}
	id, err := e.nextDialID()
	if err != nil {
		return 0, result, err
	}
	request.Carriers = slices.Clone(request.Carriers)
	request.Policy = clonePolicy(request.Policy)
	d := &dial{id: id, request: request}
	e.dials[id] = d
	err = e.fillDial(now, d, &result)
	if d.running == 0 {
		delete(e.dials, id)
		if err == nil {
			err = ErrInvalid
		}
		return 0, result, err
	}
	return id, result, nil
}

func (e *Engine) startAttempt(now time.Time, d *dial, carrier Carrier, result *Result) error {
	id, err := e.newID()
	if err != nil {
		return err
	}
	nonce, err := e.randomBlock()
	if err != nil {
		return err
	}
	policy := d.request.Policy
	hello := negotiationv2.Advertisement{Profile: policy.Profile, BootstrapPathID: carrier.PathID}
	tlvs, err := hello.TLVs()
	if err != nil {
		return err
	}
	var a *attempt
	if prepared := d.prepared; prepared != nil {
		if prepared.scope == nil || prepared.scope.Snapshot().Closed {
			return ErrClosed
		}
		if prepared.packets == nil {
			prepared.packets = new(packets)
		}
		a = &attempt{setup: Setup{Scope: prepared.scope, Receive: policy.Receive, Initial: prepared.initial},
			receiveLease: prepared.receive, packetLease: prepared.packet, retirement: prepared.storage,
			packets: prepared.packets, prepared: prepared}
	} else {
		scope, receiveLease, packetLease, initial, retirement, err := e.reserve(policy)
		if err != nil {
			return err
		}
		a = &attempt{setup: Setup{Scope: scope, Receive: policy.Receive, Initial: initial},
			receiveLease: receiveLease, packetLease: packetLease, retirement: retirement, packets: new(packets)}
	}
	deadline := now.Add(Lifetime)
	if !d.request.Deadline.IsZero() && d.request.Deadline.Before(deadline) {
		deadline = d.request.Deadline
	}
	a.setup.ID, a.setup.DialID, a.setup.Role, a.setup.PathID, a.setup.Binding = id, d.id, negotiationv2.Initiator, carrier.PathID, carrier.Binding
	a.state, a.policy, a.hello, a.deadline = waitChallenge, policy, hello, deadline
	message := wirev2.Handshake{Header: wirev2.Header{Type: wirev2.TypeHello, SessionID: id}, ClientNonce: nonce, TLVs: tlvs}
	if err := encode(&a.packets.hello, message, e.handshakeKey); err != nil {
		e.disposeAttempt(a)
		return err
	}
	e.sessions[id] = a
	e.pending++
	d.running++
	e.send(a, wirev2.TypeHello, a.packets.hello[:], now, result, true)
	return nil
}

func (e *Engine) fillDial(now time.Time, d *dial, result *Result) error {
	if !d.request.Deadline.IsZero() && !now.Before(d.request.Deadline) {
		return ErrExpired
	}
	for d.running < d.request.Concurrent && d.next < len(d.request.Carriers) {
		if err := e.startAttempt(now, d, d.request.Carriers[d.next], result); err != nil {
			return err
		}
		d.next++
	}
	return nil
}

func (e *Engine) fillDials(now time.Time, result *Result) {
	ids := make([]DialID, 0, len(e.dials))
	for id := range e.dials {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		d := e.dials[id]
		err := e.fillDial(now, d, result)
		if d.running == 0 {
			delete(e.dials, id)
			if d.prepared != nil {
				d.prepared.retire(nil)
			}
			if err != nil {
				result.Failures = append(result.Failures, Failure{DialID: id, Err: err})
			}
		}
	}
}

func (e *Engine) receiveChallenge(now time.Time, a *attempt, packet []byte, envelope wirev2.AuthenticatedEnvelope, result *Result) error {
	if a.state == waitReady {
		if bytes.Equal(packet, a.packets.challenge[:]) {
			e.send(a, wirev2.TypeFinish, a.packets.finish[:], now, result, false)
		}
		return nil
	}
	if a.state != waitChallenge {
		return nil
	}
	message, err := wirev2.DecodeHandshake(envelope)
	if err != nil {
		return err
	}
	selected, err := negotiationv2.DecodeChallenge(message)
	if err != nil {
		return err
	}
	contract, err := negotiationv2.Accept(a.hello, selected)
	if err != nil {
		return err
	}
	// Validate in temporary reserved packet storage before publishing new keys.
	copy(a.packets.challenge[:], packet)
	if err := e.prepareTranscript(a); err != nil {
		clear(a.packets.challenge[:])
		clear(a.packets.finish[:])
		clear(a.packets.ready[:])
		a.transcript = wirev2.Transcript{}
		a.setup.Keys = wirev2.DirectionalKeys{}
		return err
	}
	a.setup.Contract = contract
	a.state = waitReady
	e.send(a, wirev2.TypeFinish, a.packets.finish[:], now, result, true)
	return nil
}

func (e *Engine) receiveReject(now time.Time, a *attempt, envelope wirev2.AuthenticatedEnvelope, result *Result) error {
	if a.state != waitChallenge {
		return nil
	}
	hello, err := authenticate(a.packets.hello[:], e.handshakeKey)
	if err != nil {
		return err
	}
	if err := wirev2.ValidateReject(hello, envelope); err != nil {
		return err
	}
	e.failAttempt(a, ErrRejected, now, result, false)
	return nil
}

func (e *Engine) receiveReady(now time.Time, a *attempt, packet []byte, envelope wirev2.AuthenticatedEnvelope, result *Result) error {
	if a.state != waitReady {
		return nil
	}
	if err := a.transcript.ValidateConfirmation(envelope); err != nil {
		return err
	}
	if !bytes.Equal(packet, a.packets.ready[:]) {
		return wirev2.ErrTranscript
	}
	if err := e.install(a, result); err != nil {
		e.failAttempt(a, err, now, result, true)
		return err
	}
	if d := e.dials[a.setup.DialID]; d != nil {
		delete(e.dials, d.id)
		for _, id := range e.orderedIDs() {
			other := e.sessions[id]
			if other != a && other.setup.DialID == d.id {
				e.failAttempt(other, ErrCancelled, now, result, true)
			}
		}
	}
	e.releasePackets(a)
	return nil
}
