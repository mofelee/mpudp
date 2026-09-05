package handshakev2

import (
	"bytes"
	"errors"
	"time"

	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func (e *Engine) receiveHello(now time.Time, binding Binding, packet []byte, envelope wirev2.AuthenticatedEnvelope, a *attempt, result *Result) (Result, error) {
	if len(packet) != wirev2.HandshakePacketSize {
		return *result, wirev2.ErrMalformed
	}
	if a != nil {
		if a.state == established || a.setup.Role != negotiationv2.Responder || a.setup.Binding != binding {
			return *result, nil
		}
		if !now.Before(a.deadline) {
			e.failAttempt(a, ErrExpired, now, result, true)
		} else {
			if bytes.Equal(packet, a.packets.hello[:]) {
				e.send(a, wirev2.TypeChallenge, a.packets.challenge[:], now, result, false)
			} else {
				e.reject(now, binding, packet, 2, result)
			}
			return *result, nil
		}
	}
	message, err := wirev2.DecodeHandshake(envelope)
	if err != nil {
		code := uint16(3)
		if errors.Is(err, wirev2.ErrRequiredTLV) {
			code = 1
		}
		e.reject(now, binding, packet, code, result)
		return *result, err
	}
	hello, err := negotiationv2.DecodeHello(message)
	if err != nil {
		e.reject(now, binding, packet, rejectCode(err), result)
		return *result, err
	}
	selected, contract, err := negotiationv2.Select(hello, e.config.Listener.Profile)
	if err != nil {
		e.reject(now, binding, packet, rejectCode(err), result)
		return *result, err
	}
	policy := *e.config.Listener
	scope, receiveLease, packetLease, initial, retirement, err := e.reserve(policy)
	if err != nil {
		e.reject(now, binding, packet, 4, result)
		return *result, err
	}
	a = &attempt{
		setup: Setup{ID: message.Header.SessionID, Role: negotiationv2.Responder, PathID: contract.BootstrapPathID, Binding: binding, Contract: contract, Scope: scope, Receive: policy.Receive, Initial: initial},
		state: waitFinish, policy: policy, hello: hello, deadline: now.Add(Lifetime), packets: new(packets), receiveLease: receiveLease, packetLease: packetLease, retirement: retirement,
	}
	copy(a.packets.hello[:], packet)
	challenge := wirev2.Handshake{Header: wirev2.Header{Type: wirev2.TypeChallenge, SessionID: a.setup.ID}, ClientNonce: message.ClientNonce}
	challenge.ServerNonce, err = e.randomBlock()
	if err == nil {
		challenge.ReturnPathToken, err = e.randomBlock()
	}
	if err == nil {
		challenge.TranscriptDigest, err = wirev2.HandshakeDigest(envelope)
	}
	if err == nil {
		challenge.TLVs, err = selected.TLVs()
	}
	if err == nil {
		err = encode(&a.packets.challenge, challenge, e.handshakeKey)
	}
	if err == nil {
		err = e.prepareTranscript(a)
	}
	if err != nil {
		e.disposeAttempt(a)
		return *result, err
	}
	e.sessions[a.setup.ID] = a
	e.pending++
	e.send(a, wirev2.TypeChallenge, a.packets.challenge[:], now, result, true)
	return *result, nil
}

func rejectCode(err error) uint16 {
	if errors.Is(err, negotiationv2.ErrUnsupportedCapability) {
		return 1
	}
	if errors.Is(err, negotiationv2.ErrIncompatible) {
		return 2
	}
	return 3
}

func (e *Engine) receiveFinish(now time.Time, a *attempt, packet []byte, envelope wirev2.AuthenticatedEnvelope, result *Result) error {
	if err := a.transcript.ValidateConfirmation(envelope); err != nil {
		return err
	}
	if !bytes.Equal(packet, a.packets.finish[:]) {
		return wirev2.ErrTranscript
	}
	if a.state == established {
		e.send(a, wirev2.TypeReady, a.packets.ready[:], now, result, false)
		return nil
	}
	if a.state != waitFinish {
		return nil
	}
	if err := e.install(a, result); err != nil {
		e.failAttempt(a, err, now, result, true)
		return err
	}
	e.send(a, wirev2.TypeReady, a.packets.ready[:], now, result, true)
	return nil
}

func (e *Engine) install(a *attempt, result *Result) error {
	if err := a.setup.Scope.Promote(); err != nil {
		return err
	}
	if e.config.InstallDeferred != nil {
		dispose, err := e.config.InstallDeferred(cloneSetup(a.setup))
		// Failure follows the same ownership handoff as successful retirement.
		// failAttempt closes the scope and invokes this disposer exactly once.
		a.disposeDeferred = dispose
		if err != nil || dispose == nil || a.setup.Scope.Snapshot().Closed {
			return ErrInstallation
		}
	} else if err := e.installSynchronous(a); err != nil {
		return err
	}
	if a.prepared != nil {
		a.prepared.clearStorage()
		a.prepared = nil
	}
	a.state = established
	e.pending--
	result.Established = append(result.Established, cloneSetup(a.setup))
	return nil
}

func (e *Engine) installSynchronous(a *attempt) error {
	dispose, err := e.config.Install(cloneSetup(a.setup))
	if err != nil {
		if dispose != nil {
			dispose()
		}
		return ErrInstallation
	}
	if dispose == nil {
		return ErrInstallation
	}
	if a.setup.Scope.Snapshot().Closed {
		dispose()
		return ErrInstallation
	}
	a.dispose = dispose
	return nil
}
