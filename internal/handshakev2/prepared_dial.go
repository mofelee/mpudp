package handshakev2

import (
	"slices"
	"time"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

type preparedEngineIdentity struct{ marker byte }

type preparationPhase uint8

const (
	preparationWaiting preparationPhase = iota
	preparationAdopted
	preparationAborted
)

type preparedDialState struct {
	identity        *preparedEngineIdentity
	phase           preparationPhase
	policy          Policy
	scope           *creditv2.Session
	receive, packet *creditv2.Lease
	initial         []*creditv2.Lease
	storage         *retiredStorage
	packets         *packets
	disposePending  func(func())
}

// PrepareDial prepays one future serial dial before an adapter allocates its
// wrapper or opens sockets. It requires InstallDeferred. Failure never invokes
// disposePending. Success owns the normal Receive/Initial/packet/disposal claims
// plus PreparedDialBytes, and consumes pending and future Session capacity.
//
// Abort, terminal dial failure or Engine.Close invokes disposePending after
// closing the scope. The callback must stop the wrapper and retain releaseStorage
// until construction has joined and every owned socket/buffer is cleared. It runs
// synchronously under the serialized Engine caller and must not block or reenter
// Engine. The releaseStorage continuation is concurrent and idempotent.
func (e *Engine) PrepareDial(now time.Time, policy Policy, disposePending func(releaseStorage func())) (*PreparedDial, error) {
	if err := e.enter(now, false); err != nil {
		return nil, err
	}
	defer e.leave()
	if disposePending == nil || e.config.InstallDeferred == nil {
		return nil, ErrInvalid
	}
	if err := validatePolicy(policy, false); err != nil {
		return nil, err
	}
	scope, receive, packet, initial, storage, err := e.reserve(policy)
	if err != nil {
		return nil, err
	}
	metadata, err := scope.Reserve(creditv2.Claim{Bytes: PreparedDialBytes})
	if err != nil {
		scope.Close()
		packet.Release()
		storage.release()
		return nil, err
	}
	storage.preparedMetadata = metadata
	if e.preparationIdentity == nil {
		e.preparationIdentity = new(preparedEngineIdentity)
		e.preparations = make(map[*preparedDialState]struct{})
	}
	state := &preparedDialState{identity: e.preparationIdentity, policy: clonePolicy(policy),
		scope: scope, receive: receive, packet: packet, initial: initial, storage: storage,
		disposePending: disposePending}
	e.preparations[state] = struct{}{}
	return &PreparedDial{state: state}, nil
}

// BeginPreparedDial adopts a preparation exactly once and starts serial attempts
// in the configured Carrier order. Invalid carriers/deadline leave it unconsumed.
// After valid adoption, any startup failure consumes the handle and invokes its
// pending disposer before return, even if no attempt ID was published.
//
// Pre-promotion fallback reuses one scope and its leases with fresh protocol IDs
// and nonces. Installation failure is terminal, because promoted or bound storage
// cannot become pending admission again. Success hands ownership to InstallDeferred.
func (e *Engine) BeginPreparedDial(now time.Time, prepared *PreparedDial, carriers []Carrier, deadline time.Time) (DialID, Result, error) {
	var result Result
	if err := e.enter(now, false); err != nil {
		return 0, result, err
	}
	defer e.leave()
	state, err := e.waitingPreparation(prepared)
	if err != nil {
		return 0, result, err
	}
	if len(carriers) < 1 || len(carriers) > MaxPending || len(carriers) != int(state.policy.Profile.MaxPaths) {
		return 0, result, ErrInvalid
	}
	if !deadline.IsZero() && !now.Before(deadline) {
		return 0, result, ErrExpired
	}
	for i, carrier := range carriers {
		if carrier.PathID != uint16(i+1) || !validBinding(carrier.Binding) {
			return 0, result, ErrInvalid
		}
	}
	if state.scope.Snapshot().Closed {
		return 0, result, ErrClosed
	}
	if len(e.dials) >= MaxPending {
		return 0, result, creditv2.ErrResourceLimit
	}
	delete(e.preparations, state)
	state.phase = preparationAdopted
	id, err := e.nextDialID()
	if err != nil {
		state.retire(nil)
		return 0, result, err
	}
	d := &dial{id: id, prepared: state, request: DialRequest{
		Policy: state.policy, Carriers: slices.Clone(carriers), Concurrent: 1, Deadline: deadline,
	}}
	state.policy = Policy{}
	e.dials[id] = d
	err = e.fillDial(now, d, &result)
	if d.running == 0 {
		delete(e.dials, id)
		state.retire(nil)
		if err == nil {
			err = ErrInvalid
		}
		return 0, result, err
	}
	return id, result, nil
}

// AbortPreparedDial retires only an unadopted preparation. Repeated aborts of
// that preparation are harmless, including after Engine.Close. An adopted handle
// returns ErrInvalid; use CancelDial or CloseSession for its current owner.
func (e *Engine) AbortPreparedDial(now time.Time, prepared *PreparedDial) error {
	if err := e.enter(now, true); err != nil {
		return err
	}
	defer e.leave()
	if prepared == nil || prepared.state == nil || prepared.state.identity != e.preparationIdentity {
		return ErrInvalid
	}
	state := prepared.state
	if state.phase == preparationAborted {
		return nil
	}
	if state.phase != preparationWaiting {
		return ErrInvalid
	}
	if _, exists := e.preparations[state]; !exists {
		return ErrInvalid
	}
	delete(e.preparations, state)
	state.phase = preparationAborted
	state.retire(nil)
	return nil
}

func (e *Engine) waitingPreparation(prepared *PreparedDial) (*preparedDialState, error) {
	if prepared == nil || prepared.state == nil || prepared.state.identity != e.preparationIdentity ||
		prepared.state.phase != preparationWaiting {
		return nil, ErrInvalid
	}
	state := prepared.state
	if _, exists := e.preparations[state]; !exists {
		return nil, ErrInvalid
	}
	return state, nil
}

func (p *preparedDialState) retire(dispose func(func())) {
	if p.storage == nil {
		return
	}
	p.scope.Close()
	clearPackets(p.packets)
	p.packet.Release()
	if dispose == nil {
		dispose = p.disposePending
	}
	storage := p.storage
	p.clearStorage()
	dispose(storage.release)
}

func (p *preparedDialState) clearStorage() {
	p.policy = Policy{}
	p.scope, p.receive, p.packet, p.initial, p.storage, p.packets, p.disposePending = nil, nil, nil, nil, nil, nil, nil
}

func (e *Engine) recyclePreparedAttempt(a *attempt, now time.Time) bool {
	if a.prepared == nil || e.closed {
		return false
	}
	d := e.dials[a.setup.DialID]
	scope := a.setup.Scope.Snapshot()
	if d == nil || d.next >= len(d.request.Carriers) || scope.Closed || !scope.PendingHandshake ||
		(!d.request.Deadline.IsZero() && !now.Before(d.request.Deadline)) {
		return false
	}
	clearPreparedAttempt(a)
	return true
}

func clearPreparedAttempt(a *attempt) {
	clearPackets(a.packets)
	a.packets, a.packetLease, a.receiveLease, a.retirement, a.prepared, a.disposeDeferred = nil, nil, nil, nil, nil, nil
	a.setup.Scope, a.setup.Initial = nil, nil
	a.setup.Keys = wirev2.DirectionalKeys{}
	a.transcript = wirev2.Transcript{}
}
