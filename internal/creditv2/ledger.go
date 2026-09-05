package creditv2

import (
	"errors"
	"fmt"
	"sync"
)

const (
	MaxRetainedBytes = 1 << 30
	MaxReservations  = 1 << 20
)

var (
	ErrInvalid       = errors.New("invalid v2 credit operation")
	ErrResourceLimit = errors.New("v2 credit resource limit reached")
	ErrClosed        = errors.New("v2 credit scope closed")
	ErrReleased      = errors.New("v2 credit lease released")
)

// Limits are ceilings, not preallocations. Byte ceilings may be smaller than
// public configuration minima for internal callers. All limits are explicit.
// MaxReservations bounds live lease metadata, including count-only leases;
// Session metadata is independently bounded by MaxSessions reserved slots.
type Limits struct {
	MaxPeerBytes         uint64
	MaxSessionBytes      uint64
	MaxSessions          int
	MaxPendingHandshakes int
	MaxPendingAccepts    int
	MaxStreamsPerSession int
	MaxPeerStreams       int
	MaxReservations      int
}

// Claim reserves owned bytes and optionally one business stream and/or one
// pending application accept. Reserve the entire receive-window obligation
// before advertising it. A reserved control stream uses its own byte lease
// with BusinessStream false, so business admission cannot consume that floor.
type Claim struct {
	Bytes          uint64
	BusinessStream bool
	PendingAccept  bool
}

type Usage struct {
	Bytes           uint64
	Reservations    int
	BusinessStreams int
	PendingAccepts  int
}

type PeerSnapshot struct {
	Usage
	SessionSlots        int
	PendingHandshakes   int
	EstablishedSessions int
	Closed              bool
}

type SessionSnapshot struct {
	Usage
	PendingHandshake bool
	Closed           bool
	Retired          bool
}

type LeaseSnapshot struct {
	Claim
	Released bool
}

// Handles may be copied; copies share private state and cannot double-release.
type Peer struct{ state *peerState }
type Session struct{ state *sessionState }
type Lease struct{ state *leaseState }

type peerState struct {
	mu       sync.Mutex
	limits   Limits
	usage    PeerSnapshot
	sessions map[*sessionState]struct{}
}

type sessionState struct {
	peer             *peerState
	usage            Usage
	pendingHandshake bool
	closed, retired  bool
}

type leaseState struct {
	peer           *peerState
	owner          *sessionState
	claim          Claim
	acceptReserved bool
	bound          bool
	released       bool
}

func New(limits Limits) (*Peer, error) {
	if limits.MaxPeerBytes == 0 || limits.MaxPeerBytes > MaxRetainedBytes || limits.MaxSessionBytes == 0 || limits.MaxSessionBytes > limits.MaxPeerBytes {
		return nil, invalid("byte limits outside bounds")
	}
	if limits.MaxSessions < 1 || limits.MaxSessions > 65536 || limits.MaxPendingHandshakes < 1 || limits.MaxPendingHandshakes > 4096 || limits.MaxPendingAccepts < 1 || limits.MaxPendingAccepts > 65536 {
		return nil, invalid("admission limits outside bounds")
	}
	if limits.MaxStreamsPerSession < 1 || limits.MaxStreamsPerSession > 4096 || limits.MaxPeerStreams < 1 || limits.MaxPeerStreams > 65536 || limits.MaxReservations < 1 || limits.MaxReservations > MaxReservations {
		return nil, invalid("stream or reservation limits outside bounds")
	}
	return &Peer{state: &peerState{limits: limits, sessions: make(map[*sessionState]struct{})}}, nil
}

func invalid(reason string) error { return fmt.Errorf("%w: %s", ErrInvalid, reason) }

func (claim Claim) empty() bool {
	return claim.Bytes == 0 && !claim.BusinessStream && !claim.PendingAccept
}

// BeginHandshake atomically reserves a future established Session slot, a
// separate pending-handshake slot, and the full selected initial Claim before
// CHALLENGE. Promote never competes again for those credits. Authentication,
// transcript identity and original deadlines remain caller responsibilities.
func (p *Peer) BeginHandshake(initial Claim) (*Session, *Lease, error) {
	return p.begin(initial, true)
}

// BeginSession directly admits an established scope with an atomic initial
// Claim. Protocol callers normally use BeginHandshake followed by Promote.
// An empty initial Claim returns a nil lease, but still reserves the Session.
func (p *Peer) BeginSession(initial Claim) (*Session, *Lease, error) {
	return p.begin(initial, false)
}

func (p *Peer) begin(initial Claim, pending bool) (*Session, *Lease, error) {
	if p == nil || p.state == nil {
		return nil, nil, invalid("nil Peer")
	}
	state := p.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.usage.Closed {
		return nil, nil, ErrClosed
	}
	if state.usage.SessionSlots == state.limits.MaxSessions || (pending && state.usage.PendingHandshakes == state.limits.MaxPendingHandshakes) {
		return nil, nil, ErrResourceLimit
	}
	s := &sessionState{peer: state, pendingHandshake: pending}
	if !initial.empty() {
		if err := state.checkClaimLocked(s, initial); err != nil {
			return nil, nil, err
		}
	}
	session := &Session{state: s}
	var lease *Lease
	if !initial.empty() {
		lease = newLease(s, initial)
		state.chargeLocked(s, initial)
	}
	state.sessions[s] = struct{}{}
	state.usage.SessionSlots++
	if pending {
		state.usage.PendingHandshakes++
	} else {
		state.usage.EstablishedSessions++
	}
	return session, lease, nil
}

// Promote commits a live pending scope after matching FINISH. It only changes
// classification: all counts and bytes were already reserved. It is idempotent
// for an established scope and cannot revive a closed scope.
func (s *Session) Promote() error {
	if s == nil || s.state == nil {
		return invalid("nil Session")
	}
	state := s.state
	p := state.peer
	p.mu.Lock()
	defer p.mu.Unlock()
	if state.closed || p.usage.Closed {
		return ErrClosed
	}
	if state.pendingHandshake {
		state.pendingHandshake = false
		p.usage.PendingHandshakes--
		p.usage.EstablishedSessions++
	}
	return nil
}

// Reserve atomically acquires a nonempty Claim. Separate simultaneous copies
// require separate reservations, even when their contents are equal.
func (s *Session) Reserve(claim Claim) (*Lease, error) {
	if s == nil || s.state == nil || claim.empty() {
		return nil, invalid("nil Session or empty Claim")
	}
	state := s.state
	p := state.peer
	p.mu.Lock()
	defer p.mu.Unlock()
	if state.closed || p.usage.Closed {
		return nil, ErrClosed
	}
	if err := p.checkClaimLocked(state, claim); err != nil {
		return nil, err
	}
	lease := newLease(state, claim)
	p.chargeLocked(state, claim)
	return lease, nil
}

// BindBytes dedicates an existing byte-only lease to one storage owner without
// reserving again. The lease must belong to this open, established Session and
// cover required bytes. Copies share the one-time binding; a bound lease cannot
// move to another Session. Failure leaves the caller's lease unchanged.
// The returned handle shares release state with the original. The caller must
// dispose of the storage owner before releasing any retained original handle.
func (s *Session) BindBytes(lease *Lease, required uint64) (*Lease, error) {
	if s == nil || s.state == nil || lease == nil || lease.state == nil || required == 0 || required > MaxRetainedBytes {
		return nil, invalid("nil Session, lease or invalid required bytes")
	}
	state := s.state
	p := state.peer
	if lease.state.peer != p {
		return nil, invalid("lease belongs to another Peer")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if state.closed || p.usage.Closed {
		return nil, ErrClosed
	}
	if state.pendingHandshake {
		return nil, invalid("an established Session is required")
	}
	l := lease.state
	if l.released {
		return nil, ErrReleased
	}
	if l.owner != state || l.bound || l.claim.BusinessStream || l.claim.PendingAccept || l.claim.Bytes < required {
		return nil, invalid("lease is not dedicated byte storage for this Session")
	}
	l.bound = true
	return &Lease{state: l}, nil
}

func newLease(s *sessionState, claim Claim) *Lease {
	return &Lease{state: &leaseState{peer: s.peer, owner: s, claim: claim, acceptReserved: claim.PendingAccept}}
}

func (p *peerState) checkSessionClaimLocked(s *sessionState, claim Claim) error {
	if claim.Bytes > p.limits.MaxSessionBytes-s.usage.Bytes || (claim.BusinessStream && s.usage.BusinessStreams == p.limits.MaxStreamsPerSession) {
		return ErrResourceLimit
	}
	return nil
}

func (p *peerState) checkClaimLocked(s *sessionState, claim Claim) error {
	if err := p.checkSessionClaimLocked(s, claim); err != nil {
		return err
	}
	if claim.Bytes > p.limits.MaxPeerBytes-p.usage.Bytes || p.usage.Reservations == p.limits.MaxReservations || (claim.BusinessStream && p.usage.BusinessStreams == p.limits.MaxPeerStreams) || (claim.PendingAccept && p.usage.PendingAccepts == p.limits.MaxPendingAccepts) {
		return ErrResourceLimit
	}
	return nil
}

func addClaim(usage *Usage, claim Claim) {
	usage.Bytes += claim.Bytes
	usage.Reservations++
	if claim.BusinessStream {
		usage.BusinessStreams++
	}
	if claim.PendingAccept {
		usage.PendingAccepts++
	}
}

func subtractClaim(usage *Usage, claim Claim) {
	usage.Bytes -= claim.Bytes
	usage.Reservations--
	if claim.BusinessStream {
		usage.BusinessStreams--
	}
	if claim.PendingAccept {
		usage.PendingAccepts--
	}
}

func (p *peerState) chargeLocked(s *sessionState, claim Claim) {
	addClaim(&p.usage.Usage, claim)
	addClaim(&s.usage, claim)
}

// Transfer moves the same ownership obligation to an open Session of the same
// Peer. The destination is checked before any debit; Peer usage is unchanged.
// A closed source may transfer live storage to an open destination. Transfer
// is not a copy and must not be used while both owners retain separate copies.
// A lease dedicated by BindBytes cannot transfer to another Session.
func (l *Lease) Transfer(destination *Session) error {
	if l == nil || l.state == nil || destination == nil || destination.state == nil {
		return invalid("nil lease or destination")
	}
	state := l.state
	p := state.peer
	if destination.state.peer != p {
		return invalid("transfer across Peers")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if state.released {
		return ErrReleased
	}
	target := destination.state
	if target.closed || p.usage.Closed {
		return ErrClosed
	}
	source := state.owner
	if source == target {
		return nil
	}
	if state.bound {
		return invalid("bound storage cannot change Session")
	}
	if err := p.checkSessionClaimLocked(target, state.claim); err != nil {
		return err
	}
	subtractClaim(&source.usage, state.claim)
	addClaim(&target.usage, state.claim)
	state.owner = target
	p.retireLocked(source)
	return nil
}

// MarkAccepted releases only the pending-accept slot when the application
// takes ownership. Owned bytes and business-stream count remain reserved.
// Repeated calls are harmless, but a pending or closed scope cannot accept.
func (l *Lease) MarkAccepted() error {
	if l == nil || l.state == nil {
		return invalid("nil lease")
	}
	state := l.state
	p := state.peer
	p.mu.Lock()
	defer p.mu.Unlock()
	if state.released {
		return ErrReleased
	}
	if state.owner.closed || p.usage.Closed {
		return ErrClosed
	}
	if state.owner.pendingHandshake || !state.acceptReserved {
		return invalid("lease is not an established pending accept")
	}
	if state.claim.PendingAccept {
		state.claim.PendingAccept = false
		state.owner.usage.PendingAccepts--
		p.usage.PendingAccepts--
	}
	return nil
}

// Release is idempotent and safe after Close. The caller must first clear
// actual owned storage/obligations; closing a scope never releases it for them.
func (l *Lease) Release() {
	if l == nil || l.state == nil {
		return
	}
	state := l.state
	p := state.peer
	p.mu.Lock()
	defer p.mu.Unlock()
	if state.released {
		return
	}
	owner := state.owner
	subtractClaim(&owner.usage, state.claim)
	subtractClaim(&p.usage.Usage, state.claim)
	state.released = true
	state.owner = nil
	state.claim = Claim{}
	p.retireLocked(owner)
}

// Close prevents new reservations, promotion and acceptance. The Session slot
// and all owned claims stay charged until the last lease is released or moved.
// This includes count-only leases and scopes closed during a handshake.
func (s *Session) Close() {
	if s == nil || s.state == nil {
		return
	}
	state := s.state
	p := state.peer
	p.mu.Lock()
	defer p.mu.Unlock()
	state.closed = true
	p.retireLocked(state)
}

// Close stops all Peer/Session admissions in bounded O(MaxSessions) work.
// Empty scopes retire immediately. Live leases are not revoked; callers must
// dispose of their buffers and Release them, even after Peer Close returns.
func (p *Peer) Close() {
	if p == nil || p.state == nil {
		return
	}
	state := p.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.usage.Closed {
		return
	}
	state.usage.Closed = true
	for s := range state.sessions {
		s.closed = true
		state.retireLocked(s)
	}
}

func (p *peerState) retireLocked(s *sessionState) {
	if !s.closed || s.retired || s.usage.Reservations != 0 {
		return
	}
	s.retired = true
	p.usage.SessionSlots--
	if s.pendingHandshake {
		p.usage.PendingHandshakes--
	} else {
		p.usage.EstablishedSessions--
	}
	delete(p.sessions, s)
}

func (p *Peer) Snapshot() PeerSnapshot {
	if p == nil || p.state == nil {
		return PeerSnapshot{Closed: true}
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	return p.state.usage
}

func (s *Session) Snapshot() SessionSnapshot {
	if s == nil || s.state == nil {
		return SessionSnapshot{Closed: true, Retired: true}
	}
	state := s.state
	state.peer.mu.Lock()
	defer state.peer.mu.Unlock()
	return SessionSnapshot{Usage: state.usage, PendingHandshake: state.pendingHandshake, Closed: state.closed, Retired: state.retired}
}

func (l *Lease) Snapshot() LeaseSnapshot {
	if l == nil || l.state == nil {
		return LeaseSnapshot{Released: true}
	}
	l.state.peer.mu.Lock()
	defer l.state.peer.mu.Unlock()
	return LeaseSnapshot{Claim: l.state.claim, Released: l.state.released}
}
