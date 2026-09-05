package handshakev2

import (
	"bytes"
	"crypto/sha256"
	"io"
	"math"
	"slices"
	"time"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

type phase uint8

const (
	waitChallenge phase = iota + 1
	waitFinish
	waitReady
	established
)

type packets struct {
	hello, challenge, finish, ready [wirev2.HandshakePacketSize]byte
}

type attempt struct {
	setup                     Setup
	state                     phase
	policy                    Policy
	hello                     negotiationv2.Advertisement
	transcript                wirev2.Transcript
	deadline, nextSend        time.Time
	sends                     int
	packets                   *packets
	receiveLease, packetLease *creditv2.Lease
	dispose                   func()
	closeSent                 bool
}

type dial struct {
	id            DialID
	request       DialRequest
	next, running int
}

type rejected struct {
	id       wirev2.SessionID
	digest   wirev2.Digest
	deadline time.Time
}

type Engine struct {
	config                Config
	psk                   []byte
	handshakeKey          wirev2.Key
	sessions              map[wirev2.SessionID]*attempt
	dials                 map[DialID]*dial
	rejections            [MaxRejections]rejected
	pending               int
	nextDial              DialID
	lastNow               time.Time
	hasTime, busy, closed bool
}

// New copies PSK and listener policy. No credits or protocol objects are
// admitted until BeginDial or a valid authenticated compatible HELLO.
func New(psk []byte, config Config) (*Engine, error) {
	if config.Credits == nil || config.Credits.Snapshot().Closed || config.Entropy == nil || config.Emit == nil || config.Install == nil {
		return nil, ErrInvalid
	}
	key, err := wirev2.DeriveHandshakeKey(psk)
	if err != nil {
		return nil, err
	}
	if config.Listener != nil {
		copyPolicy := clonePolicy(*config.Listener)
		if err := validatePolicy(copyPolicy, true); err != nil {
			return nil, err
		}
		config.Listener = &copyPolicy
	}
	return &Engine{config: config, psk: slices.Clone(psk), handshakeKey: key, sessions: make(map[wirev2.SessionID]*attempt), dials: make(map[DialID]*dial)}, nil
}

func validatePolicy(policy Policy, listener bool) error {
	if err := policy.Profile.Validate(); err != nil {
		return err
	}
	minimum := uint64(policy.Profile.Streams.SessionReceiveBytes)
	if policy.Profile.Protocol == negotiationv2.Datagram {
		minimum = 16 * ((uint64(policy.Profile.Datagram.DatagramWindow)+63)/64 + (uint64(policy.Profile.Datagram.GroupWindow)+63)/64)
	}
	if len(policy.Initial) > MaxInitialReservations || policy.Receive.PendingAccept != listener || policy.Receive.Bytes > creditv2.MaxRetainedBytes {
		return ErrInvalid
	}
	total := policy.Receive.Bytes
	for _, claim := range policy.Initial {
		if claim.Bytes == 0 || claim.Bytes > creditv2.MaxRetainedBytes-total || claim.BusinessStream || claim.PendingAccept {
			return ErrInvalid
		}
		total += claim.Bytes
	}
	if total < minimum {
		return ErrInvalid
	}
	return nil
}

func clonePolicy(policy Policy) Policy {
	policy.Initial = slices.Clone(policy.Initial)
	return policy
}

func cloneSetup(setup Setup) Setup {
	setup.Initial = slices.Clone(setup.Initial)
	return setup
}

func validBinding(binding Binding) bool {
	return binding.SocketID != 0 && validAddress(binding.Local) && validAddress(binding.Remote)
}

func (e *Engine) enter(now time.Time, allowClosed bool) error {
	if e == nil {
		return ErrInvalid
	}
	if e.busy {
		return ErrReentrant
	}
	if e.closed && !allowClosed {
		return ErrClosed
	}
	if e.hasTime && now.Before(e.lastNow) {
		return ErrTime
	}
	e.lastNow, e.hasTime, e.busy = now, true, true
	return nil
}

func (e *Engine) leave() { e.busy = false }

func (e *Engine) randomBlock() (wirev2.Nonce, error) {
	for i := 0; i < 4; i++ {
		var value wirev2.Nonce
		if _, err := io.ReadFull(e.config.Entropy, value[:]); err != nil {
			return wirev2.Nonce{}, ErrEntropy
		}
		if value != (wirev2.Nonce{}) {
			return value, nil
		}
	}
	return wirev2.Nonce{}, ErrEntropy
}

func (e *Engine) newID() (wirev2.SessionID, error) {
	for i := 0; i < 4; i++ {
		value, err := e.randomBlock()
		if err != nil {
			return wirev2.SessionID{}, err
		}
		id := wirev2.SessionID(value)
		if e.sessions[id] == nil {
			return id, nil
		}
	}
	return wirev2.SessionID{}, ErrEntropy
}

func (e *Engine) reserve(policy Policy) (*creditv2.Session, *creditv2.Lease, *creditv2.Lease, []*creditv2.Lease, error) {
	if e.pending >= MaxPending {
		return nil, nil, nil, nil, creditv2.ErrResourceLimit
	}
	scope, receive, err := e.config.Credits.BeginHandshake(policy.Receive)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	initial := make([]*creditv2.Lease, 0, len(policy.Initial))
	rollback := func() {
		scope.Close()
		for _, lease := range initial {
			lease.Release()
		}
		receive.Release()
	}
	for _, claim := range policy.Initial {
		lease, err := scope.Reserve(claim)
		if err != nil {
			rollback()
			return nil, nil, nil, nil, err
		}
		initial = append(initial, lease)
	}
	packet, err := scope.Reserve(creditv2.Claim{Bytes: PacketReservationBytes})
	if err != nil {
		rollback()
		return nil, nil, nil, nil, err
	}
	return scope, receive, packet, initial, nil
}

func (e *Engine) orderedIDs() []wirev2.SessionID {
	ids := make([]wirev2.SessionID, 0, len(e.sessions))
	for id := range e.sessions {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b wirev2.SessionID) int { return bytes.Compare(a[:], b[:]) })
	return ids
}

func (e *Engine) send(a *attempt, kind wirev2.PacketType, packet []byte, now time.Time, result *Result, transition bool) {
	if a.sends >= MaxSends || (!transition && now.Before(a.nextSend)) {
		return
	}
	if (kind == wirev2.TypeHello || kind == wirev2.TypeChallenge) && a.sends >= MaxSends-1 {
		return
	}
	a.sends++
	a.nextSend = now.Add(RetryInterval)
	err := e.config.Emit(a.setup.Binding, packet)
	result.Sends = append(result.Sends, SendAttempt{ID: a.setup.ID, Type: kind, PathID: a.setup.PathID, Err: err})
}

func authenticate(packet []byte, key wirev2.Key) (wirev2.AuthenticatedEnvelope, error) {
	envelope, err := wirev2.ParseEnvelope(packet)
	if err != nil {
		return wirev2.AuthenticatedEnvelope{}, err
	}
	return envelope.Authenticate(key)
}

func encode(packet *[wirev2.HandshakePacketSize]byte, message wirev2.Handshake, key wirev2.Key) error {
	encoded, err := wirev2.AppendHandshake(packet[:0], message, key)
	if err != nil {
		return err
	}
	if len(encoded) != len(packet) {
		return ErrInvalid
	}
	return nil
}

func (e *Engine) prepareTranscript(a *attempt) error {
	h, err := authenticate(a.packets.hello[:], e.handshakeKey)
	if err != nil {
		return err
	}
	c, err := authenticate(a.packets.challenge[:], e.handshakeKey)
	if err != nil {
		return err
	}
	a.transcript, err = wirev2.NewTranscript(h, c)
	if err != nil {
		return err
	}
	a.setup.Keys, err = wirev2.DeriveDirectionalKeys(e.psk, a.transcript)
	if err != nil {
		return err
	}
	finish, err := a.transcript.Confirmation(wirev2.TypeFinish)
	if err != nil {
		return err
	}
	ready, err := a.transcript.Confirmation(wirev2.TypeReady)
	if err != nil {
		return err
	}
	if err := encode(&a.packets.finish, finish, a.setup.Keys.ClientToServer); err != nil {
		return err
	}
	return encode(&a.packets.ready, ready, a.setup.Keys.ServerToClient)
}

// Receive authenticates before changing an attempt. Packet and binding remain
// immutable for the call. Unknown IDs/types, wrong bindings, and unexpected
// transitions are silently ignored; authentication/semantic errors are returned
// as nonfatal packet errors, not instructions to close an unrelated Session.
func (e *Engine) Receive(now time.Time, binding Binding, packet []byte) (Result, error) {
	var result Result
	if err := e.enter(now, false); err != nil {
		return result, err
	}
	defer e.leave()
	if !validBinding(binding) {
		return result, ErrInvalid
	}
	envelope, err := wirev2.ParseEnvelope(packet)
	if err != nil {
		return result, err
	}
	header := envelope.Header()
	a := e.sessions[header.SessionID]
	if header.Type == wirev2.TypeHello {
		if e.config.Listener == nil {
			return result, nil
		}
		authenticated, err := envelope.Authenticate(e.handshakeKey)
		if err != nil {
			return result, err
		}
		return e.receiveHello(now, binding, packet, authenticated, a, &result)
	}
	if a == nil || a.setup.Binding != binding {
		return result, nil
	}
	var key wirev2.Key
	switch header.Type {
	case wirev2.TypeChallenge, wirev2.TypeReject:
		if a.setup.Role != negotiationv2.Initiator || a.state == established {
			return result, nil
		}
		key = e.handshakeKey
	case wirev2.TypeFinish:
		if a.setup.Role != negotiationv2.Responder || a.packets == nil {
			return result, nil
		}
		key = a.setup.Keys.ClientToServer
	case wirev2.TypeReady:
		if a.setup.Role != negotiationv2.Initiator || a.state != waitReady {
			return result, nil
		}
		key = a.setup.Keys.ServerToClient
	case wirev2.TypeClose:
		if a.setup.Role == negotiationv2.Initiator {
			key = a.setup.Keys.ServerToClient
		} else {
			key = a.setup.Keys.ClientToServer
		}
		if key == (wirev2.Key{}) {
			return result, nil
		}
	default:
		return result, nil
	}
	authenticated, err := envelope.Authenticate(key)
	if err != nil {
		return result, err
	}
	if header.Type == wirev2.TypeClose {
		return e.receiveClose(now, a, authenticated, &result)
	}
	if !now.Before(a.deadline) {
		if a.state != established {
			e.failAttempt(a, ErrExpired, now, &result, true)
			e.fillDials(now, &result)
		} else {
			e.releasePackets(a)
		}
		return result, nil
	}
	switch header.Type {
	case wirev2.TypeChallenge:
		err = e.receiveChallenge(now, a, packet, authenticated, &result)
	case wirev2.TypeReject:
		err = e.receiveReject(now, a, authenticated, &result)
	case wirev2.TypeFinish:
		err = e.receiveFinish(now, a, packet, authenticated, &result)
	case wirev2.TypeReady:
		err = e.receiveReady(now, a, packet, authenticated, &result)
	}
	e.fillDials(now, &result)
	return result, err
}

func (e *Engine) reject(now time.Time, binding Binding, packet []byte, code uint16, result *Result) {
	if len(packet) != wirev2.HandshakePacketSize {
		return
	}
	var id wirev2.SessionID
	copy(id[:], packet[8:24])
	digest := sha256.Sum256(packet[:wirev2.HandshakePacketSize-wirev2.AuthenticationTagSize])
	slot := -1
	for i, entry := range e.rejections {
		if entry.id == (wirev2.SessionID{}) || !now.Before(entry.deadline) {
			if slot < 0 {
				slot = i
			}
			continue
		}
		if entry.id == id && entry.digest == digest {
			return
		}
	}
	if slot < 0 {
		return
	}
	message := wirev2.Handshake{Header: wirev2.Header{Type: wirev2.TypeReject, SessionID: id}, TranscriptDigest: digest, TLVs: []wirev2.TLV{{Type: wirev2.TLVError, Value: []byte{byte(code >> 8), byte(code), 0, 0}}}}
	copy(message.ClientNonce[:], packet[wirev2.PrefixSize:wirev2.PrefixSize+16])
	var response [wirev2.HandshakePacketSize]byte
	if err := encode(&response, message, e.handshakeKey); err != nil {
		return
	}
	e.rejections[slot] = rejected{id: id, digest: digest, deadline: now.Add(Lifetime)}
	err := e.config.Emit(binding, response[:])
	result.Sends = append(result.Sends, SendAttempt{ID: id, Type: wirev2.TypeReject, Err: err})
	clear(response[:])
}

func (e *Engine) Snapshot() Snapshot {
	if e == nil {
		return Snapshot{Closed: true}
	}
	result := Snapshot{Pending: e.pending, Dials: len(e.dials), Closed: e.closed}
	for _, a := range e.sessions {
		if a.state == established {
			result.Established++
		}
		if a.packetLease != nil {
			result.PacketBytes += PacketReservationBytes
		}
	}
	for _, entry := range e.rejections {
		if e.lastNow.Before(entry.deadline) {
			result.Rejections++
		}
	}
	return result
}

func (e *Engine) nextDialID() (DialID, error) {
	if e.nextDial == DialID(math.MaxUint64) {
		return 0, ErrExhausted
	}
	e.nextDial++
	return e.nextDial, nil
}
