package sessionv2

import (
	"errors"
	"math"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/mofelee/mpudp/internal/handshakev2"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
)

var (
	ErrSendPending = errors.New("v2 send ownership is still pending")
	ErrSendToken   = errors.New("invalid v2 send completion token")
	ErrSendExpired = errors.New("v2 send queue residence expired")
)

type SendToken uint64

// SendIntent transfers one immutable packet to the adapter. Packet and Sender
// are borrowed from its private owner until Release, after Send returns or a
// before-send cancellation. They must not be accessed during or after Release.
// ExpiresAt limits waiting before invocation, not already executing I/O.
// NativeTiming records exact native capability captured at binding setup.
// Release does not complete the controller's token.
type SendIntent struct {
	Token        SendToken
	Type         wirev2.PacketType
	PathID       uint16
	Route        wirev2.Route
	Binding      handshakev2.Binding
	Sender       transport.ReplyPath
	Packet       []byte
	ExpiresAt    time.Time
	NativeTiming bool
	state        *sendIntentState
}

type sendIntentState struct {
	mu       sync.Mutex
	packet   []byte
	sender   transport.ReplyPath
	released bool
}

func (i *SendIntent) Release() {
	if i == nil || i.state == nil {
		return
	}
	i.state.mu.Lock()
	if !i.state.released {
		clear(i.state.packet)
		i.state.packet, i.state.sender = nil, nil
		i.state.released = true
	}
	i.Packet, i.Sender = nil, nil
	i.state.mu.Unlock()
}

func (s *sendIntentState) isReleased() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.released
}

// Invoked records a completed Sender.Send call. AttemptKnown identifies native
// transport observation; its zero StartedAt means rejection before Write.
// Unknown custom timing preserves Send's success contract without inventing an
// attempt timestamp. FinishedAt is execution data, not the controller clock.
type SendOutcome struct {
	Token        SendToken
	Invoked      bool
	AttemptKnown bool
	StartedAt    time.Time
	FinishedAt   time.Time
	Err          error
}

type sendSource uint8

const (
	sendData sendSource = iota
	sendJoin
	sendBudget
	sendContext
	sendReply0
	sendReply1
)

type shardSendState uint8

const (
	shardWaiting shardSendState = iota
	shardDispatched
	shardTerminal
)

type sendSlot struct {
	token               SendToken
	owner               *sendIntentState
	kind                wirev2.PacketType
	pathID              uint16
	pathGeneration      uint64
	transportGeneration uint64
	binding             handshakev2.Binding
	route               wirev2.Route
	native              bool
	bytes               int
	source              sendSource
	shard               int
	transaction         uint64
	version             uint64
	issued, expires     time.Time
}

type ownedSendState struct {
	slots       []sendSlot
	nextToken   SendToken
	active      int
	shards      [256]shardSendState
	remaining   int
	groupQueued time.Time
	cursor      int
	closing     bool
}

func captureBoundSender(binding handshakev2.Binding, sender transport.ReplyPath) (transport.ReplyPath, uint64, bool, error) {
	captured, native, err := transport.CaptureSendPath(sender)
	if err != nil {
		return nil, 0, false, err
	}
	if native {
		local, localOK := captured.LocalAddr().(*net.UDPAddr)
		remote, remoteOK := captured.RemoteAddr().(*net.UDPAddr)
		normalize := func(addr *net.UDPAddr) netip.AddrPort {
			value := addr.AddrPort()
			return netip.AddrPortFrom(value.Addr().Unmap(), value.Port())
		}
		if !localOK || !remoteOK || local == nil || remote == nil || normalize(local) != binding.Local || normalize(remote) != binding.Remote {
			return nil, 0, false, transport.ErrGenerationReplaced
		}
	}
	return captured, captured.Generation(), native, nil
}

func (c *Controller) nextControlID() (uint64, error) {
	if c.nextControl == math.MaxUint64 {
		return 0, ErrExhausted
	}
	c.nextControl++
	return c.nextControl, nil
}

func (c *Controller) frameSet(frame *controlFrame, packet []byte, now time.Time) error {
	if !c.cfg.OwnedSends {
		return frame.set(packet)
	}
	version, err := c.nextControlID()
	if err != nil {
		return err
	}
	if err := frame.set(packet); err != nil {
		return err
	}
	frame.version, frame.queuedAt, frame.dispatched = version, now, false
	return nil
}

func (c *Controller) newControlRetry(now time.Time) (controlRetry, error) {
	state := controlRetry{pending: true, deadline: now.Add(ControlLifetime), next: now}
	if c.cfg.OwnedSends {
		id, err := c.nextControlID()
		if err != nil {
			return controlRetry{}, err
		}
		state.id = id
	}
	return state, nil
}

func (c *Controller) PendingSends() int {
	if c == nil || c.sends == nil {
		return 0
	}
	return c.sends.active
}

func (c *Controller) freeSendSlot() *sendSlot {
	for i := range c.sends.slots {
		if c.sends.slots[i].token == 0 {
			return &c.sends.slots[i]
		}
	}
	return nil
}

func (c *Controller) pathCanDispatch(p *pathState, now time.Time, bytes int) bool {
	return p.sendToken == 0 && !now.Before(p.pacedAt) && p.queuedPackets < c.cfg.MaxPathQueuedPackets && uint64(bytes) <= c.cfg.MaxPathQueuedBytes-p.queuedBytes
}

func (c *Controller) installIntent(slot *sendSlot, p *pathState, sender transport.ReplyPath, packet []byte) *SendIntent {
	c.sends.nextToken++
	slot.token = c.sends.nextToken
	slot.owner = &sendIntentState{packet: packet, sender: sender}
	slot.kind, slot.bytes = wirev2.PacketType(packet[5]), cap(packet)
	p.sendToken = slot.token
	p.queuedPackets++
	p.queuedBytes += uint64(slot.bytes)
	c.sends.active++
	return &SendIntent{Token: slot.token, Type: slot.kind, PathID: slot.pathID, Route: slot.route, Binding: slot.binding, Sender: sender, Packet: packet, ExpiresAt: slot.expires, NativeTiming: slot.native, state: slot.owner}
}

// TakeSend returns work only when the adapter can transfer it to an idle worker.
// No intent means no ready send, not a completed socket attempt.
// Errors return no intent. A nonnil intent must be retained before processing
// Result, which may include earlier terminal failures from waiting work.
func (c *Controller) TakeSend(now time.Time) (intent *SendIntent, result Result, err error) {
	if err = c.enter(now); err != nil {
		return nil, result, err
	}
	defer c.leave(&result)
	if !c.cfg.OwnedSends || c.sends == nil {
		return nil, result, ErrUnsupported
	}
	if err = c.driveOwned(now, &result); err != nil {
		return nil, result, err
	}
	slot := c.freeSendSlot()
	if slot == nil {
		return nil, result, nil
	}
	if c.sends.nextToken == SendToken(math.MaxUint64) {
		return nil, result, ErrExhausted
	}
	if intent, err = c.takeOwnedControl(now, slot, &result); intent != nil || err != nil {
		return intent, result, err
	}
	intent, err = c.takeOwnedData(now, slot)
	return intent, result, err
}

func (c *Controller) CompleteSend(now time.Time, outcome SendOutcome) (result Result, err error) {
	if c == nil || c.sends == nil || !c.cfg.OwnedSends {
		return result, ErrClosed
	}
	if c.busy {
		return result, ErrReentrant
	}
	if c.haveTime && now.Before(c.last) {
		return result, ErrTime
	}
	var slot *sendSlot
	for i := range c.sends.slots {
		if outcome.Token != 0 && c.sends.slots[i].token == outcome.Token {
			slot = &c.sends.slots[i]
			break
		}
	}
	if slot == nil {
		return result, ErrSendToken
	}
	if outcome.FinishedAt.Before(slot.issued) || outcome.FinishedAt.After(now) || (!outcome.Invoked && (outcome.Err == nil || !outcome.StartedAt.IsZero())) || (outcome.Invoked && outcome.AttemptKnown != slot.native) || (!outcome.AttemptKnown && !outcome.StartedAt.IsZero()) || (!outcome.StartedAt.IsZero() && (outcome.StartedAt.Before(slot.issued) || outcome.StartedAt.After(outcome.FinishedAt))) || (outcome.Invoked && outcome.AttemptKnown && outcome.StartedAt.IsZero() && outcome.Err == nil) {
		return result, ErrInvalid
	}
	if !slot.owner.isReleased() {
		return result, ErrSendPending
	}
	if c.setup.Scope.Snapshot().Closed {
		c.BeginClose()
	}
	c.last, c.haveTime, c.busy = now, true, true
	defer c.leave(&result)
	p := &c.paths[slot.pathID-1]
	if p.sendToken == slot.token {
		p.sendToken = 0
	}
	p.queuedPackets--
	p.queuedBytes -= uint64(slot.bytes)
	c.sends.active--
	completed := *slot
	*slot = sendSlot{}
	result.Sends = append(result.Sends, SendAttempt{Type: completed.kind, PathID: completed.pathID, Bytes: completed.bytes, Err: outcome.Err})
	if c.sends.closing {
		return result, nil
	}
	samePath := p.generation == completed.pathGeneration && p.binding == completed.binding && p.transportGeneration == completed.transportGeneration
	if completed.source == sendJoin {
		samePath = p.join.generation == completed.route.Generation && p.join.binding == completed.binding && p.join.transportGeneration == completed.transportGeneration
	}
	if outcome.Invoked && (!outcome.AttemptKnown || !outcome.StartedAt.IsZero()) && samePath {
		at := outcome.FinishedAt
		if outcome.AttemptKnown {
			at = outcome.StartedAt
		}
		p.pacedAt = later(p.pacedAt, at.Add(serializationTime(p.rate, completed.binding, completed.bytes)))
	}
	if completed.source == sendData {
		c.completeOwnedShard(completed.shard, outcome.Err)
	} else {
		c.completeOwnedControl(&completed, outcome)
	}
	err = c.driveOwned(now, &result)
	return result, err
}

func serializationTime(rate uint64, binding handshakev2.Binding, bytes int) time.Duration {
	overhead := uint64(28)
	if binding.Local.Addr().Is6() {
		overhead = 48
	}
	bits := (uint64(bytes) + overhead) * 8
	return time.Duration((bits*uint64(time.Second) + rate - 1) / rate)
}
