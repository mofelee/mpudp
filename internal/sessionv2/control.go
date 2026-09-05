package sessionv2

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"time"

	"github.com/mofelee/mpudp/internal/fecv2"
	"github.com/mofelee/mpudp/internal/handshakev2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
)

type controlFrame struct {
	packet [512]byte
	length int
	queued bool
}

func (f *controlFrame) set(packet []byte) error {
	if len(packet) > len(f.packet) {
		return ErrInvalid
	}
	clear(f.packet[:])
	copy(f.packet[:], packet)
	f.length, f.queued = len(packet), true
	return nil
}

type controlRetry struct {
	frame          controlFrame
	deadline, next time.Time
	sends          int
	pending        bool
}

type pathJoin struct {
	controlRetry
	kind           wirev2.PacketType
	client, server wirev2.Nonce
	binding        handshakev2.Binding
	sender         transport.ReplyPath
	generation     uint64
	committed      bool
}

type oldRoute struct {
	binding    handshakev2.Binding
	sender     transport.ReplyPath
	generation uint64
	epoch      uint32
	budget     uint16
	until      time.Time
}

type pathState struct {
	id                        uint16
	active                    bool
	generation, floor         uint64
	binding                   handshakev2.Binding
	sender                    transport.ReplyPath
	configured                Carrier
	sendBudget, receiveBudget uint16
	sendEpoch, receiveEpoch   uint32
	oldBudget                 uint16
	oldEpoch                  uint32
	oldUntil                  time.Time
	budgetPeerDeadline        time.Time
	old                       [8]oldRoute
	join                      pathJoin
	budget                    controlRetry
	replies                   [2]controlFrame
	pacedAt                   time.Time
	rate                      uint64
}

func (p *pathState) route() wirev2.Route {
	return wirev2.Route{PathID: uint32(p.id), Generation: p.generation, BudgetEpoch: p.sendEpoch}
}

func (c *Controller) validRoute(p *pathState, binding handshakev2.Binding, route wirev2.Route, now time.Time) (uint16, bool, bool) {
	if p.active && p.binding == binding && p.generation == route.Generation {
		if p.receiveEpoch == route.BudgetEpoch {
			return p.receiveBudget, false, true
		}
		if p.oldEpoch == route.BudgetEpoch && now.Before(p.oldUntil) {
			return p.oldBudget, true, true
		}
	}
	for _, old := range p.old {
		if old.generation == route.Generation && old.epoch == route.BudgetEpoch && old.binding == binding && now.Before(old.until) {
			return old.budget, true, true
		}
	}
	return 0, false, false
}

func (c *Controller) randomNonce() (wirev2.Nonce, error) {
	for range 4 {
		var nonce wirev2.Nonce
		if _, err := io.ReadFull(c.cfg.Entropy, nonce[:]); err != nil {
			return wirev2.Nonce{}, ErrEntropy
		}
		if nonce != (wirev2.Nonce{}) {
			return nonce, nil
		}
	}
	return wirev2.Nonce{}, ErrEntropy
}

func (c *Controller) joinFrame(p *pathState, kind wirev2.PacketType) error {
	var body [440]byte
	copy(body[:16], p.join.client[:])
	copy(body[16:32], p.join.server[:])
	packet, err := wirev2.AppendEstablished(nil, wirev2.Header{Type: kind, SessionID: c.setup.ID}, wirev2.Route{PathID: uint32(p.id), Generation: p.join.generation}, body[:], c.sendKey)
	if err != nil {
		return err
	}
	p.join.kind = kind
	return p.join.frame.set(packet)
}

func (c *Controller) startJoin(carrier Carrier, now time.Time) error {
	p := &c.paths[carrier.PathID-1]
	if p.join.pending || p.floor == math.MaxUint64 {
		return ErrExhausted
	}
	nonce, err := c.randomNonce()
	if err != nil {
		return err
	}
	p.floor++
	p.join = pathJoin{controlRetry: controlRetry{pending: true, deadline: now.Add(ControlLifetime), next: now}, client: nonce, binding: carrier.Binding, sender: carrier.Sender, generation: p.floor}
	return c.joinFrame(p, wirev2.TypePathJoin)
}

// Join revalidates one configured Carrier with a fresh monotonic generation.
// It never replaces an active tuple until matching authenticated PATH_READY.
func (c *Controller) Join(now time.Time, carrier Carrier) (result Result, err error) {
	if err = c.enter(now); err != nil {
		return result, err
	}
	defer c.leave(&result)
	if !c.started || c.setup.Role != negotiationv2.Initiator || carrier.PathID == 0 || carrier.PathID > c.setup.Contract.MaxPaths || !validBinding(carrier.Binding) || carrier.Sender == nil {
		return result, ErrInvalid
	}
	err = c.startJoin(carrier, now)
	if err == nil {
		c.driveControl(now, &result)
	}
	return result, err
}

func (c *Controller) commitPath(p *pathState, now time.Time, result *Result) error {
	if p.active {
		slot := -1
		for i := 0; i < int(c.setup.Contract.Epochs.MaxOldEpochs); i++ {
			if !now.Before(p.old[i].until) {
				slot = i
				break
			}
		}
		if slot < 0 {
			return ErrNotReady
		}
		p.old[slot] = oldRoute{binding: p.binding, sender: p.sender, generation: p.generation, epoch: p.receiveEpoch, budget: p.receiveBudget, until: now.Add(time.Duration(c.setup.Contract.Epochs.GraceMS) * time.Millisecond)}
	}
	p.active, p.generation, p.binding, p.sender = true, p.join.generation, p.join.binding, p.join.sender
	p.sendBudget, p.receiveBudget, p.sendEpoch, p.receiveEpoch = 512, 512, 1, 1
	p.oldBudget, p.oldEpoch, p.oldUntil = 0, 0, time.Time{}
	p.budgetPeerDeadline = time.Time{}
	p.budget, p.replies = controlRetry{}, [2]controlFrame{}
	p.join.committed = true
	result.PathsChanged = true
	return c.startBudget(p, now)
}

func (c *Controller) receiveJoin(p *pathState, now time.Time, binding handshakev2.Binding, reply transport.ReplyPath, message wirev2.Established, result *Result) error {
	if len(message.Body) != 440 || message.Route.BudgetEpoch != 0 || !allZero(message.Body[32:]) {
		return ErrProtocol
	}
	var client, server wirev2.Nonce
	copy(client[:], message.Body[:16])
	copy(server[:], message.Body[16:32])
	if client == (wirev2.Nonce{}) {
		return ErrProtocol
	}
	if message.Header.Type == wirev2.TypePathJoin {
		if c.setup.Role != negotiationv2.Responder || server != (wirev2.Nonce{}) {
			return ErrProtocol
		}
		if p.join.pending {
			if !now.Before(p.join.deadline) || p.join.binding != binding || p.join.generation != message.Route.Generation || p.join.client != client {
				return ErrProtocol
			}
			return nil
		}
		if message.Route.Generation <= p.floor {
			return ErrProtocol
		}
		nonce, err := c.randomNonce()
		if err != nil {
			return err
		}
		p.floor = message.Route.Generation
		p.join = pathJoin{controlRetry: controlRetry{pending: true, deadline: now.Add(ControlLifetime), next: now}, client: client, server: nonce, binding: binding, sender: reply, generation: p.floor}
		return c.joinFrame(p, wirev2.TypePathChallenge)
	}
	if message.Header.Type == wirev2.TypePathReady && p.join.committed && p.join.binding == binding && p.join.generation == message.Route.Generation && p.join.client == client && p.join.server == server {
		return nil
	}
	if !p.join.pending || !now.Before(p.join.deadline) || p.join.binding != binding || p.join.generation != message.Route.Generation || p.join.client != client || server == (wirev2.Nonce{}) {
		return ErrProtocol
	}
	switch message.Header.Type {
	case wirev2.TypePathChallenge:
		if c.setup.Role != negotiationv2.Initiator {
			return ErrProtocol
		}
		if p.join.kind == wirev2.TypePathConfirm {
			if p.join.server != server {
				return ErrProtocol
			}
			return nil
		}
		if p.join.kind != wirev2.TypePathJoin {
			return ErrProtocol
		}
		p.join.server, p.join.next = server, now
		return c.joinFrame(p, wirev2.TypePathConfirm)
	case wirev2.TypePathConfirm:
		if c.setup.Role != negotiationv2.Responder || p.join.server != server {
			return ErrProtocol
		}
		if !p.join.committed {
			if err := c.commitPath(p, now, result); err != nil {
				return err
			}
			p.join.next = now
			return c.joinFrame(p, wirev2.TypePathReady)
		}
		return nil
	case wirev2.TypePathReady:
		if c.setup.Role != negotiationv2.Initiator || p.join.kind != wirev2.TypePathConfirm || p.join.server != server {
			return ErrProtocol
		}
		if err := c.commitPath(p, now, result); err != nil {
			return err
		}
		p.join.pending = false
		return nil
	}
	return ErrProtocol
}

func (c *Controller) sendDirection() byte {
	if c.setup.Role == negotiationv2.Initiator {
		return 0
	}
	return 1
}

// Static publication retries retain their original control deadline even when
// receive-only DATA grace is shorter than the250ms retry interval.
func (c *Controller) allowBudgetRetry(p *pathState, binding handshakev2.Binding, message wirev2.Established, now time.Time) bool {
	if !p.active || p.binding != binding || p.generation != message.Route.Generation || message.Route.BudgetEpoch != 1 || len(message.Body) != 8 {
		return false
	}
	body := message.Body
	if message.Header.Type == wirev2.TypePathBudgetUpdate {
		return now.Before(p.budgetPeerDeadline) && body[0] == c.sendDirection()^1 && body[1] == 0 && binary.BigEndian.Uint16(body[2:4]) == p.receiveBudget && binary.BigEndian.Uint32(body[4:]) == 2
	}
	if message.Header.Type == wirev2.TypePathBudgetAck && p.budget.pending && now.Before(p.budget.deadline) {
		return bytes.Equal(body, p.budget.frame.packet[wirev2.PrefixSize+wirev2.RouteSize:p.budget.frame.length-wirev2.AuthenticationTagSize])
	}
	return false
}

func (c *Controller) startBudget(p *pathState, now time.Time) error {
	var body [8]byte
	body[0] = c.sendDirection()
	binary.BigEndian.PutUint16(body[2:4], c.cfg.FixedPayloadBudget)
	binary.BigEndian.PutUint32(body[4:], 2)
	packet, err := wirev2.AppendEstablished(nil, wirev2.Header{Type: wirev2.TypePathBudgetUpdate, SessionID: c.setup.ID}, p.route(), body[:], c.sendKey)
	if err != nil {
		return err
	}
	p.budget = controlRetry{pending: true, next: now, deadline: now.Add(ControlLifetime)}
	return p.budget.frame.set(packet)
}

func (c *Controller) queueReply(p *pathState, slot int, kind wirev2.PacketType, body []byte) error {
	packet, err := wirev2.AppendEstablished(nil, wirev2.Header{Type: kind, SessionID: c.setup.ID}, p.route(), body, c.sendKey)
	if err != nil {
		return err
	}
	return p.replies[slot].set(packet)
}

func (c *Controller) receiveBudget(p *pathState, now time.Time, message wirev2.Established) error {
	body := message.Body
	if len(body) != 8 || body[1] != 0 || binary.BigEndian.Uint32(body[4:]) != 2 {
		return ErrProtocol
	}
	budget := binary.BigEndian.Uint16(body[2:4])
	if message.Header.Type == wirev2.TypePathBudgetUpdate {
		if body[0] != c.sendDirection()^1 || budget < 512 || budget > min(c.cfg.LocalProfile.Payload.ReceiveHardCap, c.remote.Payload.SendHardCap) {
			return ErrProtocol
		}
		if p.receiveEpoch == 2 {
			if p.receiveBudget != budget {
				return ErrProtocol
			}
		} else {
			p.oldBudget, p.oldEpoch = p.receiveBudget, p.receiveEpoch
			p.oldUntil = now.Add(time.Duration(c.setup.Contract.Epochs.GraceMS) * time.Millisecond)
			p.budgetPeerDeadline = now.Add(ControlLifetime)
			p.receiveBudget, p.receiveEpoch = budget, 2
		}
		return c.queueReply(p, 0, wirev2.TypePathBudgetAck, body)
	}
	if body[0] != c.sendDirection() || budget != c.cfg.FixedPayloadBudget {
		return ErrProtocol
	}
	if p.sendEpoch == 2 {
		return nil
	}
	if !p.budget.pending || !now.Before(p.budget.deadline) || !bytes.Equal(body, p.budget.frame.packet[wirev2.PrefixSize+wirev2.RouteSize:p.budget.frame.length-wirev2.AuthenticationTagSize]) {
		return ErrProtocol
	}
	p.sendBudget, p.sendEpoch, p.budget.pending = budget, 2, false
	if p.id == c.setup.PathID && !c.context.pending && !c.contextAcknowledged {
		packet, err := wirev2.AppendEncodingContext(nil, c.setup.ID, p.route(), c.sendContext, c.sendKey)
		if err != nil {
			return err
		}
		c.context = controlRetry{pending: true, next: now, deadline: now.Add(ControlLifetime)}
		return c.context.frame.set(packet)
	}
	return nil
}

func (c *Controller) receiveEncoding(p *pathState, now time.Time, envelope wirev2.AuthenticatedEnvelope) error {
	if envelope.Header().Type == wirev2.TypeEncodingContextAck {
		_, ack, err := wirev2.DecodeEncodingContextAck(envelope)
		if err != nil {
			return err
		}
		if err := ack.ValidateContext(c.sendContext); err != nil {
			return err
		}
		if c.contextAcknowledged {
			return nil
		}
		if !c.context.pending || !now.Before(c.context.deadline) {
			return ErrProtocol
		}
		c.contextAcknowledged, c.context.pending = true, false
		return nil
	}
	_, context, err := wirev2.DecodeEncodingContext(envelope)
	if err != nil {
		return err
	}
	if context.Epoch != 1 || context.DataShards != c.remote.DataShards || context.ParityShards != c.remote.ParityShards || context.MaxDescriptors > c.cfg.LocalProfile.Datagram.MaxDescriptors || uint32(context.ShardBytes)+94 > uint32(p.receiveBudget) || p.receiveEpoch != 2 {
		return ErrProtocol
	}
	if c.receiveContext.Epoch != 0 && c.receiveContext != context {
		return ErrProtocol
	}
	if c.receiveContext.Epoch == 0 {
		codec, err := fecv2.New(parameters(context, c.cfg.LocalProfile.Datagram.MaxDatagramBytes))
		if err != nil {
			return err
		}
		c.receiveContext, c.receiveCodec = context, codec
	}
	ack, err := wirev2.NewEncodingContextAck(context)
	if err != nil {
		return err
	}
	packet, err := wirev2.AppendEncodingContextAck(nil, c.setup.ID, p.route(), ack, c.sendKey)
	if err != nil {
		return err
	}
	return p.replies[1].set(packet)
}

func allZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}

func (c *Controller) emit(p *pathState, sender transport.ReplyPath, binding handshakev2.Binding, packet []byte, now time.Time, result *Result) bool {
	if len(result.Sends) >= MaxSendsPerStep || now.Before(p.pacedAt) {
		return false
	}
	overhead := uint64(28)
	if binding.Local.Addr().Is6() {
		overhead = 48
	}
	bits := (uint64(len(packet)) + overhead) * 8
	nanoseconds := (bits*uint64(time.Second) + p.rate - 1) / p.rate
	p.pacedAt = now.Add(time.Duration(nanoseconds))
	err := c.cfg.Emit(sender, packet)
	result.Sends = append(result.Sends, SendAttempt{Type: wirev2.PacketType(packet[5]), PathID: p.id, Bytes: len(packet), Err: err})
	return true
}

func (c *Controller) sendRetry(p *pathState, retry *controlRetry, sender transport.ReplyPath, binding handshakev2.Binding, now time.Time, result *Result, firstPhase bool) {
	limit := ControlSends
	if firstPhase {
		limit--
	}
	if !retry.pending || retry.sends >= limit || now.Before(retry.next) || !now.Before(retry.deadline) {
		return
	}
	if c.emit(p, sender, binding, retry.frame.packet[:retry.frame.length], now, result) {
		retry.sends++
		retry.next = now.Add(ControlRetry)
	}
}

func (c *Controller) driveControl(now time.Time, result *Result) {
	for offset := 0; offset < len(c.paths) && len(result.Sends) < MaxSendsPerStep; offset++ {
		p := &c.paths[(c.controlCursor+offset)%len(c.paths)]
		if p.join.pending {
			first := p.join.kind == wirev2.TypePathJoin || p.join.kind == wirev2.TypePathChallenge
			c.sendRetry(p, &p.join.controlRetry, p.join.sender, p.join.binding, now, result, first)
		}
		if !p.active {
			continue
		}
		for i := range p.replies {
			frame := &p.replies[i]
			if frame.queued && c.emit(p, p.sender, p.binding, frame.packet[:frame.length], now, result) {
				frame.queued = false
				if wirev2.PacketType(frame.packet[5]) == wirev2.TypeEncodingContextAck && result.Sends[len(result.Sends)-1].Err == nil {
					c.receiveAckSent = true
				}
			}
		}
		c.sendRetry(p, &p.budget, p.sender, p.binding, now, result, false)
		if p.id == c.setup.PathID {
			c.sendRetry(p, &c.context, p.sender, p.binding, now, result, false)
		}
	}
	if len(c.paths) > 0 {
		c.controlCursor = (c.controlCursor + 1) % len(c.paths)
	}
}

func (c *Controller) expireControl(now time.Time) error {
	if c.context.pending && !now.Before(c.context.deadline) {
		return ErrExpired
	}
	for i := range c.paths {
		p := &c.paths[i]
		if !p.join.deadline.IsZero() && !now.Before(p.join.deadline) {
			p.join = pathJoin{}
		}
		if p.budget.pending && !now.Before(p.budget.deadline) {
			if p.id == c.setup.PathID {
				return ErrExpired
			}
			p.active, p.budget.pending = false, false
		}
		for j := range p.old {
			if !now.Before(p.old[j].until) {
				p.old[j] = oldRoute{}
			}
		}
	}
	return nil
}

func (c *Controller) NextDeadline() time.Time {
	if c == nil || c.closed || !c.started {
		return time.Time{}
	}
	var next time.Time
	include := func(value time.Time) {
		if !value.IsZero() && (next.IsZero() || value.Before(next)) {
			next = value
		}
	}
	retry := func(p *pathState, state *controlRetry, limit int) {
		if state.pending {
			include(state.deadline)
			if state.sends < limit {
				include(later(state.next, p.pacedAt))
			}
		}
	}
	for i := range c.paths {
		p := &c.paths[i]
		limit := ControlSends
		if p.join.kind == wirev2.TypePathJoin || p.join.kind == wirev2.TypePathChallenge {
			limit--
		}
		retry(p, &p.join.controlRetry, limit)
		if p.join.committed {
			include(p.join.deadline)
		}
		retry(p, &p.budget, ControlSends)
		if p.id == c.setup.PathID {
			retry(p, &c.context, ControlSends)
		}
		for _, frame := range p.replies {
			if frame.queued {
				include(later(c.last, p.pacedAt))
			}
		}
		for _, old := range p.old {
			include(old.until)
		}
	}
	for _, group := range c.groups {
		include(group.admitted.Add(c.cfg.GroupTimeout))
		if group.fragments != nil {
			include(later(c.last, c.retryStorage))
		}
	}
	if c.originals != nil {
		include(c.originals.NextDeadline())
	}
	if c.ready() {
		if c.out != nil {
			p := &c.paths[c.outPaths[c.outNext]-1]
			include(later(later(c.last, p.pacedAt), c.retryStorage))
		} else if q := c.queue.Snapshot(); q.QueuedDatagrams > 0 {
			due := q.OldestDeadline
			if c.forced > c.completed {
				due = c.last
			}
			include(later(due, c.retryStorage))
		}
	}
	return next
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
