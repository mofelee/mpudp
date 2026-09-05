package session

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mofelee/mpudp/internal/fec"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wire"
)

type carrierState struct {
	path      Path
	attempts  int
	acked     bool
	exhausted bool
	nextRetry time.Time
	healthy   bool
}

type endpointState struct {
	key          string
	path         ReplyPath
	lastActivity time.Time
	rtt          time.Duration
	hasRTT       bool
	healthy      bool
	statistics   *transport.Counters
}

type outstandingProbe struct {
	token     uint64
	timestamp uint64
	sentAt    time.Time
}

type sendPlan struct {
	message  wire.Message
	path     Path
	budget   int
	probeKey string
	probe    outstandingProbe
}

// Session is one concurrency-safe, authenticated protocol state machine.
// It owns no UDP socket and starts no goroutine. Advance executes all logical
// deadlines due at Clock.Now, allowing one bounded driver to serve many
// Sessions in the integration layer.
type Session struct {
	mu       sync.Mutex
	active   sync.WaitGroup
	id       wire.SessionID
	role     Role
	settings *settings
	state    State
	started  bool

	carriers    []*carrierState
	carrierByID map[string]*carrierState
	endpoints   map[string]*endpointState
	outstanding map[string]outstandingProbe
	nextToken   uint64

	peerMaxUDPPayload    int
	sendMaxUDPPayload    int
	receiveMaxUDPPayload int
	encoder              *fec.Encoder
	decoder              *fec.Decoder

	nextKeepalive   time.Time
	nextDecodeSweep time.Time

	lifetime  context.Context
	cancel    context.CancelFunc
	onClose   func(*Session)
	closeOnce sync.Once
	closeErr  error
}

// NewInitiator constructs a handshaking Session over ordered, long-lived
// Carrier paths. Start performs the first HELLO round.
func NewInitiator(id wire.SessionID, config Config, carriers []Path) (*Session, error) {
	settings, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	if id == (wire.SessionID{}) {
		return nil, invalidConfig("Session ID must not be zero")
	}
	if len(carriers) == 0 || len(carriers) > maxPaths {
		return nil, invalidConfig("initiator Carrier count must be in [1, 256]")
	}
	states := make([]*carrierState, 0, len(carriers))
	byID := make(map[string]*carrierState, len(carriers))
	for _, path := range carriers {
		if path == nil || path.PathID() == "" {
			return nil, invalidConfig("initiator Carrier paths must have nonempty IDs")
		}
		if _, exists := byID[path.PathID()]; exists {
			return nil, invalidConfig("initiator Carrier path IDs must be unique")
		}
		state := &carrierState{path: path, healthy: true}
		states = append(states, state)
		byID[path.PathID()] = state
	}
	lifetime, cancel := context.WithCancel(context.Background())
	return &Session{
		id:          id,
		role:        RoleInitiator,
		settings:    settings,
		state:       StateHandshaking,
		carriers:    states,
		carrierByID: byID,
		endpoints:   make(map[string]*endpointState),
		outstanding: make(map[string]outstandingProbe),
		nextToken:   initialToken(id),
		lifetime:    lifetime,
		cancel:      cancel,
	}, nil
}

// ID returns the immutable wire Session ID.
func (s *Session) ID() wire.SessionID { return s.id }

// Start sends the initial HELLO once on every configured Carrier. Individual
// path failures are reported without preventing the remaining attempts.
func (s *Session) Start(ctx context.Context) ([]SendAttempt, error) {
	if ctx == nil {
		return nil, invalidConfig("nil Start context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := s.settings.clock.Now()
	s.mu.Lock()
	if s.state == StateClosed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	if s.role != RoleInitiator || s.state != StateHandshaking {
		s.mu.Unlock()
		return nil, ErrUnexpectedPacket
	}
	if s.started {
		s.mu.Unlock()
		return nil, nil
	}
	s.started = true
	plans := make([]sendPlan, 0, len(s.carriers))
	for _, carrier := range s.carriers {
		plan, err := s.planHelloLocked(carrier, now)
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		plans = append(plans, plan)
	}
	s.active.Add(1)
	s.mu.Unlock()
	defer s.active.Done()
	return s.executePlans(ctx, plans), nil
}

// HandleTransportPacket adapts and processes a transport receive callback.
func (s *Session) HandleTransportPacket(ctx context.Context, packet transport.ReceivedPacket) (HandleResult, error) {
	return s.HandlePacket(ctx, NewReceivedPacket(packet))
}

// HandlePacket authenticates raw bytes before it reads or changes any mutable
// Session state. It returns authenticated DATA completion to the integration
// layer and sends HELLO_ACK/PONG responses through the exact receive path.
func (s *Session) HandlePacket(ctx context.Context, packet ReceivedPacket) (HandleResult, error) {
	if ctx == nil {
		return HandleResult{}, invalidConfig("nil HandlePacket context")
	}
	if err := ctx.Err(); err != nil {
		return HandleResult{}, err
	}
	message, err := wire.DecodeAuthenticated(packet.Payload, s.settings.psk, s.settings.localMaxUDPPayload)
	if err != nil {
		return HandleResult{}, err
	}
	return s.handleAuthenticated(ctx, packet, message)
}

func (s *Session) handleAuthenticated(ctx context.Context, packet ReceivedPacket, message wire.Message) (HandleResult, error) {
	result := HandleResult{Message: message}
	if message.Header.SessionID != s.id {
		return result, ErrUnknownSession
	}
	key, err := replyIdentity(packet.Reply)
	if err != nil {
		return result, err
	}
	now := s.settings.clock.Now()

	s.mu.Lock()
	if s.state == StateClosed {
		s.mu.Unlock()
		return result, ErrClosed
	}
	if s.role == RoleInitiator {
		if _, exists := s.carrierByID[packet.Reply.PathID()]; !exists {
			s.mu.Unlock()
			return result, ErrUnknownPath
		}
	}
	if s.state == StateEstablished && len(packet.Payload) > s.receiveMaxUDPPayload {
		s.mu.Unlock()
		return result, ErrPacketOverBudget
	}

	var response *sendPlan
	var decoderToClose *fec.Decoder
	var notifyClose bool
	switch message.Header.Type {
	case wire.TypeHello:
		if s.role != RoleListener || s.state != StateEstablished {
			err = ErrUnexpectedPacket
			break
		}
		if err = s.validateFrozenHandshakeLocked(message.Handshake); err != nil {
			break
		}
		result.EndpointAdded, result.EndpointRefreshed, err = s.learnEndpointLocked(key, packet.Reply, now)
		if err != nil {
			break
		}
		ack, makeErr := wire.NewHelloAck(s.id, uint8(s.settings.params.DataShards), uint8(s.settings.params.ParityShards), uint16(s.settings.localMaxUDPPayload))
		if makeErr != nil {
			err = makeErr
			break
		}
		response = &sendPlan{message: ack, path: s.endpoints[key].path, budget: s.sendMaxUDPPayload}
	case wire.TypeHelloAck:
		if s.role != RoleInitiator || (s.state != StateHandshaking && s.state != StateEstablished) {
			err = ErrUnexpectedPacket
			break
		}
		carrier := s.carrierByID[packet.Reply.PathID()]
		if s.state == StateHandshaking {
			err = s.establishLocked(message.Handshake, now)
			if err != nil {
				break
			}
			result.Established = true
		} else if err = s.validateFrozenHandshakeLocked(message.Handshake); err != nil {
			break
		}
		result.EndpointAdded, result.EndpointRefreshed, err = s.learnEndpointLocked(key, packet.Reply, now)
		if err == nil {
			carrier.acked = true
			carrier.healthy = true
			carrier.exhausted = false
			carrier.nextRetry = time.Time{}
		}
	case wire.TypeDataShard:
		if s.state != StateEstablished {
			err = ErrNotEstablished
			break
		}
		if int(message.DataShard.DataShards) != s.settings.params.DataShards || int(message.DataShard.ParityShards) != s.settings.params.ParityShards {
			err = ErrHandshakeIncompatible
			break
		}
		if err = s.canLearnEndpointLocked(key, packet.Reply, now); err != nil {
			break
		}
		decoded, decodeErr := s.decoder.AddVerifiedShard(fec.IncomingShard{
			Key:    fec.BlockKey{SessionID: [16]byte(s.id), PacketID: message.DataShard.PacketID},
			Params: s.settings.params, Index: int(message.DataShard.ShardIndex),
			OriginalLength: int(message.DataShard.OriginalLength), Payload: message.DataShard.Payload,
		})
		if decodeErr != nil {
			err = decodeErr
			break
		}
		if decoded.Outcome == fec.OutcomeTooOld {
			s.mu.Unlock()
			return result, nil
		}
		result.EndpointAdded, result.EndpointRefreshed, err = s.learnEndpointLocked(key, packet.Reply, now)
		if err == nil && decoded.Outcome == fec.OutcomeComplete {
			result.Datagram = decoded.Datagram
		}
	case wire.TypePing:
		if s.state != StateEstablished {
			err = ErrNotEstablished
			break
		}
		result.EndpointAdded, result.EndpointRefreshed, err = s.learnEndpointLocked(key, packet.Reply, now)
		if err != nil {
			break
		}
		pong, makeErr := wire.NewPong(s.id, message.Probe.Token, message.Probe.Timestamp)
		if makeErr != nil {
			err = makeErr
			break
		}
		response = &sendPlan{message: pong, path: s.endpoints[key].path, budget: s.sendMaxUDPPayload}
	case wire.TypePong:
		if s.state != StateEstablished {
			err = ErrNotEstablished
			break
		}
		probeKey := s.probeKeyLocked(packet.Reply, key)
		probe, exists := s.outstanding[probeKey]
		if !exists || probe.token != message.Probe.Token || probe.timestamp != message.Probe.Timestamp || now.Before(probe.sentAt) {
			err = ErrProbeMismatch
			break
		}
		if err = s.canLearnEndpointLocked(key, packet.Reply, now); err != nil {
			break
		}
		delete(s.outstanding, probeKey)
		result.EndpointAdded, result.EndpointRefreshed, err = s.learnEndpointLocked(key, packet.Reply, now)
		if err == nil {
			result.RTTUpdated = true
			result.RTT = now.Sub(probe.sentAt)
			endpoint := s.endpoints[key]
			endpoint.rtt = result.RTT
			endpoint.hasRTT = true
		}
	case wire.TypeClose:
		s.receiveEndpointLocked(key, len(packet.Payload))
		decoderToClose = s.decoder
		s.decoder = nil
		s.encoder = nil
		s.markClosedLocked()
		notifyClose = true
	default:
		err = ErrUnexpectedPacket
	}
	if err != nil {
		s.mu.Unlock()
		return result, err
	}
	s.receiveEndpointLocked(key, len(packet.Payload))
	if response != nil {
		s.active.Add(1)
	}
	s.mu.Unlock()

	if decoderToClose != nil {
		_ = decoderToClose.Close()
	}
	if notifyClose {
		s.notifyClosed()
	}
	if response != nil {
		attempts := s.executePlans(ctx, []sendPlan{*response})
		s.active.Done()
		result.Response = &attempts[0]
	}
	return result, nil
}

// Advance expires Endpoint/FEC state, performs due handshake retries, and
// sends one PING on every current path at each keepalive interval.
func (s *Session) Advance(ctx context.Context) (AdvanceResult, error) {
	if ctx == nil {
		return AdvanceResult{}, invalidConfig("nil Advance context")
	}
	if err := ctx.Err(); err != nil {
		return AdvanceResult{}, err
	}
	now := s.settings.clock.Now()
	var result AdvanceResult
	s.mu.Lock()
	if s.state == StateClosed {
		s.mu.Unlock()
		return result, ErrClosed
	}
	result.ExpiredEndpoints = s.sweepEndpointsLocked(now)
	if s.decoder != nil && !s.nextDecodeSweep.IsZero() && !now.Before(s.nextDecodeSweep) {
		result.ExpiredFEC = s.decoder.Sweep()
		s.nextDecodeSweep = now.Add(s.settings.decodeTimeout)
	}

	plans := make([]sendPlan, 0, len(s.carriers)+len(s.endpoints))
	if s.role == RoleInitiator && s.started {
		for _, carrier := range s.carriers {
			if carrier.acked || carrier.exhausted || carrier.nextRetry.IsZero() || now.Before(carrier.nextRetry) {
				continue
			}
			if carrier.attempts >= s.settings.maxHandshakeAttempts {
				carrier.exhausted = true
				carrier.healthy = false
				carrier.nextRetry = time.Time{}
				continue
			}
			plan, planErr := s.planHelloLocked(carrier, now)
			if planErr != nil {
				s.mu.Unlock()
				return result, planErr
			}
			plans = append(plans, plan)
		}
		if s.state == StateHandshaking && s.allCarriersExhaustedLocked() {
			s.state = StateHandshakeFailed
			result.HandshakeFailed = true
		}
	}
	if s.state == StateEstablished && !s.nextKeepalive.IsZero() && !now.Before(s.nextKeepalive) {
		paths := s.keepalivePathsLocked(now)
		for _, path := range paths {
			plan, planErr := s.planPingLocked(path, now)
			if planErr != nil {
				s.mu.Unlock()
				return result, planErr
			}
			plans = append(plans, plan)
		}
		s.nextKeepalive = now.Add(s.settings.keepaliveInterval)
	}
	if len(plans) != 0 {
		s.active.Add(1)
	}
	s.mu.Unlock()

	if len(plans) != 0 {
		result.Sends = s.executePlans(ctx, plans)
		s.active.Done()
	}
	if result.HandshakeFailed {
		return result, ErrHandshakeFailed
	}
	return result, nil
}

// WritePacket encodes one Datagram into one FEC block, authenticates every
// shard, and sends every shard exactly once through the current path snapshot.
func (s *Session) WritePacket(ctx context.Context, payload []byte) (WriteResult, error) {
	if ctx == nil {
		return WriteResult{}, invalidConfig("nil WritePacket context")
	}
	if err := ctx.Err(); err != nil {
		return WriteResult{}, err
	}
	now := s.settings.clock.Now()
	s.mu.Lock()
	if s.state == StateClosed {
		s.mu.Unlock()
		return WriteResult{}, ErrClosed
	}
	if s.state != StateEstablished || s.encoder == nil {
		s.mu.Unlock()
		return WriteResult{}, ErrNotEstablished
	}
	s.sweepEndpointsLocked(now)
	paths := s.sendPathsLocked(now)
	encoder := s.encoder
	budget := s.sendMaxUDPPayload
	s.active.Add(1)
	s.mu.Unlock()
	defer s.active.Done()

	block, err := encoder.Encode(payload)
	if err != nil {
		return WriteResult{}, err
	}
	packets := make([][]byte, len(block.Shards))
	for index, shard := range block.Shards {
		message, makeErr := wire.NewDataShard(s.id, block.PacketID, uint8(block.Params.DataShards), uint8(block.Params.ParityShards), uint8(index), uint32(block.OriginalLength), shard)
		if makeErr != nil {
			return WriteResult{PacketID: block.PacketID}, makeErr
		}
		packets[index], makeErr = wire.AppendAuthenticated(nil, message, s.settings.psk, budget)
		if makeErr != nil {
			return WriteResult{PacketID: block.PacketID}, makeErr
		}
	}
	transportPaths := make([]transport.Path, len(paths))
	for index, path := range paths {
		transportPaths[index] = path
	}
	sendContext, cancel := s.operationContextWithCancel(ctx)
	defer cancel()
	send, err := transport.SendBlock(sendContext, block.PacketID, packets, transportPaths)
	s.recordBlockPathFailures(send, paths)
	return WriteResult{PacketID: block.PacketID, Send: send}, err
}

// Close cancels in-flight work, removes retained state, and makes one
// best-effort CLOSE attempt on every current path. It never waits for a peer ACK.
func (s *Session) Close(ctx context.Context) error {
	if ctx == nil {
		return invalidConfig("nil Close context")
	}
	s.closeOnce.Do(func() {
		now := s.settings.clock.Now()
		s.mu.Lock()
		wasEstablished := s.state == StateEstablished
		budget := s.sendMaxUDPPayload
		paths := s.closePathsLocked(now)
		decoder := s.decoder
		s.decoder = nil
		s.encoder = nil
		s.markClosedLocked()
		s.mu.Unlock()

		s.active.Wait()
		if decoder != nil {
			_ = decoder.Close()
		}
		if wasEstablished {
			message, err := wire.NewClose(s.id)
			if err != nil {
				s.closeErr = err
			} else {
				plans := make([]sendPlan, 0, len(paths))
				for _, path := range paths {
					plans = append(plans, sendPlan{message: message, path: path, budget: budget})
				}
				attempts := s.executePlansWithoutLifetime(ctx, plans)
				var failures []error
				for _, attempt := range attempts {
					if attempt.Err != nil {
						failures = append(failures, attempt.Err)
					}
				}
				s.closeErr = errors.Join(failures...)
			}
		}
		s.notifyClosed()
	})
	return s.closeErr
}

func (s *Session) establishLocked(handshake wire.Handshake, now time.Time) error {
	if err := s.validateHandshake(handshake); err != nil {
		return err
	}
	negotiated := min(s.settings.localMaxUDPPayload, int(handshake.MaxUDPPayload))
	budget := fec.Budget{MaxUDPPayload: negotiated, DataShardWireOverhead: wire.DataShardOverhead, MaxDatagramSize: s.settings.maxDatagramSize}
	encoder, err := fec.NewEncoder(s.settings.params, budget)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrHandshakeIncompatible, err)
	}
	decoder, err := fec.NewDecoder(fec.DecoderConfig{
		Params: s.settings.params, Budget: budget, DecodeTimeout: s.settings.decodeTimeout,
		CompletionTTL: s.settings.completionTTL, MaxPendingBlocks: s.settings.maxPendingFECBlocks,
		MaxCompletedBlocks: s.settings.maxCompletedFECBlocks, Clock: s.settings.clock,
		Statistics:   s.settings.fecStatistics,
		ReplayWindow: &fec.ReplayWindowConfig{SessionID: [16]byte(s.id)},
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrHandshakeIncompatible, err)
	}
	s.peerMaxUDPPayload = int(handshake.MaxUDPPayload)
	s.sendMaxUDPPayload = negotiated
	s.receiveMaxUDPPayload = negotiated
	s.encoder = encoder
	s.decoder = decoder
	s.state = StateEstablished
	s.nextKeepalive = now.Add(s.settings.keepaliveInterval)
	s.nextDecodeSweep = now.Add(s.settings.decodeTimeout)
	return nil
}

func (s *Session) validateHandshake(handshake wire.Handshake) error {
	if int(handshake.DataShards) != s.settings.params.DataShards || int(handshake.ParityShards) != s.settings.params.ParityShards {
		return ErrHandshakeIncompatible
	}
	if int(handshake.MaxUDPPayload) < wire.MinUDPPayload || int(handshake.MaxUDPPayload) > wire.MaxUDPPayload {
		return ErrHandshakeIncompatible
	}
	return nil
}

func (s *Session) validateFrozenHandshakeLocked(handshake wire.Handshake) error {
	if err := s.validateHandshake(handshake); err != nil {
		return err
	}
	if int(handshake.MaxUDPPayload) != s.peerMaxUDPPayload {
		return ErrHandshakeIncompatible
	}
	return nil
}

func (s *Session) planHelloLocked(carrier *carrierState, now time.Time) (sendPlan, error) {
	message, err := wire.NewHello(s.id, uint8(s.settings.params.DataShards), uint8(s.settings.params.ParityShards), uint16(s.settings.localMaxUDPPayload))
	if err != nil {
		return sendPlan{}, err
	}
	carrier.attempts++
	carrier.nextRetry = now.Add(s.settings.handshakeRetryInterval + s.retryJitter(carrier.path.PathID(), carrier.attempts))
	return sendPlan{message: message, path: carrier.path, budget: s.settings.localMaxUDPPayload}, nil
}

func (s *Session) planPingLocked(path Path, now time.Time) (sendPlan, error) {
	token := s.nextToken
	s.nextToken++
	if s.nextToken == 0 {
		s.nextToken = 1
	}
	timestamp := uint64(now.UnixNano())
	message, err := wire.NewPing(s.id, token, timestamp)
	if err != nil {
		return sendPlan{}, err
	}
	key := s.probePathKeyLocked(path)
	probe := outstandingProbe{token: token, timestamp: timestamp, sentAt: now}
	s.outstanding[key] = probe
	return sendPlan{message: message, path: path, budget: s.sendMaxUDPPayload, probeKey: key, probe: probe}, nil
}

func (s *Session) executePlans(ctx context.Context, plans []sendPlan) []SendAttempt {
	ctx, cancel := s.operationContextWithCancel(ctx)
	defer cancel()
	return s.execute(ctx, plans, true)
}

func (s *Session) executePlansWithoutLifetime(ctx context.Context, plans []sendPlan) []SendAttempt {
	return s.execute(ctx, plans, false)
}

func (s *Session) execute(ctx context.Context, plans []sendPlan, cleanFailedProbes bool) []SendAttempt {
	attempts := make([]SendAttempt, 0, len(plans))
	for _, plan := range plans {
		attempt := SendAttempt{Type: plan.message.Header.Type, PathID: plan.path.PathID()}
		packet, err := wire.AppendAuthenticated(nil, plan.message, s.settings.psk, plan.budget)
		if err == nil {
			err = plan.path.Send(ctx, packet)
		}
		attempt.Err = err
		attempts = append(attempts, attempt)
		if cleanFailedProbes && err != nil && plan.probeKey != "" {
			s.mu.Lock()
			if current, exists := s.outstanding[plan.probeKey]; exists && current.token == plan.probe.token {
				delete(s.outstanding, plan.probeKey)
			}
			s.mu.Unlock()
		}
		if isPathHealthFailure(err) {
			s.setPathHealthForPath(plan.path, false)
		}
	}
	return attempts
}

func (s *Session) operationContextWithCancel(ctx context.Context) (context.Context, context.CancelFunc) {
	result, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.lifetime, cancel)
	return result, func() {
		stop()
		cancel()
	}
}

func (s *Session) retryJitter(pathID string, attempt int) time.Duration {
	limit := s.settings.handshakeJitterLimit
	if limit <= 0 {
		return 0
	}
	hasher := sha256.New()
	_, _ = hasher.Write(s.id[:])
	_, _ = hasher.Write([]byte(pathID))
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(attempt))
	_, _ = hasher.Write(encoded[:])
	sum := hasher.Sum(nil)
	return time.Duration(binary.BigEndian.Uint64(sum[:8]) % (uint64(limit) + 1))
}

func (s *Session) allCarriersExhaustedLocked() bool {
	for _, carrier := range s.carriers {
		if carrier.acked {
			return false
		}
		if !carrier.exhausted {
			return false
		}
	}
	return true
}

func (s *Session) keepalivePathsLocked(now time.Time) []Path {
	if s.role == RoleInitiator {
		paths := make([]Path, 0, len(s.carriers))
		for _, carrier := range s.carriers {
			if carrier.path.Available() {
				paths = append(paths, carrier.path)
			}
		}
		return paths
	}
	keys := make([]string, 0, len(s.endpoints))
	for key, endpoint := range s.endpoints {
		if now.Before(endpoint.lastActivity.Add(s.settings.endpointTTL)) && endpoint.path.Available() {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	paths := make([]Path, 0, len(keys))
	for _, key := range keys {
		paths = append(paths, s.endpoints[key].path)
	}
	return paths
}

func (s *Session) sendPathsLocked(now time.Time) []Path {
	if s.state != StateEstablished {
		return nil
	}
	if s.role == RoleInitiator {
		paths := make([]Path, 0, len(s.carriers))
		for _, carrier := range s.carriers {
			if carrier.healthy && carrier.path.Available() {
				paths = append(paths, carrier.path)
			}
		}
		return paths
	}
	keys := make([]string, 0, len(s.endpoints))
	for key, endpoint := range s.endpoints {
		if now.Before(endpoint.lastActivity.Add(s.settings.endpointTTL)) && endpoint.healthy && endpoint.path.Available() {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	paths := make([]Path, 0, len(keys))
	for _, key := range keys {
		paths = append(paths, s.endpoints[key].path)
	}
	return paths
}

func (s *Session) closePathsLocked(now time.Time) []Path {
	if s.role == RoleInitiator {
		paths := make([]Path, 0, len(s.carriers))
		for _, carrier := range s.carriers {
			if carrier.path.Available() {
				paths = append(paths, carrier.path)
			}
		}
		return paths
	}
	keys := make([]string, 0, len(s.endpoints))
	for key, endpoint := range s.endpoints {
		if now.Before(endpoint.lastActivity.Add(s.settings.endpointTTL)) && endpoint.path.Available() {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	paths := make([]Path, 0, len(keys))
	for _, key := range keys {
		paths = append(paths, s.endpoints[key].path)
	}
	return paths
}

func (s *Session) canLearnEndpointLocked(key string, path ReplyPath, now time.Time) error {
	s.sweepEndpointsLocked(now)
	if _, exists := s.endpoints[key]; exists {
		return nil
	}
	if s.role == RoleInitiator {
		for oldKey, endpoint := range s.endpoints {
			if endpoint.path.PathID() == path.PathID() && !endpoint.path.Available() {
				delete(s.endpoints, oldKey)
			}
		}
	}
	if len(s.endpoints) >= s.settings.maxEndpoints {
		return ErrEndpointLimit
	}
	return nil
}

func (s *Session) learnEndpointLocked(key string, path ReplyPath, now time.Time) (bool, bool, error) {
	if err := s.canLearnEndpointLocked(key, path, now); err != nil {
		return false, false, err
	}
	var counters *transport.Counters
	if s.role == RoleListener {
		if endpoint := s.endpoints[key]; endpoint != nil {
			counters = endpoint.statistics
		} else {
			counters = s.settings.listenerPathStatistics.Learn(key)
		}
		path = transport.WithReplyStatistics(path, counters)
	}
	if endpoint := s.endpoints[key]; endpoint != nil {
		endpoint.path = path
		endpoint.statistics = counters
		endpoint.lastActivity = now
		endpoint.healthy = true
		if carrier := s.carrierByID[path.PathID()]; carrier != nil {
			carrier.healthy = true
		}
		return false, true, nil
	}
	s.endpoints[key] = &endpointState{key: key, path: path, lastActivity: now, healthy: true, statistics: counters}
	if carrier := s.carrierByID[path.PathID()]; carrier != nil {
		carrier.healthy = true
	}
	return true, false, nil
}

func (s *Session) receiveEndpointLocked(key string, size int) {
	if endpoint := s.endpoints[key]; endpoint != nil {
		endpoint.statistics.ReceiveAccepted(size)
	}
}

func (s *Session) sweepEndpointsLocked(now time.Time) int {
	expired := 0
	for key, endpoint := range s.endpoints {
		if !now.Before(endpoint.lastActivity.Add(s.settings.endpointTTL)) {
			delete(s.endpoints, key)
			if s.role == RoleListener {
				delete(s.outstanding, key)
			} else if carrier := s.carrierByID[endpoint.path.PathID()]; carrier != nil {
				carrier.healthy = false
			}
			expired++
		}
	}
	return expired
}

func (s *Session) probeKeyLocked(path ReplyPath, endpointKey string) string {
	if s.role == RoleInitiator {
		return path.PathID()
	}
	return endpointKey
}

func (s *Session) probePathKeyLocked(path Path) string {
	if s.role == RoleInitiator {
		return path.PathID()
	}
	if reply, ok := path.(ReplyPath); ok {
		if key, err := replyIdentity(reply); err == nil {
			return key
		}
	}
	return path.PathID()
}

func (s *Session) markClosedLocked() {
	s.state = StateClosed
	s.cancel()
	s.nextKeepalive = time.Time{}
	s.nextDecodeSweep = time.Time{}
	for _, carrier := range s.carriers {
		carrier.nextRetry = time.Time{}
	}
	clear(s.endpoints)
	clear(s.outstanding)
}

func (s *Session) notifyClosed() {
	if s.onClose != nil {
		s.onClose(s)
	}
}

func initialToken(id wire.SessionID) uint64 {
	sum := sha256.Sum256(id[:])
	token := binary.BigEndian.Uint64(sum[:8])
	if token == 0 {
		return 1
	}
	return token
}

func replyIdentity(path ReplyPath) (string, error) {
	key, err := replyIdentityUnchecked(path)
	if err != nil || !path.Available() {
		return "", ErrInvalidReplyPath
	}
	return key, nil
}

func replyIdentityUnchecked(path ReplyPath) (string, error) {
	if path == nil {
		return "", ErrInvalidReplyPath
	}
	pathID := path.PathID()
	local := path.LocalAddr()
	remote := path.RemoteAddr()
	if pathID == "" || local == nil || remote == nil {
		return "", ErrInvalidReplyPath
	}
	parts := []string{
		pathID,
		strconv.FormatUint(path.Generation(), 10),
		local.Network(), local.String(),
		remote.Network(), remote.String(),
	}
	return strings.Join(parts, "\x00"), nil
}

// Snapshot returns immutable state without expiring entries.
func (s *Session) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	attempts := 0
	acked := 0
	for _, carrier := range s.carriers {
		attempts += carrier.attempts
		if carrier.acked {
			acked++
		}
	}
	return Snapshot{
		ID: s.id, Role: s.role, State: s.state,
		PeerMaxUDPPayload: s.peerMaxUDPPayload, SendMaxUDPPayload: s.sendMaxUDPPayload,
		ReceiveMaxUDPPayload: s.receiveMaxUDPPayload, HandshakeAttempts: attempts,
		AcknowledgedCarriers: acked, Endpoints: len(s.endpoints), OutstandingProbes: len(s.outstanding),
		HasRetryDeadline: s.hasRetryDeadlineLocked(), HasKeepaliveDeadline: !s.nextKeepalive.IsZero(),
		HasDecodeSweepDeadline: !s.nextDecodeSweep.IsZero(), NextDeadline: s.nextDeadlineLocked(),
	}
}

// Endpoints returns a canonical key-ordered diagnostic snapshot.
func (s *Session) Endpoints() []EndpointSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.endpoints))
	for key := range s.endpoints {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]EndpointSnapshot, 0, len(keys))
	for _, key := range keys {
		endpoint := s.endpoints[key]
		result = append(result, EndpointSnapshot{
			Key: key, PathID: endpoint.path.PathID(), Generation: endpoint.path.Generation(),
			LocalAddr: endpoint.path.LocalAddr().String(), RemoteAddr: endpoint.path.RemoteAddr().String(),
			LastActivity: endpoint.lastActivity, RTT: endpoint.rtt, HasRTT: endpoint.hasRTT,
			Available: endpoint.healthy && endpoint.path.Available(),
		})
	}
	return result
}

// SendPaths returns the ordered currently usable scheduler paths.
func (s *Session) SendPaths() []Path {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.settings.clock.Now()
	s.sweepEndpointsLocked(now)
	return append([]Path(nil), s.sendPathsLocked(now)...)
}

// SetPathHealthy is the bounded recovery hook used after transport rebuild or
// operator diagnosis. A PMTU send error marks only that path unhealthy; false
// removes it from DATA scheduling without closing the Session. Authenticated
// traffic on the path also restores health.
func (s *Session) SetPathHealthy(pathID string, healthy bool) error {
	if pathID == "" {
		return ErrUnknownPath
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == StateClosed {
		return ErrClosed
	}
	found := false
	if carrier := s.carrierByID[pathID]; carrier != nil {
		carrier.healthy = healthy
		found = true
	}
	for _, endpoint := range s.endpoints {
		if endpoint.path.PathID() == pathID || endpoint.key == pathID {
			endpoint.healthy = healthy
			found = true
		}
	}
	if !found {
		return ErrUnknownPath
	}
	return nil
}

func (s *Session) recordBlockPathFailures(result transport.BlockSendResult, paths []Path) {
	for _, attempt := range result.Attempts {
		if isPathHealthFailure(attempt.Err) && attempt.PathIndex >= 0 && attempt.PathIndex < len(paths) {
			s.setPathHealthForPath(paths[attempt.PathIndex], false)
		}
	}
}

func isPathHealthFailure(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var pathError *transport.PathError
	return errors.As(err, &pathError) || errors.Is(err, transport.ErrPathUnavailable) || errors.Is(err, transport.ErrPathMTUExceeded)
}

func (s *Session) setPathHealthForPath(path Path, healthy bool) {
	if path == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.role == RoleInitiator {
		if carrier := s.carrierByID[path.PathID()]; carrier != nil {
			carrier.healthy = healthy
		}
		return
	}
	reply, ok := path.(ReplyPath)
	if !ok {
		return
	}
	key, err := replyIdentityUnchecked(reply)
	if err != nil {
		return
	}
	if endpoint := s.endpoints[key]; endpoint != nil {
		endpoint.healthy = healthy
	}
}

func (s *Session) hasRetryDeadlineLocked() bool {
	for _, carrier := range s.carriers {
		if !carrier.nextRetry.IsZero() {
			return true
		}
	}
	return false
}

func (s *Session) nextDeadlineLocked() time.Time {
	var next time.Time
	consider := func(candidate time.Time) {
		if !candidate.IsZero() && (next.IsZero() || candidate.Before(next)) {
			next = candidate
		}
	}
	for _, carrier := range s.carriers {
		consider(carrier.nextRetry)
	}
	consider(s.nextKeepalive)
	consider(s.nextDecodeSweep)
	for _, endpoint := range s.endpoints {
		consider(endpoint.lastActivity.Add(s.settings.endpointTTL))
	}
	return next
}

// NextDeadline returns the earliest logical wakeup, or zero when none exists.
func (s *Session) NextDeadline() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextDeadlineLocked()
}

func (s *Session) String() string {
	snapshot := s.Snapshot()
	fingerprint := sha256.Sum256(snapshot.ID[:])
	return fmt.Sprintf("Session{%x role:%d state:%d endpoints:%d}", fingerprint[:6], snapshot.Role, snapshot.State, snapshot.Endpoints)
}
