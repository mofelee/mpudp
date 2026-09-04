// rsprobe drives deterministic Reed-Solomon integration scenarios through the
// authenticated Session seam. It records packet metadata only.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"syscall"
	"time"

	"github.com/mofelee/mpudp/internal/fec"
	"github.com/mofelee/mpudp/internal/scheduler"
	"github.com/mofelee/mpudp/internal/session"
	"github.com/mofelee/mpudp/internal/wire"
)

const (
	scenarioFiveCarrierLoss = "rs53-five-carrier-loss"
	scenarioTwoCarrier      = "rs53-two-carrier-rotation"
	scenarioSlowPath        = "slow-path-early-recovery"

	scenarioKey       = "rs53-integration-scenario-key"
	scenarioBudget    = 1200
	decodeTimeout     = time.Second
	completionTTL     = 10 * time.Second
	endpointTTL       = 30 * time.Second
	keepaliveInterval = 30 * time.Second
)

var (
	runIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)
	scenarioID   = wire.SessionID{0x52, 0x53, 0x35, 0x33, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
)

type options struct {
	scenario     string
	runID        string
	events       string
	ready        string
	continuePath string
	timeout      time.Duration
}

type scenarioEvent struct {
	RunID           string `json:"run_id"`
	Scenario        string `json:"scenario"`
	Event           string `json:"event"`
	Block           int    `json:"block,omitempty"`
	Combination     int    `json:"combination,omitempty"`
	DroppedShards   []int  `json:"dropped_shards,omitempty"`
	DroppedPaths    []int  `json:"dropped_paths,omitempty"`
	PathShardCounts []int  `json:"path_shard_counts,omitempty"`
	ShardIndex      *int   `json:"shard_index,omitempty"`
	PathIndex       *int   `json:"path_index,omitempty"`
	ArrivalMS       int64  `json:"arrival_ms,omitempty"`
	EventOrder      int    `json:"event_order,omitempty"`
	Attempts        int    `json:"attempts,omitempty"`
	Deliveries      int    `json:"deliveries,omitempty"`
	ExpiredBlocks   int    `json:"expired_blocks,omitempty"`
	Blocks          int    `json:"blocks,omitempty"`
	NoDataResponses bool   `json:"no_data_responses,omitempty"`
	NoRetransmits   bool   `json:"no_retransmits,omitempty"`
}

func main() {
	var opts options
	flag.StringVar(&opts.scenario, "scenario", "", "scenario name")
	flag.StringVar(&opts.runID, "run-id", "", "integration run identifier")
	flag.StringVar(&opts.events, "events", "", "metadata-only NDJSON output")
	flag.StringVar(&opts.ready, "ready-file", "", "startup-ready marker")
	flag.StringVar(&opts.continuePath, "continue-file", "", "marker that releases scenario execution")
	flag.DurationVar(&opts.timeout, "timeout", 20*time.Second, "whole probe timeout")
	flag.Parse()

	if err := validateOptions(opts); err != nil {
		fmt.Fprintf(os.Stderr, "rsprobe: %v\n", err)
		os.Exit(2)
	}
	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalContext, opts.timeout)
	defer cancel()
	if err := createMarker(opts.ready); err != nil {
		fmt.Fprintf(os.Stderr, "rsprobe: write ready marker: %v\n", err)
		os.Exit(1)
	}
	if err := waitForMarker(ctx, opts.continuePath); err != nil {
		fmt.Fprintf(os.Stderr, "rsprobe: wait for execution release: %v\n", err)
		os.Exit(1)
	}
	events, err := runScenario(ctx, opts.scenario, opts.runID)
	writeErr := writeEvents(opts.events, events)
	if err != nil {
		fmt.Fprintf(os.Stderr, "rsprobe: %v\n", err)
		os.Exit(1)
	}
	if writeErr != nil {
		fmt.Fprintf(os.Stderr, "rsprobe: %v\n", writeErr)
		os.Exit(1)
	}
	fmt.Printf("rsprobe: scenario passed: %s\n", opts.scenario)
}

func validateOptions(opts options) error {
	if !knownScenario(opts.scenario) {
		return errors.New("unknown scenario")
	}
	if !runIDPattern.MatchString(opts.runID) {
		return errors.New("invalid run ID")
	}
	if opts.events == "" {
		return errors.New("events path is required")
	}
	if opts.ready == "" || opts.continuePath == "" || opts.ready == opts.continuePath {
		return errors.New("distinct ready-file and continue-file paths are required")
	}
	if opts.events == opts.ready || opts.events == opts.continuePath {
		return errors.New("events, ready-file, and continue-file paths must be distinct")
	}
	if opts.timeout <= 0 || opts.timeout > 2*time.Minute {
		return errors.New("timeout must be within (0,2m]")
	}
	return nil
}

func knownScenario(name string) bool {
	switch name {
	case scenarioFiveCarrierLoss, scenarioTwoCarrier, scenarioSlowPath:
		return true
	default:
		return false
	}
}

func runScenario(ctx context.Context, name, runID string) ([]scenarioEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch name {
	case scenarioFiveCarrierLoss:
		return runFiveCarrierLoss(ctx, runID)
	case scenarioTwoCarrier:
		return runTwoCarrierRotation(ctx, runID)
	case scenarioSlowPath:
		return runSlowPathRecovery(ctx, runID)
	default:
		return nil, errors.New("unknown scenario")
	}
}

func writeEvents(path string, events []scenarioEvent) error {
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return errors.New("refusing symlink event output")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect event output: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open event output: %w", err)
	}
	encoder := json.NewEncoder(file)
	for _, current := range events {
		if err := encoder.Encode(current); err != nil {
			_ = file.Close()
			return fmt.Errorf("write event metadata: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close event output: %w", err)
	}
	return nil
}

func createMarker(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.WriteString("ready\n"); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func waitForMarker(ctx context.Context, path string) error {
	for {
		info, err := os.Lstat(path)
		if err == nil {
			if !info.Mode().IsRegular() {
				return errors.New("marker is not a regular file")
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

type logicalClock struct {
	now time.Time
}

func newLogicalClock() *logicalClock {
	return &logicalClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *logicalClock) Now() time.Time              { return c.now }
func (c *logicalClock) Set(now time.Time)           { c.now = now }
func (c *logicalClock) Advance(delta time.Duration) { c.now = c.now.Add(delta) }

type memoryAddr string

func (a memoryAddr) Network() string { return "udp" }
func (a memoryAddr) String() string  { return string(a) }

type pathDirection uint8

const (
	toListener pathDirection = iota + 1
	toInitiator
)

type memoryPath struct {
	pair      *scenarioPair
	pathIndex int
	direction pathDirection
}

func (p *memoryPath) PathID() string     { return fmt.Sprintf("carrier-%d", p.pathIndex+1) }
func (p *memoryPath) Generation() uint64 { return 1 }
func (p *memoryPath) Available() bool    { return true }
func (p *memoryPath) LocalAddr() net.Addr {
	if p.direction == toListener {
		return memoryAddr(fmt.Sprintf("192.0.2.%d:%d", p.pathIndex+1, 41000+p.pathIndex))
	}
	return memoryAddr(fmt.Sprintf("198.51.100.%d:9000", p.pathIndex+1))
}
func (p *memoryPath) RemoteAddr() net.Addr {
	if p.direction == toListener {
		return memoryAddr(fmt.Sprintf("198.51.100.%d:9000", p.pathIndex+1))
	}
	return memoryAddr(fmt.Sprintf("192.0.2.%d:%d", p.pathIndex+1, 41000+p.pathIndex))
}
func (p *memoryPath) Send(ctx context.Context, packet []byte) error {
	return p.pair.route(ctx, p.pathIndex, p.direction, packet)
}

type dataAttempt struct {
	packetID uint64
	shard    int
	path     int
	packet   []byte
	dropped  bool
}

type shardArrival struct {
	packetID uint64
	shard    int
	path     int
	at       time.Time
	order    int
}

type datagramDelivery struct {
	packetID uint64
	body     []byte
	at       time.Time
	order    int
}

type queuedPacket struct {
	at      time.Time
	attempt *dataAttempt
}

type scenarioPair struct {
	clock     *logicalClock
	listener  *session.Listener
	initiator *session.Session
	receiver  *session.Session
	forward   []*memoryPath
	reverse   []*memoryPath

	dropPaths   map[int]bool
	dropShards  map[int]bool
	queueData   bool
	queueOrigin time.Time
	arrivalPlan map[int]time.Duration
	queued      []queuedPacket
	attempts    []*dataAttempt
	arrivals    []shardArrival
	deliveries  []datagramDelivery
	reverseWire []wire.PacketType
	eventOrder  int
	silent      bool
}

func newScenarioPair(ctx context.Context, pathCount int) (*scenarioPair, error) {
	clock := newLogicalClock()
	config := scenarioSessionConfig(clock, pathCount)
	listener, err := session.NewListener(session.ListenerConfig{Session: config, MaxSessions: 1})
	if err != nil {
		return nil, fmt.Errorf("create listener seam: %w", err)
	}
	pair := &scenarioPair{clock: clock, listener: listener}
	paths := make([]session.Path, pathCount)
	for index := 0; index < pathCount; index++ {
		forward := &memoryPath{pair: pair, pathIndex: index, direction: toListener}
		reverse := &memoryPath{pair: pair, pathIndex: index, direction: toInitiator}
		pair.forward = append(pair.forward, forward)
		pair.reverse = append(pair.reverse, reverse)
		paths[index] = forward
	}
	pair.initiator, err = session.NewInitiator(scenarioID, config, paths)
	if err != nil {
		_ = listener.Close(context.Background())
		return nil, fmt.Errorf("create initiator seam: %w", err)
	}
	attempts, err := pair.initiator.Start(ctx)
	if err != nil {
		pair.close()
		return nil, fmt.Errorf("start authenticated seam: %w", err)
	}
	if len(attempts) != pathCount {
		pair.close()
		return nil, fmt.Errorf("HELLO attempts=%d, want %d", len(attempts), pathCount)
	}
	for _, attempt := range attempts {
		if attempt.Type != wire.TypeHello || attempt.Err != nil {
			pair.close()
			return nil, fmt.Errorf("HELLO attempt was not successful: %+v", attempt)
		}
	}
	pair.receiver, _ = listener.Session(scenarioID)
	if pair.receiver == nil {
		pair.close()
		return nil, errors.New("listener seam did not retain the authenticated Session")
	}
	initiatorSnapshot := pair.initiator.Snapshot()
	receiverSnapshot := pair.receiver.Snapshot()
	if initiatorSnapshot.State != session.StateEstablished || initiatorSnapshot.AcknowledgedCarriers != pathCount ||
		initiatorSnapshot.Endpoints != pathCount || receiverSnapshot.State != session.StateEstablished ||
		receiverSnapshot.Endpoints != pathCount {
		pair.close()
		return nil, fmt.Errorf("authenticated seam did not establish every Carrier: initiator=%+v receiver=%+v", initiatorSnapshot, receiverSnapshot)
	}
	pair.resetTraffic()
	return pair, nil
}

func scenarioSessionConfig(clock session.Clock, pathCount int) session.Config {
	return session.Config{
		PSK: []byte(scenarioKey), FEC: fec.Params{DataShards: 3, ParityShards: 2},
		LocalMaxUDPPayload: scenarioBudget, MaxDatagramSize: 64 * 1024,
		MaxEndpoints: pathCount, MaxHandshakeAttempts: 3,
		MaxPendingFECBlocks: 32, MaxCompletedFECBlocks: 32,
		DecodeTimeout: decodeTimeout, CompletionTTL: completionTTL,
		EndpointTTL: endpointTTL, KeepaliveInterval: keepaliveInterval,
		HandshakeRetryInterval:    100 * time.Millisecond,
		HandshakeRetryJitterLimit: 25 * time.Millisecond,
		Clock:                     clock,
	}
}

func (p *scenarioPair) close() {
	p.silent = true
	if p.initiator != nil {
		_ = p.initiator.Close(context.Background())
	}
	if p.listener != nil {
		_ = p.listener.Close(context.Background())
	}
}

func (p *scenarioPair) resetTraffic() {
	p.dropPaths = nil
	p.dropShards = nil
	p.queueData = false
	p.arrivalPlan = nil
	p.queued = nil
	p.attempts = nil
	p.arrivals = nil
	p.deliveries = nil
	p.reverseWire = nil
	p.eventOrder = 0
}

func (p *scenarioPair) route(ctx context.Context, pathIndex int, direction pathDirection, packet []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.silent {
		return nil
	}
	message, err := wire.DecodeAuthenticated(packet, []byte(scenarioKey), scenarioBudget)
	if err != nil {
		return fmt.Errorf("decode seam packet: %w", err)
	}
	if direction == toListener && message.Header.Type == wire.TypeDataShard {
		attempt := &dataAttempt{
			packetID: message.DataShard.PacketID,
			shard:    int(message.DataShard.ShardIndex),
			path:     pathIndex,
			packet:   bytes.Clone(packet),
		}
		p.attempts = append(p.attempts, attempt)
		if p.dropPaths[pathIndex] || p.dropShards[attempt.shard] {
			attempt.dropped = true
			return nil
		}
		if p.queueData {
			delay, ok := p.arrivalPlan[attempt.shard]
			if !ok {
				return fmt.Errorf("no arrival plan for shard %d", attempt.shard)
			}
			p.queued = append(p.queued, queuedPacket{at: p.queueOrigin.Add(delay), attempt: attempt})
			return nil
		}
		return p.deliverData(ctx, attempt)
	}
	if direction == toListener {
		_, result, err := p.listener.HandlePacket(ctx, session.ReceivedPacket{
			Payload: bytes.Clone(packet), Reply: p.reverse[pathIndex],
		})
		if err != nil {
			return fmt.Errorf("listener seam handle %d: %w", message.Header.Type, err)
		}
		if result.Response != nil && result.Response.Err != nil {
			return fmt.Errorf("listener seam response %d: %w", result.Response.Type, result.Response.Err)
		}
		return nil
	}
	p.reverseWire = append(p.reverseWire, message.Header.Type)
	result, err := p.initiator.HandlePacket(ctx, session.ReceivedPacket{
		Payload: bytes.Clone(packet), Reply: p.forward[pathIndex],
	})
	if err != nil {
		return fmt.Errorf("initiator seam handle %d: %w", message.Header.Type, err)
	}
	if result.Response != nil && result.Response.Err != nil {
		return fmt.Errorf("initiator seam response %d: %w", result.Response.Type, result.Response.Err)
	}
	return nil
}

func (p *scenarioPair) deliverData(ctx context.Context, attempt *dataAttempt) error {
	p.eventOrder++
	p.arrivals = append(p.arrivals, shardArrival{
		packetID: attempt.packetID, shard: attempt.shard, path: attempt.path,
		at: p.clock.Now(), order: p.eventOrder,
	})
	_, result, err := p.listener.HandlePacket(ctx, session.ReceivedPacket{
		Payload: bytes.Clone(attempt.packet), Reply: p.reverse[attempt.path],
	})
	if err != nil {
		return fmt.Errorf("deliver DATA shard %d: %w", attempt.shard, err)
	}
	if result.Response != nil {
		return fmt.Errorf("DATA shard %d produced unexpected response type %d", attempt.shard, result.Response.Type)
	}
	if result.Datagram != nil {
		p.eventOrder++
		p.deliveries = append(p.deliveries, datagramDelivery{
			packetID: attempt.packetID, body: bytes.Clone(result.Datagram),
			at: p.clock.Now(), order: p.eventOrder,
		})
	}
	return nil
}

func (p *scenarioPair) queueByShard(arrivals map[int]time.Duration, dropped ...int) {
	p.queueData = true
	p.queueOrigin = p.clock.Now()
	p.arrivalPlan = arrivals
	p.dropShards = make(map[int]bool, len(dropped))
	for _, shard := range dropped {
		p.dropShards[shard] = true
	}
}

func (p *scenarioPair) flushQueued(ctx context.Context) error {
	sort.Slice(p.queued, func(i, j int) bool {
		if p.queued[i].at.Equal(p.queued[j].at) {
			return p.queued[i].attempt.shard < p.queued[j].attempt.shard
		}
		return p.queued[i].at.Before(p.queued[j].at)
	})
	for _, queued := range p.queued {
		p.clock.Set(queued.at)
		if err := p.deliverData(ctx, queued.attempt); err != nil {
			return err
		}
	}
	p.queued = nil
	return nil
}

func runFiveCarrierLoss(ctx context.Context, runID string) ([]scenarioEvent, error) {
	pair, err := newScenarioPair(ctx, 5)
	if err != nil {
		return nil, err
	}
	defer pair.close()
	var events []scenarioEvent
	totalAttempts := 0
	totalDeliveries := 0
	combination := 0
	for first := 0; first < 5; first++ {
		for second := first + 1; second < 5; second++ {
			combination++
			pair.resetTraffic()
			expectedPacketID := uint64(combination - 1)
			droppedShards := []int{first, second}
			droppedPaths, err := pathsForShards(expectedPacketID, 5, droppedShards)
			if err != nil {
				return events, err
			}
			pair.dropPaths = indexSet(droppedPaths)
			payload := makeDatagram(257+combination, byte(0x20+combination))
			write, err := pair.initiator.WritePacket(ctx, payload)
			if err != nil {
				return events, fmt.Errorf("two-loss combination %d write: %w", combination, err)
			}
			if err := assertWrite(pair.attempts, write, expectedPacketID, 5, 5); err != nil {
				return events, fmt.Errorf("two-loss combination %d: %w", combination, err)
			}
			actualDropped := droppedShardIndexes(pair.attempts)
			if !equalInts(actualDropped, droppedShards) {
				return events, fmt.Errorf("two-loss combination %d dropped shards %v, want %v", combination, actualDropped, droppedShards)
			}
			if len(pair.arrivals) != 3 {
				return events, fmt.Errorf("two-loss combination %d arrivals=%d, want 3", combination, len(pair.arrivals))
			}
			if err := assertSingleDelivery(pair.deliveries, expectedPacketID, payload); err != nil {
				return events, fmt.Errorf("two-loss combination %d: %w", combination, err)
			}
			duplicate := firstArrivedAttempt(pair.attempts)
			if duplicate == nil {
				return events, fmt.Errorf("two-loss combination %d has no surviving shard", combination)
			}
			if err := pair.deliverData(ctx, duplicate); err != nil {
				return events, fmt.Errorf("two-loss combination %d duplicate: %w", combination, err)
			}
			if len(pair.deliveries) != 1 {
				return events, fmt.Errorf("two-loss combination %d delivered %d times after duplicate", combination, len(pair.deliveries))
			}
			if len(pair.reverseWire) != 0 {
				return events, fmt.Errorf("two-loss combination %d emitted reverse DATA response types %v", combination, pair.reverseWire)
			}
			totalAttempts += write.Send.Attempted
			totalDeliveries++
			events = append(events, scenarioEvent{
				RunID: runID, Scenario: scenarioFiveCarrierLoss, Event: "two_loss_recovered",
				Block: combination, Combination: combination,
				DroppedShards: droppedShards, DroppedPaths: droppedPaths,
				PathShardCounts: pathCounts(write), Attempts: write.Send.Attempted, Deliveries: 1,
				NoDataResponses: true, NoRetransmits: true,
			})
		}
	}

	pair.resetTraffic()
	expectedPacketID := uint64(10)
	droppedShards := []int{0, 1, 2}
	droppedPaths, err := pathsForShards(expectedPacketID, 5, droppedShards)
	if err != nil {
		return events, err
	}
	pair.dropPaths = indexSet(droppedPaths)
	payload := makeDatagram(311, 0x7d)
	write, err := pair.initiator.WritePacket(ctx, payload)
	if err != nil {
		return events, fmt.Errorf("three-loss write: %w", err)
	}
	if err := assertWrite(pair.attempts, write, expectedPacketID, 5, 5); err != nil {
		return events, fmt.Errorf("three-loss: %w", err)
	}
	if actual := droppedShardIndexes(pair.attempts); !equalInts(actual, droppedShards) {
		return events, fmt.Errorf("three-loss dropped shards %v, want %v", actual, droppedShards)
	}
	if len(pair.arrivals) != 2 || len(pair.deliveries) != 0 {
		return events, fmt.Errorf("three-loss arrivals/deliveries=%d/%d, want 2/0", len(pair.arrivals), len(pair.deliveries))
	}
	attemptsBeforeExpiry := len(pair.attempts)
	pair.clock.Advance(decodeTimeout + time.Nanosecond)
	advance, err := pair.receiver.Advance(ctx)
	if err != nil {
		return events, fmt.Errorf("expire incomplete block: %w", err)
	}
	senderAdvance, err := pair.initiator.Advance(ctx)
	if err != nil {
		return events, fmt.Errorf("advance sender after incomplete block: %w", err)
	}
	if advance.ExpiredFEC.PendingBlocks != 1 || advance.ExpiredFEC.CompletedBlocks != 0 || len(advance.Sends) != 0 {
		return events, fmt.Errorf("three-loss expiry=%+v sends=%d, want one pending block and no wire response", advance.ExpiredFEC, len(advance.Sends))
	}
	if len(senderAdvance.Sends) != 0 || len(pair.attempts) != attemptsBeforeExpiry || len(pair.deliveries) != 0 || len(pair.reverseWire) != 0 {
		return events, errors.New("three-loss timeout emitted a retry, DATA response, or delivery")
	}
	totalAttempts += write.Send.Attempted
	events = append(events,
		scenarioEvent{
			RunID: runID, Scenario: scenarioFiveCarrierLoss, Event: "three_loss_expired",
			Block: 11, DroppedShards: droppedShards, DroppedPaths: droppedPaths,
			PathShardCounts: pathCounts(write), Attempts: write.Send.Attempted, ExpiredBlocks: 1,
			NoDataResponses: true, NoRetransmits: true,
		},
		scenarioEvent{
			RunID: runID, Scenario: scenarioFiveCarrierLoss, Event: "scenario_complete",
			Attempts: totalAttempts, Deliveries: totalDeliveries, Blocks: 11, Combination: combination,
			NoDataResponses: true, NoRetransmits: true,
		},
	)
	return events, nil
}

func runTwoCarrierRotation(ctx context.Context, runID string) ([]scenarioEvent, error) {
	pair, err := newScenarioPair(ctx, 2)
	if err != nil {
		return nil, err
	}
	defer pair.close()
	var events []scenarioEvent
	totalAttempts := 0
	totalDeliveries := 0

	for block := 0; block < 4; block++ {
		pair.resetTraffic()
		payload := makeDatagram(320+block, byte(0x40+block))
		write, err := pair.initiator.WritePacket(ctx, payload)
		if err != nil {
			return events, fmt.Errorf("rotation block %d write: %w", block, err)
		}
		if err := assertWrite(pair.attempts, write, uint64(block), 5, 2); err != nil {
			return events, fmt.Errorf("rotation block %d: %w", block, err)
		}
		wantCounts := []int{3, 2}
		if block%2 == 1 {
			wantCounts = []int{2, 3}
		}
		counts := pathCounts(write)
		if !equalInts(counts, wantCounts) {
			return events, fmt.Errorf("rotation block %d path counts=%v, want %v", block, counts, wantCounts)
		}
		if err := assertSingleDelivery(pair.deliveries, write.PacketID, payload); err != nil {
			return events, fmt.Errorf("rotation block %d: %w", block, err)
		}
		if len(pair.reverseWire) != 0 {
			return events, fmt.Errorf("rotation block %d emitted reverse packet types %v", block, pair.reverseWire)
		}
		totalAttempts += write.Send.Attempted
		totalDeliveries++
		events = append(events, scenarioEvent{
			RunID: runID, Scenario: scenarioTwoCarrier, Event: "rotation_observed",
			Block: block + 1, PathShardCounts: counts,
			Attempts: write.Send.Attempted, Deliveries: 1,
		})
	}

	pair.resetTraffic()
	twoShardPath := 1
	pair.dropPaths = indexSet([]int{twoShardPath})
	payload := makeDatagram(341, 0x61)
	write, err := pair.initiator.WritePacket(ctx, payload)
	if err != nil {
		return events, fmt.Errorf("two-shard Carrier loss write: %w", err)
	}
	if err := assertWrite(pair.attempts, write, 4, 5, 2); err != nil {
		return events, fmt.Errorf("two-shard Carrier loss: %w", err)
	}
	counts := pathCounts(write)
	if counts[twoShardPath] != 2 || len(pair.arrivals) != 3 {
		return events, fmt.Errorf("selected two-shard Carrier counts=%v arrivals=%d", counts, len(pair.arrivals))
	}
	if err := assertSingleDelivery(pair.deliveries, write.PacketID, payload); err != nil {
		return events, fmt.Errorf("two-shard Carrier loss: %w", err)
	}
	totalAttempts += write.Send.Attempted
	totalDeliveries++
	events = append(events, scenarioEvent{
		RunID: runID, Scenario: scenarioTwoCarrier, Event: "two_shard_carrier_lost_recovered",
		Block: 5, DroppedPaths: []int{twoShardPath}, PathShardCounts: counts,
		Attempts: write.Send.Attempted, Deliveries: 1,
	})

	pair.resetTraffic()
	threeShardPath := 1
	pair.dropPaths = indexSet([]int{threeShardPath})
	payload = makeDatagram(342, 0x62)
	write, err = pair.initiator.WritePacket(ctx, payload)
	if err != nil {
		return events, fmt.Errorf("three-shard Carrier loss write: %w", err)
	}
	if err := assertWrite(pair.attempts, write, 5, 5, 2); err != nil {
		return events, fmt.Errorf("three-shard Carrier loss: %w", err)
	}
	counts = pathCounts(write)
	if counts[threeShardPath] != 3 || len(pair.arrivals) != 2 || len(pair.deliveries) != 0 {
		return events, fmt.Errorf("selected three-shard Carrier counts=%v arrivals/deliveries=%d/%d", counts, len(pair.arrivals), len(pair.deliveries))
	}
	pair.clock.Advance(decodeTimeout + time.Nanosecond)
	advance, err := pair.receiver.Advance(ctx)
	if err != nil {
		return events, fmt.Errorf("expire three-shard Carrier loss block: %w", err)
	}
	senderAdvance, err := pair.initiator.Advance(ctx)
	if err != nil {
		return events, fmt.Errorf("advance sender after three-shard Carrier loss: %w", err)
	}
	if advance.ExpiredFEC.PendingBlocks != 1 || len(advance.Sends) != 0 || len(senderAdvance.Sends) != 0 || len(pair.reverseWire) != 0 {
		return events, fmt.Errorf("three-shard Carrier expiry=%+v receiver/sender sends=%d/%d reverse=%v",
			advance.ExpiredFEC, len(advance.Sends), len(senderAdvance.Sends), pair.reverseWire)
	}
	totalAttempts += write.Send.Attempted
	events = append(events, scenarioEvent{
		RunID: runID, Scenario: scenarioTwoCarrier, Event: "three_shard_carrier_lost_expired",
		Block: 6, DroppedPaths: []int{threeShardPath}, PathShardCounts: counts,
		Attempts: write.Send.Attempted, ExpiredBlocks: 1,
	})

	pair.resetTraffic()
	payload = makeDatagram(343, 0x63)
	write, err = pair.initiator.WritePacket(ctx, payload)
	if err != nil {
		return events, fmt.Errorf("post-loss recovery write: %w", err)
	}
	if err := assertWrite(pair.attempts, write, 6, 5, 2); err != nil {
		return events, fmt.Errorf("post-loss recovery: %w", err)
	}
	if err := assertSingleDelivery(pair.deliveries, write.PacketID, payload); err != nil {
		return events, fmt.Errorf("post-loss recovery: %w", err)
	}
	if pair.receiver.Snapshot().State != session.StateEstablished {
		return events, errors.New("three-shard loss closed the logical Session")
	}
	totalAttempts += write.Send.Attempted
	totalDeliveries++
	events = append(events,
		scenarioEvent{
			RunID: runID, Scenario: scenarioTwoCarrier, Event: "post_loss_session_recovered",
			Block: 7, PathShardCounts: pathCounts(write),
			Attempts: write.Send.Attempted, Deliveries: 1,
		},
		scenarioEvent{
			RunID: runID, Scenario: scenarioTwoCarrier, Event: "scenario_complete",
			Attempts: totalAttempts, Deliveries: totalDeliveries, ExpiredBlocks: 1, Blocks: 7,
			NoDataResponses: true, NoRetransmits: true,
		},
	)
	return events, nil
}

func runSlowPathRecovery(ctx context.Context, runID string) ([]scenarioEvent, error) {
	pair, err := newScenarioPair(ctx, 5)
	if err != nil {
		return nil, err
	}
	defer pair.close()
	var events []scenarioEvent
	pair.resetTraffic()
	pair.queueByShard(map[int]time.Duration{
		0: 10 * time.Millisecond,
		1: 20 * time.Millisecond,
		2: 30 * time.Millisecond,
		3: 500 * time.Millisecond,
	}, 4)
	payload := makeDatagram(513, 0x73)
	write, err := pair.initiator.WritePacket(ctx, payload)
	if err != nil {
		return events, fmt.Errorf("slow-path write: %w", err)
	}
	if err := assertWrite(pair.attempts, write, 0, 5, 5); err != nil {
		return events, fmt.Errorf("slow-path: %w", err)
	}
	if len(pair.deliveries) != 0 || len(pair.queued) != 4 || !pair.attempts[4].dropped {
		return events, fmt.Errorf("slow-path pre-flush deliveries/queued/drop=%d/%d/%t, want 0/4/true", len(pair.deliveries), len(pair.queued), pair.attempts[4].dropped)
	}
	origin := pair.queueOrigin
	if err := pair.flushQueued(ctx); err != nil {
		return events, err
	}
	if len(pair.arrivals) != 4 {
		return events, fmt.Errorf("slow-path arrivals=%d, want 4", len(pair.arrivals))
	}
	wantShards := []int{0, 1, 2, 3}
	wantArrivalMS := []int64{10, 20, 30, 500}
	for index, arrival := range pair.arrivals {
		elapsed := arrival.at.Sub(origin).Milliseconds()
		if arrival.shard != wantShards[index] || elapsed != wantArrivalMS[index] {
			return events, fmt.Errorf("slow-path arrival %d shard/time=%d/%dms, want %d/%dms", index, arrival.shard, elapsed, wantShards[index], wantArrivalMS[index])
		}
	}
	if err := assertSingleDelivery(pair.deliveries, write.PacketID, payload); err != nil {
		return events, fmt.Errorf("slow-path: %w", err)
	}
	delivery := pair.deliveries[0]
	if delivery.at.Sub(origin) != 30*time.Millisecond || delivery.order <= pair.arrivals[2].order || delivery.order >= pair.arrivals[3].order {
		return events, fmt.Errorf("delivery time/order=%s/%d, third/late orders=%d/%d", delivery.at.Sub(origin), delivery.order, pair.arrivals[2].order, pair.arrivals[3].order)
	}
	if len(pair.reverseWire) != 0 {
		return events, fmt.Errorf("slow-path DATA emitted reverse packet types %v", pair.reverseWire)
	}
	for _, arrival := range pair.arrivals {
		events = append(events, scenarioEvent{
			RunID: runID, Scenario: scenarioSlowPath, Event: "shard_arrived",
			Block: 1, ShardIndex: intPointer(arrival.shard), PathIndex: intPointer(arrival.path),
			ArrivalMS: arrival.at.Sub(origin).Milliseconds(), EventOrder: arrival.order,
		})
		if arrival.shard == 2 {
			events = append(events, scenarioEvent{
				RunID: runID, Scenario: scenarioSlowPath, Event: "datagram_recovered",
				Block: 1, ArrivalMS: delivery.at.Sub(origin).Milliseconds(),
				EventOrder: delivery.order, Deliveries: 1,
			})
		}
	}
	events = append(events, scenarioEvent{
		RunID: runID, Scenario: scenarioSlowPath, Event: "scenario_complete",
		Block: 1, DroppedShards: []int{4}, PathShardCounts: pathCounts(write),
		Attempts: write.Send.Attempted, Deliveries: 1, Blocks: 1,
		NoDataResponses: true, NoRetransmits: true,
	})
	return events, nil
}

func assertWrite(captured []*dataAttempt, write session.WriteResult, packetID uint64, attempts, paths int) error {
	if write.PacketID != packetID {
		return fmt.Errorf("PacketID=%d, want %d", write.PacketID, packetID)
	}
	if write.Send.PacketID != packetID || write.Send.Attempted != attempts || write.Send.Succeeded != attempts ||
		write.Send.PathsAvailable != paths || len(write.Send.Attempts) != attempts {
		return fmt.Errorf("send result=%+v, want packet=%d attempts/success=%d paths=%d", write.Send, packetID, attempts, paths)
	}
	for shard, attempt := range write.Send.Attempts {
		if attempt.ShardIndex != shard || attempt.Err != nil {
			return fmt.Errorf("shard attempt %d=%+v", shard, attempt)
		}
	}
	if len(captured) != len(write.Send.Attempts) {
		return fmt.Errorf("decoded DATA attempts=%d, want %d", len(captured), len(write.Send.Attempts))
	}
	for index, actual := range captured {
		want := write.Send.Attempts[index]
		if actual.packetID != packetID || actual.shard != want.ShardIndex || actual.path != want.PathIndex {
			return fmt.Errorf("decoded DATA attempt %d packet/shard/path=%d/%d/%d, want %d/%d/%d",
				index, actual.packetID, actual.shard, actual.path, packetID, want.ShardIndex, want.PathIndex)
		}
	}
	return nil
}

func assertSingleDelivery(deliveries []datagramDelivery, packetID uint64, payload []byte) error {
	if len(deliveries) != 1 {
		return fmt.Errorf("deliveries=%d, want 1", len(deliveries))
	}
	if deliveries[0].packetID != packetID || !bytes.Equal(deliveries[0].body, payload) {
		return fmt.Errorf("delivered Datagram metadata does not match packet %d", packetID)
	}
	return nil
}

func pathsForShards(packetID uint64, pathCount int, shards []int) ([]int, error) {
	assignments, err := scheduler.Assign(packetID, 5, pathCount)
	if err != nil {
		return nil, err
	}
	paths := make([]int, 0, len(shards))
	seen := make(map[int]bool, len(shards))
	for _, shard := range shards {
		if shard < 0 || shard >= len(assignments) {
			return nil, fmt.Errorf("invalid shard index %d", shard)
		}
		path := assignments[shard]
		if seen[path] {
			return nil, fmt.Errorf("shards %v do not map to distinct paths", shards)
		}
		seen[path] = true
		paths = append(paths, path)
	}
	sort.Ints(paths)
	return paths, nil
}

func pathCounts(write session.WriteResult) []int {
	counts := make([]int, write.Send.PathsAvailable)
	for _, attempt := range write.Send.Attempts {
		if attempt.PathIndex >= 0 && attempt.PathIndex < len(counts) {
			counts[attempt.PathIndex]++
		}
	}
	return counts
}

func droppedShardIndexes(attempts []*dataAttempt) []int {
	var result []int
	for _, attempt := range attempts {
		if attempt.dropped {
			result = append(result, attempt.shard)
		}
	}
	sort.Ints(result)
	return result
}

func firstArrivedAttempt(attempts []*dataAttempt) *dataAttempt {
	for _, attempt := range attempts {
		if !attempt.dropped {
			return attempt
		}
	}
	return nil
}

func indexSet(indices []int) map[int]bool {
	result := make(map[int]bool, len(indices))
	for _, index := range indices {
		result[index] = true
	}
	return result
}

func equalInts(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func intPointer(value int) *int { return &value }

func makeDatagram(size int, seed byte) []byte {
	body := make([]byte, size)
	for index := range body {
		body[index] = byte((int(seed) + index*29 + index/5) % 251)
	}
	return body
}
