// rspeerprobe drives the canonical Reed-Solomon scenarios through the public
// MPUDP API. The namespace harness owns path faults and wire observations.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mofelee/mpudp"
	"github.com/mofelee/mpudp/config"
)

const (
	scenarioFiveCarrier = "rs53-five-carrier-loss"
	scenarioTwoCarrier  = "rs53-two-carrier-rotation"
	scenarioSlowPath    = "slow-path-early-recovery"

	integrationKey = "integration-test-key"
	bootstrapBytes = 96
	warmupBytes    = 97
	decodeTimeout  = 300 * time.Millisecond
	quietPeriod    = 1500 * time.Millisecond
	markerPoll     = 10 * time.Millisecond
)

var runIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)

type options struct {
	role       string
	scenario   string
	family     int
	listen     string
	carriers   string
	timeout    time.Duration
	runID      string
	eventsPath string
	syncDir    string
}

type scenarioStep struct {
	bytes   int
	salt    byte
	deliver bool
}

type event struct {
	RunID      string `json:"run_id"`
	Role       string `json:"role"`
	Scenario   string `json:"scenario"`
	Event      string `json:"event"`
	Step       int    `json:"step,omitempty"`
	Bytes      *int   `json:"bytes,omitempty"`
	Digest     string `json:"sha256_prefix,omitempty"`
	ElapsedMS  int64  `json:"elapsed_ms"`
	ObservedNS int64  `json:"observed_unix_nano"`
}

type eventLog struct {
	mu      sync.Mutex
	file    *os.File
	encoder *json.Encoder
	start   time.Time
	base    event
}

type packetResult struct {
	body []byte
	err  error
}

func main() {
	var opts options
	flag.StringVar(&opts.role, "role", "", "listener or initiator")
	flag.StringVar(&opts.scenario, "scenario", "", "canonical RS scenario")
	flag.IntVar(&opts.family, "family", 0, "address family: 4 or 6")
	flag.StringVar(&opts.listen, "listen", "", "listener address")
	flag.StringVar(&opts.carriers, "carriers", "", "comma-separated initiator remotes")
	flag.DurationVar(&opts.timeout, "timeout", 30*time.Second, "whole helper timeout")
	flag.StringVar(&opts.runID, "run-id", "", "integration run identifier")
	flag.StringVar(&opts.eventsPath, "events", "", "metadata-only NDJSON output")
	flag.StringVar(&opts.syncDir, "sync-dir", "", "harness-owned marker directory")
	flag.Parse()

	if err := validateOptions(opts); err != nil {
		fmt.Fprintf(os.Stderr, "rspeerprobe: %v\n", err)
		os.Exit(2)
	}
	log, err := openEventLog(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "rspeerprobe: %v\n", err)
		os.Exit(1)
	}
	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGTERM)
	ctx, cancel := context.WithTimeout(signalContext, opts.timeout)
	err = run(ctx, opts, log)
	cancel()
	stop()
	if closeErr := log.close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "rspeerprobe: %v\n", err)
		os.Exit(1)
	}
}

func validateOptions(opts options) error {
	if opts.role != "listener" && opts.role != "initiator" {
		return errors.New("role must be listener or initiator")
	}
	steps, carrierCount, err := planFor(opts.scenario)
	if err != nil || len(steps) == 0 {
		return errors.New("unknown scenario")
	}
	if opts.family != 4 && opts.family != 6 {
		return errors.New("family must be 4 or 6")
	}
	if !runIDPattern.MatchString(opts.runID) {
		return errors.New("invalid run ID")
	}
	if opts.timeout <= quietPeriod || opts.timeout > 2*time.Minute {
		return errors.New("timeout must be greater than the quiet period and at most 2m")
	}
	if opts.eventsPath == "" || opts.syncDir == "" {
		return errors.New("events and sync-dir are required")
	}
	info, err := os.Lstat(opts.syncDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("sync-dir must be an existing real directory")
	}
	if opts.role == "listener" {
		if opts.listen == "" || opts.carriers != "" {
			return errors.New("listener requires listen and no carriers")
		}
		return nil
	}
	if opts.listen != "" || opts.carriers == "" {
		return errors.New("initiator requires carriers and no listen")
	}
	carriers := strings.Split(opts.carriers, ",")
	if len(carriers) != carrierCount {
		return fmt.Errorf("scenario requires exactly %d Carriers", carrierCount)
	}
	for _, carrier := range carriers {
		if strings.TrimSpace(carrier) == "" {
			return errors.New("carrier address must not be empty")
		}
	}
	return nil
}

func planFor(name string) ([]scenarioStep, int, error) {
	switch name {
	case scenarioFiveCarrier:
		steps := make([]scenarioStep, 0, 13)
		for index := 0; index < 10; index++ {
			steps = append(steps, scenarioStep{bytes: 201 + index, salt: byte(0x20 + index), deliver: true})
		}
		steps = append(steps,
			scenarioStep{bytes: 311, salt: 0x3e, deliver: false},
			scenarioStep{bytes: 312, salt: 0x3f, deliver: false},
			scenarioStep{bytes: 313, salt: 0x40, deliver: true},
		)
		return steps, 5, nil
	case scenarioTwoCarrier:
		return []scenarioStep{
			{bytes: 401, salt: 0x41, deliver: true},
			{bytes: 402, salt: 0x42, deliver: true},
			{bytes: 403, salt: 0x43, deliver: true},
			{bytes: 404, salt: 0x44, deliver: true},
			{bytes: 405, salt: 0x45, deliver: true},
			{bytes: 406, salt: 0x46, deliver: false},
			{bytes: 407, salt: 0x47, deliver: true},
		}, 2, nil
	case scenarioSlowPath:
		return []scenarioStep{{bytes: 510, salt: 0x51, deliver: true}}, 5, nil
	default:
		return nil, 0, errors.New("unknown scenario")
	}
}

func run(ctx context.Context, opts options, log *eventLog) (returnErr error) {
	cfg := config.Default()
	cfg.FEC = config.FECConfig{DataShards: 3, ParityShards: 2}
	cfg.PSK = config.NewSecret(integrationKey)
	cfg.Transport.MaxUDPPayload = 1200
	cfg.Timers.DecodeTimeout = decodeTimeout
	cfg.Timers.EndpointTTL = 30 * time.Second
	cfg.Timers.KeepaliveInterval = config.MaxKeepaliveInterval
	cfg.Timers.HandshakeRetryInterval = config.MinHandshakeRetryInterval
	if opts.role == "listener" {
		cfg.Listen = opts.listen
	} else {
		cfg.Carriers = strings.Split(opts.carriers, ",")
	}
	peer, err := mpudp.NewPeer(cfg)
	if err != nil {
		return fmt.Errorf("create public Peer: %w", err)
	}
	defer func() {
		if closeErr := peer.Close(); returnErr == nil && closeErr != nil {
			returnErr = fmt.Errorf("close public Peer: %w", closeErr)
		}
	}()
	if err := log.write("peer_ready", 0, nil); err != nil {
		return err
	}
	if opts.role == "listener" {
		return runListener(ctx, opts, log, peer)
	}
	return runInitiator(ctx, opts, log, peer)
}

func runListener(ctx context.Context, opts options, log *eventLog, peer *mpudp.Peer) error {
	listener, err := peer.Listener()
	if err != nil {
		return fmt.Errorf("obtain public Listener: %w", err)
	}
	if err := createMarker(marker(opts, "listener-ready")); err != nil {
		return fmt.Errorf("create listener-ready marker: %w", err)
	}
	current, err := listener.Accept(ctx)
	if err != nil {
		return fmt.Errorf("accept public Session: %w", err)
	}
	if err := log.write("session_accepted", 0, nil); err != nil {
		return err
	}
	if err := readExpected(ctx, log, current, "bootstrap_received", 0, makeDatagram(bootstrapBytes, 0x11)); err != nil {
		return err
	}
	if err := writeExpected(log, current, "bootstrap_sent", 0, makeDatagram(bootstrapBytes, 0x12)); err != nil {
		return err
	}
	if err := readExpected(ctx, log, current, "warmup_received", 0, makeDatagram(warmupBytes, 0x13)); err != nil {
		return err
	}
	if err := writeExpected(log, current, "warmup_sent", 0, makeDatagram(warmupBytes, 0x14)); err != nil {
		return err
	}

	steps, _, _ := planFor(opts.scenario)
	var pending <-chan packetResult
	for index, step := range steps {
		stepNumber := index + 1
		if pending == nil {
			pending = readPacketAsync(current)
		}
		if step.deliver {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case got := <-pending:
				if err := checkPacket(got, makeDatagram(step.bytes, step.salt)); err != nil {
					return fmt.Errorf("step %d delivery: %w", stepNumber, err)
				}
				pending = nil
			}
			body := makeDatagram(step.bytes, step.salt)
			if err := log.write("datagram_received", stepNumber, body); err != nil {
				return err
			}
			if err := createMarker(stepMarker(opts, stepNumber, "received")); err != nil {
				return fmt.Errorf("create step %d received marker: %w", stepNumber, err)
			}
			continue
		}

		if err := waitForMarker(ctx, stepMarker(opts, stepNumber, "sent")); err != nil {
			return fmt.Errorf("wait for step %d send: %w", stepNumber, err)
		}
		timer := time.NewTimer(quietPeriod)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case got := <-pending:
			timer.Stop()
			if got.err != nil {
				return fmt.Errorf("step %d quiet read: %w", stepNumber, got.err)
			}
			return fmt.Errorf("step %d unexpectedly delivered bytes=%d digest=%s", stepNumber, len(got.body), digest(got.body))
		case <-timer.C:
		}
		if err := log.write("datagram_not_delivered", stepNumber, nil); err != nil {
			return err
		}
		if err := createMarker(stepMarker(opts, stepNumber, "quiet")); err != nil {
			return fmt.Errorf("create step %d quiet marker: %w", stepNumber, err)
		}
	}

	if pending == nil {
		pending = readPacketAsync(current)
	}
	timer := time.NewTimer(quietPeriod)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case got := <-pending:
		timer.Stop()
		if got.err != nil {
			return fmt.Errorf("final quiet read: %w", got.err)
		}
		return fmt.Errorf("late or duplicate Datagram delivered bytes=%d digest=%s", len(got.body), digest(got.body))
	case <-timer.C:
	}
	if err := log.write("no_late_or_duplicate_delivery", 0, nil); err != nil {
		return err
	}
	if err := createMarker(marker(opts, "listener-complete")); err != nil {
		return fmt.Errorf("create listener completion marker: %w", err)
	}
	if err := waitForMarker(ctx, marker(opts, "finish")); err != nil {
		return fmt.Errorf("wait for finish marker: %w", err)
	}
	return log.write("scenario_complete", 0, nil)
}

func runInitiator(ctx context.Context, opts options, log *eventLog, peer *mpudp.Peer) error {
	current, err := peer.NewSession()
	if err != nil {
		return fmt.Errorf("create public Session: %w", err)
	}
	if err := writeEventually(ctx, log, current, "bootstrap_sent", makeDatagram(bootstrapBytes, 0x11)); err != nil {
		return err
	}
	if err := readExpected(ctx, log, current, "bootstrap_received", 0, makeDatagram(bootstrapBytes, 0x12)); err != nil {
		return err
	}
	if err := createMarker(marker(opts, "handshake-complete")); err != nil {
		return fmt.Errorf("create handshake marker: %w", err)
	}
	if err := waitForMarker(ctx, marker(opts, "warmup-go")); err != nil {
		return fmt.Errorf("wait for warmup release: %w", err)
	}
	if err := writeExpected(log, current, "warmup_sent", 0, makeDatagram(warmupBytes, 0x13)); err != nil {
		return err
	}
	if err := readExpected(ctx, log, current, "warmup_received", 0, makeDatagram(warmupBytes, 0x14)); err != nil {
		return err
	}
	if err := createMarker(marker(opts, "scenario-ready")); err != nil {
		return fmt.Errorf("create scenario-ready marker: %w", err)
	}

	steps, _, _ := planFor(opts.scenario)
	for index, step := range steps {
		stepNumber := index + 1
		if err := waitForMarker(ctx, stepMarker(opts, stepNumber, "go")); err != nil {
			return fmt.Errorf("wait for step %d release: %w", stepNumber, err)
		}
		body := makeDatagram(step.bytes, step.salt)
		if err := writeExpected(log, current, "datagram_sent", stepNumber, body); err != nil {
			return fmt.Errorf("step %d: %w", stepNumber, err)
		}
		if err := createMarker(stepMarker(opts, stepNumber, "sent")); err != nil {
			return fmt.Errorf("create step %d sent marker: %w", stepNumber, err)
		}
	}
	if err := createMarker(marker(opts, "initiator-complete")); err != nil {
		return fmt.Errorf("create initiator completion marker: %w", err)
	}
	if err := waitForMarker(ctx, marker(opts, "finish")); err != nil {
		return fmt.Errorf("wait for finish marker: %w", err)
	}
	return log.write("scenario_complete", 0, nil)
}

func readPacketAsync(current mpudp.Session) <-chan packetResult {
	result := make(chan packetResult, 1)
	go func() {
		body, err := current.ReadPacket()
		result <- packetResult{body: body, err: err}
	}()
	return result
}

func readExpected(ctx context.Context, log *eventLog, current mpudp.Session, eventName string, step int, want []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case got := <-readPacketAsync(current):
		if err := checkPacket(got, want); err != nil {
			return err
		}
		return log.write(eventName, step, got.body)
	}
}

func checkPacket(got packetResult, want []byte) error {
	if got.err != nil {
		return got.err
	}
	if !bytes.Equal(got.body, want) {
		return fmt.Errorf("Datagram mismatch bytes=%d digest=%s", len(got.body), digest(got.body))
	}
	return nil
}

func writeEventually(ctx context.Context, log *eventLog, current mpudp.Session, eventName string, body []byte) error {
	for {
		err := current.WritePacket(body)
		if err == nil {
			return log.write(eventName, 0, body)
		}
		if !errors.Is(err, mpudp.ErrNotReady) {
			return fmt.Errorf("write bootstrap Datagram: %w", err)
		}
		timer := time.NewTimer(markerPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func writeExpected(log *eventLog, current mpudp.Session, eventName string, step int, body []byte) error {
	if err := current.WritePacket(body); err != nil {
		return fmt.Errorf("write public Datagram: %w", err)
	}
	return log.write(eventName, step, body)
}

func marker(opts options, name string) string {
	return filepath.Join(opts.syncDir, name)
}

func stepMarker(opts options, step int, suffix string) string {
	return marker(opts, fmt.Sprintf("step-%02d-%s", step, suffix))
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
		timer := time.NewTimer(markerPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func openEventLog(opts options) (*eventLog, error) {
	if info, err := os.Lstat(opts.eventsPath); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("refusing symlink event output")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect event output: %w", err)
	}
	file, err := os.OpenFile(opts.eventsPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open event output: %w", err)
	}
	return &eventLog{
		file: file, encoder: json.NewEncoder(file), start: time.Now(),
		base: event{RunID: opts.runID, Role: opts.role, Scenario: opts.scenario},
	}, nil
}

func (l *eventLog) write(name string, step int, body []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	entry := l.base
	entry.Event = name
	entry.Step = step
	entry.ElapsedMS = now.Sub(l.start).Milliseconds()
	entry.ObservedNS = now.UnixNano()
	if body != nil {
		size := len(body)
		entry.Bytes = &size
		entry.Digest = digest(body)
	}
	return l.encoder.Encode(entry)
}

func (l *eventLog) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

func makeDatagram(size int, salt byte) []byte {
	body := make([]byte, size)
	for index := range body {
		body[index] = byte((index*37 + int(salt)) % 251)
	}
	return body
}

func digest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:6])
}
