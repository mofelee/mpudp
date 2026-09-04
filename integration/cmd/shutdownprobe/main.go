// shutdownprobe holds a public Peer at a named lifecycle phase so the Linux
// harness can trigger and time SIGTERM- or API-driven shutdown. Its event log
// contains lifecycle metadata only.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mofelee/mpudp"
	"github.com/mofelee/mpudp/config"
)

const (
	integrationKey       = "integration-test-key"
	markerPollInterval   = 10 * time.Millisecond
	maxPeerCloseDuration = 2 * time.Second
	bootstrapBytes       = 96
)

var runIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)

type options struct {
	role           string
	phase          string
	action         string
	family         int
	listen         string
	carriers       string
	bodyBytes      int
	timeout        time.Duration
	runID          string
	eventsPath     string
	readyPath      string
	phaseReadyPath string
	bootstrapPath  string
	sendPath       string
	sentPath       string
	closePath      string
}

type eventLog struct {
	mu      sync.Mutex
	file    *os.File
	encoder *json.Encoder
	start   time.Time
	runID   string
	role    string
	phase   string
}

type readResult struct {
	body []byte
	err  error
}

func main() {
	var opts options
	flag.StringVar(&opts.role, "role", "", "listener or initiator")
	flag.StringVar(&opts.phase, "phase", "", "handshake, active-transfer, decode-incomplete, or network-fault")
	flag.StringVar(&opts.action, "action", "", "signal or close")
	flag.IntVar(&opts.family, "family", 0, "address family: 4 or 6")
	flag.StringVar(&opts.listen, "listen", "", "listener address")
	flag.StringVar(&opts.carriers, "carriers", "", "comma-separated initiator remotes")
	flag.IntVar(&opts.bodyBytes, "body-bytes", 1536, "in-flight Datagram size")
	flag.DurationVar(&opts.timeout, "timeout", 20*time.Second, "whole helper timeout")
	flag.StringVar(&opts.runID, "run-id", "", "integration run identifier")
	flag.StringVar(&opts.eventsPath, "events", "", "metadata-only NDJSON output")
	flag.StringVar(&opts.readyPath, "ready-file", "", "listener socket-ready marker")
	flag.StringVar(&opts.phaseReadyPath, "phase-ready-file", "", "role-specific phase marker")
	flag.StringVar(&opts.bootstrapPath, "bootstrap-file", "", "release marker after every Carrier authenticates")
	flag.StringVar(&opts.sendPath, "send-file", "", "release marker for an in-flight Datagram")
	flag.StringVar(&opts.sentPath, "sent-file", "", "marker after the in-flight write returns")
	flag.StringVar(&opts.closePath, "close-file", "", "marker that requests public Peer.Close")
	flag.Parse()

	if err := validateOptions(opts); err != nil {
		fmt.Fprintf(os.Stderr, "shutdownprobe: %v\n", err)
		os.Exit(2)
	}
	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalContext, opts.timeout)
	defer cancel()
	if err := run(ctx, opts); err != nil {
		fmt.Fprintf(os.Stderr, "shutdownprobe: %v\n", err)
		os.Exit(1)
	}
}

func validateOptions(opts options) error {
	if opts.role != "listener" && opts.role != "initiator" {
		return errors.New("role must be listener or initiator")
	}
	wantAction := map[string]string{
		"handshake":         "signal",
		"active-transfer":   "close",
		"decode-incomplete": "signal",
		"network-fault":     "close",
	}
	if expected, ok := wantAction[opts.phase]; !ok || opts.action != expected {
		return errors.New("phase/action pair must be handshake/signal, active-transfer/close, decode-incomplete/signal, or network-fault/close")
	}
	if opts.family != 4 && opts.family != 6 {
		return errors.New("family must be 4 or 6")
	}
	if !runIDPattern.MatchString(opts.runID) {
		return errors.New("invalid run ID")
	}
	if opts.eventsPath == "" || opts.phaseReadyPath == "" {
		return errors.New("events and phase-ready-file are required")
	}
	if opts.timeout <= 0 || opts.timeout > 2*time.Minute {
		return errors.New("timeout must be within (0,2m]")
	}
	if opts.bodyBytes <= 0 {
		return errors.New("body-bytes must be positive")
	}
	if opts.action == "close" && opts.closePath == "" {
		return errors.New("close action requires close-file")
	}
	if opts.action == "signal" && opts.closePath != "" {
		return errors.New("signal action must not use close-file")
	}
	needsSend := opts.phase == "active-transfer" || opts.phase == "decode-incomplete" || opts.phase == "network-fault"
	if opts.role == "listener" {
		if opts.listen == "" || opts.carriers != "" || opts.readyPath == "" || opts.bootstrapPath != "" || opts.sendPath != "" || opts.sentPath != "" {
			return errors.New("listener requires listen and ready-file without carriers or send markers")
		}
		return nil
	}
	if opts.carriers == "" || opts.listen != "" || opts.readyPath != "" {
		return errors.New("initiator requires carriers without listen or ready-file")
	}
	if opts.phase == "handshake" && opts.bootstrapPath != "" {
		return errors.New("handshake phase must not use bootstrap-file")
	}
	if opts.phase != "handshake" && opts.bootstrapPath == "" {
		return errors.New("established phase requires bootstrap-file")
	}
	if needsSend && (opts.sendPath == "" || opts.sentPath == "") {
		return errors.New("in-flight phase requires send-file and sent-file")
	}
	if !needsSend && (opts.sendPath != "" || opts.sentPath != "") {
		return errors.New("non-transfer phase must not use send markers")
	}
	return nil
}

func run(ctx context.Context, opts options) error {
	log, err := openEventLog(opts)
	if err != nil {
		return err
	}
	defer log.close()
	cfg := config.Default()
	cfg.FEC = config.FECConfig{DataShards: 3, ParityShards: 2}
	cfg.PSK = config.NewSecret(integrationKey)
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
	returnErr := error(nil)
	defer func() {
		// This fallback is reached only if a pre-phase error bypasses closePeer.
		if returnErr == nil {
			return
		}
		_ = peer.Close()
	}()
	if err := log.write("peer_ready", ""); err != nil {
		returnErr = err
		return returnErr
	}

	if opts.role == "listener" {
		returnErr = runListener(ctx, opts, log, peer)
	} else {
		returnErr = runInitiator(ctx, opts, log, peer)
	}
	closeErr := closePeer(log, peer)
	if returnErr == nil {
		returnErr = closeErr
	}
	return returnErr
}

func runListener(ctx context.Context, opts options, log *eventLog, peer *mpudp.Peer) error {
	listener, err := peer.Listener()
	if err != nil {
		return fmt.Errorf("obtain public Listener: %w", err)
	}
	if err := createMarker(opts.readyPath); err != nil {
		return fmt.Errorf("create listener-ready marker: %w", err)
	}
	if opts.phase == "handshake" {
		if err := reachPhase(log, opts.phaseReadyPath); err != nil {
			return err
		}
		return waitForShutdown(ctx, opts, log, nil)
	}
	current, err := listener.Accept(ctx)
	if err != nil {
		return fmt.Errorf("accept public Session: %w", err)
	}
	if err := log.write("session_accepted", ""); err != nil {
		return err
	}
	if err := readExpected(ctx, current, makeDatagram(bootstrapBytes, 0x31)); err != nil {
		return fmt.Errorf("read bootstrap: %w", err)
	}
	if err := current.WritePacket(makeDatagram(bootstrapBytes, 0x72)); err != nil {
		return fmt.Errorf("write bootstrap reply: %w", err)
	}
	if err := log.write("bootstrap_complete", ""); err != nil {
		return err
	}
	if err := reachPhase(log, opts.phaseReadyPath); err != nil {
		return err
	}
	if opts.phase != "active-transfer" && opts.phase != "decode-incomplete" {
		return waitForShutdown(ctx, opts, log, nil)
	}
	readChannel := make(chan readResult, 1)
	go func() {
		body, readErr := current.ReadPacket()
		readChannel <- readResult{body: body, err: readErr}
	}()
	return waitForShutdown(ctx, opts, log, readChannel)
}

func runInitiator(ctx context.Context, opts options, log *eventLog, peer *mpudp.Peer) error {
	current, err := peer.NewSession()
	if err != nil {
		return fmt.Errorf("create public Session: %w", err)
	}
	if err := log.write("session_started", ""); err != nil {
		return err
	}
	if opts.phase == "handshake" {
		if err := reachPhase(log, opts.phaseReadyPath); err != nil {
			return err
		}
		return waitForShutdown(ctx, opts, log, nil)
	}
	if err := waitForMarker(ctx, opts.bootstrapPath); err != nil {
		return fmt.Errorf("wait for authenticated Carrier gate: %w", err)
	}
	if err := writeEventually(ctx, current, makeDatagram(bootstrapBytes, 0x31)); err != nil {
		return fmt.Errorf("write bootstrap: %w", err)
	}
	if err := readExpected(ctx, current, makeDatagram(bootstrapBytes, 0x72)); err != nil {
		return fmt.Errorf("read bootstrap reply: %w", err)
	}
	if err := log.write("bootstrap_complete", ""); err != nil {
		return err
	}
	if err := reachPhase(log, opts.phaseReadyPath); err != nil {
		return err
	}
	if opts.phase == "active-transfer" || opts.phase == "decode-incomplete" || opts.phase == "network-fault" {
		if err := waitForMarker(ctx, opts.sendPath); err != nil {
			return fmt.Errorf("wait for send release: %w", err)
		}
		body := makeDatagram(opts.bodyBytes, 0xa3)
		if err := current.WritePacket(body); err != nil {
			return fmt.Errorf("write in-flight Datagram: %w", err)
		}
		if err := log.write("main_sent", ""); err != nil {
			return err
		}
		if err := createMarker(opts.sentPath); err != nil {
			return fmt.Errorf("create sent marker: %w", err)
		}
	}
	return waitForShutdown(ctx, opts, log, nil)
}

func waitForShutdown(ctx context.Context, opts options, log *eventLog, reads <-chan readResult) error {
	if opts.action == "signal" {
		select {
		case result := <-reads:
			return unexpectedRead(result)
		case <-ctx.Done():
			if !errors.Is(ctx.Err(), context.Canceled) {
				return fmt.Errorf("signal phase timed out: %w", ctx.Err())
			}
			return log.write("shutdown_trigger", "signal")
		}
	}
	ticker := time.NewTicker(markerPollInterval)
	defer ticker.Stop()
	for {
		select {
		case result := <-reads:
			return unexpectedRead(result)
		case <-ctx.Done():
			return fmt.Errorf("wait for API close marker: %w", ctx.Err())
		case <-ticker.C:
			info, err := os.Lstat(opts.closePath)
			if err == nil {
				if !info.Mode().IsRegular() {
					return errors.New("close marker is not a regular file")
				}
				return log.write("shutdown_trigger", "close")
			}
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
}

func unexpectedRead(result readResult) error {
	if result.err != nil {
		return fmt.Errorf("blocked phase read ended before shutdown: %w", result.err)
	}
	return fmt.Errorf("Datagram completed before shutdown: bytes=%d", len(result.body))
}

func closePeer(log *eventLog, peer *mpudp.Peer) error {
	if err := log.write("peer_close_started", ""); err != nil {
		_ = peer.Close()
		return err
	}
	started := time.Now()
	closeErr := peer.Close()
	elapsed := time.Since(started)
	if err := log.writeMetrics("peer_close_complete", map[string]int64{"close_ms": elapsed.Milliseconds()}); err != nil && closeErr == nil {
		closeErr = err
	}
	if elapsed > maxPeerCloseDuration {
		closeErr = errors.Join(closeErr, fmt.Errorf("Peer.Close took %s, limit %s", elapsed, maxPeerCloseDuration))
	}
	return closeErr
}

func reachPhase(log *eventLog, marker string) error {
	if err := log.write("phase_ready", ""); err != nil {
		return err
	}
	if err := createMarker(marker); err != nil {
		return fmt.Errorf("create phase marker: %w", err)
	}
	return nil
}

func writeEventually(ctx context.Context, current mpudp.Session, body []byte) error {
	for {
		err := current.WritePacket(body)
		if err == nil {
			return nil
		}
		if !errors.Is(err, mpudp.ErrNotReady) {
			return err
		}
		if err := waitDuration(ctx, markerPollInterval); err != nil {
			return err
		}
	}
}

func readExpected(ctx context.Context, current mpudp.Session, want []byte) error {
	resultChannel := make(chan readResult, 1)
	go func() {
		body, err := current.ReadPacket()
		resultChannel <- readResult{body: body, err: err}
	}()
	select {
	case <-ctx.Done():
		_ = current.Close()
		return ctx.Err()
	case result := <-resultChannel:
		if result.err != nil {
			return result.err
		}
		if !bytes.Equal(result.body, want) {
			return fmt.Errorf("Datagram metadata mismatch: bytes=%d", len(result.body))
		}
		return nil
	}
}

func makeDatagram(size int, seed byte) []byte {
	body := make([]byte, size)
	for index := range body {
		body[index] = byte((int(seed) + index*31 + index/7) % 251)
	}
	return body
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
		runID: opts.runID, role: opts.role, phase: opts.phase,
	}, nil
}

func (l *eventLog) write(name, trigger string) error {
	record := map[string]any{
		"elapsed_ms": time.Since(l.start).Milliseconds(),
		"event":      name,
		"phase":      l.phase,
		"role":       l.role,
		"run_id":     l.runID,
	}
	if trigger != "" {
		record["trigger"] = trigger
	}
	return l.encode(record)
}

func (l *eventLog) writeMetrics(name string, metrics map[string]int64) error {
	record := map[string]any{
		"elapsed_ms": time.Since(l.start).Milliseconds(),
		"event":      name,
		"phase":      l.phase,
		"role":       l.role,
		"run_id":     l.runID,
	}
	for name, value := range metrics {
		record[name] = value
	}
	return l.encode(record)
}

func (l *eventLog) encode(record map[string]any) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.encoder.Encode(record); err != nil {
		return fmt.Errorf("write event metadata: %w", err)
	}
	return nil
}

func (l *eventLog) close() error {
	if l == nil || l.file == nil {
		return nil
	}
	return l.file.Close()
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
		if err := waitDuration(ctx, markerPollInterval); err != nil {
			return err
		}
	}
}

func waitDuration(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
