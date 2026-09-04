// peerprobe is a test-only driver for the public MPUDP Datagram API. It emits
// metadata and short digests, never Datagram contents, keys, tags, or IDs.
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
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mofelee/mpudp"
	"github.com/mofelee/mpudp/config"
)

const (
	integrationKey        = "integration-test-key"
	dataShardWireOverhead = 71
	bootstrapBytes        = 96
	markerPollInterval    = 10 * time.Millisecond
	carrierSettleDuration = 500 * time.Millisecond
)

var (
	runIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)
	flowPattern  = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
)

type options struct {
	role                 string
	flow                 string
	family               int
	listen               string
	carriers             string
	maxUDPPayload        int
	bodyBytes            int
	replyBytes           int
	oversizeBytes        int
	expectPartialMTU     bool
	rejectSecondSession  bool
	endpointTTL          time.Duration
	keepaliveInterval    time.Duration
	expiryWait           time.Duration
	timeout              time.Duration
	runID                string
	eventsPath           string
	readyPath            string
	oversizeReadyPath    string
	oversizeContinuePath string
	oversizeDonePath     string
	phasePath            string
	continuePath         string
	donePath             string
	replyCompletePath    string
	exitPath             string
	finalPath            string
}

type event struct {
	RunID      string   `json:"run_id"`
	Role       string   `json:"role"`
	Flow       string   `json:"flow"`
	Event      string   `json:"event"`
	Phase      string   `json:"phase,omitempty"`
	Bytes      int      `json:"bytes,omitempty"`
	Digest     string   `json:"sha256_prefix,omitempty"`
	ErrorKinds []string `json:"error_kinds,omitempty"`
	ElapsedMS  int64    `json:"elapsed_ms"`
}

type eventLog struct {
	mu      sync.Mutex
	file    *os.File
	encoder *json.Encoder
	start   time.Time
	base    event
}

func main() {
	var opts options
	flag.StringVar(&opts.role, "role", "", "listener or initiator")
	flag.StringVar(&opts.flow, "flow", "", "stable integration flow name")
	flag.IntVar(&opts.family, "family", 0, "address family: 4 or 6")
	flag.StringVar(&opts.listen, "listen", "", "listener address")
	flag.StringVar(&opts.carriers, "carriers", "", "comma-separated initiator remotes")
	flag.IntVar(&opts.maxUDPPayload, "max-udp-payload", config.DefaultMaxUDPPayload, "local maximum MPUDP UDP payload")
	flag.IntVar(&opts.bodyBytes, "body-bytes", 0, "initiator-to-listener Datagram size")
	flag.IntVar(&opts.replyBytes, "reply-bytes", 0, "listener-to-initiator Datagram size")
	flag.IntVar(&opts.oversizeBytes, "oversize-bytes", 0, "Datagram size that must be rejected before send")
	flag.BoolVar(&opts.expectPartialMTU, "expect-partial-mtu", false, "require ErrPartialSend and ErrPathMTUExceeded")
	flag.BoolVar(&opts.rejectSecondSession, "reject-second-session", false, "require no second accepted Session")
	flag.DurationVar(&opts.endpointTTL, "endpoint-ttl", config.DefaultEndpointTTL, "Endpoint lifetime")
	flag.DurationVar(&opts.keepaliveInterval, "keepalive", config.MaxKeepaliveInterval, "keepalive interval")
	flag.DurationVar(&opts.expiryWait, "expiry-wait", 0, "quiet period before the expiry assertion")
	flag.DurationVar(&opts.timeout, "timeout", 20*time.Second, "whole helper timeout")
	flag.StringVar(&opts.runID, "run-id", "", "integration run identifier")
	flag.StringVar(&opts.eventsPath, "events", "", "metadata-only NDJSON output")
	flag.StringVar(&opts.readyPath, "ready-file", "", "listener-ready marker")
	flag.StringVar(&opts.oversizeReadyPath, "oversize-ready-file", "", "initiator marker before an oversize write")
	flag.StringVar(&opts.oversizeContinuePath, "oversize-continue-file", "", "marker that releases an oversize write")
	flag.StringVar(&opts.oversizeDonePath, "oversize-done-file", "", "initiator marker after an oversize write")
	flag.StringVar(&opts.phasePath, "phase-file", "", "initiator marker before the main write")
	flag.StringVar(&opts.continuePath, "continue-file", "", "marker that releases the main write")
	flag.StringVar(&opts.donePath, "done-file", "", "successful listener completion marker")
	flag.StringVar(&opts.replyCompletePath, "reply-complete-file", "", "listener marker after every reply shard is sent")
	flag.StringVar(&opts.exitPath, "exit-file", "", "initiator completion marker used for ordered shutdown")
	flag.StringVar(&opts.finalPath, "final-file", "", "optional marker that releases final Peer shutdown")
	flag.Parse()

	if err := validateOptions(opts); err != nil {
		fmt.Fprintf(os.Stderr, "peerprobe: %v\n", err)
		os.Exit(2)
	}

	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalContext, opts.timeout)
	defer cancel()
	if err := run(ctx, opts); err != nil {
		fmt.Fprintf(os.Stderr, "peerprobe: %v\n", err)
		os.Exit(1)
	}
}

func validateOptions(opts options) error {
	if opts.role != "listener" && opts.role != "initiator" {
		return errors.New("role must be listener or initiator")
	}
	if !flowPattern.MatchString(opts.flow) {
		return errors.New("invalid flow name")
	}
	if opts.family != 4 && opts.family != 6 {
		return errors.New("family must be 4 or 6")
	}
	if !runIDPattern.MatchString(opts.runID) {
		return errors.New("invalid run ID")
	}
	if opts.eventsPath == "" {
		return errors.New("events path is required")
	}
	if opts.timeout <= 0 || opts.timeout > 2*time.Minute {
		return errors.New("timeout must be within (0,2m]")
	}
	if opts.bodyBytes < 0 || opts.replyBytes < 0 || opts.oversizeBytes < 0 {
		return errors.New("Datagram sizes must not be negative")
	}
	if opts.flow == "expiry" {
		if opts.expiryWait <= opts.endpointTTL {
			return errors.New("expiry-wait must exceed endpoint-ttl")
		}
		if opts.keepaliveInterval <= opts.expiryWait {
			return errors.New("keepalive must exceed expiry-wait")
		}
	} else if opts.bodyBytes == 0 || opts.replyBytes == 0 {
		return errors.New("non-expiry flows require positive body-bytes and reply-bytes")
	}
	if opts.role == "listener" {
		if opts.listen == "" || opts.carriers != "" || opts.readyPath == "" ||
			opts.oversizeReadyPath != "" || opts.oversizeContinuePath != "" || opts.oversizeDonePath != "" ||
			opts.phasePath != "" || opts.continuePath != "" {
			return errors.New("listener requires listen and ready-file, but no carriers")
		}
		if opts.flow != "expiry" && (opts.replyCompletePath == "" || opts.exitPath == "") {
			return errors.New("non-expiry listener requires reply-complete-file and exit-file")
		}
		if opts.flow == "expiry" && opts.replyCompletePath != "" {
			return errors.New("expiry listener does not use reply-complete-file")
		}
	} else {
		if opts.carriers == "" || opts.listen != "" || opts.readyPath != "" || opts.donePath != "" {
			return errors.New("initiator requires carriers, but no listen, ready-file, or done-file")
		}
		if opts.flow == "expiry" {
			if opts.phasePath != "" || opts.continuePath == "" || opts.replyCompletePath != "" || opts.exitPath != "" ||
				opts.oversizeReadyPath != "" || opts.oversizeContinuePath != "" || opts.oversizeDonePath != "" {
				return errors.New("expiry initiator requires only continue-file")
			}
		} else {
			if opts.replyCompletePath == "" || opts.exitPath == "" {
				return errors.New("non-expiry initiator requires reply-complete-file and exit-file")
			}
			if (opts.phasePath == "") != (opts.continuePath == "") {
				return errors.New("phase-file and continue-file must be supplied together")
			}
		}
	}
	if opts.expectPartialMTU && opts.role != "initiator" {
		return errors.New("expect-partial-mtu is initiator-only")
	}
	if opts.oversizeBytes != 0 && opts.role != "initiator" {
		return errors.New("oversize-bytes is initiator-only")
	}
	oversizeMarkers := []string{opts.oversizeReadyPath, opts.oversizeContinuePath, opts.oversizeDonePath}
	oversizeMarkerCount := 0
	for _, path := range oversizeMarkers {
		if path != "" {
			oversizeMarkerCount++
		}
	}
	if opts.oversizeBytes != 0 {
		if opts.role != "initiator" || oversizeMarkerCount != len(oversizeMarkers) || opts.phasePath == "" {
			return errors.New("oversize write requires all oversize markers and the main phase gate")
		}
	} else if oversizeMarkerCount != 0 {
		return errors.New("oversize markers require oversize-bytes")
	}
	return nil
}

func run(ctx context.Context, opts options) (returnErr error) {
	log, err := openEventLog(opts)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := log.close(); returnErr == nil && closeErr != nil {
			returnErr = closeErr
		}
	}()

	cfg := config.Default()
	cfg.FEC = config.FECConfig{DataShards: 3, ParityShards: 2}
	cfg.PSK = config.NewSecret(integrationKey)
	cfg.Transport.MaxUDPPayload = opts.maxUDPPayload
	cfg.Timers.EndpointTTL = opts.endpointTTL
	cfg.Timers.KeepaliveInterval = opts.keepaliveInterval
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
	if err := log.write("peer_ready", "", nil, nil); err != nil {
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
	if err := createMarker(opts.readyPath); err != nil {
		return fmt.Errorf("write ready marker: %w", err)
	}
	accepted, err := listener.Accept(ctx)
	if err != nil {
		return fmt.Errorf("accept public Session: %w", err)
	}
	if err := log.write("session_accepted", "", nil, nil); err != nil {
		return err
	}

	bootstrap := makeDatagram(bootstrapBytes, 0x31)
	if err := readExpected(ctx, log, accepted, "bootstrap", bootstrap); err != nil {
		return err
	}
	bootstrapReply := makeDatagram(bootstrapBytes, 0x72)
	if err := writeExpected(log, accepted, "bootstrap", bootstrapReply, false); err != nil {
		return err
	}

	if opts.flow == "expiry" {
		if err := log.write("expiry_wait_started", "expiry", nil, nil); err != nil {
			return err
		}
		if err := waitDuration(ctx, opts.expiryWait); err != nil {
			return err
		}
		err := accepted.WritePacket(makeDatagram(32, 0x5e))
		if !errors.Is(err, mpudp.ErrNoAvailablePaths) {
			return fmt.Errorf("expired Endpoint write classification did not match ErrNoAvailablePaths")
		}
		if err := log.write("endpoint_expired", "expiry", nil, classifyError(err)); err != nil {
			return err
		}
	} else {
		body := makeDatagram(opts.bodyBytes, 0xa3)
		if err := readExpected(ctx, log, accepted, "main", body); err != nil {
			return err
		}
		reply := makeDatagram(opts.replyBytes, 0x4d)
		if err := writeExpected(log, accepted, "main", reply, false); err != nil {
			return err
		}
		if err := createMarker(opts.replyCompletePath); err != nil {
			return fmt.Errorf("write reply completion marker: %w", err)
		}
		if err := waitForMarker(ctx, opts.exitPath); err != nil {
			return fmt.Errorf("wait for initiator completion: %w", err)
		}
		if opts.rejectSecondSession {
			secondContext, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
			_, secondErr := listener.Accept(secondContext)
			cancel()
			if !errors.Is(secondErr, context.DeadlineExceeded) {
				return errors.New("listener unexpectedly accepted a second Session")
			}
			if err := log.write("single_session_retained", "main", nil, nil); err != nil {
				return err
			}
		}
		if opts.finalPath != "" {
			if err := waitForMarker(ctx, opts.finalPath); err != nil {
				return fmt.Errorf("wait for final shutdown release: %w", err)
			}
		}
	}
	if opts.donePath != "" {
		if err := createMarker(opts.donePath); err != nil {
			return fmt.Errorf("write done marker: %w", err)
		}
	}
	return log.write("flow_complete", "", nil, nil)
}

func runInitiator(ctx context.Context, opts options, log *eventLog, peer *mpudp.Peer) error {
	current, err := peer.NewSession()
	if err != nil {
		return fmt.Errorf("create public Session: %w", err)
	}
	if err := log.write("session_started", "", nil, nil); err != nil {
		return err
	}

	bootstrap := makeDatagram(bootstrapBytes, 0x31)
	if err := writeEventually(ctx, log, current, "bootstrap", bootstrap); err != nil {
		return err
	}
	bootstrapReply := makeDatagram(bootstrapBytes, 0x72)
	if err := readExpected(ctx, log, current, "bootstrap", bootstrapReply); err != nil {
		return err
	}
	if err := waitDuration(ctx, carrierSettleDuration); err != nil {
		return err
	}

	if opts.flow == "expiry" {
		if err := waitForMarker(ctx, opts.continuePath); err != nil {
			return fmt.Errorf("wait for expiry completion: %w", err)
		}
		return log.write("flow_complete", "", nil, nil)
	}

	if opts.oversizeBytes != 0 {
		if err := createMarker(opts.oversizeReadyPath); err != nil {
			return fmt.Errorf("write oversize-ready marker: %w", err)
		}
		if err := waitForMarker(ctx, opts.oversizeContinuePath); err != nil {
			return fmt.Errorf("wait for oversize release: %w", err)
		}
		oversize := makeDatagram(opts.oversizeBytes, 0xce)
		err := current.WritePacket(oversize)
		if !errors.Is(err, mpudp.ErrMessageTooLarge) {
			return errors.New("oversize write classification did not match ErrMessageTooLarge")
		}
		if err := log.write("oversize_rejected", "oversize", oversize, classifyError(err)); err != nil {
			return err
		}
		if err := createMarker(opts.oversizeDonePath); err != nil {
			return fmt.Errorf("write oversize-done marker: %w", err)
		}
	}
	if opts.phasePath != "" {
		if err := createMarker(opts.phasePath); err != nil {
			return fmt.Errorf("write phase marker: %w", err)
		}
		if err := waitForMarker(ctx, opts.continuePath); err != nil {
			return fmt.Errorf("wait for phase release: %w", err)
		}
	}

	body := makeDatagram(opts.bodyBytes, 0xa3)
	err = current.WritePacket(body)
	if opts.expectPartialMTU {
		if !errors.Is(err, mpudp.ErrPartialSend) || !errors.Is(err, mpudp.ErrPathMTUExceeded) {
			return errors.New("main write did not match ErrPartialSend and ErrPathMTUExceeded")
		}
		if err := log.write("datagram_partially_sent", "main", body, classifyError(err)); err != nil {
			return err
		}
	} else {
		if err != nil {
			return fmt.Errorf("write main Datagram: %w", err)
		}
		if err := log.write("datagram_sent", "main", body, nil); err != nil {
			return err
		}
	}
	reply := makeDatagram(opts.replyBytes, 0x4d)
	if err := readExpected(ctx, log, current, "main", reply); err != nil {
		return err
	}
	if err := waitForMarker(ctx, opts.replyCompletePath); err != nil {
		return fmt.Errorf("wait for listener reply completion: %w", err)
	}
	if err := createMarker(opts.exitPath); err != nil {
		return fmt.Errorf("write initiator completion marker: %w", err)
	}
	if opts.finalPath != "" {
		if err := waitForMarker(ctx, opts.finalPath); err != nil {
			return fmt.Errorf("wait for final shutdown release: %w", err)
		}
	}
	return log.write("flow_complete", "", nil, nil)
}

func writeEventually(ctx context.Context, log *eventLog, current mpudp.Session, phase string, body []byte) error {
	for {
		err := current.WritePacket(body)
		if err == nil {
			return log.write("datagram_sent", phase, body, nil)
		}
		if !errors.Is(err, mpudp.ErrNotReady) {
			return fmt.Errorf("write %s Datagram: %w", phase, err)
		}
		if err := waitDuration(ctx, markerPollInterval); err != nil {
			return fmt.Errorf("wait for public Session readiness: %w", err)
		}
	}
}

func writeExpected(log *eventLog, current mpudp.Session, phase string, body []byte, allowPartial bool) error {
	err := current.WritePacket(body)
	if err != nil && !(allowPartial && errors.Is(err, mpudp.ErrPartialSend)) {
		return fmt.Errorf("write %s reply Datagram: %w", phase, err)
	}
	return log.write("datagram_sent", phase, body, classifyError(err))
}

func readExpected(ctx context.Context, log *eventLog, current mpudp.Session, phase string, want []byte) error {
	type result struct {
		body []byte
		err  error
	}
	resultChannel := make(chan result, 1)
	go func() {
		body, err := current.ReadPacket()
		resultChannel <- result{body: body, err: err}
	}()
	select {
	case <-ctx.Done():
		_ = current.Close()
		return ctx.Err()
	case got := <-resultChannel:
		if got.err != nil {
			return fmt.Errorf("read %s Datagram: %w", phase, got.err)
		}
		if !bytes.Equal(got.body, want) {
			return fmt.Errorf("read %s Datagram metadata mismatch: bytes=%d digest=%s", phase, len(got.body), digest(got.body))
		}
		return log.write("datagram_received", phase, got.body, nil)
	}
}

func makeDatagram(size int, seed byte) []byte {
	body := make([]byte, size)
	for index := range body {
		body[index] = byte((int(seed) + index*31 + index/7) % 251)
	}
	return body
}

func classifyError(err error) []string {
	if err == nil {
		return nil
	}
	known := []struct {
		kind error
		name string
	}{
		{mpudp.ErrMessageTooLarge, "message_too_large"},
		{mpudp.ErrPartialSend, "partial_send"},
		{mpudp.ErrPathMTUExceeded, "path_mtu_exceeded"},
		{mpudp.ErrAllSendsFailed, "all_sends_failed"},
		{mpudp.ErrNoAvailablePaths, "no_available_paths"},
		{mpudp.ErrNotReady, "not_ready"},
		{mpudp.ErrClosed, "closed"},
	}
	var result []string
	for _, candidate := range known {
		if errors.Is(err, candidate.kind) {
			result = append(result, candidate.name)
		}
	}
	if len(result) == 0 {
		return []string{"other"}
	}
	return result
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
		base: event{RunID: opts.runID, Role: opts.role, Flow: opts.flow},
	}, nil
}

func (l *eventLog) write(name, phase string, body []byte, errorKinds []string) error {
	current := l.base
	current.Event = name
	current.Phase = phase
	current.ElapsedMS = time.Since(l.start).Milliseconds()
	current.ErrorKinds = errorKinds
	if body != nil {
		current.Bytes = len(body)
		current.Digest = digest(body)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.encoder.Encode(current); err != nil {
		return fmt.Errorf("write event metadata: %w", err)
	}
	return nil
}

func (l *eventLog) close() error {
	if l == nil || l.file == nil {
		return nil
	}
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("close event output: %w", err)
	}
	return nil
}

func digest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:6])
}

func createMarker(path string) error {
	if path == "" {
		return errors.New("empty marker path")
	}
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

func effectiveDatagramLimit(maxUDPPayload int) int {
	return 3 * (maxUDPPayload - dataShardWireOverhead)
}
