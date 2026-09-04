// pollutionprobe drives the adversarial half of the auth-and-state-pollution
// integration case. It records bounded metadata only, never keys, packet bytes,
// authentication tags, Session IDs, or application payloads.
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
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mofelee/mpudp"
	"github.com/mofelee/mpudp/config"
	"github.com/mofelee/mpudp/internal/wire"
)

const (
	integrationKey       = "integration-test-key"
	wrongIntegrationKey  = "definitely-not-the-integration-key"
	markerPollInterval   = 10 * time.Millisecond
	responseWait         = 300 * time.Millisecond
	barrierResponseWait  = 2 * time.Second
	maxRetainedHeapBytes = 8 << 20
	maxGoroutineGrowth   = 8
	bootstrapBytes       = 96
)

var runIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)

type options struct {
	role              string
	family            int
	listen            string
	target            string
	maxUDPPayload     int
	highSources       int
	bodyBytes         int
	replyBytes        int
	timeout           time.Duration
	runID             string
	eventsPath        string
	readyPath         string
	attackDonePath    string
	checkedPath       string
	replyCompletePath string
	exitPath          string
	finalPath         string
}

type eventLog struct {
	mu      sync.Mutex
	file    *os.File
	encoder *json.Encoder
	start   time.Time
	runID   string
	role    string
}

type packetSpec struct {
	kind   string
	packet []byte
}

type sentPacket struct {
	kind string
	conn *net.UDPConn
}

type responseResult struct {
	kind     string
	response bool
	err      error
}

func main() {
	var opts options
	flag.StringVar(&opts.role, "role", "", "listener or attacker")
	flag.IntVar(&opts.family, "family", 0, "address family: 4 or 6")
	flag.StringVar(&opts.listen, "listen", "", "listener address")
	flag.StringVar(&opts.target, "target", "", "attacker target")
	flag.IntVar(&opts.maxUDPPayload, "max-udp-payload", config.DefaultMaxUDPPayload, "listener UDP payload budget")
	flag.IntVar(&opts.highSources, "high-sources", 128, "number of distinct unauthenticated source sockets")
	flag.IntVar(&opts.bodyBytes, "body-bytes", 512, "legitimate client-to-listener Datagram size")
	flag.IntVar(&opts.replyBytes, "reply-bytes", 257, "legitimate listener-to-client Datagram size")
	flag.DurationVar(&opts.timeout, "timeout", 20*time.Second, "whole helper timeout")
	flag.StringVar(&opts.runID, "run-id", "", "integration run identifier")
	flag.StringVar(&opts.eventsPath, "events", "", "metadata-only NDJSON output")
	flag.StringVar(&opts.readyPath, "ready-file", "", "listener-ready marker")
	flag.StringVar(&opts.attackDonePath, "attack-done-file", "", "attacker completion marker")
	flag.StringVar(&opts.checkedPath, "checked-file", "", "clean-state marker")
	flag.StringVar(&opts.replyCompletePath, "reply-complete-file", "", "legitimate reply completion marker")
	flag.StringVar(&opts.exitPath, "exit-file", "", "legitimate initiator completion marker")
	flag.StringVar(&opts.finalPath, "final-file", "", "final shutdown release marker")
	flag.Parse()

	if err := validateOptions(opts); err != nil {
		fmt.Fprintf(os.Stderr, "pollutionprobe: %v\n", err)
		os.Exit(2)
	}
	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalContext, opts.timeout)
	defer cancel()
	if err := run(ctx, opts); err != nil {
		fmt.Fprintf(os.Stderr, "pollutionprobe: %v\n", err)
		os.Exit(1)
	}
}

func validateOptions(opts options) error {
	if opts.role != "listener" && opts.role != "attacker" {
		return errors.New("role must be listener or attacker")
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
	if opts.maxUDPPayload < config.MinMaxUDPPayload || opts.maxUDPPayload > config.MaxMaxUDPPayload {
		return errors.New("invalid max UDP payload")
	}
	if opts.highSources < 64 || opts.highSources > 512 {
		return errors.New("high-sources must be within [64,512]")
	}
	if opts.bodyBytes <= 0 || opts.replyBytes <= 0 {
		return errors.New("legitimate Datagram sizes must be positive")
	}
	listenerMarkers := []string{opts.readyPath, opts.attackDonePath, opts.checkedPath, opts.replyCompletePath, opts.exitPath, opts.finalPath}
	if opts.role == "listener" {
		if opts.listen == "" || opts.target != "" {
			return errors.New("listener requires listen and no target")
		}
		for _, marker := range listenerMarkers {
			if marker == "" {
				return errors.New("listener requires all lifecycle markers")
			}
		}
		return nil
	}
	if opts.target == "" || opts.listen != "" {
		return errors.New("attacker requires target and no listen")
	}
	for _, marker := range listenerMarkers {
		if marker != "" {
			return errors.New("attacker does not accept listener lifecycle markers")
		}
	}
	return nil
}

func run(ctx context.Context, opts options) error {
	log, err := openEventLog(opts)
	if err != nil {
		return err
	}
	defer log.close()
	if opts.role == "attacker" {
		return runAttacker(ctx, opts, log)
	}
	return runListener(ctx, opts, log)
}

func runListener(ctx context.Context, opts options, log *eventLog) (returnErr error) {
	cfg := config.Default()
	cfg.FEC = config.FECConfig{DataShards: 3, ParityShards: 2}
	cfg.PSK = config.NewSecret(integrationKey)
	cfg.Listen = opts.listen
	cfg.Transport.MaxUDPPayload = opts.maxUDPPayload
	peer, err := mpudp.NewPeer(cfg)
	if err != nil {
		return fmt.Errorf("create listener Peer: %w", err)
	}
	defer func() {
		_ = log.write("peer_close_started", "shutdown", nil)
		started := time.Now()
		closeErr := peer.Close()
		logErr := log.write("peer_close_complete", "shutdown", map[string]int64{"close_ms": time.Since(started).Milliseconds()})
		if returnErr == nil && closeErr != nil {
			returnErr = fmt.Errorf("close listener Peer: %w", closeErr)
		}
		if returnErr == nil && logErr != nil {
			returnErr = logErr
		}
	}()
	listener, err := peer.Listener()
	if err != nil {
		return fmt.Errorf("obtain public Listener: %w", err)
	}

	runtime.GC()
	baselineGoroutines := runtime.NumGoroutine()
	var baselineMemory runtime.MemStats
	runtime.ReadMemStats(&baselineMemory)
	if err := log.write("peer_ready", "attack", nil); err != nil {
		return err
	}
	if err := createMarker(opts.readyPath); err != nil {
		return fmt.Errorf("create ready marker: %w", err)
	}
	if err := waitForMarker(ctx, opts.attackDonePath); err != nil {
		return fmt.Errorf("wait for attack completion: %w", err)
	}
	if rendered := peer.String(); rendered != "Peer{mode:listener sessions:1 closed:false}" {
		return fmt.Errorf("processing barrier did not isolate exactly one Session: %s", rendered)
	}
	barrier, err := listener.Accept(ctx)
	if err != nil {
		return fmt.Errorf("accept processing barrier Session: %w", err)
	}
	if err := barrier.Close(); err != nil {
		return fmt.Errorf("close processing barrier Session: %w", err)
	}
	if rendered := peer.String(); rendered != "Peer{mode:listener sessions:0 closed:false}" {
		return fmt.Errorf("processing barrier retained state after close: %s", rendered)
	}
	if err := log.write("processing_barrier_drained", "attack", map[string]int64{"sessions": 0}); err != nil {
		return err
	}

	acceptContext, cancelAccept := context.WithTimeout(ctx, 200*time.Millisecond)
	unexpected, acceptErr := listener.Accept(acceptContext)
	cancelAccept()
	if unexpected != nil {
		_ = unexpected.Close()
		return errors.New("unauthenticated traffic created an accepted Session")
	}
	if !errors.Is(acceptErr, context.DeadlineExceeded) {
		return fmt.Errorf("pollution Accept result = %v, want deadline with no Session", acceptErr)
	}
	if rendered := peer.String(); rendered != "Peer{mode:listener sessions:0 closed:false}" {
		return fmt.Errorf("listener retained state after attacks: %s", rendered)
	}
	diagnostics := 0
drainDiagnostics:
	for {
		select {
		case <-peer.Errors():
			diagnostics++
		default:
			break drainDiagnostics
		}
	}
	if diagnostics > 1 {
		return fmt.Errorf("diagnostic queue retained %d entries, want at most 1", diagnostics)
	}
	runtime.GC()
	var afterMemory runtime.MemStats
	runtime.ReadMemStats(&afterMemory)
	heapGrowth := int64(afterMemory.HeapAlloc) - int64(baselineMemory.HeapAlloc)
	goroutineGrowth := runtime.NumGoroutine() - baselineGoroutines
	if heapGrowth > maxRetainedHeapBytes {
		return fmt.Errorf("retained heap grew by %d bytes, limit %d", heapGrowth, maxRetainedHeapBytes)
	}
	if goroutineGrowth > maxGoroutineGrowth {
		return fmt.Errorf("goroutines grew by %d, limit %d", goroutineGrowth, maxGoroutineGrowth)
	}
	metrics := map[string]int64{
		"diagnostics":     int64(diagnostics),
		"goroutine_delta": int64(goroutineGrowth),
		"heap_growth":     heapGrowth,
		"sessions":        0,
	}
	if err := log.write("pollution_state_clean", "attack", metrics); err != nil {
		return err
	}
	if err := createMarker(opts.checkedPath); err != nil {
		return fmt.Errorf("create checked marker: %w", err)
	}

	accepted, err := listener.Accept(ctx)
	if err != nil {
		return fmt.Errorf("accept legitimate Session after attacks: %w", err)
	}
	if err := log.write("legitimate_session_accepted", "liveness", nil); err != nil {
		return err
	}
	bootstrap := makeDatagram(bootstrapBytes, 0x31)
	if err := readExpected(ctx, accepted, bootstrap); err != nil {
		return fmt.Errorf("read legitimate bootstrap: %w", err)
	}
	if err := accepted.WritePacket(makeDatagram(bootstrapBytes, 0x72)); err != nil {
		return fmt.Errorf("write legitimate bootstrap reply: %w", err)
	}
	body := makeDatagram(opts.bodyBytes, 0xa3)
	if err := readExpected(ctx, accepted, body); err != nil {
		return fmt.Errorf("read legitimate Datagram: %w", err)
	}
	reply := makeDatagram(opts.replyBytes, 0x4d)
	if err := accepted.WritePacket(reply); err != nil {
		return fmt.Errorf("write legitimate reply: %w", err)
	}
	if err := log.write("legitimate_exchange_complete", "liveness", map[string]int64{"received_bytes": int64(len(body)), "reply_bytes": int64(len(reply))}); err != nil {
		return err
	}
	if err := createMarker(opts.replyCompletePath); err != nil {
		return fmt.Errorf("create reply-complete marker: %w", err)
	}
	if err := waitForMarker(ctx, opts.exitPath); err != nil {
		return fmt.Errorf("wait for legitimate client completion: %w", err)
	}
	if err := waitForMarker(ctx, opts.finalPath); err != nil {
		return fmt.Errorf("wait for final release: %w", err)
	}
	return log.write("flow_complete", "liveness", nil)
}

func runAttacker(ctx context.Context, opts options, log *eventLog) error {
	target, err := net.ResolveUDPAddr(fmt.Sprintf("udp%d", opts.family), opts.target)
	if err != nil {
		return fmt.Errorf("resolve attack target: %w", err)
	}
	specs, err := buildAttackPackets(opts.maxUDPPayload, opts.highSources)
	if err != nil {
		return err
	}
	sent := make([]sentPacket, 0, len(specs))
	defer func() {
		for _, current := range sent {
			_ = current.conn.Close()
		}
	}()
	uniqueSources := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		connection, dialErr := net.DialUDP(fmt.Sprintf("udp%d", opts.family), nil, target)
		if dialErr != nil {
			return fmt.Errorf("open %s source socket: %w", spec.kind, dialErr)
		}
		if _, writeErr := connection.Write(spec.packet); writeErr != nil {
			_ = connection.Close()
			return fmt.Errorf("send %s packet: %w", spec.kind, writeErr)
		}
		sent = append(sent, sentPacket{kind: spec.kind, conn: connection})
		uniqueSources[connection.LocalAddr().String()] = struct{}{}
	}
	if len(uniqueSources) != len(sent) {
		return fmt.Errorf("attack source tuples = %d, want %d distinct", len(uniqueSources), len(sent))
	}

	deadline := time.Now().Add(responseWait)
	results := make(chan responseResult, len(sent))
	for _, current := range sent {
		go func(current sentPacket) {
			if err := current.conn.SetReadDeadline(deadline); err != nil {
				results <- responseResult{kind: current.kind, err: err}
				return
			}
			buffer := make([]byte, opts.maxUDPPayload+1)
			n, _, readErr := current.conn.ReadFromUDP(buffer)
			if n > 0 {
				results <- responseResult{kind: current.kind, response: true}
				return
			}
			var networkError net.Error
			if readErr != nil && errors.As(readErr, &networkError) && networkError.Timeout() {
				results <- responseResult{kind: current.kind}
				return
			}
			results <- responseResult{kind: current.kind, err: readErr}
		}(current)
	}
	counts := make(map[string]int64)
	responses := make(map[string]int64)
	for range sent {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-results:
			if result.err != nil {
				return fmt.Errorf("observe %s response: %w", result.kind, result.err)
			}
			counts[result.kind]++
			if result.response {
				responses[result.kind]++
			}
		}
	}
	for _, kind := range attackKinds() {
		if responses[kind] != 0 {
			return fmt.Errorf("%s produced %d response packets", kind, responses[kind])
		}
		if err := log.write("attack_class_complete", kind, map[string]int64{"packets": counts[kind], "responses": responses[kind]}); err != nil {
			return err
		}
	}
	if err := log.write("attack_batch_complete", "attack", map[string]int64{"packets": int64(len(sent)), "unique_sources": int64(len(uniqueSources)), "responses": 0}); err != nil {
		return err
	}
	return runProcessingBarrier(ctx, opts, target, log)
}

func runProcessingBarrier(ctx context.Context, opts options, target *net.UDPAddr, log *eventLog) error {
	connection, err := net.DialUDP(fmt.Sprintf("udp%d", opts.family), nil, target)
	if err != nil {
		return fmt.Errorf("open processing barrier socket: %w", err)
	}
	defer connection.Close()
	barrierID := sessionID(opts.highSources + 1000)
	packet, err := encodeHello(barrierID, []byte(integrationKey), opts.maxUDPPayload)
	if err != nil {
		return fmt.Errorf("encode processing barrier: %w", err)
	}
	deadline := time.Now().Add(barrierResponseWait)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := connection.SetDeadline(deadline); err != nil {
		return fmt.Errorf("set processing barrier deadline: %w", err)
	}
	if _, err := connection.Write(packet); err != nil {
		return fmt.Errorf("send processing barrier: %w", err)
	}
	response := make([]byte, opts.maxUDPPayload+1)
	length, err := connection.Read(response)
	if err != nil {
		return fmt.Errorf("read processing barrier response: %w", err)
	}
	message, err := wire.DecodeAuthenticated(response[:length], []byte(integrationKey), opts.maxUDPPayload)
	if err != nil {
		return fmt.Errorf("authenticate processing barrier response: %w", err)
	}
	if message.Header.Type != wire.TypeHelloAck || message.Header.SessionID != barrierID {
		return errors.New("processing barrier returned an unexpected packet")
	}
	return log.write("processing_barrier_complete", "attack", map[string]int64{"responses": 1})
}

func buildAttackPackets(maxUDPPayload, highSources int) ([]packetSpec, error) {
	wrongHello, err := encodeHello(sessionID(1), []byte(wrongIntegrationKey), maxUDPPayload)
	if err != nil {
		return nil, err
	}
	tampered, err := encodeHello(sessionID(2), []byte(integrationKey), maxUDPPayload)
	if err != nil {
		return nil, err
	}
	tampered[len(tampered)-1] ^= 0x01
	unknownData, err := wire.NewDataShard(sessionID(3), 1, 3, 2, 0, 1, []byte{0x5a})
	if err != nil {
		return nil, err
	}
	forged, err := wire.AppendAuthenticated(nil, unknownData, []byte(integrationKey), maxUDPPayload)
	if err != nil {
		return nil, err
	}
	specs := []packetSpec{
		{kind: "wrong-psk", packet: wrongHello},
		{kind: "single-bit-tamper", packet: tampered},
		{kind: "forged-session-id", packet: forged},
		{kind: "malformed", packet: []byte("not-an-mpudp-packet")},
		{kind: "oversized", packet: make([]byte, maxUDPPayload+1)},
	}
	for index := 0; index < highSources; index++ {
		packet, encodeErr := encodeHello(sessionID(index+10), []byte(wrongIntegrationKey), maxUDPPayload)
		if encodeErr != nil {
			return nil, encodeErr
		}
		specs = append(specs, packetSpec{kind: "high-cardinality", packet: packet})
	}
	return specs, nil
}

func attackKinds() []string {
	return []string{"wrong-psk", "single-bit-tamper", "forged-session-id", "malformed", "oversized", "high-cardinality"}
}

func encodeHello(id wire.SessionID, key []byte, maxUDPPayload int) ([]byte, error) {
	message, err := wire.NewHello(id, 3, 2, uint16(maxUDPPayload))
	if err != nil {
		return nil, err
	}
	return wire.AppendAuthenticated(nil, message, key, maxUDPPayload)
}

func sessionID(value int) wire.SessionID {
	var id wire.SessionID
	for index := range id {
		id[index] = byte((value*29 + index*17) % 251)
	}
	if id == (wire.SessionID{}) {
		id[len(id)-1] = 1
	}
	return id
}

func readExpected(ctx context.Context, current mpudp.Session, want []byte) error {
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
			return got.err
		}
		if !bytes.Equal(got.body, want) {
			return fmt.Errorf("Datagram metadata mismatch: bytes=%d", len(got.body))
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
	return &eventLog{file: file, encoder: json.NewEncoder(file), start: time.Now(), runID: opts.runID, role: opts.role}, nil
}

func (l *eventLog) write(name, phase string, metrics map[string]int64) error {
	record := map[string]any{
		"elapsed_ms": time.Since(l.start).Milliseconds(),
		"event":      name,
		"phase":      phase,
		"role":       l.role,
		"run_id":     l.runID,
	}
	for name, value := range metrics {
		record[name] = value
	}
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

func forbiddenDiagnosticText(contents string) bool {
	lower := strings.ToLower(contents)
	for _, forbidden := range []string{integrationKey, wrongIntegrationKey, `"payload"`, "authentication_tag", `"session_id"`} {
		if strings.Contains(lower, strings.ToLower(forbidden)) {
			return true
		}
	}
	return false
}
