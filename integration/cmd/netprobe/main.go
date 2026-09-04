// netprobe is a test-only UDP endpoint for the Linux namespace harness. It
// logs packet metadata and digests, never packet contents.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"time"
)

var runIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)

type options struct {
	mode                 string
	network              string
	listen               string
	targets              string
	count                int
	timeout              time.Duration
	runID                string
	eventsPath           string
	readyPath            string
	requireUniqueRemotes bool
	localPortBase        int
}

type event struct {
	RunID     string `json:"run_id"`
	Role      string `json:"role"`
	Event     string `json:"event"`
	Path      int    `json:"path,omitempty"`
	Network   string `json:"network"`
	Local     string `json:"local"`
	Remote    string `json:"remote"`
	Bytes     int    `json:"bytes"`
	Digest    string `json:"sha256_prefix"`
	ElapsedMS int64  `json:"elapsed_ms"`
}

type eventLog struct {
	file    *os.File
	encoder *json.Encoder
}

func main() {
	var config options
	flag.StringVar(&config.mode, "mode", "", "server or client")
	flag.StringVar(&config.network, "network", "udp4", "udp4 or udp6")
	flag.StringVar(&config.listen, "listen", "", "server listen address")
	flag.StringVar(&config.targets, "targets", "", "comma-separated client targets")
	flag.IntVar(&config.count, "count", 0, "expected server packet count")
	flag.DurationVar(&config.timeout, "timeout", 10*time.Second, "per-operation timeout")
	flag.StringVar(&config.runID, "run-id", "", "integration run identifier")
	flag.StringVar(&config.eventsPath, "events", "", "NDJSON event output")
	flag.StringVar(&config.readyPath, "ready-file", "", "server readiness marker")
	flag.BoolVar(&config.requireUniqueRemotes, "require-unique-remotes", false, "require every received remote endpoint to differ")
	flag.IntVar(&config.localPortBase, "local-port-base", 0, "optional first client source port")
	flag.Parse()

	if err := run(config); err != nil {
		fmt.Fprintf(os.Stderr, "netprobe: %v\n", err)
		os.Exit(1)
	}
}

func run(config options) error {
	if !runIDPattern.MatchString(config.runID) {
		return errors.New("invalid run ID")
	}
	if config.network != "udp4" && config.network != "udp6" {
		return errors.New("network must be udp4 or udp6")
	}
	if config.timeout <= 0 || config.timeout > time.Minute {
		return errors.New("timeout must be within (0,1m]")
	}
	log, err := openEventLog(config.eventsPath)
	if err != nil {
		return err
	}
	defer log.close()

	switch config.mode {
	case "server":
		return runServer(config, log)
	case "client":
		return runClient(config, log)
	default:
		return errors.New("mode must be server or client")
	}
}

func openEventLog(path string) (*eventLog, error) {
	if path == "" {
		return nil, errors.New("events path is required")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open event log: %w", err)
	}
	return &eventLog{file: file, encoder: json.NewEncoder(file)}, nil
}

func (l *eventLog) write(value event) error {
	if err := l.encoder.Encode(value); err != nil {
		return fmt.Errorf("write event metadata: %w", err)
	}
	return nil
}

func (l *eventLog) close() {
	if l != nil && l.file != nil {
		_ = l.file.Close()
	}
}

func runServer(config options, log *eventLog) error {
	if config.listen == "" || config.count < 1 || config.count > 256 {
		return errors.New("server requires listen and count within 1..256")
	}
	listenAddress, err := net.ResolveUDPAddr(config.network, config.listen)
	if err != nil {
		return fmt.Errorf("resolve listen address: %w", err)
	}
	conn, err := net.ListenUDP(config.network, listenAddress)
	if err != nil {
		return fmt.Errorf("listen UDP: %w", err)
	}
	defer conn.Close()
	if config.readyPath != "" {
		ready, openErr := os.OpenFile(config.readyPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if openErr != nil {
			return fmt.Errorf("create ready marker: %w", openErr)
		}
		if _, writeErr := fmt.Fprintf(ready, "%s\n", conn.LocalAddr()); writeErr != nil {
			_ = ready.Close()
			return fmt.Errorf("write ready marker: %w", writeErr)
		}
		if closeErr := ready.Close(); closeErr != nil {
			return fmt.Errorf("close ready marker: %w", closeErr)
		}
	}

	started := time.Now()
	remotes := make(map[string]struct{}, config.count)
	buffer := make([]byte, 65507)
	for index := 0; index < config.count; index++ {
		if err := conn.SetReadDeadline(time.Now().Add(config.timeout)); err != nil {
			return fmt.Errorf("set server read deadline: %w", err)
		}
		n, remote, readErr := conn.ReadFromUDP(buffer)
		if readErr != nil {
			return fmt.Errorf("server receive packet %d: %w", index+1, readErr)
		}
		remoteKey := remote.String()
		if config.requireUniqueRemotes {
			if _, duplicate := remotes[remoteKey]; duplicate {
				return fmt.Errorf("server packet %d reused remote endpoint %s", index+1, remoteKey)
			}
			remotes[remoteKey] = struct{}{}
		}
		payload := buffer[:n]
		if err := log.write(packetEvent(config, "server", index+1, conn.LocalAddr(), remote, payload, started)); err != nil {
			return err
		}
		if err := conn.SetWriteDeadline(time.Now().Add(config.timeout)); err != nil {
			return fmt.Errorf("set server write deadline: %w", err)
		}
		written, writeErr := conn.WriteToUDP(payload, remote)
		if writeErr != nil {
			return fmt.Errorf("server reply packet %d: %w", index+1, writeErr)
		}
		if written != n {
			return fmt.Errorf("server short reply packet %d: wrote %d of %d bytes", index+1, written, n)
		}
	}
	return nil
}

func runClient(config options, log *eventLog) error {
	if config.targets == "" {
		return errors.New("client targets are required")
	}
	targetStrings := strings.Split(config.targets, ",")
	if len(targetStrings) < 1 || len(targetStrings) > 256 {
		return errors.New("client target count must be within 1..256")
	}
	if config.localPortBase != 0 && (config.localPortBase < 1024 || config.localPortBase+len(targetStrings)-1 > 65535) {
		return errors.New("client local port range must be within 1024..65535")
	}
	connections := make([]*net.UDPConn, 0, len(targetStrings))
	defer func() {
		for _, conn := range connections {
			_ = conn.Close()
		}
	}()
	ports := make(map[int]struct{}, len(targetStrings))
	for index, target := range targetStrings {
		remote, err := net.ResolveUDPAddr(config.network, target)
		if err != nil {
			return fmt.Errorf("resolve target %d: %w", index+1, err)
		}
		var bindAddress *net.UDPAddr
		if config.localPortBase != 0 {
			bindAddress = &net.UDPAddr{Port: config.localPortBase + index}
		}
		conn, err := net.DialUDP(config.network, bindAddress, remote)
		if err != nil {
			return fmt.Errorf("dial target %d: %w", index+1, err)
		}
		local := conn.LocalAddr().(*net.UDPAddr)
		if _, duplicate := ports[local.Port]; duplicate {
			_ = conn.Close()
			return fmt.Errorf("target %d reused local UDP port %d", index+1, local.Port)
		}
		ports[local.Port] = struct{}{}
		connections = append(connections, conn)
	}

	started := time.Now()
	for index, conn := range connections {
		payload := []byte(fmt.Sprintf("mpudp-it/%s/path/%d", config.runID, index+1))
		if err := conn.SetDeadline(time.Now().Add(config.timeout)); err != nil {
			return fmt.Errorf("set client deadline for path %d: %w", index+1, err)
		}
		written, err := conn.Write(payload)
		if err != nil {
			return fmt.Errorf("client send path %d: %w", index+1, err)
		}
		if written != len(payload) {
			return fmt.Errorf("client short send path %d: wrote %d of %d bytes", index+1, written, len(payload))
		}
		buffer := make([]byte, len(payload)+1)
		n, err := conn.Read(buffer)
		if err != nil {
			return fmt.Errorf("client receive path %d: %w", index+1, err)
		}
		if n != len(payload) || !equalBytes(buffer[:n], payload) {
			return fmt.Errorf("client reply mismatch on path %d: received %d bytes, expected %d", index+1, n, len(payload))
		}
		if err := log.write(packetEvent(config, "client", index+1, conn.LocalAddr(), conn.RemoteAddr(), payload, started)); err != nil {
			return err
		}
	}
	return nil
}

func packetEvent(config options, role string, path int, local, remote net.Addr, payload []byte, started time.Time) event {
	sum := sha256.Sum256(payload)
	return event{
		RunID:     config.runID,
		Role:      role,
		Event:     "udp_datagram",
		Path:      path,
		Network:   config.network,
		Local:     local.String(),
		Remote:    remote.String(),
		Bytes:     len(payload),
		Digest:    hex.EncodeToString(sum[:6]),
		ElapsedMS: time.Since(started).Milliseconds(),
	}
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var different byte
	for i := range left {
		different |= left[i] ^ right[i]
	}
	return different == 0
}
