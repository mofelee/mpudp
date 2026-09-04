package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHandshakeCancellationClosesPeer(t *testing.T) {
	address := reserveUDPAddress(t)
	directory := t.TempDir()
	events := filepath.Join(directory, "events.ndjson")
	ready := filepath.Join(directory, "ready")
	phaseReady := filepath.Join(directory, "phase-ready")
	opts := options{
		role: "listener", phase: "handshake", action: "signal", family: 4,
		listen: address, bodyBytes: 128, timeout: 3 * time.Second,
		runID: "shutdown-unit", eventsPath: events, readyPath: ready, phaseReadyPath: phaseReady,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, opts) }()
	waitForTestFile(t, phaseReady)
	started := time.Now()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("signal cancellation run error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > maxPeerCloseDuration {
		t.Fatalf("canceled helper took %s to close", elapsed)
	}
	contents, err := os.ReadFile(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{`"event":"shutdown_trigger"`, `"trigger":"signal"`, `"event":"peer_close_complete"`} {
		if !strings.Contains(string(contents), required) {
			t.Errorf("event output is missing %s: %s", required, contents)
		}
	}
	for _, forbidden := range []string{integrationKey, `"payload"`, `"session_id"`, "authentication_tag"} {
		if strings.Contains(strings.ToLower(string(contents)), strings.ToLower(forbidden)) {
			t.Errorf("event output contains forbidden diagnostic text %q", forbidden)
		}
	}
}

func TestValidateOptionsPinsShutdownMechanismByPhase(t *testing.T) {
	base := options{
		role: "initiator", phase: "decode-incomplete", action: "signal", family: 4,
		carriers: "127.0.0.1:9000", bodyBytes: 128, timeout: time.Second,
		runID: "shutdown-unit", eventsPath: "events", phaseReadyPath: "phase", bootstrapPath: "bootstrap",
		sendPath: "send", sentPath: "sent",
	}
	if err := validateOptions(base); err != nil {
		t.Fatalf("valid decode-incomplete options rejected: %v", err)
	}
	base.action = "close"
	base.closePath = "close"
	if err := validateOptions(base); err == nil {
		t.Error("decode-incomplete accepted API close instead of required signal")
	}
	base.phase = "active-transfer"
	if err := validateOptions(base); err != nil {
		t.Fatalf("valid active-transfer API close rejected: %v", err)
	}
	base.sendPath = ""
	if err := validateOptions(base); err == nil {
		t.Error("active-transfer without complete send markers accepted")
	}
	base.phase = "network-fault"
	base.sendPath = "send"
	if err := validateOptions(base); err != nil {
		t.Fatalf("valid network-fault transfer options rejected: %v", err)
	}
}

func reserveUDPAddress(t *testing.T) string {
	t.Helper()
	connection, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := connection.LocalAddr().String()
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitForTestFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(time.Millisecond)
	}
}
