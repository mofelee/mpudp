package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPublicPeerExchange(t *testing.T) {
	address := reserveUDPAddress(t)
	directory := t.TempDir()
	ready := filepath.Join(directory, "ready")
	replyComplete := filepath.Join(directory, "reply-complete")
	exit := filepath.Join(directory, "exit")
	listenerEvents := filepath.Join(directory, "listener.ndjson")
	initiatorEvents := filepath.Join(directory, "initiator.ndjson")
	base := options{
		flow: "unit-exchange", family: 4, maxUDPPayload: 1200,
		bodyBytes: 321, replyBytes: 257, endpointTTL: 2 * time.Minute,
		keepaliveInterval: 5 * time.Minute, timeout: 5 * time.Second,
		runID: "peerprobe-unit",
	}
	listener := base
	listener.role = "listener"
	listener.listen = address
	listener.eventsPath = listenerEvents
	listener.readyPath = ready
	listener.replyCompletePath = replyComplete
	listener.exitPath = exit
	initiator := base
	initiator.role = "initiator"
	initiator.carriers = address
	initiator.eventsPath = initiatorEvents
	initiator.replyCompletePath = replyComplete
	initiator.exitPath = exit

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	listenerResult := make(chan error, 1)
	go func() { listenerResult <- run(ctx, listener) }()
	waitForTestFile(t, ready)
	if err := run(ctx, initiator); err != nil {
		t.Fatalf("initiator run error = %v", err)
	}
	if err := <-listenerResult; err != nil {
		t.Fatalf("listener run error = %v", err)
	}

	for _, path := range []string{listenerEvents, initiatorEvents} {
		events := readEvents(t, path)
		if len(events) < 7 {
			t.Fatalf("%s has %d events, want at least 7", path, len(events))
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		lower := strings.ToLower(string(contents))
		for _, forbidden := range []string{"integration-test-key", `"payload"`, "authentication_tag", `"session_id"`} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("%s contains forbidden diagnostic text %q", path, forbidden)
			}
		}
	}
}

func TestEffectiveDatagramLimit(t *testing.T) {
	for budget, want := range map[int]int{1000: 2787, 1200: 3387, 1400: 3987} {
		if got := effectiveDatagramLimit(budget); got != want {
			t.Fatalf("effectiveDatagramLimit(%d) = %d, want %d", budget, got, want)
		}
	}
}

func TestValidateOptionsRejectsIncompleteRoles(t *testing.T) {
	base := options{
		role: "initiator", flow: "smoke", family: 4, carriers: "127.0.0.1:9000",
		maxUDPPayload: 1200, bodyBytes: 1, replyBytes: 1,
		endpointTTL: 2 * time.Minute, keepaliveInterval: 5 * time.Minute,
		timeout: time.Second, runID: "validation", eventsPath: "events",
	}
	base.replyCompletePath = "reply-complete"
	base.exitPath = "exit"
	base.phasePath = "phase"
	if err := validateOptions(base); err == nil || !strings.Contains(err.Error(), "supplied together") {
		t.Fatalf("unpaired phase validation error = %v", err)
	}
	base.phasePath = ""
	base.replyCompletePath = ""
	if err := validateOptions(base); err == nil || !strings.Contains(err.Error(), "reply-complete-file") {
		t.Fatalf("missing reply completion validation error = %v", err)
	}
	base.replyCompletePath = "reply-complete"
	base.readyPath = "not-allowed"
	if err := validateOptions(base); err == nil || !strings.Contains(err.Error(), "initiator requires") {
		t.Fatalf("initiator ready-file validation error = %v", err)
	}
}

func TestValidateOptionsRequiresCompleteOversizeGate(t *testing.T) {
	base := options{
		role: "initiator", flow: "mtu-safe", family: 4, carriers: "127.0.0.1:9000",
		maxUDPPayload: 520, bodyBytes: 1347, replyBytes: 1347, oversizeBytes: 1348,
		endpointTTL: 2 * time.Minute, keepaliveInterval: 5 * time.Minute,
		timeout: time.Second, runID: "validation", eventsPath: "events",
		replyCompletePath: "reply-complete", exitPath: "exit", phasePath: "main-ready", continuePath: "main-go",
	}
	if err := validateOptions(base); err == nil || !strings.Contains(err.Error(), "all oversize markers") {
		t.Fatalf("missing oversize gate validation error = %v", err)
	}
	base.oversizeReadyPath = "oversize-ready"
	base.oversizeContinuePath = "oversize-go"
	base.oversizeDonePath = "oversize-done"
	if err := validateOptions(base); err != nil {
		t.Fatalf("complete oversize gate rejected: %v", err)
	}
	base.oversizeBytes = 0
	if err := validateOptions(base); err == nil || !strings.Contains(err.Error(), "require oversize-bytes") {
		t.Fatalf("orphaned oversize markers validation error = %v", err)
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
	deadline := time.Now().Add(3 * time.Second)
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

func readEvents(t *testing.T, path string) []event {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var events []event
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var current event
		if err := json.Unmarshal(scanner.Bytes(), &current); err != nil {
			t.Fatalf("decode event: %v", err)
		}
		events = append(events, current)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return events
}
