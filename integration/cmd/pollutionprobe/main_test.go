package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/wire"
)

func TestBuildAttackPacketsCoversRequiredClasses(t *testing.T) {
	const highSources = 64
	packets, err := buildAttackPackets(1200, highSources)
	if err != nil {
		t.Fatal(err)
	}
	if len(packets) != highSources+5 {
		t.Fatalf("packet count = %d, want %d", len(packets), highSources+5)
	}
	counts := make(map[string]int)
	for _, packet := range packets {
		counts[packet.kind]++
	}
	for _, kind := range attackKinds() {
		want := 1
		if kind == "high-cardinality" {
			want = highSources
		}
		if counts[kind] != want {
			t.Errorf("%s count = %d, want %d", kind, counts[kind], want)
		}
	}
	if len(packets[4].packet) != 1201 {
		t.Errorf("oversized packet length = %d, want 1201", len(packets[4].packet))
	}
	if _, err := wire.DecodeAuthenticated(packets[0].packet, []byte(integrationKey), 1200); err == nil {
		t.Error("wrong-PSK packet authenticated with the listener key")
	}
	if _, err := wire.DecodeAuthenticated(packets[1].packet, []byte(integrationKey), 1200); err == nil {
		t.Error("single-bit-tampered packet authenticated")
	}
	message, err := wire.DecodeAuthenticated(packets[2].packet, []byte(integrationKey), 1200)
	if err != nil {
		t.Fatalf("forged SessionID packet must be correctly authenticated: %v", err)
	}
	if message.Header.Type != wire.TypeDataShard {
		t.Fatalf("forged SessionID packet type = %v, want DATA_SHARD", message.Header.Type)
	}
	if bytes.Equal(packets[1].packet, packets[0].packet) {
		t.Error("tampered and wrong-key probes unexpectedly match")
	}
}

func TestEventLogExcludesSensitiveFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.ndjson")
	opts := options{role: "attacker", runID: "pollution-unit", eventsPath: path}
	log, err := openEventLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.write("attack_class_complete", "wrong-psk", map[string]int64{"packets": 1, "responses": 0}); err != nil {
		t.Fatal(err)
	}
	if err := log.close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if forbiddenDiagnosticText(string(contents)) {
		t.Fatalf("event output contains forbidden diagnostic text: %s", contents)
	}
}

func TestValidateOptionsRequiresBoundedAttackAndLifecycleMarkers(t *testing.T) {
	base := options{
		role: "attacker", family: 4, target: "127.0.0.1:9000", maxUDPPayload: 1200,
		highSources: 64, bodyBytes: 1, replyBytes: 1, timeout: time.Second,
		runID: "pollution-unit", eventsPath: "events",
	}
	if err := validateOptions(base); err != nil {
		t.Fatalf("valid attacker options rejected: %v", err)
	}
	base.highSources = 63
	if err := validateOptions(base); err == nil {
		t.Error("undersized high-cardinality attack accepted")
	}
	listener := options{
		role: "listener", family: 4, listen: "127.0.0.1:9000", maxUDPPayload: 1200,
		highSources: 64, bodyBytes: 1, replyBytes: 1, timeout: time.Second,
		runID: "pollution-unit", eventsPath: "events", readyPath: "ready",
	}
	if err := validateOptions(listener); err == nil {
		t.Error("listener without full lifecycle markers accepted")
	}
}
