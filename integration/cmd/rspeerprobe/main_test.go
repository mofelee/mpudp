package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestScenarioPlansFixCarrierCountsAndDeliveryBoundaries(t *testing.T) {
	tests := []struct {
		name          string
		carriers      int
		steps         int
		nondeliveries []int
	}{
		{name: scenarioFiveCarrier, carriers: 5, steps: 13, nondeliveries: []int{11, 12}},
		{name: scenarioTwoCarrier, carriers: 2, steps: 7, nondeliveries: []int{6}},
		{name: scenarioSlowPath, carriers: 5, steps: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			steps, carriers, err := planFor(test.name)
			if err != nil {
				t.Fatal(err)
			}
			if carriers != test.carriers || len(steps) != test.steps {
				t.Fatalf("plan carriers/steps=%d/%d, want %d/%d", carriers, len(steps), test.carriers, test.steps)
			}
			var nondeliveries []int
			seenSizes := make(map[int]bool)
			for index, step := range steps {
				if step.bytes <= 0 || seenSizes[step.bytes] {
					t.Fatalf("step %d has invalid or duplicate size %d", index+1, step.bytes)
				}
				seenSizes[step.bytes] = true
				if !step.deliver {
					nondeliveries = append(nondeliveries, index+1)
				}
			}
			if len(nondeliveries) != len(test.nondeliveries) {
				t.Fatalf("non-delivery steps=%v, want %v", nondeliveries, test.nondeliveries)
			}
			for index := range nondeliveries {
				if nondeliveries[index] != test.nondeliveries[index] {
					t.Fatalf("non-delivery steps=%v, want %v", nondeliveries, test.nondeliveries)
				}
			}
		})
	}
}

func TestOptionsRequireRoleSpecificRealCarrierSurface(t *testing.T) {
	directory := t.TempDir()
	base := options{
		role: "initiator", scenario: scenarioFiveCarrier, family: 4,
		carriers: "one,two,three,four,five", timeout: 5 * time.Second,
		runID: "rs-public-test", eventsPath: filepath.Join(directory, "events"), syncDir: directory,
	}
	if err := validateOptions(base); err != nil {
		t.Fatal(err)
	}
	invalid := base
	invalid.carriers = "one,two"
	if err := validateOptions(invalid); err == nil || !strings.Contains(err.Error(), "exactly 5") {
		t.Fatalf("wrong Carrier count error=%v", err)
	}
	invalid = base
	invalid.scenario = "not-a-scenario"
	if err := validateOptions(invalid); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unknown scenario error=%v", err)
	}
	invalid = base
	invalid.timeout = quietPeriod
	if err := validateOptions(invalid); err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("short timeout error=%v", err)
	}
	invalid = base
	invalid.syncDir = filepath.Join(directory, "missing")
	if err := validateOptions(invalid); err == nil || !strings.Contains(err.Error(), "sync-dir") {
		t.Fatalf("missing sync directory error=%v", err)
	}
	listener := base
	listener.role = "listener"
	listener.carriers = ""
	listener.listen = "0.0.0.0:9000"
	if err := validateOptions(listener); err != nil {
		t.Fatal(err)
	}
}

func TestMarkersAndEventsArePrivateMetadataOnly(t *testing.T) {
	directory := t.TempDir()
	opts := options{
		role: "listener", scenario: scenarioSlowPath, family: 4, listen: "0.0.0.0:9000",
		timeout: 5 * time.Second, runID: "metadata-test",
		eventsPath: filepath.Join(directory, "events.ndjson"), syncDir: directory,
	}
	markerPath := stepMarker(opts, 3, "received")
	if err := createMarker(markerPath); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(markerPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("marker mode=%#o, want 0600", info.Mode().Perm())
	}
	if err := createMarker(markerPath); err == nil {
		t.Fatal("duplicate marker creation unexpectedly succeeded")
	}

	log, err := openEventLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	body := makeDatagram(510, 0x51)
	if err := log.write("datagram_received", 1, body); err != nil {
		t.Fatal(err)
	}
	if err := log.close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(opts.eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(string(contents))
	for _, forbidden := range []string{integrationKey, `"payload"`, `"psk"`, `"session_id"`, `"authentication_tag"`} {
		if strings.Contains(lower, strings.ToLower(forbidden)) {
			t.Fatalf("event output contains forbidden text %q", forbidden)
		}
	}
	var decoded event
	if err := json.Unmarshal(contents, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Event != "datagram_received" || decoded.Step != 1 || decoded.Bytes == nil || *decoded.Bytes != len(body) ||
		len(decoded.Digest) != 12 || decoded.ObservedNS <= 0 {
		t.Fatalf("unexpected metadata event: %+v", decoded)
	}

	target := filepath.Join(directory, "target")
	if err := os.WriteFile(target, []byte("preserve\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(directory, "alias")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	opts.eventsPath = alias
	if _, err := openEventLog(opts); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink event output error=%v", err)
	}
	preserved, err := os.ReadFile(target)
	if err != nil || string(preserved) != "preserve\n" {
		t.Fatalf("symlink target changed: contents=%q err=%v", preserved, err)
	}
}
