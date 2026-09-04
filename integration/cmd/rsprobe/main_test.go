package main

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFiveCarrierLossCoversEveryTwoShardCombinationAndThreeLoss(t *testing.T) {
	events, err := runScenario(context.Background(), scenarioFiveCarrierLoss, "five-loss-test")
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[[2]int]bool)
	for _, current := range events {
		if current.Event != "two_loss_recovered" {
			continue
		}
		if len(current.DroppedShards) != 2 || current.Attempts != 5 || current.Deliveries != 1 ||
			!equalInts(current.PathShardCounts, []int{1, 1, 1, 1, 1}) {
			t.Fatalf("invalid two-loss evidence: %+v", current)
		}
		seen[[2]int{current.DroppedShards[0], current.DroppedShards[1]}] = true
	}
	if len(seen) != 10 {
		t.Fatalf("covered %d two-shard combinations, want 10: %v", len(seen), seen)
	}
	threeLoss := requireEvent(t, events, "three_loss_expired")
	if !equalInts(threeLoss.DroppedShards, []int{0, 1, 2}) || threeLoss.ExpiredBlocks != 1 || threeLoss.Deliveries != 0 {
		t.Fatalf("invalid three-loss evidence: %+v", threeLoss)
	}
	complete := requireEvent(t, events, "scenario_complete")
	if complete.Combination != 10 || complete.Blocks != 11 || complete.Attempts != 55 || complete.Deliveries != 10 ||
		!complete.NoDataResponses || !complete.NoRetransmits {
		t.Fatalf("invalid completion evidence: %+v", complete)
	}
}

func TestTwoCarrierRotationAndLossBoundaries(t *testing.T) {
	events, err := runScenario(context.Background(), scenarioTwoCarrier, "rotation-test")
	if err != nil {
		t.Fatal(err)
	}
	var rotations []scenarioEvent
	for _, current := range events {
		if current.Event == "rotation_observed" {
			rotations = append(rotations, current)
		}
	}
	if len(rotations) != 4 {
		t.Fatalf("rotation events=%d, want 4", len(rotations))
	}
	for index, current := range rotations {
		want := []int{3, 2}
		if index%2 == 1 {
			want = []int{2, 3}
		}
		if current.Block != index+1 || !equalInts(current.PathShardCounts, want) || current.Deliveries != 1 {
			t.Fatalf("rotation %d evidence=%+v, want counts %v", index, current, want)
		}
	}
	twoLoss := requireEvent(t, events, "two_shard_carrier_lost_recovered")
	if twoLoss.PathShardCounts[twoLoss.DroppedPaths[0]] != 2 || twoLoss.Deliveries != 1 {
		t.Fatalf("invalid two-shard Carrier boundary: %+v", twoLoss)
	}
	threeLoss := requireEvent(t, events, "three_shard_carrier_lost_expired")
	if threeLoss.PathShardCounts[threeLoss.DroppedPaths[0]] != 3 || threeLoss.Deliveries != 0 || threeLoss.ExpiredBlocks != 1 {
		t.Fatalf("invalid three-shard Carrier boundary: %+v", threeLoss)
	}
	recovered := requireEvent(t, events, "post_loss_session_recovered")
	if recovered.Block != 7 || recovered.Deliveries != 1 {
		t.Fatalf("invalid post-loss recovery: %+v", recovered)
	}
}

func TestSlowPathRecoversAtThirdArrivalBeforeLateShard(t *testing.T) {
	events, err := runScenario(context.Background(), scenarioSlowPath, "slow-path-test")
	if err != nil {
		t.Fatal(err)
	}
	var arrivals []scenarioEvent
	for _, current := range events {
		if current.Event == "shard_arrived" {
			arrivals = append(arrivals, current)
		}
	}
	if len(arrivals) != 4 {
		t.Fatalf("arrival events=%d, want 4", len(arrivals))
	}
	wantTimes := []int64{10, 20, 30, 500}
	for index, current := range arrivals {
		if current.ShardIndex == nil || *current.ShardIndex != index || current.PathIndex == nil || *current.PathIndex != index ||
			current.ArrivalMS != wantTimes[index] {
			t.Fatalf("arrival %d=%+v, want shard/time %d/%d", index, current, index, wantTimes[index])
		}
	}
	recovery := requireEvent(t, events, "datagram_recovered")
	if recovery.ArrivalMS != 30 || recovery.Deliveries != 1 ||
		recovery.EventOrder <= arrivals[2].EventOrder || recovery.EventOrder >= arrivals[3].EventOrder {
		t.Fatalf("recovery was not ordered between the third and late shard: recovery=%+v arrivals=%+v", recovery, arrivals)
	}
	complete := requireEvent(t, events, "scenario_complete")
	if !equalInts(complete.DroppedShards, []int{4}) || complete.Attempts != 5 || complete.Deliveries != 1 {
		t.Fatalf("invalid slow-path completion evidence: %+v", complete)
	}
}

func TestScenarioEventsAreMetadataOnlyAndSymlinkSafe(t *testing.T) {
	events, err := runScenario(context.Background(), scenarioSlowPath, "event-output-test")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "events.ndjson")
	if err := writeEvents(path, events); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("event mode=%#o, want 0600", info.Mode().Perm())
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(string(contents))
	for _, forbidden := range []string{scenarioKey, `"payload"`, `"session_id"`, "authentication_tag"} {
		if strings.Contains(lower, strings.ToLower(forbidden)) {
			t.Fatalf("event output contains forbidden text %q", forbidden)
		}
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(file)
	decoded := 0
	for scanner.Scan() {
		var current scenarioEvent
		if err := json.Unmarshal(scanner.Bytes(), &current); err != nil {
			t.Fatal(err)
		}
		decoded++
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if decoded != len(events) {
		t.Fatalf("decoded events=%d, want %d", decoded, len(events))
	}

	target := filepath.Join(directory, "target")
	if err := os.WriteFile(target, []byte("preserve\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(directory, "alias")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	if err := writeEvents(alias, events); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink output error=%v", err)
	}
	preserved, err := os.ReadFile(target)
	if err != nil || string(preserved) != "preserve\n" {
		t.Fatalf("symlink target changed: contents=%q err=%v", preserved, err)
	}
}

func TestOptionsRejectUnknownScenarioAndUnboundedTimeout(t *testing.T) {
	valid := options{
		scenario: scenarioFiveCarrierLoss, runID: "valid-run", events: "events",
		ready: "ready", continuePath: "continue", timeout: time.Second,
	}
	if err := validateOptions(valid); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.scenario = "not-a-scenario"
	if err := validateOptions(invalid); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unknown scenario error=%v", err)
	}
	invalid = valid
	invalid.timeout = 0
	if err := validateOptions(invalid); err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("zero timeout error=%v", err)
	}
	for _, events := range []string{valid.ready, valid.continuePath} {
		invalid = valid
		invalid.events = events
		if err := validateOptions(invalid); err == nil || !strings.Contains(err.Error(), "distinct") {
			t.Fatalf("colliding event path %q error=%v", events, err)
		}
	}
}

func requireEvent(t *testing.T, events []scenarioEvent, name string) scenarioEvent {
	t.Helper()
	for _, current := range events {
		if current.Event == name {
			return current
		}
	}
	t.Fatalf("missing event %q in %+v", name, events)
	return scenarioEvent{}
}
