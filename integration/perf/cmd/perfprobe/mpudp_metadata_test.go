package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/mofelee/mpudp/config"
)

func metadataConfig(t *testing.T, extra string) config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(`psk: private-metadata-psk
carriers: ["192.0.2.41:19000", "192.0.2.42:19000"]
listen: "192.0.2.43:19000"
fec: {data_shards: 3, parity_shards: 2}
` + extra))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestMPUDPConfigMetadataProfiles(t *testing.T) {
	for _, tc := range []struct {
		name, yaml, profile, wire string
		aggregation               map[string]any
	}{
		{"legacy", "", "v1", "v1", map[string]any{"enabled": false}},
		{"explicit v1", "protocol: datagram\nwire: {version: v1}\n", "v1", "v1", map[string]any{"enabled": false}},
		{"v2 default", "wire: {version: v2}\n", "v2", "v2", map[string]any{
			"enabled": false, "max_delay_ns": int64(250000), "max_records": int64(32),
			"max_queued_datagrams": int64(256), "max_queued_bytes": int64(1048576), "max_group_bytes": int64(1048576),
		}},
		{"v2 aggregation", `wire: {version: v2}
aggregation:
  enabled: true
  max_delay: 700us
  max_records: 48
  max_queued_datagrams: 128
  max_queued_bytes: 262144
  max_group_bytes: 65536
`, "v2-aggregation", "v2", map[string]any{
			"enabled": true, "max_delay_ns": int64(700000), "max_records": int64(48),
			"max_queued_datagrams": int64(128), "max_queued_bytes": int64(262144), "max_group_bytes": int64(65536),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := metadataConfig(t, tc.yaml)
			metadata := mpudpConfigMetadata(cfg)
			if metadata["mpudp_profile"] != tc.profile || metadata["wire_version"] != tc.wire || metadata["protocol"] != "datagram" {
				t.Fatalf("incorrect profile metadata: %v", metadata)
			}
			if !reflect.DeepEqual(metadata["aggregation"], tc.aggregation) {
				t.Fatalf("aggregation = %#v, want %#v", metadata["aggregation"], tc.aggregation)
			}
			if !reflect.DeepEqual(metadata["repair"], map[string]any{"enabled": false}) {
				t.Fatalf("incorrect repair metadata: %v", metadata["repair"])
			}
			for key, want := range map[string]any{
				"fec": cfg.FEC, "transport": cfg.Transport, "limits": cfg.Limits,
				"timers": cfg.Timers, "configured_carriers": 2,
			} {
				if !reflect.DeepEqual(metadata[key], want) {
					t.Fatalf("legacy field %s changed", key)
				}
			}
			wantScheduler := map[string]any{
				"outbound_path_rates_bps": map[string]int64{}, "inbound_path_rates_bps": map[string]int64{},
			}
			if tc.wire == "v2" {
				wantScheduler["default_path_rate_bps"] = int64(100000000)
			}
			if !reflect.DeepEqual(metadata["scheduler"], wantScheduler) {
				t.Fatalf("scheduler = %#v, want %#v", metadata["scheduler"], wantScheduler)
			}
			if !reflect.DeepEqual(metadata["udp_caps"], map[string]any{"send_hard_cap": 1200, "receive_hard_cap": 1200}) {
				t.Fatalf("incorrect default UDP caps: %v", metadata["udp_caps"])
			}
			encoded, err := json.Marshal(metadata)
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{"private-metadata-psk", "192.0.2.41", "192.0.2.42", "192.0.2.43", "PSK", "Listen", "Carriers"} {
				if strings.Contains(string(encoded), secret) {
					t.Fatalf("metadata contains private field or value %q", secret)
				}
			}
		})
	}
}

func TestMPUDPConfigMetadataDirectionalSettings(t *testing.T) {
	cfg := metadataConfig(t, `wire: {version: v2}
transport: {max_udp_payload: 1472, max_receive_udp_payload: 1000}
scheduler:
  outbound_path_rates_bps: {1: 50000000, 2: 70000000}
  inbound_path_rates_bps: {2: 90000000}
`)
	metadata := mpudpConfigMetadata(cfg)
	if !reflect.DeepEqual(metadata["udp_caps"], map[string]any{"send_hard_cap": 1472, "receive_hard_cap": 1000}) {
		t.Fatalf("directional UDP caps lost: %v", metadata["udp_caps"])
	}
	want := map[string]any{
		"outbound_path_rates_bps": map[string]int64{"1": 50000000, "2": 70000000},
		"inbound_path_rates_bps":  map[string]int64{"2": 90000000},
		"default_path_rate_bps":   int64(100000000),
	}
	if !reflect.DeepEqual(metadata["scheduler"], want) {
		t.Fatalf("directional rates lost: %#v", metadata["scheduler"])
	}
	metadata["scheduler"].(map[string]any)["outbound_path_rates_bps"].(map[string]int64)["1"] = 1
	if !reflect.DeepEqual(mpudpConfigMetadata(cfg)["scheduler"], want) {
		t.Fatal("metadata rates alias the config")
	}
	legacy := mpudpConfigMetadata(metadataConfig(t, "transport: {max_udp_payload: 1400}\n"))
	if !reflect.DeepEqual(legacy["udp_caps"], map[string]any{"send_hard_cap": 1400, "receive_hard_cap": 1400}) {
		t.Fatalf("legacy receive cap must follow send cap: %v", legacy["udp_caps"])
	}
}
