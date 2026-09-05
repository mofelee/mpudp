package config_test

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mofelee/mpudp/config"
)

func v2Config(t *testing.T, extra string) config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(validYAML("wire: {version: v2}\n" + extra)))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestV2SharedDefaults(t *testing.T) {
	for _, protocol := range []config.Protocol{config.ProtocolDatagram, config.ProtocolKCP} {
		input := validYAML("wire: {version: v2}\n")
		if protocol == config.ProtocolKCP {
			input = kcpYAML("")
		}
		cfg, err := config.Parse([]byte(input))
		if err != nil {
			t.Fatal(err)
		}
		want := config.DefaultV2(protocol)
		want.Carriers, want.Listen, want.FEC, want.PSK = cfg.Carriers, cfg.Listen, cfg.FEC, cfg.PSK
		if !reflect.DeepEqual(cfg, want) {
			t.Fatal("YAML and Go v2 defaults differ")
		}
		if cfg.Transport.MaxReceiveUDPPayload != 1200 || cfg.Transport.MTUDiscovery != config.MTUDiscoveryFixed || cfg.Transport.BudgetStrategy != config.BudgetStrategySession || cfg.Transport.PLPMTUD != (config.PLPMTUDConfig{}) || cfg.Transport.MaxRetainedEpochs != 2 || cfg.Transport.MaxMigrations != 2 || cfg.Transport.MaxEpochAge != 5*time.Second {
			t.Fatal("incorrect transport defaults")
		}
		l := cfg.Limits
		if l.MaxPendingHandshakes != 256 || l.MaxPendingAccepts != 256 || l.MaxPeerRetainedBytes != 256<<20 || l.MaxSessionRetainedBytes != 16<<20 || l.MaxDatagramReassemblies != 1024 || l.MaxFragmentsPerDatagram != 256 || l.MaxMigrationTransactionBytes != 8<<20 || l.MaxStreamsPerSession != 128 || l.MaxPeerStreams != 4096 || l.MaxStreamRetainedBytes != 278528 || l.MaxPathQueuedPackets != 256 || l.MaxPathQueuedBytes != 1<<20 || l.MaxSendWorkers != 8 {
			t.Fatal("incorrect resource defaults")
		}
		if cfg.Timers.DatagramReassemblyTimeout != 10*time.Second || cfg.Timers.GroupDecodeTimeout != 10*time.Second {
			t.Fatal("incorrect receive deadline defaults")
		}
		if cfg.Scheduler.OutboundPathRateBPS(1) != 100000000 || cfg.Scheduler.InboundPathRateBPS(1) != 100000000 {
			t.Fatal("incorrect omitted path rate")
		}
	}
	if config.DefaultV2("").Protocol != config.ProtocolDatagram {
		t.Fatal("empty Go protocol lost Datagram default")
	}
	cfg := v2Config(t, "transport: {max_udp_payload: 1472}\n")
	if cfg.Transport.MaxReceiveUDPPayload != 1472 {
		t.Fatal("receive default preceded send override")
	}
	cfg = v2Config(t, "transport: {max_udp_payload: 1472, max_receive_udp_payload: 512, mtu_discovery: plpmtud}\n")
	if cfg.Transport.MaxReceiveUDPPayload != 512 || cfg.Transport.PLPMTUD != (config.PLPMTUDConfig{BaseUDPPayload: 512, ProbeInterval: time.Second, MaxOutstandingPerPath: 1}) {
		t.Fatal("explicit receive or conditional PLPMTUD defaults lost")
	}
	legacy := validConfig()
	legacy.Wire.Version = config.WireVersionV2
	before := legacy.Clone()
	assertInvalid(t, legacy.Validate())
	if !reflect.DeepEqual(legacy, before) {
		t.Fatal("Validate initialized a Go v2 literal")
	}
}

func TestV2SharedStrictScalarsAndV1Boundary(t *testing.T) {
	fields := []string{
		"transport: {max_receive_udp_payload: %s}",
		"transport: {max_retained_epochs: %s}",
		"transport: {max_migrations: %s}",
		"transport: {mtu_discovery: plpmtud, plpmtud: {base_udp_payload: %s}}",
		"transport: {mtu_discovery: plpmtud, plpmtud: {max_outstanding_per_path: %s}}",
		"limits: {max_pending_handshakes: %s}", "limits: {max_pending_accepts: %s}",
		"limits: {max_peer_retained_bytes: %s}", "limits: {max_session_retained_bytes: %s}",
		"limits: {max_datagram_reassemblies: %s}", "limits: {max_fragments_per_datagram: %s}",
		"limits: {max_migration_transaction_bytes: %s}", "limits: {max_streams_per_session: %s}",
		"limits: {max_peer_streams: %s}", "limits: {max_stream_retained_bytes: %s}",
		"limits: {max_path_queued_packets: %s}", "limits: {max_path_queued_bytes: %s}", "limits: {max_send_workers: %s}",
	}
	for _, field := range fields {
		for _, value := range []string{"0", "-1", "1.5", "1.0", "'1'", "true", "null", "[]", "{}", "999999999999999999999999999999"} {
			_, err := config.Parse([]byte(validYAML("wire: {version: v2}\n" + fmt.Sprintf(field, value) + "\n")))
			if err == nil {
				t.Fatalf("accepted %s", fmt.Sprintf(field, value))
			}
		}
		_, err := config.Parse([]byte(validYAML(fmt.Sprintf(field, "0") + "\n")))
		assertInvalid(t, err)
	}
	for _, field := range []string{"transport: {mtu_discovery: %s}", "transport: {budget_strategy: %s}"} {
		for _, value := range []string{"''", "true", "0", "null", "[]", "{}", "FIXED", "private-enum-marker"} {
			_, err := config.Parse([]byte(validYAML("wire: {version: v2}\n" + fmt.Sprintf(field, value) + "\n")))
			assertInvalid(t, err)
			if strings.Contains(err.Error(), "private-enum-marker") {
				t.Fatal("enum error leaked input")
			}
		}
	}
	for _, field := range []string{"transport: {max_epoch_age: %s}", "transport: {mtu_discovery: plpmtud, plpmtud: {probe_interval: %s}}", "timers: {datagram_reassembly_timeout: %s}", "timers: {group_decode_timeout: %s}"} {
		for _, value := range []string{"0", "true", "null", "[]", "{}", "'0s'", "'99ms'", "'61s'", "'99999999999999999999999h'"} {
			_, err := config.Parse([]byte(validYAML("wire: {version: v2}\n" + fmt.Sprintf(field, value) + "\n")))
			assertInvalid(t, err)
		}
	}
	for _, field := range []string{"transport: {max_receive_udp_payload: 1200}", "transport: {mtu_discovery: fixed}", "transport: {budget_strategy: session}", "transport: {plpmtud: {}}", "transport: {outbound_path_budgets: []}", "scheduler: {}", "timers: {datagram_reassembly_timeout: 10s}", "timers: {group_decode_timeout: 10s}"} {
		_, err := config.Parse([]byte(validYAML(field + "\n")))
		assertInvalid(t, err)
	}
}

func TestV2SharedRangesAndCrossLimits(t *testing.T) {
	base := v2Config(t, "")
	for _, test := range []struct {
		name     string
		min, max int
		set      func(*config.Config, int)
	}{
		{"receive", 512, 65507, func(c *config.Config, v int) { c.Transport.MaxReceiveUDPPayload = v }},
		{"epochs", 1, 8, func(c *config.Config, v int) { c.Transport.MaxRetainedEpochs = v }},
		{"migrations", 1, 2, func(c *config.Config, v int) { c.Transport.MaxMigrations = v }},
		{"handshakes", 1, 4096, func(c *config.Config, v int) { c.Limits.MaxPendingHandshakes = v }},
		{"accepts", 1, 65536, func(c *config.Config, v int) { c.Limits.MaxPendingAccepts = v }},
		{"assemblies", 1, 65536, func(c *config.Config, v int) { c.Limits.MaxDatagramReassemblies = v }},
		{"fragments", 1, 4096, func(c *config.Config, v int) { c.Limits.MaxFragmentsPerDatagram = v }},
		{"migration-bytes", 1, 8 << 20, func(c *config.Config, v int) { c.Limits.MaxMigrationTransactionBytes = v }},
		{"session-streams", 1, 4096, func(c *config.Config, v int) { c.Limits.MaxStreamsPerSession = v }},
		{"peer-streams", 1, 65536, func(c *config.Config, v int) { c.Limits.MaxPeerStreams = v }},
		{"stream-bytes", 1, 16 << 20, func(c *config.Config, v int) { c.Limits.MaxStreamRetainedBytes = v }},
		{"queued-packets", 1, 4096, func(c *config.Config, v int) { c.Limits.MaxPathQueuedPackets = v }},
		{"queued-bytes", 512, 16 << 20, func(c *config.Config, v int) { c.Limits.MaxPathQueuedBytes = v }},
		{"workers", 1, 32, func(c *config.Config, v int) { c.Limits.MaxSendWorkers = v }},
	} {
		for _, value := range []int{test.min - 1, test.min, test.max, test.max + 1} {
			cfg := base.Clone()
			test.set(&cfg, value)
			err := cfg.Validate()
			if value >= test.min && value <= test.max {
				if err != nil {
					t.Fatalf("%s=%d: %v", test.name, value, err)
				}
			} else {
				assertInvalid(t, err)
			}
		}
	}
	for _, change := range []func(*config.Config){
		func(c *config.Config) { c.Limits.MaxPeerRetainedBytes = 1<<30 + 1 },
		func(c *config.Config) { c.Limits.MaxPeerRetainedBytes = 1<<20 - 1 },
		func(c *config.Config) { c.Limits.MaxSessionRetainedBytes = c.Limits.MaxPeerRetainedBytes + 1 },
		func(c *config.Config) { c.Limits.MaxSessionRetainedBytes = 1<<20 - 1 },
		func(c *config.Config) {
			c.Limits.MaxSessionRetainedBytes = 1 << 20
			c.Limits.MaxMigrationTransactionBytes = 2 << 20
		},
		func(c *config.Config) { c.Limits.MaxPathQueuedBytes = c.Limits.MaxSessionRetainedBytes + 1 },
		func(c *config.Config) { c.Limits.MaxStreamRetainedBytes = c.Limits.MaxSessionRetainedBytes + 1 },
	} {
		cfg := base.Clone()
		change(&cfg)
		assertInvalid(t, cfg.Validate())
	}
	cfg := base.Clone()
	cfg.Limits.MaxPeerRetainedBytes = 1 << 30
	cfg.Limits.MaxSessionRetainedBytes = 1 << 30
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestV2StaticDirectionalProfilesAndClone(t *testing.T) {
	input := `wire: {version: v2}
listen: ':9000'
transport:
  budget_strategy: per_carrier
  outbound_path_budgets: [{path_id: 1, max_udp_payload: 1000}]
  inbound_path_budgets: [{path_id: 2, max_udp_payload: 1100}, {path_id: 1, max_udp_payload: 512}]
scheduler:
  outbound_path_rates_bps: {1: 1000}
  inbound_path_rates_bps: {2: 1000000000000}
`
	cfg, err := config.Parse([]byte(validYAML(input)))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Transport.InboundPathBudgets[0].PathID != 2 || cfg.Scheduler.OutboundPathRateBPS(1) != 1000 || cfg.Scheduler.InboundPathRateBPS(2) != 1000000000000 || cfg.Scheduler.InboundPathRateBPS(1) != 100000000 {
		t.Fatal("path IDs or directional rates were remapped")
	}
	clone := cfg.Clone()
	clone.Transport.OutboundPathBudgets[0].MaxUDPPayload = 512
	clone.Transport.InboundPathBudgets[0].PathID = 1
	clone.Scheduler.OutboundPathRatesBPS[1] = 5000
	clone.Scheduler.InboundPathRatesBPS[2] = 5000
	if cfg.Transport.OutboundPathBudgets[0].MaxUDPPayload != 1000 || cfg.Transport.InboundPathBudgets[0].PathID != 2 || cfg.Scheduler.OutboundPathRatesBPS[1] != 1000 || cfg.Scheduler.InboundPathRatesBPS[2] != 1000000000000 {
		t.Fatal("Clone aliased new directional settings")
	}
	for _, change := range []func(*config.Config){
		func(c *config.Config) { c.Transport.OutboundPathBudgets = nil },
		func(c *config.Config) { c.Transport.InboundPathBudgets = nil },
		func(c *config.Config) { c.Transport.InboundPathBudgets[0].PathID = 1 },
		func(c *config.Config) { c.Transport.InboundPathBudgets[0].PathID = 3 },
		func(c *config.Config) { c.Transport.InboundPathBudgets[0].MaxUDPPayload = 1201 },
		func(c *config.Config) { c.Transport.InboundPathBudgets[0].MaxUDPPayload = 511 },
		func(c *config.Config) { c.Transport.BudgetStrategy = config.BudgetStrategySession },
		func(c *config.Config) { c.Carriers = nil },
		func(c *config.Config) { c.Listen = "" },
		func(c *config.Config) { c.Limits.MaxEndpointsPerSession = 1 },
		func(c *config.Config) { c.Scheduler.OutboundPathRatesBPS[2] = 1000 },
		func(c *config.Config) { c.Scheduler.InboundPathRatesBPS[3] = 1000 },
	} {
		bad := cfg.Clone()
		change(&bad)
		assertInvalid(t, bad.Validate())
	}
	listener, err := config.Parse([]byte(kcpYAML("transport: {budget_strategy: per_carrier, inbound_path_budgets: [{path_id: 1, max_udp_payload: 1200}]}\n")))
	if err != nil || listener.InitiatorEnabled() {
		t.Fatalf("listener-only static profile: %v", err)
	}
}

func TestV2MalformedPathsAndRates(t *testing.T) {
	for _, extra := range []string{
		"transport: {plpmtud: {}}", "transport: {plpmtud: {base_udp_payload: 512}}",
		"transport: {mtu_discovery: plpmtud, outbound_path_budgets: []}",
		"transport: {outbound_path_budgets: []}",
		"transport: {budget_strategy: per_carrier, outbound_path_budgets: [{}]}",
		"transport: {budget_strategy: per_carrier, outbound_path_budgets: [{path_id: 1}]}",
		"transport: {budget_strategy: per_carrier, outbound_path_budgets: [{path_id: 1.5, max_udp_payload: 1200}]}",
		"transport: {budget_strategy: per_carrier, outbound_path_budgets: [{path_id: 1, max_udp_payload: 1200.0}]}",
		"transport: {budget_strategy: per_carrier, outbound_path_budgets: [{path_id: 1, max_udp_payload: 1200, unknown: 1}]}",
		"transport: {mtu_discovery: plpmtud, plpmtud: {unknown: 1}}",
		"transport: {max_migrations: 1, max_migrations: 2}",
		"scheduler: {unknown: 1}", "scheduler: {outbound_path_rates_bps: []}",
		"scheduler: {outbound_path_rates_bps: {1.5: 1000}}", "scheduler: {outbound_path_rates_bps: {'1': 1000}}",
		"scheduler: {outbound_path_rates_bps: {true: 1000}}", "scheduler: {outbound_path_rates_bps: {1: 1000, 0x1: 2000}}",
		"scheduler: {outbound_path_rates_bps: {1: 1000, 1: 2000}}", "scheduler: {outbound_path_rates_bps: {1: 1000.5}}",
		"scheduler: {outbound_path_rates_bps: {1: '1000'}}", "scheduler: {outbound_path_rates_bps: {1: true}}",
		"scheduler: {outbound_path_rates_bps: {1: 999}}", "scheduler: {outbound_path_rates_bps: {1: 1000000000001}}",
		"scheduler: {outbound_path_rates_bps: {1: 9223372036854775808}}", "scheduler: {outbound_path_rates_bps: {0: 1000}}",
		"scheduler: {outbound_path_rates_bps: {2: 1000}}", "scheduler: {inbound_path_rates_bps: {}}",
		"scheduler: {outbound_path_rates_bps: {1: null}}", "limits: {max_send_workers: null}",
	} {
		input := []byte(validYAML("wire: {version: v2}\n" + extra + "\n"))
		before := bytes.Clone(input)
		cfg, err := config.Parse(input)
		assertInvalid(t, err)
		if !reflect.DeepEqual(cfg, config.Config{}) || !bytes.Equal(input, before) {
			t.Fatal("invalid parse retained config or changed input")
		}
	}
	cfg := v2Config(t, "scheduler: {outbound_path_rates_bps: {0x1: 0x3e8}}\n")
	if cfg.Scheduler.OutboundPathRatesBPS[1] != 1000 {
		t.Fatal("integer key/value notation rejected")
	}
}
