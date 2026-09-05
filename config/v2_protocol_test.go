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

func parseProtocolConfig(t *testing.T, protocol config.Protocol, extra string) config.Config {
	t.Helper()
	input := validYAML("wire: {version: v2}\n" + extra)
	if protocol == config.ProtocolKCP {
		input = kcpYAML(extra)
	}
	cfg, err := config.Parse([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestV2ProtocolDefaultsAndExplicitFalse(t *testing.T) {
	dg := parseProtocolConfig(t, config.ProtocolDatagram, "")
	if dg.Aggregation != (config.AggregationConfig{MaxDelay: 250 * time.Microsecond, MaxRecords: 32, MaxQueuedDatagrams: 256, MaxQueuedBytes: 1 << 20, MaxGroupBytes: 1 << 20}) || dg.Repair != (config.RepairConfig{MaxAge: 5 * time.Second, MaxAttempts: 3, MaxCachedBlocks: 1024, MaxCachedBytes: 8 << 20, MaxOutstandingDatagramSpan: 65536, MaxOutstandingGroupSpan: 65536}) {
		t.Fatal("incorrect Datagram defaults")
	}
	if dg.KCP != (config.KCPConfig{}) || dg.StreamMux != (config.StreamMuxConfig{}) {
		t.Fatal("Datagram initialized reliable settings")
	}
	kcp := parseProtocolConfig(t, config.ProtocolKCP, "")
	if kcp.KCP != (config.KCPConfig{FastRetransmit: config.FastRetransmitConfig{Enabled: true, Threshold: 2}, UpdateInterval: 10 * time.Millisecond, SendWindowSegments: 1024, ReceiveWindowSegments: 1024, CongestionControl: true}) || kcp.StreamMux != (config.StreamMuxConfig{MaxFrameSize: 16384, MaxPendingOpens: 128, OpenTimeout: 5 * time.Second, MaxControlRecordBytes: 256, MaxQueuedControlBytes: 32768}) {
		t.Fatal("incorrect KCP defaults")
	}
	if kcp.Aggregation != (config.AggregationConfig{}) || kcp.Repair != (config.RepairConfig{}) {
		t.Fatal("KCP initialized Datagram features")
	}
	override := parseProtocolConfig(t, config.ProtocolKCP, "kcp: {fast_retransmit: {enabled: false, threshold: 255}, congestion_control: false}\n")
	if override.KCP.FastRetransmit.Enabled || override.KCP.CongestionControl || override.KCP.FastRetransmit.Threshold != 255 {
		t.Fatal("explicit false was replaced or threshold enabled retransmit")
	}
	defaultEnabled := parseProtocolConfig(t, config.ProtocolKCP, "kcp: {fast_retransmit: {threshold: 7}}\n")
	if !defaultEnabled.KCP.FastRetransmit.Enabled || !defaultEnabled.KCP.CongestionControl || defaultEnabled.KCP.FastRetransmit.Threshold != 7 {
		t.Fatal("omitted booleans did not use defaults")
	}
	dg = parseProtocolConfig(t, config.ProtocolDatagram, "aggregation: {enabled: true}\nrepair: {enabled: true}\n")
	if !dg.Aggregation.Enabled || !dg.Repair.Enabled {
		t.Fatal("Datagram features not enabled explicitly")
	}
}

func TestV2InactiveSectionsRequireBareDisabledForm(t *testing.T) {
	for _, version := range []string{"v1", "v2"} {
		base := validYAML("wire: {version: " + version + "}\n")
		want, err := config.Parse([]byte(base))
		if err != nil {
			t.Fatal(err)
		}
		sections := []string{"stream_mux"}
		if version == "v1" {
			sections = append(sections, "aggregation", "repair")
		}
		for _, section := range sections {
			cfg, err := config.Parse([]byte(base + section + ": {enabled: false}\n"))
			if err != nil || !reflect.DeepEqual(cfg, want) {
				t.Fatalf("%s neutral %s: %v", version, section, err)
			}
			for _, value := range []string{"{}", "{enabled: true}", "{enabled: false, unknown: 1}"} {
				_, err := config.Parse([]byte(base + section + ": " + value + "\n"))
				assertInvalid(t, err)
			}
		}
		for _, value := range []string{"{}", "{enabled: false}", "{fast_retransmit: {enabled: false}}", "{send_window_segments: 1024}"} {
			_, err := config.Parse([]byte(base + "kcp: " + value + "\n"))
			assertInvalid(t, err)
		}
	}
	base := kcpYAML("")
	want, err := config.Parse([]byte(base))
	if err != nil {
		t.Fatal(err)
	}
	for _, section := range []string{"aggregation", "repair"} {
		cfg, err := config.Parse([]byte(base + section + ": {enabled: false}\n"))
		if err != nil || !reflect.DeepEqual(cfg, want) {
			t.Fatalf("neutral %s: %v", section, err)
		}
		for _, value := range []string{"{}", "{enabled: true}"} {
			_, err := config.Parse([]byte(base + section + ": " + value + "\n"))
			assertInvalid(t, err)
		}
	}
	for _, input := range []string{
		validYAML("aggregation: {enabled: false, max_records: 32}\n"),
		validYAML("repair: {enabled: false, max_age: 5s}\n"),
		validYAML("stream_mux: {enabled: false, max_frame_size: 16384}\n"),
		validYAML("wire: {version: v2}\nstream_mux: {enabled: false, max_pending_opens: 128}\n"),
		kcpYAML("aggregation: {enabled: false, max_delay: 250us}\n"),
		kcpYAML("repair: {enabled: false, max_attempts: 3}\n"),
		kcpYAML("kcp: {enabled: false}\n"),
	} {
		_, err := config.Parse([]byte(input))
		assertInvalid(t, err)
	}
	for _, cfg := range []config.Config{
		func() config.Config { c := validConfig(); c.Aggregation.MaxRecords = 32; return c }(),
		func() config.Config {
			c := parseProtocolConfig(t, config.ProtocolDatagram, "")
			c.KCP = config.DefaultV2(config.ProtocolKCP).KCP
			return c
		}(),
		func() config.Config {
			c := parseProtocolConfig(t, config.ProtocolKCP, "")
			c.Repair = config.DefaultV2(config.ProtocolDatagram).Repair
			return c
		}(),
	} {
		assertInvalid(t, cfg.Validate())
	}
}

func TestV2FeatureScalarTypes(t *testing.T) {
	for _, test := range []struct {
		protocol config.Protocol
		field    string
	}{
		{config.ProtocolDatagram, "aggregation: {enabled: %s}"},
		{config.ProtocolDatagram, "repair: {enabled: %s}"},
		{config.ProtocolKCP, "kcp: {fast_retransmit: {enabled: %s}}"},
		{config.ProtocolKCP, "kcp: {congestion_control: %s}"},
		{config.ProtocolKCP, "stream_mux: {enabled: %s}"},
	} {
		for _, value := range []string{"'false'", "yes", "no", "0", "1", "0.0", "null", "[]", "{}", "private-bool-marker"} {
			extra := fmt.Sprintf(test.field, value) + "\n"
			input := validYAML("wire: {version: v2}\n" + extra)
			if test.protocol == config.ProtocolKCP {
				input = kcpYAML(extra)
			}
			_, err := config.Parse([]byte(input))
			assertInvalid(t, err)
			if strings.Contains(err.Error(), "private-bool-marker") {
				t.Fatal("boolean error leaked input")
			}
		}
	}
	for _, test := range []struct {
		protocol config.Protocol
		field    string
	}{
		{config.ProtocolDatagram, "aggregation: {max_records: %s}"},
		{config.ProtocolDatagram, "aggregation: {max_queued_datagrams: %s}"},
		{config.ProtocolDatagram, "aggregation: {max_queued_bytes: %s}"},
		{config.ProtocolDatagram, "aggregation: {max_group_bytes: %s}"},
		{config.ProtocolDatagram, "repair: {max_attempts: %s}"},
		{config.ProtocolDatagram, "repair: {max_cached_blocks: %s}"},
		{config.ProtocolDatagram, "repair: {max_cached_bytes: %s}"},
		{config.ProtocolDatagram, "repair: {max_outstanding_datagram_span: %s}"},
		{config.ProtocolDatagram, "repair: {max_outstanding_group_span: %s}"},
		{config.ProtocolKCP, "kcp: {fast_retransmit: {threshold: %s}}"},
		{config.ProtocolKCP, "kcp: {send_window_segments: %s}"},
		{config.ProtocolKCP, "kcp: {receive_window_segments: %s}"},
		{config.ProtocolKCP, "stream_mux: {max_frame_size: %s}"},
		{config.ProtocolKCP, "stream_mux: {max_pending_opens: %s}"},
		{config.ProtocolKCP, "stream_mux: {max_control_record_bytes: %s}"},
		{config.ProtocolKCP, "stream_mux: {max_queued_control_bytes: %s}"},
	} {
		for _, value := range []string{"0", "-1", "1.5", "'1'", "true", "null", "[]", "{}", "999999999999999999999999999"} {
			extra := fmt.Sprintf(test.field, value) + "\n"
			input := validYAML("wire: {version: v2}\n" + extra)
			if test.protocol == config.ProtocolKCP {
				input = kcpYAML(extra)
			}
			before := []byte(input)
			cfg, err := config.Parse(before)
			assertInvalid(t, err)
			if !reflect.DeepEqual(cfg, config.Config{}) || !bytes.Equal(before, []byte(input)) {
				t.Fatal("invalid parse retained config or changed input")
			}
		}
	}
	for _, test := range []struct {
		protocol config.Protocol
		field    string
		min, max time.Duration
	}{
		{config.ProtocolDatagram, "aggregation: {max_delay: %s}", time.Microsecond, 10 * time.Millisecond},
		{config.ProtocolDatagram, "repair: {max_age: %s}", 100 * time.Millisecond, time.Minute},
		{config.ProtocolKCP, "kcp: {update_interval: %s}", 10 * time.Millisecond, 100 * time.Millisecond},
		{config.ProtocolKCP, "stream_mux: {open_timeout: %s}", 100 * time.Millisecond, 5 * time.Second},
	} {
		for _, value := range []string{"0", "true", "null", "[]", "{}", "0s", (test.min - time.Nanosecond).String(), (test.max + time.Nanosecond).String()} {
			extra := fmt.Sprintf(test.field, value) + "\n"
			input := validYAML("wire: {version: v2}\n" + extra)
			if test.protocol == config.ProtocolKCP {
				input = kcpYAML(extra)
			}
			_, err := config.Parse([]byte(input))
			assertInvalid(t, err)
		}
		for _, value := range []time.Duration{test.min, test.max} {
			parseProtocolConfig(t, test.protocol, fmt.Sprintf(test.field, value.String())+"\n")
		}
	}
	for _, input := range []string{
		validYAML("wire: {version: v2}\naggregation: {enabled: false, enabled: true}\n"),
		validYAML("wire: {version: v2}\nrepair: {mystery: 1}\n"),
		kcpYAML("kcp: {fast_retransmit: {threshold: 2, threshold: 3}}\n"),
		kcpYAML("kcp: {fast_retransmit: {mystery: 1}}\n"),
		kcpYAML("stream_mux: {mystery: 1}\n"),
	} {
		_, err := config.Parse([]byte(input))
		assertInvalid(t, err)
	}
}

func TestV2FeatureRanges(t *testing.T) {
	for _, test := range []struct {
		protocol config.Protocol
		min, max int
		set      func(*config.Config, int)
	}{
		{config.ProtocolDatagram, 1, 256, func(c *config.Config, v int) { c.Aggregation.MaxRecords = v }},
		{config.ProtocolDatagram, 1, 65536, func(c *config.Config, v int) { c.Aggregation.MaxQueuedDatagrams = v }},
		{config.ProtocolDatagram, 1, 16 << 20, func(c *config.Config, v int) { c.Aggregation.MaxQueuedBytes = v }},
		{config.ProtocolDatagram, 24, 16 << 20, func(c *config.Config, v int) { c.Aggregation.MaxGroupBytes = v }},
		{config.ProtocolDatagram, 1, 16, func(c *config.Config, v int) { c.Repair.MaxAttempts = v }},
		{config.ProtocolDatagram, 1, 65536, func(c *config.Config, v int) { c.Repair.MaxCachedBlocks = v }},
		{config.ProtocolDatagram, 1, 16 << 20, func(c *config.Config, v int) { c.Repair.MaxCachedBytes = v }},
		{config.ProtocolDatagram, 1, 65536, func(c *config.Config, v int) { c.Repair.MaxOutstandingDatagramSpan = v }},
		{config.ProtocolDatagram, 1, 65536, func(c *config.Config, v int) { c.Repair.MaxOutstandingGroupSpan = v; c.Repair.MaxCachedBlocks = 1 }},
		{config.ProtocolKCP, 1, 255, func(c *config.Config, v int) { c.KCP.FastRetransmit.Threshold = v }},
		{config.ProtocolKCP, 32, 65535, func(c *config.Config, v int) { c.KCP.SendWindowSegments = v }},
		{config.ProtocolKCP, 32, 65535, func(c *config.Config, v int) { c.KCP.ReceiveWindowSegments = v }},
		{config.ProtocolKCP, 128, 65535, func(c *config.Config, v int) { c.StreamMux.MaxFrameSize = v }},
		{config.ProtocolKCP, 1, 128, func(c *config.Config, v int) { c.StreamMux.MaxPendingOpens = v }},
		{config.ProtocolKCP, 256, 256, func(c *config.Config, v int) { c.StreamMux.MaxControlRecordBytes = v }},
		{config.ProtocolKCP, 256, 32768, func(c *config.Config, v int) { c.StreamMux.MaxQueuedControlBytes = v }},
	} {
		for _, value := range []int{test.min - 1, test.min, test.max, test.max + 1} {
			cfg := parseProtocolConfig(t, test.protocol, "")
			test.set(&cfg, value)
			err := cfg.Validate()
			if value >= test.min && value <= test.max {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				assertInvalid(t, err)
			}
		}
	}
}

func TestV2RepairAndMuxCrossLimits(t *testing.T) {
	for _, deadline := range []string{"datagram_reassembly_timeout", "group_decode_timeout"} {
		input := validYAML("wire: {version: v2}\nrepair: {enabled: true, max_age: 5s}\ntimers: {" + deadline + ": 4999ms}\n")
		_, err := config.Parse([]byte(input))
		assertInvalid(t, err)
		parseProtocolConfig(t, config.ProtocolDatagram, "repair: {enabled: true, max_age: 5s}\ntimers: {"+deadline+": 5s}\n")
	}
	for _, extra := range []string{
		"repair: {max_cached_blocks: 1024, max_outstanding_group_span: 1023}\n",
		"repair: {max_cached_bytes: 16777217}\n",
		"aggregation: {max_queued_bytes: 16777217}\n",
	} {
		_, err := config.Parse([]byte(validYAML("wire: {version: v2}\n" + extra)))
		assertInvalid(t, err)
	}
	for _, frame := range []int{128, 16384, 65535} {
		cfg := parseProtocolConfig(t, config.ProtocolKCP, fmt.Sprintf("stream_mux: {enabled: true, max_frame_size: %d}\n", frame))
		if cfg.Limits.MaxStreamRetainedBytes != 262144+frame {
			t.Fatal("stream receive default preceded frame override")
		}
		cfg.Limits.MaxStreamRetainedBytes--
		assertInvalid(t, cfg.Validate())
		_, err := config.Parse([]byte(kcpYAML(fmt.Sprintf("stream_mux: {enabled: true, max_frame_size: %d}\nlimits: {max_stream_retained_bytes: %d}\n", frame, 262144+frame-1))))
		assertInvalid(t, err)
	}
	raw := parseProtocolConfig(t, config.ProtocolKCP, "stream_mux: {enabled: false, max_frame_size: 65535}\nlimits: {max_stream_retained_bytes: 1}\n")
	if raw.Limits.MaxStreamRetainedBytes != 1 {
		t.Fatal("explicit raw-stream bytes replaced by mux default")
	}
	largeWindows := parseProtocolConfig(t, config.ProtocolKCP, "kcp: {send_window_segments: 65535, receive_window_segments: 65535}\nlimits: {max_session_retained_bytes: 1048576, max_migration_transaction_bytes: 1048576}\n")
	if largeWindows.KCP.SendWindowSegments != 65535 || largeWindows.KCP.ReceiveWindowSegments != 65535 {
		t.Fatal("configured windows silently clamped")
	}
}
