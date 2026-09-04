package config_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mofelee/mpudp/config"
	"gopkg.in/yaml.v3"
)

const testPSK = "correct horse battery staple"

func TestParseValidModesAndDefaults(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		yaml      string
		initiator bool
		listener  bool
	}{
		{
			name: "initiator only",
			yaml: `carriers:
  - "192.0.2.11:4000"
fec:
  data_shards: 3
  parity_shards: 2
psk: "correct horse battery staple"
`,
			initiator: true,
		},
		{
			name: "listener only",
			yaml: `listen: ":9000"
fec:
  data_shards: 3
  parity_shards: 2
psk: "correct horse battery staple"
`,
			listener: true,
		},
		{
			name: "dual",
			yaml: `carriers: ["example.com:4000", "[2001:db8::1]:4000"]
listen: "0.0.0.0:9000"
fec: {data_shards: 3, parity_shards: 2}
psk: "correct horse battery staple"
`,
			initiator: true,
			listener:  true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := config.Parse([]byte(test.yaml))
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if got := cfg.InitiatorEnabled(); got != test.initiator {
				t.Errorf("InitiatorEnabled() = %v, want %v", got, test.initiator)
			}
			if got := cfg.ListenerEnabled(); got != test.listener {
				t.Errorf("ListenerEnabled() = %v, want %v", got, test.listener)
			}
			assertDefaults(t, cfg)
			if got := string(cfg.PSK.Bytes()); got != testPSK {
				t.Fatalf("PSK bytes = %q, want test value", got)
			}
		})
	}
}

func TestParseExplicitRuntimeSettings(t *testing.T) {
	t.Parallel()
	cfg, err := config.Parse([]byte(`listen: "[::]:9000"
fec:
  data_shards: 10
  parity_shards: 4
psk: "key"
transport:
  max_udp_payload: 1000
limits:
  max_datagram_size: 32768
  max_pending_fec_blocks: 64
  receive_queue_capacity: 32
  delivery_queue_capacity: 16
  max_sessions: 128
  max_endpoints_per_session: 24
  max_handshake_attempts: 6
timers:
  decode_timeout: "750ms"
  endpoint_ttl: "30s"
  keepalive_interval: "5s"
  handshake_retry_interval: "250ms"
`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if cfg.Transport.MaxUDPPayload != 1000 ||
		cfg.Limits.MaxDatagramSize != 32768 ||
		cfg.Limits.MaxPendingFECBlocks != 64 ||
		cfg.Limits.ReceiveQueueCapacity != 32 ||
		cfg.Limits.DeliveryQueueCapacity != 16 ||
		cfg.Limits.MaxSessions != 128 ||
		cfg.Limits.MaxEndpointsPerSession != 24 ||
		cfg.Limits.MaxHandshakeAttempts != 6 ||
		cfg.Timers.DecodeTimeout != 750*time.Millisecond ||
		cfg.Timers.EndpointTTL != 30*time.Second ||
		cfg.Timers.KeepaliveInterval != 5*time.Second ||
		cfg.Timers.HandshakeRetryInterval != 250*time.Millisecond {
		t.Fatalf("explicit settings not preserved: %+v", cfg)
	}
}

func TestParseMaxUDPPayloadBoundaries(t *testing.T) {
	t.Parallel()
	valid := []int{config.MinMaxUDPPayload, config.DefaultMaxUDPPayload, config.MaxMaxUDPPayload}
	for _, value := range valid {
		value := value
		t.Run(fmt.Sprintf("valid-%d", value), func(t *testing.T) {
			t.Parallel()
			cfg, err := config.Parse([]byte(validYAML(fmt.Sprintf("transport:\n  max_udp_payload: %d\n", value))))
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if cfg.Transport.MaxUDPPayload != value {
				t.Fatalf("max_udp_payload = %d, want %d", cfg.Transport.MaxUDPPayload, value)
			}
		})
	}

	invalid := []string{
		fmt.Sprint(config.MinMaxUDPPayload - 1),
		fmt.Sprint(config.MaxMaxUDPPayload + 1),
		"0",
		"-1",
		"999999999999999999999999999999",
	}
	for _, value := range invalid {
		value := value
		t.Run("invalid-"+value, func(t *testing.T) {
			t.Parallel()
			_, err := config.Parse([]byte(validYAML("transport:\n  max_udp_payload: " + value + "\n")))
			assertInvalid(t, err)
		})
	}
}

func TestParseRejectsMalformedAndUnknownYAML(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		yaml string
	}{
		{name: "empty document", yaml: ""},
		{name: "unknown top level", yaml: validYAML("mystery: true\n")},
		{name: "unknown transport", yaml: validYAML("transport:\n  mtu: 1200\n")},
		{name: "unknown limits", yaml: validYAML("limits:\n  queue_size: 4\n")},
		{name: "unknown timers", yaml: validYAML("timers:\n  retry: \"1s\"\n")},
		{name: "forbidden peer id", yaml: validYAML("peer:\n  id: alice\n")},
		{name: "forbidden session id", yaml: validYAML("session_id: fixed\n")},
		{name: "duplicate key", yaml: validYAML("psk: \"another\"\n")},
		{name: "multiple documents", yaml: validYAML("---\nlisten: \":9001\"\n")},
		{name: "numeric overflow", yaml: strings.ReplaceAll(validYAML(""), "data_shards: 3", "data_shards: 999999999999999999999999999999")},
		{name: "wrong duration type", yaml: validYAML("timers:\n  decode_timeout: 1000\n")},
		{name: "invalid duration", yaml: validYAML("timers:\n  decode_timeout: never\n")},
		{name: "PSK must be string", yaml: strings.ReplaceAll(validYAML(""), `psk: "correct horse battery staple"`, "psk: [secret]")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := config.Parse([]byte(test.yaml))
			assertInvalid(t, err)
		})
	}
}

func TestParseRejectsExplicitNullAtEveryConfigField(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		yaml string
	}{
		{name: "carriers", yaml: `carriers: null
listen: ":9000"
fec: {data_shards: 3, parity_shards: 2}
psk: "key"
`},
		{name: "listen", yaml: `carriers: ["example.com:4000"]
listen: null
fec: {data_shards: 3, parity_shards: 2}
psk: "key"
`},
		{name: "fec", yaml: `carriers: ["example.com:4000"]
fec: null
psk: "key"
`},
		{name: "psk", yaml: `carriers: ["example.com:4000"]
fec: {data_shards: 3, parity_shards: 2}
psk: null
`},
		{name: "transport", yaml: validYAML("transport: null\n")},
		{name: "limits", yaml: validYAML("limits: null\n")},
		{name: "timers", yaml: validYAML("timers: null\n")},
		{name: "tilde null", yaml: validYAML("transport: ~\n")},
		{name: "empty value null", yaml: validYAML("listen:\n")},
		{name: "carrier item", yaml: `carriers: ["example.com:4000", null]
fec: {data_shards: 3, parity_shards: 2}
psk: "key"
`},
		{name: "fec.data_shards", yaml: `carriers: ["example.com:4000"]
fec: {data_shards: null, parity_shards: 2}
psk: "key"
`},
		{name: "fec.parity_shards", yaml: `carriers: ["example.com:4000"]
fec: {data_shards: 3, parity_shards: null}
psk: "key"
`},
		{name: "transport.max_udp_payload", yaml: validYAML("transport:\n  max_udp_payload: null\n")},
		{name: "limits.max_datagram_size", yaml: validYAML("limits:\n  max_datagram_size: null\n")},
		{name: "limits.max_pending_fec_blocks", yaml: validYAML("limits:\n  max_pending_fec_blocks: null\n")},
		{name: "limits.receive_queue_capacity", yaml: validYAML("limits:\n  receive_queue_capacity: null\n")},
		{name: "limits.delivery_queue_capacity", yaml: validYAML("limits:\n  delivery_queue_capacity: null\n")},
		{name: "limits.max_sessions", yaml: validYAML("limits:\n  max_sessions: null\n")},
		{name: "limits.max_endpoints_per_session", yaml: validYAML("limits:\n  max_endpoints_per_session: null\n")},
		{name: "limits.max_handshake_attempts", yaml: validYAML("limits:\n  max_handshake_attempts: null\n")},
		{name: "timers.decode_timeout", yaml: validYAML("timers:\n  decode_timeout: null\n")},
		{name: "timers.endpoint_ttl", yaml: validYAML("timers:\n  endpoint_ttl: null\n")},
		{name: "timers.keepalive_interval", yaml: validYAML("timers:\n  keepalive_interval: null\n")},
		{name: "timers.handshake_retry_interval", yaml: validYAML("timers:\n  handshake_retry_interval: null\n")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := config.Parse([]byte(test.yaml))
			assertInvalid(t, err)
			if !strings.Contains(err.Error(), "null") {
				t.Fatalf("error = %q, want explicit null diagnosis", err)
			}
		})
	}
}

func TestConfigurationSizeLimit(t *testing.T) {
	t.Parallel()
	exact := paddedYAML(t, validYAML(""), config.MaxConfigBytes)
	if len(exact) != config.MaxConfigBytes {
		t.Fatalf("exact fixture length = %d", len(exact))
	}
	if _, err := config.Parse(exact); err != nil {
		t.Fatalf("Parse(exact limit) error = %v", err)
	}
	if _, err := config.Decode(bytes.NewReader(exact)); err != nil {
		t.Fatalf("Decode(exact limit) error = %v", err)
	}

	tooLarge := append(append([]byte(nil), exact...), '\n')
	if _, err := config.Parse(tooLarge); !errors.Is(err, config.ErrInvalidConfig) {
		t.Fatalf("Parse(limit+1) error = %v, want ErrInvalidConfig", err)
	}
	reader := &trackingReader{data: tooLarge}
	if _, err := config.Decode(reader); !errors.Is(err, config.ErrInvalidConfig) {
		t.Fatalf("Decode(limit+1) error = %v, want ErrInvalidConfig", err)
	}
	if reader.bytesRead != config.MaxConfigBytes+1 {
		t.Fatalf("Decode(limit+1) read %d bytes, want exactly %d", reader.bytesRead, config.MaxConfigBytes+1)
	}
}

func TestDecodeRejectsNilAndReadFailures(t *testing.T) {
	t.Parallel()
	if _, err := config.Decode(nil); !errors.Is(err, config.ErrInvalidConfig) {
		t.Fatalf("Decode(nil) error = %v, want ErrInvalidConfig", err)
	}
	if _, err := config.Decode(failingReader{}); !errors.Is(err, config.ErrInvalidConfig) {
		t.Fatalf("Decode(failing reader) error = %v, want ErrInvalidConfig", err)
	}
}

func TestParseRejectsInvalidAddresses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		carriers string
		listen   string
	}{
		{name: "empty carrier", carriers: `[""]`},
		{name: "empty remote host", carriers: `[":4000"]`},
		{name: "missing port", carriers: `["example.com"]`},
		{name: "zero port", carriers: `["example.com:0"]`},
		{name: "port overflow", carriers: `["example.com:65536"]`},
		{name: "non numeric port", carriers: `["example.com:udp"]`},
		{name: "unbracketed IPv6", carriers: `["2001:db8::1:4000"]`},
		{name: "invalid DNS label", carriers: `["bad_host:4000"]`},
		{name: "surrounding whitespace", carriers: `[" example.com:4000"]`},
		{name: "unspecified IPv4 remote", carriers: `["0.0.0.0:4000"]`},
		{name: "unspecified IPv6 remote", carriers: `["[::]:4000"]`},
		{name: "duplicate exact", carriers: `["example.com:4000", "example.com:4000"]`},
		{name: "duplicate canonical DNS", carriers: `["EXAMPLE.com:04000", "example.com.:4000"]`},
		{name: "duplicate canonical IP", carriers: `["[2001:0db8::1]:4000", "[2001:db8::1]:4000"]`},
		{name: "invalid listen", carriers: "[]", listen: `"bad_host:9000"`},
		{name: "empty listen port", carriers: "[]", listen: `"localhost:"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			yaml := fmt.Sprintf("carriers: %s\nlisten: %s\nfec: {data_shards: 3, parity_shards: 2}\npsk: %q\n", test.carriers, test.listen, testPSK)
			_, err := config.Parse([]byte(yaml))
			assertInvalid(t, err)
		})
	}
}

func TestParseAcceptsAddressForms(t *testing.T) {
	t.Parallel()
	yaml := `carriers:
  - "example.com:4000"
  - "192.0.2.1:4001"
  - "[2001:db8::1]:4002"
  - "[fe80::1%eth0]:4003"
listen: ":9000"
fec: {data_shards: 3, parity_shards: 2}
psk: "key"
`
	if _, err := config.Parse([]byte(yaml)); err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
}

func TestValidateRequiredValuesAndFECBoundaries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*config.Config)
		valid  bool
	}{
		{name: "minimum FEC", mutate: func(c *config.Config) { c.FEC = config.FECConfig{DataShards: 1, ParityShards: 1} }, valid: true},
		{name: "maximum total FEC", mutate: func(c *config.Config) { c.FEC = config.FECConfig{DataShards: 255, ParityShards: 1} }, valid: true},
		{name: "missing mode", mutate: func(c *config.Config) { c.Carriers = nil }},
		{name: "zero data shards", mutate: func(c *config.Config) { c.FEC.DataShards = 0 }},
		{name: "negative data shards", mutate: func(c *config.Config) { c.FEC.DataShards = -1 }},
		{name: "zero parity shards", mutate: func(c *config.Config) { c.FEC.ParityShards = 0 }},
		{name: "negative parity shards", mutate: func(c *config.Config) { c.FEC.ParityShards = -1 }},
		{name: "too many shards", mutate: func(c *config.Config) { c.FEC = config.FECConfig{DataShards: 255, ParityShards: 2} }},
		{name: "empty PSK", mutate: func(c *config.Config) { c.PSK = config.Secret{} }},
		{name: "oversize PSK", mutate: func(c *config.Config) { c.PSK = config.NewSecret(strings.Repeat("x", config.MaxPSKBytes+1)) }},
		{name: "too many carriers", mutate: func(c *config.Config) {
			c.Carriers = make([]string, config.MaxCarriers+1)
			for i := range c.Carriers {
				c.Carriers[i] = fmt.Sprintf("host-%d.example:4000", i)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg := validConfig()
			test.mutate(&cfg)
			err := cfg.Validate()
			if test.valid {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			assertInvalid(t, err)
		})
	}
}

func TestValidateIntegerRanges(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		min    int
		max    int
		mutate func(*config.Config, int)
	}{
		{"transport.max_udp_payload", config.MinMaxUDPPayload, config.MaxMaxUDPPayload, func(c *config.Config, v int) { c.Transport.MaxUDPPayload = v }},
		{"limits.max_datagram_size", config.MinMaxDatagramSize, config.MaxMaxDatagramSize, func(c *config.Config, v int) { c.Limits.MaxDatagramSize = v }},
		{"limits.max_pending_fec_blocks", config.MinMaxPendingFECBlocks, config.MaxMaxPendingFECBlocks, func(c *config.Config, v int) { c.Limits.MaxPendingFECBlocks = v }},
		{"limits.receive_queue_capacity", config.MinReceiveQueueCapacity, config.MaxReceiveQueueCapacity, func(c *config.Config, v int) { c.Limits.ReceiveQueueCapacity = v }},
		{"limits.delivery_queue_capacity", config.MinDeliveryQueueCapacity, config.MaxDeliveryQueueCapacity, func(c *config.Config, v int) { c.Limits.DeliveryQueueCapacity = v }},
		{"limits.max_sessions", config.MinMaxSessions, config.MaxMaxSessions, func(c *config.Config, v int) { c.Limits.MaxSessions = v }},
		{"limits.max_endpoints_per_session", config.MinMaxEndpointsPerSession, config.MaxMaxEndpointsPerSession, func(c *config.Config, v int) { c.Limits.MaxEndpointsPerSession = v }},
		{"limits.max_handshake_attempts", config.MinMaxHandshakeAttempts, config.MaxMaxHandshakeAttempts, func(c *config.Config, v int) { c.Limits.MaxHandshakeAttempts = v }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			for _, value := range []int{test.min, test.max} {
				cfg := validConfig()
				test.mutate(&cfg, value)
				if err := cfg.Validate(); err != nil {
					t.Errorf("value %d rejected: %v", value, err)
				}
			}
			for _, value := range []int{test.min - 1, test.max + 1} {
				cfg := validConfig()
				test.mutate(&cfg, value)
				assertInvalid(t, cfg.Validate())
			}
		})
	}
}

func TestValidateDurationRanges(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		min    time.Duration
		max    time.Duration
		mutate func(*config.Config, time.Duration)
	}{
		{"timers.decode_timeout", config.MinDecodeTimeout, config.MaxDecodeTimeout, func(c *config.Config, v time.Duration) { c.Timers.DecodeTimeout = v }},
		{"timers.endpoint_ttl", config.MinEndpointTTL, config.MaxEndpointTTL, func(c *config.Config, v time.Duration) { c.Timers.EndpointTTL = v }},
		{"timers.keepalive_interval", config.MinKeepaliveInterval, config.MaxKeepaliveInterval, func(c *config.Config, v time.Duration) { c.Timers.KeepaliveInterval = v }},
		{"timers.handshake_retry_interval", config.MinHandshakeRetryInterval, config.MaxHandshakeRetryInterval, func(c *config.Config, v time.Duration) { c.Timers.HandshakeRetryInterval = v }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			for _, value := range []time.Duration{test.min, test.max} {
				cfg := validConfig()
				test.mutate(&cfg, value)
				if err := cfg.Validate(); err != nil {
					t.Errorf("value %s rejected: %v", value, err)
				}
			}
			for _, value := range []time.Duration{test.min - 1, test.max + 1} {
				cfg := validConfig()
				test.mutate(&cfg, value)
				assertInvalid(t, cfg.Validate())
			}
		})
	}
}

func TestExplicitZeroDoesNotReceiveDefault(t *testing.T) {
	t.Parallel()
	fields := []string{
		"transport:\n  max_udp_payload: 0",
		"limits:\n  max_datagram_size: 0",
		"limits:\n  max_pending_fec_blocks: 0",
		"limits:\n  receive_queue_capacity: 0",
		"limits:\n  delivery_queue_capacity: 0",
		"limits:\n  max_sessions: 0",
		"limits:\n  max_endpoints_per_session: 0",
		"limits:\n  max_handshake_attempts: 0",
		"timers:\n  decode_timeout: \"0s\"",
		"timers:\n  endpoint_ttl: \"0s\"",
		"timers:\n  keepalive_interval: \"0s\"",
		"timers:\n  handshake_retry_interval: \"0s\"",
	}
	for _, field := range fields {
		field := field
		t.Run(strings.Split(field, "\n")[0]+field, func(t *testing.T) {
			t.Parallel()
			_, err := config.Parse([]byte(validYAML(field + "\n")))
			assertInvalid(t, err)
		})
	}
}

func TestSecretNeverAppearsInFormattingErrorsOrYAML(t *testing.T) {
	t.Parallel()
	const marker = "psk-DO-NOT-LEAK-7f9101"
	cfg := validConfig()
	cfg.PSK = config.NewSecret(marker)

	outputs := []string{
		fmt.Sprintf("%s", cfg.PSK),
		fmt.Sprintf("%v", cfg.PSK),
		fmt.Sprintf("%+v", cfg.PSK),
		fmt.Sprintf("%#v", cfg.PSK),
		fmt.Sprintf("%q", cfg.PSK),
		fmt.Sprintf("%x", cfg.PSK),
		fmt.Sprintf("%d", cfg.PSK),
		fmt.Sprintf("%v", cfg),
		fmt.Sprintf("%+v", cfg),
		fmt.Sprintf("%#v", cfg),
	}
	encoded, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("yaml.Marshal() error = %v", err)
	}
	outputs = append(outputs, string(encoded))

	cfg.Carriers = []string{"invalid"}
	outputs = append(outputs, cfg.Validate().Error())
	_, decodeErr := config.Parse([]byte(`carriers: ["example.com:4000"]
fec: {data_shards: 3, parity_shards: 2}
psk: "psk-DO-NOT-LEAK-7f9101"
unknown_after_psk: true
`))
	if decodeErr == nil {
		t.Fatal("strict Parse() unexpectedly accepted unknown field")
	}
	outputs = append(outputs, decodeErr.Error())
	for _, output := range outputs {
		if strings.Contains(output, marker) {
			t.Fatalf("secret leaked in output %q", output)
		}
	}
}

func TestCloneDoesNotAliasMutableValues(t *testing.T) {
	t.Parallel()
	original := validConfig()
	clone := original.Clone()
	clone.Carriers[0] = "changed.example:1"
	secretBytes := clone.PSK.Bytes()
	secretBytes[0] ^= 0xff
	if original.Carriers[0] == clone.Carriers[0] {
		t.Fatal("Clone() aliases carrier slice")
	}
	if got := string(original.PSK.Bytes()); got != testPSK {
		t.Fatalf("secret copy mutated original: %q", got)
	}
}

func validConfig() config.Config {
	cfg := config.Default()
	cfg.Carriers = []string{"example.com:4000"}
	cfg.FEC = config.FECConfig{DataShards: 3, ParityShards: 2}
	cfg.PSK = config.NewSecret(testPSK)
	return cfg
}

func validYAML(extra string) string {
	return `carriers: ["example.com:4000"]
fec:
  data_shards: 3
  parity_shards: 2
psk: "correct horse battery staple"
` + extra
}

func assertDefaults(t *testing.T, cfg config.Config) {
	t.Helper()
	if cfg.Transport.MaxUDPPayload != config.DefaultMaxUDPPayload ||
		cfg.Limits.MaxDatagramSize != config.DefaultMaxDatagramSize ||
		cfg.Limits.MaxPendingFECBlocks != config.DefaultMaxPendingFECBlocks ||
		cfg.Limits.ReceiveQueueCapacity != config.DefaultReceiveQueueCapacity ||
		cfg.Limits.DeliveryQueueCapacity != config.DefaultDeliveryQueueCapacity ||
		cfg.Limits.MaxSessions != config.DefaultMaxSessions ||
		cfg.Limits.MaxEndpointsPerSession != config.DefaultMaxEndpointsPerSession ||
		cfg.Limits.MaxHandshakeAttempts != config.DefaultMaxHandshakeAttempts ||
		cfg.Timers.DecodeTimeout != config.DefaultDecodeTimeout ||
		cfg.Timers.EndpointTTL != config.DefaultEndpointTTL ||
		cfg.Timers.KeepaliveInterval != config.DefaultKeepaliveInterval ||
		cfg.Timers.HandshakeRetryInterval != config.DefaultHandshakeRetryInterval {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
}

func assertInvalid(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected configuration error, got nil")
	}
	if !errors.Is(err, config.ErrInvalidConfig) {
		t.Fatalf("error %q does not match ErrInvalidConfig", err)
	}
}

func paddedYAML(t *testing.T, base string, size int) []byte {
	t.Helper()
	if len(base) > size-2 {
		t.Fatalf("base fixture is too large: %d", len(base))
	}
	padding := size - len(base)
	return []byte(base + "#" + strings.Repeat("x", padding-2) + "\n")
}

type trackingReader struct {
	data      []byte
	offset    int
	bytesRead int
}

func (r *trackingReader) Read(payload []byte) (int, error) {
	if r.offset == len(r.data) {
		return 0, io.EOF
	}
	n := copy(payload, r.data[r.offset:])
	r.offset += n
	r.bytesRead += n
	return n, nil
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New("synthetic read failure")
}
