package config_test

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/mofelee/mpudp/config"
)

func TestProtocolAndWireSelections(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		yaml     string
		protocol config.Protocol
		version  config.WireVersion
		fec      config.FECConfig
	}{
		{"omitted legacy fields", validYAML(""), config.ProtocolDatagram, config.WireVersionV1, config.FECConfig{DataShards: 3, ParityShards: 2}},
		{"explicit v1 datagram", validYAML("protocol: datagram\nwire: {version: v1}\n"), config.ProtocolDatagram, config.WireVersionV1, config.FECConfig{DataShards: 3, ParityShards: 2}},
		{"v2 default datagram", validYAML("wire: {version: v2}\n"), config.ProtocolDatagram, config.WireVersionV2, config.FECConfig{DataShards: 3, ParityShards: 2}},
		{"empty wire mapping", validYAML("wire: {}\n"), config.ProtocolDatagram, config.WireVersionV1, config.FECConfig{DataShards: 3, ParityShards: 2}},
		{"kcp omitted fec", kcpYAML(""), config.ProtocolKCP, config.WireVersionV2, config.FECConfig{}},
		{"kcp empty fec", kcpYAML("fec: {}\n"), config.ProtocolKCP, config.WireVersionV2, config.FECConfig{}},
		{"kcp explicit zeros", kcpYAML("fec: {data_shards: 0, parity_shards: 0}\n"), config.ProtocolKCP, config.WireVersionV2, config.FECConfig{}},
		{"kcp one omitted zero", kcpYAML("fec: {parity_shards: 0}\n"), config.ProtocolKCP, config.WireVersionV2, config.FECConfig{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			input := []byte(test.yaml)
			original := bytes.Clone(input)
			cfg, err := config.Parse(input)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if cfg.Protocol != test.protocol || cfg.Wire.Version != test.version || cfg.FEC != test.fec {
				t.Fatalf("selection = (%q, %q, %+v)", cfg.Protocol, cfg.Wire.Version, cfg.FEC)
			}
			if !bytes.Equal(input, original) {
				t.Fatal("Parse mutated its input")
			}
			decoded, err := config.Decode(bytes.NewReader(original))
			if err != nil || !reflect.DeepEqual(cfg, decoded) {
				t.Fatalf("Decode disagrees with Parse: %v", err)
			}
		})
	}
}

func TestProtocolAndWireRejectStrictYAML(t *testing.T) {
	t.Parallel()
	for _, field := range []string{
		"protocol: null", "protocol: ~", "protocol:", "protocol: ''",
		"protocol: 1", "protocol: true", "protocol: [datagram]", "protocol: {value: datagram}",
		"protocol: tcp", "protocol: DATAGRAM", "protocol: ' datagram'",
		"protocol: datagram\nprotocol: kcp", "wire: null", "wire: []", "wire: v2",
		"wire: {version: null}", "wire: {version: ''}", "wire: {version: 2}",
		"wire: {version: true}", "wire: {version: [v1]}", "wire: {version: {value: v1}}",
		"wire: {version: v3}", "wire: {version: V2}", "wire: {version: 'v2 '}",
		"wire: {version: v1, version: v2}", "wire: {version: v2, mystery: 1}",
		"protocol: kcp", "protocol: kcp\nwire: {version: v1}",
		"wire: {version: v2}\nrepair: {enabled: true}",
		"wire: {version: v2}\naggregation: {enabled: true}",
		"wire: {version: v2}\nkcp: {send_window_segments: 1024}",
	} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			input := []byte(validYAML(field + "\n"))
			before := bytes.Clone(input)
			cfg, err := config.Parse(input)
			assertInvalid(t, err)
			if !reflect.DeepEqual(cfg, config.Config{}) || !bytes.Equal(input, before) {
				t.Fatal("failed Parse retained configuration or mutated input")
			}
		})
	}
}

func TestProtocolSpecificFECValidation(t *testing.T) {
	t.Parallel()
	for _, values := range []string{
		"{data_shards: 1}", "{parity_shards: 1}", "{data_shards: -1}", "{parity_shards: -1}",
		"{data_shards: 3, parity_shards: 2}", "{data_shards: 99999999999999999999999999999}",
		"{data_shards: null}", "{data_shards: '0'}", "{data_shards: false}", "null",
		"{data_shards: 0.0}", "{data_shards: 0.5}", "{parity_shards: -0.5}",
	} {
		t.Run("kcp/"+values, func(t *testing.T) {
			t.Parallel()
			_, err := config.Parse([]byte(kcpYAML("fec: " + values + "\n")))
			assertInvalid(t, err)
		})
	}
	for _, version := range []string{"v1", "v2"} {
		for _, fec := range []string{"", "fec: {}\n", "fec: {data_shards: 3}\n", "fec: {data_shards: 0, parity_shards: 0}\n"} {
			t.Run(version+"/"+fec, func(t *testing.T) {
				t.Parallel()
				_, err := config.Parse([]byte("listen: ':9000'\npsk: key\nwire: {version: " + version + "}\n" + fec))
				assertInvalid(t, err)
			})
		}
	}
}

func TestV2UDPPayloadBoundaries(t *testing.T) {
	t.Parallel()
	for _, protocol := range []config.Protocol{config.ProtocolDatagram, config.ProtocolKCP} {
		for _, size := range []int{0, 71, 72, 511, 512, 1200, 65507, 65508} {
			t.Run(fmt.Sprintf("%s/%d", protocol, size), func(t *testing.T) {
				t.Parallel()
				cfg := config.DefaultV2(protocol)
				legacy := validConfig()
				cfg.Carriers, cfg.PSK, cfg.FEC = legacy.Carriers, legacy.PSK, legacy.FEC
				if protocol == config.ProtocolKCP {
					cfg.FEC = config.FECConfig{}
				}
				cfg.Transport.MaxUDPPayload = size
				err := cfg.Validate()
				if size >= config.MinV2MaxUDPPayload && size <= config.MaxMaxUDPPayload {
					if err != nil {
						t.Fatalf("Validate() error = %v", err)
					}
				} else {
					assertInvalid(t, err)
				}
			})
		}
	}
}

func TestLegacyGoConfigSelectionIsReadOnly(t *testing.T) {
	t.Parallel()
	cfg := validConfig()
	cfg.Protocol, cfg.Wire = "", config.WireConfig{}
	cfg.Transport.MaxUDPPayload = config.MinMaxUDPPayload
	before := cfg.Clone()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("legacy Validate() error = %v", err)
	}
	if cfg.EffectiveProtocol() != config.ProtocolDatagram || cfg.EffectiveWireVersion() != config.WireVersionV1 {
		t.Fatal("legacy Go configuration lost datagram/v1 defaults")
	}
	if !reflect.DeepEqual(cfg, before) {
		t.Fatal("Validate rewrote legacy Go configuration")
	}
	cfg.Transport.MaxUDPPayload = 0
	assertInvalid(t, cfg.Validate())
}

func TestSelectionErrorsDoNotEchoInput(t *testing.T) {
	t.Parallel()
	const marker = "private-selection-marker-19d92"
	for _, field := range []string{"protocol: " + marker, "wire: {version: " + marker + "}"} {
		_, err := config.Parse([]byte(validYAML(field + "\n")))
		assertInvalid(t, err)
		if strings.Contains(err.Error(), marker) || strings.Contains(err.Error(), testPSK) {
			t.Fatal("selection error disclosed input")
		}
	}
	for _, field := range []string{"protocol", "version"} {
		cfg := validConfig()
		if field == "protocol" {
			cfg.Protocol = config.Protocol(marker)
		} else {
			cfg.Wire.Version = config.WireVersion(marker)
		}
		err := cfg.Validate()
		assertInvalid(t, err)
		if strings.Contains(err.Error(), marker) || strings.Contains(err.Error(), testPSK) {
			t.Fatal("Go selection error disclosed input")
		}
	}
}

func kcpYAML(extra string) string {
	return "listen: ':9000'\npsk: key\nprotocol: kcp\nwire: {version: v2}\n" + extra
}
