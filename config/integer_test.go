package config_test

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/mofelee/mpudp/config"
)

func integerFieldYAML(field, value string) string {
	group, name, _ := strings.Cut(field, ".")
	if group == "fec" {
		old := "data_shards: 3"
		if name == "parity_shards" {
			old = "parity_shards: 2"
		}
		return strings.Replace(validYAML(""), old, name+": "+value, 1)
	}
	return validYAML(group + ":\n  " + name + ": " + value + "\n")
}

func TestEveryIntegerFieldRejectsImplicitConversion(t *testing.T) {
	t.Parallel()
	for _, field := range []string{
		"fec.data_shards", "fec.parity_shards", "transport.max_udp_payload",
		"limits.max_datagram_size", "limits.max_pending_fec_blocks",
		"limits.receive_queue_capacity", "limits.delivery_queue_capacity",
		"limits.max_sessions", "limits.max_endpoints_per_session", "limits.max_handshake_attempts",
	} {
		for _, value := range []string{"120.5", "120.0", "1.2e2", "!!float 120", "'120'", "true", "[120]", "{value: 120}", ".inf", ".nan", "999999999999999999999999999999999"} {
			t.Run(field+"/"+value, func(t *testing.T) {
				t.Parallel()
				input := []byte(integerFieldYAML(field, value))
				original := bytes.Clone(input)
				parsed, err := config.Parse(input)
				assertInvalid(t, err)
				decoded, decodeErr := config.Decode(bytes.NewReader(input))
				assertInvalid(t, decodeErr)
				if !reflect.DeepEqual(parsed, config.Config{}) || !reflect.DeepEqual(decoded, config.Config{}) || !bytes.Equal(input, original) {
					t.Fatal("failed parsing retained configuration or mutated input")
				}
			})
		}
	}
}

func TestIntegerSyntaxAndOmittedDefaultsRemainCompatible(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"1200", "+1200", "0x4b0", "0o2260", "0b10010110000", "1_200", "!!int 1200"} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			cfg, err := config.Parse([]byte(integerFieldYAML("transport.max_udp_payload", value)))
			if err != nil || cfg.Transport.MaxUDPPayload != 1200 {
				t.Fatalf("integer syntax rejected: payload=%d err=%v", cfg.Transport.MaxUDPPayload, err)
			}
		})
	}
	cfg, err := config.Parse([]byte(validYAML("")))
	if err != nil {
		t.Fatal(err)
	}
	assertDefaults(t, cfg)
	for _, value := range []string{"0", "-1"} {
		_, err := config.Parse([]byte(integerFieldYAML("limits.max_sessions", value)))
		assertInvalid(t, err)
	}
}
