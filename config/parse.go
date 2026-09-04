package config

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"gopkg.in/yaml.v3"
)

type rawConfig struct {
	Carriers  []string     `yaml:"carriers,omitempty"`
	Listen    string       `yaml:"listen,omitempty"`
	FEC       *rawFEC      `yaml:"fec"`
	PSK       Secret       `yaml:"psk"`
	Transport rawTransport `yaml:"transport,omitempty"`
	Limits    rawLimits    `yaml:"limits,omitempty"`
	Timers    rawTimers    `yaml:"timers,omitempty"`
}

type rawFEC struct {
	DataShards   *int `yaml:"data_shards"`
	ParityShards *int `yaml:"parity_shards"`
}

type rawTransport struct {
	MaxUDPPayload *int `yaml:"max_udp_payload"`
}

type rawLimits struct {
	MaxDatagramSize        *int `yaml:"max_datagram_size"`
	MaxPendingFECBlocks    *int `yaml:"max_pending_fec_blocks"`
	ReceiveQueueCapacity   *int `yaml:"receive_queue_capacity"`
	DeliveryQueueCapacity  *int `yaml:"delivery_queue_capacity"`
	MaxSessions            *int `yaml:"max_sessions"`
	MaxEndpointsPerSession *int `yaml:"max_endpoints_per_session"`
	MaxHandshakeAttempts   *int `yaml:"max_handshake_attempts"`
}

type rawTimers struct {
	DecodeTimeout          *yamlDuration `yaml:"decode_timeout"`
	EndpointTTL            *yamlDuration `yaml:"endpoint_ttl"`
	KeepaliveInterval      *yamlDuration `yaml:"keepalive_interval"`
	HandshakeRetryInterval *yamlDuration `yaml:"handshake_retry_interval"`
}

type yamlDuration struct {
	time.Duration
}

func (d *yamlDuration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return fmt.Errorf("duration must be a string such as 500ms, 15s, or 2m")
	}
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("invalid duration: %w", err)
	}
	d.Duration = parsed
	return nil
}

// Parse decodes one strict YAML document up to MaxConfigBytes and applies
// defaults only to omitted optional fields. Explicit null is always invalid.
func Parse(data []byte) (Config, error) {
	if len(data) > MaxConfigBytes {
		return Config{}, invalidf("configuration exceeds the maximum size of %d bytes", MaxConfigBytes)
	}
	return parseBytes(data)
}

// Decode reads at most MaxConfigBytes+1 bytes and decodes exactly one strict
// YAML document. Unknown fields, duplicate keys, explicit null, invalid scalar
// types, and trailing documents are rejected.
func Decode(reader io.Reader) (Config, error) {
	if reader == nil {
		return Config{}, invalidf("configuration reader must not be nil")
	}
	data, err := io.ReadAll(io.LimitReader(reader, MaxConfigBytes+1))
	if err != nil {
		return Config{}, invalidf("read configuration: %v", err)
	}
	if len(data) > MaxConfigBytes {
		return Config{}, invalidf("configuration exceeds the maximum size of %d bytes", MaxConfigBytes)
	}
	return parseBytes(data)
}

func parseBytes(data []byte) (Config, error) {
	if err := rejectExplicitNull(data); err != nil {
		return Config{}, err
	}

	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)

	var raw rawConfig
	if err := decoder.Decode(&raw); err != nil {
		return Config{}, invalidf("YAML decode failed: %v", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Config{}, invalidf("multiple YAML documents are not allowed")
		}
		return Config{}, invalidf("trailing YAML decode failed: %v", err)
	}

	cfg := Default()
	cfg.Carriers = append([]string(nil), raw.Carriers...)
	cfg.Listen = raw.Listen
	cfg.PSK = raw.PSK.clone()
	if raw.FEC != nil {
		if raw.FEC.DataShards != nil {
			cfg.FEC.DataShards = *raw.FEC.DataShards
		}
		if raw.FEC.ParityShards != nil {
			cfg.FEC.ParityShards = *raw.FEC.ParityShards
		}
	}
	if raw.Transport.MaxUDPPayload != nil {
		cfg.Transport.MaxUDPPayload = *raw.Transport.MaxUDPPayload
	}
	if raw.Limits.MaxDatagramSize != nil {
		cfg.Limits.MaxDatagramSize = *raw.Limits.MaxDatagramSize
	}
	if raw.Limits.MaxPendingFECBlocks != nil {
		cfg.Limits.MaxPendingFECBlocks = *raw.Limits.MaxPendingFECBlocks
	}
	if raw.Limits.ReceiveQueueCapacity != nil {
		cfg.Limits.ReceiveQueueCapacity = *raw.Limits.ReceiveQueueCapacity
	}
	if raw.Limits.DeliveryQueueCapacity != nil {
		cfg.Limits.DeliveryQueueCapacity = *raw.Limits.DeliveryQueueCapacity
	}
	if raw.Limits.MaxSessions != nil {
		cfg.Limits.MaxSessions = *raw.Limits.MaxSessions
	}
	if raw.Limits.MaxEndpointsPerSession != nil {
		cfg.Limits.MaxEndpointsPerSession = *raw.Limits.MaxEndpointsPerSession
	}
	if raw.Limits.MaxHandshakeAttempts != nil {
		cfg.Limits.MaxHandshakeAttempts = *raw.Limits.MaxHandshakeAttempts
	}
	if raw.Timers.DecodeTimeout != nil {
		cfg.Timers.DecodeTimeout = raw.Timers.DecodeTimeout.Duration
	}
	if raw.Timers.EndpointTTL != nil {
		cfg.Timers.EndpointTTL = raw.Timers.EndpointTTL.Duration
	}
	if raw.Timers.KeepaliveInterval != nil {
		cfg.Timers.KeepaliveInterval = raw.Timers.KeepaliveInterval.Duration
	}
	if raw.Timers.HandshakeRetryInterval != nil {
		cfg.Timers.HandshakeRetryInterval = raw.Timers.HandshakeRetryInterval.Duration
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func rejectExplicitNull(data []byte) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		if err == io.EOF {
			return nil
		}
		return invalidf("YAML decode failed: %v", err)
	}
	return rejectNullNode(&document, "$", make(map[*yaml.Node]bool))
}

func rejectNullNode(node *yaml.Node, path string, visiting map[*yaml.Node]bool) error {
	if node == nil {
		return nil
	}
	if node.Tag == "!!null" {
		return invalidf("%s must not be null", path)
	}
	if visiting[node] {
		return nil
	}
	visiting[node] = true
	defer delete(visiting, node)

	switch node.Kind {
	case yaml.DocumentNode:
		for _, child := range node.Content {
			if err := rejectNullNode(child, path, visiting); err != nil {
				return err
			}
		}
	case yaml.MappingNode:
		for index := 0; index+1 < len(node.Content); index += 2 {
			key := node.Content[index]
			value := node.Content[index+1]
			childPath := path + ".<key>"
			if key.Kind == yaml.ScalarNode {
				childPath = path + "." + key.Value
			}
			if err := rejectNullNode(value, childPath, visiting); err != nil {
				return err
			}
		}
	case yaml.SequenceNode:
		for index, child := range node.Content {
			if err := rejectNullNode(child, fmt.Sprintf("%s[%d]", path, index), visiting); err != nil {
				return err
			}
		}
	case yaml.AliasNode:
		return rejectNullNode(node.Alias, path, visiting)
	}
	return nil
}
