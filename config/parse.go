package config

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"gopkg.in/yaml.v3"
)

type rawConfig struct {
	Protocol  *Protocol    `yaml:"protocol,omitempty"`
	Wire      rawWire      `yaml:"wire,omitempty"`
	Carriers  []string     `yaml:"carriers,omitempty"`
	Listen    string       `yaml:"listen,omitempty"`
	FEC       *rawFEC      `yaml:"fec"`
	PSK       Secret       `yaml:"psk"`
	Transport rawTransport `yaml:"transport,omitempty"`
	Limits    rawLimits    `yaml:"limits,omitempty"`
	Timers    rawTimers    `yaml:"timers,omitempty"`
}

type rawWire struct {
	Version *WireVersion `yaml:"version"`
}

type rawFEC struct {
	DataShards   *yamlInteger `yaml:"data_shards"`
	ParityShards *yamlInteger `yaml:"parity_shards"`
}

type yamlInteger int

func (c *yamlInteger) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!int" {
		return fmt.Errorf("numeric configuration value must be an integer")
	}
	var value int
	if err := node.Decode(&value); err != nil {
		return fmt.Errorf("numeric configuration value exceeds the integer range")
	}
	*c = yamlInteger(value)
	return nil
}

type rawTransport struct {
	MaxUDPPayload *yamlInteger `yaml:"max_udp_payload"`
}

type rawLimits struct {
	MaxDatagramSize        *yamlInteger `yaml:"max_datagram_size"`
	MaxPendingFECBlocks    *yamlInteger `yaml:"max_pending_fec_blocks"`
	ReceiveQueueCapacity   *yamlInteger `yaml:"receive_queue_capacity"`
	DeliveryQueueCapacity  *yamlInteger `yaml:"delivery_queue_capacity"`
	MaxSessions            *yamlInteger `yaml:"max_sessions"`
	MaxEndpointsPerSession *yamlInteger `yaml:"max_endpoints_per_session"`
	MaxHandshakeAttempts   *yamlInteger `yaml:"max_handshake_attempts"`
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

// UnmarshalYAML rejects implicit scalar conversion and explicit empty values.
func (p *Protocol) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return fmt.Errorf("protocol must be a string: datagram or kcp")
	}
	value := Protocol(node.Value)
	if value != ProtocolDatagram && value != ProtocolKCP {
		return fmt.Errorf("protocol must be datagram or kcp")
	}
	*p = value
	return nil
}

// UnmarshalYAML accepts only explicit supported version names, never numbers.
func (v *WireVersion) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return fmt.Errorf("wire.version must be a string: v1 or v2")
	}
	value := WireVersion(node.Value)
	if value != WireVersionV1 && value != WireVersionV2 {
		return fmt.Errorf("wire.version must be v1 or v2")
	}
	*v = value
	return nil
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
	if raw.Protocol != nil {
		cfg.Protocol = *raw.Protocol
	}
	if raw.Wire.Version != nil {
		cfg.Wire.Version = *raw.Wire.Version
	}
	cfg.Carriers = append([]string(nil), raw.Carriers...)
	cfg.Listen = raw.Listen
	cfg.PSK = raw.PSK.clone()
	if raw.FEC != nil {
		if raw.FEC.DataShards != nil {
			cfg.FEC.DataShards = int(*raw.FEC.DataShards)
		}
		if raw.FEC.ParityShards != nil {
			cfg.FEC.ParityShards = int(*raw.FEC.ParityShards)
		}
	}
	if raw.Transport.MaxUDPPayload != nil {
		cfg.Transport.MaxUDPPayload = int(*raw.Transport.MaxUDPPayload)
	}
	if raw.Limits.MaxDatagramSize != nil {
		cfg.Limits.MaxDatagramSize = int(*raw.Limits.MaxDatagramSize)
	}
	if raw.Limits.MaxPendingFECBlocks != nil {
		cfg.Limits.MaxPendingFECBlocks = int(*raw.Limits.MaxPendingFECBlocks)
	}
	if raw.Limits.ReceiveQueueCapacity != nil {
		cfg.Limits.ReceiveQueueCapacity = int(*raw.Limits.ReceiveQueueCapacity)
	}
	if raw.Limits.DeliveryQueueCapacity != nil {
		cfg.Limits.DeliveryQueueCapacity = int(*raw.Limits.DeliveryQueueCapacity)
	}
	if raw.Limits.MaxSessions != nil {
		cfg.Limits.MaxSessions = int(*raw.Limits.MaxSessions)
	}
	if raw.Limits.MaxEndpointsPerSession != nil {
		cfg.Limits.MaxEndpointsPerSession = int(*raw.Limits.MaxEndpointsPerSession)
	}
	if raw.Limits.MaxHandshakeAttempts != nil {
		cfg.Limits.MaxHandshakeAttempts = int(*raw.Limits.MaxHandshakeAttempts)
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
