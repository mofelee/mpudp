// Package config defines MPUDP's strict, side-effect-free configuration model.
package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/klauspost/reedsolomon"
)

// ErrInvalidConfig classifies every configuration decoding and validation
// failure. Callers should use errors.Is rather than matching error text.
var ErrInvalidConfig = errors.New("invalid MPUDP configuration")

// Protocol selects Datagram or reliable KCP framing independently of Peer roles.
type Protocol string

const (
	ProtocolDatagram Protocol = "datagram"
	ProtocolKCP      Protocol = "kcp"
)

// WireVersion selects a protocol incarnation without permitting downgrade.
type WireVersion string

const (
	WireVersionV1 WireVersion = "v1"
	WireVersionV2 WireVersion = "v2"
)

// WireConfig selects the wire version. Recognition does not imply that the
// runtime implements that version.
type WireConfig struct {
	Version WireVersion `yaml:"version"`
}

// Config is the strict configuration model. V2 selections can be validated
// before the runtime implements them.
// Carriers are remote UDP entries; they are never local bind addresses.
type Config struct {
	Protocol    Protocol          `yaml:"protocol,omitempty"`
	Wire        WireConfig        `yaml:"wire,omitempty"`
	Carriers    []string          `yaml:"carriers,omitempty"`
	Listen      string            `yaml:"listen,omitempty"`
	FEC         FECConfig         `yaml:"fec"`
	Aggregation AggregationConfig `yaml:"aggregation,omitempty"`
	Repair      RepairConfig      `yaml:"repair,omitempty"`
	KCP         KCPConfig         `yaml:"kcp,omitempty"`
	StreamMux   StreamMuxConfig   `yaml:"stream_mux,omitempty"`
	PSK         Secret            `yaml:"psk"`
	Transport   TransportConfig   `yaml:"transport,omitempty"`
	Scheduler   SchedulerConfig   `yaml:"scheduler,omitempty"`
	Limits      LimitsConfig      `yaml:"limits,omitempty"`
	Timers      TimerConfig       `yaml:"timers,omitempty"`
}

// FECConfig selects k data shards and r parity shards.
type FECConfig struct {
	DataShards   int `yaml:"data_shards"`
	ParityShards int `yaml:"parity_shards"`
}

// TransportConfig owns limits that apply to complete UDP payloads.
type TransportConfig struct {
	MaxUDPPayload        int            `yaml:"max_udp_payload"`
	MaxReceiveUDPPayload int            `yaml:"max_receive_udp_payload,omitempty"`
	MTUDiscovery         MTUDiscovery   `yaml:"mtu_discovery,omitempty"`
	BudgetStrategy       BudgetStrategy `yaml:"budget_strategy,omitempty"`
	OutboundPathBudgets  []PathBudget   `yaml:"outbound_path_budgets,omitempty"`
	InboundPathBudgets   []PathBudget   `yaml:"inbound_path_budgets,omitempty"`
	PLPMTUD              PLPMTUDConfig  `yaml:"plpmtud,omitempty"`
	MaxRetainedEpochs    int            `yaml:"max_retained_epochs,omitempty"`
	MaxEpochAge          time.Duration  `yaml:"max_epoch_age,omitempty"`
	MaxMigrations        int            `yaml:"max_migrations,omitempty"`
}

// LimitsConfig owns bounded memory and queue resource settings.
type LimitsConfig struct {
	MaxDatagramSize              int `yaml:"max_datagram_size"`
	MaxPendingFECBlocks          int `yaml:"max_pending_fec_blocks"`
	ReceiveQueueCapacity         int `yaml:"receive_queue_capacity"`
	DeliveryQueueCapacity        int `yaml:"delivery_queue_capacity"`
	MaxSessions                  int `yaml:"max_sessions"`
	MaxEndpointsPerSession       int `yaml:"max_endpoints_per_session"`
	MaxHandshakeAttempts         int `yaml:"max_handshake_attempts"`
	MaxPendingHandshakes         int `yaml:"max_pending_handshakes,omitempty"`
	MaxPendingAccepts            int `yaml:"max_pending_accepts,omitempty"`
	MaxPeerRetainedBytes         int `yaml:"max_peer_retained_bytes,omitempty"`
	MaxSessionRetainedBytes      int `yaml:"max_session_retained_bytes,omitempty"`
	MaxDatagramReassemblies      int `yaml:"max_datagram_reassemblies,omitempty"`
	MaxFragmentsPerDatagram      int `yaml:"max_fragments_per_datagram,omitempty"`
	MaxMigrationTransactionBytes int `yaml:"max_migration_transaction_bytes,omitempty"`
	MaxStreamsPerSession         int `yaml:"max_streams_per_session,omitempty"`
	MaxPeerStreams               int `yaml:"max_peer_streams,omitempty"`
	MaxStreamRetainedBytes       int `yaml:"max_stream_retained_bytes,omitempty"`
	MaxPathQueuedPackets         int `yaml:"max_path_queued_packets,omitempty"`
	MaxPathQueuedBytes           int `yaml:"max_path_queued_bytes,omitempty"`
	MaxSendWorkers               int `yaml:"max_send_workers,omitempty"`
}

// TimerConfig owns protocol timing settings. YAML values use Go duration
// strings such as "500ms", "15s", or "2m".
type TimerConfig struct {
	DecodeTimeout             time.Duration `yaml:"decode_timeout"`
	EndpointTTL               time.Duration `yaml:"endpoint_ttl"`
	KeepaliveInterval         time.Duration `yaml:"keepalive_interval"`
	HandshakeRetryInterval    time.Duration `yaml:"handshake_retry_interval"`
	DatagramReassemblyTimeout time.Duration `yaml:"datagram_reassembly_timeout,omitempty"`
	GroupDecodeTimeout        time.Duration `yaml:"group_decode_timeout,omitempty"`
}

// EffectiveProtocol preserves Datagram behavior for Go Config literals that
// predate the Protocol field. It does not mutate the configuration.
func (c Config) EffectiveProtocol() Protocol {
	if c.Protocol == "" {
		return ProtocolDatagram
	}
	return c.Protocol
}

// EffectiveWireVersion preserves v1 behavior for Go Config literals that
// predate Wire.Version. Explicit empty YAML values are rejected by Parse.
func (c Config) EffectiveWireVersion() WireVersion {
	if c.Wire.Version == "" {
		return WireVersionV1
	}
	return c.Wire.Version
}

// Validate checks Config without opening sockets or starting background work.
// It checks recognized configurations, not runtime protocol availability.
func (c Config) Validate() error {
	protocol, version := c.EffectiveProtocol(), c.EffectiveWireVersion()
	if protocol != ProtocolDatagram && protocol != ProtocolKCP {
		return invalidf("protocol must be datagram or kcp")
	}
	if version != WireVersionV1 && version != WireVersionV2 {
		return invalidf("wire.version must be v1 or v2")
	}
	if version == WireVersionV1 && protocol == ProtocolKCP {
		return invalidf("protocol kcp requires wire.version v2")
	}
	if len(c.Carriers) == 0 && c.Listen == "" {
		return invalidf("at least one of carriers or listen is required")
	}
	if len(c.Carriers) > MaxCarriers {
		return invalidf("carriers has %d entries; maximum is %d", len(c.Carriers), MaxCarriers)
	}

	seen := make(map[string]struct{}, len(c.Carriers))
	for i, carrier := range c.Carriers {
		key, err := validateAddress(carrier, false)
		if err != nil {
			return invalidf("carriers[%d]: %v", i, err)
		}
		if _, ok := seen[key]; ok {
			return invalidf("carriers[%d] duplicates an earlier remote address", i)
		}
		seen[key] = struct{}{}
	}
	if c.Listen != "" {
		if _, err := validateAddress(c.Listen, true); err != nil {
			return invalidf("listen: %v", err)
		}
	}

	if protocol == ProtocolKCP {
		if c.FEC.DataShards != 0 || c.FEC.ParityShards != 0 {
			return invalidf("fec.data_shards and fec.parity_shards must be zero for protocol kcp")
		}
	} else {
		if c.FEC.DataShards <= 0 {
			return invalidf("fec.data_shards must be greater than zero")
		}
		if c.FEC.ParityShards <= 0 {
			return invalidf("fec.parity_shards must be greater than zero")
		}
		if c.FEC.DataShards > MaxTotalShards-c.FEC.ParityShards {
			return invalidf("fec.data_shards + fec.parity_shards must not exceed %d", MaxTotalShards)
		}
		if _, err := reedsolomon.New(c.FEC.DataShards, c.FEC.ParityShards); err != nil {
			return invalidf("fec parameters are unsupported by the selected Reed-Solomon codec: %v", err)
		}
	}

	if c.PSK.Len() == 0 {
		return invalidf("psk must not be empty")
	}
	if c.PSK.Len() > MaxPSKBytes {
		return invalidf("psk exceeds the maximum length of %d bytes", MaxPSKBytes)
	}

	minPayload := MinMaxUDPPayload
	if version == WireVersionV2 {
		minPayload = MinV2MaxUDPPayload
	}
	if err := intRange("transport.max_udp_payload", c.Transport.MaxUDPPayload, minPayload, MaxMaxUDPPayload); err != nil {
		return err
	}
	if err := intRange("limits.max_datagram_size", c.Limits.MaxDatagramSize, MinMaxDatagramSize, MaxMaxDatagramSize); err != nil {
		return err
	}
	if err := intRange("limits.max_pending_fec_blocks", c.Limits.MaxPendingFECBlocks, MinMaxPendingFECBlocks, MaxMaxPendingFECBlocks); err != nil {
		return err
	}
	if err := intRange("limits.receive_queue_capacity", c.Limits.ReceiveQueueCapacity, MinReceiveQueueCapacity, MaxReceiveQueueCapacity); err != nil {
		return err
	}
	if err := intRange("limits.delivery_queue_capacity", c.Limits.DeliveryQueueCapacity, MinDeliveryQueueCapacity, MaxDeliveryQueueCapacity); err != nil {
		return err
	}
	if err := intRange("limits.max_sessions", c.Limits.MaxSessions, MinMaxSessions, MaxMaxSessions); err != nil {
		return err
	}
	if err := intRange("limits.max_endpoints_per_session", c.Limits.MaxEndpointsPerSession, MinMaxEndpointsPerSession, MaxMaxEndpointsPerSession); err != nil {
		return err
	}
	if err := intRange("limits.max_handshake_attempts", c.Limits.MaxHandshakeAttempts, MinMaxHandshakeAttempts, MaxMaxHandshakeAttempts); err != nil {
		return err
	}
	if err := durationRange("timers.decode_timeout", c.Timers.DecodeTimeout, MinDecodeTimeout, MaxDecodeTimeout); err != nil {
		return err
	}
	if err := durationRange("timers.endpoint_ttl", c.Timers.EndpointTTL, MinEndpointTTL, MaxEndpointTTL); err != nil {
		return err
	}
	if err := durationRange("timers.keepalive_interval", c.Timers.KeepaliveInterval, MinKeepaliveInterval, MaxKeepaliveInterval); err != nil {
		return err
	}
	if err := durationRange("timers.handshake_retry_interval", c.Timers.HandshakeRetryInterval, MinHandshakeRetryInterval, MaxHandshakeRetryInterval); err != nil {
		return err
	}
	return c.validateV2()
}

// Clone returns a deep copy suitable for retaining beyond the call boundary.
func (c Config) Clone() Config {
	cloned := c
	cloned.Carriers = append([]string(nil), c.Carriers...)
	cloned.PSK = c.PSK.clone()
	cloneV2(&cloned)
	return cloned
}

// InitiatorEnabled reports whether this configuration can create an outbound
// Session.
func (c Config) InitiatorEnabled() bool {
	return len(c.Carriers) != 0
}

// ListenerEnabled reports whether this configuration can accept Sessions.
func (c Config) ListenerEnabled() bool {
	return c.Listen != ""
}

// String renders configuration metadata without revealing the PSK.
func (c Config) String() string {
	return fmt.Sprintf("Config{carriers:%d listen:%q fec:%d+%d psk:%s max_udp_payload:%d}",
		len(c.Carriers), c.Listen, c.FEC.DataShards, c.FEC.ParityShards, redactedSecret, c.Transport.MaxUDPPayload)
}

// GoString renders configuration metadata without revealing the PSK.
func (c Config) GoString() string {
	return c.String()
}

func intRange(name string, value, min, max int) error {
	if value < min || value > max {
		return invalidf("%s must be in [%d, %d]", name, min, max)
	}
	return nil
}

func durationRange(name string, value, min, max time.Duration) error {
	if value < min || value > max {
		return invalidf("%s must be in [%s, %s]", name, min, max)
	}
	return nil
}

func invalidf(format string, args ...any) error {
	detail := fmt.Sprintf(format, args...)
	detail = strings.ReplaceAll(detail, "\n", " ")
	return fmt.Errorf("%w: %s", ErrInvalidConfig, detail)
}
