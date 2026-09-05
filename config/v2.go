package config

import (
	"maps"
	"slices"
	"time"
)

type MTUDiscovery string

const (
	MTUDiscoveryFixed   MTUDiscovery = "fixed"
	MTUDiscoveryPLPMTUD MTUDiscovery = "plpmtud"
)

type BudgetStrategy string

const (
	BudgetStrategySession    BudgetStrategy = "session"
	BudgetStrategyPerCarrier BudgetStrategy = "per_carrier"
)

type PLPMTUDConfig struct {
	BaseUDPPayload        int           `yaml:"base_udp_payload"`
	ProbeInterval         time.Duration `yaml:"probe_interval"`
	MaxOutstandingPerPath int           `yaml:"max_outstanding_per_path"`
}

// PathBudget uses configured Carrier indices, never discovery order.
type PathBudget struct {
	PathID        int `yaml:"path_id"`
	MaxUDPPayload int `yaml:"max_udp_payload"`
}

const DefaultPathRateBPS int64 = 100000000

// SchedulerConfig stores only explicit directional rates. Omitted paths use
// DefaultPathRateBPS; these values are operator settings, not measurements.
type SchedulerConfig struct {
	OutboundPathRatesBPS map[int]int64 `yaml:"outbound_path_rates_bps,omitempty"`
	InboundPathRatesBPS  map[int]int64 `yaml:"inbound_path_rates_bps,omitempty"`
}

func (s SchedulerConfig) OutboundPathRateBPS(pathID int) int64 {
	if rate, ok := s.OutboundPathRatesBPS[pathID]; ok {
		return rate
	}
	return DefaultPathRateBPS
}

func (s SchedulerConfig) InboundPathRateBPS(pathID int) int64 {
	if rate, ok := s.InboundPathRatesBPS[pathID]; ok {
		return rate
	}
	return DefaultPathRateBPS
}

// DefaultV2 returns explicit v2 settings without activating the v2 runtime.
// An empty protocol selects Datagram. Callers still supply roles, PSK and
// positive Datagram FEC. Validate never fills omitted Go struct fields.
func DefaultV2(protocol Protocol) Config {
	c := Default()
	if protocol != "" {
		c.Protocol = protocol
	}
	c.Wire.Version = WireVersionV2
	c.Transport.MaxReceiveUDPPayload = c.Transport.MaxUDPPayload
	c.Transport.MTUDiscovery = MTUDiscoveryFixed
	c.Transport.BudgetStrategy = BudgetStrategySession
	c.Transport.MaxRetainedEpochs = 2
	c.Transport.MaxEpochAge = 5 * time.Second
	c.Transport.MaxMigrations = 2
	c.Limits.MaxPendingHandshakes = 256
	c.Limits.MaxPendingAccepts = 256
	c.Limits.MaxPeerRetainedBytes = 256 << 20
	c.Limits.MaxSessionRetainedBytes = 16 << 20
	c.Limits.MaxDatagramReassemblies = 1024
	c.Limits.MaxFragmentsPerDatagram = 256
	c.Limits.MaxMigrationTransactionBytes = 8 << 20
	c.Limits.MaxStreamsPerSession = 128
	c.Limits.MaxPeerStreams = 4096
	c.Limits.MaxStreamRetainedBytes = 262144 + 16384
	c.Limits.MaxPathQueuedPackets = 256
	c.Limits.MaxPathQueuedBytes = 1 << 20
	c.Limits.MaxSendWorkers = 8
	c.Timers.DatagramReassemblyTimeout = 10 * time.Second
	c.Timers.GroupDecodeTimeout = 10 * time.Second
	applyV2ProtocolDefaults(&c)
	return c
}

func cloneV2(c *Config) {
	c.Transport.OutboundPathBudgets = slices.Clone(c.Transport.OutboundPathBudgets)
	c.Transport.InboundPathBudgets = slices.Clone(c.Transport.InboundPathBudgets)
	c.Scheduler.OutboundPathRatesBPS = maps.Clone(c.Scheduler.OutboundPathRatesBPS)
	c.Scheduler.InboundPathRatesBPS = maps.Clone(c.Scheduler.InboundPathRatesBPS)
}

func (c Config) validateV2() error {
	if c.EffectiveWireVersion() == WireVersionV1 {
		t := c.Transport
		if t.MaxReceiveUDPPayload != 0 || t.MTUDiscovery != "" || t.BudgetStrategy != "" || t.OutboundPathBudgets != nil || t.InboundPathBudgets != nil || t.PLPMTUD != (PLPMTUDConfig{}) || t.MaxRetainedEpochs != 0 || t.MaxEpochAge != 0 || t.MaxMigrations != 0 || c.Scheduler.OutboundPathRatesBPS != nil || c.Scheduler.InboundPathRatesBPS != nil || c.hasV2Limits() || c.Timers.DatagramReassemblyTimeout != 0 || c.Timers.GroupDecodeTimeout != 0 {
			return invalidf("v2 settings require wire.version v2")
		}
		return c.validateV2Protocol()
	}
	t := c.Transport
	if err := intRange("transport.max_receive_udp_payload", t.MaxReceiveUDPPayload, 512, 65507); err != nil {
		return err
	}
	if t.MTUDiscovery != MTUDiscoveryFixed && t.MTUDiscovery != MTUDiscoveryPLPMTUD {
		return invalidf("transport.mtu_discovery must be fixed or plpmtud")
	}
	if t.BudgetStrategy != BudgetStrategySession && t.BudgetStrategy != BudgetStrategyPerCarrier {
		return invalidf("transport.budget_strategy must be session or per_carrier")
	}
	if t.MTUDiscovery == MTUDiscoveryFixed {
		if t.PLPMTUD != (PLPMTUDConfig{}) {
			return invalidf("transport.plpmtud requires plpmtud discovery")
		}
	} else {
		if t.PLPMTUD.BaseUDPPayload != 512 || t.PLPMTUD.MaxOutstandingPerPath != 1 {
			return invalidf("transport.plpmtud requires base_udp_payload 512 and max_outstanding_per_path 1")
		}
		if err := durationRange("transport.plpmtud.probe_interval", t.PLPMTUD.ProbeInterval, 100*time.Millisecond, time.Minute); err != nil {
			return err
		}
	}
	if err := intRange("transport.max_retained_epochs", t.MaxRetainedEpochs, 1, 8); err != nil {
		return err
	}
	if err := intRange("transport.max_migrations", t.MaxMigrations, 1, 2); err != nil {
		return err
	}
	if err := durationRange("transport.max_epoch_age", t.MaxEpochAge, 100*time.Millisecond, time.Minute); err != nil {
		return err
	}
	if err := c.validateV2Limits(); err != nil {
		return err
	}
	if err := durationRange("timers.datagram_reassembly_timeout", c.Timers.DatagramReassemblyTimeout, 100*time.Millisecond, time.Minute); err != nil {
		return err
	}
	if err := durationRange("timers.group_decode_timeout", c.Timers.GroupDecodeTimeout, 100*time.Millisecond, time.Minute); err != nil {
		return err
	}
	if err := c.validateV2Paths(); err != nil {
		return err
	}
	return c.validateV2Protocol()
}

func (c Config) hasV2Limits() bool {
	l := c.Limits
	return l.MaxPendingHandshakes != 0 || l.MaxPendingAccepts != 0 || l.MaxPeerRetainedBytes != 0 || l.MaxSessionRetainedBytes != 0 || l.MaxDatagramReassemblies != 0 || l.MaxFragmentsPerDatagram != 0 || l.MaxMigrationTransactionBytes != 0 || l.MaxStreamsPerSession != 0 || l.MaxPeerStreams != 0 || l.MaxStreamRetainedBytes != 0 || l.MaxPathQueuedPackets != 0 || l.MaxPathQueuedBytes != 0 || l.MaxSendWorkers != 0
}

func (c Config) validateV2Limits() error {
	l := c.Limits
	for _, bound := range []struct {
		name            string
		value, min, max int
	}{
		{"max_pending_handshakes", l.MaxPendingHandshakes, 1, 4096},
		{"max_pending_accepts", l.MaxPendingAccepts, 1, 65536},
		{"max_peer_retained_bytes", l.MaxPeerRetainedBytes, 1 << 20, 1 << 30},
		{"max_session_retained_bytes", l.MaxSessionRetainedBytes, 1 << 20, l.MaxPeerRetainedBytes},
		{"max_datagram_reassemblies", l.MaxDatagramReassemblies, 1, 65536},
		{"max_fragments_per_datagram", l.MaxFragmentsPerDatagram, 1, 4096},
		{"max_migration_transaction_bytes", l.MaxMigrationTransactionBytes, 1, min(8<<20, l.MaxSessionRetainedBytes)},
		{"max_streams_per_session", l.MaxStreamsPerSession, 1, 4096},
		{"max_peer_streams", l.MaxPeerStreams, 1, 65536},
		{"max_stream_retained_bytes", l.MaxStreamRetainedBytes, 1, l.MaxSessionRetainedBytes},
		{"max_path_queued_packets", l.MaxPathQueuedPackets, 1, 4096},
		{"max_path_queued_bytes", l.MaxPathQueuedBytes, 512, l.MaxSessionRetainedBytes},
		{"max_send_workers", l.MaxSendWorkers, 1, 32},
	} {
		if err := intRange("limits."+bound.name, bound.value, bound.min, bound.max); err != nil {
			return err
		}
	}
	return nil
}

func (c Config) validateV2Paths() error {
	t := c.Transport
	static := t.MTUDiscovery == MTUDiscoveryFixed && t.BudgetStrategy == BudgetStrategyPerCarrier
	if !static && (t.OutboundPathBudgets != nil || t.InboundPathBudgets != nil) {
		return invalidf("static path budgets require fixed/per_carrier strategy")
	}
	if !c.InitiatorEnabled() && (t.OutboundPathBudgets != nil || c.Scheduler.OutboundPathRatesBPS != nil) {
		return invalidf("outbound path settings require an initiator role")
	}
	if !c.ListenerEnabled() && (t.InboundPathBudgets != nil || c.Scheduler.InboundPathRatesBPS != nil) {
		return invalidf("inbound path settings require a listener role")
	}
	inboundCount := c.Limits.MaxEndpointsPerSession
	if static {
		if c.InitiatorEnabled() {
			if err := validatePathBudgets(t.OutboundPathBudgets, len(c.Carriers), t.MaxUDPPayload); err != nil {
				return err
			}
		}
		if c.ListenerEnabled() {
			inboundCount = len(t.InboundPathBudgets)
			if inboundCount < 1 || inboundCount > c.Limits.MaxEndpointsPerSession {
				return invalidf("inbound path budget count exceeds endpoint limits")
			}
			if err := validatePathBudgets(t.InboundPathBudgets, inboundCount, t.MaxUDPPayload); err != nil {
				return err
			}
		}
	}
	if err := validatePathRates(c.Scheduler.OutboundPathRatesBPS, len(c.Carriers)); err != nil {
		return err
	}
	return validatePathRates(c.Scheduler.InboundPathRatesBPS, inboundCount)
}

func validatePathBudgets(budgets []PathBudget, count, hardCap int) error {
	if len(budgets) != count || count < 1 || count > MaxCarriers {
		return invalidf("static path budgets must cover configured directional indices")
	}
	var seen [MaxCarriers]bool
	for _, budget := range budgets {
		if budget.PathID < 1 || budget.PathID > count || seen[budget.PathID-1] {
			return invalidf("static path budget indices must be unique and contiguous")
		}
		if err := intRange("path budget max_udp_payload", budget.MaxUDPPayload, 512, hardCap); err != nil {
			return err
		}
		seen[budget.PathID-1] = true
	}
	return nil
}

func validatePathRates(rates map[int]int64, count int) error {
	if len(rates) > count {
		return invalidf("path rates exceed configured directional indices")
	}
	for path, rate := range rates {
		if path < 1 || path > count {
			return invalidf("path rate index exceeds configured directional indices")
		}
		if rate < 1000 || rate > 1000000000000 {
			return invalidf("path rates must be in [1000, 1000000000000] bits/s")
		}
	}
	return nil
}
