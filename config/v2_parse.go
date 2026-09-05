package config

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

type rawV2Transport struct {
	MaxReceiveUDPPayload *yamlInteger     `yaml:"max_receive_udp_payload"`
	MTUDiscovery         *MTUDiscovery    `yaml:"mtu_discovery"`
	BudgetStrategy       *BudgetStrategy  `yaml:"budget_strategy"`
	OutboundPathBudgets  *[]rawPathBudget `yaml:"outbound_path_budgets"`
	InboundPathBudgets   *[]rawPathBudget `yaml:"inbound_path_budgets"`
	PLPMTUD              *rawPLPMTUD      `yaml:"plpmtud"`
	MaxRetainedEpochs    *yamlInteger     `yaml:"max_retained_epochs"`
	MaxEpochAge          *yamlDuration    `yaml:"max_epoch_age"`
	MaxMigrations        *yamlInteger     `yaml:"max_migrations"`
}

type rawPLPMTUD struct {
	BaseUDPPayload        *yamlInteger  `yaml:"base_udp_payload"`
	ProbeInterval         *yamlDuration `yaml:"probe_interval"`
	MaxOutstandingPerPath *yamlInteger  `yaml:"max_outstanding_per_path"`
}

type rawPathBudget struct {
	PathID        *yamlInteger `yaml:"path_id"`
	MaxUDPPayload *yamlInteger `yaml:"max_udp_payload"`
}

type rawV2Limits struct {
	MaxPendingHandshakes         *yamlInteger `yaml:"max_pending_handshakes"`
	MaxPendingAccepts            *yamlInteger `yaml:"max_pending_accepts"`
	MaxPeerRetainedBytes         *yamlInteger `yaml:"max_peer_retained_bytes"`
	MaxSessionRetainedBytes      *yamlInteger `yaml:"max_session_retained_bytes"`
	MaxDatagramReassemblies      *yamlInteger `yaml:"max_datagram_reassemblies"`
	MaxFragmentsPerDatagram      *yamlInteger `yaml:"max_fragments_per_datagram"`
	MaxMigrationTransactionBytes *yamlInteger `yaml:"max_migration_transaction_bytes"`
	MaxStreamsPerSession         *yamlInteger `yaml:"max_streams_per_session"`
	MaxPeerStreams               *yamlInteger `yaml:"max_peer_streams"`
	MaxStreamRetainedBytes       *yamlInteger `yaml:"max_stream_retained_bytes"`
	MaxPathQueuedPackets         *yamlInteger `yaml:"max_path_queued_packets"`
	MaxPathQueuedBytes           *yamlInteger `yaml:"max_path_queued_bytes"`
	MaxSendWorkers               *yamlInteger `yaml:"max_send_workers"`
}

type rawScheduler struct {
	OutboundPathRatesBPS *rawPathRates `yaml:"outbound_path_rates_bps"`
	InboundPathRatesBPS  *rawPathRates `yaml:"inbound_path_rates_bps"`
}

type rawPathRates map[int]int64

func (r *rawPathRates) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode || len(node.Content)%2 != 0 || len(node.Content)/2 > MaxCarriers {
		return fmt.Errorf("path rates must be a bounded integer PathID map")
	}
	rates := make(rawPathRates, len(node.Content)/2)
	for i := 0; i < len(node.Content); i += 2 {
		var path yamlInteger
		if err := path.UnmarshalYAML(node.Content[i]); err != nil {
			return err
		}
		if _, exists := rates[int(path)]; exists {
			return fmt.Errorf("duplicate normalized path rate index")
		}
		value := node.Content[i+1]
		if value.Kind != yaml.ScalarNode || value.Tag != "!!int" {
			return fmt.Errorf("path rate must be an integer")
		}
		var rate int64
		if err := value.Decode(&rate); err != nil {
			return fmt.Errorf("path rate exceeds the integer range")
		}
		rates[int(path)] = rate
	}
	*r = rates
	return nil
}

func (m *MTUDiscovery) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" || (node.Value != string(MTUDiscoveryFixed) && node.Value != string(MTUDiscoveryPLPMTUD)) {
		return fmt.Errorf("transport.mtu_discovery must be fixed or plpmtud")
	}
	*m = MTUDiscovery(node.Value)
	return nil
}

func (b *BudgetStrategy) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" || (node.Value != string(BudgetStrategySession) && node.Value != string(BudgetStrategyPerCarrier)) {
		return fmt.Errorf("transport.budget_strategy must be session or per_carrier")
	}
	*b = BudgetStrategy(node.Value)
	return nil
}

func assignInt(dst *int, src *yamlInteger) {
	if src != nil {
		*dst = int(*src)
	}
}

func assignDuration(dst *time.Duration, src *yamlDuration) {
	if src != nil {
		*dst = src.Duration
	}
}

func applyRawV2(raw rawConfig, c *Config) error {
	if c.EffectiveWireVersion() == WireVersionV1 {
		if raw.Transport.rawV2Transport != (rawV2Transport{}) || raw.Limits.rawV2Limits != (rawV2Limits{}) || raw.Scheduler != nil || raw.Timers.DatagramReassemblyTimeout != nil || raw.Timers.GroupDecodeTimeout != nil {
			return invalidf("v2 settings require wire.version v2")
		}
		return nil
	}
	t := &c.Transport
	t.MaxReceiveUDPPayload = t.MaxUDPPayload
	assignInt(&t.MaxReceiveUDPPayload, raw.Transport.MaxReceiveUDPPayload)
	if raw.Transport.MTUDiscovery != nil {
		t.MTUDiscovery = *raw.Transport.MTUDiscovery
	}
	if raw.Transport.BudgetStrategy != nil {
		t.BudgetStrategy = *raw.Transport.BudgetStrategy
	}
	if t.MTUDiscovery == MTUDiscoveryPLPMTUD {
		t.PLPMTUD = PLPMTUDConfig{BaseUDPPayload: 512, ProbeInterval: time.Second, MaxOutstandingPerPath: 1}
	} else if raw.Transport.PLPMTUD != nil {
		return invalidf("transport.plpmtud requires plpmtud discovery")
	}
	if raw.Transport.PLPMTUD != nil {
		assignInt(&t.PLPMTUD.BaseUDPPayload, raw.Transport.PLPMTUD.BaseUDPPayload)
		assignInt(&t.PLPMTUD.MaxOutstandingPerPath, raw.Transport.PLPMTUD.MaxOutstandingPerPath)
		assignDuration(&t.PLPMTUD.ProbeInterval, raw.Transport.PLPMTUD.ProbeInterval)
	}
	assignInt(&t.MaxRetainedEpochs, raw.Transport.MaxRetainedEpochs)
	assignInt(&t.MaxMigrations, raw.Transport.MaxMigrations)
	assignDuration(&t.MaxEpochAge, raw.Transport.MaxEpochAge)
	var err error
	if t.OutboundPathBudgets, err = decodePathBudgets(raw.Transport.OutboundPathBudgets); err != nil {
		return err
	}
	if t.InboundPathBudgets, err = decodePathBudgets(raw.Transport.InboundPathBudgets); err != nil {
		return err
	}
	if raw.Scheduler != nil {
		if raw.Scheduler.OutboundPathRatesBPS != nil {
			c.Scheduler.OutboundPathRatesBPS = map[int]int64(*raw.Scheduler.OutboundPathRatesBPS)
		}
		if raw.Scheduler.InboundPathRatesBPS != nil {
			c.Scheduler.InboundPathRatesBPS = map[int]int64(*raw.Scheduler.InboundPathRatesBPS)
		}
	}
	l, r := &c.Limits, raw.Limits
	assignInt(&l.MaxPendingHandshakes, r.MaxPendingHandshakes)
	assignInt(&l.MaxPendingAccepts, r.MaxPendingAccepts)
	assignInt(&l.MaxPeerRetainedBytes, r.MaxPeerRetainedBytes)
	assignInt(&l.MaxSessionRetainedBytes, r.MaxSessionRetainedBytes)
	assignInt(&l.MaxDatagramReassemblies, r.MaxDatagramReassemblies)
	assignInt(&l.MaxFragmentsPerDatagram, r.MaxFragmentsPerDatagram)
	assignInt(&l.MaxMigrationTransactionBytes, r.MaxMigrationTransactionBytes)
	assignInt(&l.MaxStreamsPerSession, r.MaxStreamsPerSession)
	assignInt(&l.MaxPeerStreams, r.MaxPeerStreams)
	assignInt(&l.MaxStreamRetainedBytes, r.MaxStreamRetainedBytes)
	assignInt(&l.MaxPathQueuedPackets, r.MaxPathQueuedPackets)
	assignInt(&l.MaxPathQueuedBytes, r.MaxPathQueuedBytes)
	assignInt(&l.MaxSendWorkers, r.MaxSendWorkers)
	assignDuration(&c.Timers.DatagramReassemblyTimeout, raw.Timers.DatagramReassemblyTimeout)
	assignDuration(&c.Timers.GroupDecodeTimeout, raw.Timers.GroupDecodeTimeout)
	return nil
}

func decodePathBudgets(raw *[]rawPathBudget) ([]PathBudget, error) {
	if raw == nil {
		return nil, nil
	}
	if len(*raw) > MaxCarriers {
		return nil, invalidf("too many static path budgets")
	}
	result := make([]PathBudget, len(*raw))
	for i, budget := range *raw {
		if budget.PathID == nil || budget.MaxUDPPayload == nil {
			return nil, invalidf("static path budget requires path_id and max_udp_payload")
		}
		result[i] = PathBudget{PathID: int(*budget.PathID), MaxUDPPayload: int(*budget.MaxUDPPayload)}
	}
	return result, nil
}
