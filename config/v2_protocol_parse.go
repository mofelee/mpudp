package config

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

type yamlBoolean bool

func (b *yamlBoolean) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!bool" {
		return fmt.Errorf("boolean configuration value must be true or false")
	}
	var value bool
	if err := node.Decode(&value); err != nil {
		return fmt.Errorf("boolean configuration value must be true or false")
	}
	*b = yamlBoolean(value)
	return nil
}

type rawAggregation struct {
	Enabled            *yamlBoolean  `yaml:"enabled"`
	MaxDelay           *yamlDuration `yaml:"max_delay"`
	MaxRecords         *yamlInteger  `yaml:"max_records"`
	MaxQueuedDatagrams *yamlInteger  `yaml:"max_queued_datagrams"`
	MaxQueuedBytes     *yamlInteger  `yaml:"max_queued_bytes"`
	MaxGroupBytes      *yamlInteger  `yaml:"max_group_bytes"`
}

type rawRepair struct {
	Enabled                    *yamlBoolean  `yaml:"enabled"`
	MaxAge                     *yamlDuration `yaml:"max_age"`
	MaxAttempts                *yamlInteger  `yaml:"max_attempts"`
	MaxCachedBlocks            *yamlInteger  `yaml:"max_cached_blocks"`
	MaxCachedBytes             *yamlInteger  `yaml:"max_cached_bytes"`
	MaxOutstandingDatagramSpan *yamlInteger  `yaml:"max_outstanding_datagram_span"`
	MaxOutstandingGroupSpan    *yamlInteger  `yaml:"max_outstanding_group_span"`
}

type rawFastRetransmit struct {
	Enabled   *yamlBoolean `yaml:"enabled"`
	Threshold *yamlInteger `yaml:"threshold"`
}

type rawKCP struct {
	FastRetransmit        *rawFastRetransmit `yaml:"fast_retransmit"`
	UpdateInterval        *yamlDuration      `yaml:"update_interval"`
	SendWindowSegments    *yamlInteger       `yaml:"send_window_segments"`
	ReceiveWindowSegments *yamlInteger       `yaml:"receive_window_segments"`
	CongestionControl     *yamlBoolean       `yaml:"congestion_control"`
}

type rawStreamMux struct {
	Enabled               *yamlBoolean  `yaml:"enabled"`
	MaxFrameSize          *yamlInteger  `yaml:"max_frame_size"`
	MaxPendingOpens       *yamlInteger  `yaml:"max_pending_opens"`
	OpenTimeout           *yamlDuration `yaml:"open_timeout"`
	MaxControlRecordBytes *yamlInteger  `yaml:"max_control_record_bytes"`
	MaxQueuedControlBytes *yamlInteger  `yaml:"max_queued_control_bytes"`
}

func assignBool(dst *bool, src *yamlBoolean) {
	if src != nil {
		*dst = bool(*src)
	}
}

func applyRawV2Protocol(raw rawConfig, c *Config) error {
	datagram := c.EffectiveWireVersion() == WireVersionV2 && c.EffectiveProtocol() == ProtocolDatagram
	reliable := c.EffectiveWireVersion() == WireVersionV2 && c.EffectiveProtocol() == ProtocolKCP
	if raw.Aggregation != nil {
		r := *raw.Aggregation
		if !datagram {
			enabled := r.Enabled
			r.Enabled = nil
			if enabled == nil || bool(*enabled) || r != (rawAggregation{}) {
				return invalidf("aggregation settings require v2 Datagram or bare enabled: false")
			}
		} else {
			a := &c.Aggregation
			assignBool(&a.Enabled, r.Enabled)
			assignDuration(&a.MaxDelay, r.MaxDelay)
			assignInt(&a.MaxRecords, r.MaxRecords)
			assignInt(&a.MaxQueuedDatagrams, r.MaxQueuedDatagrams)
			assignInt(&a.MaxQueuedBytes, r.MaxQueuedBytes)
			assignInt(&a.MaxGroupBytes, r.MaxGroupBytes)
		}
	}
	if raw.Repair != nil {
		r := *raw.Repair
		if !datagram {
			enabled := r.Enabled
			r.Enabled = nil
			if enabled == nil || bool(*enabled) || r != (rawRepair{}) {
				return invalidf("repair settings require v2 Datagram or bare enabled: false")
			}
		} else {
			v := &c.Repair
			assignBool(&v.Enabled, r.Enabled)
			assignDuration(&v.MaxAge, r.MaxAge)
			assignInt(&v.MaxAttempts, r.MaxAttempts)
			assignInt(&v.MaxCachedBlocks, r.MaxCachedBlocks)
			assignInt(&v.MaxCachedBytes, r.MaxCachedBytes)
			assignInt(&v.MaxOutstandingDatagramSpan, r.MaxOutstandingDatagramSpan)
			assignInt(&v.MaxOutstandingGroupSpan, r.MaxOutstandingGroupSpan)
		}
	}
	if raw.KCP != nil {
		if !reliable {
			return invalidf("kcp settings require protocol kcp and wire.version v2")
		}
		r, v := raw.KCP, &c.KCP
		if r.FastRetransmit != nil {
			assignBool(&v.FastRetransmit.Enabled, r.FastRetransmit.Enabled)
			assignInt(&v.FastRetransmit.Threshold, r.FastRetransmit.Threshold)
		}
		assignDuration(&v.UpdateInterval, r.UpdateInterval)
		assignInt(&v.SendWindowSegments, r.SendWindowSegments)
		assignInt(&v.ReceiveWindowSegments, r.ReceiveWindowSegments)
		assignBool(&v.CongestionControl, r.CongestionControl)
	}
	if raw.StreamMux != nil {
		r := *raw.StreamMux
		if !reliable {
			enabled := r.Enabled
			r.Enabled = nil
			if enabled == nil || bool(*enabled) || r != (rawStreamMux{}) {
				return invalidf("stream_mux settings require v2 KCP or bare enabled: false")
			}
		} else {
			v := &c.StreamMux
			assignBool(&v.Enabled, r.Enabled)
			assignInt(&v.MaxFrameSize, r.MaxFrameSize)
			assignInt(&v.MaxPendingOpens, r.MaxPendingOpens)
			assignDuration(&v.OpenTimeout, r.OpenTimeout)
			assignInt(&v.MaxControlRecordBytes, r.MaxControlRecordBytes)
			assignInt(&v.MaxQueuedControlBytes, r.MaxQueuedControlBytes)
		}
	}
	if reliable && raw.Limits.MaxStreamRetainedBytes == nil {
		if err := intRange("stream_mux.max_frame_size", c.StreamMux.MaxFrameSize, 128, 65535); err != nil {
			return err
		}
		c.Limits.MaxStreamRetainedBytes = 262144 + c.StreamMux.MaxFrameSize
	}
	return nil
}
