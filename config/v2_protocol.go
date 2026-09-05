package config

import "time"

type AggregationConfig struct {
	Enabled            bool          `yaml:"enabled"`
	MaxDelay           time.Duration `yaml:"max_delay"`
	MaxRecords         int           `yaml:"max_records"`
	MaxQueuedDatagrams int           `yaml:"max_queued_datagrams"`
	MaxQueuedBytes     int           `yaml:"max_queued_bytes"`
	MaxGroupBytes      int           `yaml:"max_group_bytes"`
}

type RepairConfig struct {
	Enabled                    bool          `yaml:"enabled"`
	MaxAge                     time.Duration `yaml:"max_age"`
	MaxAttempts                int           `yaml:"max_attempts"`
	MaxCachedBlocks            int           `yaml:"max_cached_blocks"`
	MaxCachedBytes             int           `yaml:"max_cached_bytes"`
	MaxOutstandingDatagramSpan int           `yaml:"max_outstanding_datagram_span"`
	MaxOutstandingGroupSpan    int           `yaml:"max_outstanding_group_span"`
}

type FastRetransmitConfig struct {
	Enabled   bool `yaml:"enabled"`
	Threshold int  `yaml:"threshold"`
}

type KCPConfig struct {
	FastRetransmit        FastRetransmitConfig `yaml:"fast_retransmit"`
	UpdateInterval        time.Duration        `yaml:"update_interval"`
	SendWindowSegments    int                  `yaml:"send_window_segments"`
	ReceiveWindowSegments int                  `yaml:"receive_window_segments"`
	CongestionControl     bool                 `yaml:"congestion_control"`
}

type StreamMuxConfig struct {
	Enabled               bool          `yaml:"enabled"`
	MaxFrameSize          int           `yaml:"max_frame_size"`
	MaxPendingOpens       int           `yaml:"max_pending_opens"`
	OpenTimeout           time.Duration `yaml:"open_timeout"`
	MaxControlRecordBytes int           `yaml:"max_control_record_bytes"`
	MaxQueuedControlBytes int           `yaml:"max_queued_control_bytes"`
}

func applyV2ProtocolDefaults(c *Config) {
	if c.EffectiveProtocol() == ProtocolDatagram {
		c.Aggregation = AggregationConfig{MaxDelay: 250 * time.Microsecond, MaxRecords: 32, MaxQueuedDatagrams: 256, MaxQueuedBytes: 1 << 20, MaxGroupBytes: 1 << 20}
		c.Repair = RepairConfig{MaxAge: 5 * time.Second, MaxAttempts: 3, MaxCachedBlocks: 1024, MaxCachedBytes: 8 << 20, MaxOutstandingDatagramSpan: 65536, MaxOutstandingGroupSpan: 65536}
	} else if c.EffectiveProtocol() == ProtocolKCP {
		c.KCP = KCPConfig{FastRetransmit: FastRetransmitConfig{Enabled: true, Threshold: 2}, UpdateInterval: 10 * time.Millisecond, SendWindowSegments: 1024, ReceiveWindowSegments: 1024, CongestionControl: true}
		c.StreamMux = StreamMuxConfig{MaxFrameSize: 16384, MaxPendingOpens: 128, OpenTimeout: 5 * time.Second, MaxControlRecordBytes: 256, MaxQueuedControlBytes: 32768}
	}
}

func (c Config) validateV2Protocol() error {
	if c.EffectiveWireVersion() == WireVersionV1 {
		if c.Aggregation != (AggregationConfig{}) || c.Repair != (RepairConfig{}) || c.KCP != (KCPConfig{}) || c.StreamMux != (StreamMuxConfig{}) {
			return invalidf("protocol feature settings require wire.version v2")
		}
		return nil
	}
	if c.EffectiveProtocol() == ProtocolDatagram {
		if c.KCP != (KCPConfig{}) || c.StreamMux != (StreamMuxConfig{}) {
			return invalidf("KCP and mux settings require protocol kcp")
		}
		return c.validateDatagramFeatures()
	}
	if c.Aggregation != (AggregationConfig{}) || c.Repair != (RepairConfig{}) {
		return invalidf("aggregation and repair settings require protocol datagram")
	}
	return c.validateReliableFeatures()
}

func (c Config) validateDatagramFeatures() error {
	a, r := c.Aggregation, c.Repair
	if err := durationRange("aggregation.max_delay", a.MaxDelay, time.Microsecond, 10*time.Millisecond); err != nil {
		return err
	}
	if err := durationRange("repair.max_age", r.MaxAge, 100*time.Millisecond, time.Minute); err != nil {
		return err
	}
	for _, bound := range []struct {
		name            string
		value, min, max int
	}{
		{"aggregation.max_records", a.MaxRecords, 1, 256},
		{"aggregation.max_queued_datagrams", a.MaxQueuedDatagrams, 1, 65536},
		{"aggregation.max_queued_bytes", a.MaxQueuedBytes, 1, c.Limits.MaxSessionRetainedBytes},
		{"aggregation.max_group_bytes", a.MaxGroupBytes, 24, 1 << 24},
		{"repair.max_attempts", r.MaxAttempts, 1, 16},
		{"repair.max_outstanding_datagram_span", r.MaxOutstandingDatagramSpan, 1, 65536},
		{"repair.max_outstanding_group_span", r.MaxOutstandingGroupSpan, 1, 65536},
		{"repair.max_cached_blocks", r.MaxCachedBlocks, 1, r.MaxOutstandingGroupSpan},
		{"repair.max_cached_bytes", r.MaxCachedBytes, 1, c.Limits.MaxSessionRetainedBytes},
	} {
		if err := intRange(bound.name, bound.value, bound.min, bound.max); err != nil {
			return err
		}
	}
	if r.Enabled && (c.Timers.DatagramReassemblyTimeout < r.MaxAge || c.Timers.GroupDecodeTimeout < r.MaxAge) {
		return invalidf("repair.max_age exceeds a receive deadline")
	}
	return nil
}

func (c Config) validateReliableFeatures() error {
	k, m := c.KCP, c.StreamMux
	if err := durationRange("kcp.update_interval", k.UpdateInterval, 10*time.Millisecond, 100*time.Millisecond); err != nil {
		return err
	}
	if err := durationRange("stream_mux.open_timeout", m.OpenTimeout, 100*time.Millisecond, 5*time.Second); err != nil {
		return err
	}
	for _, bound := range []struct {
		name            string
		value, min, max int
	}{
		{"kcp.fast_retransmit.threshold", k.FastRetransmit.Threshold, 1, 255},
		{"kcp.send_window_segments", k.SendWindowSegments, 32, 65535},
		{"kcp.receive_window_segments", k.ReceiveWindowSegments, 32, 65535},
		{"stream_mux.max_frame_size", m.MaxFrameSize, 128, 65535},
		{"stream_mux.max_pending_opens", m.MaxPendingOpens, 1, 128},
		{"stream_mux.max_control_record_bytes", m.MaxControlRecordBytes, 256, 256},
		{"stream_mux.max_queued_control_bytes", m.MaxQueuedControlBytes, 256, min(32768, c.Limits.MaxSessionRetainedBytes)},
	} {
		if err := intRange(bound.name, bound.value, bound.min, bound.max); err != nil {
			return err
		}
	}
	if m.Enabled {
		minimum := int64(262144) + int64(m.MaxFrameSize)
		if int64(c.Limits.MaxStreamRetainedBytes) < minimum || int64(c.Limits.MaxSessionRetainedBytes) < 2*minimum+int64(m.MaxQueuedControlBytes) {
			return invalidf("mux receive credits require control, an initial business window and queued control capacity")
		}
	}
	return nil
}
