package config

import "time"

// MaxConfigBytes bounds one complete YAML configuration document.
const MaxConfigBytes = 1 << 20

// FEC limits follow the selected github.com/klauspost/reedsolomon codec.
const (
	MaxTotalShards = 256
	MaxCarriers    = 256
	MaxPSKBytes    = 4096
)

// UDP payload limits are for the complete MPUDP wire packet, not an IP MTU or
// the shard data alone.
const (
	DefaultMaxUDPPayload = 1200
	MinMaxUDPPayload     = 72
	MaxMaxUDPPayload     = 65507
)

// Upper Datagram resource limits are independent of the negotiated wire
// budget. The Session/FEC layer applies the smaller effective limit.
const (
	DefaultMaxDatagramSize = 64 * 1024
	MinMaxDatagramSize     = 1
	MaxMaxDatagramSize     = 16 * 1024 * 1024
)

const (
	DefaultMaxPendingFECBlocks = 1024
	MinMaxPendingFECBlocks     = 1
	MaxMaxPendingFECBlocks     = 65536

	DefaultReceiveQueueCapacity  = 256
	MinReceiveQueueCapacity      = 1
	MaxReceiveQueueCapacity      = 65536
	DefaultDeliveryQueueCapacity = 256
	MinDeliveryQueueCapacity     = 1
	MaxDeliveryQueueCapacity     = 65536

	DefaultMaxSessions            = 1024
	MinMaxSessions                = 1
	MaxMaxSessions                = 65536
	DefaultMaxEndpointsPerSession = 256
	MinMaxEndpointsPerSession     = 1
	MaxMaxEndpointsPerSession     = 256
	DefaultMaxHandshakeAttempts   = 8
	MinMaxHandshakeAttempts       = 1
	MaxMaxHandshakeAttempts       = 64
)

const (
	DefaultDecodeTimeout          = 3 * time.Second
	MinDecodeTimeout              = 100 * time.Millisecond
	MaxDecodeTimeout              = time.Minute
	DefaultEndpointTTL            = 2 * time.Minute
	MinEndpointTTL                = 5 * time.Second
	MaxEndpointTTL                = 24 * time.Hour
	DefaultKeepaliveInterval      = 15 * time.Second
	MinKeepaliveInterval          = time.Second
	MaxKeepaliveInterval          = 5 * time.Minute
	DefaultHandshakeRetryInterval = time.Second
	MinHandshakeRetryInterval     = 100 * time.Millisecond
	MaxHandshakeRetryInterval     = time.Minute
)

// Default returns a Config populated with every optional runtime default.
// Callers constructing a Config in Go must still provide a mode, FEC values,
// and a non-empty PSK before validation.
func Default() Config {
	return Config{
		Transport: TransportConfig{
			MaxUDPPayload: DefaultMaxUDPPayload,
		},
		Limits: LimitsConfig{
			MaxDatagramSize:        DefaultMaxDatagramSize,
			MaxPendingFECBlocks:    DefaultMaxPendingFECBlocks,
			ReceiveQueueCapacity:   DefaultReceiveQueueCapacity,
			DeliveryQueueCapacity:  DefaultDeliveryQueueCapacity,
			MaxSessions:            DefaultMaxSessions,
			MaxEndpointsPerSession: DefaultMaxEndpointsPerSession,
			MaxHandshakeAttempts:   DefaultMaxHandshakeAttempts,
		},
		Timers: TimerConfig{
			DecodeTimeout:          DefaultDecodeTimeout,
			EndpointTTL:            DefaultEndpointTTL,
			KeepaliveInterval:      DefaultKeepaliveInterval,
			HandshakeRetryInterval: DefaultHandshakeRetryInterval,
		},
	}
}
