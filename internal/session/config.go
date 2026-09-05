package session

import (
	"fmt"
	"time"

	runtimeconfig "github.com/mofelee/mpudp/config"
	"github.com/mofelee/mpudp/internal/fec"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wire"
)

const maxPaths = 256

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type settings struct {
	psk                    []byte
	params                 fec.Params
	localMaxUDPPayload     int
	maxDatagramSize        int
	maxEndpoints           int
	maxHandshakeAttempts   int
	maxPendingFECBlocks    int
	maxCompletedFECBlocks  int
	decodeTimeout          time.Duration
	completionTTL          time.Duration
	endpointTTL            time.Duration
	keepaliveInterval      time.Duration
	handshakeRetryInterval time.Duration
	handshakeJitterLimit   time.Duration
	clock                  Clock
	fecStatistics          *fec.Counters
	listenerPathStatistics *transport.ListenerPathCounters
}

func normalizeConfig(config Config) (*settings, error) {
	if len(config.PSK) == 0 || len(config.PSK) > runtimeconfig.MaxPSKBytes {
		return nil, invalidConfig("PSK length is outside the configured range")
	}
	if config.FEC.DataShards <= 0 || config.FEC.DataShards > 255 ||
		config.FEC.ParityShards <= 0 || config.FEC.ParityShards > 255 ||
		config.FEC.DataShards > fec.MaxTotalShards-config.FEC.ParityShards {
		return nil, invalidConfig("FEC parameters are outside the v0.1 range")
	}
	if config.LocalMaxUDPPayload < wire.MinUDPPayload || config.LocalMaxUDPPayload > wire.MaxUDPPayload {
		return nil, invalidConfig("local max UDP payload is outside the wire range")
	}
	if config.MaxDatagramSize < runtimeconfig.MinMaxDatagramSize || config.MaxDatagramSize > runtimeconfig.MaxMaxDatagramSize {
		return nil, invalidConfig("max Datagram size is outside the configured range")
	}
	if config.MaxEndpoints <= 0 || config.MaxEndpoints > maxPaths {
		return nil, invalidConfig("max Endpoints must be in [1, 256]")
	}
	if config.MaxHandshakeAttempts <= 0 || config.MaxHandshakeAttempts > 64 {
		return nil, invalidConfig("max handshake attempts must be in [1, 64]")
	}
	if config.MaxPendingFECBlocks < runtimeconfig.MinMaxPendingFECBlocks || config.MaxPendingFECBlocks > runtimeconfig.MaxMaxPendingFECBlocks ||
		config.MaxCompletedFECBlocks < runtimeconfig.MinMaxPendingFECBlocks || config.MaxCompletedFECBlocks > runtimeconfig.MaxMaxPendingFECBlocks {
		return nil, invalidConfig("FEC block limits are outside the configured range")
	}
	if config.DecodeTimeout < runtimeconfig.MinDecodeTimeout || config.DecodeTimeout > runtimeconfig.MaxDecodeTimeout ||
		config.CompletionTTL <= 0 ||
		config.EndpointTTL < runtimeconfig.MinEndpointTTL || config.EndpointTTL > runtimeconfig.MaxEndpointTTL ||
		config.KeepaliveInterval < runtimeconfig.MinKeepaliveInterval || config.KeepaliveInterval > runtimeconfig.MaxKeepaliveInterval ||
		config.HandshakeRetryInterval < runtimeconfig.MinHandshakeRetryInterval || config.HandshakeRetryInterval > runtimeconfig.MaxHandshakeRetryInterval {
		return nil, invalidConfig("Session durations are outside the configured range")
	}
	jitter := config.HandshakeRetryJitterLimit
	if jitter == 0 {
		jitter = config.HandshakeRetryInterval / 4
	}
	if jitter < 0 || jitter > config.HandshakeRetryInterval {
		return nil, invalidConfig("handshake retry jitter must be no greater than the retry interval")
	}
	clock := config.Clock
	if clock == nil {
		clock = realClock{}
	}
	return &settings{
		psk:                    append([]byte(nil), config.PSK...),
		params:                 config.FEC,
		localMaxUDPPayload:     config.LocalMaxUDPPayload,
		maxDatagramSize:        config.MaxDatagramSize,
		maxEndpoints:           config.MaxEndpoints,
		maxHandshakeAttempts:   config.MaxHandshakeAttempts,
		maxPendingFECBlocks:    config.MaxPendingFECBlocks,
		maxCompletedFECBlocks:  config.MaxCompletedFECBlocks,
		decodeTimeout:          config.DecodeTimeout,
		completionTTL:          config.CompletionTTL,
		endpointTTL:            config.EndpointTTL,
		keepaliveInterval:      config.KeepaliveInterval,
		handshakeRetryInterval: config.HandshakeRetryInterval,
		handshakeJitterLimit:   jitter,
		clock:                  clock,
		fecStatistics:          config.FECStatistics,
		listenerPathStatistics: config.ListenerPathStatistics,
	}, nil
}

func invalidConfig(detail string) error {
	return fmt.Errorf("%w: %s", ErrInvalidConfig, detail)
}
