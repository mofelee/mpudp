package mpudp

import (
	"fmt"
	"runtime"
	"time"

	"github.com/mofelee/mpudp/config"
	"github.com/mofelee/mpudp/internal/aggregationv2"
	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/fecv2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/reassemblyv2"
	"github.com/mofelee/mpudp/internal/recvwindow"
	"github.com/mofelee/mpudp/internal/sessionv2"
)

// The first runtime requires Linux destination-address replies and PMTU
// enforcement. Recognition of other valid v2 settings does not activate them.
func supportedV2Config(cfg config.Config) bool {
	return runtime.GOOS == "linux" && cfg.EffectiveWireVersion() == config.WireVersionV2 &&
		cfg.EffectiveProtocol() == config.ProtocolDatagram && !cfg.Repair.Enabled &&
		cfg.Transport.MTUDiscovery == config.MTUDiscoveryFixed && cfg.Transport.BudgetStrategy == config.BudgetStrategySession &&
		!cfg.StreamMux.Enabled
}

// v2ControllerConfig builds a socket-free template. The runtime fills only the
// Carrier, reply-path, entropy and emission fields after admission succeeds.
func v2ControllerConfig(cfg config.Config, responder bool) (sessionv2.Config, error) {
	if err := cfg.Validate(); err != nil {
		return sessionv2.Config{}, err
	}
	if !supportedV2Config(cfg) || (responder && !cfg.ListenerEnabled()) || (!responder && !cfg.InitiatorEnabled()) {
		return sessionv2.Config{}, fmt.Errorf("%w: unsupported v2 controller role or policy", ErrInvalidConfig)
	}
	maxPaths, rates := len(cfg.Carriers), cfg.Scheduler.OutboundPathRatesBPS
	if responder {
		maxPaths, rates = cfg.Limits.MaxEndpointsPerSession, cfg.Scheduler.InboundPathRatesBPS
	}
	profile := negotiationv2.Profile{
		Protocol: negotiationv2.Datagram, LayoutID: 1,
		OfferedCaps:  negotiationv2.FragmentManifest | negotiationv2.Aggregation,
		RequiredCaps: negotiationv2.FragmentManifest,
		DataShards:   uint8(cfg.FEC.DataShards), ParityShards: uint8(cfg.FEC.ParityShards),
		Payload: negotiationv2.PayloadLimits{
			SendHardCap: uint16(cfg.Transport.MaxUDPPayload), ReceiveHardCap: uint16(cfg.Transport.MaxReceiveUDPPayload), BootstrapBytes: 512,
		},
		Datagram: negotiationv2.DatagramLimits{
			DatagramWindow: recvwindow.MaxSpan, GroupWindow: recvwindow.MaxSpan,
			MaxDatagramBytes: uint32(cfg.Limits.MaxDatagramSize), MaxFragments: uint16(cfg.Limits.MaxFragmentsPerDatagram),
			MaxDescriptors: fecv2.MaxDescriptors, MaxDatagramAssemblies: uint32(cfg.Limits.MaxDatagramReassemblies),
		},
		Epochs:   negotiationv2.EpochLimits{MaxOldEpochs: uint16(cfg.Transport.MaxRetainedEpochs), GraceMS: uint32(cfg.Transport.MaxEpochAge / time.Millisecond)},
		MaxPaths: uint16(maxPaths),
	}
	if cfg.Aggregation.Enabled {
		profile.RequiredCaps |= negotiationv2.Aggregation
	}
	controller := sessionv2.Config{
		LocalProfile: profile,
		SendLimits: negotiationv2.SendLimits{Datagram: negotiationv2.DatagramLimits{
			DatagramWindow: uint32(cfg.Repair.MaxOutstandingDatagramSpan), GroupWindow: uint32(cfg.Repair.MaxOutstandingGroupSpan),
			MaxDatagramBytes: uint32(cfg.Limits.MaxDatagramSize), MaxFragments: uint16(cfg.Limits.MaxFragmentsPerDatagram),
			MaxDescriptors: uint16(cfg.Aggregation.MaxRecords), MaxDatagramAssemblies: uint32(min(cfg.Aggregation.MaxQueuedDatagrams, cfg.Repair.MaxOutstandingDatagramSpan)),
		}},
		FixedPayloadBudget: uint16(cfg.Transport.MaxUDPPayload), Aggregation: cfg.Aggregation.Enabled,
		MaxGroupBytes: uint32(cfg.Aggregation.MaxGroupBytes),
		Queue: aggregationv2.Limits{
			MaxQueuedDatagrams: cfg.Aggregation.MaxQueuedDatagrams, MaxQueuedBytes: uint64(cfg.Aggregation.MaxQueuedBytes),
			MaxDatagramBytes: uint32(cfg.Limits.MaxDatagramSize), MaxFragmentsPerDatagram: cfg.Limits.MaxFragmentsPerDatagram,
			MaxDelay: cfg.Aggregation.MaxDelay,
		},
		Reassembly: reassemblyv2.Limits{
			MaxDatagrams: cfg.Limits.MaxDatagramReassemblies, MaxDatagramBytes: cfg.Limits.MaxDatagramSize,
			MaxFragments: cfg.Limits.MaxFragmentsPerDatagram, Span: profile.Datagram.DatagramWindow,
			Timeout: cfg.Timers.DatagramReassemblyTimeout,
		},
		MaxPendingGroups: cfg.Limits.MaxPendingFECBlocks, GroupTimeout: cfg.Timers.GroupDecodeTimeout,
		PathRatesBPS: make(map[uint16]uint64, len(rates)),
	}
	for path, rate := range rates {
		controller.PathRatesBPS[uint16(path)] = uint64(rate)
	}
	if _, err := sessionv2.RequiredInitialClaims(controller); err != nil {
		return sessionv2.Config{}, fmt.Errorf("%w: v2 controller limits: %v", ErrInvalidConfig, err)
	}
	return controller, nil
}

// Fixed Peer-owned runtime allocations live outside Session leases, so their
// charge is removed before the shared ledger admits any Session.
func v2CreditLimits(cfg config.Config, runtimeBytes uint64) (creditv2.Limits, error) {
	if err := cfg.Validate(); err != nil {
		return creditv2.Limits{}, err
	}
	if !supportedV2Config(cfg) {
		return creditv2.Limits{}, fmt.Errorf("%w: unsupported v2 credit policy", ErrInvalidConfig)
	}
	peerBytes := uint64(cfg.Limits.MaxPeerRetainedBytes)
	if runtimeBytes >= peerBytes {
		return creditv2.Limits{}, fmt.Errorf("%w: v2 runtime storage exhausts Peer bytes", ErrResourceLimit)
	}
	peerBytes -= runtimeBytes
	// Bound lease metadata independently of byte pressure. These validated
	// maxima cover queued originals, shard/decode work, receive/delivery owners
	// and fixed handshake/component claims; the ledger imposes its global cap.
	shards := uint64(cfg.FEC.DataShards + cfg.FEC.ParityShards)
	perSession := uint64(cfg.Aggregation.MaxQueuedDatagrams) + uint64(cfg.Limits.MaxPendingFECBlocks)*(shards+2) +
		uint64(cfg.Limits.MaxDatagramReassemblies) + uint64(cfg.Limits.DeliveryQueueCapacity) + 16
	reservations := min(uint64(creditv2.MaxReservations), uint64(cfg.Limits.MaxSessions)*perSession)
	return creditv2.Limits{
		MaxPeerBytes: peerBytes, MaxSessionBytes: min(uint64(cfg.Limits.MaxSessionRetainedBytes), peerBytes),
		MaxSessions: cfg.Limits.MaxSessions, MaxPendingHandshakes: cfg.Limits.MaxPendingHandshakes,
		MaxPendingAccepts: cfg.Limits.MaxPendingAccepts, MaxStreamsPerSession: cfg.Limits.MaxStreamsPerSession,
		MaxPeerStreams: cfg.Limits.MaxPeerStreams, MaxReservations: int(reservations),
	}, nil
}
