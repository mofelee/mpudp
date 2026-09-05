package mpudp

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mofelee/mpudp/config"
	"github.com/mofelee/mpudp/internal/transport"
)

func TestUnavailableProtocolRejectsBeforeRuntimeDependencies(t *testing.T) {
	t.Parallel()
	for _, protocol := range []config.Protocol{config.ProtocolDatagram, config.ProtocolKCP} {
		t.Run(string(protocol), func(t *testing.T) {
			t.Parallel()
			cfg := config.DefaultV2(protocol)
			legacy := baseConfig()
			cfg.PSK, cfg.FEC = legacy.PSK, legacy.FEC
			cfg.Listen, cfg.Carriers = "127.0.0.1:9000", []string{"example.invalid:9001"}
			if protocol == config.ProtocolKCP {
				cfg.FEC = config.FECConfig{}
			} else {
				cfg.Repair.Enabled = true
			}
			before := cfg.Clone()
			deps := runtimeDependencies{
				openCarrier: func(context.Context, string, string, transport.CarrierOptions) (runtimeCarrier, error) {
					panic("unavailable protocol opened a Carrier")
				},
				openListener: func(context.Context, string, string, transport.ListenerOptions) (runtimePacketListener, error) {
					panic("unavailable protocol opened a Listener")
				},
				newTimer: func() runtimeDeadlineTimer {
					panic("unavailable protocol started its dispatcher timer")
				},
			}
			peer, err := newPeerWithContextAndDependencies(unusedPeerContext{}, cfg, unusedPeerRandom{}, deps)
			if peer != nil || !errors.Is(err, ErrProtocolUnavailable) || errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("construction = (%v, %v), want nil ErrProtocolUnavailable", peer, err)
			}
			if !reflect.DeepEqual(cfg, before) {
				t.Fatal("unavailable construction mutated caller configuration")
			}
			if peer, err := NewPeer(cfg); peer != nil || !errors.Is(err, ErrProtocolUnavailable) {
				t.Fatalf("NewPeer() = (%v, %v)", peer, err)
			}
			if peer, err := NewPeerContext(unusedPeerContext{}, cfg); peer != nil || !errors.Is(err, ErrProtocolUnavailable) {
				t.Fatalf("NewPeerContext() = (%v, %v)", peer, err)
			}
			features := cfg.Clone()
			if protocol == config.ProtocolDatagram {
				features.Aggregation.Enabled, features.Repair.Enabled = true, true
			} else {
				features.StreamMux.Enabled = true
				features.KCP.FastRetransmit.Enabled, features.KCP.CongestionControl = false, false
			}
			featuresBefore := features.Clone()
			if peer, err := newPeerWithContextAndDependencies(unusedPeerContext{}, features, unusedPeerRandom{}, deps); peer != nil || !errors.Is(err, ErrProtocolUnavailable) {
				t.Fatalf("configured v2 features = (%v, %v)", peer, err)
			}
			if !reflect.DeepEqual(features, featuresBefore) {
				t.Fatal("feature validation rewrote caller configuration")
			}
			cfg.Transport.MaxUDPPayload = config.MinV2MaxUDPPayload - 1
			if peer, err := newPeerWithDependencies(cfg, unusedPeerRandom{}, deps); peer != nil || !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("invalid v2 construction = (%v, %v), want configuration error", peer, err)
			}
		})
	}
}

func TestLegacyGoSelectionStillConstructsV1Peer(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Carriers = []string{"127.0.0.1:9"}
	cfg.Protocol, cfg.Wire = "", config.WireConfig{}
	peer, err := NewPeer(cfg)
	if err != nil {
		t.Fatalf("legacy NewPeer() error = %v", err)
	}
	defer peer.Close()
	if got := peer.Config(); got.Protocol != "" || got.Wire.Version != "" || peer.Mode() != ModeInitiator {
		t.Fatal("legacy configuration or Peer role changed")
	}
}

type unusedPeerRandom struct{}

func (unusedPeerRandom) Read([]byte) (int, error) { panic("unavailable protocol read randomness") }

type unusedPeerContext struct{}

func (unusedPeerContext) Deadline() (time.Time, bool) { panic("unavailable protocol checked deadline") }
func (unusedPeerContext) Done() <-chan struct{}       { panic("unavailable protocol observed context") }
func (unusedPeerContext) Err() error                  { panic("unavailable protocol checked context") }
func (unusedPeerContext) Value(any) any               { panic("unavailable protocol derived context") }
