package mpudp

import (
	"errors"
	"math"
	"reflect"
	"runtime"
	"testing"

	"github.com/mofelee/mpudp/config"
	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/recvwindow"
	"github.com/mofelee/mpudp/internal/sessionv2"
)

func v2MappingConfig(t *testing.T) config.Config {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("v2 runtime requires Linux destination replies and PMTU enforcement")
	}
	cfg := config.DefaultV2(config.ProtocolDatagram)
	legacy := baseConfig()
	cfg.PSK, cfg.FEC = legacy.PSK, legacy.FEC
	cfg.Carriers = []string{"127.0.0.1:9001", "127.0.0.1:9002"}
	cfg.Listen = "127.0.0.1:9000"
	return cfg
}

func TestV2ControllerConfigKeepsDirectionalAndReceiveLimitsIndependent(t *testing.T) {
	cfg := v2MappingConfig(t)
	cfg.Transport.MaxUDPPayload, cfg.Transport.MaxReceiveUDPPayload = 1000, 1400
	cfg.Limits.MaxEndpointsPerSession, cfg.Limits.MaxDatagramReassemblies = 4, 13
	cfg.Aggregation.MaxQueuedDatagrams, cfg.Aggregation.MaxRecords = 7, 3
	cfg.Repair.MaxOutstandingDatagramSpan, cfg.Repair.MaxOutstandingGroupSpan = 31, 1024
	cfg.Scheduler.OutboundPathRatesBPS = map[int]int64{1: 1000}
	cfg.Scheduler.InboundPathRatesBPS = map[int]int64{4: 9000}
	before := cfg.Clone()
	for _, responder := range []bool{false, true} {
		mapped, err := v2ControllerConfig(cfg, responder)
		if err != nil {
			t.Fatal(err)
		}
		profile, send := mapped.LocalProfile, mapped.SendLimits.Datagram
		if profile.Payload.SendHardCap != 1000 || profile.Payload.ReceiveHardCap != 1400 || mapped.FixedPayloadBudget != 1000 {
			t.Fatal("send and receive UDP limits were conflated")
		}
		if profile.Datagram.DatagramWindow != recvwindow.MaxSpan || profile.Datagram.GroupWindow != recvwindow.MaxSpan || profile.Datagram.MaxDescriptors != 256 || profile.Datagram.MaxDatagramAssemblies != 13 {
			t.Fatal("sender settings changed advertised receive ceilings")
		}
		if send.DatagramWindow != 31 || send.GroupWindow != 1024 || send.MaxDescriptors != 3 || send.MaxDatagramAssemblies != 7 || mapped.Reassembly.MaxDatagrams != 13 {
			t.Fatal("independent sender or reassembly limits were lost")
		}
		if profile.OfferedCaps != negotiationv2.FragmentManifest|negotiationv2.Aggregation || profile.RequiredCaps != negotiationv2.FragmentManifest || profile.Repair != (negotiationv2.RepairLimits{}) || profile.Epochs.MaxMigrations != 0 {
			t.Fatal("dormant settings enabled unsupported negotiated features")
		}
		if responder {
			if profile.MaxPaths != 4 || len(mapped.PathRatesBPS) != 1 || mapped.PathRatesBPS[4] != 9000 {
				t.Fatal("listener rates or path ceiling came from initiator settings")
			}
		} else if profile.MaxPaths != 2 || len(mapped.PathRatesBPS) != 1 || mapped.PathRatesBPS[1] != 1000 {
			t.Fatal("initiator rates or Carrier count came from listener settings")
		}
		mapped.PathRatesBPS[1] = 7777
		if mapped.Emit != nil || mapped.Entropy != nil || mapped.BootstrapPath != nil || len(mapped.Carriers) != 0 {
			t.Fatal("configuration mapping created runtime dependencies")
		}
		if claims, err := sessionv2.RequiredInitialClaims(mapped); err != nil || len(claims) != sessionv2.InitialCount {
			t.Fatalf("socket-free template cannot reserve initial storage: %v", err)
		}
	}
	if !reflect.DeepEqual(cfg, before) {
		t.Fatal("configuration mapping retained or mutated caller-owned settings")
	}
	cfg.Aggregation.Enabled = true
	mapped, err := v2ControllerConfig(cfg, false)
	if err != nil || mapped.LocalProfile.RequiredCaps != negotiationv2.FragmentManifest|negotiationv2.Aggregation {
		t.Fatal("enabled aggregation did not require receiver support")
	}
}

func TestV2ConfigSupportAndInvalidMapping(t *testing.T) {
	cfg := v2MappingConfig(t)
	if !supportedV2Config(cfg) {
		t.Fatal("fixed Session Datagram configuration rejected")
	}
	for _, change := range []func(*config.Config){
		func(c *config.Config) { c.Wire.Version = config.WireVersionV1 },
		func(c *config.Config) { c.Protocol = config.ProtocolKCP },
		func(c *config.Config) { c.Repair.Enabled = true },
		func(c *config.Config) { c.StreamMux.Enabled = true },
		func(c *config.Config) { c.Transport.MTUDiscovery = config.MTUDiscoveryPLPMTUD },
		func(c *config.Config) { c.Transport.BudgetStrategy = config.BudgetStrategyPerCarrier },
	} {
		other := cfg.Clone()
		change(&other)
		if supportedV2Config(other) {
			t.Fatal("unsupported configuration activated the runtime")
		}
		if _, err := v2ControllerConfig(other, false); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("unsupported mapping error = %v", err)
		}
	}
	cfg.Transport.MaxReceiveUDPPayload = math.MaxInt
	if _, err := v2ControllerConfig(cfg, false); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal("invalid integer narrowed into a valid wire field")
	}
}

func TestV2CreditLimitsSubtractRuntimeWithoutUnderflow(t *testing.T) {
	cfg := v2MappingConfig(t)
	for _, remaining := range []uint64{uint64(cfg.Limits.MaxPeerRetainedBytes) - 4096, 1} {
		limits, err := v2CreditLimits(cfg, uint64(cfg.Limits.MaxPeerRetainedBytes)-remaining)
		if err != nil {
			t.Fatal(err)
		}
		if limits.MaxPeerBytes != remaining || limits.MaxSessionBytes != min(remaining, uint64(cfg.Limits.MaxSessionRetainedBytes)) || limits.MaxSessions != cfg.Limits.MaxSessions || limits.MaxPendingHandshakes != cfg.Limits.MaxPendingHandshakes || limits.MaxPendingAccepts != cfg.Limits.MaxPendingAccepts || limits.MaxReservations < 1 || limits.MaxReservations > creditv2.MaxReservations {
			t.Fatal("runtime charge or independent admission limits were not preserved")
		}
		ledger, err := creditv2.New(limits)
		if err != nil {
			t.Fatalf("mapped ledger limits are invalid: %v", err)
		}
		ledger.Close()
	}
	for _, runtimeBytes := range []uint64{uint64(cfg.Limits.MaxPeerRetainedBytes), math.MaxUint64} {
		if _, err := v2CreditLimits(cfg, runtimeBytes); !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("runtime exhaustion error = %v", err)
		}
	}
	cfg.Limits.MaxSessionRetainedBytes = 0
	if _, err := v2CreditLimits(cfg, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal("invalid byte limit was accepted")
	}
}
