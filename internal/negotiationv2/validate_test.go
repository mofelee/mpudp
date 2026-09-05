package negotiationv2

import (
	"errors"
	"testing"
)

func datagramProfile() Profile {
	return Profile{
		Protocol: Datagram, OfferedCaps: FragmentManifest | Aggregation | GroupMigration, RequiredCaps: FragmentManifest,
		LayoutID: 1, DataShards: 3, ParityShards: 2,
		Payload:  PayloadLimits{SendHardCap: 1200, ReceiveHardCap: 1472, BootstrapBytes: 512},
		Datagram: DatagramLimits{DatagramWindow: 65536, GroupWindow: 65536, MaxDatagramBytes: 65536, MaxFragments: 256, MaxDescriptors: 32, MaxDatagramAssemblies: 1024},
		Epochs:   EpochLimits{MaxOldEpochs: 2, MaxMigrations: 2, GraceMS: 5000}, MaxPaths: 5,
	}
}

func kcpProfile(mux MuxProfile) Profile {
	p := datagramProfile()
	p.Protocol, p.LayoutID, p.DataShards, p.ParityShards = KCP, 0, 0, 0
	p.OfferedCaps, p.RequiredCaps = NativeKCP, NativeKCP
	p.Datagram, p.Epochs.MaxMigrations = DatagramLimits{}, 0
	p.Payload.InnerKCPBytes, p.Mux = 1500, mux
	p.Streams = StreamLimits{MaxPendingAccepts: 256, SessionReceiveBytes: 1 << 20, StreamReceiveBytes: 278528}
	if mux == SmuxWire2 {
		p.OfferedCaps |= SmuxAdmission
		p.RequiredCaps |= SmuxAdmission
		p.Streams.MaxBusinessStreams, p.Streams.MaxPendingOpens = 128, 128
		p.Streams.ControlReceiveReserve, p.Streams.MaxFrameBytes = 278528, 16384
	}
	return p
}

func TestProfilesAndNumericEdges(t *testing.T) {
	dg := datagramProfile()
	dg.DataShards, dg.ParityShards = 255, 1
	dg.Payload.SendHardCap, dg.Payload.ReceiveHardCap = 512, 65507
	dg.Datagram = DatagramLimits{DatagramWindow: 65536, GroupWindow: 1, MaxDatagramBytes: 1 << 24, MaxFragments: 4096, MaxDescriptors: 256, MaxDatagramAssemblies: 65536}
	dg.Epochs = EpochLimits{MaxOldEpochs: 8, MaxMigrations: 1, GraceMS: 60000}
	dg.MaxPaths = 256
	raw := kcpProfile(MuxOff)
	raw.Streams.SessionReceiveBytes, raw.Streams.StreamReceiveBytes = 1, 1
	for _, p := range []Profile{datagramProfile(), kcpProfile(MuxOff), kcpProfile(SmuxWire2), dg, raw} {
		if err := p.Validate(); err != nil {
			t.Fatalf("valid profile rejected: %v", err)
		}
	}
}

func TestInvalidProfiles(t *testing.T) {
	tests := []struct {
		name   string
		base   Profile
		change func(*Profile)
	}{
		{"protocol", datagramProfile(), func(p *Profile) { p.Protocol = 0 }},
		{"mux-enum", datagramProfile(), func(p *Profile) { p.Mux = 2 }},
		{"discovery-enum", datagramProfile(), func(p *Profile) { p.Discovery = 2 }},
		{"scope-enum", datagramProfile(), func(p *Profile) { p.Scope = 2 }},
		{"required-not-offered", datagramProfile(), func(p *Profile) { p.RequiredCaps |= NativeKCP }},
		{"manifest-not-required", datagramProfile(), func(p *Profile) { p.RequiredCaps = 0 }},
		{"send-small", datagramProfile(), func(p *Profile) { p.Payload.SendHardCap = 511 }},
		{"send-large", datagramProfile(), func(p *Profile) { p.Payload.SendHardCap = 65508 }},
		{"receive-small", datagramProfile(), func(p *Profile) { p.Payload.ReceiveHardCap = 511 }},
		{"receive-large", datagramProfile(), func(p *Profile) { p.Payload.ReceiveHardCap = 65508 }},
		{"bootstrap", datagramProfile(), func(p *Profile) { p.Payload.BootstrapBytes = 513 }},
		{"paths-zero", datagramProfile(), func(p *Profile) { p.MaxPaths = 0 }},
		{"paths-large", datagramProfile(), func(p *Profile) { p.MaxPaths = 257 }},
		{"old-epochs-zero", datagramProfile(), func(p *Profile) { p.Epochs.MaxOldEpochs = 0 }},
		{"old-epochs-large", datagramProfile(), func(p *Profile) { p.Epochs.MaxOldEpochs = 9 }},
		{"grace-small", datagramProfile(), func(p *Profile) { p.Epochs.GraceMS = 99 }},
		{"grace-large", datagramProfile(), func(p *Profile) { p.Epochs.GraceMS = 60001 }},
		{"migrations-zero", datagramProfile(), func(p *Profile) { p.Epochs.MaxMigrations = 0 }},
		{"migrations-large", datagramProfile(), func(p *Profile) { p.Epochs.MaxMigrations = 3 }},
		{"inactive-migrations", datagramProfile(), func(p *Profile) { p.OfferedCaps &^= GroupMigration }},
		{"layout", datagramProfile(), func(p *Profile) { p.LayoutID = 0 }},
		{"data-zero", datagramProfile(), func(p *Profile) { p.DataShards = 0 }},
		{"parity-zero", datagramProfile(), func(p *Profile) { p.ParityShards = 0 }},
		{"fec-sum-overflow", datagramProfile(), func(p *Profile) { p.DataShards, p.ParityShards = 255, 2 }},
		{"datagram-inner-kcp", datagramProfile(), func(p *Profile) { p.Payload.InnerKCPBytes = 1500 }},
		{"datagram-streams", datagramProfile(), func(p *Profile) { p.Streams.MaxPendingAccepts = 1 }},
		{"dg-window-zero", datagramProfile(), func(p *Profile) { p.Datagram.DatagramWindow = 0 }},
		{"dg-window-large", datagramProfile(), func(p *Profile) { p.Datagram.DatagramWindow = 65537 }},
		{"group-window-zero", datagramProfile(), func(p *Profile) { p.Datagram.GroupWindow = 0 }},
		{"group-window-large", datagramProfile(), func(p *Profile) { p.Datagram.GroupWindow = 65537 }},
		{"dg-bytes-zero", datagramProfile(), func(p *Profile) { p.Datagram.MaxDatagramBytes = 0 }},
		{"dg-bytes-large", datagramProfile(), func(p *Profile) { p.Datagram.MaxDatagramBytes = 1<<24 + 1 }},
		{"fragments-zero", datagramProfile(), func(p *Profile) { p.Datagram.MaxFragments = 0 }},
		{"fragments-large", datagramProfile(), func(p *Profile) { p.Datagram.MaxFragments = 4097 }},
		{"descriptors-zero", datagramProfile(), func(p *Profile) { p.Datagram.MaxDescriptors = 0 }},
		{"descriptors-large", datagramProfile(), func(p *Profile) { p.Datagram.MaxDescriptors = 257 }},
		{"assemblies-zero", datagramProfile(), func(p *Profile) { p.Datagram.MaxDatagramAssemblies = 0 }},
		{"assemblies-over-window", datagramProfile(), func(p *Profile) { p.Datagram.MaxDatagramAssemblies = 65537 }},
		{"repair-disabled-fields", datagramProfile(), func(p *Profile) { p.Repair.MaxAgeMS = 100 }},
		{"repair-disabled-offer", datagramProfile(), func(p *Profile) { p.OfferedCaps |= DatagramRepair }},
		{"mux-disabled-offer", datagramProfile(), func(p *Profile) { p.OfferedCaps |= SmuxAdmission }},
		{"plpmtud-not-required", datagramProfile(), func(p *Profile) { p.Discovery = ProbeMTU; p.OfferedCaps |= PLPMTUD }},
		{"carrier-not-required", datagramProfile(), func(p *Profile) { p.Scope = CarrierBudget; p.OfferedCaps |= PerCarrierBudget }},
		{"kcp-layout", kcpProfile(MuxOff), func(p *Profile) { p.LayoutID = 1 }},
		{"kcp-fec", kcpProfile(MuxOff), func(p *Profile) { p.DataShards = 1 }},
		{"kcp-datagram", kcpProfile(MuxOff), func(p *Profile) { p.Datagram.GroupWindow = 1 }},
		{"kcp-inner", kcpProfile(MuxOff), func(p *Profile) { p.Payload.InnerKCPBytes = 1499 }},
		{"kcp-native", kcpProfile(MuxOff), func(p *Profile) { p.RequiredCaps = 0 }},
		{"raw-business", kcpProfile(MuxOff), func(p *Profile) { p.Streams.MaxBusinessStreams = 1 }},
		{"raw-control", kcpProfile(MuxOff), func(p *Profile) { p.Streams.ControlReceiveReserve = 1 }},
		{"raw-opens", kcpProfile(MuxOff), func(p *Profile) { p.Streams.MaxPendingOpens = 1 }},
		{"raw-frame", kcpProfile(MuxOff), func(p *Profile) { p.Streams.MaxFrameBytes = 128 }},
		{"pending-accept-zero", kcpProfile(MuxOff), func(p *Profile) { p.Streams.MaxPendingAccepts = 0 }},
		{"pending-accept-large", kcpProfile(MuxOff), func(p *Profile) { p.Streams.MaxPendingAccepts = 65537 }},
		{"session-zero", kcpProfile(MuxOff), func(p *Profile) { p.Streams.SessionReceiveBytes = 0 }},
		{"session-large", kcpProfile(MuxOff), func(p *Profile) { p.Streams.SessionReceiveBytes = 1<<30 + 1 }},
		{"stream-zero", kcpProfile(MuxOff), func(p *Profile) { p.Streams.StreamReceiveBytes = 0 }},
		{"stream-over-session", kcpProfile(MuxOff), func(p *Profile) { p.Streams.StreamReceiveBytes = 1<<20 + 1 }},
		{"mux-business-zero", kcpProfile(SmuxWire2), func(p *Profile) { p.Streams.MaxBusinessStreams = 0 }},
		{"mux-business-large", kcpProfile(SmuxWire2), func(p *Profile) { p.Streams.MaxBusinessStreams = 4097 }},
		{"mux-opens-zero", kcpProfile(SmuxWire2), func(p *Profile) { p.Streams.MaxPendingOpens = 0 }},
		{"mux-opens-large", kcpProfile(SmuxWire2), func(p *Profile) { p.Streams.MaxPendingOpens = 129 }},
		{"mux-frame-small", kcpProfile(SmuxWire2), func(p *Profile) { p.Streams.MaxFrameBytes = 127 }},
		{"mux-frame-large", kcpProfile(SmuxWire2), func(p *Profile) { p.Streams.MaxFrameBytes = 65536 }},
		{"mux-stream-credit", kcpProfile(SmuxWire2), func(p *Profile) { p.Streams.StreamReceiveBytes-- }},
		{"mux-control-credit", kcpProfile(SmuxWire2), func(p *Profile) { p.Streams.ControlReceiveReserve-- }},
		{"mux-control-overflow", kcpProfile(SmuxWire2), func(p *Profile) { p.Streams.ControlReceiveReserve = ^uint32(0) }},
		{"mux-total-credit", kcpProfile(SmuxWire2), func(p *Profile) { p.Streams.SessionReceiveBytes = 2*278528 - 1 }},
		{"mux-not-required", kcpProfile(SmuxWire2), func(p *Profile) { p.RequiredCaps &^= SmuxAdmission }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.change(&tt.base)
			if err := tt.base.Validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("got %v, want ErrInvalid", err)
			}
		})
	}
}

func TestRepairAndKCPPiecePolicies(t *testing.T) {
	p := datagramProfile()
	p.OfferedCaps |= DatagramRepair
	p.RequiredCaps |= DatagramRepair
	p.Repair = RepairLimits{MaxAgeMS: 100, MaxAttempts: 1}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, limits := range []RepairLimits{{99, 1}, {60001, 1}, {100, 0}, {100, 17}} {
		p.Repair = limits
		if err := p.Validate(); !errors.Is(err, ErrInvalid) {
			t.Fatalf("repair %+v: %v", limits, err)
		}
	}
	p = kcpProfile(MuxOff)
	p.OfferedCaps |= DatagramRepair
	p.RequiredCaps |= DatagramRepair
	p.Repair = RepairLimits{100, 1}
	if err := p.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("KCP repair: %v", err)
	}
	for _, strategy := range []struct {
		discovery Discovery
		scope     BudgetScope
		cap       Capabilities
	}{{ProbeMTU, SessionBudget, PLPMTUD}, {Fixed, CarrierBudget, PerCarrierBudget}} {
		p = kcpProfile(MuxOff)
		p.Discovery, p.Scope = strategy.discovery, strategy.scope
		p.OfferedCaps |= strategy.cap | KCPPacketPieces
		p.RequiredCaps |= strategy.cap
		if err := p.Validate(); !errors.Is(err, ErrInvalid) {
			t.Fatalf("pieces not required: %v", err)
		}
		p.RequiredCaps |= KCPPacketPieces
		if err := p.Validate(); err != nil {
			t.Fatal(err)
		}
	}
}
