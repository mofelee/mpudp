package negotiationv2

import (
	"errors"
	"testing"
)

func TestSelectionCapabilitiesAndOwnership(t *testing.T) {
	hello := Advertisement{Profile: datagramProfile(), BootstrapPathID: 3}
	hello.OfferedCaps |= NativeKCP | PLPMTUD | PerCarrierBudget | 1<<63
	listener := hello.Profile
	listener.OfferedCaps &^= Aggregation | GroupMigration
	listener.Epochs.MaxMigrations = 0
	beforeHello, beforeListener := hello, listener
	challenge, contract, err := Select(hello, listener)
	if err != nil {
		t.Fatal(err)
	}
	wantSelected := FragmentManifest | NativeKCP | PLPMTUD | PerCarrierBudget
	if contract.SelectedCaps != wantSelected || contract.ActiveCaps != FragmentManifest || challenge.OfferedCaps != wantSelected {
		t.Fatalf("incorrect selected/active capabilities: %+v", contract)
	}
	if hello != beforeHello || listener != beforeListener {
		t.Fatal("selection mutated its inputs")
	}
	if challenge.BootstrapPathID != 3 || contract.BootstrapPathID != 3 || contract.Epochs.MaxMigrations != 0 {
		t.Fatal("bootstrap path or disabled migration bounds changed")
	}
	accepted, err := Accept(hello, challenge)
	if err != nil || accepted != contract {
		t.Fatalf("accept disagreed with select: %v", err)
	}
	hello.Datagram.DatagramWindow = 1
	challenge.Datagram.DatagramWindow = 1
	if contract.client.Datagram.DatagramWindow != 65536 || contract.server.Datagram.DatagramWindow != 65536 {
		t.Fatal("contract did not retain value copies")
	}
}

func TestCapabilityRequirements(t *testing.T) {
	hello := Advertisement{Profile: datagramProfile(), BootstrapPathID: 1}
	listener := hello.Profile
	hello.RequiredCaps |= Aggregation
	listener.OfferedCaps &^= Aggregation
	if _, _, err := Select(hello, listener); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("client requirement: %v", err)
	}
	hello = Advertisement{Profile: datagramProfile(), BootstrapPathID: 1}
	hello.OfferedCaps &^= Aggregation
	listener = datagramProfile()
	listener.RequiredCaps |= Aggregation
	if _, _, err := Select(hello, listener); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("server requirement: %v", err)
	}
	hello.RequiredCaps |= 1 << 63
	hello.OfferedCaps |= 1 << 63
	if _, _, err := Select(hello, listener); !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("unknown required capability: %v", err)
	}
}

func TestInactiveRequiredCapabilities(t *testing.T) {
	for _, tt := range []struct {
		profile Profile
		cap     Capabilities
	}{
		{datagramProfile(), NativeKCP},
		{datagramProfile(), KCPPacketPieces},
		{datagramProfile(), PLPMTUD},
		{datagramProfile(), PerCarrierBudget},
		{kcpProfile(MuxOff), FragmentManifest},
		{kcpProfile(MuxOff), Aggregation},
		{kcpProfile(MuxOff), GroupMigration},
		{kcpProfile(SmuxWire2), PLPMTUD},
		{kcpProfile(SmuxWire2), PerCarrierBudget},
	} {
		offer := tt.profile
		offer.OfferedCaps |= tt.cap
		if tt.cap == GroupMigration {
			offer.Epochs.MaxMigrations = 2
		}
		if err := offer.Validate(); err != nil {
			t.Fatalf("inactive offer rejected: %v", err)
		}
		hello := Advertisement{Profile: offer, BootstrapPathID: 1}
		challenge, c, err := Select(hello, offer)
		if err != nil || c.SelectedCaps&tt.cap == 0 || c.ActiveCaps&tt.cap != 0 {
			t.Fatalf("inactive offer selection: %+v, %v", c, err)
		}
		required := offer
		required.RequiredCaps |= tt.cap
		if err := required.Validate(); !errors.Is(err, ErrInvalid) {
			t.Fatalf("inactive required profile accepted: %v", err)
		}
		if _, _, err := Select(hello, required); !errors.Is(err, ErrInvalid) {
			t.Fatalf("inactive listener requirement accepted: %v", err)
		}
		hello.Profile = required
		if _, _, err := Select(hello, offer); !errors.Is(err, ErrInvalid) {
			t.Fatalf("inactive initiator requirement accepted: %v", err)
		}
		if _, err := Accept(hello, challenge); !errors.Is(err, ErrInvalid) {
			t.Fatalf("inactive HELLO requirement accepted: %v", err)
		}
		hello.Profile = offer
		challenge.RequiredCaps |= tt.cap
		if _, err := Accept(hello, challenge); !errors.Is(err, ErrInvalid) {
			t.Fatalf("inactive CHALLENGE requirement accepted: %v", err)
		}
	}
}

func TestExactPolicy(t *testing.T) {
	tests := []struct {
		name            string
		hello, listener Profile
	}{
		{"protocol", datagramProfile(), kcpProfile(MuxOff)},
		{"fec", datagramProfile(), datagramProfile()},
		{"mux", kcpProfile(MuxOff), kcpProfile(SmuxWire2)},
		{"discovery", datagramProfile(), datagramProfile()},
		{"scope", datagramProfile(), datagramProfile()},
		{"repair", datagramProfile(), datagramProfile()},
	}
	tests[1].listener.DataShards++
	tests[3].listener.Discovery = ProbeMTU
	tests[3].listener.OfferedCaps |= PLPMTUD
	tests[3].listener.RequiredCaps |= PLPMTUD
	tests[4].listener.Scope = CarrierBudget
	tests[4].listener.OfferedCaps |= PerCarrierBudget
	tests[4].listener.RequiredCaps |= PerCarrierBudget
	tests[5].listener.OfferedCaps |= DatagramRepair
	tests[5].listener.RequiredCaps |= DatagramRepair
	tests[5].listener.Repair = RepairLimits{100, 1}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hello := Advertisement{Profile: tt.hello, BootstrapPathID: 1}
			challenge := Advertisement{Profile: tt.listener, BootstrapPathID: 1}
			if _, _, err := Select(hello, tt.listener); !errors.Is(err, ErrIncompatible) {
				t.Fatalf("Select: %v", err)
			}
			if _, err := Accept(hello, challenge); !errors.Is(err, ErrIncompatible) {
				t.Fatalf("Accept: %v", err)
			}
		})
	}
}

func TestPathCountAndWinningIndex(t *testing.T) {
	for _, scope := range []BudgetScope{SessionBudget, CarrierBudget} {
		for _, discovery := range []Discovery{Fixed, ProbeMTU} {
			hello := Advertisement{Profile: datagramProfile(), BootstrapPathID: 3}
			hello.Scope, hello.Discovery = scope, discovery
			if scope == CarrierBudget {
				hello.OfferedCaps |= PerCarrierBudget
				hello.RequiredCaps |= PerCarrierBudget
			}
			if discovery == ProbeMTU {
				hello.OfferedCaps |= PLPMTUD
				hello.RequiredCaps |= PLPMTUD
			}
			listener := hello.Profile
			listener.MaxPaths = 3
			challenge, c, err := Select(hello, listener)
			if scope == CarrierBudget && discovery == Fixed {
				if !errors.Is(err, ErrIncompatible) {
					t.Fatalf("partial static profile accepted: %v", err)
				}
				listener.MaxPaths = 5
				challenge, c, err = Select(hello, listener)
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.MaxPaths != listener.MaxPaths || c.BootstrapPathID != 3 || challenge.BootstrapPathID != 3 {
				t.Fatal("incorrect path selection")
			}
			listener.MaxPaths = 2
			if _, _, err := Select(hello, listener); err == nil {
				t.Fatal("accepted bootstrap outside effective path count")
			}
		}
	}
	for _, path := range []uint16{0, 6} {
		hello := Advertisement{Profile: datagramProfile(), BootstrapPathID: path}
		if _, _, err := Select(hello, hello.Profile); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid bootstrap %d: %v", path, err)
		}
	}
}

func TestForgedChallenge(t *testing.T) {
	hello := Advertisement{Profile: datagramProfile(), BootstrapPathID: 3}
	challenge, _, err := Select(hello, hello.Profile)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Advertisement){
		func(a *Advertisement) { a.OfferedCaps |= 1 << 63 },
		func(a *Advertisement) { a.OfferedCaps |= NativeKCP },
		func(a *Advertisement) { a.OfferedCaps &^= FragmentManifest; a.RequiredCaps &^= FragmentManifest },
		func(a *Advertisement) { a.BootstrapPathID = 2 },
		func(a *Advertisement) { a.LayoutID = 2 },
	} {
		forged := challenge
		change(&forged)
		if _, err := Accept(hello, forged); err == nil {
			t.Fatal("forged selection accepted")
		}
	}
}

func TestDatagramDirectionalMinima(t *testing.T) {
	hello := Advertisement{Profile: datagramProfile(), BootstrapPathID: 3}
	hello.Payload.SendHardCap, hello.Payload.ReceiveHardCap = 1400, 900
	hello.Datagram = DatagramLimits{10, 11, 12, 13, 14, 5}
	listener := datagramProfile()
	listener.Payload.SendHardCap, listener.Payload.ReceiveHardCap = 1300, 1100
	listener.Datagram = DatagramLimits{100, 101, 102, 103, 104, 50}
	_, c, err := Select(hello, listener)
	if err != nil {
		t.Fatal(err)
	}
	sender := SendLimits{Datagram: DatagramLimits{80, 150, 90, 160, 70, 60}}
	forward, err := c.EffectiveSend(Initiator, sender)
	if err != nil {
		t.Fatal(err)
	}
	want := DirectionLimits{MaxUDPPayload: 1100, Datagram: DatagramLimits{80, 101, 90, 103, 70, 50}}
	if forward != want {
		t.Fatalf("forward=%+v want=%+v", forward, want)
	}
	reverse, err := c.EffectiveSend(Responder, sender)
	if err != nil {
		t.Fatal(err)
	}
	if reverse.MaxUDPPayload != 900 || reverse.Datagram != hello.Datagram {
		t.Fatalf("reverse=%+v", reverse)
	}
	if _, err := c.EffectiveSend(0, sender); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid role: %v", err)
	}
	if _, err := (Contract{}).EffectiveSend(Initiator, sender); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero contract: %v", err)
	}
	sender.Streams.SessionBytes = 1
	if _, err := c.EffectiveSend(Initiator, sender); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nonneutral send fields: %v", err)
	}
	sender = SendLimits{}
	if _, err := c.EffectiveSend(Initiator, sender); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero send limits: %v", err)
	}
}

func TestReliableDirectionalMinima(t *testing.T) {
	for _, mux := range []MuxProfile{MuxOff, SmuxWire2} {
		hello := Advertisement{Profile: kcpProfile(mux), BootstrapPathID: 3}
		listener := kcpProfile(mux)
		hello.Streams.SessionReceiveBytes, hello.Streams.StreamReceiveBytes = 600000, 300000
		listener.Streams.SessionReceiveBytes, listener.Streams.StreamReceiveBytes = 900000, 400000
		sender := SendLimits{Streams: StreamSendLimits{SessionBytes: 1000000, StreamBytes: 500000}}
		if mux == SmuxWire2 {
			hello.Streams.MaxBusinessStreams, hello.Streams.MaxPendingOpens, hello.Streams.MaxPendingAccepts = 10, 11, 12
			listener.Streams.MaxBusinessStreams, listener.Streams.MaxPendingOpens, listener.Streams.MaxPendingAccepts = 100, 101, 40
			listener.Streams.MaxFrameBytes = 8192
			sender.Streams.MaxOpenStreams, sender.Streams.MaxPendingOpens = 80, 90
		}
		_, c, err := Select(hello, listener)
		if err != nil {
			t.Fatal(err)
		}
		forward, err := c.EffectiveSend(Initiator, sender)
		if err != nil {
			t.Fatal(err)
		}
		if forward.Streams.SessionBytes != 900000 || forward.Streams.StreamBytes != 400000 {
			t.Fatalf("forward=%+v", forward)
		}
		reverse, err := c.EffectiveSend(Responder, sender)
		if err != nil {
			t.Fatal(err)
		}
		if reverse.Streams.SessionBytes != 600000 || reverse.Streams.StreamBytes != 300000 {
			t.Fatalf("reverse=%+v", reverse)
		}
		if mux == SmuxWire2 {
			if forward.Streams.MaxOpenStreams != 80 || forward.Streams.MaxPendingOpens != 40 || reverse.Streams.MaxOpenStreams != 10 || reverse.Streams.MaxPendingOpens != 11 || c.MuxFrameBytes != 8192 {
				t.Fatal("incorrect mux minima")
			}
			if forward.ControlReceiveReserve != 278528 || forward.BusinessSessionBytes != 621472 || reverse.ControlReceiveReserve != 278528 || reverse.BusinessSessionBytes != 321472 {
				t.Fatal("business bytes consumed the peer control reserve")
			}
		} else if forward.Streams.MaxOpenStreams != 0 || forward.Streams.MaxPendingOpens != 0 || c.MuxFrameBytes != 0 {
			t.Fatal("raw mux fields not neutral")
		} else if forward.ControlReceiveReserve != 0 || forward.BusinessSessionBytes != forward.Streams.SessionBytes {
			t.Fatal("raw KCP has a mux control reserve")
		}
		for _, change := range []func(*SendLimits){
			func(s *SendLimits) { s.Datagram.GroupWindow = 1 },
			func(s *SendLimits) { s.Streams.SessionBytes = 0 },
			func(s *SendLimits) { s.Streams.StreamBytes = s.Streams.SessionBytes + 1 },
			func(s *SendLimits) { s.Streams.MaxOpenStreams = 4097 },
			func(s *SendLimits) { s.Streams.MaxPendingOpens = 129 },
		} {
			bad := sender
			change(&bad)
			if _, err := c.EffectiveSend(Initiator, bad); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid reliable send limits: %v", err)
			}
		}
	}
}

func TestMuxIndependentSendAndReceiveOwnership(t *testing.T) {
	hello := Advertisement{Profile: kcpProfile(SmuxWire2), BootstrapPathID: 1}
	_, c, err := Select(hello, hello.Profile)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct{ local, wantBusiness uint32 }{
		{65536, 65536},
		{1 << 20, 1<<20 - 278528},
	} {
		sender := SendLimits{Streams: StreamSendLimits{MaxOpenStreams: 1, MaxPendingOpens: 1, SessionBytes: tt.local, StreamBytes: tt.local}}
		limits, err := c.EffectiveSend(Initiator, sender)
		if err != nil {
			t.Fatal(err)
		}
		if limits.Streams.SessionBytes != tt.local || limits.BusinessSessionBytes != tt.wantBusiness || limits.ControlReceiveReserve != 278528 || limits.Streams.StreamBytes != min(tt.wantBusiness, hello.Streams.StreamReceiveBytes) {
			t.Fatalf("incorrect independent ownership limits: %+v", limits)
		}
	}
	listener := hello.Profile
	listener.Streams.SessionReceiveBytes = 2 * 278528
	listener.Streams.StreamReceiveBytes = 400000
	_, c, err = Select(hello, listener)
	if err != nil {
		t.Fatal(err)
	}
	sender := SendLimits{Streams: StreamSendLimits{MaxOpenStreams: 1, MaxPendingOpens: 1, SessionBytes: 1 << 20, StreamBytes: 1 << 20}}
	limits, err := c.EffectiveSend(Initiator, sender)
	if err != nil {
		t.Fatal(err)
	}
	if limits.BusinessSessionBytes != 278528 || limits.Streams.StreamBytes != 278528 {
		t.Fatalf("stream consumed control reserve: %+v", limits)
	}
}

func TestRepairEpochAndActiveMTUSelection(t *testing.T) {
	hello := Advertisement{Profile: datagramProfile(), BootstrapPathID: 3}
	hello.Discovery, hello.Scope = ProbeMTU, CarrierBudget
	hello.OfferedCaps |= DatagramRepair | PLPMTUD | PerCarrierBudget
	hello.RequiredCaps |= DatagramRepair | PLPMTUD | PerCarrierBudget
	hello.Repair = RepairLimits{1000, 4}
	listener := hello.Profile
	listener.Repair = RepairLimits{500, 6}
	listener.Epochs = EpochLimits{1, 1, 4000}
	_, c, err := Select(hello, listener)
	if err != nil {
		t.Fatal(err)
	}
	if c.Repair != (RepairLimits{500, 4}) || c.Epochs != listener.Epochs || c.ActiveCaps != c.SelectedCaps {
		t.Fatalf("incorrect selected bounds: %+v", c)
	}
}
