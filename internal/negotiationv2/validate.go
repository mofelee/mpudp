package negotiationv2

import "fmt"

const (
	maxReceiveBytes   = 1 << 30
	initialSmuxWindow = 262144
)

func invalid(reason string) error { return fmt.Errorf("%w: %s", ErrInvalid, reason) }

// Validate checks normalized wire semantics, not whether advertised resources
// have been reserved or whether an implementation supports the offered features.
func (p Profile) Validate() error {
	if p.Protocol != Datagram && p.Protocol != KCP {
		return invalid("unknown protocol")
	}
	if p.Mux > SmuxWire2 || p.Discovery > ProbeMTU || p.Scope > CarrierBudget {
		return invalid("unknown mux or MTU profile")
	}
	if p.RequiredCaps & ^KnownCapabilities != 0 {
		return ErrUnsupportedCapability
	}
	if p.RequiredCaps & ^p.OfferedCaps != 0 {
		return invalid("required capabilities are not offered")
	}
	if p.Payload.SendHardCap < 512 || p.Payload.SendHardCap > 65507 || p.Payload.ReceiveHardCap < 512 || p.Payload.ReceiveHardCap > 65507 || p.Payload.BootstrapBytes != 512 {
		return invalid("invalid payload bounds")
	}
	if p.MaxPaths < 1 || p.MaxPaths > 256 {
		return invalid("invalid path count")
	}
	if p.Epochs.MaxOldEpochs < 1 || p.Epochs.MaxOldEpochs > 8 || p.Epochs.GraceMS < 100 || p.Epochs.GraceMS > 60000 {
		return invalid("invalid epoch bounds")
	}
	if p.OfferedCaps&GroupMigration == 0 {
		if p.Epochs.MaxMigrations != 0 {
			return invalid("migration limit must be zero without its capability")
		}
	} else if p.Epochs.MaxMigrations < 1 || p.Epochs.MaxMigrations > 2 {
		return invalid("invalid migration limit")
	}
	required := Capabilities(0)
	if p.Discovery == ProbeMTU {
		required |= PLPMTUD
	}
	if p.Scope == CarrierBudget {
		required |= PerCarrierBudget
	}
	if p.Protocol == Datagram {
		required |= FragmentManifest
		if p.LayoutID != 1 || p.DataShards == 0 || p.ParityShards == 0 || int(p.DataShards)+int(p.ParityShards) > 256 {
			return invalid("invalid Datagram layout or FEC")
		}
		if p.Payload.InnerKCPBytes != 0 || p.Mux != MuxOff || p.Streams != (StreamLimits{}) {
			return invalid("Datagram requires neutral reliable parameters")
		}
		if err := validateDatagram(p.Datagram); err != nil {
			return err
		}
	} else {
		required |= NativeKCP
		if p.LayoutID != 0 || p.DataShards != 0 || p.ParityShards != 0 || p.Datagram != (DatagramLimits{}) || p.Payload.InnerKCPBytes != 1500 {
			return invalid("KCP requires neutral Datagram parameters and its inner limit")
		}
		if p.Discovery == ProbeMTU || p.Scope == CarrierBudget {
			required |= KCPPacketPieces
		}
		if err := validateStreams(p.Streams, p.Mux); err != nil {
			return err
		}
	}
	if p.RequiredCaps&DatagramRepair != 0 {
		if p.Protocol != Datagram || p.Repair.MaxAgeMS < 100 || p.Repair.MaxAgeMS > 60000 || p.Repair.MaxAttempts < 1 || p.Repair.MaxAttempts > 16 {
			return invalid("invalid repair policy")
		}
	} else if p.Repair != (RepairLimits{}) {
		return invalid("disabled repair requires neutral parameters")
	}
	if p.OfferedCaps&DatagramRepair != p.RequiredCaps&DatagramRepair {
		return invalid("disabled repair cannot be offered")
	}
	if p.Mux == SmuxWire2 {
		required |= SmuxAdmission
	} else if p.OfferedCaps&SmuxAdmission != 0 {
		return invalid("disabled mux cannot be offered")
	}
	if p.RequiredCaps&required != required {
		return invalid("active framing and MTU capabilities must be required")
	}
	if p.RequiredCaps & ^applicableCaps(p) != 0 {
		return invalid("required capabilities are inactive in selected policy")
	}
	return nil
}

func applicableCaps(p Profile) Capabilities {
	var caps Capabilities
	if p.Protocol == Datagram {
		caps = FragmentManifest | Aggregation | DatagramRepair | GroupMigration
	} else {
		caps = NativeKCP | KCPPacketPieces
		if p.Mux == SmuxWire2 {
			caps |= SmuxAdmission
		}
	}
	if p.Discovery == ProbeMTU {
		caps |= PLPMTUD
	}
	if p.Scope == CarrierBudget {
		caps |= PerCarrierBudget
	}
	return caps
}

func validateDatagram(d DatagramLimits) error {
	if d.DatagramWindow < 1 || d.DatagramWindow > 65536 || d.GroupWindow < 1 || d.GroupWindow > 65536 || d.MaxDatagramBytes < 1 || d.MaxDatagramBytes > 1<<24 || d.MaxFragments < 1 || d.MaxFragments > 4096 || d.MaxDescriptors < 1 || d.MaxDescriptors > 256 || d.MaxDatagramAssemblies < 1 || d.MaxDatagramAssemblies > d.DatagramWindow {
		return invalid("invalid Datagram receive limits")
	}
	return nil
}

func validateStreams(s StreamLimits, mux MuxProfile) error {
	if s.MaxPendingAccepts < 1 || s.MaxPendingAccepts > 65536 || s.SessionReceiveBytes < 1 || s.SessionReceiveBytes > maxReceiveBytes || s.StreamReceiveBytes < 1 || s.StreamReceiveBytes > s.SessionReceiveBytes {
		return invalid("invalid reliable receive limits")
	}
	if mux == MuxOff {
		if s.MaxBusinessStreams != 0 || s.ControlReceiveReserve != 0 || s.MaxPendingOpens != 0 || s.MaxFrameBytes != 0 {
			return invalid("raw KCP requires neutral mux parameters")
		}
		return nil
	}
	if s.MaxBusinessStreams < 1 || s.MaxBusinessStreams > 4096 || s.MaxPendingOpens < 1 || s.MaxPendingOpens > 128 || s.MaxFrameBytes < 128 || s.MaxFrameBytes > 65535 {
		return invalid("invalid mux receive limits")
	}
	minimum := uint64(initialSmuxWindow) + uint64(s.MaxFrameBytes)
	if uint64(s.StreamReceiveBytes) < minimum || uint64(s.ControlReceiveReserve) < minimum || uint64(s.ControlReceiveReserve)+minimum > uint64(s.SessionReceiveBytes) {
		return invalid("mux initial receive credits exceed advertised capacity")
	}
	return nil
}

func (a Advertisement) validate() error {
	if err := a.Profile.Validate(); err != nil {
		return err
	}
	if a.BootstrapPathID < 1 || a.BootstrapPathID > a.MaxPaths {
		return invalid("invalid bootstrap path index")
	}
	return nil
}
