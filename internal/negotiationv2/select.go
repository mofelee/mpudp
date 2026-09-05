package negotiationv2

// Profiles returns immutable endpoint settings in local, remote order for the
// selected role. It does not expose mutable state or perform resource admission.
func (c Contract) Profiles(role Role) (Profile, Profile, error) {
	if !c.valid || (role != Initiator && role != Responder) {
		return Profile{}, Profile{}, invalid("invalid contract or sender role")
	}
	if role == Responder {
		return c.server, c.client, nil
	}
	return c.client, c.server, nil
}

// Select constructs the listener CHALLENGE advertisement. The listener's
// original receive capabilities are preserved; OfferedCaps becomes the known
// intersection. The winning configured path is echoed without renumbering.
func Select(hello Advertisement, listener Profile) (Advertisement, Contract, error) {
	if err := hello.validate(); err != nil {
		return Advertisement{}, Contract{}, err
	}
	if err := listener.Validate(); err != nil {
		return Advertisement{}, Contract{}, err
	}
	if !samePolicy(hello.Profile, listener) {
		return Advertisement{}, Contract{}, ErrIncompatible
	}
	selected := hello.OfferedCaps & listener.OfferedCaps & KnownCapabilities
	if (hello.RequiredCaps|listener.RequiredCaps) & ^selected != 0 {
		return Advertisement{}, Contract{}, ErrIncompatible
	}
	challenge := Advertisement{Profile: listener, BootstrapPathID: hello.BootstrapPathID}
	challenge.OfferedCaps = selected
	if selected&GroupMigration == 0 {
		challenge.Epochs.MaxMigrations = 0
	}
	contract, err := Accept(hello, challenge)
	if err != nil {
		return Advertisement{}, Contract{}, err
	}
	return challenge, contract, nil
}

// Accept checks a selected CHALLENGE against the original HELLO. The client
// can prove selection is a subset of its offer and covers both required sets;
// only Select has the listener's original full offer to prove intersection.
// Transcript/nonces, authentication and the live attempt remain caller checks.
func Accept(hello, challenge Advertisement) (Contract, error) {
	if err := hello.validate(); err != nil {
		return Contract{}, err
	}
	if err := challenge.validate(); err != nil {
		return Contract{}, err
	}
	selected := challenge.OfferedCaps
	if !samePolicy(hello.Profile, challenge.Profile) || selected & ^KnownCapabilities != 0 || selected & ^hello.OfferedCaps != 0 || (hello.RequiredCaps|challenge.RequiredCaps) & ^selected != 0 || hello.BootstrapPathID != challenge.BootstrapPathID {
		return Contract{}, ErrIncompatible
	}
	paths := min(hello.MaxPaths, challenge.MaxPaths)
	if hello.BootstrapPathID > paths || (hello.Discovery == Fixed && hello.Scope == CarrierBudget && hello.MaxPaths > challenge.MaxPaths) {
		return Contract{}, ErrIncompatible
	}
	return Contract{
		SelectedCaps: selected, ActiveCaps: selected & applicableCaps(hello.Profile), MaxPaths: paths,
		BootstrapPathID: hello.BootstrapPathID,
		MuxFrameBytes:   min(hello.Streams.MaxFrameBytes, challenge.Streams.MaxFrameBytes),
		Repair:          RepairLimits{MaxAgeMS: min(hello.Repair.MaxAgeMS, challenge.Repair.MaxAgeMS), MaxAttempts: min(hello.Repair.MaxAttempts, challenge.Repair.MaxAttempts)},
		Epochs:          EpochLimits{MaxOldEpochs: min(hello.Epochs.MaxOldEpochs, challenge.Epochs.MaxOldEpochs), MaxMigrations: min(hello.Epochs.MaxMigrations, challenge.Epochs.MaxMigrations), GraceMS: min(hello.Epochs.GraceMS, challenge.Epochs.GraceMS)},
		client:          hello.Profile, server: challenge.Profile, valid: true,
	}, nil
}

func samePolicy(a, b Profile) bool {
	return a.Protocol == b.Protocol && a.LayoutID == b.LayoutID && a.DataShards == b.DataShards && a.ParityShards == b.ParityShards && a.Mux == b.Mux && a.Discovery == b.Discovery && a.Scope == b.Scope && a.RequiredCaps&DatagramRepair == b.RequiredCaps&DatagramRepair
}

// EffectiveSend intersects explicit local sender ceilings with the peer's
// receive limits. The UDP result is a hard ceiling, not a confirmed safe path
// budget: both directions still bootstrap at 512 until publication/probing.
func (c Contract) EffectiveSend(role Role, local SendLimits) (DirectionLimits, error) {
	if !c.valid || (role != Initiator && role != Responder) {
		return DirectionLimits{}, invalid("invalid contract or sender role")
	}
	self, peer := c.client, c.server
	if role == Responder {
		self, peer = peer, self
	}
	result := DirectionLimits{MaxUDPPayload: min(self.Payload.SendHardCap, peer.Payload.ReceiveHardCap)}
	if self.Protocol == Datagram {
		if err := validateDatagram(local.Datagram); err != nil {
			return DirectionLimits{}, err
		}
		if local.Streams != (StreamSendLimits{}) {
			return DirectionLimits{}, invalid("Datagram requires neutral reliable send limits")
		}
		a, b := local.Datagram, peer.Datagram
		result.Datagram = DatagramLimits{
			DatagramWindow: min(a.DatagramWindow, b.DatagramWindow), GroupWindow: min(a.GroupWindow, b.GroupWindow),
			MaxDatagramBytes: min(a.MaxDatagramBytes, b.MaxDatagramBytes), MaxFragments: min(a.MaxFragments, b.MaxFragments),
			MaxDescriptors: min(a.MaxDescriptors, b.MaxDescriptors), MaxDatagramAssemblies: min(a.MaxDatagramAssemblies, b.MaxDatagramAssemblies, a.DatagramWindow, b.DatagramWindow),
		}
		return result, nil
	}
	if local.Datagram != (DatagramLimits{}) || local.Streams.SessionBytes < 1 || local.Streams.SessionBytes > maxReceiveBytes || local.Streams.StreamBytes < 1 || local.Streams.StreamBytes > local.Streams.SessionBytes {
		return DirectionLimits{}, invalid("invalid reliable send limits")
	}
	s := local.Streams
	if self.Mux == MuxOff {
		if s.MaxOpenStreams != 0 || s.MaxPendingOpens != 0 {
			return DirectionLimits{}, invalid("raw KCP requires neutral mux send limits")
		}
	} else if s.MaxOpenStreams < 1 || s.MaxOpenStreams > 4096 || s.MaxPendingOpens < 1 || s.MaxPendingOpens > 128 {
		return DirectionLimits{}, invalid("invalid mux send limits")
	}
	result.ControlReceiveReserve = peer.Streams.ControlReceiveReserve
	result.BusinessSessionBytes = min(s.SessionBytes, peer.Streams.SessionReceiveBytes-peer.Streams.ControlReceiveReserve)
	result.Streams = StreamSendLimits{
		SessionBytes:    min(s.SessionBytes, peer.Streams.SessionReceiveBytes),
		StreamBytes:     min(s.StreamBytes, peer.Streams.StreamReceiveBytes, result.BusinessSessionBytes),
		MaxOpenStreams:  min(s.MaxOpenStreams, peer.Streams.MaxBusinessStreams),
		MaxPendingOpens: min(s.MaxPendingOpens, peer.Streams.MaxPendingOpens, peer.Streams.MaxPendingAccepts),
	}
	return result, nil
}
