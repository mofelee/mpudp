// Package negotiationv2 validates and selects the proposed v2 handshake
// contract. It does not authenticate, retain attempts, reserve resources, or
// activate a Session. Callers must first validate the wire handshake and its
// transcript, and must perform admission before using any negotiated limits.
package negotiationv2

import "errors"

var (
	ErrInvalid               = errors.New("invalid MPUDP v2 contract")
	ErrIncompatible          = errors.New("incompatible MPUDP v2 contract")
	ErrUnsupportedCapability = errors.New("unsupported required MPUDP v2 capability")
)

type Protocol uint8

const (
	Datagram Protocol = 1
	KCP      Protocol = 2
)

type Capabilities uint64

const (
	FragmentManifest Capabilities = 1 << iota
	Aggregation
	DatagramRepair
	NativeKCP
	KCPPacketPieces
	SmuxAdmission
	PLPMTUD
	PerCarrierBudget
	GroupMigration
	KnownCapabilities = FragmentManifest | Aggregation | DatagramRepair | NativeKCP | KCPPacketPieces | SmuxAdmission | PLPMTUD | PerCarrierBudget | GroupMigration
)

type MuxProfile uint16

const (
	MuxOff MuxProfile = iota
	SmuxWire2
)

type Discovery uint8

const (
	Fixed Discovery = iota
	ProbeMTU
)

type BudgetScope uint8

const (
	SessionBudget BudgetScope = iota
	CarrierBudget
)

type PayloadLimits struct {
	SendHardCap, ReceiveHardCap   uint16
	BootstrapBytes, InnerKCPBytes uint16
}

type DatagramLimits struct {
	DatagramWindow, GroupWindow, MaxDatagramBytes uint32
	MaxFragments, MaxDescriptors                  uint16
	MaxDatagramAssemblies                         uint32
}

type RepairLimits struct {
	MaxAgeMS    uint32
	MaxAttempts uint16
}

type StreamLimits struct {
	MaxBusinessStreams, MaxPendingAccepts                 uint32
	SessionReceiveBytes, StreamReceiveBytes               uint32
	ControlReceiveReserve, MaxPendingOpens, MaxFrameBytes uint32
}

type EpochLimits struct {
	MaxOldEpochs, MaxMigrations uint16
	GraceMS                     uint32
}

// Profile contains endpoint settings and advertised receive ceilings. It has
// no mutable references. Send limits are supplied independently to EffectiveSend.
type Profile struct {
	Protocol                  Protocol
	OfferedCaps, RequiredCaps Capabilities
	LayoutID                  uint16
	DataShards, ParityShards  uint8
	Mux                       MuxProfile
	Discovery                 Discovery
	Scope                     BudgetScope
	Payload                   PayloadLimits
	Datagram                  DatagramLimits
	Repair                    RepairLimits
	Streams                   StreamLimits
	Epochs                    EpochLimits
	MaxPaths                  uint16
}

// Advertisement adds the configured winning Carrier index, never a remapped ID.
type Advertisement struct {
	Profile
	BootstrapPathID uint16
}

type Role uint8

const (
	Initiator Role = iota + 1
	Responder
)

type StreamSendLimits struct {
	MaxOpenStreams, MaxPendingOpens uint32
	// SessionBytes bounds all locally retained sender bytes, including control.
	// StreamBytes bounds one business stream (the sole stream in raw KCP).
	SessionBytes, StreamBytes uint32
}

// SendLimits are explicit local sender ceilings, independent of what that
// endpoint advertised for its own receive direction. Datagram assemblies here
// bound locally outstanding originals, rather than local receive assemblies.
type SendLimits struct {
	Datagram DatagramLimits
	Streams  StreamSendLimits
}

type DirectionLimits struct {
	MaxUDPPayload uint16
	Datagram      DatagramLimits
	Streams       StreamSendLimits
	// ControlReceiveReserve is the peer's receive floor for the mux control
	// stream. BusinessSessionBytes excludes that floor from peer capacity,
	// then intersects the independent local sender ceiling. Local control
	// storage is charged separately under Streams.SessionBytes by the caller.
	ControlReceiveReserve, BusinessSessionBytes uint32
}

// Contract retains copied endpoint settings. ActiveCaps filters the negotiated
// intersection to capabilities applicable to this mode/strategy; it does not
// enable local aggregation or replace admission/window reservations.
type Contract struct {
	SelectedCaps, ActiveCaps  Capabilities
	MaxPaths, BootstrapPathID uint16
	MuxFrameBytes             uint32
	Repair                    RepairLimits
	Epochs                    EpochLimits
	client, server            Profile
	valid                     bool
}
