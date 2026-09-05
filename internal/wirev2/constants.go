package wirev2

const (
	Magic                       = "MPUD"
	Version               uint8 = 2
	PrefixSize                  = 24
	AuthenticationTagSize       = 32
	EnvelopeOverhead            = PrefixSize + AuthenticationTagSize
	RouteSize                   = 16
	TypedBodyOverhead           = EnvelopeOverhead + RouteSize
	MaxUDPPayload               = 65507
	MaxBodySize                 = MaxUDPPayload - EnvelopeOverhead
	HandshakePacketSize         = 512
	HandshakeBodySize           = HandshakePacketSize - EnvelopeOverhead
	HandshakeFixedSize          = 84
	MaxTLVBytes                 = HandshakeBodySize - HandshakeFixedSize
	MaxTLVs                     = 16
	RequiredTLVBytes            = 149
)

type PacketType uint8

const (
	TypeHello                 PacketType = 1
	TypeChallenge             PacketType = 2
	TypeFinish                PacketType = 3
	TypeReady                 PacketType = 4
	TypeReject                PacketType = 5
	TypePathJoin              PacketType = 16
	TypePathChallenge         PacketType = 17
	TypePathConfirm           PacketType = 18
	TypePathReady             PacketType = 19
	TypePathRebindHint        PacketType = 20
	TypeHealthPing            PacketType = 21
	TypeHealthPong            PacketType = 22
	TypeMTUProbe              PacketType = 23
	TypeMTUProbeAck           PacketType = 24
	TypePathBudgetUpdate      PacketType = 25
	TypePathBudgetAck         PacketType = 26
	TypeEncodingContext       PacketType = 27
	TypeEncodingContextAck    PacketType = 28
	TypeFECBundle             PacketType = 32
	TypeDatagramComplete      PacketType = 33
	TypeGroupMissing          PacketType = 34
	TypeDatagramStatusRequest PacketType = 35
	TypeGroupMigrate          PacketType = 36
	TypeGroupMigrateAck       PacketType = 37
	TypeDatagramExpired       PacketType = 38
	TypeKCPBundle             PacketType = 48
	TypeClose                 PacketType = 63
)

func (t PacketType) IsHandshake() bool { return t >= TypeHello && t <= TypeReject }

func knownPacketType(t PacketType) bool {
	return t.IsHandshake() || (t >= TypePathJoin && t <= TypeEncodingContextAck) ||
		(t >= TypeFECBundle && t <= TypeDatagramExpired) || t == TypeKCPBundle || t == TypeClose
}

type TLVType uint16

const (
	TLVProtocol       TLVType = 0x8001
	TLVCapabilities   TLVType = 0x8002
	TLVLayout         TLVType = 0x8003
	TLVFEC            TLVType = 0x8004
	TLVMux            TLVType = 0x8005
	TLVMTUStrategy    TLVType = 0x8006
	TLVPayloadLimits  TLVType = 0x8007
	TLVDatagramLimits TLVType = 0x8008
	TLVRepair         TLVType = 0x8009
	TLVStreamLimits   TLVType = 0x800a
	TLVEpochLimits    TLVType = 0x800b
	TLVPaths          TLVType = 0x800c
	TLVError          TLVType = 0x800d
)

func tlvLength(t TLVType) (int, bool) {
	switch t {
	case TLVProtocol:
		return 1, true
	case TLVCapabilities:
		return 16, true
	case TLVLayout, TLVFEC, TLVMux, TLVMTUStrategy:
		return 2, true
	case TLVPayloadLimits, TLVRepair, TLVEpochLimits:
		return 8, true
	case TLVDatagramLimits:
		return 20, true
	case TLVStreamLimits:
		return 28, true
	case TLVPaths, TLVError:
		return 4, true
	default:
		return 0, false
	}
}
