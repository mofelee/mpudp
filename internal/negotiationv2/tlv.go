package negotiationv2

import (
	"encoding/binary"

	"github.com/mofelee/mpudp/internal/wirev2"
)

var mandatoryLengths = [...]int{1, 16, 2, 2, 2, 2, 8, 20, 8, 28, 8, 4}

// DecodeHello reads the contract from an already authenticated, structurally
// validated HELLO. Authentication and live-attempt checks remain caller duties.
// Returned values do not retain any reference to message or its TLV buffers.
func DecodeHello(message wirev2.Handshake) (Advertisement, error) {
	return decodeAdvertisement(message, wirev2.TypeHello)
}

// DecodeChallenge reads a CHALLENGE contract. Accept must subsequently check
// its selection against the original HELLO; this function only validates shape
// and normalized endpoint semantics.
func DecodeChallenge(message wirev2.Handshake) (Advertisement, error) {
	return decodeAdvertisement(message, wirev2.TypeChallenge)
}

func decodeAdvertisement(message wirev2.Handshake, packetType wirev2.PacketType) (Advertisement, error) {
	if message.Header.Type != packetType || len(message.TLVs) < len(mandatoryLengths) || len(message.TLVs) > wirev2.MaxTLVs {
		return Advertisement{}, invalid("invalid contract handshake shape")
	}
	var values [len(mandatoryLengths)][]byte
	total := 0
	for i, tlv := range message.TLVs {
		if i > 0 && tlv.Type <= message.TLVs[i-1].Type {
			return Advertisement{}, invalid("noncanonical contract TLV order")
		}
		if len(tlv.Value) > wirev2.MaxTLVBytes-4 || total > wirev2.MaxTLVBytes-4-len(tlv.Value) {
			return Advertisement{}, invalid("contract TLV bytes exceed handshake limit")
		}
		total += 4 + len(tlv.Value)
		if tlv.Type < wirev2.TLVProtocol || tlv.Type > wirev2.TLVPaths {
			if tlv.Type&0x8000 != 0 {
				return Advertisement{}, invalid("unknown required contract TLV")
			}
			continue
		}
		index := int(tlv.Type - wirev2.TLVProtocol)
		if len(tlv.Value) != mandatoryLengths[index] {
			return Advertisement{}, invalid("invalid contract TLV length")
		}
		values[index] = tlv.Value
	}
	for _, value := range values {
		if value == nil {
			return Advertisement{}, invalid("missing required contract TLV")
		}
	}
	if binary.BigEndian.Uint16(values[8][6:]) != 0 {
		return Advertisement{}, invalid("nonzero contract reserved field")
	}
	read16, read32, read64 := binary.BigEndian.Uint16, binary.BigEndian.Uint32, binary.BigEndian.Uint64
	a := Advertisement{Profile: Profile{
		Protocol:    Protocol(values[0][0]),
		OfferedCaps: Capabilities(read64(values[1])), RequiredCaps: Capabilities(read64(values[1][8:])),
		LayoutID: read16(values[2]), DataShards: values[3][0], ParityShards: values[3][1],
		Mux: MuxProfile(read16(values[4])), Discovery: Discovery(values[5][0]), Scope: BudgetScope(values[5][1]),
		Payload: PayloadLimits{
			SendHardCap: read16(values[6]), ReceiveHardCap: read16(values[6][2:]),
			BootstrapBytes: read16(values[6][4:]), InnerKCPBytes: read16(values[6][6:]),
		},
		Datagram: DatagramLimits{
			DatagramWindow: read32(values[7]), GroupWindow: read32(values[7][4:]), MaxDatagramBytes: read32(values[7][8:]),
			MaxFragments: read16(values[7][12:]), MaxDescriptors: read16(values[7][14:]), MaxDatagramAssemblies: read32(values[7][16:]),
		},
		Repair: RepairLimits{MaxAgeMS: read32(values[8]), MaxAttempts: read16(values[8][4:])},
		Streams: StreamLimits{
			MaxBusinessStreams: read32(values[9]), MaxPendingAccepts: read32(values[9][4:]),
			SessionReceiveBytes: read32(values[9][8:]), StreamReceiveBytes: read32(values[9][12:]),
			ControlReceiveReserve: read32(values[9][16:]), MaxPendingOpens: read32(values[9][20:]), MaxFrameBytes: read32(values[9][24:]),
		},
		Epochs:   EpochLimits{MaxOldEpochs: read16(values[10]), MaxMigrations: read16(values[10][2:]), GraceMS: read32(values[10][4:])},
		MaxPaths: read16(values[11]),
	}, BootstrapPathID: read16(values[11][2:])}
	if err := a.validate(); err != nil {
		return Advertisement{}, err
	}
	return a, nil
}

// TLVs returns the twelve canonical mandatory contract TLVs with owned value
// buffers. Optional extensions are not retained or re-emitted; callers must
// preserve the original authenticated wire transcript separately.
func (a Advertisement) TLVs() ([]wirev2.TLV, error) {
	if err := a.validate(); err != nil {
		return nil, err
	}
	tlvs := make([]wirev2.TLV, len(mandatoryLengths))
	for i, length := range mandatoryLengths {
		tlvs[i] = wirev2.TLV{Type: wirev2.TLVProtocol + wirev2.TLVType(i), Value: make([]byte, length)}
	}
	put16, put32, put64 := binary.BigEndian.PutUint16, binary.BigEndian.PutUint32, binary.BigEndian.PutUint64
	tlvs[0].Value[0] = byte(a.Protocol)
	put64(tlvs[1].Value, uint64(a.OfferedCaps))
	put64(tlvs[1].Value[8:], uint64(a.RequiredCaps))
	put16(tlvs[2].Value, a.LayoutID)
	tlvs[3].Value[0], tlvs[3].Value[1] = a.DataShards, a.ParityShards
	put16(tlvs[4].Value, uint16(a.Mux))
	tlvs[5].Value[0], tlvs[5].Value[1] = byte(a.Discovery), byte(a.Scope)
	for i, value := range [...]uint16{a.Payload.SendHardCap, a.Payload.ReceiveHardCap, a.Payload.BootstrapBytes, a.Payload.InnerKCPBytes} {
		put16(tlvs[6].Value[i*2:], value)
	}
	for i, value := range [...]uint32{a.Datagram.DatagramWindow, a.Datagram.GroupWindow, a.Datagram.MaxDatagramBytes} {
		put32(tlvs[7].Value[i*4:], value)
	}
	put16(tlvs[7].Value[12:], a.Datagram.MaxFragments)
	put16(tlvs[7].Value[14:], a.Datagram.MaxDescriptors)
	put32(tlvs[7].Value[16:], a.Datagram.MaxDatagramAssemblies)
	put32(tlvs[8].Value, a.Repair.MaxAgeMS)
	put16(tlvs[8].Value[4:], a.Repair.MaxAttempts)
	for i, value := range [...]uint32{a.Streams.MaxBusinessStreams, a.Streams.MaxPendingAccepts, a.Streams.SessionReceiveBytes, a.Streams.StreamReceiveBytes, a.Streams.ControlReceiveReserve, a.Streams.MaxPendingOpens, a.Streams.MaxFrameBytes} {
		put32(tlvs[9].Value[i*4:], value)
	}
	put16(tlvs[10].Value, a.Epochs.MaxOldEpochs)
	put16(tlvs[10].Value[2:], a.Epochs.MaxMigrations)
	put32(tlvs[10].Value[4:], a.Epochs.GraceMS)
	put16(tlvs[11].Value, a.MaxPaths)
	put16(tlvs[11].Value[2:], a.BootstrapPathID)
	return tlvs, nil
}
