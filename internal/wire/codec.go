package wire

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
)

// EncodedLen returns the exact authenticated wire length of message.
func EncodedLen(message Message) (int, error) {
	if err := validateMessage(message); err != nil {
		return 0, err
	}

	bodySize := 0
	switch message.Header.Type {
	case TypeHello, TypeHelloAck:
		bodySize = helloBodySize
	case TypeDataShard:
		bodySize = DataShardMetadataSize + len(message.DataShard.Payload)
	case TypePing, TypePong:
		bodySize = probeBodySize
	case TypeClose:
		bodySize = 0
	default:
		return 0, ErrUnknownPacketType
	}
	packetSize := PrefixSize + bodySize + AuthenticationTagSize
	if bodySize > int(^uint16(0)) || packetSize > MaxUDPPayload {
		return 0, ErrPacketTooLarge
	}
	return packetSize, nil
}

// AppendAuthenticated appends the canonical authenticated encoding of message
// to dst. It returns dst unchanged on every validation error.
func AppendAuthenticated(dst []byte, message Message, psk []byte, budget int) ([]byte, error) {
	if len(psk) == 0 {
		return dst, ErrInvalidKey
	}
	if err := validateBudget(budget); err != nil {
		return dst, err
	}
	packetSize, err := EncodedLen(message)
	if err != nil {
		return dst, err
	}
	if packetSize > budget {
		return dst, ErrPacketTooLarge
	}

	start := len(dst)
	dst = append(dst, make([]byte, packetSize)...)
	packet := dst[start:]
	packet[0], packet[1], packet[2], packet[3] = magic0, magic1, magic2, magic3
	packet[4] = Version
	packet[5] = byte(message.Header.Type)
	bodySize := packetSize - PrefixSize - AuthenticationTagSize
	binary.BigEndian.PutUint16(packet[6:8], uint16(bodySize))
	copy(packet[8:24], message.Header.SessionID[:])

	body := packet[PrefixSize : PrefixSize+bodySize]
	switch message.Header.Type {
	case TypeHello, TypeHelloAck:
		body[0] = message.Handshake.DataShards
		body[1] = message.Handshake.ParityShards
		binary.BigEndian.PutUint16(body[2:4], message.Handshake.MaxUDPPayload)
	case TypeDataShard:
		binary.BigEndian.PutUint64(body[0:8], message.DataShard.PacketID)
		body[8] = message.DataShard.DataShards
		body[9] = message.DataShard.ParityShards
		body[10] = message.DataShard.ShardIndex
		binary.BigEndian.PutUint32(body[11:15], message.DataShard.OriginalLength)
		copy(body[15:], message.DataShard.Payload)
	case TypePing, TypePong:
		binary.BigEndian.PutUint64(body[0:8], message.Probe.Token)
		binary.BigEndian.PutUint64(body[8:16], message.Probe.Timestamp)
	case TypeClose:
	}

	mac := hmac.New(sha256.New, psk)
	_, _ = mac.Write(packet[:PrefixSize+bodySize])
	copy(packet[PrefixSize+bodySize:], mac.Sum(nil))
	return dst, nil
}

// DecodeAuthenticated validates and authenticates one complete UDP payload.
// The returned DATA_SHARD Payload aliases datagram; callers retaining it must
// keep datagram alive or copy the payload after this function succeeds.
func DecodeAuthenticated(datagram, psk []byte, receiveLimit int) (Message, error) {
	if len(psk) == 0 {
		return Message{}, ErrInvalidKey
	}
	if err := validateBudget(receiveLimit); err != nil {
		return Message{}, err
	}
	if len(datagram) > receiveLimit || len(datagram) > MaxUDPPayload {
		return Message{}, ErrPacketTooLarge
	}
	if len(datagram) < MinimumPacketSize {
		return Message{}, ErrMalformed
	}
	if datagram[0] != magic0 || datagram[1] != magic1 || datagram[2] != magic2 || datagram[3] != magic3 {
		return Message{}, ErrMalformed
	}
	if datagram[4] != Version {
		return Message{}, ErrUnsupportedVersion
	}
	packetType := PacketType(datagram[5])
	if !knownPacketType(packetType) {
		return Message{}, ErrUnknownPacketType
	}
	bodySize := int(binary.BigEndian.Uint16(datagram[6:8]))
	tagOffset := PrefixSize + bodySize
	if tagOffset+AuthenticationTagSize != len(datagram) {
		return Message{}, ErrMalformed
	}

	var sessionID SessionID
	copy(sessionID[:], datagram[8:24])
	if sessionID == (SessionID{}) {
		return Message{}, ErrInvalidSessionID
	}

	mac := hmac.New(sha256.New, psk)
	_, _ = mac.Write(datagram[:tagOffset])
	if !hmac.Equal(mac.Sum(nil), datagram[tagOffset:]) {
		return Message{}, ErrAuthentication
	}

	message := Message{Header: Header{Type: packetType, SessionID: sessionID}}
	body := datagram[PrefixSize:tagOffset]
	switch packetType {
	case TypeHello, TypeHelloAck:
		if len(body) != helloBodySize {
			return Message{}, ErrMalformed
		}
		message.Handshake = Handshake{
			DataShards:    body[0],
			ParityShards:  body[1],
			MaxUDPPayload: binary.BigEndian.Uint16(body[2:4]),
		}
	case TypeDataShard:
		if len(body) < DataShardMetadataSize {
			return Message{}, ErrMalformed
		}
		message.DataShard = DataShard{
			PacketID:       binary.BigEndian.Uint64(body[0:8]),
			DataShards:     body[8],
			ParityShards:   body[9],
			ShardIndex:     body[10],
			OriginalLength: binary.BigEndian.Uint32(body[11:15]),
			Payload:        body[15:],
		}
	case TypePing, TypePong:
		if len(body) != probeBodySize {
			return Message{}, ErrMalformed
		}
		message.Probe = Probe{
			Token:     binary.BigEndian.Uint64(body[0:8]),
			Timestamp: binary.BigEndian.Uint64(body[8:16]),
		}
	case TypeClose:
		if len(body) != 0 {
			return Message{}, ErrMalformed
		}
	}
	if err := validateMessage(message); err != nil {
		return Message{}, err
	}
	return message, nil
}

// DecodeAndDispatch invokes handler exactly once and only after the packet has
// passed complete envelope, authentication, and semantic validation.
func DecodeAndDispatch(datagram, psk []byte, receiveLimit int, handler func(Message) error) error {
	if handler == nil {
		return ErrInvalidHandler
	}
	message, err := DecodeAuthenticated(datagram, psk, receiveLimit)
	if err != nil {
		return err
	}
	return handler(message)
}

func validateMessage(message Message) error {
	if message.Header.SessionID == (SessionID{}) {
		return ErrInvalidSessionID
	}
	if !knownPacketType(message.Header.Type) {
		return ErrUnknownPacketType
	}

	switch message.Header.Type {
	case TypeHello, TypeHelloAck:
		if !zeroDataShard(message.DataShard) || message.Probe != (Probe{}) {
			return ErrMalformed
		}
		if err := validateFEC(message.Handshake.DataShards, message.Handshake.ParityShards); err != nil {
			return err
		}
		if err := validateBudget(int(message.Handshake.MaxUDPPayload)); err != nil {
			return err
		}
	case TypeDataShard:
		if message.Handshake != (Handshake{}) || message.Probe != (Probe{}) {
			return ErrMalformed
		}
		if err := validateFEC(message.DataShard.DataShards, message.DataShard.ParityShards); err != nil {
			return err
		}
		total := int(message.DataShard.DataShards) + int(message.DataShard.ParityShards)
		if int(message.DataShard.ShardIndex) >= total {
			return ErrInvalidFEC
		}
		payloadSize := len(message.DataShard.Payload)
		if message.DataShard.OriginalLength == 0 {
			if payloadSize != 1 || message.DataShard.Payload[0] != 0 {
				return ErrMalformed
			}
		} else {
			expected := 1 + (uint64(message.DataShard.OriginalLength)-1)/uint64(message.DataShard.DataShards)
			if uint64(payloadSize) != expected {
				return ErrMalformed
			}
		}
	case TypePing, TypePong:
		if message.Handshake != (Handshake{}) || !zeroDataShard(message.DataShard) {
			return ErrMalformed
		}
		if message.Probe.Token == 0 {
			return ErrMalformed
		}
	case TypeClose:
		if message.Handshake != (Handshake{}) || !zeroDataShard(message.DataShard) || message.Probe != (Probe{}) {
			return ErrMalformed
		}
	}
	return nil
}

func validateFEC(dataShards, parityShards uint8) error {
	if dataShards == 0 || parityShards == 0 || int(dataShards)+int(parityShards) > 256 {
		return ErrInvalidFEC
	}
	return nil
}

func validateBudget(budget int) error {
	if budget < MinUDPPayload || budget > MaxUDPPayload {
		return ErrInvalidCapability
	}
	return nil
}

func knownPacketType(packetType PacketType) bool {
	return packetType >= TypeHello && packetType <= TypeClose
}

func zeroDataShard(data DataShard) bool {
	return data.PacketID == 0 && data.DataShards == 0 && data.ParityShards == 0 && data.ShardIndex == 0 && data.OriginalLength == 0 && len(data.Payload) == 0
}
