package wirev2

import (
	"encoding/binary"
	"slices"
)

// AppendHandshake validates canonical handshake shape and appends exactly 512
// bytes. Select K_hs for HELLO/CHALLENGE/REJECT, K_c2s for FINISH, and K_s2c
// for READY. Contract negotiation and live-attempt checks belong to the caller.
// It borrows message.TLVs only during the call and leaves dst unchanged on error.
func AppendHandshake(dst []byte, message Handshake, key Key) ([]byte, error) {
	if err := validateHandshake(message); err != nil {
		return dst, err
	}
	var body [HandshakeBodySize]byte
	copy(body[0:16], message.ClientNonce[:])
	copy(body[16:32], message.ServerNonce[:])
	copy(body[32:64], message.TranscriptDigest[:])
	copy(body[64:80], message.ReturnPathToken[:])
	offset := HandshakeFixedSize
	for _, tlv := range message.TLVs {
		binary.BigEndian.PutUint16(body[offset:offset+2], uint16(tlv.Type))
		binary.BigEndian.PutUint16(body[offset+2:offset+4], uint16(len(tlv.Value)))
		copy(body[offset+4:], tlv.Value)
		offset += 4 + len(tlv.Value)
	}
	binary.BigEndian.PutUint16(body[80:82], uint16(offset-HandshakeFixedSize))
	return AppendEnvelope(dst, message.Header, body[:], key)
}

// DecodeHandshake requires an authenticated envelope and returns owned TLV
// values. It validates reserved fields, padding, canonical ordering, required
// TLV presence and registered lengths, but not endpoint policy negotiation.
func DecodeHandshake(envelope AuthenticatedEnvelope) (Handshake, error) {
	if !envelope.verified {
		return Handshake{}, ErrAuthentication
	}
	if !envelope.Header().Type.IsHandshake() {
		return Handshake{}, ErrUnknownPacketType
	}
	body := envelope.Body()
	if len(body) != HandshakeBodySize {
		return Handshake{}, ErrMalformed
	}
	tlvBytes := int(binary.BigEndian.Uint16(body[80:82]))
	if tlvBytes > MaxTLVBytes || binary.BigEndian.Uint16(body[82:84]) != 0 ||
		!allZero(body[HandshakeFixedSize+tlvBytes:]) {
		return Handshake{}, ErrMalformed
	}
	message := Handshake{Header: envelope.Header()}
	copy(message.ClientNonce[:], body[0:16])
	copy(message.ServerNonce[:], body[16:32])
	copy(message.TranscriptDigest[:], body[32:64])
	copy(message.ReturnPathToken[:], body[64:80])
	remaining := body[HandshakeFixedSize : HandshakeFixedSize+tlvBytes]
	for len(remaining) > 0 {
		if len(remaining) < 4 || len(message.TLVs) == MaxTLVs {
			return Handshake{}, ErrInvalidTLV
		}
		valueBytes := int(binary.BigEndian.Uint16(remaining[2:4]))
		if valueBytes > len(remaining)-4 {
			return Handshake{}, ErrInvalidTLV
		}
		message.TLVs = append(message.TLVs, TLV{
			Type:  TLVType(binary.BigEndian.Uint16(remaining[:2])),
			Value: remaining[4 : 4+valueBytes],
		})
		remaining = remaining[4+valueBytes:]
	}
	if err := validateHandshake(message); err != nil {
		return Handshake{}, err
	}
	for i := range message.TLVs {
		message.TLVs[i].Value = slices.Clone(message.TLVs[i].Value)
	}
	return message, nil
}

func validateHandshake(message Handshake) error {
	if !message.Header.Type.IsHandshake() {
		return ErrUnknownPacketType
	}
	if message.Header.SessionID == (SessionID{}) {
		return ErrInvalidSessionID
	}
	if message.ClientNonce == (Nonce{}) {
		return ErrMalformed
	}
	switch message.Header.Type {
	case TypeHello:
		if message.ServerNonce != (Nonce{}) || message.TranscriptDigest != (Digest{}) ||
			message.ReturnPathToken != (Nonce{}) {
			return ErrMalformed
		}
	case TypeReject:
		if message.ServerNonce != (Nonce{}) || message.ReturnPathToken != (Nonce{}) {
			return ErrMalformed
		}
	}
	return validateTLVs(message.Header.Type, message.TLVs)
}

func validateTLVs(packetType PacketType, tlvs []TLV) error {
	if len(tlvs) > MaxTLVs {
		return ErrInvalidTLV
	}
	if packetType == TypeFinish || packetType == TypeReady {
		if len(tlvs) != 0 {
			return ErrInvalidTLV
		}
		return nil
	}
	if packetType == TypeReject && (len(tlvs) != 1 || tlvs[0].Type != TLVError) {
		return ErrInvalidTLV
	}
	var seen uint16
	total := 0
	for i, tlv := range tlvs {
		if i > 0 && tlv.Type <= tlvs[i-1].Type {
			return ErrInvalidTLV
		}
		if len(tlv.Value) > MaxTLVBytes-4 || total > MaxTLVBytes-4-len(tlv.Value) {
			return ErrInvalidTLV
		}
		total += 4 + len(tlv.Value)
		if length, known := tlvLength(tlv.Type); known {
			if len(tlv.Value) != length || (packetType != TypeReject && tlv.Type == TLVError) {
				return ErrInvalidTLV
			}
			if tlv.Type >= TLVProtocol && tlv.Type <= TLVPaths {
				seen |= 1 << (tlv.Type - TLVProtocol)
			}
			if tlv.Type == TLVRepair && !allZero(tlv.Value[6:8]) {
				return ErrInvalidTLV
			}
			if tlv.Type == TLVError {
				code := binary.BigEndian.Uint16(tlv.Value[:2])
				if code < 1 || code > 9 || binary.BigEndian.Uint16(tlv.Value[2:]) > 1000 {
					return ErrInvalidTLV
				}
			}
		} else if tlv.Type&0x8000 != 0 {
			return ErrRequiredTLV
		}
	}
	if packetType != TypeReject && seen != (1<<12)-1 {
		return ErrInvalidTLV
	}
	return nil
}

func allZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}
