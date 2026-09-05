package wirev2

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"slices"
)

// ParseEnvelope performs bounded framing checks without authenticating,
// allocating protocol state, or validating any type-specific body semantics.
// It borrows packet; see the package's immutable-buffer ownership contract.
func ParseEnvelope(packet []byte) (Envelope, error) {
	if len(packet) > MaxUDPPayload {
		return Envelope{}, ErrPacketTooLarge
	}
	if len(packet) < EnvelopeOverhead || string(packet[:4]) != Magic {
		return Envelope{}, ErrMalformed
	}
	if packet[4] != Version {
		return Envelope{}, ErrUnsupportedVersion
	}
	header := Header{Type: PacketType(packet[5])}
	copy(header.SessionID[:], packet[8:24])
	bodySize := int(binary.BigEndian.Uint16(packet[6:8]))
	if bodySize+EnvelopeOverhead != len(packet) {
		return Envelope{}, ErrMalformed
	}
	if err := validateEnvelope(header, bodySize); err != nil {
		return Envelope{}, err
	}
	return Envelope{packet: packet, header: header}, nil
}

// Authenticate verifies the exact prefix and body using constant-time tag
// comparison. Callers select the handshake or sender-direction key; no key
// fallback or version downgrade is attempted. Success does not validate body
// semantics and does not authorize learning an endpoint or allocating state.
func (e Envelope) Authenticate(key Key) (AuthenticatedEnvelope, error) {
	if key == (Key{}) {
		return AuthenticatedEnvelope{}, ErrInvalidKey
	}
	if len(e.packet) < EnvelopeOverhead {
		return AuthenticatedEnvelope{}, ErrMalformed
	}
	tagOffset := len(e.packet) - AuthenticationTagSize
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(e.packet[:tagOffset])
	if !hmac.Equal(mac.Sum(nil), e.packet[tagOffset:]) {
		return AuthenticatedEnvelope{}, ErrAuthentication
	}
	return AuthenticatedEnvelope{envelope: e, verified: true}, nil
}

// AppendEnvelope appends framing and authentication around a caller-supplied
// body. It checks only envelope shape; callers must validate typed bodies.
// On error dst is unchanged. Body is borrowed for the duration of the call
// and may alias dst, including spare capacity, because it is copied first.
func AppendEnvelope(dst []byte, header Header, body []byte, key Key) ([]byte, error) {
	if key == (Key{}) {
		return dst, ErrInvalidKey
	}
	if err := validateEnvelope(header, len(body)); err != nil {
		return dst, err
	}
	var prefix [PrefixSize]byte
	copy(prefix[:4], Magic)
	prefix[4] = Version
	prefix[5] = byte(header.Type)
	binary.BigEndian.PutUint16(prefix[6:8], uint16(len(body)))
	copy(prefix[8:], header.SessionID[:])
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(prefix[:])
	_, _ = mac.Write(body)
	var tag [AuthenticationTagSize]byte
	mac.Sum(tag[:0])
	start := len(dst)
	dst = slices.Grow(dst, PrefixSize+len(body)+AuthenticationTagSize)
	dst = dst[:start+PrefixSize+len(body)+AuthenticationTagSize]
	// copy handles overlap; prefix/tag are written only after body is preserved.
	copy(dst[start+PrefixSize:], body)
	copy(dst[start:], prefix[:])
	copy(dst[len(dst)-AuthenticationTagSize:], tag[:])
	return dst, nil
}

func validateEnvelope(header Header, bodySize int) error {
	if !knownPacketType(header.Type) {
		return ErrUnknownPacketType
	}
	if header.SessionID == (SessionID{}) {
		return ErrInvalidSessionID
	}
	if bodySize > MaxBodySize {
		return ErrPacketTooLarge
	}
	if (header.Type.IsHandshake() && bodySize != HandshakeBodySize) ||
		(!header.Type.IsHandshake() && bodySize < RouteSize) {
		return ErrMalformed
	}
	return nil
}
