package wirev2

import "crypto/sha256"

// Transcript owns the binding values from an authenticated HELLO/CHALLENGE
// pair. It does not represent a negotiated, admitted, or replay-safe Session.
// A future runtime must retain it only within the original pending deadline.
type Transcript struct {
	sessionID       SessionID
	clientNonce     Nonce
	serverNonce     Nonce
	returnPathToken Nonce
	digest          Digest
	valid           bool
}

func (t Transcript) Digest() Digest { return t.digest }

// HandshakeDigest hashes the exact prefix+body, excluding the tag, after
// authentication and canonical handshake validation. It supplies SHA256(H)
// for CHALLENGE/REJECT and never hashes a reconstructed representation.
func HandshakeDigest(envelope AuthenticatedEnvelope) (Digest, error) {
	if _, err := DecodeHandshake(envelope); err != nil {
		return Digest{}, err
	}
	return sha256.Sum256(envelope.envelope.packet[:HandshakePacketSize-AuthenticationTagSize]), nil
}

// NewTranscript checks the echoed client nonce, SessionID and SHA256(H), then
// computes T=SHA256(H||C) over the exact authenticated bytes without tags.
// It does not compare the offered/selected contract or create replay state.
func NewTranscript(hello, challenge AuthenticatedEnvelope) (Transcript, error) {
	h, err := DecodeHandshake(hello)
	if err != nil {
		return Transcript{}, err
	}
	c, err := DecodeHandshake(challenge)
	if err != nil {
		return Transcript{}, err
	}
	if h.Header.Type != TypeHello || c.Header.Type != TypeChallenge ||
		h.Header.SessionID != c.Header.SessionID || h.ClientNonce != c.ClientNonce {
		return Transcript{}, ErrTranscript
	}
	hBytes := hello.envelope.packet[:HandshakePacketSize-AuthenticationTagSize]
	cBytes := challenge.envelope.packet[:HandshakePacketSize-AuthenticationTagSize]
	if c.TranscriptDigest != sha256.Sum256(hBytes) {
		return Transcript{}, ErrTranscript
	}
	hash := sha256.New()
	_, _ = hash.Write(hBytes)
	_, _ = hash.Write(cBytes)
	var digest Digest
	hash.Sum(digest[:0])
	return Transcript{
		sessionID:       h.Header.SessionID,
		clientNonce:     h.ClientNonce,
		serverNonce:     c.ServerNonce,
		returnPathToken: c.ReturnPathToken,
		digest:          digest,
		valid:           true,
	}, nil
}

// Confirmation constructs FINISH or READY from this transcript. The caller
// must authenticate it with the matching directional key, never K_hs.
func (t Transcript) Confirmation(packetType PacketType) (Handshake, error) {
	if !t.valid || (packetType != TypeFinish && packetType != TypeReady) {
		return Handshake{}, ErrTranscript
	}
	return Handshake{
		Header:           Header{Type: packetType, SessionID: t.sessionID},
		ClientNonce:      t.clientNonce,
		ServerNonce:      t.serverNonce,
		TranscriptDigest: t.digest,
		ReturnPathToken:  t.returnPathToken,
	}, nil
}

// ValidateConfirmation binds an already authenticated FINISH/READY to this
// transcript. The caller must still check expected type, sender key, source
// tuple, pending lifetime and state transition before any runtime mutation.
func (t Transcript) ValidateConfirmation(envelope AuthenticatedEnvelope) error {
	if !t.valid {
		return ErrTranscript
	}
	message, err := DecodeHandshake(envelope)
	if err != nil {
		return err
	}
	if (message.Header.Type != TypeFinish && message.Header.Type != TypeReady) ||
		message.Header.SessionID != t.sessionID || message.ClientNonce != t.clientNonce ||
		message.ServerNonce != t.serverNonce || message.ReturnPathToken != t.returnPathToken ||
		message.TranscriptDigest != t.digest {
		return ErrTranscript
	}
	return nil
}

// ValidateReject binds an authenticated REJECT to an authenticated HELLO.
// The caller must require a still-live matching attempt; rejection never
// authorizes protocol downgrade or restarts the original deadline.
func ValidateReject(hello, reject AuthenticatedEnvelope) error {
	h, err := DecodeHandshake(hello)
	if err != nil {
		return err
	}
	r, err := DecodeHandshake(reject)
	if err != nil {
		return err
	}
	if h.Header.Type != TypeHello || r.Header.Type != TypeReject ||
		h.Header.SessionID != r.Header.SessionID || h.ClientNonce != r.ClientNonce ||
		r.TranscriptDigest != sha256.Sum256(hello.envelope.packet[:HandshakePacketSize-AuthenticationTagSize]) {
		return ErrTranscript
	}
	return nil
}
