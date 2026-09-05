package wirev2

import (
	"crypto/hkdf"
	"crypto/sha256"
)

// DeriveHandshakeKey uses RFC 5869 HKDF-SHA-256 with empty salt and the exact
// ASCII label MPUDP/v2/handshake, with no NUL terminator.
func DeriveHandshakeKey(psk []byte) (Key, error) {
	return deriveKey(psk, nil, "MPUDP/v2/handshake")
}

// DeriveDirectionalKeys binds both sender roles to one authenticated
// HELLO/CHALLENGE transcript. The transcript must have been checked against
// the caller's live attempt and selected contract before runtime use.
func DeriveDirectionalKeys(psk []byte, transcript Transcript) (DirectionalKeys, error) {
	if !transcript.valid {
		return DirectionalKeys{}, ErrTranscript
	}
	var salt [32]byte
	copy(salt[:16], transcript.clientNonce[:])
	copy(salt[16:], transcript.serverNonce[:])
	context := string(transcript.sessionID[:]) + string(transcript.digest[:])
	c2s, err := deriveKey(psk, salt[:], "MPUDP/v2/c2s"+context)
	if err != nil {
		return DirectionalKeys{}, err
	}
	s2c, err := deriveKey(psk, salt[:], "MPUDP/v2/s2c"+context)
	if err != nil {
		return DirectionalKeys{}, err
	}
	return DirectionalKeys{ClientToServer: c2s, ServerToClient: s2c}, nil
}

func deriveKey(psk, salt []byte, info string) (Key, error) {
	if len(psk) == 0 {
		return Key{}, ErrInvalidKey
	}
	key, err := hkdf.Key(sha256.New, psk, salt, info, 32)
	if err != nil {
		return Key{}, err
	}
	var result Key
	copy(result[:], key)
	clear(key)
	return result, nil
}
