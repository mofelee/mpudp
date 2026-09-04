package mpudp

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
)

const maxSessionIDAttempts = 8

// SessionID identifies a logical Session independently of any UDP five-tuple.
// It is generated at runtime and is never accepted from configuration.
type SessionID [16]byte

// NewSessionID returns a SessionID generated from crypto/rand.Reader.
func NewSessionID() (SessionID, error) {
	return newSessionID(cryptorand.Reader)
}

func newSessionID(reader io.Reader) (SessionID, error) {
	for attempt := 0; attempt < maxSessionIDAttempts; attempt++ {
		var id SessionID
		if _, err := io.ReadFull(reader, id[:]); err != nil {
			return SessionID{}, fmt.Errorf("generate MPUDP session ID: %w", err)
		}
		if id != (SessionID{}) {
			return id, nil
		}
	}
	return SessionID{}, fmt.Errorf("generate MPUDP session ID: random source returned an all-zero value %d times", maxSessionIDAttempts)
}

// String returns the 32-character hexadecimal Session ID.
func (id SessionID) String() string {
	return hex.EncodeToString(id[:])
}
