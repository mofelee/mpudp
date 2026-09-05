package wire

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"hash"
)

const authenticationCacheCapacity = 4

// Authenticator owns an immutable key and a concurrency-safe cache of HMAC
// working state. The cache retains at most four states; a busy cache uses a
// fresh state without waiting or limiting concurrent callers. No references to
// caller-owned key or packet buffers are retained. No background work is started.
type Authenticator struct {
	key    []byte
	states chan *authenticationState
}

type authenticationState struct {
	mac hash.Hash
	tag [sha256.Size]byte
}

// NewAuthenticator copies a nonempty key. The returned authenticator may be
// shared for the lifetime of that key.
func NewAuthenticator(psk []byte) (*Authenticator, error) {
	if len(psk) == 0 {
		return nil, ErrInvalidKey
	}
	return &Authenticator{
		key: bytes.Clone(psk), states: make(chan *authenticationState, authenticationCacheCapacity),
	}, nil
}

// Append has the encoding, output ownership, and validation-error guarantees
// of AppendAuthenticated, using this authenticator's immutable key.
func (a *Authenticator) Append(dst []byte, message Message, budget int) ([]byte, error) {
	if a == nil {
		return dst, ErrInvalidKey
	}
	return appendAuthenticated(dst, message, a.key, budget, a)
}

// Decode has the validation and payload-aliasing guarantees of
// DecodeAuthenticated, using this authenticator's immutable key.
func (a *Authenticator) Decode(datagram []byte, receiveLimit int) (Message, error) {
	if a == nil {
		return Message{}, ErrInvalidKey
	}
	return decodeAuthenticated(datagram, a.key, receiveLimit, a)
}

func (a *Authenticator) acquire() *authenticationState {
	select {
	case state := <-a.states:
		state.mac.Reset()
		return state
	default:
		return &authenticationState{mac: hmac.New(sha256.New, a.key)}
	}
}

func (a *Authenticator) release(state *authenticationState) {
	clear(state.tag[:])
	select {
	case a.states <- state:
	default:
	}
}

func (a *Authenticator) sign(packet []byte, tagOffset int) {
	state := a.acquire()
	defer a.release(state)
	_, _ = state.mac.Write(packet[:tagOffset])
	copy(packet[tagOffset:], state.mac.Sum(packet[tagOffset:tagOffset]))
}

func (a *Authenticator) verify(packet []byte, tagOffset int) bool {
	state := a.acquire()
	defer a.release(state)
	_, _ = state.mac.Write(packet[:tagOffset])
	return hmac.Equal(state.mac.Sum(state.tag[:0]), packet[tagOffset:])
}
