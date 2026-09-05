package wirev2

import (
	"crypto/hmac"
	"crypto/sha256"
	"hash"
)

// AuthenticatorStateBytes is the prepaid allowance for one directional owner.
// Go 1.24/1.26 native HMAC-SHA256, including native FIPS, retains one HMAC,
// two SHA256 digests, and two 108-byte marshaled keyed states after Reset.
// Together with this wrapper and allocator rounding, these are below 1 KiB;
// 4 KiB also covers the old pads during initialization. This is a retained
// state allowance, not an RSS or opaque alternate-provider heap guarantee.
const AuthenticatorStateBytes = 4096

// Authenticator owns one directional key and reusable HMAC working state.
// Its owner must serialize every method, including Close, and must not copy
// it after construction. It has no cache, background work, or packet references.
// Concurrent callers can continue to use the stateless wire functions.
type Authenticator struct {
	key    Key
	mac    hash.Hash
	prefix [PrefixSize]byte
	tag    [AuthenticationTagSize]byte
}

// NewAuthenticator copies a nonzero key and prepares its standing hash state.
// The caller must prepay AuthenticatorStateBytes before constructing it.
func NewAuthenticator(key Key) (*Authenticator, error) {
	if key == (Key{}) {
		return nil, ErrInvalidKey
	}
	a := &Authenticator{key: key, mac: hmac.New(sha256.New, key[:])}
	// Native Go HMAC prepares reusable keyed digest snapshots on the first Reset.
	a.mac.Reset()
	return a, nil
}

// AppendFECBundle preserves the stateless encoder's validation and ownership
// guarantees while using this owner's directional key and working state.
func (a *Authenticator) AppendFECBundle(dst []byte, bundle FECBundle, lookup ContextLookup, maxPayload int) ([]byte, error) {
	if a == nil || a.mac == nil {
		return dst, ErrInvalidKey
	}
	return appendFECBundle(dst, bundle, lookup, a.key, maxPayload, a)
}

// Authenticate preserves Envelope.Authenticate's validation order and borrowed
// packet ownership. Success authenticates bytes without authorizing admission.
func (a *Authenticator) Authenticate(envelope Envelope) (AuthenticatedEnvelope, error) {
	if a == nil || a.mac == nil {
		return AuthenticatedEnvelope{}, ErrInvalidKey
	}
	return envelope.authenticate(a.key, a)
}

// Close clears owned key/scratch arrays and drops the hash references. The
// standard hash API cannot explicitly erase opaque internal keyed state.
func (a *Authenticator) Close() {
	if a == nil {
		return
	}
	clear(a.key[:])
	clear(a.prefix[:])
	clear(a.tag[:])
	a.mac = nil
}
