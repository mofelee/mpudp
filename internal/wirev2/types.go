package wirev2

type SessionID [16]byte
type Nonce [16]byte
type Digest [32]byte
type Key [32]byte

type Header struct {
	Type      PacketType
	SessionID SessionID
}

// Envelope is a structurally checked, unauthenticated, borrowed packet view.
// Its zero value is invalid. The packet must remain immutable for its lifetime.
type Envelope struct {
	packet []byte
	header Header
}

// Header returns untrusted metadata for read-only key/session lookup only.
func (e Envelope) Header() Header { return e.header }

// AuthenticatedEnvelope can only be obtained through successful authentication.
// It borrows the same immutable packet as Envelope. Its zero value is invalid.
type AuthenticatedEnvelope struct {
	envelope Envelope
	verified bool
}

func (e AuthenticatedEnvelope) Header() Header { return e.envelope.header }

// Body returns a read-only borrowed view, or nil for an invalid zero value.
// Type-specific semantic validation is still required before using its fields.
func (e AuthenticatedEnvelope) Body() []byte {
	if !e.verified {
		return nil
	}
	return e.envelope.packet[PrefixSize : len(e.envelope.packet)-AuthenticationTagSize]
}

// TLV values are borrowed by encoding and owned by decoded Handshakes.
type TLV struct {
	Type  TLVType
	Value []byte
}

type Handshake struct {
	Header           Header
	ClientNonce      Nonce
	ServerNonce      Nonce
	TranscriptDigest Digest
	ReturnPathToken  Nonce
	TLVs             []TLV
}

type DirectionalKeys struct {
	ClientToServer Key
	ServerToClient Key
}
