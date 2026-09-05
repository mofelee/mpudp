// Package handshakev2 implements the bounded v2 bootstrap state machine.
// It owns no sockets, goroutines, timers, or outbound queue. A single caller
// must serialize every Engine method and supply nondecreasing explicit times.
// Emit, Install, and disposal callbacks must be bounded and must not reenter
// the same Engine. The public fixed Datagram runtime supplies these callbacks.
package handshakev2

import (
	"errors"
	"io"
	"net/netip"
	"time"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

const (
	Lifetime               = 10 * time.Second
	RetryInterval          = time.Second
	MaxSends               = 8
	MaxPending             = 256
	MaxRejections          = 256
	MaxInitialReservations = 16
	PacketReservationBytes = 4 * wirev2.HandshakePacketSize
)

var (
	ErrInvalid      = errors.New("invalid v2 handshake operation")
	ErrClosed       = errors.New("v2 handshake engine closed")
	ErrReentrant    = errors.New("v2 handshake callback reentered engine")
	ErrTime         = errors.New("v2 handshake time moved backwards")
	ErrExpired      = errors.New("v2 handshake deadline expired")
	ErrCancelled    = errors.New("v2 handshake cancelled")
	ErrRejected     = errors.New("v2 handshake rejected")
	ErrEntropy      = errors.New("v2 handshake entropy unavailable")
	ErrInstallation = errors.New("v2 handshake installation failed")
	ErrExhausted    = errors.New("v2 handshake identifier exhausted")
)

// Binding identifies the receiving socket incarnation and complete local and
// remote address tuple. SocketID must not be reused while retained by Engine.
// Local is the actual packet destination/source, including wildcard-listener
// packet-info selection, not an unspecified listening address.
type Binding struct {
	SocketID uint64
	Local    netip.AddrPort
	Remote   netip.AddrPort
}

func validAddress(address netip.AddrPort) bool {
	return address.IsValid() && address.Port() != 0 && !address.Addr().IsUnspecified() && !address.Addr().IsMulticast() && address.Addr() == address.Addr().Unmap()
}

type Carrier struct {
	PathID  uint16
	Binding Binding
}

// Policy is copied at admission. Receive and Initial cover the full local initial
// receive-window/backend obligation, independent of the remote's advertised
// maxima. Engine enforces at least both Datagram terminal bitmaps or the full
// advertised KCP SessionReceiveBytes; backend overhead requires extra bytes.
// Engine separately reserves PacketReservationBytes. Listener policy
// requires PendingAccept; initiator policy forbids it. Subsequent streams and
// queued transport copies require their own leases in the installation adapter.
type Policy struct {
	Profile negotiationv2.Profile
	Receive creditv2.Claim
	// Initial partitions prepaid component storage into at most sixteen
	// positive byte-only leases. These bytes supplement Receive, and count
	// toward the same advertised minimum. Install may consume the matching
	// handles without a second reservation. Receive may then be count-only.
	Initial []creditv2.Claim
}

type Config struct {
	Credits  *creditv2.Peer
	Entropy  io.Reader
	Listener *Policy
	// Emit borrows packet only until return. A queued adapter must reserve and
	// copy its own storage before returning. Errors count as attempted sends.
	Emit func(Binding, []byte) error
	// Install runs only after validated FINISH/READY and credit promotion. It
	// installs under already reserved Receive/Initial credits before READY.
	// On error a returned disposer is called; nil means partial storage is
	// already cleared. Success requires a nonnil disposer that clears storage
	// before Engine releases its leases. Extra adapter leases also belong to
	// disposal. Install must not require new mandatory initial reservations.
	Install func(Setup) (dispose func(), err error)
}

type DialID uint64

type DialRequest struct {
	Policy   Policy
	Carriers []Carrier
	// Concurrent defaults to one and cannot exceed the configured path count.
	Concurrent int
	// Deadline is optional and only shortens each attempt's original lifetime.
	Deadline time.Time
}

// Setup is a copied installed contract. Engine retains ownership of its
// initial receive/accept leases; Scope allows the adapter to reserve further
// obligations. CloseSession/Close invoke the supplied disposer before releasing
// initial credits. The adapter must call MarkAccepted only on public acceptance.
type Setup struct {
	ID       wirev2.SessionID
	DialID   DialID
	Role     negotiationv2.Role
	PathID   uint16
	Binding  Binding
	Contract negotiationv2.Contract
	Keys     wirev2.DirectionalKeys
	Scope    *creditv2.Session
	Receive  creditv2.Claim
	// Initial matches Policy.Initial order. Engine retains ownership and
	// releases these leases after disposal; component cleanup may also release
	// copied handles idempotently after clearing their storage.
	Initial []*creditv2.Lease
}

type SendAttempt struct {
	ID     wirev2.SessionID
	Type   wirev2.PacketType
	PathID uint16
	Err    error
}

type Failure struct {
	ID     wirev2.SessionID
	DialID DialID
	Err    error
}

// Result contains bounded metadata and no packet buffers. Established is
// emitted once, after Install succeeds; listener READY has then been attempted.
type Result struct {
	Sends       []SendAttempt
	Established []Setup
	Closed      []wirev2.SessionID
	Failures    []Failure
}

type Snapshot struct {
	Pending, Established, Dials, Rejections int
	PacketBytes                             uint64
	Closed                                  bool
}
