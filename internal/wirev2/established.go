package wirev2

import (
	"encoding/binary"
	"errors"
)

var (
	ErrInvalidRoute        = errors.New("invalid MPUDP v2 route")
	ErrInvalidPayloadLimit = errors.New("invalid MPUDP v2 payload limit")
)

// Route identifies one path incarnation and directed budget. PathBudgetEpoch
// is independent of any FEC encoding epoch. Negotiated path limits, source
// tuple, retained-generation authority and live budget state are caller-owned.
type Route struct {
	PathID      uint32
	Generation  uint64
	BudgetEpoch uint32
}

// Established is an authenticated route and a read-only borrowed typed body.
// The complete original packet must remain immutable and alive while used.
// Typed-body validation and runtime state checks are still required.
type Established struct {
	Header Header
	Route  Route
	Body   []byte
}

// DecodeEstablished requires successful authentication before reading route
// semantics. Pending path-validation types use epoch zero; other types require
// a nonzero budget epoch. It does not authorize endpoint learning or admission.
func DecodeEstablished(envelope AuthenticatedEnvelope) (Established, error) {
	if !envelope.verified {
		return Established{}, ErrAuthentication
	}
	header := envelope.Header()
	if header.Type.IsHandshake() {
		return Established{}, ErrUnknownPacketType
	}
	body := envelope.Body()
	if len(body) < RouteSize {
		return Established{}, ErrMalformed
	}
	route := Route{
		PathID:      binary.BigEndian.Uint32(body[:4]),
		Generation:  binary.BigEndian.Uint64(body[4:12]),
		BudgetEpoch: binary.BigEndian.Uint32(body[12:16]),
	}
	if err := validateRoute(header.Type, route); err != nil {
		return Established{}, err
	}
	return Established{Header: header, Route: route, Body: body[RouteSize:len(body):len(body)]}, nil
}

// AppendEstablished authenticates a route and caller-validated typed body.
// It preserves dst on error and permits typedBody to alias dst. The caller
// must enforce the selected route's packet budget before sending the result.
func AppendEstablished(dst []byte, header Header, route Route, typedBody []byte, key Key) ([]byte, error) {
	if key == (Key{}) {
		return dst, ErrInvalidKey
	}
	if header.Type.IsHandshake() {
		return dst, ErrUnknownPacketType
	}
	if len(typedBody) > MaxBodySize-RouteSize {
		return dst, ErrPacketTooLarge
	}
	if err := validateEnvelope(header, RouteSize+len(typedBody)); err != nil {
		return dst, err
	}
	if err := validateRoute(header.Type, route); err != nil {
		return dst, err
	}
	body := make([]byte, RouteSize+len(typedBody))
	encodeRoute(body, route)
	copy(body[RouteSize:], typedBody)
	return AppendEnvelope(dst, header, body, key)
}

func validateRoute(packetType PacketType, route Route) error {
	if route.PathID < 1 || route.PathID > 256 || route.Generation == 0 {
		return ErrInvalidRoute
	}
	pending := packetType >= TypePathJoin && packetType <= TypePathReady
	if (pending && route.BudgetEpoch != 0) || (!pending && route.BudgetEpoch == 0) {
		return ErrInvalidRoute
	}
	return nil
}

func encodeRoute(body []byte, route Route) {
	binary.BigEndian.PutUint32(body[:4], route.PathID)
	binary.BigEndian.PutUint64(body[4:12], route.Generation)
	binary.BigEndian.PutUint32(body[12:16], route.BudgetEpoch)
}

func validatePayloadLimit(maxPayload int) error {
	if maxPayload < HandshakePacketSize || maxPayload > MaxUDPPayload {
		return ErrInvalidPayloadLimit
	}
	return nil
}
