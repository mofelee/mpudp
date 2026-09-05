package wirev2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"testing"
)

func TestEstablishedRouteBounds(t *testing.T) {
	for _, route := range []Route{{1, 1, 1}, {256, math.MaxUint64, math.MaxUint32}} {
		packet, err := AppendEstablished(nil, Header{Type: TypeClose, SessionID: SessionID{1}}, route, make([]byte, 8), Key{1})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeEstablished(authenticate(t, packet, Key{1}))
		if err != nil || decoded.Route != route {
			t.Fatalf("route round trip: %v", err)
		}
	}
	for _, route := range []Route{{0, 1, 1}, {257, 1, 1}, {1, 0, 1}, {1, 1, 0}} {
		if _, err := AppendEstablished(nil, Header{Type: TypeClose, SessionID: SessionID{1}}, route, make([]byte, 8), Key{1}); !errors.Is(err, ErrInvalidRoute) {
			t.Fatalf("invalid route %+v: %v", route, err)
		}
	}
	for _, packetType := range []PacketType{TypePathJoin, TypePathChallenge, TypePathConfirm, TypePathReady} {
		packet, err := AppendEstablished(nil, Header{Type: packetType, SessionID: SessionID{1}}, Route{1, 1, 0}, make([]byte, 440), Key{1})
		if err != nil {
			t.Fatal(err)
		}
		if len(packet) != 512 {
			t.Fatal("path validation size differs")
		}
		if _, err := DecodeEstablished(authenticate(t, packet, Key{1})); err != nil {
			t.Fatal(err)
		}
		if _, err := AppendEstablished(nil, Header{Type: packetType, SessionID: SessionID{1}}, Route{1, 1, 1}, make([]byte, 440), Key{1}); !errors.Is(err, ErrInvalidRoute) {
			t.Fatalf("pending budget: %v", err)
		}
	}
	if _, err := DecodeEstablished(AuthenticatedEnvelope{}); !errors.Is(err, ErrAuthentication) {
		t.Fatal(err)
	}
}

func TestRouteTamperAndAliasing(t *testing.T) {
	v := establishedVectors(t)
	key := keyFrom(t, v["key"])
	for _, offset := range []int{0, 4, 12} {
		packet := slices.Clone(v["context_packet"])
		if offset == 4 {
			binary.BigEndian.PutUint64(packet[PrefixSize+offset:], 0)
		} else {
			binary.BigEndian.PutUint32(packet[PrefixSize+offset:], 0)
		}
		envelope, err := ParseEnvelope(packet)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := envelope.Authenticate(key); !errors.Is(err, ErrAuthentication) {
			t.Fatal(err)
		}
		sign(packet, key)
		if _, err := DecodeEstablished(authenticate(t, packet, key)); !errors.Is(err, ErrInvalidRoute) {
			t.Fatal(err)
		}
	}
	backing := bytes.Repeat([]byte{0x42}, 1024)
	typed := backing[3:512]
	expected := slices.Clone(typed)
	packet, err := AppendEstablished(backing[:8], Header{Type: TypeFECBundle, SessionID: SessionID{1}}, Route{1, 1, 1}, typed, key)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeEstablished(authenticate(t, packet[8:], key))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Body, expected) || !bytes.Equal(packet[:8], bytes.Repeat([]byte{0x42}, 8)) {
		t.Fatal("aliased typed body corrupted")
	}
	if cap(decoded.Body) != len(decoded.Body) {
		t.Fatal("borrowed typed body has uncapped capacity")
	}
}
