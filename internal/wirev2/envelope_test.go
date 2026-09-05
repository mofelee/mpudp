package wirev2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"
)

func TestEnvelopeFraming(t *testing.T) {
	v := vectors(t)
	for _, test := range []struct {
		name   string
		packet func() []byte
		want   error
	}{
		{"empty", func() []byte { return nil }, ErrMalformed},
		{"truncated", func() []byte { return slices.Clone(v["hello"][:511]) }, ErrMalformed},
		{"trailing", func() []byte { return append(slices.Clone(v["hello"]), 0) }, ErrMalformed},
		{"magic", func() []byte { p := slices.Clone(v["hello"]); p[0] = 'X'; return p }, ErrMalformed},
		{"v1", func() []byte { p := slices.Clone(v["hello"]); p[4] = 1; return p }, ErrUnsupportedVersion},
		{"zero_type", func() []byte { p := slices.Clone(v["hello"]); p[5] = 0; return p }, ErrUnknownPacketType},
		{"unknown_type", func() []byte { p := slices.Clone(v["hello"]); p[5] = 6; return p }, ErrUnknownPacketType},
		{"zero_session", func() []byte { p := slices.Clone(v["hello"]); clear(p[8:24]); return p }, ErrInvalidSessionID},
		{"oversize", func() []byte { return make([]byte, MaxUDPPayload+1) }, ErrPacketTooLarge},
		{"claimed_length", func() []byte { p := slices.Clone(v["hello"]); binary.BigEndian.PutUint16(p[6:8], 65535); return p }, ErrMalformed},
		{"short_handshake", func() []byte { p := slices.Clone(v["hello"][:511]); binary.BigEndian.PutUint16(p[6:8], 455); return p }, ErrMalformed},
		{"short_route", func() []byte {
			p := slices.Clone(v["hello"][:EnvelopeOverhead+15])
			p[5] = byte(TypeClose)
			binary.BigEndian.PutUint16(p[6:8], 15)
			return p
		}, ErrMalformed},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseEnvelope(test.packet()); !errors.Is(err, test.want) {
				t.Fatalf("got%v want%v", err, test.want)
			}
		})
	}
	if (AuthenticatedEnvelope{}).Body() != nil {
		t.Fatal("zero authenticated envelope exposes body")
	}
	if _, err := (Envelope{}).Authenticate(Key{1}); !errors.Is(err, ErrMalformed) {
		t.Fatalf("zero envelope: %v", err)
	}
	e, err := ParseEnvelope(v["hello"])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Authenticate(Key{}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("zero key: %v", err)
	}
}

func TestCompletePacketTypeRegistry(t *testing.T) {
	registered := map[PacketType]bool{}
	for _, value := range []PacketType{1, 2, 3, 4, 5, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 32, 33, 34, 35, 36, 37, 38, 48, 63} {
		registered[value] = true
	}
	for value := 0; value < 256; value++ {
		typ := PacketType(value)
		body := make([]byte, RouteSize)
		if typ.IsHandshake() {
			body = make([]byte, HandshakeBodySize)
		}
		packet, err := AppendEnvelope(nil, Header{Type: typ, SessionID: SessionID{1}}, body, Key{1})
		if !registered[typ] {
			if !errors.Is(err, ErrUnknownPacketType) {
				t.Fatalf("unknown type%d: %v", value, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("registered type%d: %v", value, err)
		}
		authenticated := authenticate(t, packet, Key{1})
		if authenticated.Header().Type != typ || !bytes.Equal(authenticated.Body(), body) {
			t.Fatalf("type%d round trip mismatch", value)
		}
	}
}

func TestEnvelopeMaximumAndAppendAliasing(t *testing.T) {
	header := Header{Type: TypeFECBundle, SessionID: SessionID{1}}
	key := Key{1}
	packet, err := AppendEnvelope(nil, header, make([]byte, MaxBodySize), key)
	if err != nil {
		t.Fatal(err)
	}
	if len(packet) != MaxUDPPayload {
		t.Fatalf("maximum length=%d", len(packet))
	}
	if len(authenticate(t, packet, key).Body()) != MaxBodySize {
		t.Fatal("maximum body truncated")
	}
	dst := []byte("retained")
	if result, err := AppendEnvelope(dst, header, make([]byte, MaxBodySize+1), key); !errors.Is(err, ErrPacketTooLarge) || !bytes.Equal(result, dst) {
		t.Fatalf("over maximum append: %v", err)
	}
	for _, offset := range []int{0, 4, 8, 10, 32, 70} {
		backing := make([]byte, 1024)
		for i := range backing {
			backing[i] = byte(i)
		}
		prefix := slices.Clone(backing[:8])
		body := backing[offset : offset+456]
		expected := slices.Clone(body)
		out, err := AppendEnvelope(backing[:8], header, body, key)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out[:8], prefix) || !bytes.Equal(authenticate(t, out[8:], key).Body(), expected) {
			t.Fatalf("aliased body offset%d corrupted", offset)
		}
	}
}
