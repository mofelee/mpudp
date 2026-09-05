package wirev2

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"slices"
	"testing"
)

func vectors(t testing.TB) map[string][]byte {
	t.Helper()
	data, err := os.ReadFile("testdata/handshake.json")
	if err != nil {
		t.Fatal(err)
	}
	var encoded map[string]string
	if err := json.Unmarshal(data, &encoded); err != nil {
		t.Fatal(err)
	}
	result := make(map[string][]byte, len(encoded))
	for name, value := range encoded {
		result[name], err = hex.DecodeString(value)
		if err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func keyFrom(t testing.TB, value []byte) Key {
	t.Helper()
	if len(value) != 32 {
		t.Fatalf("key length=%d", len(value))
	}
	return Key(value)
}

func authenticate(t testing.TB, packet []byte, key Key) AuthenticatedEnvelope {
	t.Helper()
	envelope, err := ParseEnvelope(packet)
	if err != nil {
		t.Fatal(err)
	}
	authenticated, err := envelope.Authenticate(key)
	if err != nil {
		t.Fatal(err)
	}
	return authenticated
}

func decode(t testing.TB, packet []byte, key Key) Handshake {
	t.Helper()
	message, err := DecodeHandshake(authenticate(t, packet, key))
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func sign(packet []byte, key Key) {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(packet[:len(packet)-AuthenticationTagSize])
	copy(packet[len(packet)-AuthenticationTagSize:], mac.Sum(nil))
}

func TestBootstrapVectors(t *testing.T) {
	v := vectors(t)
	hs, err := DeriveHandshakeKey(v["psk"])
	if err != nil {
		t.Fatal(err)
	}
	if hs != keyFrom(t, v["handshake_key"]) {
		t.Fatal("handshake HKDF vector mismatch")
	}
	hello := authenticate(t, v["hello"], hs)
	challenge := authenticate(t, v["challenge"], hs)
	hDigest, err := HandshakeDigest(hello)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(hDigest[:], v["hello_digest"]) {
		t.Fatal("HELLO digest vector mismatch")
	}
	transcript, err := NewTranscript(hello, challenge)
	if err != nil {
		t.Fatal(err)
	}
	digest := transcript.Digest()
	if !bytes.Equal(digest[:], v["transcript"]) {
		t.Fatal("transcript vector mismatch")
	}
	keys, err := DeriveDirectionalKeys(v["psk"], transcript)
	if err != nil {
		t.Fatal(err)
	}
	if keys.ClientToServer != keyFrom(t, v["c2s_key"]) || keys.ServerToClient != keyFrom(t, v["s2c_key"]) {
		t.Fatal("directional HKDF vector mismatch")
	}
	for _, test := range []struct {
		name       string
		key        Key
		packetType PacketType
	}{
		{"hello", hs, TypeHello}, {"challenge", hs, TypeChallenge},
		{"finish", keys.ClientToServer, TypeFinish}, {"ready", keys.ServerToClient, TypeReady},
		{"reject", hs, TypeReject},
	} {
		t.Run(test.name, func(t *testing.T) {
			packet := v[test.name]
			if len(packet) != HandshakePacketSize {
				t.Fatalf("packet length=%d", len(packet))
			}
			message := decode(t, packet, test.key)
			if message.Header.Type != test.packetType {
				t.Fatal("incorrect type")
			}
			encoded, err := AppendHandshake([]byte("prefix"), message, test.key)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(encoded[:6], []byte("prefix")) || !bytes.Equal(encoded[6:], packet) {
				t.Fatal("canonical packet differs from independent vector")
			}
			if test.packetType == TypeFinish || test.packetType == TypeReady {
				if err := transcript.ValidateConfirmation(authenticate(t, packet, test.key)); err != nil {
					t.Fatal(err)
				}
				confirmation, err := transcript.Confirmation(test.packetType)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(confirmation, message) {
					t.Fatal("confirmation differs from vector")
				}
			}
		})
	}
	if err := ValidateReject(hello, authenticate(t, v["reject"], hs)); err != nil {
		t.Fatal(err)
	}
	message := decode(t, v["hello"], hs)
	if len(message.TLVs) != 12 {
		t.Fatalf("TLV count=%d", len(message.TLVs))
	}
	total := 0
	for _, tlv := range message.TLVs {
		total += 4 + len(tlv.Value)
	}
	if RequiredTLVBytes != 149 {
		t.Fatalf("required TLV byte constant=%d", RequiredTLVBytes)
	}
	if total != RequiredTLVBytes {
		t.Fatalf("mandatory bytes=%d", total)
	}
	if got := binary.BigEndian.Uint16(v["hello"][PrefixSize+80:]); int(got) != total {
		t.Fatalf("TLVBytes=%d", got)
	}
	if !allZero(v["hello"][PrefixSize+HandshakeFixedSize+total : 480]) {
		t.Fatal("nonzero padding")
	}
	path := message.TLVs[len(message.TLVs)-1].Value
	if binary.BigEndian.Uint16(path[:2]) != 5 || binary.BigEndian.Uint16(path[2:]) != 3 {
		t.Fatal("bootstrap Carrier was renumbered")
	}
}

func TestAuthenticationPrecedesHandshakeSemantics(t *testing.T) {
	v := vectors(t)
	key := keyFrom(t, v["handshake_key"])
	packet := slices.Clone(v["hello"])
	packet[PrefixSize+82] = 1
	envelope, err := ParseEnvelope(packet)
	if err != nil {
		t.Fatalf("structural parser inspected flags: %v", err)
	}
	if _, err := envelope.Authenticate(key); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("bad MAC: %v", err)
	}
	if _, err := DecodeHandshake(AuthenticatedEnvelope{}); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("unauthenticated decoder: %v", err)
	}
	sign(packet, key)
	if _, err := DecodeHandshake(authenticate(t, packet, key)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("signed invalid flags: %v", err)
	}
	for i := range v["hello"] {
		packet := slices.Clone(v["hello"])
		packet[i] ^= 1
		envelope, err := ParseEnvelope(packet)
		if err != nil {
			continue
		}
		if _, err := envelope.Authenticate(key); !errors.Is(err, ErrAuthentication) {
			t.Fatalf("tamper byte%d accepted: %v", i, err)
		}
	}
}

func TestHandshakeMalformedBodies(t *testing.T) {
	v := vectors(t)
	key := keyFrom(t, v["handshake_key"])
	for _, test := range []struct {
		name   string
		mutate func([]byte)
		want   error
	}{
		{"zero_client", func(b []byte) { clear(b[:16]) }, ErrMalformed},
		{"hello_server", func(b []byte) { b[16] = 1 }, ErrMalformed},
		{"hello_digest", func(b []byte) { b[32] = 1 }, ErrMalformed},
		{"hello_token", func(b []byte) { b[64] = 1 }, ErrMalformed},
		{"flags", func(b []byte) { b[83] = 1 }, ErrMalformed},
		{"padding", func(b []byte) { b[len(b)-1] = 1 }, ErrMalformed},
		{"oversize_tlv_area", func(b []byte) { binary.BigEndian.PutUint16(b[80:82], 373) }, ErrMalformed},
		{"truncated_tlv_header", func(b []byte) { binary.BigEndian.PutUint16(b[80:82], 1); clear(b[85:]) }, ErrInvalidTLV},
		{"truncated_tlv_value", func(b []byte) { binary.BigEndian.PutUint16(b[86:88], 372) }, ErrInvalidTLV},
		{"wrong_registered_length", func(b []byte) { binary.BigEndian.PutUint16(b[86:88], 0) }, ErrInvalidTLV},
		{"missing_required", func(b []byte) { clear(b[84:]); binary.BigEndian.PutUint16(b[80:82], 0) }, ErrInvalidTLV},
	} {
		t.Run(test.name, func(t *testing.T) {
			packet := slices.Clone(v["hello"])
			test.mutate(packet[PrefixSize:480])
			sign(packet, key)
			if _, err := DecodeHandshake(authenticate(t, packet, key)); !errors.Is(err, test.want) {
				t.Fatalf("got%v want%v", err, test.want)
			}
		})
	}
}

func TestTLVCanonicalBounds(t *testing.T) {
	v := vectors(t)
	key := keyFrom(t, v["handshake_key"])
	for _, test := range []struct {
		name   string
		mutate func(*Handshake)
		want   error
	}{
		{"duplicate", func(h *Handshake) { h.TLVs[1].Type = h.TLVs[0].Type }, ErrInvalidTLV},
		{"out_of_order", func(h *Handshake) { h.TLVs[0], h.TLVs[1] = h.TLVs[1], h.TLVs[0] }, ErrInvalidTLV},
		{"missing", func(h *Handshake) { h.TLVs = h.TLVs[:11] }, ErrInvalidTLV},
		{"old_paths_length", func(h *Handshake) { h.TLVs[11].Value = h.TLVs[11].Value[:2] }, ErrInvalidTLV},
		{"old_stream_limits_length", func(h *Handshake) { h.TLVs[9].Value = h.TLVs[9].Value[:24] }, ErrInvalidTLV},
		{"unknown_required", func(h *Handshake) { h.TLVs = append(h.TLVs, TLV{Type: 0x800e}) }, ErrRequiredTLV},
		{"reject_only_tlv", func(h *Handshake) { h.TLVs = append(h.TLVs, TLV{Type: TLVError, Value: []byte{0, 4, 0, 0}}) }, ErrInvalidTLV},
		{"repair_reserved", func(h *Handshake) { h.TLVs[8].Value[7] = 1 }, ErrInvalidTLV},
		{"too_many", func(h *Handshake) {
			optional := make([]TLV, 5)
			for i := range optional {
				optional[i].Type = TLVType(i + 1)
			}
			h.TLVs = append(optional, h.TLVs...)
		}, ErrInvalidTLV},
		{"too_many_bytes", func(h *Handshake) { h.TLVs = append([]TLV{{Type: 1, Value: make([]byte, 220)}}, h.TLVs...) }, ErrInvalidTLV},
	} {
		t.Run(test.name, func(t *testing.T) {
			message := decode(t, v["hello"], key)
			test.mutate(&message)
			backing := bytes.Repeat([]byte{0xaa}, 1024)
			dst := backing[:8]
			out, err := AppendHandshake(dst, message, key)
			if !errors.Is(err, test.want) {
				t.Fatalf("got%v want%v", err, test.want)
			}
			if len(out) != len(dst) || !bytes.Equal(backing, bytes.Repeat([]byte{0xaa}, 1024)) {
				t.Fatal("invalid encoding changed dst")
			}
		})
	}
	message := decode(t, v["hello"], key)
	message.TLVs = append([]TLV{{Type: 0x1234, Value: bytes.Repeat([]byte{0xc3}, 219)}}, message.TLVs...)
	encoded, err := AppendHandshake(nil, message, key)
	if err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint16(encoded[PrefixSize+80:]) != MaxTLVBytes {
		t.Fatal("optional TLV did not fill exact bound")
	}
	decoded := decode(t, encoded, key)
	if !reflect.DeepEqual(message, decoded) {
		t.Fatal("unknown optional TLV not preserved")
	}
	owned := slices.Clone(decoded.TLVs[0].Value)
	clear(encoded)
	if !bytes.Equal(decoded.TLVs[0].Value, owned) {
		t.Fatal("decoded TLV aliases receive buffer")
	}
}

func TestTranscriptAndReflectionRejection(t *testing.T) {
	v := vectors(t)
	hs := keyFrom(t, v["handshake_key"])
	hello := authenticate(t, v["hello"], hs)
	challenge := authenticate(t, v["challenge"], hs)
	transcript, err := NewTranscript(hello, challenge)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := DeriveDirectionalKeys(v["psk"], transcript)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"finish", "ready"} {
		envelope, err := ParseEnvelope(v[name])
		if err != nil {
			t.Fatal(err)
		}
		wrong := keys.ServerToClient
		if name == "ready" {
			wrong = keys.ClientToServer
		}
		for _, key := range []Key{wrong, hs} {
			if _, err := envelope.Authenticate(key); !errors.Is(err, ErrAuthentication) {
				t.Fatalf("%s reflected under wrong role: %v", name, err)
			}
		}
	}
	for _, offset := range []int{8, PrefixSize, PrefixSize + 16, PrefixSize + 32, PrefixSize + 64} {
		packet := slices.Clone(v["finish"])
		packet[offset] ^= 1
		sign(packet, keys.ClientToServer)
		if err := transcript.ValidateConfirmation(authenticate(t, packet, keys.ClientToServer)); !errors.Is(err, ErrTranscript) {
			t.Fatalf("confirmation binding offset%d: %v", offset, err)
		}
	}
	for _, offset := range []int{8, PrefixSize, PrefixSize + 32} {
		packet := slices.Clone(v["challenge"])
		packet[offset] ^= 1
		sign(packet, hs)
		if _, err := NewTranscript(hello, authenticate(t, packet, hs)); !errors.Is(err, ErrTranscript) {
			t.Fatalf("challenge binding offset%d: %v", offset, err)
		}
	}
	if _, err := NewTranscript(challenge, hello); !errors.Is(err, ErrTranscript) {
		t.Fatalf("reversed transcript: %v", err)
	}
	if _, err := DeriveDirectionalKeys(v["psk"], Transcript{}); !errors.Is(err, ErrTranscript) {
		t.Fatalf("zero transcript: %v", err)
	}
	if _, err := DeriveHandshakeKey(nil); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("empty PSK: %v", err)
	}
	for _, packetType := range []PacketType{0, TypeHello, TypeChallenge, TypeReject, TypeClose} {
		if _, err := transcript.Confirmation(packetType); !errors.Is(err, ErrTranscript) {
			t.Fatalf("confirmation type%d: %v", packetType, err)
		}
	}
	if err := (Transcript{}).ValidateConfirmation(authenticate(t, v["finish"], keys.ClientToServer)); !errors.Is(err, ErrTranscript) {
		t.Fatalf("expired/unavailable transcript: %v", err)
	}
}

func TestOptionalTLVBoundToTranscript(t *testing.T) {
	v := vectors(t)
	key := keyFrom(t, v["handshake_key"])
	original, err := NewTranscript(authenticate(t, v["hello"], key), authenticate(t, v["challenge"], key))
	if err != nil {
		t.Fatal(err)
	}
	h := decode(t, v["hello"], key)
	h.TLVs = append([]TLV{{Type: 0x0123, Value: []byte("optional")}}, h.TLVs...)
	newHello, err := AppendHandshake(nil, h, key)
	if err != nil {
		t.Fatal(err)
	}
	authHello := authenticate(t, newHello, key)
	if _, err := NewTranscript(authHello, authenticate(t, v["challenge"], key)); !errors.Is(err, ErrTranscript) {
		t.Fatalf("stale hello digest accepted: %v", err)
	}
	c := decode(t, v["challenge"], key)
	c.TranscriptDigest, err = HandshakeDigest(authHello)
	if err != nil {
		t.Fatal(err)
	}
	newChallenge, err := AppendHandshake(nil, c, key)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := NewTranscript(authHello, authenticate(t, newChallenge, key))
	if err != nil {
		t.Fatal(err)
	}
	if original.Digest() == changed.Digest() {
		t.Fatal("optional TLV omitted from transcript")
	}
	originalKeys, err := DeriveDirectionalKeys(v["psk"], original)
	if err != nil {
		t.Fatal(err)
	}
	changedKeys, err := DeriveDirectionalKeys(v["psk"], changed)
	if err != nil {
		t.Fatal(err)
	}
	if originalKeys == changedKeys {
		t.Fatal("changed transcript reused keys")
	}
}
