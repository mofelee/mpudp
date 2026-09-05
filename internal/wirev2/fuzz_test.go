package wirev2

import (
	"bytes"
	"testing"
)

func FuzzEnvelope(f *testing.F) {
	v := vectors(f)
	key := keyFrom(f, v["handshake_key"])
	for _, name := range []string{"hello", "challenge", "finish", "ready", "reject"} {
		f.Add(v[name])
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, packet []byte) {
		envelope, err := ParseEnvelope(packet)
		if err != nil {
			return
		}
		if len(packet) > MaxUDPPayload {
			t.Fatal("oversized envelope")
		}
		authenticated, err := envelope.Authenticate(key)
		if err != nil {
			return
		}
		reencoded, err := AppendEnvelope(nil, authenticated.Header(), authenticated.Body(), key)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(reencoded, packet) {
			t.Fatal("envelope round trip differs")
		}
		_, _ = DecodeHandshake(authenticated)
	})
}

func FuzzAuthenticatedHandshake(f *testing.F) {
	v := vectors(f)
	key := keyFrom(f, v["handshake_key"])
	for _, name := range []string{"hello", "challenge", "finish", "ready", "reject"} {
		f.Add(v[name][5], v[name][PrefixSize:480])
	}
	f.Fuzz(func(t *testing.T, packetType byte, body []byte) {
		if len(body) != HandshakeBodySize {
			return
		}
		packet, err := AppendEnvelope(nil, Header{Type: PacketType(packetType), SessionID: SessionID{1}}, body, key)
		if err != nil {
			return
		}
		message, err := DecodeHandshake(authenticate(t, packet, key))
		if err != nil {
			return
		}
		if len(message.TLVs) > MaxTLVs {
			t.Fatal("unbounded TLV count")
		}
		reencoded, err := AppendHandshake(nil, message, key)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(reencoded, packet) {
			t.Fatal("handshake canonical round trip differs")
		}
	})
}
