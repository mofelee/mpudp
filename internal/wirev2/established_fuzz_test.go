package wirev2

import (
	"bytes"
	"testing"
)

func FuzzEstablishedBodies(f *testing.F) {
	v := establishedVectors(f)
	key := keyFrom(f, v["key"])
	for _, name := range []string{"context_packet", "ack_packet"} {
		f.Add(v[name][5], v[name][PrefixSize+RouteSize:len(v[name])-AuthenticationTagSize])
	}
	f.Fuzz(func(t *testing.T, packetType byte, body []byte) {
		if len(body) > 128 {
			return
		}
		header := Header{Type: PacketType(packetType), SessionID: SessionID{1}}
		packet, err := AppendEstablished(nil, header, Route{1, 1, 1}, body, key)
		if err != nil {
			return
		}
		envelope := authenticate(t, packet, key)
		var encoded []byte
		switch header.Type {
		case TypeEncodingContext:
			route, context, err := DecodeEncodingContext(envelope)
			if err != nil {
				return
			}
			encoded, err = AppendEncodingContext(nil, header.SessionID, route, context, key)
			if err != nil {
				t.Fatal(err)
			}
		case TypeEncodingContextAck:
			route, ack, err := DecodeEncodingContextAck(envelope)
			if err != nil {
				return
			}
			encoded, err = AppendEncodingContextAck(nil, header.SessionID, route, ack, key)
			if err != nil {
				t.Fatal(err)
			}
		default:
			return
		}
		if !bytes.Equal(encoded, packet) {
			t.Fatal("canonical established body differs")
		}
	})
}

func FuzzFECBundle(f *testing.F) {
	v := establishedVectors(f)
	key := keyFrom(f, v["key"])
	packet := v["bundle_packet"]
	f.Add(packet[PrefixSize+RouteSize : len(packet)-AuthenticationTagSize])
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > MaxBodySize-RouteSize {
			return
		}
		packet, err := AppendEstablished(nil, Header{Type: TypeFECBundle, SessionID: SessionID{1}}, Route{1, 1, 1}, body, key)
		if err != nil {
			return
		}
		bundle, err := DecodeFECBundle(authenticate(t, packet, key), testLookup, MaxUDPPayload)
		if err != nil {
			return
		}
		encoded, err := AppendFECBundle(nil, bundle, testLookup, key, MaxUDPPayload)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(encoded, packet) {
			t.Fatal("canonical bundle differs")
		}
	})
}
