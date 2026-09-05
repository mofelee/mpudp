package negotiationv2

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/mofelee/mpudp/internal/wirev2"
)

func handshake(t testing.TB, a Advertisement, packetType wirev2.PacketType) wirev2.Handshake {
	t.Helper()
	tlvs, err := a.TLVs()
	if err != nil {
		t.Fatal(err)
	}
	return wirev2.Handshake{Header: wirev2.Header{Type: packetType}, TLVs: tlvs}
}

func TestIndependentWireFixture(t *testing.T) {
	data, err := os.ReadFile("../wirev2/testdata/handshake.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture map[string]string
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	decodeHex := func(name string) []byte {
		t.Helper()
		result, err := hex.DecodeString(fixture[name])
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	keyBytes := decodeHex("handshake_key")
	if len(keyBytes) != len(wirev2.Key{}) {
		t.Fatal("invalid fixture key")
	}
	var advertisements [2]Advertisement
	for i, name := range []string{"hello", "challenge"} {
		packet := decodeHex(name)
		envelope, err := wirev2.ParseEnvelope(packet)
		if err != nil {
			t.Fatal(err)
		}
		authenticated, err := envelope.Authenticate(wirev2.Key(keyBytes))
		if err != nil {
			t.Fatal(err)
		}
		message, err := wirev2.DecodeHandshake(authenticated)
		if err != nil {
			t.Fatal(err)
		}
		decode := DecodeHello
		if i == 1 {
			decode = DecodeChallenge
		}
		advertisements[i], err = decode(message)
		if err != nil {
			t.Fatal(err)
		}
		want := Advertisement{Profile: datagramProfile(), BootstrapPathID: 3}
		want.RequiredCaps |= GroupMigration
		if i == 1 {
			want.Payload.SendHardCap, want.Payload.ReceiveHardCap = 1472, 1200
		}
		if advertisements[i] != want {
			t.Fatalf("%s advertisement differs: %+v", name, advertisements[i])
		}
		tlvs, err := advertisements[i].TLVs()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(tlvs, message.TLVs) {
			t.Fatal("canonical encoding differs from independent fixture")
		}
		message.TLVs = tlvs
		encoded, err := wirev2.AppendHandshake(nil, message, wirev2.Key(keyBytes))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(encoded, packet) {
			t.Fatal("re-encoded fixture differs")
		}
	}
	challenge, selected, err := Select(advertisements[0], advertisements[1].Profile)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := Accept(advertisements[0], advertisements[1])
	if err != nil || accepted != selected || challenge != advertisements[1] {
		t.Fatalf("fixture selection: %v", err)
	}
}

func TestTLVRoundTripAndOwnership(t *testing.T) {
	for _, p := range []Profile{datagramProfile(), kcpProfile(MuxOff), kcpProfile(SmuxWire2)} {
		want := Advertisement{Profile: p, BootstrapPathID: 3}
		message := handshake(t, want, wirev2.TypeHello)
		got, err := DecodeHello(message)
		if err != nil || got != want {
			t.Fatalf("round trip: %+v, %v", got, err)
		}
		other, err := want.TLVs()
		if err != nil {
			t.Fatal(err)
		}
		for i := range message.TLVs {
			clear(message.TLVs[i].Value)
		}
		if got != want {
			t.Fatal("decoded advertisement borrowed TLV data")
		}
		message.TLVs = other
		got, err = DecodeHello(message)
		if err != nil || got != want {
			t.Fatal("encoding calls shared value buffers")
		}
		message.TLVs = append([]wirev2.TLV{{Type: 1, Value: []byte("private-optional-extension")}}, message.TLVs...)
		got, err = DecodeHello(message)
		if err != nil || got != want {
			t.Fatalf("unknown optional TLV: %v", err)
		}
		encoded, err := got.TLVs()
		if err != nil || len(encoded) != 12 {
			t.Fatal("canonical encoder retained optional extension")
		}
	}
	if tlvs, err := (Advertisement{}).TLVs(); !errors.Is(err, ErrInvalid) || tlvs != nil {
		t.Fatalf("invalid encoder: %v", err)
	}
}

func TestDefensiveTLVValidation(t *testing.T) {
	base := Advertisement{Profile: datagramProfile(), BootstrapPathID: 3}
	tests := []struct {
		name   string
		change func(*wirev2.Handshake)
	}{
		{"wrong-packet", func(m *wirev2.Handshake) { m.Header.Type = wirev2.TypeChallenge }},
		{"empty", func(m *wirev2.Handshake) { m.TLVs = nil }},
		{"missing", func(m *wirev2.Handshake) { m.TLVs = m.TLVs[1:] }},
		{"missing-with-optional", func(m *wirev2.Handshake) { m.TLVs[0] = wirev2.TLV{Type: 1, Value: []byte("private-extension")} }},
		{"duplicate", func(m *wirev2.Handshake) { m.TLVs[1] = m.TLVs[0] }},
		{"unordered", func(m *wirev2.Handshake) { m.TLVs[0], m.TLVs[1] = m.TLVs[1], m.TLVs[0] }},
		{"required-extension", func(m *wirev2.Handshake) {
			m.TLVs = append(m.TLVs, wirev2.TLV{Type: 0x800e, Value: []byte("private-extension")})
		}},
		{"reject-only", func(m *wirev2.Handshake) {
			m.TLVs = append(m.TLVs, wirev2.TLV{Type: wirev2.TLVError, Value: make([]byte, 4)})
		}},
		{"too-many", func(m *wirev2.Handshake) {
			for i := range 5 {
				m.TLVs = append([]wirev2.TLV{{Type: wirev2.TLVType(5 - i)}}, m.TLVs...)
			}
		}},
		{"total-too-large", func(m *wirev2.Handshake) {
			m.TLVs = append([]wirev2.TLV{{Type: 1, Value: make([]byte, wirev2.MaxTLVBytes-wirev2.RequiredTLVBytes-3)}}, m.TLVs...)
		}},
		{"one-too-large", func(m *wirev2.Handshake) {
			m.TLVs = append([]wirev2.TLV{{Type: 1, Value: make([]byte, wirev2.MaxTLVBytes)}}, m.TLVs...)
		}},
		{"reserved", func(m *wirev2.Handshake) { m.TLVs[8].Value[7] = 1 }},
		{"semantic-neutral", func(m *wirev2.Handshake) { m.TLVs[9].Value[3] = 1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message := handshake(t, base, wirev2.TypeHello)
			tt.change(&message)
			got, err := DecodeHello(message)
			if !errors.Is(err, ErrInvalid) || got != (Advertisement{}) {
				t.Fatalf("got %+v, %v", got, err)
			}
			if strings.Contains(err.Error(), "private") || len(err.Error()) > 160 {
				t.Fatal("error leaked input or was unbounded")
			}
		})
	}
	for index := range 12 {
		for _, delta := range []int{-1, 1} {
			message := handshake(t, base, wirev2.TypeHello)
			message.TLVs[index].Value = make([]byte, len(message.TLVs[index].Value)+delta)
			if _, err := DecodeHello(message); !errors.Is(err, ErrInvalid) {
				t.Fatalf("length index=%d delta=%d: %v", index, delta, err)
			}
		}
	}
	message := handshake(t, base, wirev2.TypeHello)
	if _, err := DecodeChallenge(message); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong challenge type: %v", err)
	}
	message.TLVs = append([]wirev2.TLV{{Type: 1, Value: make([]byte, wirev2.MaxTLVBytes-wirev2.RequiredTLVBytes-4)}}, message.TLVs...)
	if _, err := DecodeHello(message); err != nil {
		t.Fatalf("exact byte boundary: %v", err)
	}
}

func FuzzDecodeContractTLV(f *testing.F) {
	base := Advertisement{Profile: datagramProfile(), BootstrapPathID: 3}
	tlvs, err := base.TLVs()
	if err != nil {
		f.Fatal(err)
	}
	for i, tlv := range tlvs {
		f.Add(uint8(i), uint16(tlv.Type), tlv.Value)
	}
	f.Add(uint8(8), uint16(wirev2.TLVRepair), []byte{})
	f.Fuzz(func(t *testing.T, index uint8, typ uint16, value []byte) {
		message := handshake(t, base, wirev2.TypeHello)
		message.TLVs[int(index)%len(message.TLVs)] = wirev2.TLV{Type: wirev2.TLVType(typ), Value: value}
		original := slices.Clone(value)
		advertisement, err := DecodeHello(message)
		if !bytes.Equal(value, original) {
			t.Fatal("decode mutated source bytes")
		}
		if err != nil {
			return
		}
		encoded := handshake(t, advertisement, wirev2.TypeHello)
		roundTrip, err := DecodeHello(encoded)
		if err != nil || roundTrip != advertisement {
			t.Fatalf("round trip: %v", err)
		}
	})
}
