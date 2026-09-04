package wire

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

const testKey = "mpudp-v0.1-test-key"

var testSessionID = SessionID{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}

type goldenFixture struct {
	name        string
	message     Message
	wireHex     string
	fingerprint string
}

func TestProtocolSizes(t *testing.T) {
	if Magic != "MPUD" || PrefixSize != 24 || AuthenticationTagSize != 32 || MinimumPacketSize != 56 {
		t.Fatalf("unexpected envelope constants: magic=%q prefix=%d tag=%d minimum=%d", Magic, PrefixSize, AuthenticationTagSize, MinimumPacketSize)
	}
	if HelloPacketSize != 60 || ProbePacketSize != 72 {
		t.Fatalf("unexpected control packet sizes: hello=%d probe=%d", HelloPacketSize, ProbePacketSize)
	}
	if DataShardMetadataSize != 15 || DataShardOverhead != 71 {
		t.Fatalf("unexpected DATA_SHARD sizes: metadata=%d overhead=%d", DataShardMetadataSize, DataShardOverhead)
	}
	if MinUDPPayload != 72 || MaxUDPPayload != 65507 {
		t.Fatalf("unexpected UDP payload range: [%d,%d]", MinUDPPayload, MaxUDPPayload)
	}
}

func TestGoldenVectorsAndRoundTrips(t *testing.T) {
	for _, fixture := range goldenFixtures(t) {
		t.Run(fixture.name, func(t *testing.T) {
			want := mustDecodeHex(t, fixture.wireHex)
			encoded, err := AppendAuthenticated(nil, fixture.message, []byte(testKey), MaxUDPPayload)
			if err != nil {
				t.Fatalf("AppendAuthenticated failed: %v", err)
			}
			if !bytes.Equal(encoded, want) {
				t.Fatalf("golden mismatch: got_sha256=%x want_sha256=%x", sha256.Sum256(encoded), sha256.Sum256(want))
			}
			if got := fingerprint(encoded); got != fixture.fingerprint {
				t.Fatalf("fingerprint mismatch: got=%s want=%s", got, fixture.fingerprint)
			}
			length, err := EncodedLen(fixture.message)
			if err != nil {
				t.Fatalf("EncodedLen failed: %v", err)
			}
			if length != len(want) {
				t.Fatalf("EncodedLen=%d, want %d", length, len(want))
			}
			decoded, err := DecodeAuthenticated(want, []byte(testKey), MaxUDPPayload)
			if err != nil {
				t.Fatalf("DecodeAuthenticated failed: %v", err)
			}
			assertMessageEqual(t, decoded, fixture.message)
		})
	}
}

func TestNetworkByteOrderAndOffsets(t *testing.T) {
	fixtures := goldenFixtures(t)
	hello := mustDecodeHex(t, fixtures[0].wireHex)
	if got := binary.BigEndian.Uint16(hello[6:8]); got != 4 {
		t.Fatalf("HELLO BodyLength=%d, want 4", got)
	}
	if hello[24] != 3 || hello[25] != 2 || binary.BigEndian.Uint16(hello[26:28]) != 1200 {
		t.Fatal("HELLO body offsets or byte order differ from the protocol")
	}

	data := mustDecodeHex(t, fixtures[2].wireHex)
	if got := binary.BigEndian.Uint16(data[6:8]); got != 19 {
		t.Fatalf("DATA_SHARD BodyLength=%d, want 19", got)
	}
	if got := binary.BigEndian.Uint64(data[24:32]); got != 0x0102030405060708 {
		t.Fatalf("PacketID=%x, want 0102030405060708", got)
	}
	if data[32] != 3 || data[33] != 2 || data[34] != 4 {
		t.Fatal("DATA_SHARD one-byte metadata is at the wrong offset")
	}
	if got := binary.BigEndian.Uint32(data[35:39]); got != 10 {
		t.Fatalf("OriginalLength=%d, want 10", got)
	}
	if !bytes.Equal(data[39:43], []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Fatal("shard payload is at the wrong offset")
	}

	ping := mustDecodeHex(t, fixtures[3].wireHex)
	if got := binary.BigEndian.Uint64(ping[24:32]); got != 0x1122334455667788 {
		t.Fatalf("PING Token=%x, want 1122334455667788", got)
	}
	if got := binary.BigEndian.Uint64(ping[32:40]); got != 0x0102030405060708 {
		t.Fatalf("PING Timestamp=%x, want 0102030405060708", got)
	}
}

func TestConstructorsAndBoundaries(t *testing.T) {
	tests := []struct {
		name string
		make func() (Message, error)
		want int
	}{
		{"hello", func() (Message, error) { return NewHello(testSessionID, 3, 2, 1200) }, 60},
		{"hello ack", func() (Message, error) { return NewHelloAck(testSessionID, 255, 1, 72) }, 60},
		{"data", func() (Message, error) {
			return NewDataShard(testSessionID, math.MaxUint64, 1, 255, 255, 1, []byte{0xa5})
		}, 72},
		{"empty datagram shard", func() (Message, error) { return NewDataShard(testSessionID, 0, 3, 2, 0, 0, []byte{0}) }, 72},
		{"ping", func() (Message, error) { return NewPing(testSessionID, 1, 0) }, 72},
		{"pong", func() (Message, error) { return NewPong(testSessionID, math.MaxUint64, math.MaxUint64) }, 72},
		{"close", func() (Message, error) { return NewClose(testSessionID) }, 56},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message, err := test.make()
			if err != nil {
				t.Fatalf("constructor failed: %v", err)
			}
			got, err := EncodedLen(message)
			if err != nil {
				t.Fatalf("EncodedLen failed: %v", err)
			}
			if got != test.want {
				t.Fatalf("EncodedLen=%d, want %d", got, test.want)
			}
			packet, err := AppendAuthenticated(nil, message, []byte(testKey), MaxUDPPayload)
			if err != nil {
				t.Fatalf("AppendAuthenticated failed: %v", err)
			}
			decoded, err := DecodeAuthenticated(packet, []byte(testKey), MaxUDPPayload)
			if err != nil {
				t.Fatalf("DecodeAuthenticated failed: %v", err)
			}
			assertMessageEqual(t, decoded, message)
		})
	}

	maxPayload := bytes.Repeat([]byte{0xa5}, MaxUDPPayload-DataShardOverhead)
	message, err := NewDataShard(testSessionID, 9, 1, 1, 0, uint32(len(maxPayload)), maxPayload)
	if err != nil {
		t.Fatalf("construct maximum packet: %v", err)
	}
	packet, err := AppendAuthenticated(nil, message, []byte(testKey), MaxUDPPayload)
	if err != nil {
		t.Fatalf("append maximum packet: %v", err)
	}
	if len(packet) != MaxUDPPayload {
		t.Fatalf("maximum packet length=%d, want %d", len(packet), MaxUDPPayload)
	}
	if _, err := DecodeAuthenticated(packet, []byte(testKey), MaxUDPPayload); err != nil {
		t.Fatalf("decode maximum packet: %v", err)
	}

	overPayload := bytes.Repeat([]byte{0xa5}, MaxUDPPayload-DataShardOverhead+1)
	if _, err := NewDataShard(testSessionID, 9, 1, 1, 0, uint32(len(overPayload)), overPayload); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("oversize constructor error=%v, want ErrPacketTooLarge", err)
	}
}

func TestRejectsInvalidMessages(t *testing.T) {
	validHello := Message{Header: Header{Type: TypeHello, SessionID: testSessionID}, Handshake: Handshake{DataShards: 3, ParityShards: 2, MaxUDPPayload: 1200}}
	tests := []struct {
		name string
		msg  Message
		err  error
	}{
		{"zero session", Message{Header: Header{Type: TypeClose}}, ErrInvalidSessionID},
		{"unknown type", Message{Header: Header{Type: 7, SessionID: testSessionID}}, ErrUnknownPacketType},
		{"zero data shards", Message{Header: Header{Type: TypeHello, SessionID: testSessionID}, Handshake: Handshake{ParityShards: 1, MaxUDPPayload: 1200}}, ErrInvalidFEC},
		{"zero parity shards", Message{Header: Header{Type: TypeHello, SessionID: testSessionID}, Handshake: Handshake{DataShards: 1, MaxUDPPayload: 1200}}, ErrInvalidFEC},
		{"too many total shards", Message{Header: Header{Type: TypeHello, SessionID: testSessionID}, Handshake: Handshake{DataShards: 255, ParityShards: 2, MaxUDPPayload: 1200}}, ErrInvalidFEC},
		{"capability below minimum", Message{Header: Header{Type: TypeHello, SessionID: testSessionID}, Handshake: Handshake{DataShards: 1, ParityShards: 1, MaxUDPPayload: 71}}, ErrInvalidCapability},
		{"capability above maximum", Message{Header: Header{Type: TypeHello, SessionID: testSessionID}, Handshake: Handshake{DataShards: 1, ParityShards: 1, MaxUDPPayload: math.MaxUint16}}, ErrInvalidCapability},
		{"hello with inactive data", func() Message { m := validHello; m.DataShard.Payload = []byte{1}; return m }(), ErrMalformed},
		{"index equals total", Message{Header: Header{Type: TypeDataShard, SessionID: testSessionID}, DataShard: DataShard{DataShards: 3, ParityShards: 2, ShardIndex: 5, OriginalLength: 1, Payload: []byte{1}}}, ErrInvalidFEC},
		{"empty datagram missing sentinel", Message{Header: Header{Type: TypeDataShard, SessionID: testSessionID}, DataShard: DataShard{DataShards: 3, ParityShards: 2}}, ErrMalformed},
		{"empty datagram nonzero sentinel", Message{Header: Header{Type: TypeDataShard, SessionID: testSessionID}, DataShard: DataShard{DataShards: 3, ParityShards: 2, Payload: []byte{1}}}, ErrMalformed},
		{"noncanonical shard short", Message{Header: Header{Type: TypeDataShard, SessionID: testSessionID}, DataShard: DataShard{DataShards: 3, ParityShards: 2, OriginalLength: 10, Payload: []byte{1, 2, 3}}}, ErrMalformed},
		{"noncanonical shard long", Message{Header: Header{Type: TypeDataShard, SessionID: testSessionID}, DataShard: DataShard{DataShards: 3, ParityShards: 2, OriginalLength: 10, Payload: []byte{1, 2, 3, 4, 5}}}, ErrMalformed},
		{"zero ping token", Message{Header: Header{Type: TypePing, SessionID: testSessionID}}, ErrMalformed},
		{"close with body", Message{Header: Header{Type: TypeClose, SessionID: testSessionID}, Probe: Probe{Token: 1}}, ErrMalformed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := EncodedLen(test.msg); !errors.Is(err, test.err) {
				t.Fatalf("EncodedLen error=%v, want %v", err, test.err)
			}
		})
	}
}

func TestAppendFailureLeavesDestinationUnchanged(t *testing.T) {
	valid, err := NewDataShard(testSessionID, 1, 1, 1, 0, 2, []byte{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.DataShard.ShardIndex = 2
	tests := []struct {
		name   string
		msg    Message
		key    []byte
		budget int
	}{
		{"empty key", valid, nil, 1200},
		{"low budget", valid, []byte(testKey), 71},
		{"high budget", valid, []byte(testKey), 65508},
		{"packet exceeds budget", valid, []byte(testKey), 72},
		{"invalid message", invalid, []byte(testKey), 1200},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backing := make([]byte, 4, 4096)
			copy(backing, []byte{1, 2, 3, 4})
			before := append([]byte(nil), backing...)
			got, err := AppendAuthenticated(backing, test.msg, test.key, test.budget)
			if err == nil {
				t.Fatal("AppendAuthenticated unexpectedly succeeded")
			}
			if len(got) != len(before) || !bytes.Equal(got, before) {
				t.Fatal("destination changed after failed append")
			}
		})
	}
}

func TestDecodeRejectsWrongKeyTamperingTruncationAndTrailingData(t *testing.T) {
	for _, fixture := range goldenFixtures(t) {
		packet := mustDecodeHex(t, fixture.wireHex)
		t.Run(fixture.name+"/wrong-key", func(t *testing.T) {
			if _, err := DecodeAuthenticated(packet, []byte("wrong-test-key"), MaxUDPPayload); !errors.Is(err, ErrAuthentication) {
				t.Fatalf("error=%v, want ErrAuthentication", err)
			}
		})
		t.Run(fixture.name+"/truncated", func(t *testing.T) {
			for length := 0; length < len(packet); length++ {
				if _, err := DecodeAuthenticated(packet[:length], []byte(testKey), MaxUDPPayload); err == nil {
					t.Fatalf("accepted truncation at length %d", length)
				}
			}
		})
		t.Run(fixture.name+"/trailing", func(t *testing.T) {
			trailing := append(append([]byte(nil), packet...), 0)
			if _, err := DecodeAuthenticated(trailing, []byte(testKey), MaxUDPPayload); !errors.Is(err, ErrMalformed) {
				t.Fatalf("error=%v, want ErrMalformed", err)
			}
		})
		t.Run(fixture.name+"/single-bit", func(t *testing.T) {
			for index := range packet {
				for bit := uint8(1); bit != 0; bit <<= 1 {
					mutated := append([]byte(nil), packet...)
					mutated[index] ^= bit
					if _, err := DecodeAuthenticated(mutated, []byte(testKey), MaxUDPPayload); err == nil {
						t.Fatalf("accepted mutation at byte %d bit %02x", index, bit)
					}
				}
			}
		})
	}
}

func TestDecodeEnvelopeAndBudgetErrors(t *testing.T) {
	closeMessage, err := NewClose(testSessionID)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := AppendAuthenticated(nil, closeMessage, []byte(testKey), MinUDPPayload)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		packet []byte
		key    []byte
		limit  int
		want   error
	}{
		{"empty key", packet, nil, MinUDPPayload, ErrInvalidKey},
		{"limit below minimum", packet, []byte(testKey), 71, ErrInvalidCapability},
		{"limit above maximum", packet, []byte(testKey), 65508, ErrInvalidCapability},
		{"packet over receive limit", append(packet, make([]byte, 17)...), []byte(testKey), MinUDPPayload, ErrPacketTooLarge},
		{"bad magic", mutateByte(packet, 0, 0xff), []byte(testKey), MinUDPPayload, ErrMalformed},
		{"bad version", mutateByte(packet, 4, 2), []byte(testKey), MinUDPPayload, ErrUnsupportedVersion},
		{"unknown type", mutateByte(packet, 5, 7), []byte(testKey), MinUDPPayload, ErrUnknownPacketType},
		{"body length mismatch", mutateByte(packet, 7, 1), []byte(testKey), MinUDPPayload, ErrMalformed},
		{"zero session", authenticatedRaw(TypeClose, SessionID{}, nil, []byte(testKey)), []byte(testKey), MinUDPPayload, ErrInvalidSessionID},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeAuthenticated(test.packet, test.key, test.limit); !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
		})
	}

	random := rand.New(rand.NewSource(1))
	for size := MinimumPacketSize; size <= 256; size++ {
		garbage := make([]byte, size)
		_, _ = random.Read(garbage)
		if _, err := DecodeAuthenticated(garbage, []byte(testKey), MaxUDPPayload); err == nil {
			t.Fatalf("accepted random garbage of length %d", size)
		}
	}
}

func TestAuthenticatedSemanticValidation(t *testing.T) {
	tests := []struct {
		name   string
		typeID PacketType
		body   []byte
		want   error
	}{
		{"hello short", TypeHello, []byte{3, 2, 4}, ErrMalformed},
		{"hello trailing", TypeHello, []byte{3, 2, 0x04, 0xb0, 0}, ErrMalformed},
		{"hello invalid fec", TypeHello, []byte{0, 2, 0x04, 0xb0}, ErrInvalidFEC},
		{"hello invalid capability", TypeHello, []byte{3, 2, 0, 71}, ErrInvalidCapability},
		{"data short", TypeDataShard, make([]byte, DataShardMetadataSize-1), ErrMalformed},
		{"data invalid index", TypeDataShard, dataBody(1, 3, 2, 5, 1, []byte{0xaa}), ErrInvalidFEC},
		{"data inconsistent length", TypeDataShard, dataBody(1, 3, 2, 0, 10, []byte{1, 2, 3}), ErrMalformed},
		{"data empty missing zero byte", TypeDataShard, dataBody(1, 3, 2, 0, 0, nil), ErrMalformed},
		{"data empty nonzero byte", TypeDataShard, dataBody(1, 3, 2, 0, 0, []byte{1}), ErrMalformed},
		{"ping short", TypePing, make([]byte, probeBodySize-1), ErrMalformed},
		{"ping zero token", TypePing, make([]byte, probeBodySize), ErrMalformed},
		{"pong trailing", TypePong, make([]byte, probeBodySize+1), ErrMalformed},
		{"close body", TypeClose, []byte{0}, ErrMalformed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			packet := authenticatedRaw(test.typeID, testSessionID, test.body, []byte(testKey))
			if _, err := DecodeAuthenticated(packet, []byte(testKey), MaxUDPPayload); !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
		})
	}
}

func TestAuthenticationPrecedesBodySemantics(t *testing.T) {
	message, err := NewHello(testSessionID, 3, 2, 1200)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := AppendAuthenticated(nil, message, []byte(testKey), MaxUDPPayload)
	if err != nil {
		t.Fatal(err)
	}
	packet[24] = 0
	if _, err := DecodeAuthenticated(packet, []byte(testKey), MaxUDPPayload); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("error=%v, want authentication failure before FEC semantics", err)
	}
}

func TestDecodeAndDispatchCallsHookOnlyAfterValidation(t *testing.T) {
	message, err := NewHello(testSessionID, 3, 2, 1200)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := AppendAuthenticated(nil, message, []byte(testKey), MaxUDPPayload)
	if err != nil {
		t.Fatal(err)
	}

	calls := 0
	handler := func(got Message) error {
		calls++
		assertMessageEqual(t, got, message)
		return nil
	}
	invalidPackets := [][]byte{
		nil,
		mutateByte(packet, 0, 0),
		mutateByte(packet, len(packet)-1, packet[len(packet)-1]^1),
		authenticatedRaw(TypeHello, testSessionID, []byte{0, 2, 0x04, 0xb0}, []byte(testKey)),
	}
	for _, invalid := range invalidPackets {
		if err := DecodeAndDispatch(invalid, []byte(testKey), MaxUDPPayload, handler); err == nil {
			t.Fatal("invalid packet unexpectedly dispatched")
		}
	}
	if calls != 0 {
		t.Fatalf("handler called %d times for invalid packets", calls)
	}
	if err := DecodeAndDispatch(packet, []byte(testKey), MaxUDPPayload, handler); err != nil {
		t.Fatalf("valid dispatch failed: %v", err)
	}
	if calls != 1 {
		t.Fatalf("handler calls=%d, want 1", calls)
	}
	if err := DecodeAndDispatch(packet, []byte(testKey), MaxUDPPayload, nil); !errors.Is(err, ErrInvalidHandler) {
		t.Fatalf("nil handler error=%v, want ErrInvalidHandler", err)
	}

	handlerError := errors.New("handler failed")
	if err := DecodeAndDispatch(packet, []byte(testKey), MaxUDPPayload, func(Message) error { return handlerError }); !errors.Is(err, handlerError) {
		t.Fatalf("handler error=%v, want propagated error", err)
	}
}

func TestDecodedPayloadAliasesInput(t *testing.T) {
	message, err := NewDataShard(testSessionID, 1, 3, 2, 0, 10, []byte{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	packet, err := AppendAuthenticated(nil, message, []byte(testKey), MaxUDPPayload)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeAuthenticated(packet, []byte(testKey), MaxUDPPayload)
	if err != nil {
		t.Fatal(err)
	}
	packet[39] = 0xfe
	if decoded.DataShard.Payload[0] != 0xfe {
		t.Fatal("decoded payload does not alias its authenticated input buffer")
	}
}

func TestErrorsDoNotExposeAuthenticationMaterialOrPayload(t *testing.T) {
	secret := []byte("never-print-this-key")
	payload := []byte("never-print-this-payload")
	message, err := NewDataShard(testSessionID, 1, 1, 1, 0, uint32(len(payload)), payload)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := AppendAuthenticated(nil, message, secret, MaxUDPPayload)
	if err != nil {
		t.Fatal(err)
	}
	tagHex := hex.EncodeToString(packet[len(packet)-AuthenticationTagSize:])
	packet[len(packet)-1] ^= 1
	_, err = DecodeAuthenticated(packet, secret, MaxUDPPayload)
	if err == nil {
		t.Fatal("tampered packet unexpectedly decoded")
	}
	for _, forbidden := range []string{string(secret), string(payload), tagHex} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatal("error exposed authentication material or payload")
		}
	}
}

func TestUnauthenticatedAllocationCountDoesNotScaleWithClaimedPayload(t *testing.T) {
	small := makeUnauthenticatedPacket(TypeClose, 0)
	large := makeUnauthenticatedPacket(TypeDataShard, MaxUDPPayload-MinimumPacketSize)
	smallAllocs := testing.AllocsPerRun(100, func() {
		_, _ = DecodeAuthenticated(small, []byte(testKey), MaxUDPPayload)
	})
	largeAllocs := testing.AllocsPerRun(100, func() {
		_, _ = DecodeAuthenticated(large, []byte(testKey), MaxUDPPayload)
	})
	if largeAllocs > smallAllocs+1 {
		t.Fatalf("unauthenticated large input allocations scale with payload: small=%v large=%v", smallAllocs, largeAllocs)
	}
}

func goldenFixtures(t *testing.T) []goldenFixture {
	t.Helper()
	helloMessage, helloErr := NewHello(testSessionID, 3, 2, 1200)
	hello := mustMessage(t, helloMessage, helloErr)
	helloAckMessage, helloAckErr := NewHelloAck(testSessionID, 3, 2, 1000)
	helloAck := mustMessage(t, helloAckMessage, helloAckErr)
	dataMessage, dataErr := NewDataShard(testSessionID, 0x0102030405060708, 3, 2, 4, 10, []byte{0xde, 0xad, 0xbe, 0xef})
	data := mustMessage(t, dataMessage, dataErr)
	pingMessage, pingErr := NewPing(testSessionID, 0x1122334455667788, 0x0102030405060708)
	ping := mustMessage(t, pingMessage, pingErr)
	pongMessage, pongErr := NewPong(testSessionID, 0x1122334455667788, 0x0102030405060708)
	pong := mustMessage(t, pongMessage, pongErr)
	closeValue, closeErr := NewClose(testSessionID)
	closeMessage := mustMessage(t, closeValue, closeErr)
	return []goldenFixture{
		{"hello", hello, "4d50554401010004000102030405060708090a0b0c0d0e0f030204b0833beb8bf2e1c9333c198a9e265f55f5a5e52156d02326e595b6a0f3aa6595e5", "ab20eed995e27a638eb78780bd9893e4f189b597aeb9a9d2832474469b50168e"},
		{"hello_ack", helloAck, "4d50554401020004000102030405060708090a0b0c0d0e0f030203e854d2f8c301403ac388e15c5e30037a4a7db06e34b6776bba51fcd860cc642e36", "e9ec593499bca62f13738f99524b8cfc3b0c60ba0fa937adc0395a79c6ddce5c"},
		{"data_shard", data, "4d50554401030013000102030405060708090a0b0c0d0e0f01020304050607080302040000000adeadbeef08fb48f166b97aac758410baa7de3e0525f0c3c40682b45b290b1d45427cc4d2", "b1f408fac4ab8871eeda6e62e3de79a892663be58a39e2a6c7e7184988a4fcaf"},
		{"ping", ping, "4d50554401040010000102030405060708090a0b0c0d0e0f1122334455667788010203040506070872d4e392c109689b758f6724a01afb3f3a2948dd5600852aa4690fa9769f2234", "13fb47fdd97023ebd50b910c19b92c08716a6ab1526535f7340dbb20434d583d"},
		{"pong", pong, "4d50554401050010000102030405060708090a0b0c0d0e0f112233445566778801020304050607086ce02b78cb7aeaffd3c823ef0575d19c029f59bd0b6d1a0f37bf50fdd3444c3e", "f1d6239acf1df26bed55a74c54116c628516bb3393f8d0cfafde5f203e71bdbd"},
		{"close", closeMessage, "4d50554401060000000102030405060708090a0b0c0d0e0f03108370ef4ec9963e864767fe5d57556de161e8b4bfd19577ee6f9d7324e943", "1f6d406c87b59d56c4104c6f4074fccffe975e9b234f9f86cc2111dc52917357"},
	}
}

func mustMessage(t *testing.T, message Message, err error) Message {
	t.Helper()
	if err != nil {
		t.Fatalf("construct message: %v", err)
	}
	return message
}

func mustDecodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatalf("decode test vector: %v", err)
	}
	return decoded
}

func fingerprint(packet []byte) string {
	sum := sha256.Sum256(packet)
	return hex.EncodeToString(sum[:])
}

func assertMessageEqual(t *testing.T, got, want Message) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("message mismatch: got type=%d want type=%d", got.Header.Type, want.Header.Type)
	}
}

func mutateByte(packet []byte, index int, value byte) []byte {
	mutated := append([]byte(nil), packet...)
	mutated[index] = value
	return mutated
}

func authenticatedRaw(packetType PacketType, sessionID SessionID, body, key []byte) []byte {
	packet := make([]byte, PrefixSize+len(body)+AuthenticationTagSize)
	packet[0], packet[1], packet[2], packet[3] = magic0, magic1, magic2, magic3
	packet[4] = Version
	packet[5] = byte(packetType)
	binary.BigEndian.PutUint16(packet[6:8], uint16(len(body)))
	copy(packet[8:24], sessionID[:])
	copy(packet[24:], body)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(packet[:PrefixSize+len(body)])
	copy(packet[PrefixSize+len(body):], mac.Sum(nil))
	return packet
}

func dataBody(packetID uint64, dataShards, parityShards, shardIndex uint8, originalLength uint32, payload []byte) []byte {
	body := make([]byte, DataShardMetadataSize+len(payload))
	binary.BigEndian.PutUint64(body[0:8], packetID)
	body[8], body[9], body[10] = dataShards, parityShards, shardIndex
	binary.BigEndian.PutUint32(body[11:15], originalLength)
	copy(body[15:], payload)
	return body
}

func makeUnauthenticatedPacket(packetType PacketType, bodySize int) []byte {
	packet := make([]byte, PrefixSize+bodySize+AuthenticationTagSize)
	packet[0], packet[1], packet[2], packet[3] = magic0, magic1, magic2, magic3
	packet[4] = Version
	packet[5] = byte(packetType)
	binary.BigEndian.PutUint16(packet[6:8], uint16(bodySize))
	copy(packet[8:24], testSessionID[:])
	return packet
}
