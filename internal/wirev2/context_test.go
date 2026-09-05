package wirev2

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"slices"
	"testing"
)

func establishedVectors(t testing.TB) map[string][]byte {
	t.Helper()
	data, err := os.ReadFile("testdata/established.json")
	if err != nil {
		t.Fatal(err)
	}
	var values map[string]string
	if err := json.Unmarshal(data, &values); err != nil {
		t.Fatal(err)
	}
	result := make(map[string][]byte, len(values))
	for name, value := range values {
		result[name], err = hex.DecodeString(value)
		if err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func testContext(epoch uint32) EncodingContext {
	return EncodingContext{Epoch: epoch, LayoutID: 1, ProtectionID: 1, DataShards: 3, ParityShards: 2, ShardBytes: 64, MaxDescriptors: 4, MaxLogicalBytes: 192}
}

func TestEncodingContextVectors(t *testing.T) {
	v := establishedVectors(t)
	key := keyFrom(t, v["key"])
	packet := v["context_packet"]
	envelope := authenticate(t, packet, key)
	route, context, err := DecodeEncodingContext(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if context != testContext(7) || route != (Route{PathID: 3, Generation: 0x0102030405060708, BudgetEpoch: 9}) {
		t.Fatalf("decoded route/context mismatch: %+v %+v", route, context)
	}
	body, err := encodeContext(context)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body[:], v["context_body"]) {
		t.Fatal("context typed body differs")
	}
	reencoded, err := AppendEncodingContext(nil, envelope.Header().SessionID, route, context, key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reencoded, packet) || len(packet) != TypedBodyOverhead+24 {
		t.Fatal("context packet differs")
	}
	ack, err := NewEncodingContextAck(context)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ack.Digest[:], v["context_digest"]) {
		t.Fatal("context digest differs")
	}
	ackPacket, err := AppendEncodingContextAck(nil, envelope.Header().SessionID, route, ack, key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ackPacket, v["ack_packet"]) || len(ackPacket) != TypedBodyOverhead+36 {
		t.Fatal("ACK packet differs")
	}
	ackRoute, decodedAck, err := DecodeEncodingContextAck(authenticate(t, ackPacket, key))
	if err != nil {
		t.Fatal(err)
	}
	if ackRoute != route || decodedAck != ack {
		t.Fatal("ACK round trip differs")
	}
	if err := decodedAck.ValidateContext(context); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*EncodingContext){
		func(c *EncodingContext) { c.Epoch++ }, func(c *EncodingContext) { c.ShardBytes++ }, func(c *EncodingContext) { c.MaxDescriptors++ },
		func(c *EncodingContext) { c.MaxLogicalBytes-- }, func(c *EncodingContext) { c.ParityShards++ },
	} {
		changed := context
		mutate(&changed)
		if err := ack.ValidateContext(changed); !errors.Is(err, ErrContextAck) {
			t.Fatalf("altered context accepted: %v", err)
		}
	}
}

func TestEncodingContextBounds(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*EncodingContext)
	}{
		{"epoch_zero", func(c *EncodingContext) { c.Epoch = 0 }},
		{"layout", func(c *EncodingContext) { c.LayoutID = 2 }},
		{"protection", func(c *EncodingContext) { c.ProtectionID = 2 }},
		{"no_data", func(c *EncodingContext) { c.DataShards = 0 }},
		{"no_parity", func(c *EncodingContext) { c.ParityShards = 0 }},
		{"shard_sum", func(c *EncodingContext) { c.DataShards = 255; c.ParityShards = 2 }},
		{"shard_zero", func(c *EncodingContext) { c.ShardBytes = 0 }},
		{"shard_max", func(c *EncodingContext) { c.ShardBytes = MaxFECShardBytes + 1 }},
		{"descriptors_zero", func(c *EncodingContext) { c.MaxDescriptors = 0 }},
		{"descriptors_max", func(c *EncodingContext) { c.MaxDescriptors = MaxFECDescriptors + 1 }},
		{"logical_min", func(c *EncodingContext) { c.MaxLogicalBytes = 23 }},
		{"logical_product", func(c *EncodingContext) { c.MaxLogicalBytes = 193 }},
		{"logical_max", func(c *EncodingContext) { c.MaxLogicalBytes = math.MaxUint32 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			context := testContext(1)
			test.mutate(&context)
			if err := context.Validate(); !errors.Is(err, ErrInvalidContext) {
				t.Fatalf("got %v", err)
			}
			dst := []byte("keep")
			out, err := AppendEncodingContext(dst, SessionID{1}, Route{1, 1, 1}, context, Key{1})
			if !errors.Is(err, ErrInvalidContext) || !bytes.Equal(out, dst) {
				t.Fatalf("invalid context append: %v", err)
			}
		})
	}
	max := EncodingContext{Epoch: math.MaxUint32, LayoutID: 1, ProtectionID: 1, DataShards: 255, ParityShards: 1, ShardBytes: MaxFECShardBytes, MaxDescriptors: 256, MaxLogicalBytes: 255 * MaxFECShardBytes}
	if err := max.Validate(); err != nil {
		t.Fatalf("maximum context: %v", err)
	}
}

func TestContextCanonicalShapeAndAuthentication(t *testing.T) {
	v := establishedVectors(t)
	key := keyFrom(t, v["key"])
	for _, offset := range []int{14, 15, 20, 21, 22, 23} {
		packet := slices.Clone(v["context_packet"])
		packet[PrefixSize+RouteSize+offset] = 1
		envelope, err := ParseEnvelope(packet)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := envelope.Authenticate(key); !errors.Is(err, ErrAuthentication) {
			t.Fatalf("unverified reserved byte accepted: %v", err)
		}
		sign(packet, key)
		if _, _, err := DecodeEncodingContext(authenticate(t, packet, key)); !errors.Is(err, ErrMalformed) {
			t.Fatalf("reserved offset %d: %v", offset, err)
		}
	}
	for _, name := range []string{"context_packet", "ack_packet"} {
		original := authenticate(t, v[name], key)
		message, err := DecodeEstablished(original)
		if err != nil {
			t.Fatal(err)
		}
		for _, size := range []int{len(message.Body) - 1, len(message.Body) + 1} {
			body := make([]byte, size)
			copy(body, message.Body)
			packet, err := AppendEstablished(nil, message.Header, message.Route, body, key)
			if err != nil {
				t.Fatal(err)
			}
			if name == "context_packet" {
				_, _, err = DecodeEncodingContext(authenticate(t, packet, key))
			} else {
				_, _, err = DecodeEncodingContextAck(authenticate(t, packet, key))
			}
			if !errors.Is(err, ErrMalformed) {
				t.Fatalf("%s length %d: %v", name, size, err)
			}
		}
	}
	if _, _, err := DecodeEncodingContext(AuthenticatedEnvelope{}); !errors.Is(err, ErrAuthentication) {
		t.Fatal(err)
	}
	if _, _, err := DecodeEncodingContextAck(AuthenticatedEnvelope{}); !errors.Is(err, ErrAuthentication) {
		t.Fatal(err)
	}
}
