package wirev2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"slices"
	"testing"
)

func testLookup(epoch uint32) (EncodingContext, bool) {
	context := testContext(epoch)
	switch epoch {
	case 7:
		return context, true
	case 8:
		context.DataShards = 2
		context.ParityShards = 1
		context.ShardBytes = 96
		return context, true
	default:
		return EncodingContext{}, false
	}
}

func TestFECBundleVectorAndOwnedPayloads(t *testing.T) {
	v := establishedVectors(t)
	key := keyFrom(t, v["key"])
	packet := slices.Clone(v["bundle_packet"])
	calls := map[uint32]int{}
	lookup := func(epoch uint32) (EncodingContext, bool) { calls[epoch]++; return testLookup(epoch) }
	bundle, err := DecodeFECBundle(authenticate(t, packet, key), lookup, 512)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Records) != 3 || calls[7] != 1 || calls[8] != 1 {
		t.Fatalf("records/cached lookups: %d %+v", len(bundle.Records), calls)
	}
	if len(bundle.Records[0].Payload) != 64 || len(bundle.Records[1].Payload) != 96 || len(bundle.Records[2].Payload) != 64 {
		t.Fatal("mixed-epoch boundaries differ")
	}
	if bundle.Records[0].LogicalBytes != 25 || bundle.Records[1].LogicalBytes != 120 || bundle.Records[2].ShardIndex != 3 {
		t.Fatal("record metadata differs")
	}
	if !bytes.Equal(bundle.Records[2].Payload, bytes.Repeat([]byte{255}, 64)) {
		t.Fatal("parity was interpreted as data padding")
	}
	clear(calls)
	reencoded, err := AppendFECBundle([]byte("prefix"), bundle, lookup, key, 512)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reencoded[6:], v["bundle_packet"]) || !bytes.Equal(reencoded[:6], []byte("prefix")) {
		t.Fatal("independent bundle vector differs")
	}
	if calls[7] != 1 || calls[8] != 1 {
		t.Fatalf("encoder context lookup not cached: %+v", calls)
	}
	for _, record := range bundle.Records {
		if len(record.Payload) != cap(record.Payload) {
			t.Fatal("owned record has uncapped capacity")
		}
	}
	clear(packet)
	if bundle.Records[0].Payload[0] != 1 || bundle.Records[1].Payload[0] != 128 || bundle.Records[2].Payload[0] != 255 {
		t.Fatal("owned payload aliases receive buffer")
	}
	first := bundle.Records[0].Payload
	appended := append(first, 99)
	if appended[len(first)] != 99 || bundle.Records[1].Payload[0] != 128 {
		t.Fatal("appending record overwrote sibling")
	}
}

func TestFECBundleContextLookupFailures(t *testing.T) {
	v := establishedVectors(t)
	key := keyFrom(t, v["key"])
	envelope := authenticate(t, v["bundle_packet"], key)
	for _, test := range []struct {
		name   string
		lookup ContextLookup
		want   error
	}{
		{"nil", nil, ErrContextUnavailable},
		{"unknown", func(uint32) (EncodingContext, bool) { return EncodingContext{}, false }, ErrContextUnavailable},
		{"unacknowledged_second", func(epoch uint32) (EncodingContext, bool) {
			if epoch == 8 {
				return EncodingContext{}, false
			}
			return testLookup(epoch)
		}, ErrContextUnavailable},
		{"mismatched_epoch", func(epoch uint32) (EncodingContext, bool) {
			context, _ := testLookup(epoch)
			context.Epoch++
			return context, true
		}, ErrInvalidContext},
		{"invalid_context", func(epoch uint32) (EncodingContext, bool) {
			context, _ := testLookup(epoch)
			context.DataShards = 0
			return context, true
		}, ErrInvalidContext},
	} {
		t.Run(test.name, func(t *testing.T) {
			bundle, err := DecodeFECBundle(envelope, test.lookup, 512)
			if !errors.Is(err, test.want) || !reflect.DeepEqual(bundle, FECBundle{}) {
				t.Fatalf("partial result or wrong failure: %+v %v", bundle, err)
			}
		})
	}
	calls := map[uint32]int{}
	changing := func(epoch uint32) (EncodingContext, bool) {
		calls[epoch]++
		if calls[epoch] > 1 {
			return EncodingContext{}, false
		}
		return testLookup(epoch)
	}
	if _, err := DecodeFECBundle(envelope, changing, 512); err != nil {
		t.Fatalf("repeated lookup observed inconsistent state: %v", err)
	}
	if calls[7] != 1 || calls[8] != 1 {
		t.Fatalf("repeated lookup: %+v", calls)
	}
	called := false
	if _, err := DecodeFECBundle(AuthenticatedEnvelope{}, func(uint32) (EncodingContext, bool) { called = true; return EncodingContext{}, false }, 512); !errors.Is(err, ErrAuthentication) || called {
		t.Fatalf("preauthentication lookup: %v called=%v", err, called)
	}
}

func TestFECBundleMalformedRecords(t *testing.T) {
	v := establishedVectors(t)
	key := keyFrom(t, v["key"])
	original, err := DecodeEstablished(authenticate(t, v["bundle_packet"], key))
	if err != nil {
		t.Fatal(err)
	}
	second := FECBundlePrefixSize + FECRecordHeaderSize + 64
	third := second + FECRecordHeaderSize + 96
	for _, test := range []struct {
		name   string
		mutate func([]byte) []byte
		want   error
	}{
		{"count_zero", func(b []byte) []byte { clear(b[:2]); return b }, ErrInvalidFECBundle},
		{"count_max", func(b []byte) []byte { binary.BigEndian.PutUint16(b[:2], 17); return b }, ErrInvalidFECBundle},
		{"count_mismatch", func(b []byte) []byte { binary.BigEndian.PutUint16(b[:2], 2); return b }, ErrMalformed},
		{"bundle_flags", func(b []byte) []byte { b[3] = 1; return b }, ErrMalformed},
		{"record_flags", func(b []byte) []byte { b[third+17] = 1; return b }, ErrInvalidFECBundle},
		{"zero_group", func(b []byte) []byte { clear(b[third : third+8]); return b }, ErrInvalidFECBundle},
		{"duplicate_group_mixed_epoch", func(b []byte) []byte { copy(b[second:second+8], b[4:12]); return b }, ErrInvalidFECBundle},
		{"duplicate_group_same_epoch", func(b []byte) []byte { copy(b[third:third+8], b[4:12]); return b }, ErrInvalidFECBundle},
		{"zero_epoch", func(b []byte) []byte { clear(b[third+8 : third+12]); return b }, ErrInvalidContext},
		{"unknown_epoch", func(b []byte) []byte { binary.BigEndian.PutUint32(b[third+8:third+12], 9); return b }, ErrContextUnavailable},
		{"logical_min", func(b []byte) []byte { binary.BigEndian.PutUint32(b[third+12:third+16], 23); return b }, ErrInvalidFECBundle},
		{"logical_max", func(b []byte) []byte { binary.BigEndian.PutUint32(b[third+12:third+16], 193); return b }, ErrInvalidFECBundle},
		{"shard_index", func(b []byte) []byte { b[third+16] = 5; return b }, ErrInvalidFECBundle},
		{"partial_tail_padding", func(b []byte) []byte { b[second+FECRecordHeaderSize+24] = 1; return b }, ErrInvalidFECBundle},
		{"full_tail_padding", func(b []byte) []byte { b[third+16] = 2; return b }, ErrInvalidFECBundle},
		{"truncated_final_payload", func(b []byte) []byte { return b[:len(b)-1] }, ErrMalformed},
		{"truncated_header", func(b []byte) []byte { return b[:third+17] }, ErrMalformed},
		{"trailing", func(b []byte) []byte { return append(b, 0) }, ErrMalformed},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := test.mutate(slices.Clone(original.Body))
			packet, err := AppendEstablished(nil, original.Header, original.Route, body, key)
			if err != nil {
				t.Fatal(err)
			}
			envelope := authenticate(t, packet, key)
			bundle, err := DecodeFECBundle(envelope, testLookup, 512)
			if !errors.Is(err, test.want) || !reflect.DeepEqual(bundle, FECBundle{}) {
				t.Fatalf("partial result/wrong error: %+v %v, want %v", bundle, err, test.want)
			}
			if allocations := testing.AllocsPerRun(20, func() { _, _ = DecodeFECBundle(envelope, testLookup, 512) }); allocations != 0 {
				t.Fatalf("invalid bundle allocated ownership before full validation: %g", allocations)
			}
		})
	}
}

func TestFECBundleBoundsAndAtomicAppend(t *testing.T) {
	context := testContext(7)
	lookup := func(epoch uint32) (EncodingContext, bool) { return context, epoch == context.Epoch }
	bundle := FECBundle{Header: Header{Type: TypeFECBundle, SessionID: SessionID{1}}, Route: Route{1, 1, 1}}
	for i := 0; i < MaxFECRecords; i++ {
		bundle.Records = append(bundle.Records, FECRecord{GroupID: uint64(i + 1), EncodingEpoch: 7, LogicalBytes: 24, ShardIndex: 2, Payload: make([]byte, 64)})
	}
	packet, err := AppendFECBundle(nil, bundle, lookup, Key{1}, 1472)
	if err != nil {
		t.Fatal(err)
	}
	if len(packet) != TypedBodyOverhead+4+16*(18+64) {
		t.Fatal("encoded size differs")
	}
	decoded, err := DecodeFECBundle(authenticate(t, packet, Key{1}), lookup, 1472)
	if err != nil || len(decoded.Records) != 16 {
		t.Fatalf("16-record limit: %v", err)
	}
	called := false
	if _, err := DecodeFECBundle(authenticate(t, packet, Key{1}), func(epoch uint32) (EncodingContext, bool) { called = true; return lookup(epoch) }, 1200); !errors.Is(err, ErrPacketTooLarge) || called {
		t.Fatalf("budget not enforced before context lookup: %v called=%v", err, called)
	}
	for _, maxPayload := range []int{0, 511, 65508} {
		if _, err := DecodeFECBundle(authenticate(t, packet, Key{1}), lookup, maxPayload); !errors.Is(err, ErrInvalidPayloadLimit) {
			t.Fatal(err)
		}
	}
	backing := bytes.Repeat([]byte{0xac}, 2048)
	for _, mutate := range []func(*FECBundle){
		func(b *FECBundle) { b.Records = append(b.Records, b.Records[0]) },
		func(b *FECBundle) { b.Records = nil },
		func(b *FECBundle) { b.Records[15].GroupID = b.Records[0].GroupID },
		func(b *FECBundle) { b.Records[15].Payload = b.Records[15].Payload[:63] },
		func(b *FECBundle) { b.Records[15].LogicalBytes = 193 },
	} {
		invalid := bundle
		invalid.Records = slices.Clone(bundle.Records)
		mutate(&invalid)
		out, err := AppendFECBundle(backing[:8], invalid, lookup, Key{1}, 1472)
		if err == nil || len(out) != 8 || !bytes.Equal(backing, bytes.Repeat([]byte{0xac}, 2048)) {
			t.Fatalf("invalid append modified destination: %v", err)
		}
	}
	if _, err := AppendFECBundle(nil, bundle, lookup, Key{1}, 1200); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("send budget not enforced: %v", err)
	}
	context.ShardBytes = MaxFECShardBytes
	context.MaxLogicalBytes = uint32(context.DataShards) * uint32(context.ShardBytes)
	bundle.Records = []FECRecord{{GroupID: 1, EncodingEpoch: 7, LogicalBytes: 24, ShardIndex: 2, Payload: make([]byte, MaxFECShardBytes)}}
	packet, err = AppendFECBundle(nil, bundle, lookup, Key{1}, MaxUDPPayload)
	if err != nil || len(packet) != MaxUDPPayload {
		t.Fatalf("maximum payload: %d %v", len(packet), err)
	}
	if _, err := DecodeFECBundle(authenticate(t, packet, Key{1}), lookup, MaxUDPPayload); err != nil {
		t.Fatal(err)
	}
}

func TestFECBundleMaximumShardIndexAndID(t *testing.T) {
	context := EncodingContext{Epoch: math.MaxUint32, LayoutID: 1, ProtectionID: 1, DataShards: 255, ParityShards: 1, ShardBytes: 24, MaxDescriptors: 1, MaxLogicalBytes: 24}
	lookup := func(epoch uint32) (EncodingContext, bool) { return context, epoch == context.Epoch }
	bundle := FECBundle{
		Header:  Header{Type: TypeFECBundle, SessionID: SessionID{1}},
		Route:   Route{1, 1, 1},
		Records: []FECRecord{{GroupID: math.MaxUint64, EncodingEpoch: context.Epoch, LogicalBytes: 24, ShardIndex: 255, Payload: bytes.Repeat([]byte{0xfe}, 24)}},
	}
	packet, err := AppendFECBundle(nil, bundle, lookup, Key{1}, 512)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeFECBundle(authenticate(t, packet, Key{1}), lookup, 512)
	if err != nil || decoded.Records[0].ShardIndex != 255 || decoded.Records[0].GroupID != math.MaxUint64 {
		t.Fatalf("maximum index/ID rejected: %v", err)
	}
	context.DataShards = 254
	if _, err := DecodeFECBundle(authenticate(t, packet, Key{1}), lookup, 512); !errors.Is(err, ErrInvalidFECBundle) {
		t.Fatalf("index outside reduced shard set accepted: %v", err)
	}
}
