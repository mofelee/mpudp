package wirev2

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"
)

func TestFECBundleAppendDestinationsAndOwnership(t *testing.T) {
	v := establishedVectors(t)
	key := keyFrom(t, v["key"])
	for _, destination := range []struct {
		name string
		make func() []byte
	}{
		{"nil", func() []byte { return nil }},
		{"empty", func() []byte { return []byte{} }},
		{"empty_spare", func() []byte { return make([]byte, 0, 512) }},
		{"prefix", func() []byte { return []byte("prefix") }},
		{"prefix_spare", func() []byte { return append(make([]byte, 0, 512), "prefix"...) }},
	} {
		t.Run(destination.name, func(t *testing.T) {
			t.Parallel()
			bundle, err := DecodeFECBundle(authenticate(t, v["bundle_packet"], key), testLookup, 512)
			if err != nil {
				t.Fatal(err)
			}
			dst := destination.make()
			prefix := slices.Clone(dst)
			want := append(slices.Clone(prefix), v["bundle_packet"]...)
			packet, err := AppendFECBundle(dst, bundle, testLookup, key, 512)
			if err != nil || !bytes.Equal(packet, want) {
				t.Fatalf("destination changed independent wire vector: %v", err)
			}
			for range 8 {
				other, err := AppendFECBundle(nil, bundle, testLookup, key, 512)
				if err != nil || !bytes.Equal(other, v["bundle_packet"]) {
					t.Fatalf("subsequent encoding changed vector: %v", err)
				}
				clear(other)
			}
			for _, record := range bundle.Records {
				clear(record.Payload)
			}
			if !bytes.Equal(packet, want) {
				t.Fatal("retained packet aliases input payloads or subsequent output")
			}
			if _, err := ParseEnvelope(packet[len(prefix):]); err != nil {
				t.Fatal(err)
			}
			authenticate(t, packet[len(prefix):], key)
		})
	}
}

func TestFECBundleAppendAliasedRecords(t *testing.T) {
	v := establishedVectors(t)
	key := keyFrom(t, v["key"])
	for _, offsets := range [][3]int{{0, 96, 224}, {480, 320, 160}} {
		for _, prefixBytes := range []int{0, 8} {
			t.Run(fmt.Sprintf("offsets_%v/prefix_%d", offsets, prefixBytes), func(t *testing.T) {
				bundle, err := DecodeFECBundle(authenticate(t, v["bundle_packet"], key), testLookup, 512)
				if err != nil {
					t.Fatal(err)
				}
				backing := bytes.Repeat([]byte{0xac}, 1024)
				for i := range bundle.Records {
					record := &bundle.Records[i]
					copy(backing[offsets[i]:], record.Payload)
					record.Payload = backing[offsets[i] : offsets[i]+len(record.Payload)]
				}
				prefix := slices.Clone(backing[:prefixBytes])
				packet, err := AppendFECBundle(backing[:prefixBytes], bundle, testLookup, key, 512)
				if err != nil || !bytes.Equal(packet[:prefixBytes], prefix) || !bytes.Equal(packet[prefixBytes:], v["bundle_packet"]) {
					t.Fatalf("aliased record input changed wire vector: %v", err)
				}
				authenticate(t, packet[prefixBytes:], key)
			})
		}
	}
}

func TestFECBundleNilAppendRejectsBeforeAllocation(t *testing.T) {
	v := establishedVectors(t)
	key := keyFrom(t, v["key"])
	bundle, err := DecodeFECBundle(authenticate(t, v["bundle_packet"], key), testLookup, 512)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*FECBundle)
		key    Key
		lookup ContextLookup
		budget int
		want   error
	}{
		{"key", func(*FECBundle) {}, Key{}, testLookup, 512, ErrInvalidKey},
		{"type", func(b *FECBundle) { b.Header.Type = TypeClose }, key, testLookup, 512, ErrUnknownPacketType},
		{"route", func(b *FECBundle) { b.Route.PathID = 0 }, key, testLookup, 512, ErrInvalidRoute},
		{"lookup", func(*FECBundle) {}, key, nil, 512, ErrContextUnavailable},
		{"budget", func(*FECBundle) {}, key, testLookup, 511, ErrInvalidPayloadLimit},
		{"final_record", func(b *FECBundle) { b.Records[2].Payload = b.Records[2].Payload[:63] }, key, testLookup, 512, ErrInvalidFECBundle},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := bundle
			invalid.Records = slices.Clone(bundle.Records)
			test.mutate(&invalid)
			if packet, err := AppendFECBundle(nil, invalid, test.lookup, test.key, test.budget); packet != nil || !errors.Is(err, test.want) {
				t.Fatalf("invalid nil append: %v", err)
			}
			if allocations := testing.AllocsPerRun(20, func() {
				_, _ = AppendFECBundle(nil, invalid, test.lookup, test.key, test.budget)
			}); allocations != 0 {
				t.Fatalf("invalid append allocated before complete validation: %g", allocations)
			}
		})
	}
}
