package wirev2

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"unsafe"
)

func newTestAuthenticator(t testing.TB, key Key) *Authenticator {
	t.Helper()
	a, err := NewAuthenticator(key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	return a
}

func TestAuthenticatorFECVectorsDestinationsAndLifetime(t *testing.T) {
	v := establishedVectors(t)
	key := keyFrom(t, v["key"])
	for _, prefix := range []int{-1, 0, 8} {
		t.Run(fmt.Sprint(prefix), func(t *testing.T) {
			t.Parallel()
			ownedKey := key
			a := newTestAuthenticator(t, ownedKey)
			clear(ownedKey[:])
			bundle, err := DecodeFECBundle(authenticate(t, v["bundle_packet"], key), testLookup, 512)
			if err != nil {
				t.Fatal(err)
			}
			var dst []byte
			if prefix >= 0 {
				backing := bytes.Repeat([]byte{0xac}, 1024)
				// Both the empty and nonempty destinations overlap record inputs.
				for i, offset := range []int{0, 96, 224} {
					record := &bundle.Records[i]
					copy(backing[offset:], record.Payload)
					record.Payload = backing[offset : offset+len(record.Payload)]
				}
				dst = backing[:prefix]
			}
			want := append(slices.Clone(dst), v["bundle_packet"]...)
			packet, err := a.AppendFECBundle(dst, bundle, testLookup, 512)
			if err != nil || !bytes.Equal(packet, want) {
				t.Fatalf("directional encoder changed independent vector: %v", err)
			}
			fresh, err := DecodeFECBundle(authenticate(t, v["bundle_packet"], key), testLookup, 512)
			if err != nil {
				t.Fatal(err)
			}
			for range 8 {
				other, err := a.AppendFECBundle(nil, fresh, testLookup, 512)
				if err != nil || !bytes.Equal(other, v["bundle_packet"]) {
					t.Fatalf("reused hash changed packet: %v", err)
				}
				clear(other)
			}
			if prefix < 0 {
				for _, record := range bundle.Records {
					clear(record.Payload)
				}
			}
			for _, name := range []string{"context_packet", "ack_packet", "bundle_packet"} {
				for range 8 {
					envelope, err := ParseEnvelope(v[name])
					if err != nil {
						t.Fatal(err)
					}
					got, err := a.Authenticate(envelope)
					if err != nil || !bytes.Equal(got.Body(), v[name][PrefixSize:len(v[name])-AuthenticationTagSize]) {
						t.Fatalf("directional authentication changed %s: %v", name, err)
					}
				}
			}
			a.Close()
			a.Close()
			if a.key != (Key{}) || a.prefix != ([PrefixSize]byte{}) || a.tag != ([AuthenticationTagSize]byte{}) || a.mac != nil {
				t.Fatal("Close retained owned key, scratch, or hash reference")
			}
			if !bytes.Equal(packet, want) {
				t.Fatal("subsequent authentication or Close changed retained output")
			}
			authenticate(t, packet[max(prefix, 0):], key)
		})
	}
}

func TestAuthenticatorValidationOrderAndFailureReuse(t *testing.T) {
	v := establishedVectors(t)
	key := keyFrom(t, v["key"])
	a := newTestAuthenticator(t, key)
	for _, name := range []string{"context_packet", "ack_packet", "bundle_packet"} {
		packet := v[name]
		for index := range packet {
			mutated := slices.Clone(packet)
			mutated[index] ^= 1
			envelope, err := ParseEnvelope(mutated)
			if err != nil {
				continue
			}
			_, want := envelope.Authenticate(key)
			got, err := a.Authenticate(envelope)
			if !errors.Is(err, want) || !reflect.DeepEqual(got, AuthenticatedEnvelope{}) {
				t.Fatalf("tampered %s byte %d: %v, want %v", name, index, err, want)
			}
		}
	}
	envelope, err := ParseEnvelope(v["bundle_packet"])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Authenticate(envelope); err != nil {
		t.Fatalf("failed authentication poisoned reusable state: %v", err)
	}
	wrong := newTestAuthenticator(t, Key{1})
	if _, err := wrong.Authenticate(envelope); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("wrong direction/key accepted: %v", err)
	}
	if _, err := a.Authenticate(Envelope{}); !errors.Is(err, ErrMalformed) {
		t.Fatalf("empty envelope: %v", err)
	}
	bundle, err := DecodeFECBundle(authenticate(t, v["bundle_packet"], key), testLookup, 512)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*FECBundle){
		func(b *FECBundle) { b.Header.Type = TypeClose },
		func(b *FECBundle) { b.Route.PathID = 0 },
		func(b *FECBundle) { b.Records[2].Payload = b.Records[2].Payload[:63] },
	} {
		invalid := bundle
		invalid.Records = slices.Clone(bundle.Records)
		mutate(&invalid)
		backing := bytes.Repeat([]byte{0xac}, 1024)
		_, want := AppendFECBundle(nil, invalid, testLookup, key, 512)
		if out, err := a.AppendFECBundle(backing[:8], invalid, testLookup, 512); !errors.Is(err, want) || len(out) != 8 || !bytes.Equal(backing, bytes.Repeat([]byte{0xac}, 1024)) {
			t.Fatalf("failed append changed destination or error: %v", err)
		}
		if allocations := testing.AllocsPerRun(20, func() { _, _ = a.AppendFECBundle(nil, invalid, testLookup, 512) }); allocations != 0 {
			t.Fatalf("invalid append allocated: %g", allocations)
		}
	}
	if out, err := a.AppendFECBundle(nil, bundle, testLookup, 512); err != nil || !bytes.Equal(out, v["bundle_packet"]) {
		t.Fatalf("invalid append poisoned reusable state: %v", err)
	}
	if auth, err := NewAuthenticator(Key{}); auth != nil || !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("zero key accepted: %v", err)
	}
	a.Close()
	for _, invalid := range []*Authenticator{nil, {}, a} {
		called := false
		lookup := func(uint32) (EncodingContext, bool) { called = true; return EncodingContext{}, false }
		if _, err := invalid.AppendFECBundle(nil, bundle, lookup, 512); !errors.Is(err, ErrInvalidKey) || called {
			t.Fatalf("invalid authenticator reached context lookup: %v", err)
		}
		if _, err := invalid.Authenticate(envelope); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("invalid authenticator verified packet: %v", err)
		}
		invalid.Close()
	}
}

func TestAuthenticatorMaximumBundleAndBudget(t *testing.T) {
	a := newTestAuthenticator(t, Key{1})
	context := EncodingContext{Epoch: 7, LayoutID: 1, ProtectionID: 1, DataShards: 3, ParityShards: 2,
		ShardBytes: MaxFECShardBytes, MaxDescriptors: 1, MaxLogicalBytes: 3 * MaxFECShardBytes}
	lookup := func(epoch uint32) (EncodingContext, bool) { return context, epoch == context.Epoch }
	bundle := FECBundle{Header: Header{Type: TypeFECBundle, SessionID: SessionID{1}}, Route: Route{1, 1, 1},
		Records: []FECRecord{{GroupID: 1, EncodingEpoch: 7, LogicalBytes: 24, ShardIndex: 3, Payload: bytes.Repeat([]byte{0xa5}, MaxFECShardBytes)}}}
	packet, err := a.AppendFECBundle(nil, bundle, lookup, MaxUDPPayload)
	if err != nil || len(packet) != MaxUDPPayload {
		t.Fatalf("maximum cached encoding: %d %v", len(packet), err)
	}
	authenticate(t, packet, Key{1})
	if out, err := a.AppendFECBundle(nil, bundle, lookup, MaxUDPPayload-1); out != nil || !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("cached encoding exceeded budget: %v", err)
	}
}

func TestAuthenticatorRetainsBorrowedEnvelopeContract(t *testing.T) {
	v := establishedVectors(t)
	a := newTestAuthenticator(t, keyFrom(t, v["key"]))
	packet := slices.Clone(v["bundle_packet"])
	envelope, err := ParseEnvelope(packet)
	if err != nil {
		t.Fatal(err)
	}
	authenticated, err := a.Authenticate(envelope)
	if err != nil {
		t.Fatal(err)
	}
	body := authenticated.Body()
	a.Close()
	packet[PrefixSize] ^= 1
	if body[0] != packet[PrefixSize] {
		t.Fatal("authenticated view stopped borrowing the input packet")
	}
}

func TestAuthenticatorNativeGoStorageAllowance(t *testing.T) {
	a := newTestAuthenticator(t, Key{1})
	macType := reflect.TypeOf(a.mac).Elem()
	if macType.PkgPath() != "crypto/internal/fips140/hmac" {
		t.Skip("opaque or alternate provider needs its own storage audit")
	}
	digest := sha256.New()
	marshaled, err := digest.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	// Round each named native allocation upward independently. Include the
	// old 64-byte pads while the first Reset creates its keyed snapshots.
	round := func(bytes uintptr) uintptr { return (bytes + 127) &^ 127 }
	bound := round(unsafe.Sizeof(*a)) + round(macType.Size()) +
		2*round(reflect.TypeOf(digest).Elem().Size()) + 2*round(uintptr(cap(marshaled))) + 2*round(sha256.BlockSize)
	if bound > AuthenticatorStateBytes {
		t.Fatalf("reviewed native state %d exceeds prepaid allowance %d", bound, AuthenticatorStateBytes)
	}
}
