package wire

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestAuthenticator(t testing.TB, key []byte) *Authenticator {
	t.Helper()
	authenticator, err := NewAuthenticator(key)
	if err != nil {
		t.Fatal(err)
	}
	return authenticator
}

func TestAuthenticatorGoldenVectorsAndOutputOwnership(t *testing.T) {
	authenticator := newTestAuthenticator(t, []byte(testKey))
	for _, fixture := range goldenFixtures(t) {
		t.Run(fixture.name, func(t *testing.T) {
			want := mustDecodeHex(t, fixture.wireHex)
			prefix := []byte{1, 2, 3}
			got, err := authenticator.Append(bytes.Clone(prefix), fixture.message, MaxUDPPayload)
			if err != nil || !bytes.Equal(got, append(bytes.Clone(prefix), want...)) {
				t.Fatalf("cached encoding changed golden packet: %v", err)
			}
			for range 8 {
				encoded, err := authenticator.Append(nil, fixture.message, MaxUDPPayload)
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := DecodeAuthenticated(encoded, []byte(testKey), MaxUDPPayload)
				if err != nil {
					t.Fatal(err)
				}
				assertMessageEqual(t, decoded, fixture.message)
				decoded, err = authenticator.Decode(want, MaxUDPPayload)
				if err != nil {
					t.Fatal(err)
				}
				assertMessageEqual(t, decoded, fixture.message)
				clear(encoded)
			}
			if !bytes.Equal(got, append(bytes.Clone(prefix), want...)) {
				t.Fatal("cache reuse changed a retained encoded packet")
			}
		})
	}
}

func TestAuthenticatorDataBoundariesAndDecodedAlias(t *testing.T) {
	authenticator := newTestAuthenticator(t, []byte(testKey))
	for _, size := range []int{0, 1, 1129, MaxUDPPayload - DataShardOverhead} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			payload := bytes.Repeat([]byte{0x5a}, size)
			if size == 0 {
				payload = []byte{0}
			}
			message, err := NewDataShard(testSessionID, 1, 3, 2, 0, uint32(3*size), payload)
			if err != nil {
				t.Fatal(err)
			}
			want, err := AppendAuthenticated(nil, message, []byte(testKey), MaxUDPPayload)
			if err != nil {
				t.Fatal(err)
			}
			packet, err := authenticator.Append(nil, message, MaxUDPPayload)
			if err != nil || !bytes.Equal(packet, want) {
				t.Fatalf("boundary encoding differs: %v", err)
			}
			decoded, err := authenticator.Decode(packet, MaxUDPPayload)
			if err != nil {
				t.Fatal(err)
			}
			assertMessageEqual(t, decoded, message)
			packet[PrefixSize+DataShardMetadataSize] ^= 1
			if decoded.DataShard.Payload[0] != packet[PrefixSize+DataShardMetadataSize] {
				t.Fatal("decoded payload no longer aliases its input")
			}
			if message.DataShard.Payload[0] == decoded.DataShard.Payload[0] {
				t.Fatal("encoded output aliases the original message payload")
			}
		})
	}
}

func TestAuthenticatorClonesKeysIncludingFallback(t *testing.T) {
	message, err := NewClose(testSessionID)
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{1, 64, 65, 4096, 4097} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			key := bytes.Repeat([]byte{0x5a}, size)
			want, err := AppendAuthenticated(nil, message, key, MaxUDPPayload)
			if err != nil {
				t.Fatal(err)
			}
			authenticator := newTestAuthenticator(t, key)
			clear(key)
			for range 2 {
				packet, err := authenticator.Append(nil, message, MaxUDPPayload)
				if err != nil || !bytes.Equal(packet, want) {
					t.Fatalf("caller key mutation changed encoding: %v", err)
				}
			}
			held := authenticator.acquire()
			defer authenticator.release(held)
			if _, err := authenticator.Decode(want, MaxUDPPayload); err != nil {
				t.Fatalf("fresh fallback did not use the owned key: %v", err)
			}
			other := newTestAuthenticator(t, bytes.Repeat([]byte{0xa5}, size))
			if _, err := other.Decode(want, MaxUDPPayload); !errors.Is(err, ErrAuthentication) {
				t.Fatalf("separate authenticator accepted the wrong key: %v", err)
			}
		})
	}
}

func TestAuthenticatorInvalidKeysAndAppendAtomicity(t *testing.T) {
	for _, key := range [][]byte{nil, {}} {
		if authenticator, err := NewAuthenticator(key); authenticator != nil || !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("empty key accepted: %v", err)
		}
	}
	message, err := NewDataShard(testSessionID, 1, 1, 1, 0, 2, []byte{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	invalid := message
	invalid.DataShard.ShardIndex = 2
	authenticator := newTestAuthenticator(t, []byte(testKey))
	for _, test := range []struct {
		name   string
		auth   *Authenticator
		msg    Message
		budget int
		want   error
	}{
		{"nil authenticator", nil, message, 1200, ErrInvalidKey},
		{"zero authenticator", &Authenticator{}, message, 1200, ErrInvalidKey},
		{"low budget", authenticator, message, 71, ErrInvalidCapability},
		{"high budget", authenticator, message, 65508, ErrInvalidCapability},
		{"packet over budget", authenticator, message, 72, ErrPacketTooLarge},
		{"invalid message", authenticator, invalid, 1200, ErrInvalidFEC},
	} {
		t.Run(test.name, func(t *testing.T) {
			backing := bytes.Repeat([]byte{0xaa}, 4096)
			before := bytes.Clone(backing)
			got, err := test.auth.Append(backing[:4], test.msg, test.budget)
			if !errors.Is(err, test.want) || len(got) != 4 || !bytes.Equal(backing, before) {
				t.Fatalf("failed append changed output or spare capacity: %v", err)
			}
		})
	}
	if len(authenticator.states) != 0 {
		t.Fatal("invalid append allocated authentication state")
	}
	for _, invalid := range []*Authenticator{nil, {}} {
		if _, err := invalid.Decode(nil, MaxUDPPayload); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("invalid authenticator decode error = %v", err)
		}
	}
}

func TestAuthenticatorDecodeErrorsMatchStateless(t *testing.T) {
	key := []byte(testKey)
	authenticator := newTestAuthenticator(t, key)
	check := func(packet []byte, limit int) {
		t.Helper()
		want, wantErr := DecodeAuthenticated(packet, key, limit)
		got, gotErr := authenticator.Decode(packet, limit)
		if !errors.Is(gotErr, wantErr) {
			t.Fatalf("decode error = %v, want %v", gotErr, wantErr)
		}
		if wantErr == nil {
			assertMessageEqual(t, got, want)
		}
	}
	for _, fixture := range goldenFixtures(t) {
		packet := mustDecodeHex(t, fixture.wireHex)
		for length := 0; length < len(packet); length++ {
			check(packet[:length], MaxUDPPayload)
		}
		check(append(bytes.Clone(packet), 0), MaxUDPPayload)
		for index := range packet {
			for bit := byte(1); bit != 0; bit <<= 1 {
				mutated := bytes.Clone(packet)
				mutated[index] ^= bit
				check(mutated, MaxUDPPayload)
			}
		}
		for _, limit := range []int{71, 72, 1200, MaxUDPPayload, MaxUDPPayload + 1} {
			check(packet, limit)
		}
	}
	for _, packet := range [][]byte{
		authenticatedRaw(TypeClose, testSessionID, []byte{1}, key),
		authenticatedRaw(TypeHello, testSessionID, []byte{3, 2}, key),
		authenticatedRaw(TypeDataShard, testSessionID, dataBody(1, 3, 2, 5, 3, []byte{1}), key),
		authenticatedRaw(TypePing, testSessionID, make([]byte, probeBodySize), key),
	} {
		check(packet, MaxUDPPayload)
	}
	if len(authenticator.states) != 1 {
		t.Fatal("decode failure lost its borrowed authentication state")
	}
}

func TestAuthenticatorMalformedEnvelopeDoesNotAllocateState(t *testing.T) {
	authenticator := newTestAuthenticator(t, []byte(testKey))
	for _, packet := range [][]byte{nil, {1}, make([]byte, MinimumPacketSize), make([]byte, MaxUDPPayload+1)} {
		if _, err := authenticator.Decode(packet, MaxUDPPayload); err == nil {
			t.Fatal("malformed envelope was accepted")
		}
	}
	if len(authenticator.states) != 0 {
		t.Fatal("malformed envelope allocated authentication state")
	}
}

func TestAuthenticatorCacheExhaustionNeverWaitsAndBoundsRetention(t *testing.T) {
	authenticator := newTestAuthenticator(t, []byte(testKey))
	held := make([]*authenticationState, authenticationCacheCapacity)
	for index := range held {
		held[index] = authenticator.acquire()
	}
	done := make(chan error, 1)
	go func() {
		state := authenticator.acquire()
		defer authenticator.release(state)
		message, err := NewClose(testSessionID)
		if err == nil {
			var packet []byte
			packet, err = authenticator.Append(nil, message, MaxUDPPayload)
			if err == nil {
				_, err = authenticator.Decode(packet, MaxUDPPayload)
			}
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		for _, state := range held {
			authenticator.release(state)
		}
		t.Fatal("exhausted cache blocked authentication")
	}
	for _, state := range held {
		authenticator.release(state)
	}
	if len(authenticator.states) != authenticationCacheCapacity {
		t.Fatalf("retained %d states, want %d", len(authenticator.states), authenticationCacheCapacity)
	}
}

func TestAuthenticatorConcurrentPacketsKeepIndependentOutput(t *testing.T) {
	authenticator := newTestAuthenticator(t, []byte(testKey))
	var workers sync.WaitGroup
	for worker := range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			var retained [][]byte
			for index := range 100 {
				payload := bytes.Repeat([]byte{byte(worker), byte(index)}, 32)
				message, err := NewDataShard(testSessionID, uint64(worker*100+index+1), 3, 2, 0, uint32(len(payload)*3), payload)
				if err != nil {
					t.Error(err)
					return
				}
				packet, err := authenticator.Append(nil, message, 1200)
				if err != nil {
					t.Error(err)
					return
				}
				decoded, err := authenticator.Decode(packet, 1200)
				if err != nil || !bytes.Equal(decoded.DataShard.Payload, payload) {
					t.Errorf("concurrent decode failed: %v", err)
					return
				}
				retained = append(retained, packet)
			}
			for _, packet := range retained {
				if _, err := DecodeAuthenticated(packet, []byte(testKey), 1200); err != nil {
					t.Errorf("cache reuse changed retained output: %v", err)
				}
			}
		}()
	}
	workers.Wait()
	if len(authenticator.states) > authenticationCacheCapacity {
		t.Fatal("concurrent callers exceeded retained cache capacity")
	}
}

func TestAuthenticatorWarmCodecAllocations(t *testing.T) {
	key := []byte(testKey)
	authenticator := newTestAuthenticator(t, key)
	message, err := NewDataShard(testSessionID, 1, 3, 2, 0, 1440, make([]byte, 480))
	if err != nil {
		t.Fatal(err)
	}
	packet, err := authenticator.Append(nil, message, 1200)
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 0, len(packet))
	cryptoAllocations := testing.AllocsPerRun(100, func() {
		mac := hmac.New(sha256.New, key)
		_, _ = mac.Write(packet[:len(packet)-AuthenticationTagSize])
		_ = mac.Sum(nil)
	})
	for name, operation := range map[string]struct{ stateless, cached func() }{
		"append reused destination": {
			func() { _, _ = AppendAuthenticated(buffer, message, key, 1200) },
			func() { _, _ = authenticator.Append(buffer, message, 1200) },
		},
		"append fresh destination": {
			func() { _, _ = AppendAuthenticated(nil, message, key, 1200) },
			func() { _, _ = authenticator.Append(nil, message, 1200) },
		},
		"decode": {
			func() { _, _ = DecodeAuthenticated(packet, key, 1200) },
			func() { _, _ = authenticator.Decode(packet, 1200) },
		},
	} {
		t.Run(name, func(t *testing.T) {
			// Packet construction can allocate differently under race instrumentation.
			// Only the complete per-packet authentication cost must disappear.
			baseline := testing.AllocsPerRun(100, operation.stateless)
			if allocations := testing.AllocsPerRun(100, operation.cached); allocations != baseline-cryptoAllocations {
				t.Fatalf("warm codec allocations = %v, want %v - %v authentication allocations", allocations, baseline, cryptoAllocations)
			}
		})
	}
}
