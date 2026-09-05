package handshakev2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func TestSignedWrongSizeHelloHasNoEffect(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprint(pending), func(t *testing.T) {
			client, server := newSide(t, false, 1, nil), newSide(t, true, 1, nil)
			begin(t, client, 1, 1, startTime)
			hello := pop(t, client, wirev2.TypeHello)
			if pending {
				deliver(t, hello, server, true, startTime)
				pop(t, server, wirev2.TypeChallenge)
			}
			before, credits := server.engine.Snapshot(), server.ledger.Snapshot()
			for size := wirev2.EnvelopeOverhead; size <= wirev2.HandshakePacketSize+1; size++ {
				if size == wirev2.HandshakePacketSize {
					continue
				}
				packet := make([]byte, size)
				copy(packet, hello.packet)
				binary.BigEndian.PutUint16(packet[6:8], uint16(size-wirev2.EnvelopeOverhead))
				resign(packet, server.engine.handshakeKey)
				result, err := server.engine.Receive(startTime, reverse(hello, true), packet)
				if err == nil || len(result.Sends) != 0 || server.engine.Snapshot() != before || server.ledger.Snapshot() != credits {
					t.Fatalf("signed size%d mutated state: %+v %v", size, result, err)
				}
				// The defensive rejection guard remains safe independently of framing.
				server.engine.reject(startTime, reverse(hello, true), packet, 3, &result)
				if len(server.out) != 0 || len(result.Sends) != 0 || server.engine.Snapshot() != before {
					t.Fatalf("wrong-size reject responded at%d", size)
				}
			}
			closeSide(t, client, startTime)
			closeSide(t, server, startTime)
		})
	}
}

func TestEachLostPacketRecoversWithExactRetry(t *testing.T) {
	kinds := []wirev2.PacketType{wirev2.TypeHello, wirev2.TypeChallenge, wirev2.TypeFinish, wirev2.TypeReady}
	for _, lost := range kinds {
		t.Run(fmt.Sprint(lost), func(t *testing.T) {
			client, server := newSide(t, false, 1, nil), newSide(t, true, 1, nil)
			begin(t, client, 1, 1, startTime)
			now := startTime
			for i, kind := range kinds {
				from, to := client, server
				if i%2 == 1 {
					from, to = server, client
				}
				packet := pop(t, from, kind)
				if kind == lost {
					now = now.Add(RetryInterval)
					advance(t, from, now)
					retry := pop(t, from, kind)
					if !bytes.Equal(packet.packet, retry.packet) {
						t.Fatal("retry changed bytes")
					}
					packet = retry
				}
				deliver(t, packet, to, i%2 == 0, now)
			}
			if len(client.installed) != 1 || len(server.installed) != 1 {
				t.Fatal("loss failed to establish exactly once")
			}
			closeSide(t, client, now)
			closeSide(t, server, now)
		})
	}
}

func TestSendBudgetFailureAndExactDeadline(t *testing.T) {
	client, server := newSide(t, false, 1, nil), newSide(t, true, 1, nil)
	originalEmit := client.engine.config.Emit
	client.engine.config.Emit = func(binding Binding, packet []byte) error {
		_ = originalEmit(binding, packet)
		return io.ErrClosedPipe
	}
	_, result, err := client.engine.BeginDial(startTime, DialRequest{Policy: client.policy, Carriers: carriers(1)})
	if err != nil || len(result.Sends) != 1 || !errors.Is(result.Sends[0].Err, io.ErrClosedPipe) {
		t.Fatalf("send failure not counted: %+v %v", result, err)
	}
	hello := pop(t, client, wirev2.TypeHello)
	deliver(t, hello, server, true, startTime)
	challenge := pop(t, server, wirev2.TypeChallenge)
	for second := 1; second <= 7; second++ {
		result := advance(t, client, startTime.Add(time.Duration(second)*time.Second))
		if second < 7 && (len(result.Sends) != 1 || !errors.Is(result.Sends[0].Err, io.ErrClosedPipe)) {
			t.Fatal("failed retry escaped budget")
		}
		if second == 7 && len(result.Sends) != 0 {
			t.Fatal("initial phase spent reserved final send")
		}
	}
	if len(client.out) != 6 {
		t.Fatal("wrong initial retry count")
	}
	client.out = nil
	deliver(t, challenge, client, false, startTime.Add(7*time.Second))
	pop(t, client, wirev2.TypeFinish)
	if result := advance(t, client, startTime.Add(9*time.Second)); len(result.Sends) != 0 || client.engine.Snapshot().Pending != 1 {
		t.Fatal("eight-send cap or original pending lifetime violated")
	}
	result = advance(t, client, startTime.Add(Lifetime))
	if len(result.Failures) != 1 || !errors.Is(result.Failures[0].Err, ErrExpired) || len(result.Sends) != 1 || result.Sends[0].Type != wirev2.TypeClose || client.engine.Snapshot().Dials != 0 {
		t.Fatalf("exact deadline did not cancel once: %+v", result)
	}
	if result := advance(t, client, startTime.Add(Lifetime)); len(result.Sends) != 0 {
		t.Fatal("expiry CLOSE repeated")
	}
	closeSide(t, client, startTime.Add(Lifetime))
	closeSide(t, server, startTime.Add(Lifetime))
}

func TestReservationFailureRollsBackBeforeChallenge(t *testing.T) {
	for _, kind := range []string{"session-bytes", "peer-bytes", "reservation-count"} {
		t.Run(kind, func(t *testing.T) {
			bound := limits()
			switch kind {
			case "session-bytes":
				bound.MaxSessionBytes = testReceiveBytes + PacketReservationBytes - 1
			case "peer-bytes":
				bound.MaxPeerBytes, bound.MaxSessionBytes = testReceiveBytes+PacketReservationBytes-1, testReceiveBytes+PacketReservationBytes-1
			case "reservation-count":
				bound.MaxReservations = 1
			}
			client, server := newSide(t, false, 1, nil), newSide(t, true, 1, &bound)
			begin(t, client, 1, 1, startTime)
			hello := pop(t, client, wirev2.TypeHello)
			result, err := server.engine.Receive(startTime, reverse(hello, true), hello.packet)
			if !errors.Is(err, creditv2.ErrResourceLimit) || len(result.Sends) != 1 || result.Sends[0].Type != wirev2.TypeReject || server.ledger.Snapshot().SessionSlots != 0 || server.ledger.Snapshot().Bytes != 0 || server.engine.Snapshot().Pending != 0 {
				t.Fatalf("partial admission survived refusal: %+v %v %+v", result, err, server.ledger.Snapshot())
			}
			closeSide(t, client, startTime)
			closeSide(t, server, startTime)
		})
	}
}

func TestPolicyReservesAdvertisedReceiveMinimum(t *testing.T) {
	_, profile := testProfile(t, 1)
	policy := Policy{Profile: profile, Receive: creditv2.Claim{Bytes: testReceiveBytes}}
	if err := validatePolicy(policy, false); err != nil {
		t.Fatal(err)
	}
	policy.Receive.Bytes--
	if !errors.Is(validatePolicy(policy, false), ErrInvalid) {
		t.Fatal("Datagram history under-reserved")
	}
	profile.Protocol, profile.LayoutID, profile.DataShards, profile.ParityShards = negotiationv2.KCP, 0, 0, 0
	profile.OfferedCaps, profile.RequiredCaps = negotiationv2.NativeKCP, negotiationv2.NativeKCP
	profile.Datagram, profile.Epochs.MaxMigrations, profile.Payload.InnerKCPBytes = negotiationv2.DatagramLimits{}, 0, 1500
	profile.Streams = negotiationv2.StreamLimits{MaxPendingAccepts: 1, SessionReceiveBytes: 1 << 20, StreamReceiveBytes: 1 << 20}
	policy.Profile, policy.Receive.Bytes = profile, 1
	if !errors.Is(validatePolicy(policy, false), ErrInvalid) {
		t.Fatal("reliable advertised receive obligation under-reserved")
	}
	policy.Receive.Bytes = 1 << 20
	if err := validatePolicy(policy, false); err != nil {
		t.Fatal(err)
	}
}

func TestTupleTranscriptAndReflectionSubstitution(t *testing.T) {
	client, server := newSide(t, false, 1, nil), newSide(t, true, 1, nil)
	begin(t, client, 1, 1, startTime)
	hello := pop(t, client, wirev2.TypeHello)
	deliver(t, hello, server, true, startTime)
	challenge := pop(t, server, wirev2.TypeChallenge)
	binding := reverse(challenge, false)
	for _, change := range []func(*Binding){func(b *Binding) { b.SocketID++ }, func(b *Binding) { b.Local = b.Remote }, func(b *Binding) { b.Remote = b.Local }} {
		wrong := binding
		change(&wrong)
		before := client.engine.Snapshot()
		result, err := client.engine.Receive(startTime, wrong, challenge.packet)
		if err != nil || len(result.Sends) != 0 || client.engine.Snapshot() != before {
			t.Fatal("wrong binding changed attempt")
		}
	}
	bad := slices.Clone(challenge.packet)
	bad[wirev2.PrefixSize] ^= 1
	resign(bad, client.engine.handshakeKey)
	if _, err := client.engine.Receive(startTime, binding, bad); err == nil || len(client.out) != 0 {
		t.Fatal("valid-MAC nonce substitution advanced transcript")
	}
	deliver(t, challenge, client, false, startTime)
	finish := pop(t, client, wirev2.TypeFinish)
	bad = slices.Clone(challenge.packet)
	bad[wirev2.PrefixSize+16] ^= 1
	resign(bad, client.engine.handshakeKey)
	if result, err := client.engine.Receive(startTime.Add(time.Second), binding, bad); err != nil || len(result.Sends) != 0 {
		t.Fatal("late alternate CHALLENGE replaced transcript")
	}
	if result, err := client.engine.Receive(startTime.Add(time.Second), binding, finish.packet); err != nil || len(result.Sends) != 0 {
		t.Fatal("reflected FINISH advanced initiator")
	}
	deliver(t, finish, server, true, startTime.Add(time.Second))
	ready := pop(t, server, wirev2.TypeReady)
	deliver(t, ready, client, false, startTime.Add(time.Second))
	var body [8]byte
	binary.BigEndian.PutUint16(body[:2], 6)
	binary.BigEndian.PutUint32(body[4:], 1)
	setup := client.installed[0]
	wrongRoute, err := wirev2.AppendEstablished(nil, wirev2.Header{Type: wirev2.TypeClose, SessionID: setup.ID}, wirev2.Route{PathID: 2, Generation: 1, BudgetEpoch: 1}, body[:], setup.Keys.ClientToServer)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := server.engine.Receive(startTime.Add(time.Second), reverse(finish, true), wrongRoute); !errors.Is(err, wirev2.ErrInvalidRoute) || len(result.Closed) != 0 || server.engine.Snapshot().Established != 1 {
		t.Fatal("authenticated wrong-route CLOSE retired another bootstrap route")
	}
	closeSide(t, client, startTime.Add(time.Second))
	closeSide(t, server, startTime.Add(time.Second))
}

func TestAuthenticatedMalformedHelloRejectsOnceWithoutAdmission(t *testing.T) {
	client, server := newSide(t, false, 1, nil), newSide(t, true, 1, nil)
	begin(t, client, 1, 1, startTime)
	hello := pop(t, client, wirev2.TypeHello)
	hello.packet[wirev2.PrefixSize+82] = 1
	resign(hello.packet, client.engine.handshakeKey)
	for i := 0; i < 2; i++ {
		result, err := server.engine.Receive(startTime, reverse(hello, true), hello.packet)
		if !errors.Is(err, wirev2.ErrMalformed) || len(result.Sends) != 1-i || server.ledger.Snapshot().SessionSlots != 0 || server.ledger.Snapshot().Bytes != 0 {
			t.Fatalf("malformed authenticated HELLO admission/refusal: %+v %v", result, err)
		}
	}
	closeSide(t, client, startTime)
	closeSide(t, server, startTime)
}
