package handshakev2

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func TestPendingAndRejectionCaps(t *testing.T) {
	client, server := newSide(t, false, 1, nil), newSide(t, true, 1, nil)
	for range MaxPending {
		begin(t, client, 1, 1, startTime)
		deliver(t, pop(t, client, wirev2.TypeHello), server, true, startTime)
		pop(t, server, wirev2.TypeChallenge)
	}
	if snapshot := server.engine.Snapshot(); snapshot.Pending != MaxPending || snapshot.PacketBytes != MaxPending*PacketReservationBytes {
		t.Fatalf("pending capacity mismatch: %+v", snapshot)
	}
	a := client.engine.sessions[client.engine.orderedIDs()[0]]
	extra := slices.Clone(a.packets.hello[:])
	extra[8] ^= 0xff
	resign(extra, client.engine.handshakeKey)
	credits := server.ledger.Snapshot()
	if result, err := server.engine.Receive(startTime, reverse(sentPacket{binding: a.setup.Binding}, true), extra); !errors.Is(err, creditv2.ErrResourceLimit) || len(result.Sends) != 1 || result.Sends[0].Type != wirev2.TypeReject || server.ledger.Snapshot() != credits || server.engine.Snapshot().Pending != MaxPending {
		t.Fatalf("listener pending hard cap failed: %+v %v", result, err)
	}
	if _, _, err := client.engine.BeginDial(startTime, DialRequest{Policy: client.policy, Carriers: carriers(1)}); !errors.Is(err, creditv2.ErrResourceLimit) {
		t.Fatal("global pending/Dial bound not enforced")
	}
	closeSide(t, client, startTime)
	closeSide(t, server, startTime)

	client, server = newSide(t, false, 1, nil), newSide(t, true, 1, nil)
	client.policy.Profile.DataShards++
	begin(t, client, 1, 1, startTime)
	hello := pop(t, client, wirev2.TypeHello)
	for i := 0; i <= MaxRejections; i++ {
		packet := slices.Clone(hello.packet)
		binary.BigEndian.PutUint32(packet[20:24], uint32(i+1))
		resign(packet, server.engine.handshakeKey)
		for retry := 0; retry < 2; retry++ {
			result, err := server.engine.Receive(startTime, reverse(hello, true), packet)
			if err == nil {
				t.Fatal("incompatible HELLO accepted")
			}
			want := 0
			if retry == 0 && i < MaxRejections {
				want = 1
			}
			if len(result.Sends) != want {
				t.Fatalf("reject cache slot%d retry%d sends%d want%d", i, retry, len(result.Sends), want)
			}
		}
	}
	if snapshot := server.engine.Snapshot(); snapshot.Rejections != MaxRejections || snapshot.Pending != 0 || server.ledger.Snapshot().Bytes != 0 {
		t.Fatalf("rejections retained protocol credits: %+v", snapshot)
	}
	if !server.engine.NextDeadline().Equal(startTime.Add(Lifetime)) {
		t.Fatal("rejection expiry omitted from next deadline")
	}
	advance(t, server, startTime.Add(Lifetime))
	if server.engine.Snapshot().Rejections != 0 {
		t.Fatal("reject cache deadline extended by duplicates")
	}
	closeSide(t, client, startTime.Add(Lifetime))
	closeSide(t, server, startTime.Add(Lifetime))
}

func TestNextDeadlineRetainsExhaustedAttemptExpiry(t *testing.T) {
	client := newSide(t, false, 1, nil)
	if !client.engine.NextDeadline().IsZero() || !(*Engine)(nil).NextDeadline().IsZero() {
		t.Fatal("empty engine has scheduled work")
	}
	begin(t, client, 1, 1, startTime)
	for i := 1; i <= 6; i++ {
		due := startTime.Add(time.Duration(i) * RetryInterval)
		if !client.engine.NextDeadline().Equal(due) {
			t.Fatalf("retry%d deadline%v want%v", i, client.engine.NextDeadline(), due)
		}
		advance(t, client, due)
	}
	if !client.engine.NextDeadline().Equal(startTime.Add(Lifetime)) {
		t.Fatal("exhausted initial phase lost original expiry")
	}
	advance(t, client, startTime.Add(Lifetime))
	if !client.engine.NextDeadline().IsZero() {
		t.Fatal("retired attempt retained timer work")
	}
	closeSide(t, client, startTime.Add(Lifetime))
}

func TestSerialFallbackAndCallerDeadline(t *testing.T) {
	for _, callerDeadline := range []bool{false, true} {
		t.Run(fmt.Sprint(callerDeadline), func(t *testing.T) {
			client := newSide(t, false, 2, nil)
			request := DialRequest{Policy: client.policy, Carriers: carriers(2)}
			if callerDeadline {
				request.Deadline = startTime.Add(2 * time.Second)
			}
			_, _, err := client.engine.BeginDial(startTime, request)
			if err != nil {
				t.Fatal(err)
			}
			first := pop(t, client, wirev2.TypeHello)
			now := startTime.Add(Lifetime)
			if callerDeadline {
				now = request.Deadline
			}
			result := advance(t, client, now)
			if callerDeadline {
				if len(client.out) != 0 || client.engine.Snapshot().Dials != 0 || client.engine.Snapshot().Pending != 0 {
					t.Fatal("caller deadline started fallback")
				}
			} else {
				second := pop(t, client, wirev2.TypeHello)
				if len(result.Sends) != 1 || result.Sends[0].PathID != 2 || second.binding.SocketID != 2 || string(first.packet[8:24]) == string(second.packet[8:24]) {
					t.Fatal("serial fallback changed PathID or reused incarnation")
				}
			}
			closeSide(t, client, now)
		})
	}
}

func TestClosePendingSessionRefillsDial(t *testing.T) {
	client := newSide(t, false, 2, nil)
	begin(t, client, 2, 1, startTime)
	first := pop(t, client, wirev2.TypeHello)
	var id wirev2.SessionID
	copy(id[:], first.packet[8:24])
	result, err := client.engine.CloseSession(startTime, id)
	if err != nil || len(result.Sends) != 1 || result.Sends[0].PathID != 2 || client.engine.Snapshot().Pending != 1 {
		t.Fatalf("pending CloseSession stranded Dial: %+v %v", result, err)
	}
	closeSide(t, client, startTime)
}

func TestCancelAfterFinishIgnoresLateResponses(t *testing.T) {
	client, server := newSide(t, false, 1, nil), newSide(t, true, 1, nil)
	id := begin(t, client, 1, 1, startTime)
	hello := pop(t, client, wirev2.TypeHello)
	deliver(t, hello, server, true, startTime)
	deliver(t, pop(t, server, wirev2.TypeChallenge), client, false, startTime)
	deliver(t, pop(t, client, wirev2.TypeFinish), server, true, startTime)
	ready := pop(t, server, wirev2.TypeReady)
	result, err := client.engine.CancelDial(startTime, id)
	if err != nil || len(result.Sends) != 1 || result.Sends[0].Type != wirev2.TypeClose {
		t.Fatalf("cancellation did not emit one CLOSE: %+v %v", result, err)
	}
	deliver(t, pop(t, client, wirev2.TypeClose), server, true, startTime)
	deliver(t, ready, client, false, startTime)
	result, err = client.engine.CancelDial(startTime, id)
	if err != nil || len(result.Sends) != 0 || len(client.installed) != 0 || server.engine.Snapshot().Established != 0 {
		t.Fatal("late response resurrected cancelled attempt")
	}
	closeSide(t, client, startTime)
	closeSide(t, server, startTime)
}

func TestInstallationFailuresReleaseAllCredits(t *testing.T) {
	for _, kind := range []string{"error-disposer", "nil-disposer", "closed-scope"} {
		t.Run(kind, func(t *testing.T) {
			client, server := newSide(t, false, 1, nil), newSide(t, true, 1, nil)
			called := 0
			server.engine.config.Install = func(setup Setup) (func(), error) {
				dispose := func() { called++ }
				switch kind {
				case "error-disposer":
					return dispose, io.ErrClosedPipe
				case "nil-disposer":
					return nil, nil
				default:
					setup.Scope.Close()
					return dispose, nil
				}
			}
			begin(t, client, 1, 1, startTime)
			deliver(t, pop(t, client, wirev2.TypeHello), server, true, startTime)
			deliver(t, pop(t, server, wirev2.TypeChallenge), client, false, startTime)
			finish := pop(t, client, wirev2.TypeFinish)
			result, err := server.engine.Receive(startTime, reverse(finish, true), finish.packet)
			if !errors.Is(err, ErrInstallation) || len(result.Established) != 0 || len(result.Sends) != 1 || result.Sends[0].Type != wirev2.TypeClose || server.ledger.Snapshot().Bytes != 0 || server.ledger.Snapshot().SessionSlots != 0 {
				t.Fatalf("installer failure leaked/published state: %+v %v", result, err)
			}
			want := 1
			if kind == "nil-disposer" {
				want = 0
			}
			if called != want {
				t.Fatalf("disposer called%d want%d", called, want)
			}
			closeSide(t, client, startTime)
			closeSide(t, server, startTime)
		})
	}
}

func TestEntropyReentrancyAndPeerClose(t *testing.T) {
	client, server := newSide(t, false, 1, nil), newSide(t, true, 1, nil)
	server.engine.config.Entropy = &io.LimitedReader{R: server.engine.config.Entropy, N: 0}
	original := client.engine.config.Emit
	client.engine.config.Emit = func(binding Binding, packet []byte) error {
		if _, err := client.engine.Advance(startTime); !errors.Is(err, ErrReentrant) {
			t.Fatal("callback reentrancy accepted")
		}
		return original(binding, packet)
	}
	begin(t, client, 1, 1, startTime)
	hello := pop(t, client, wirev2.TypeHello)
	if result, err := server.engine.Receive(startTime, reverse(hello, true), hello.packet); !errors.Is(err, ErrEntropy) || len(result.Sends) != 0 || server.ledger.Snapshot().Bytes != 0 || server.ledger.Snapshot().SessionSlots != 0 {
		t.Fatalf("entropy failure leaked admission: %+v %v", result, err)
	}
	if _, err := client.engine.Advance(startTime.Add(-time.Nanosecond)); !errors.Is(err, ErrTime) {
		t.Fatal("backwards clock accepted")
	}
	client.ledger.Close()
	result := advance(t, client, startTime)
	if len(result.Failures) != 1 || !errors.Is(result.Failures[0].Err, ErrClosed) || len(result.Sends) != 0 {
		t.Fatalf("closed ledger retained attempt: %+v", result)
	}
	closeSide(t, client, startTime)
	closeSide(t, server, startTime)
}
