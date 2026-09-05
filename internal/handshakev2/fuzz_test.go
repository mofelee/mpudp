package handshakev2

import (
	"slices"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/wirev2"
)

func checkInvariants(t testing.TB, s *side) {
	t.Helper()
	state, credits := s.engine.Snapshot(), s.ledger.Snapshot()
	if state.Pending < 0 || state.Pending > MaxPending || state.Dials > MaxPending || state.Rejections > MaxRejections || credits.SessionSlots != state.Pending+state.Established || credits.PendingHandshakes != state.Pending || credits.EstablishedSessions != state.Established {
		t.Fatalf("state/credit admission mismatch: %+v %+v", state, credits)
	}
	if credits.Bytes != uint64(state.Pending+state.Established)*testReceiveBytes+state.PacketBytes || credits.Reservations != state.Pending+state.Established+int(state.PacketBytes/PacketReservationBytes) {
		t.Fatalf("packet/receive ownership mismatch: %+v %+v", state, credits)
	}
	for _, a := range s.engine.sessions {
		if a.sends > MaxSends || a.sends < 1 || ((a.state == waitChallenge || a.state == waitFinish) && a.sends >= MaxSends) {
			t.Fatal("handshake send budget violated")
		}
	}
}

func FuzzHandshakeEvents(f *testing.F) {
	f.Add([]byte{0, 1, 1, 2, 2, 1, 1, 2, 2, 9})
	f.Add([]byte{0, 1, 2, 1, 7, 2, 1, 4, 4, 4})
	f.Add([]byte{0, 4, 6, 4, 6, 4, 8, 3, 5})
	f.Fuzz(func(t *testing.T, events []byte) {
		if len(events) > 128 {
			events = events[:128]
		}
		client, server := newSide(t, false, 2, nil), newSide(t, true, 2, nil)
		now := startTime
		for _, event := range events {
			s := client
			if event&16 != 0 {
				s = server
			}
			switch event % 10 {
			case 0:
				if client.engine.Snapshot().Dials < 8 {
					_, _, _ = client.engine.BeginDial(now, DialRequest{Policy: client.policy, Carriers: carriers(2), Concurrent: 2})
				}
			case 1, 2:
				from, to, listener := client, server, true
				if event%10 == 2 {
					from, to, listener = server, client, false
				}
				if len(from.out) != 0 {
					packet := from.out[0]
					from.out = from.out[1:]
					_, _ = to.engine.Receive(now, reverse(packet, listener), packet.packet)
				}
			case 3:
				_, _ = s.engine.Advance(now)
			case 4:
				now = now.Add(time.Second)
				_, _ = client.engine.Advance(now)
				_, _ = server.engine.Advance(now)
			case 5:
				if len(s.out) > 0 {
					s.out = s.out[1:]
				}
			case 6, 8:
				if len(s.out) > 0 {
					packet := s.out[0]
					packet.packet = slices.Clone(packet.packet)
					if event%10 == 8 {
						packet.packet[wirev2.PrefixSize+int(event)%(len(packet.packet)-wirev2.EnvelopeOverhead)] ^= event | 1
					}
					to, listener := server, true
					if s == server {
						to, listener = client, false
					}
					_, _ = to.engine.Receive(now, reverse(packet, listener), packet.packet)
				}
			case 7:
				var first DialID
				for id := range client.engine.dials {
					if first == 0 || id < first {
						first = id
					}
				}
				if first != 0 {
					_, _ = client.engine.CancelDial(now, first)
				}
			case 9:
				ids := s.engine.orderedIDs()
				if len(ids) > 0 {
					_, _ = s.engine.CloseSession(now, ids[0])
				}
			}
			checkInvariants(t, client)
			checkInvariants(t, server)
		}
		closeSide(t, client, now)
		closeSide(t, server, now)
	})
}
