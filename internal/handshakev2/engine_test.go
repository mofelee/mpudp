package handshakev2

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/rand"
	"net/netip"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

var startTime = time.Unix(1000, 0)

const testReceiveBytes = 32768

type sentPacket struct {
	binding Binding
	packet  []byte
}

type side struct {
	engine    *Engine
	ledger    *creditv2.Peer
	policy    Policy
	out       []sentPacket
	installed []Setup
	disposed  map[wirev2.SessionID]int
	storage   map[wirev2.SessionID][]byte
}

func testProfile(t testing.TB, paths int) ([]byte, negotiationv2.Profile) {
	t.Helper()
	data, err := os.ReadFile("../wirev2/testdata/handshake.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors map[string]string
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	psk, err := hex.DecodeString(vectors["psk"])
	if err != nil {
		t.Fatal(err)
	}
	packet, err := hex.DecodeString(vectors["hello"])
	if err != nil {
		t.Fatal(err)
	}
	key, err := wirev2.DeriveHandshakeKey(psk)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := authenticate(packet, key)
	if err != nil {
		t.Fatal(err)
	}
	message, err := wirev2.DecodeHandshake(envelope)
	if err != nil {
		t.Fatal(err)
	}
	hello, err := negotiationv2.DecodeHello(message)
	if err != nil {
		t.Fatal(err)
	}
	hello.MaxPaths = uint16(paths)
	return psk, hello.Profile
}

func limits() creditv2.Limits {
	return creditv2.Limits{MaxPeerBytes: 64 << 20, MaxSessionBytes: 1 << 20, MaxSessions: 1024, MaxPendingHandshakes: 256, MaxPendingAccepts: 1024, MaxStreamsPerSession: 128, MaxPeerStreams: 4096, MaxReservations: 4096}
}

func newSide(t testing.TB, listener bool, paths int, custom *creditv2.Limits) *side {
	t.Helper()
	psk, profile := testProfile(t, paths)
	bound := limits()
	if custom != nil {
		bound = *custom
	}
	ledger, err := creditv2.New(bound)
	if err != nil {
		t.Fatal(err)
	}
	s := &side{ledger: ledger, policy: Policy{Profile: profile, Receive: creditv2.Claim{Bytes: testReceiveBytes, PendingAccept: listener}}, disposed: make(map[wirev2.SessionID]int), storage: make(map[wirev2.SessionID][]byte)}
	config := Config{Credits: ledger, Entropy: rand.New(rand.NewSource(42))}
	if listener {
		config.Listener = &s.policy
		config.Entropy = rand.New(rand.NewSource(93))
	}
	config.Emit = func(binding Binding, packet []byte) error {
		if wirev2.PacketType(packet[5]) == wirev2.TypeReady {
			var id wirev2.SessionID
			copy(id[:], packet[8:24])
			if s.storage[id] == nil {
				t.Fatal("READY emitted before installation")
			}
		}
		s.out = append(s.out, sentPacket{binding: binding, packet: slices.Clone(packet)})
		return nil
	}
	config.Install = func(setup Setup) (func(), error) {
		if setup.Scope.Snapshot().PendingHandshake {
			t.Fatal("installation before credit promotion")
		}
		if setup.Scope.Snapshot().Bytes < setup.Receive.Bytes+PacketReservationBytes {
			t.Fatal("installation without complete pre-reserved credits")
		}
		s.installed = append(s.installed, setup)
		s.storage[setup.ID] = make([]byte, setup.Receive.Bytes)
		return func() {
			if setup.Scope.Snapshot().Bytes < setup.Receive.Bytes {
				t.Fatal("receive credits released before disposal")
			}
			clear(s.storage[setup.ID])
			delete(s.storage, setup.ID)
			s.disposed[setup.ID]++
		}, nil
	}
	s.engine, err = New(psk, config)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func carriers(count int) []Carrier {
	result := make([]Carrier, count)
	for i := range result {
		result[i] = Carrier{PathID: uint16(i + 1), Binding: Binding{SocketID: uint64(i + 1), Local: netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(30001+i)), Remote: netip.MustParseAddrPort("127.0.0.1:40000")}}
	}
	return result
}

func reverse(packet sentPacket, listener bool) Binding {
	result := Binding{Local: packet.binding.Remote, Remote: packet.binding.Local, SocketID: 100}
	if !listener {
		result.SocketID = uint64(result.Local.Port() - 30000)
	}
	return result
}

func pop(t testing.TB, s *side, kind wirev2.PacketType) sentPacket {
	t.Helper()
	if len(s.out) == 0 {
		t.Fatalf("missing packet type%d", kind)
	}
	packet := s.out[0]
	s.out = s.out[1:]
	if wirev2.PacketType(packet.packet[5]) != kind {
		t.Fatalf("packet type%d want%d", packet.packet[5], kind)
	}
	return packet
}

func deliver(t testing.TB, from sentPacket, to *side, listener bool, now time.Time) Result {
	t.Helper()
	result, err := to.engine.Receive(now, reverse(from, listener), from.packet)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func begin(t testing.TB, client *side, paths, concurrent int, now time.Time) DialID {
	t.Helper()
	id, _, err := client.engine.BeginDial(now, DialRequest{Policy: client.policy, Carriers: carriers(paths), Concurrent: concurrent})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func advance(t testing.TB, s *side, now time.Time) Result {
	t.Helper()
	result, err := s.engine.Advance(now)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func closeSide(t testing.TB, s *side, now time.Time) {
	t.Helper()
	if _, err := s.engine.Close(now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.engine.Close(now); err != nil {
		t.Fatal(err)
	}
	snapshot := s.ledger.Snapshot()
	if snapshot.Bytes != 0 || snapshot.Reservations != 0 || snapshot.SessionSlots != 0 || snapshot.PendingHandshakes != 0 || snapshot.EstablishedSessions != 0 || snapshot.PendingAccepts != 0 || snapshot.BusinessStreams != 0 {
		t.Fatalf("leaked credits: %+v", snapshot)
	}
	for id, count := range s.disposed {
		if count != 1 {
			t.Fatalf("disposal count for%v=%d", id, count)
		}
	}
	if len(s.storage) != 0 {
		t.Fatal("installed storage retained")
	}
}

func TestExchangeAdmissionInstallationAndCleanup(t *testing.T) {
	client, server := newSide(t, false, 1, nil), newSide(t, true, 1, nil)
	begin(t, client, 1, 1, startTime)
	hello := pop(t, client, wirev2.TypeHello)
	deliver(t, hello, server, true, startTime)
	if snapshot := server.ledger.Snapshot(); snapshot.PendingHandshakes != 1 || snapshot.SessionSlots != 1 || snapshot.PendingAccepts != 1 || snapshot.EstablishedSessions != 0 || len(server.installed) != 0 {
		t.Fatalf("HELLO allocation boundary: %+v", snapshot)
	}
	challenge := pop(t, server, wirev2.TypeChallenge)
	deliver(t, challenge, client, false, startTime)
	finish := pop(t, client, wirev2.TypeFinish)
	result := deliver(t, finish, server, true, startTime)
	if len(result.Established) != 1 || len(server.installed) != 1 {
		t.Fatal("FINISH did not install once")
	}
	ready := pop(t, server, wirev2.TypeReady)
	result = deliver(t, ready, client, false, startTime)
	if len(result.Established) != 1 || result.Established[0].PathID != 1 || client.engine.Snapshot().Dials != 0 {
		t.Fatal("READY did not complete Dial")
	}
	id := result.Established[0].ID
	if !client.engine.NextDeadline().IsZero() || !server.engine.NextDeadline().Equal(startTime.Add(RetryInterval)) {
		t.Fatal("installed retry deadline retained on wrong endpoint")
	}
	if err := server.engine.MarkAccepted(startTime, id); err != nil {
		t.Fatal(err)
	}
	if err := server.engine.MarkAccepted(startTime, id); err != nil {
		t.Fatal(err)
	}
	if snapshot := server.ledger.Snapshot(); snapshot.PendingAccepts != 0 || snapshot.Bytes != testReceiveBytes+PacketReservationBytes {
		t.Fatalf("accept lost storage ownership: %+v", snapshot)
	}
	advance(t, server, startTime.Add(Lifetime))
	if !server.engine.NextDeadline().IsZero() {
		t.Fatal("expired listener proof retained timer work")
	}
	if snapshot := server.ledger.Snapshot(); snapshot.Bytes != testReceiveBytes || snapshot.EstablishedSessions != 1 {
		t.Fatalf("retry proof lifetime changed Session ownership: %+v", snapshot)
	}
	closeSide(t, client, startTime.Add(Lifetime))
	closeSide(t, server, startTime.Add(Lifetime))
}

func TestExactRetriesAndFreshChallengeAfterExpiry(t *testing.T) {
	client, server := newSide(t, false, 1, nil), newSide(t, true, 1, nil)
	begin(t, client, 1, 1, startTime)
	hello := pop(t, client, wirev2.TypeHello)
	deliver(t, hello, server, true, startTime)
	challenge := pop(t, server, wirev2.TypeChallenge)
	deliver(t, hello, server, true, startTime.Add(time.Millisecond))
	if len(server.out) != 0 {
		t.Fatal("duplicate bypassed retry interval")
	}
	deliver(t, hello, server, true, startTime.Add(time.Second))
	if retry := pop(t, server, wirev2.TypeChallenge); !bytes.Equal(retry.packet, challenge.packet) {
		t.Fatal("HELLO changed pending challenge")
	}
	deliver(t, challenge, client, false, startTime.Add(time.Second))
	finish := pop(t, client, wirev2.TypeFinish)
	deliver(t, challenge, client, false, startTime.Add(2*time.Second))
	if retry := pop(t, client, wirev2.TypeFinish); !bytes.Equal(retry.packet, finish.packet) {
		t.Fatal("CHALLENGE changed FINISH")
	}
	deliver(t, finish, server, true, startTime.Add(2*time.Second))
	ready := pop(t, server, wirev2.TypeReady)
	deliver(t, finish, server, true, startTime.Add(3*time.Second))
	if retry := pop(t, server, wirev2.TypeReady); !bytes.Equal(retry.packet, ready.packet) || len(server.installed) != 1 {
		t.Fatal("duplicate FINISH reinstalled or changed READY")
	}
	id := server.installed[0].ID
	if _, err := server.engine.CloseSession(startTime.Add(4*time.Second), id); err != nil {
		t.Fatal(err)
	}
	pop(t, server, wirev2.TypeClose)
	deliver(t, finish, server, true, startTime.Add(5*time.Second))
	if len(server.out) != 0 || server.engine.Snapshot().Pending != 0 {
		t.Fatal("old FINISH recreated closed attempt")
	}
	deliver(t, hello, server, true, startTime.Add(11*time.Second))
	fresh := pop(t, server, wirev2.TypeChallenge)
	if bytes.Equal(fresh.packet, challenge.packet) {
		t.Fatal("closed HELLO reused old challenge")
	}
	if _, err := server.engine.Receive(startTime.Add(11*time.Second), reverse(finish, true), finish.packet); !errors.Is(err, wirev2.ErrAuthentication) {
		t.Fatalf("old FINISH accepted in new incarnation: %v", err)
	}
	closeSide(t, client, startTime.Add(21*time.Second))
	closeSide(t, server, startTime.Add(21*time.Second))
}

func TestFirstWinnerClosesAlreadyCommittedSibling(t *testing.T) {
	client, server := newSide(t, false, 2, nil), newSide(t, true, 2, nil)
	begin(t, client, 2, 2, startTime)
	for range 2 {
		deliver(t, pop(t, client, wirev2.TypeHello), server, true, startTime)
	}
	for range 2 {
		deliver(t, pop(t, server, wirev2.TypeChallenge), client, false, startTime)
	}
	for range 2 {
		deliver(t, pop(t, client, wirev2.TypeFinish), server, true, startTime)
	}
	first, second := pop(t, server, wirev2.TypeReady), pop(t, server, wirev2.TypeReady)
	result := deliver(t, second, client, false, startTime)
	if len(result.Established) != 1 || result.Established[0].PathID != 2 || result.Established[0].Contract.BootstrapPathID != 2 {
		t.Fatal("winning configured Carrier renumbered")
	}
	closePacket := pop(t, client, wirev2.TypeClose)
	closed := deliver(t, closePacket, server, true, startTime)
	if len(closed.Closed) != 1 || len(server.out) != 0 || server.ledger.Snapshot().EstablishedSessions != 1 {
		t.Fatal("committed sibling retained credits or replied to CLOSE")
	}
	result = deliver(t, first, client, false, startTime)
	if len(result.Established) != 0 || len(client.installed) != 1 {
		t.Fatal("late sibling READY completed another Session")
	}
	closeSide(t, client, startTime)
	closeSide(t, server, startTime)
}

func resign(packet []byte, key wirev2.Key) {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(packet[:len(packet)-wirev2.AuthenticationTagSize])
	copy(packet[len(packet)-wirev2.AuthenticationTagSize:], mac.Sum(nil))
}
