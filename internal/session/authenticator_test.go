package session

import (
	"context"
	"testing"

	"github.com/mofelee/mpudp/internal/wire"
)

func TestInitiatorAuthenticatorOwnsConfigurationKey(t *testing.T) {
	clock := newFakeClock()
	path := newFakePath("carrier-a", "198.51.100.1:9000")
	config := testConfig(clock, 1200)
	session, err := NewInitiator(testSessionID, config, []Path{path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	clear(config.PSK)
	if _, err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := session.HandlePacket(context.Background(), received(path, ackPacket(t, testSessionID, 3, 2, 1200))); err != nil {
		t.Fatalf("configuration key mutation changed ACK authentication: %v", err)
	}
	if _, err := session.WritePacket(context.Background(), []byte("immutable authentication key")); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	packets := path.Packets()
	if len(packets) != 7 {
		t.Fatalf("sent %d packets, want HELLO, five shards, and CLOSE", len(packets))
	}
	for index, packet := range packets {
		message := decodeSent(t, packet, 1200)
		want := wire.TypeDataShard
		if index == 0 {
			want = wire.TypeHello
		} else if index == len(packets)-1 {
			want = wire.TypeClose
		}
		if message.Header.Type != want {
			t.Fatalf("packet %d type = %v, want %v", index, message.Header.Type, want)
		}
	}
}

func TestListenerAuthenticatorSurvivesSharedSessionClose(t *testing.T) {
	clock := newFakeClock()
	config := testConfig(clock, 1200)
	listener, err := NewListener(ListenerConfig{Session: config, MaxSessions: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close(context.Background()) })
	clear(config.PSK)
	firstPath := newFakePath("listener/198.51.100.1:4000", "198.51.100.1:4000")
	secondPath := newFakePath("listener/198.51.100.2:5000", "198.51.100.2:5000")
	first, _, err := listener.HandlePacket(context.Background(), received(firstPath,
		helloPacket(t, testSessionID, 3, 2, 1200, []byte(testPSK))))
	if err != nil {
		t.Fatalf("configuration key mutation changed listener authentication: %v", err)
	}
	second, _, err := listener.HandlePacket(context.Background(), received(secondPath,
		helloPacket(t, otherSessionID, 3, 2, 1200, []byte(testPSK))))
	if err != nil {
		t.Fatal(err)
	}
	if first.settings.authenticator == nil || first.settings.authenticator != listener.settings.authenticator ||
		second.settings.authenticator != listener.settings.authenticator {
		t.Fatal("listener Sessions did not share their immutable authenticator")
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if listener.Stats().Sessions != 1 {
		t.Fatal("closing one Session removed the wrong listener state")
	}
	ping, err := wire.NewPing(otherSessionID, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := listener.HandlePacket(context.Background(), received(secondPath,
		encodeMessage(t, ping, []byte(testPSK), 1200))); err != nil {
		t.Fatalf("shared authentication failed after another Session closed: %v", err)
	}
	if _, err := second.WritePacket(context.Background(), []byte("remaining listener Session")); err != nil {
		t.Fatal(err)
	}
	packets := secondPath.Packets()
	if len(packets) != 7 {
		t.Fatalf("sent %d packets, want HELLO_ACK, PONG, and five shards", len(packets))
	}
	for index, packet := range packets {
		message := decodeSent(t, packet, 1200)
		want := wire.TypeDataShard
		if index == 0 {
			want = wire.TypeHelloAck
		} else if index == 1 {
			want = wire.TypePong
		}
		if message.Header.SessionID != otherSessionID || message.Header.Type != want {
			t.Fatalf("packet %d changed Session or type after shared cache use", index)
		}
	}
}
