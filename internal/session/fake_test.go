package session

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/fec"
	"github.com/mofelee/mpudp/internal/wire"
)

const testPSK = "session-state-machine-test-key"

var (
	testSessionID  = wire.SessionID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	otherSessionID = wire.SessionID{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(delta time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delta)
	c.mu.Unlock()
}

func (c *fakeClock) Set(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

type fakeAddr string

func (a fakeAddr) Network() string { return "udp" }
func (a fakeAddr) String() string  { return string(a) }

type fakePath struct {
	mu         sync.Mutex
	id         string
	generation uint64
	local      net.Addr
	remote     net.Addr
	available  bool
	sendErr    error
	sent       [][]byte
	onSend     func([]byte)
}

func newFakePath(id string, remote string) *fakePath {
	return &fakePath{
		id: id, generation: 1, local: fakeAddr("127.0.0.1:9000"),
		remote: fakeAddr(remote), available: true,
	}
}

func (p *fakePath) PathID() string { return p.id }
func (p *fakePath) Generation() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.generation
}
func (p *fakePath) LocalAddr() net.Addr  { return p.local }
func (p *fakePath) RemoteAddr() net.Addr { return p.remote }
func (p *fakePath) Available() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.available
}
func (p *fakePath) Send(ctx context.Context, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	owned := append([]byte(nil), payload...)
	p.mu.Lock()
	p.sent = append(p.sent, owned)
	err := p.sendErr
	hook := p.onSend
	p.mu.Unlock()
	if hook != nil {
		hook(owned)
	}
	return err
}
func (p *fakePath) Packets() [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([][]byte, len(p.sent))
	for index := range p.sent {
		result[index] = append([]byte(nil), p.sent[index]...)
	}
	return result
}
func (p *fakePath) PacketCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.sent)
}
func (p *fakePath) SetError(err error) {
	p.mu.Lock()
	p.sendErr = err
	p.mu.Unlock()
}
func (p *fakePath) SetAvailable(available bool) {
	p.mu.Lock()
	p.available = available
	p.mu.Unlock()
}

func testConfig(clock Clock, maxUDP int) Config {
	return Config{
		PSK: []byte(testPSK), FEC: fec.Params{DataShards: 3, ParityShards: 2},
		LocalMaxUDPPayload: maxUDP, MaxDatagramSize: 64 * 1024,
		MaxEndpoints: 2, MaxHandshakeAttempts: 3,
		MaxPendingFECBlocks: 8, MaxCompletedFECBlocks: 8,
		DecodeTimeout: 100 * time.Millisecond, CompletionTTL: time.Second,
		EndpointTTL: 5 * time.Second, KeepaliveInterval: time.Second,
		HandshakeRetryInterval:    100 * time.Millisecond,
		HandshakeRetryJitterLimit: 25 * time.Millisecond, Clock: clock,
	}
}

func encodeMessage(t *testing.T, message wire.Message, key []byte, budget int) []byte {
	t.Helper()
	packet, err := wire.AppendAuthenticated(nil, message, key, budget)
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}
	return packet
}

func helloPacket(t *testing.T, id wire.SessionID, data, parity uint8, capability uint16, key []byte) []byte {
	t.Helper()
	message, err := wire.NewHello(id, data, parity, capability)
	if err != nil {
		t.Fatalf("construct HELLO: %v", err)
	}
	return encodeMessage(t, message, key, wire.MaxUDPPayload)
}

func ackPacket(t *testing.T, id wire.SessionID, data, parity uint8, capability uint16) []byte {
	t.Helper()
	message, err := wire.NewHelloAck(id, data, parity, capability)
	if err != nil {
		t.Fatalf("construct HELLO_ACK: %v", err)
	}
	return encodeMessage(t, message, []byte(testPSK), wire.MaxUDPPayload)
}

func received(path *fakePath, payload []byte) ReceivedPacket {
	return ReceivedPacket{Payload: payload, Reply: path}
}

func decodeSent(t *testing.T, packet []byte, budget int) wire.Message {
	t.Helper()
	message, err := wire.DecodeAuthenticated(packet, []byte(testPSK), budget)
	if err != nil {
		t.Fatalf("decode captured packet: %v", err)
	}
	return message
}

func establishInitiator(t *testing.T, clock *fakeClock, capability int, paths ...*fakePath) *Session {
	t.Helper()
	configured := make([]Path, len(paths))
	for index := range paths {
		configured[index] = paths[index]
	}
	session, err := NewInitiator(testSessionID, testConfig(clock, capability), configured)
	if err != nil {
		t.Fatalf("NewInitiator: %v", err)
	}
	if _, err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := session.HandlePacket(context.Background(), received(paths[0], ackPacket(t, testSessionID, 3, 2, uint16(capability)))); err != nil {
		t.Fatalf("establish ACK: %v", err)
	}
	return session
}

func requireErrorIs(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want errors.Is(_, %v)", err, target)
	}
}
