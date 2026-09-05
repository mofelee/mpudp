package session

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/wire"
)

type observedIdentityPath struct {
	*fakePath
	localCalls      atomic.Int64
	remoteCalls     atomic.Int64
	generationCalls atomic.Int64
	availableCalls  atomic.Int64
	unavailableAt   atomic.Int64
}

func (p *observedIdentityPath) LocalAddr() net.Addr {
	p.localCalls.Add(1)
	return p.fakePath.LocalAddr()
}

func (p *observedIdentityPath) RemoteAddr() net.Addr {
	p.remoteCalls.Add(1)
	return p.fakePath.RemoteAddr()
}

func (p *observedIdentityPath) Generation() uint64 {
	p.generationCalls.Add(1)
	return p.fakePath.Generation()
}

func (p *observedIdentityPath) Available() bool {
	call := p.availableCalls.Add(1)
	if threshold := p.unavailableAt.Load(); threshold != 0 && call >= threshold {
		return false
	}
	return p.fakePath.Available()
}

func (p *observedIdentityPath) resetObservations() {
	p.localCalls.Store(0)
	p.remoteCalls.Store(0)
	p.generationCalls.Store(0)
	p.availableCalls.Store(0)
}

func TestListenerReusesValidatedReplyIdentity(t *testing.T) {
	for _, test := range []struct {
		name string
		kind wire.PacketType
	}{
		{"hello", wire.TypeHello},
		{"data", wire.TypeDataShard},
		{"ping", wire.TypePing},
		{"close", wire.TypeClose},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener, err := NewListener(ListenerConfig{Session: testConfig(newFakeClock(), 1200), MaxSessions: 1})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close(context.Background()) })
			path := &observedIdentityPath{fakePath: newFakePath("listener/client", "198.51.100.1:4000")}
			hello := helloPacket(t, testSessionID, 3, 2, 1200, []byte(testPSK))
			current, _, err := listener.HandlePacket(context.Background(), ReceivedPacket{Payload: hello, Reply: path})
			if err != nil {
				t.Fatal(err)
			}
			var message wire.Message
			switch test.kind {
			case wire.TypeHello:
				message, err = wire.NewHello(testSessionID, 3, 2, 1200)
			case wire.TypeDataShard:
				message, err = wire.NewDataShard(testSessionID, 1, 3, 2, 0, 3, []byte{1})
			case wire.TypePing:
				message, err = wire.NewPing(testSessionID, 1, 1)
			case wire.TypeClose:
				message, err = wire.NewClose(testSessionID)
			}
			if err != nil {
				t.Fatal(err)
			}
			path.resetObservations()
			got, _, err := listener.HandlePacket(context.Background(), ReceivedPacket{
				Payload: encodeMessage(t, message, []byte(testPSK), 1200), Reply: path,
			})
			if err != nil || got != current {
				t.Fatalf("existing Session packet failed: %v", err)
			}
			if local, remote, generation := path.localCalls.Load(), path.remoteCalls.Load(), path.generationCalls.Load(); local != 1 || remote != 1 || generation != 1 {
				t.Fatalf("identity snapshots local/remote/generation = %d/%d/%d, want 1/1/1", local, remote, generation)
			}
			if path.availableCalls.Load() < 2 {
				t.Fatal("Session did not recheck path availability after registry lookup")
			}
		})
	}
}

func TestListenerRechecksReplyAvailabilityAfterLookup(t *testing.T) {
	clock := newFakeClock()
	listener, err := NewListener(ListenerConfig{Session: testConfig(clock, 1200), MaxSessions: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close(context.Background()) })
	path := &observedIdentityPath{fakePath: newFakePath("listener/client", "198.51.100.1:4000")}
	hello := helloPacket(t, testSessionID, 3, 2, 1200, []byte(testPSK))
	current, _, err := listener.HandlePacket(context.Background(), ReceivedPacket{Payload: hello, Reply: path})
	if err != nil {
		t.Fatal(err)
	}
	beforeActivity := current.Endpoints()[0].LastActivity
	beforeReplies := path.PacketCount()
	clock.Advance(time.Millisecond)
	path.resetObservations()
	path.unavailableAt.Store(2)
	got, result, err := listener.HandlePacket(context.Background(), ReceivedPacket{Payload: hello, Reply: path})
	if got != current || !errors.Is(err, ErrInvalidReplyPath) || result.Message.Header.SessionID != testSessionID {
		t.Fatalf("retired path result Session=%t error=%v", got == current, err)
	}
	if path.availableCalls.Load() != 2 || path.localCalls.Load() != 1 || path.remoteCalls.Load() != 1 {
		t.Fatal("availability recheck recomputed or skipped the validated identity")
	}
	if path.PacketCount() != beforeReplies || !current.Endpoints()[0].LastActivity.Equal(beforeActivity) {
		t.Fatal("retired path caused a response or refreshed Endpoint activity")
	}
}

func TestDirectSessionIdentityValidationErrorOrder(t *testing.T) {
	path := newFakePath("carrier-a", "198.51.100.1:9000")
	current := establishInitiator(t, newFakeClock(), 1200, path)
	t.Cleanup(func() { _ = current.Close(context.Background()) })
	for _, test := range []struct {
		name string
		id   wire.SessionID
		key  []byte
		want error
	}{
		{"authentication before Session ID", otherSessionID, []byte("wrong key"), wire.ErrAuthentication},
		{"Session ID before path", otherSessionID, []byte(testPSK), ErrUnknownSession},
		{"path before state", testSessionID, []byte(testPSK), ErrInvalidReplyPath},
	} {
		t.Run(test.name, func(t *testing.T) {
			message, err := wire.NewPing(test.id, 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := current.HandlePacket(context.Background(), ReceivedPacket{
				Payload: encodeMessage(t, message, test.key, 1200),
			}); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestClosedListenerPreservesAuthenticationAndPathErrorOrder(t *testing.T) {
	listener, err := NewListener(ListenerConfig{Session: testConfig(newFakeClock(), 1200), MaxSessions: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	path := &observedIdentityPath{fakePath: newFakePath("listener/client", "198.51.100.1:4000")}
	for _, test := range []struct {
		name string
		key  []byte
		path ReplyPath
		want error
	}{
		{"bad tag before path", []byte("wrong key"), path, wire.ErrAuthentication},
		{"bad path before closed", []byte(testPSK), nil, ErrInvalidReplyPath},
		{"valid packet reaches closed state", []byte(testPSK), path, ErrClosed},
	} {
		t.Run(test.name, func(t *testing.T) {
			path.resetObservations()
			_, _, err := listener.HandlePacket(context.Background(), ReceivedPacket{
				Payload: helloPacket(t, testSessionID, 3, 2, 1200, test.key), Reply: test.path,
			})
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if errors.Is(test.want, wire.ErrAuthentication) && (path.localCalls.Load() != 0 || path.remoteCalls.Load() != 0) {
				t.Fatal("unauthenticated packet inspected reply identity")
			}
		})
	}
}
