package transport_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/transport"
)

func TestListenerReplyPathUsesReceivingPacketConn(t *testing.T) {
	t.Parallel()

	conn := newFakePacketConn("listener:4000")
	packets := make(chan transport.ReceivedPacket, 1)
	listener, err := transport.ServePacketConn(conn, transport.ListenerOptions{
		PathID:   "listener-a",
		OnPacket: func(packet transport.ReceivedPacket) { packets <- packet },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	original := []byte("request")
	conn.reads <- packetRead{payload: original, remote: fakeAddr("client:5000")}
	packet := receivePacket(t, packets)
	original[0] = 'X'
	if string(packet.Payload) != "request" {
		t.Fatalf("received payload aliases read input: %q", packet.Payload)
	}
	if packet.LocalAddr.String() != "listener:4000" || packet.RemoteAddr.String() != "client:5000" {
		t.Fatalf("received route = %v <- %v", packet.LocalAddr, packet.RemoteAddr)
	}
	if packet.Reply.LocalAddr().String() != "listener:4000" || packet.Reply.RemoteAddr().String() != "client:5000" {
		t.Fatalf("reply route = %v -> %v", packet.Reply.LocalAddr(), packet.Reply.RemoteAddr())
	}
	if err := packet.Reply.Send(context.Background(), []byte("response")); err != nil {
		t.Fatal(err)
	}
	writes := conn.written()
	if len(writes) != 1 || string(writes[0].payload) != "response" || writes[0].remote.String() != "client:5000" {
		t.Fatalf("listener writes = %+v", writes)
	}
}

func TestListenerDropsOversizeReceiveAndKeepsServing(t *testing.T) {
	t.Parallel()

	conn := newFakePacketConn("listener:4000")
	packets := make(chan transport.ReceivedPacket, 1)
	errorsCh := make(chan error, 1)
	listener, err := transport.ServePacketConn(conn, transport.ListenerOptions{
		PathID:     "listener-a",
		MaxPayload: 4,
		OnPacket:   func(packet transport.ReceivedPacket) { packets <- packet },
		OnError:    func(err error) { errorsCh <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	conn.reads <- packetRead{payload: []byte("12345"), remote: fakeAddr("client:5000")}
	select {
	case got := <-errorsCh:
		if !errors.Is(got, transport.ErrPayloadTooLarge) {
			t.Fatalf("oversize read error = %v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for oversize read error")
	}
	select {
	case packet := <-packets:
		t.Fatalf("oversize packet delivered: %q", packet.Payload)
	default:
	}

	conn.reads <- packetRead{payload: []byte("1234"), remote: fakeAddr("client:5000")}
	if packet := receivePacket(t, packets); string(packet.Payload) != "1234" {
		t.Fatalf("valid packet after oversize = %q", packet.Payload)
	}
}

func TestListenerReplyFailureClassifiesMTUAndRedactsCause(t *testing.T) {
	t.Parallel()

	conn := newFakePacketConn("listener:4000")
	packets := make(chan transport.ReceivedPacket, 1)
	listener, err := transport.ServePacketConn(conn, transport.ListenerOptions{
		PathID:   "listener-a",
		OnPacket: func(packet transport.ReceivedPacket) { packets <- packet },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	conn.reads <- packetRead{payload: []byte("request"), remote: fakeAddr("client:5000")}
	packet := receivePacket(t, packets)

	secret := "complete-authenticated-packet"
	conn.mu.Lock()
	conn.writeErr = errors.Join(syscall.EMSGSIZE, errors.New(secret))
	conn.mu.Unlock()
	err = packet.Reply.Send(context.Background(), []byte("reply"))
	if !errors.Is(err, transport.ErrPathMTUExceeded) || !errors.Is(err, syscall.EMSGSIZE) {
		t.Fatalf("Reply.Send() error = %v", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("reply error leaked injected packet text: %v", err)
	}
}

func TestListenerCloseCancelsRoutes(t *testing.T) {
	t.Parallel()

	conn := newFakePacketConn("listener:4000")
	packets := make(chan transport.ReceivedPacket, 1)
	listener, err := transport.ServePacketConn(conn, transport.ListenerOptions{
		PathID:   "listener-a",
		OnPacket: func(packet transport.ReceivedPacket) { packets <- packet },
	})
	if err != nil {
		t.Fatal(err)
	}
	conn.reads <- packetRead{payload: []byte("request"), remote: fakeAddr("client:5000")}
	packet := receivePacket(t, packets)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if packet.Reply.Available() {
		t.Fatal("reply route remained available after listener close")
	}
	if err := packet.Reply.Send(context.Background(), []byte("late")); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("reply after Close error = %v", err)
	}
	select {
	case <-packet.Context.Done():
	default:
		t.Fatal("listener generation context was not canceled")
	}
}

func TestListenerConcurrentCloseSharesCompletionBarrier(t *testing.T) {
	t.Parallel()

	conn := newFakePacketConn("listener:4000")
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	listener, err := transport.ServePacketConn(conn, transport.ListenerOptions{
		PathID: "listener-a",
		OnPacket: func(transport.ReceivedPacket) {
			close(callbackStarted)
			<-releaseCallback
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	conn.reads <- packetRead{payload: []byte("request"), remote: fakeAddr("client:5000")}
	select {
	case <-callbackStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for receive callback")
	}

	const closers = 8
	results := make(chan error, closers)
	var started sync.WaitGroup
	started.Add(closers)
	for range closers {
		go func() {
			started.Done()
			results <- listener.Close()
		}()
	}
	started.Wait()
	select {
	case closeErr := <-results:
		t.Fatalf("Close returned before active callback completed: %v", closeErr)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseCallback)
	for range closers {
		if closeErr := <-results; closeErr != nil {
			t.Fatalf("concurrent Close error = %v", closeErr)
		}
	}
}

func TestServePacketConnPMTURequirementIsExplicit(t *testing.T) {
	t.Parallel()

	unsupported := newFakePacketConn("listener:4000")
	_, err := transport.ServePacketConn(unsupported, transport.ListenerOptions{PathID: "listener", RequirePMTU: true})
	if !errors.Is(err, transport.ErrPMTUUnsupported) {
		t.Fatalf("ServePacketConn() error = %v", err)
	}
	supported := newFakePacketConn("listener:4001")
	supported.pmtu = true
	listener, err := transport.ServePacketConn(supported, transport.ListenerOptions{PathID: "listener", RequirePMTU: true})
	if err != nil {
		t.Fatal(err)
	}
	if !listener.PMTUEnabled() {
		t.Fatal("listener did not retain advertised PMTU status")
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestListenerReplyWritesDoNotAliasCallerPayload(t *testing.T) {
	t.Parallel()

	conn := newFakePacketConn("listener:4000")
	packets := make(chan transport.ReceivedPacket, 1)
	listener, err := transport.ServePacketConn(conn, transport.ListenerOptions{
		PathID:   "listener-a",
		OnPacket: func(packet transport.ReceivedPacket) { packets <- packet },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	conn.reads <- packetRead{payload: []byte("request"), remote: fakeAddr("client:5000")}
	reply := receivePacket(t, packets).Reply
	payload := []byte("reply")
	if err := reply.Send(context.Background(), payload); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	if got := conn.written()[0].payload; !reflect.DeepEqual(got, []byte("reply")) {
		t.Fatalf("fake observed aliased payload: %q", got)
	}
}
