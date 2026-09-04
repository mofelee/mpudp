package transport_test

import (
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/transport"
)

type dialStep struct {
	conn *fakeConnectedConn
	err  error
}

type fakeDialer struct {
	mu      sync.Mutex
	steps   []dialStep
	remotes []string
}

func (d *fakeDialer) dial(_ context.Context, remote string) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.remotes = append(d.remotes, remote)
	if len(d.steps) == 0 {
		return nil, io.EOF
	}
	step := d.steps[0]
	d.steps = d.steps[1:]
	return step.conn, step.err
}

func TestCarrierReusesSocketAndRebuildsGeneration(t *testing.T) {
	t.Parallel()

	first := newFakeConnectedConn("127.0.0.1:30001", "127.0.0.1:4000")
	second := newFakeConnectedConn("127.0.0.1:30002", "127.0.0.1:4000")
	dialer := &fakeDialer{steps: []dialStep{{conn: first}, {conn: second}, {err: io.ErrUnexpectedEOF}}}
	carrier, err := transport.OpenCarrier(context.Background(), "carrier-a", "127.0.0.1:4000", transport.CarrierOptions{
		Dial: dialer.dial,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = carrier.Close() })

	if carrier.Generation() != 1 || carrier.LocalAddr().String() != "127.0.0.1:30001" {
		t.Fatalf("initial carrier generation/address = %d/%v", carrier.Generation(), carrier.LocalAddr())
	}
	if err := carrier.Send(context.Background(), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := carrier.Send(context.Background(), []byte("two")); err != nil {
		t.Fatal(err)
	}
	if got, want := first.written(), [][]byte{[]byte("one"), []byte("two")}; !reflect.DeepEqual(got, want) {
		t.Fatalf("first socket writes = %q, want %q", got, want)
	}

	if err := carrier.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	if carrier.Generation() != 2 || carrier.LocalAddr().String() != "127.0.0.1:30002" {
		t.Fatalf("rebuilt carrier generation/address = %d/%v", carrier.Generation(), carrier.LocalAddr())
	}
	select {
	case <-first.closed:
	default:
		t.Fatal("old connection was not closed before Rebuild returned")
	}
	if err := carrier.Send(context.Background(), []byte("three")); err != nil {
		t.Fatal(err)
	}
	if got, want := second.written(), [][]byte{[]byte("three")}; !reflect.DeepEqual(got, want) {
		t.Fatalf("second socket writes = %q, want %q", got, want)
	}

	if err := carrier.Rebuild(context.Background()); err == nil {
		t.Fatal("failed rebuild error = nil")
	}
	if carrier.Generation() != 2 || !carrier.Available() {
		t.Fatalf("failed rebuild changed live generation: generation=%d available=%t", carrier.Generation(), carrier.Available())
	}
}

func TestCarrierReadGenerationAndOwnedPayload(t *testing.T) {
	t.Parallel()

	first := newFakeConnectedConn("local-1", "remote")
	second := newFakeConnectedConn("local-2", "remote")
	dialer := &fakeDialer{steps: []dialStep{{conn: first}, {conn: second}}}
	packets := make(chan transport.ReceivedPacket, 2)
	carrier, err := transport.OpenCarrier(context.Background(), "carrier-a", "remote", transport.CarrierOptions{
		Dial:     dialer.dial,
		OnPacket: func(packet transport.ReceivedPacket) { packets <- packet },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = carrier.Close() })

	original := []byte("hello")
	first.reads <- streamRead{payload: original}
	packet := receivePacket(t, packets)
	original[0] = 'X'
	if string(packet.Payload) != "hello" || packet.Generation != 1 {
		t.Fatalf("received packet = %q generation %d", packet.Payload, packet.Generation)
	}
	if !packet.Reply.Available() {
		t.Fatal("current generation reply path is unavailable")
	}

	if err := carrier.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	if packet.Reply.Available() {
		t.Fatal("old generation reply path remained available after rebuild")
	}
	if err := packet.Reply.Send(context.Background(), []byte("late")); !errors.Is(err, transport.ErrGenerationReplaced) {
		t.Fatalf("old reply Send() error = %v", err)
	}
	second.reads <- streamRead{payload: []byte("new")}
	newPacket := receivePacket(t, packets)
	if newPacket.Generation != 2 || newPacket.LocalAddr.String() != "local-2" {
		t.Fatalf("new packet generation/local = %d/%v", newPacket.Generation, newPacket.LocalAddr)
	}
}

func TestCarrierClassifiesEMSGSIZEWithoutDisablingPath(t *testing.T) {
	t.Parallel()

	conn := newFakeConnectedConn("local", "remote")
	conn.setWriteError(syscall.EMSGSIZE)
	dialer := &fakeDialer{steps: []dialStep{{conn: conn}}}
	carrier, err := transport.OpenCarrier(context.Background(), "carrier-a", "remote", transport.CarrierOptions{Dial: dialer.dial})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = carrier.Close() })

	err = carrier.Send(context.Background(), []byte("packet"))
	if !errors.Is(err, transport.ErrPathMTUExceeded) || !errors.Is(err, syscall.EMSGSIZE) {
		t.Fatalf("Send() error = %v, want PMTU and EMSGSIZE classification", err)
	}
	if !carrier.Available() {
		t.Fatal("EMSGSIZE disabled the entire carrier")
	}
	conn.setWriteError(nil)
	if err := carrier.Send(context.Background(), []byte("fits")); err != nil {
		t.Fatalf("Send() after EMSGSIZE error = %v", err)
	}
}

func TestCarrierPayloadLimitAndErrorRedaction(t *testing.T) {
	t.Parallel()

	conn := newFakeConnectedConn("local", "remote")
	dialer := &fakeDialer{steps: []dialStep{{conn: conn}}}
	carrier, err := transport.OpenCarrier(context.Background(), "carrier-a", "remote", transport.CarrierOptions{
		Dial:       dialer.dial,
		MaxPayload: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = carrier.Close() })

	secretPacket := []byte("complete-secret-packet")
	err = carrier.Send(context.Background(), secretPacket)
	if !errors.Is(err, transport.ErrPayloadTooLarge) {
		t.Fatalf("Send() error = %v", err)
	}
	if contains(err.Error(), string(secretPacket)) {
		t.Fatalf("error leaked payload: %v", err)
	}
	if len(conn.written()) != 0 {
		t.Fatal("oversize payload reached the connection")
	}

	conn.setWriteError(errors.New(string(secretPacket)))
	err = carrier.Send(context.Background(), []byte("tiny"))
	if contains(err.Error(), string(secretPacket)) {
		t.Fatalf("PathError leaked injected underlying text: %v", err)
	}
}

func TestCarrierCloseIsIdempotentAndCancelsGeneration(t *testing.T) {
	t.Parallel()

	conn := newFakeConnectedConn("local", "remote")
	dialer := &fakeDialer{steps: []dialStep{{conn: conn}}}
	packets := make(chan transport.ReceivedPacket, 1)
	carrier, err := transport.OpenCarrier(context.Background(), "carrier-a", "remote", transport.CarrierOptions{
		Dial: dialer.dial,
		OnPacket: func(packet transport.ReceivedPacket) {
			packets <- packet
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	conn.reads <- streamRead{payload: []byte("packet")}
	packet := receivePacket(t, packets)
	if err := carrier.Close(); err != nil {
		t.Fatal(err)
	}
	if err := carrier.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	select {
	case <-packet.Context.Done():
	default:
		t.Fatal("generation context was not canceled")
	}
	if carrier.Available() {
		t.Fatal("closed carrier is available")
	}
	if err := carrier.Send(context.Background(), []byte("x")); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("Send() after Close error = %v", err)
	}
	if err := carrier.Rebuild(context.Background()); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("Rebuild() after Close error = %v", err)
	}
}

func TestCarrierConcurrentSendAndClose(t *testing.T) {
	conn := newFakeConnectedConn("local", "remote")
	dialer := &fakeDialer{steps: []dialStep{{conn: conn}}}
	carrier, err := transport.OpenCarrier(context.Background(), "carrier-a", "remote", transport.CarrierOptions{Dial: dialer.dial})
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 50; j++ {
				err := carrier.Send(context.Background(), []byte("packet"))
				if err != nil && !errors.Is(err, transport.ErrClosed) && !errors.Is(err, net.ErrClosed) {
					t.Errorf("concurrent Send() error = %v", err)
					return
				}
			}
		}()
	}
	close(start)
	if err := carrier.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestCarrierCloseCancelsConcurrentRebuild(t *testing.T) {
	t.Parallel()

	first := newFakeConnectedConn("local-1", "remote")
	dialStarted := make(chan struct{})
	var calls int
	var dialMu sync.Mutex
	dial := func(ctx context.Context, _ string) (net.Conn, error) {
		dialMu.Lock()
		calls++
		call := calls
		dialMu.Unlock()
		if call == 1 {
			return first, nil
		}
		close(dialStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	carrier, err := transport.OpenCarrier(context.Background(), "carrier-a", "remote", transport.CarrierOptions{Dial: dial})
	if err != nil {
		t.Fatal(err)
	}

	rebuildResult := make(chan error, 1)
	go func() { rebuildResult <- carrier.Rebuild(context.Background()) }()
	select {
	case <-dialStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for rebuild dial")
	}

	const closers = 8
	closeResults := make(chan error, closers)
	for range closers {
		go func() { closeResults <- carrier.Close() }()
	}
	select {
	case rebuildErr := <-rebuildResult:
		if rebuildErr == nil {
			t.Fatal("Rebuild succeeded after Close began")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent Rebuild was not canceled by Close")
	}
	for range closers {
		if closeErr := <-closeResults; closeErr != nil {
			t.Fatalf("concurrent Close error = %v", closeErr)
		}
	}
	if carrier.Available() || carrier.LocalAddr() != nil {
		t.Fatal("closed Carrier published or retained a rebuilt socket")
	}
}

func receivePacket(t *testing.T, packets <-chan transport.ReceivedPacket) transport.ReceivedPacket {
	t.Helper()
	select {
	case packet := <-packets:
		return packet
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for packet")
		return transport.ReceivedPacket{}
	}
}

func contains(value, substring string) bool {
	return strings.Contains(value, substring)
}
