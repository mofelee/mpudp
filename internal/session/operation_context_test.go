package session

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/wire"
)

type operationContextPath struct {
	send func(context.Context, []byte) error
}

func (*operationContextPath) PathID() string  { return "carrier" }
func (*operationContextPath) Available() bool { return true }
func (p *operationContextPath) Send(ctx context.Context, packet []byte) error {
	return p.send(ctx, packet)
}

func TestBackgroundWritesCancelTogetherOnSessionClose(t *testing.T) {
	entered := make(chan context.Context, 2)
	path := &operationContextPath{send: func(ctx context.Context, packet []byte) error {
		message, err := wire.DecodeAuthenticated(packet, []byte(testPSK), 1200)
		if err != nil {
			return err
		}
		if message.Header.Type != wire.TypeDataShard {
			return ctx.Err()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		entered <- ctx
		<-ctx.Done()
		return ctx.Err()
	}}
	s, err := NewInitiator(testSessionID, testConfig(newFakeClock(), 1200), []Path{path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	if _, err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	reply := newFakePath(path.PathID(), "198.51.100.1:9000")
	if _, err := s.HandlePacket(context.Background(), received(reply, ackPacket(t, testSessionID, 3, 2, 1200))); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := s.WritePacket(context.Background(), []byte("cancel on close"))
			results <- err
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent write did not enter the path")
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close(context.Background()) }()
	for range 2 {
		select {
		case err := <-results:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("WritePacket error = %v, want cancellation", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Close did not cancel an in-flight write")
		}
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("best-effort CLOSE inherited the canceled DATA context: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not join canceled writes")
	}
}

func TestOperationContextsKeepCallerSemanticsAndSessionIsolation(t *testing.T) {
	lifetime, closeSession := context.WithCancel(context.Background())
	defer closeSession()
	s := &Session{lifetime: lifetime}
	first, finishFirst := s.operationContextWithCancel(context.Background())
	finishFirst()
	second, finishSecond := s.operationContextWithCancel(context.Background())
	defer finishSecond()
	if first.Err() != nil || second.Err() != nil {
		t.Fatal("finishing one background operation canceled the Session")
	}
	type contextKey struct{}
	valueParent := context.WithValue(context.Background(), contextKey{}, "value-only")
	valueOperation, finishValue := s.operationContextWithCancel(valueParent)
	if valueOperation.Value(contextKey{}) != "value-only" {
		t.Fatal("noncancelable caller context lost its value")
	}
	finishValue()
	if !errors.Is(valueOperation.Err(), context.Canceled) || valueParent.Err() != nil || lifetime.Err() != nil {
		t.Fatal("derived operation cleanup changed its parent or Session lifetime")
	}
	deadline := time.Now().Add(time.Minute)
	parent, cancelParent := context.WithDeadline(context.WithValue(context.Background(), contextKey{}, "caller"), deadline)
	defer cancelParent()
	operation, finish := s.operationContextWithCancel(parent)
	defer finish()
	if got, ok := operation.Deadline(); !ok || !got.Equal(deadline) || operation.Value(contextKey{}) != "caller" {
		t.Fatal("operation lost the caller's deadline or value")
	}
	cancelParent()
	select {
	case <-operation.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("caller cancellation did not reach the operation")
	}
	if first.Err() != nil || second.Err() != nil {
		t.Fatal("caller cancellation affected unrelated operations")
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				_, done := s.operationContextWithCancel(context.Background())
				done()
			}
		}()
	}
	closeSession()
	wg.Wait()
	if !errors.Is(first.Err(), context.Canceled) || !errors.Is(second.Err(), context.Canceled) {
		t.Fatal("Session close did not cancel background operations")
	}
	late, finishLate := s.operationContextWithCancel(context.Background())
	defer finishLate()
	if !errors.Is(late.Err(), context.Canceled) {
		t.Fatal("operation admitted during close lost lifetime cancellation")
	}
}
