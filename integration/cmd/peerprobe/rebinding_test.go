package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mofelee/mpudp"
)

// The receiver gets enough data to decode while the sender still owns work.
type delayedFinalSession struct {
	delivered chan []byte
	release   chan struct{}
	writeErr  error
}

func (s *delayedFinalSession) WritePacket(body []byte) error {
	s.delivered <- append([]byte(nil), body...)
	<-s.release
	return s.writeErr
}
func (s *delayedFinalSession) ReadPacket() ([]byte, error) { return <-s.delivered, nil }
func (s *delayedFinalSession) Close() error                { return nil }

func TestPostExpiryShutdownRequiresSuccessfulSenderCompletion(t *testing.T) {
	for _, writeErr := range []error{nil, mpudp.ErrPartialSend} {
		name := "success"
		if writeErr != nil {
			name = "sender-failure"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			finalPath := filepath.Join(dir, "final")
			if err := createMarker(finalPath); err != nil {
				t.Fatal(err)
			}
			newLog := func(role string) *eventLog {
				t.Helper()
				log, err := openEventLog(options{runID: "barrier-unit", role: role, flow: rebindExpiryFlow, eventsPath: filepath.Join(dir, role+".ndjson")})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = log.close() })
				return log
			}
			senderLog, receiverLog := newLog("listener"), newLog("initiator")
			session := &delayedFinalSession{delivered: make(chan []byte), release: make(chan struct{}), writeErr: writeErr}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(session.release) }) }
			defer release()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			sent, received := make(chan error, 1), make(chan error, 1)
			go func() { sent <- sendPostExpiry(ctx, senderLog, session, finalPath) }()
			go func() { received <- receivePostExpiry(ctx, receiverLog, session, finalPath) }()
			waitForTestFile(t, finalPath+".post-expiry-done")
			if _, err := os.Stat(finalPath + ".post-expiry-sent"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("sender completed while writes held: %v", err)
			}
			select {
			case err := <-received:
				t.Fatalf("receiver honored early final marker before sender completion: %v", err)
			case <-time.After(2 * markerPollInterval):
			}
			release()
			if err := <-sent; !errors.Is(err, writeErr) {
				t.Fatalf("sender result=%v want=%v", err, writeErr)
			}
			if writeErr != nil {
				if _, err := os.Stat(finalPath + ".post-expiry-sent"); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("failed send published completion: %v", err)
				}
				cancel()
				if err := <-received; !errors.Is(err, context.Canceled) {
					t.Fatalf("receiver accepted failed sender: %v", err)
				}
			} else if err := <-received; err != nil {
				t.Fatalf("successful sender did not release receiver: %v", err)
			}
		})
	}
}
