package mpudp

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestNewSessionIDReadsExactlySixteenRandomBytes(t *testing.T) {
	t.Parallel()
	want := SessionID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	id, err := newSessionID(bytes.NewReader(want[:]))
	if err != nil {
		t.Fatalf("newSessionID() error = %v", err)
	}
	if id != want {
		t.Fatalf("newSessionID() = %v, want %v", id, want)
	}
	if got := id.String(); got != "000102030405060708090a0b0c0d0e0f" {
		t.Fatalf("SessionID.String() = %q", got)
	}
}

func TestNewSessionIDRetriesAllZeroValue(t *testing.T) {
	t.Parallel()
	want := SessionID{15: 1}
	input := append(make([]byte, len(SessionID{})), want[:]...)
	id, err := newSessionID(bytes.NewReader(input))
	if err != nil {
		t.Fatalf("newSessionID() error = %v", err)
	}
	if id != want {
		t.Fatalf("newSessionID() = %v, want %v", id, want)
	}
}

func TestNewSessionIDBoundsAllZeroRetries(t *testing.T) {
	t.Parallel()
	reader := &countingReader{reader: bytes.NewReader(make([]byte, maxSessionIDAttempts*len(SessionID{})))}
	id, err := newSessionID(reader)
	if err == nil {
		t.Fatal("newSessionID() error = nil, want bounded all-zero failure")
	}
	if id != (SessionID{}) {
		t.Fatalf("newSessionID() returned nonzero ID on failure: %v", id)
	}
	if want := maxSessionIDAttempts * len(SessionID{}); reader.bytesRead != want {
		t.Fatalf("random bytes read = %d, want %d", reader.bytesRead, want)
	}
}

func TestNewSessionIDPropagatesReaderFailure(t *testing.T) {
	t.Parallel()
	_, err := newSessionID(bytes.NewReader([]byte{1, 2, 3}))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("newSessionID() error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestPublicNewSessionIDUsesNonzeroUniqueValues(t *testing.T) {
	t.Parallel()
	seen := make(map[SessionID]struct{}, 128)
	for i := 0; i < 128; i++ {
		id, err := NewSessionID()
		if err != nil {
			t.Fatalf("NewSessionID() error = %v", err)
		}
		if id == (SessionID{}) {
			t.Fatal("NewSessionID() returned all-zero ID")
		}
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("NewSessionID() repeated ID %v", id)
		}
		seen[id] = struct{}{}
	}
}

type countingReader struct {
	reader    io.Reader
	bytesRead int
}

func (r *countingReader) Read(payload []byte) (int, error) {
	n, err := r.reader.Read(payload)
	r.bytesRead += n
	return n, err
}
