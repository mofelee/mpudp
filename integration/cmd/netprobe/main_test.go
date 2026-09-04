package main

import (
	"bytes"
	"encoding/json"
	"net"
	"testing"
	"time"
)

func TestPacketEventContainsDigestButNotPayload(t *testing.T) {
	payload := []byte("complete-user-payload")
	got := packetEvent(options{runID: "test-run", network: "udp4"}, "client", 1, testAddr("local"), testAddr("remote"), payload, time.Now())
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, payload) {
		t.Fatalf("event leaked payload: %s", encoded)
	}
	if len(got.Digest) != 12 || got.Bytes != len(payload) {
		t.Fatalf("event digest/length = %q/%d", got.Digest, got.Bytes)
	}
}

func TestRunRejectsInvalidInputsBeforeNetworkIO(t *testing.T) {
	tests := []options{
		{mode: "client", network: "udp4", runID: "../bad", targets: "127.0.0.1:1", timeout: time.Second, eventsPath: t.TempDir() + "/events"},
		{mode: "client", network: "tcp", runID: "valid", targets: "127.0.0.1:1", timeout: time.Second, eventsPath: t.TempDir() + "/events"},
		{mode: "other", network: "udp4", runID: "valid", timeout: time.Second, eventsPath: t.TempDir() + "/events"},
	}
	for _, test := range tests {
		if err := run(test); err == nil {
			t.Fatalf("run(%+v) error = nil", test)
		}
	}
}

func TestEqualBytes(t *testing.T) {
	if !equalBytes([]byte("same"), []byte("same")) || equalBytes([]byte("same"), []byte("diff")) || equalBytes([]byte("a"), []byte("aa")) {
		t.Fatal("equalBytes returned an invalid result")
	}
}

type testAddr string

func (a testAddr) Network() string { return "udp" }
func (a testAddr) String() string  { return string(a) }

var _ net.Addr = testAddr("")
