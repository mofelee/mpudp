package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mofelee/mpudp"
)

func TestPayloadIntegrity(t *testing.T) {
	want := payload(1400, 123, 1)
	b := append([]byte(nil), want...)
	stamp(b, kindData, 55, 123456789)
	if !validPayload(b, want) {
		t.Fatal("valid payload rejected")
	}
	for _, index := range []int{0, 4, 8, 16, 20, 24, 32, headerSize, len(b) - 1} {
		b[index] ^= 1
		if validPayload(b, want) {
			t.Fatalf("corruption at %d was accepted", index)
		}
		b[index] ^= 1
	}
	if validPayload(b[:len(b)-1], want) {
		t.Fatal("truncated payload accepted")
	}
}

func TestBoundedDuplicateWindow(t *testing.T) {
	var w sequenceWindow
	for _, seq := range []uint64{0, 2, 1, 10, dedupSize - 1} {
		if unique, old := w.accept(seq); !unique || old {
			t.Fatalf("first sequence %d rejected", seq)
		}
		if unique, old := w.accept(seq); unique || old {
			t.Fatalf("duplicate sequence %d counted", seq)
		}
	}
	if unique, old := w.accept(dedupSize); !unique || old {
		t.Fatal("window advance failed")
	}
	if unique, old := w.accept(0); unique || !old {
		t.Fatal("old replay counted")
	}
	if unique, old := w.accept(3); !unique || old {
		t.Fatal("valid reordered sequence rejected")
	}
}

func TestReceiverCountsOnlyVerifiedUniquePayload(t *testing.T) {
	c := &receiverCounters{start: time.Now(), buckets: make([]bucket, 1)}
	c.record(1400, true, true, false)
	c.record(1400, false, false, false)
	c.record(1400, true, false, false)
	c.record(1400, true, false, true)
	b := c.snapshot(0)
	if b.VerifiedBytes != 1360 || b.VerifiedPackets != 1 || b.CorruptFrames != 1 || b.DuplicateFrames != 1 || b.TooOldFrames != 1 {
		t.Fatalf("bad counters: %+v", b)
	}
	c.start = time.Now().Add(-2 * time.Second)
	c.record(1400, true, true, false)
	if c.snapshot(0) != b {
		t.Fatal("post-window packet was counted")
	}
}

func TestLatencyHistogramIncludesOverflowInPercentiles(t *testing.T) {
	var l latencyCounters
	l.markSent()
	l.markSent()
	l.observe(40*time.Millisecond + time.Microsecond)
	l.observe(11 * time.Second)
	s := l.snapshot()
	if s.P50MS == nil || *s.P50MS != 41 || s.P95MS != nil || s.Overflow != 1 {
		t.Fatalf("unexpected quantiles: %+v", s)
	}
}

type failedSession struct{ err error }

func (s failedSession) ReadPacket() ([]byte, error) { return nil, s.err }
func (s failedSession) WritePacket([]byte) error    { return s.err }
func (s failedSession) Close() error                { return nil }

func TestTransportLossDoesNotStopKCP(t *testing.T) {
	for _, err := range []error{mpudp.ErrNotReady, mpudp.ErrPartialSend, mpudp.ErrAllSendsFailed, mpudp.ErrNoAvailablePaths} {
		p := packetConn{s: failedSession{err}}
		if n, got := p.WriteTo([]byte("packet"), virtualRemote); n != 6 || got != nil || p.drops.Load() != 1 {
			t.Fatalf("%v: n=%d error=%v", err, n, got)
		}
	}
	p := packetConn{s: failedSession{mpudp.ErrClosed}}
	if _, err := p.WriteTo([]byte("packet"), virtualRemote); err == nil {
		t.Fatal("closed session hidden")
	}
}

func availableAddress(t *testing.T, network string) string {
	t.Helper()
	if network == "tcp" {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := l.Addr().String()
		_ = l.Close()
		return address
	}
	c, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := c.LocalAddr().String()
	_ = c.Close()
	return address
}

func TestProtocolMatrixRoundTrip(t *testing.T) {
	for _, protocol := range []string{"tcp", "udp", "kcp", "mpudp", "kcp-mpudp"} {
		for _, direction := range []string{"upload", "download"} {
			t.Run(protocol+"/"+direction, func(t *testing.T) {
				var captured bytes.Buffer
				output = &captured
				defer func() { output = os.Stdout }()
				o := options{Mode: "server", Protocol: protocol, Direction: direction, ID: "loopback", Seconds: 1, Payload: 1200, Flows: 2,
					KCPMTU: 1400, KCPWindow: 1024, RateMbps: 1, Control: availableAddress(t, "tcp"), Address: availableAddress(t, "udp")}
				client := o
				client.Mode = "client"
				if protocol == "mpudp" || protocol == "kcp-mpudp" {
					dir := t.TempDir()
					o.Config, client.Config = filepath.Join(dir, "server.yaml"), filepath.Join(dir, "client.yaml")
					common := "fec:\n  data_shards: 3\n  parity_shards: 2\npsk: private-loopback-marker\n"
					if err := os.WriteFile(o.Config, []byte(fmt.Sprintf("listen: %s\n%s", o.Address, common)), 0600); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(client.Config, []byte(fmt.Sprintf("carriers:\n  - %s\n%s", o.Address, common)), 0600); err != nil {
						t.Fatal(err)
					}
				}
				done := make(chan error, 1)
				go func() { done <- run(o) }()
				if err := run(client); err != nil {
					t.Fatal(err)
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if bytes.Contains(captured.Bytes(), []byte("private-loopback-marker")) {
					t.Fatal("PSK leaked in output")
				}
				decoder := json.NewDecoder(&captured)
				var receiver *summary
				for {
					var row json.RawMessage
					if err := decoder.Decode(&row); errors.Is(err, io.EOF) {
						break
					} else if err != nil {
						t.Fatal(err)
					}
					var record summary
					if err := json.Unmarshal(row, &record); err != nil {
						t.Fatal(err)
					}
					if record.Type == "summary" && record.Role == "receiver" {
						receiver = &record
					}
				}
				if receiver == nil || receiver.VerifiedBytes == 0 || receiver.CorruptFrames != 0 || receiver.Flows != 2 {
					t.Fatalf("invalid receiver result: %+v", receiver)
				}
				if len(receiver.Samples) != 1 || receiver.Samples[0].VerifiedBytes != receiver.VerifiedBytes {
					t.Fatal("summary does not match receiver window")
				}
				if receiver.EchoRTT.Scheduled != uint64(client.Seconds*5*client.Flows) {
					t.Fatalf("lost scheduled probe opportunities: %+v", receiver.EchoRTT)
				}
			})
		}
	}
}

func TestProfilesOptInAndPrivate(t *testing.T) {
	prefix := filepath.Join(t.TempDir(), "profiles")
	finish, err := startProfiles(prefix)
	if err != nil {
		t.Fatal(err)
	}
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"cpu", "allocs", "heap", "mutex", "block"} {
		info, err := os.Stat(prefix + "." + name + ".pprof")
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() == 0 || info.Mode().Perm() != 0600 {
			t.Fatalf("invalid profile %s: %+v", name, info)
		}
	}
}

type blockedConn struct {
	done chan struct{}
	once sync.Once
}

func (c *blockedConn) Read([]byte) (int, error)  { <-c.done; return 0, io.EOF }
func (c *blockedConn) Write([]byte) (int, error) { <-c.done; return 0, net.ErrClosed }
func (c *blockedConn) Close() error              { c.once.Do(func() { close(c.done) }); return nil }

func TestBlockedWriterKeepsEveryLatencyOpportunity(t *testing.T) {
	output = io.Discard
	defer func() { output = os.Stdout }()
	c := &blockedConn{done: make(chan struct{})}
	r, err := receive(&transports{conns: []messageConn{c}, paths: 1}, options{Mode: "client", Direction: "download", Protocol: "tcp", Flows: 1, Seconds: 1, Payload: 1200, ID: "blocked"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if r.EchoRTT.Scheduled != 5 || r.EchoRTT.DeadlineMissed != 5 || r.EchoRTT.Submitted != 0 || r.EchoRTT.P50MS != nil || r.EchoRTT.QueueMissed == 0 {
		t.Fatalf("blocked writes omitted latency opportunities: %+v", r.EchoRTT)
	}
}

func TestUnrepresentableRateRejected(t *testing.T) {
	o := options{Mode: "client", Protocol: "udp", Direction: "download", Flows: 1, Seconds: 1, Payload: 1400, KCPMTU: 1400, KCPWindow: 1024, ID: "invalid-rate", RateMbps: 1e-20}
	if err := o.validate(); err == nil {
		t.Fatal("unrepresentable pacing interval was accepted")
	}
}
