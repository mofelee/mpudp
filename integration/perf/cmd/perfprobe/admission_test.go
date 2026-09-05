package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mofelee/mpudp"
)

type pressuredSession struct {
	mu         sync.Mutex
	rejects    int
	attempts   [][]byte
	pending    [][]byte
	flushed    [][]byte
	flushError error
	entered    chan struct{}
	once       sync.Once
}

func (s *pressuredSession) ReadPacket() ([]byte, error) { return nil, io.EOF }
func (s *pressuredSession) Close() error                { return nil }
func (s *pressuredSession) WritePacket(packet []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts = append(s.attempts, bytes.Clone(packet))
	if s.entered != nil {
		s.once.Do(func() { close(s.entered) })
	}
	if s.rejects != 0 {
		if s.rejects > 0 {
			s.rejects--
		}
		return mpudp.ErrResourceLimit
	}
	s.pending = append(s.pending, bytes.Clone(packet))
	return nil
}
func (s *pressuredSession) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.flushError != nil {
		return s.flushError
	}
	s.flushed = append(s.flushed, s.pending...)
	s.pending = nil
	return nil
}

func TestAdmissionRetriesSameWholeDatagramWithoutKCPDrop(t *testing.T) {
	for _, kcpAdapter := range []bool{false, true} {
		session := &pressuredSession{rejects: 3}
		writer := newAdmissionWriter(session)
		defer writer.cancel()
		packet := payload(1400, 71, 0)
		stamp(packet, kindData, 1024, 0)
		var n int
		var err error
		if kcpAdapter {
			adapter := &packetConn{s: session, writer: writer}
			n, err = adapter.WriteTo(packet, virtualRemote)
			if adapter.drops.Load() != 0 {
				t.Fatal("whole-admission pressure was counted as a network drop")
			}
		} else {
			n, err = (datagramConn{session: session, writer: writer}).Write(packet)
		}
		if n != len(packet) || err != nil {
			t.Fatalf("admission = %d, %v", n, err)
		}
		if len(session.attempts) != 4 || len(session.pending) != 1 {
			t.Fatalf("attempts %d, accepted originals %d", len(session.attempts), len(session.pending))
		}
		for _, attempt := range session.attempts {
			if !bytes.Equal(attempt, packet) {
				t.Fatal("retry changed the payload or original sequence")
			}
		}
		stats := writer.snapshot()
		if stats.BackpressuredPackets != 1 || stats.RejectedAttempts != 3 || stats.RetryAttempts != 3 || stats.WaitNS == 0 || stats.TimeoutPackets != 0 || stats.CanceledPackets != 0 {
			t.Fatalf("admission counters: %+v", stats)
		}
	}
}

func TestAdmissionWaitHonorsCallerAndLifecycleCancellation(t *testing.T) {
	for _, lifetime := range []bool{false, true} {
		session := &pressuredSession{rejects: -1, entered: make(chan struct{})}
		writer := newAdmissionWriter(session)
		defer writer.cancel()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- writer.writePacket(ctx, []byte("same original")) }()
		select {
		case <-session.entered:
		case <-time.After(time.Second):
			t.Fatal("admission never reached the Session")
		}
		if lifetime {
			writer.cancel()
		} else {
			cancel()
		}
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) || !errors.Is(err, mpudp.ErrResourceLimit) {
				t.Fatalf("canceled admission = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("admission retry ignored cancellation")
		}
		if stats := writer.snapshot(); stats.CanceledPackets != 1 || stats.TimeoutPackets != 0 {
			t.Fatalf("cancellation counters: %+v", stats)
		}
	}
}

func TestAdmissionWaitDeadlineIsAnExplicitFailure(t *testing.T) {
	session := &pressuredSession{rejects: -1}
	writer := newAdmissionWriter(session)
	defer writer.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := writer.writePacket(ctx, []byte("same original"))
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, mpudp.ErrResourceLimit) {
		t.Fatalf("admission deadline = %v", err)
	}
	if stats := writer.snapshot(); stats.TimeoutPackets != 1 || stats.CanceledPackets != 0 || stats.RejectedAttempts == 0 {
		t.Fatalf("deadline counters: %+v", stats)
	}
}

func TestLocalDrainCompletesAcceptedTailAfterAdmissionsStop(t *testing.T) {
	session := &pressuredSession{}
	writer := newAdmissionWriter(session)
	tpt := &transports{writers: []*admissionWriter{writer}}
	tail := []byte("accepted before the receiver cutoff")
	if err := writer.writePacket(context.Background(), tail); err != nil {
		t.Fatal(err)
	}
	tpt.stopAdmissions()
	if err := writer.writePacket(context.Background(), []byte("late")); !errors.Is(err, context.Canceled) {
		t.Fatalf("post-boundary admission = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stats, err := tpt.drain(ctx)
	if err != nil || stats.SupportedSessions != 1 || stats.CompletedSessions != 1 || stats.FailedSessions != 0 {
		t.Fatalf("local drain = %+v, %v", stats, err)
	}
	if len(session.pending) != 0 || len(session.flushed) != 1 || !bytes.Equal(session.flushed[0], tail) {
		t.Fatal("drain lost the accepted tail or admitted a post-boundary original")
	}
	session.flushError = errors.New("local socket attempt failed")
	stats, err = tpt.drain(ctx)
	if !errors.Is(err, session.flushError) || stats.FailedSessions != 1 || stats.CompletedSessions != 0 {
		t.Fatalf("local drain concealed failure: %+v, %v", stats, err)
	}
}

func TestV2RealQueuePressureRetriesAndDrainsEveryOriginal(t *testing.T) {
	dir := t.TempDir()
	address := availableAddress(t, "udp")
	common := "protocol: datagram\nwire: {version: v2}\nfec: {data_shards: 3, parity_shards: 2}\npsk: queue-pressure-private-marker\naggregation: {enabled: true, max_delay: 10ms, max_queued_datagrams: 1}\n"
	serverOptions := options{Mode: "server", Protocol: "mpudp", Payload: 64, Config: filepath.Join(dir, "server.yaml")}
	clientOptions := serverOptions
	clientOptions.Mode, clientOptions.Config = "client", filepath.Join(dir, "client.yaml")
	for path, config := range map[string]string{
		serverOptions.Config: fmt.Sprintf("listen: %s\n%s", address, common),
		clientOptions.Config: fmt.Sprintf("carriers: [%s]\n%s", address, common),
	} {
		if err := os.WriteFile(path, []byte(config), 0600); err != nil {
			t.Fatal(err)
		}
	}
	server, err := openTransports(serverOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	accept, err := server.listen(serverOptions)
	if err != nil {
		t.Fatal(err)
	}
	client, err := openTransports(clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	sender, err := client.dial(clientOptions, 0)
	if err != nil {
		t.Fatal(err)
	}
	client.conns = append(client.conns, sender)
	receiver, err := accept(0)
	if err != nil {
		t.Fatal(err)
	}
	server.conns = append(server.conns, receiver)
	const originals = 16
	received := make(chan [][]byte, 1)
	readError := make(chan error, 1)
	go func() {
		packets := make([][]byte, 0, originals)
		for len(packets) < originals {
			buffer := make([]byte, 64)
			n, err := receiver.Read(buffer)
			if err != nil {
				readError <- err
				return
			}
			packets = append(packets, buffer[:n])
		}
		received <- packets
	}()
	want := make(map[string]bool, originals)
	readyDeadline := time.Now().Add(3 * time.Second)
	for seq := uint64(0); seq < originals; seq++ {
		packet := payload(64, 59, 0)
		stamp(packet, kindData, seq, 0)
		for {
			_, err := sender.Write(packet)
			if err == nil {
				break
			}
			if !errors.Is(err, mpudp.ErrNotReady) || time.Now().After(readyDeadline) {
				t.Fatal(err)
			}
			time.Sleep(time.Millisecond)
		}
		want[string(packet)] = true
	}
	client.stopAdmissions()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if result, err := client.drain(ctx); err != nil || result.CompletedSessions != 1 {
		t.Fatalf("real local drain = %+v, %v", result, err)
	}
	select {
	case packets := <-received:
		for _, packet := range packets {
			if !want[string(packet)] {
				t.Fatal("pressure retry changed or duplicated an original")
			}
			delete(want, string(packet))
		}
	case err := <-readError:
		t.Fatal(err)
	case <-time.After(3 * time.Second):
		t.Fatal("local drain lost the real UDP tail")
	}
	if len(want) != 0 {
		t.Fatal("missing original after local drain")
	}
	stats := client.telemetry().MPUDPAdmission
	if stats.BackpressuredPackets == 0 || stats.RejectedAttempts == 0 || stats.RetryAttempts == 0 || stats.TimeoutPackets != 0 {
		t.Fatalf("real bounded queue did not exercise successful pressure retry: %+v", stats)
	}
}

type boundaryEchoConn struct {
	packets   chan []byte
	readCalls chan struct{}
	done      chan struct{}
	once      sync.Once
}

func (c *boundaryEchoConn) Read(buffer []byte) (int, error) {
	c.readCalls <- struct{}{}
	select {
	case packet := <-c.packets:
		return copy(buffer, packet), nil
	case <-c.done:
		return 0, io.EOF
	}
}
func (c *boundaryEchoConn) Write(packet []byte) (int, error) { return len(packet), nil }
func (c *boundaryEchoConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}

func TestReceiverDrainCannotExtendEchoMeasurementWindow(t *testing.T) {
	output = io.Discard
	defer func() { output = os.Stdout }()
	c := &boundaryEchoConn{packets: make(chan []byte, 1), readCalls: make(chan struct{}, 1), done: make(chan struct{})}
	o := options{Mode: "client", Direction: "download", Protocol: "mpudp", Flows: 1, Seconds: 1, Payload: 64, ID: "late-echo", Nonce: 41}
	r, err := receive(&transports{conns: []messageConn{c}, paths: 1}, o, time.Now(), func() error {
		<-c.readCalls
		echo := payload(o.Payload, o.Nonce, 0)
		stamp(echo, kindEcho, 0, 0)
		c.packets <- echo
		select {
		case <-c.readCalls:
			return nil
		case <-time.After(time.Second):
			return errors.New("receiver did not process the drain-time echo")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.EchoRTT.Scheduled != 5 || r.EchoRTT.Received != 0 || r.EchoRTT.Unanswered != 5 || r.EchoRTT.DeadlineMissed != 5 || r.EchoRTT.P50MS != nil {
		t.Fatalf("local drain extended the RTT population: %+v", r.EchoRTT)
	}
}
