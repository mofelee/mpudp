package transport_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/transport"
)

type gatedStatisticsConn struct {
	*fakePacketConn
	writesEntered  chan struct{}
	releaseWrite   chan struct{}
	cleanupEntered chan struct{}
	releaseCleanup chan struct{}
	writes         atomic.Uint64
	cleanups       atomic.Uint64
}

func (c *gatedStatisticsConn) WriteTo(payload []byte, remote net.Addr) (int, error) {
	if c.writes.Add(1) == 1 {
		close(c.writesEntered)
		<-c.releaseWrite
	}
	return c.fakePacketConn.WriteTo(payload, remote)
}

func (c *gatedStatisticsConn) SetWriteDeadline(deadline time.Time) error {
	if deadline.IsZero() && c.cleanups.Add(1) == 1 {
		close(c.cleanupEntered)
		<-c.releaseCleanup
	}
	return nil
}

func TestListenerStatisticsMeasureSharedSocketQueueAndIOSeparately(t *testing.T) {
	conn := &gatedStatisticsConn{
		fakePacketConn: newFakePacketConn("listener:4000"),
		writesEntered:  make(chan struct{}), releaseWrite: make(chan struct{}),
		cleanupEntered: make(chan struct{}), releaseCleanup: make(chan struct{}),
	}
	var enabled atomic.Bool
	enabled.Store(true)
	aggregate := &transport.Counters{DiagnosticsEnabled: &enabled}
	first := &transport.Counters{DiagnosticsEnabled: &enabled}
	second := &transport.Counters{DiagnosticsEnabled: &enabled}
	packets := make(chan transport.ReceivedPacket, 2)
	l, err := transport.ServePacketConn(conn, transport.ListenerOptions{
		PathID: "listener", Statistics: aggregate,
		OnPacket: func(packet transport.ReceivedPacket) { packets <- packet },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	defer func() {
		for _, gate := range []chan struct{}{conn.releaseWrite, conn.releaseCleanup} {
			select {
			case <-gate:
			default:
				close(gate)
			}
		}
	}()
	conn.reads <- packetRead{payload: []byte("one"), remote: fakeAddr("client:5000")}
	conn.reads <- packetRead{payload: []byte("two"), remote: fakeAddr("client:6000")}
	firstReply := transport.WithReplyStatistics(receivePacket(t, packets).Reply, first)
	secondReply := transport.WithReplyStatistics(receivePacket(t, packets).Reply, second)
	if aggregate.ReceivedPackets.Load() != 2 || first.ReceivedPackets.Load() != 0 || second.ReceivedPackets.Load() != 0 {
		t.Fatal("raw reads were attributed before protocol acceptance")
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- firstReply.Send(context.Background(), []byte("response-one")) }()
	waitStatisticsSignal(t, conn.writesEntered)
	secondStarted := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		close(secondStarted)
		secondDone <- secondReply.Send(context.Background(), []byte("response-two"))
	}()
	waitStatisticsSignal(t, secondStarted)
	<-time.After(30 * time.Millisecond)
	if first.SocketWrite.Snapshot().Count != 0 || second.SocketWrite.Snapshot().Count != 0 {
		t.Fatal("unfinished socket calls were counted")
	}
	close(conn.releaseWrite)
	waitStatisticsSignal(t, conn.cleanupEntered)
	beforeCleanup := first.SocketWrite.Snapshot()
	if beforeCleanup.Count != 1 || beforeCleanup.TotalNS < uint64((30*time.Millisecond).Nanoseconds()) || first.SentPackets.Load() != 1 {
		t.Fatal("socket completion was not recorded at the syscall boundary")
	}
	<-time.After(30 * time.Millisecond)
	if first.SocketWrite.Snapshot() != beforeCleanup || second.SentPackets.Load() != 0 {
		t.Fatal("deadline cleanup changed socket duration or bypassed the write lock")
	}
	close(conn.releaseCleanup)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if second.WriteQueue.Snapshot().TotalNS < uint64((30 * time.Millisecond).Nanoseconds()) {
		t.Fatal("second path did not record shared socket lock contention")
	}
	if aggregate.SocketWrite.Snapshot().TotalNS != first.SocketWrite.Snapshot().TotalNS+second.SocketWrite.Snapshot().TotalNS {
		t.Fatal("aggregate and path socket durations used different observations")
	}
	if aggregate.SentPackets.Load() != 2 || first.SentPackets.Load() != 1 || second.SentPackets.Load() != 1 {
		t.Fatal("writes were attributed to the wrong path")
	}
	conn.mu.Lock()
	conn.writeErr = syscall.EMSGSIZE
	conn.mu.Unlock()
	if err := secondReply.Send(context.Background(), []byte("bad")); !errors.Is(err, syscall.EMSGSIZE) {
		t.Fatalf("write failure = %v", err)
	}
	if aggregate.SendErrors.Load() != 1 || second.SendErrors.Load() != 1 || first.SendErrors.Load() != 0 {
		t.Fatal("failed socket write was attributed incorrectly")
	}
	enabled.Store(false)
	conn.mu.Lock()
	conn.writeErr = nil
	conn.mu.Unlock()
	beforeDisabled := second.SocketWrite.Snapshot()
	if err := secondReply.Send(context.Background(), []byte("disabled")); err != nil {
		t.Fatal(err)
	}
	if second.SentPackets.Load() != 2 || second.SocketWrite.Snapshot() != beforeDisabled {
		t.Fatal("disabled diagnostics lost basic counts or recorded timing")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := secondReply.Send(context.Background(), []byte("closed")); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("closed path send = %v", err)
	}
	if second.SentPackets.Load() != 2 || second.SendErrors.Load() != 1 {
		t.Fatal("close lost counters or counted a pre-socket rejection")
	}
}

func waitStatisticsSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for socket statistics boundary")
	}
}

type setupStatisticsConn struct {
	*fakeConnectedConn
	entered chan struct{}
	release chan struct{}
}

func (c *setupStatisticsConn) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		close(c.entered)
		<-c.release
	}
	return nil
}

type setupStatisticsPacketConn struct {
	*fakePacketConn
	setup *setupStatisticsConn
}

func (c *setupStatisticsPacketConn) SetWriteDeadline(deadline time.Time) error {
	return c.setup.SetWriteDeadline(deadline)
}

func TestSocketStatisticsExcludeDeadlineSetupFromQueueAndIO(t *testing.T) {
	for _, listener := range []bool{false, true} {
		name := "carrier"
		if listener {
			name = "listener"
		}
		t.Run(name, func(t *testing.T) {
			var enabled atomic.Bool
			enabled.Store(true)
			counters := &transport.Counters{DiagnosticsEnabled: &enabled}
			conn := &setupStatisticsConn{
				fakeConnectedConn: newFakeConnectedConn("local", "remote"),
				entered:           make(chan struct{}), release: make(chan struct{}),
			}
			var send func(context.Context, []byte) error
			if listener {
				packetConn := &setupStatisticsPacketConn{fakePacketConn: newFakePacketConn("local"), setup: conn}
				packets := make(chan transport.ReceivedPacket, 1)
				l, err := transport.ServePacketConn(packetConn, transport.ListenerOptions{
					PathID: "listener", Statistics: counters,
					OnPacket: func(packet transport.ReceivedPacket) { packets <- packet },
				})
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()
				packetConn.reads <- packetRead{payload: []byte("one"), remote: fakeAddr("remote")}
				send = receivePacket(t, packets).Reply.Send
			} else {
				carrier, err := transport.OpenCarrier(context.Background(), "carrier-0", "remote", transport.CarrierOptions{
					Statistics: counters,
					Dial:       func(context.Context, string) (net.Conn, error) { return conn, nil },
				})
				if err != nil {
					t.Fatal(err)
				}
				defer carrier.Close()
				send = carrier.Send
			}
			defer func() {
				select {
				case <-conn.release:
				default:
					close(conn.release)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- send(ctx, []byte("payload")) }()
			waitStatisticsSignal(t, conn.entered)
			started := time.Now()
			<-time.After(30 * time.Millisecond)
			held := time.Since(started)
			close(conn.release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			queue, socket := counters.WriteQueue.Snapshot(), counters.SocketWrite.Snapshot()
			if queue.Count != 1 || socket.Count != 1 || queue.TotalNS >= uint64(held) || socket.TotalNS >= uint64(held) {
				t.Fatalf("deadline setup leaked into timing: queue=%+v socket=%+v held=%v", queue, socket, held)
			}
		})
	}
}
