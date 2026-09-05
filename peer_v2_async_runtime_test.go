//go:build linux

package mpudp

import (
	"context"
	"crypto/rand"
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/sessionv2"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
	"golang.org/x/sys/unix"
)

type v2AsyncBarrier struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	arrive  sync.Once
	armed   atomic.Bool
}

func newV2AsyncBarrier() *v2AsyncBarrier {
	return &v2AsyncBarrier{entered: make(chan struct{}), release: make(chan struct{})}
}

func (b *v2AsyncBarrier) wait() {
	b.arrive.Do(func() { close(b.entered) })
	<-b.release
}

func (b *v2AsyncBarrier) unblock() { b.once.Do(func() { close(b.release) }) }

type v2AsyncDataGate struct {
	barrier *v2AsyncBarrier
	failure error
	claimed atomic.Bool
	seen    atomic.Int64
}

func (g *v2AsyncDataGate) intercept(packet []byte) (bool, error) {
	if len(packet) < 6 || wirev2.PacketType(packet[5]) != wirev2.TypeFECBundle {
		return false, nil
	}
	g.seen.Add(1)
	if g.barrier == nil || !g.claimed.CompareAndSwap(false, true) {
		return false, nil
	}
	// A deliberately noncooperative injected operation makes joining testable
	// independently of the ordinary 20 ms cancellation deadline.
	g.barrier.wait()
	return true, g.failure
}

func v2AsyncReply(path transport.ReplyPath, gate *v2AsyncDataGate) transport.ReplyPath {
	return &v2WorkerTestPath{ReplyPath: path, send: func(ctx context.Context, packet []byte) error {
		if handled, err := gate.intercept(packet); handled {
			return err
		}
		return path.Send(ctx, packet)
	}}
}

type v2AsyncNativeConn struct {
	net.Conn
	gate  *v2AsyncDataGate
	close *v2AsyncBarrier
}

type v2AsyncCarrier struct {
	*v2GatedCarrier
	cleanup *v2AsyncBarrier
}

func (c *v2AsyncCarrier) Close() error {
	if c.cleanup != nil && c.cleanup.armed.Load() {
		c.cleanup.wait()
	}
	return c.v2GatedCarrier.Close()
}

// The fixture configures PMTU on its underlying IPv4 socket before wrapping it.
func (c *v2AsyncNativeConn) PMTUEnabled() bool { return true }
func (c *v2AsyncNativeConn) Write(packet []byte) (int, error) {
	if handled, err := c.gate.intercept(packet); handled {
		if err != nil {
			return 0, err
		}
		return len(packet), nil
	}
	return c.Conn.Write(packet)
}
func (c *v2AsyncNativeConn) Close() error {
	if c.close != nil && c.close.armed.Load() {
		c.close.wait()
	}
	return c.Conn.Close()
}

func v2AsyncClient(t *testing.T, workers int, native bool, gates []*v2AsyncDataGate, cleanup *v2AsyncBarrier) (*Peer, DatagramSession, DatagramSession) {
	t.Helper()
	server, listener := v2LoopbackListener(t, "0.0.0.0:0", true)
	cfg := v2LoopbackConfig(true)
	cfg.Limits.MaxSendWorkers = workers
	port := server.listenerSocket.LocalAddr().(*net.UDPAddr).Port
	for i := range gates {
		cfg.Carriers = append(cfg.Carriers, net.JoinHostPort("127.0.0."+strconv.Itoa(i+1), strconv.Itoa(port)))
	}
	deps := defaultRuntimeDependencies()
	next := 0
	deps.openCarrier = func(ctx context.Context, id, remote string, options transport.CarrierOptions) (runtimeCarrier, error) {
		gate := gates[next]
		next++
		if native {
			options.Dial = func(ctx context.Context, address string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, "udp4", address)
				if err != nil {
					return nil, err
				}
				raw, err := conn.(syscall.Conn).SyscallConn()
				if err == nil {
					var optionErr error
					err = raw.Control(func(fd uintptr) {
						optionErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_MTU_DISCOVER, unix.IP_PMTUDISC_DO)
					})
					err = errors.Join(err, optionErr)
				}
				if err != nil {
					_ = conn.Close()
					return nil, err
				}
				return &v2AsyncNativeConn{Conn: conn, gate: gate, close: cleanup}, nil
			}
			return transport.OpenCarrier(ctx, id, remote, options)
		}
		receive := options.OnPacket
		options.OnPacket = func(packet transport.ReceivedPacket) {
			packet.Reply = v2AsyncReply(packet.Reply, gate)
			receive(packet)
		}
		carrier, err := transport.OpenCarrier(ctx, id, remote, options)
		if err != nil {
			return nil, err
		}
		passthrough := &v2Gate{}
		passthrough.failAfter.Store(-1)
		return &v2AsyncCarrier{v2GatedCarrier: &v2GatedCarrier{
			v2GatedReply: &v2GatedReply{ReplyPath: v2AsyncReply(carrier, gate), gate: passthrough}, carrier: carrier,
		}, cleanup: cleanup}, nil
	}
	client, err := newPeerWithDependencies(cfg, rand.Reader, deps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	public, err := client.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), v2LoopbackTimeout)
	defer cancel()
	accepted, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sender, receiver := public.(DatagramSession), accepted.(DatagramSession)
	v2WaitReady(t, sender, len(gates))
	v2WaitReady(t, receiver, len(gates))
	if cleanup != nil {
		cleanup.armed.Store(true)
	}
	return client, sender, receiver
}

func v2AwaitAsync(t *testing.T, event <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-event:
	case <-time.After(2 * time.Second):
		t.Fatal(label)
	}
}

func v2AssertAsyncPending(t *testing.T, result <-chan error, label string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("%s returned before its owned operation: %v", label, err)
	default:
	}
}

func v2AsyncInitialBytes(t *testing.T, peer *Peer, responder bool) uint64 {
	t.Helper()
	cfg, err := v2ControllerConfig(peer.config, responder)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := sessionv2.RequiredInitialClaims(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var total uint64
	for _, claim := range claims {
		total += claim.Bytes
	}
	return total
}

func TestV2AsyncRuntimeWorkerCapacityAndPublicProgress(t *testing.T) {
	for _, native := range []bool{false, true} {
		for _, workers := range []int{1, 3} {
			t.Run(map[bool]string{false: "custom", true: "native"}[native]+"/workers="+strconv.Itoa(workers), func(t *testing.T) {
				barrier := newV2AsyncBarrier()
				defer barrier.unblock()
				gates := []*v2AsyncDataGate{{}, {barrier: barrier}, {}}
				client, sender, receiver := v2AsyncClient(t, workers, native, gates, nil)
				if len(client.v2.sendSlots) != workers {
					t.Fatal("configured worker bound was not installed")
				}
				if err := sender.WritePacket([]byte("first")); err != nil {
					t.Fatal(err)
				}
				flushed := make(chan error, 1)
				go func() { flushed <- sender.Flush(context.Background()) }()
				v2AwaitAsync(t, barrier.entered, "DATA did not enter blocked path")
				admitted := make(chan error, 1)
				go func() { admitted <- sender.WritePacket([]byte("admitted while blocked")) }()
				if err := receiveErrorWithin(t, admitted); err != nil {
					t.Fatalf("blocked DATA prevented public admission: %v", err)
				}
				if err := receiver.WritePacket([]byte("reverse direction")); err != nil {
					t.Fatal(err)
				}
				v2Flush(t, receiver)
				if got := readWithin(t, sender); string(got) != "reverse direction" {
					t.Fatalf("blocked DATA prevented public receive: %q", got)
				}
				if workers == 1 {
					if count := gates[0].seen.Load() + gates[2].seen.Load(); count != 0 {
						t.Fatalf("one worker executed another path while blocked: %d", count)
					}
				} else {
					deadline := time.Now().Add(time.Second)
					for gates[0].seen.Load()+gates[2].seen.Load() == 0 {
						if time.Now().After(deadline) {
							t.Fatal("available workers did not advance another DATA path")
						}
						time.Sleep(time.Millisecond)
					}
				}
				v2AssertAsyncPending(t, flushed, "Flush")
				barrier.unblock()
				if err := receiveErrorWithin(t, flushed); err != nil {
					t.Fatal(err)
				}
				v2Flush(t, sender)
			})
		}
	}
}

func TestV2AsyncRuntimeFlushWaitsForOutOfOrderFailure(t *testing.T) {
	barrier := newV2AsyncBarrier()
	defer barrier.unblock()
	failure := errors.New("last owned shard failed")
	gates := []*v2AsyncDataGate{{}, {barrier: barrier, failure: failure}, {}}
	_, sender, _ := v2AsyncClient(t, 3, false, gates, nil)
	if err := sender.WritePacket([]byte("failure frontier")); err != nil {
		t.Fatal(err)
	}
	flushed := make(chan error, 1)
	go func() { flushed <- sender.Flush(context.Background()) }()
	v2AwaitAsync(t, barrier.entered, "failed shard did not start")
	deadline := time.Now().Add(time.Second)
	for {
		s := sender.(*v2Session)
		s.owner.mu.Lock()
		pending := s.activeSends
		s.owner.mu.Unlock()
		if gates[0].seen.Load()+gates[2].seen.Load() >= 3 && pending == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("other shards did not complete ahead of the blocked failure")
		}
		time.Sleep(time.Millisecond)
	}
	v2AssertAsyncPending(t, flushed, "Flush")
	if snapshot := v2SessionSnapshot(t, sender); snapshot.CompletedThrough != 0 {
		t.Fatal("dispatch advanced the public completion frontier")
	}
	barrier.unblock()
	if err := receiveErrorWithin(t, flushed); !errors.Is(err, failure) {
		t.Fatalf("Flush lost the late send failure: %v", err)
	}
	if total := gates[0].seen.Load() + gates[1].seen.Load() + gates[2].seen.Load(); total != 5 {
		t.Fatalf("Flush returned before all five shard outcomes: %d", total)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := sender.Flush(ctx); !errors.Is(err, failure) {
		t.Fatalf("repeated Flush lost sticky failure: %v", err)
	}
}

func TestV2AsyncRuntimeCloseJoinsSendAndCarrierCleanup(t *testing.T) {
	for _, scope := range []string{"session", "peer"} {
		for _, native := range []bool{false, true} {
			t.Run(scope+"/"+map[bool]string{false: "custom", true: "native"}[native], func(t *testing.T) {
				send, cleanup := newV2AsyncBarrier(), newV2AsyncBarrier()
				defer send.unblock()
				defer cleanup.unblock()
				client, sender, _ := v2AsyncClient(t, 1, native, []*v2AsyncDataGate{{barrier: send}}, cleanup)
				initialBytes := v2AsyncInitialBytes(t, client, false)
				if err := sender.WritePacket([]byte("retained until terminal")); err != nil {
					t.Fatal(err)
				}
				flushed := make(chan error, 1)
				go func() { flushed <- sender.Flush(context.Background()) }()
				v2AwaitAsync(t, send.entered, "send did not enter close barrier")
				closed := make(chan error, 1)
				go func() {
					if scope == "peer" {
						closed <- client.Close()
					} else {
						closed <- sender.Close()
					}
				}()
				v2AwaitAsync(t, sender.(*v2Session).ctx.Done(), "Close did not cancel the Session")
				if err := receiveErrorWithin(t, flushed); !errors.Is(err, ErrClosed) {
					t.Fatalf("Close did not wake Flush before I/O joined: %v", err)
				}
				v2AssertAsyncPending(t, closed, scope+" Close")
				if usage := client.v2.credits.Snapshot(); usage.Bytes < initialBytes || usage.SessionSlots != 1 {
					t.Fatalf("active send lost initial credit: floor=%d now=%+v", initialBytes, usage)
				}
				select {
				case <-cleanup.entered:
					t.Fatal("carrier cleanup started before send ownership returned")
				default:
				}
				send.unblock()
				v2AwaitAsync(t, cleanup.entered, "carrier cleanup did not begin after send completion")
				v2AssertAsyncPending(t, closed, scope+" Close during carrier cleanup")
				if usage := client.v2.credits.Snapshot(); usage.Bytes == 0 || usage.SessionSlots != 1 {
					t.Fatalf("blocked carrier cleanup returned wrapper credit: %+v", usage)
				}
				cleanup.unblock()
				if err := receiveErrorWithin(t, closed); err != nil {
					t.Fatal(err)
				}
				v2AssertReleased(t, client)
			})
		}
	}
}

type v2AsyncListenerSocket struct {
	runtimePacketListener
	cleanup *v2AsyncBarrier
}

func (l *v2AsyncListenerSocket) Close() error {
	l.cleanup.wait()
	return l.runtimePacketListener.Close()
}

func TestV2AsyncRuntimeListenerCloseJoinsAcceptedSendAndSocket(t *testing.T) {
	send, cleanup := newV2AsyncBarrier(), newV2AsyncBarrier()
	defer send.unblock()
	defer cleanup.unblock()
	gate := &v2AsyncDataGate{barrier: send}
	cfg := v2LoopbackConfig(true)
	cfg.Listen = "127.0.0.1:9"
	deps := defaultRuntimeDependencies()
	deps.openListener = func(ctx context.Context, network, _ string, options transport.ListenerOptions) (runtimePacketListener, error) {
		receive := options.OnPacket
		options.OnPacket = func(packet transport.ReceivedPacket) {
			packet.Reply = v2AsyncReply(packet.Reply, gate)
			receive(packet)
		}
		socket, err := transport.OpenListener(ctx, network, "127.0.0.1:0", options)
		if err != nil {
			return nil, err
		}
		return &v2AsyncListenerSocket{runtimePacketListener: socket, cleanup: cleanup}, nil
	}
	server, err := newPeerWithDependencies(cfg, rand.Reader, deps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	listener, err := server.Listener()
	if err != nil {
		t.Fatal(err)
	}
	_, _, sender := v2LoopbackDial(t, server, listener, []string{"127.0.0.1"}, true)
	initialBytes := v2AsyncInitialBytes(t, server, true)
	if err := sender.WritePacket([]byte("accepted send")); err != nil {
		t.Fatal(err)
	}
	flushed := make(chan error, 1)
	go func() { flushed <- sender.Flush(context.Background()) }()
	v2AwaitAsync(t, send.entered, "accepted DATA did not start")
	closed := make(chan error, 1)
	go func() { closed <- listener.Close() }()
	v2AwaitAsync(t, sender.(*v2Session).ctx.Done(), "Listener.Close did not cancel accepted Session")
	if err := receiveErrorWithin(t, flushed); !errors.Is(err, ErrClosed) {
		t.Fatalf("Listener.Close did not wake Flush: %v", err)
	}
	v2AssertAsyncPending(t, closed, "Listener.Close")
	if usage := server.v2.credits.Snapshot(); usage.Bytes < initialBytes || usage.SessionSlots != 1 {
		t.Fatalf("Listener.Close returned initial send credit: floor=%d now=%+v", initialBytes, usage)
	}
	send.unblock()
	v2AwaitAsync(t, cleanup.entered, "Listener.Close did not reach socket cleanup")
	v2AssertAsyncPending(t, closed, "Listener.Close during socket cleanup")
	cleanup.unblock()
	if err := receiveErrorWithin(t, closed); err != nil {
		t.Fatal(err)
	}
	v2AssertReleased(t, server)
}

func TestV2AsyncRuntimeParentCancellationDrainsCarrierStartup(t *testing.T) {
	for _, lateCarrier := range []bool{false, true} {
		t.Run(map[bool]string{false: "opener-error", true: "late-carrier"}[lateCarrier], func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			startup := newV2AsyncBarrier()
			defer startup.unblock()
			carrier := &testRuntimeCarrier{id: "carrier-0"}
			deps := defaultRuntimeDependencies()
			deps.openCarrier = func(ctx context.Context, _, _ string, _ transport.CarrierOptions) (runtimeCarrier, error) {
				startup.wait()
				if lateCarrier {
					return carrier, nil
				}
				return nil, ctx.Err()
			}
			cfg := v2LoopbackConfig(true)
			cfg.Carriers = []string{"127.0.0.1:9001"}
			peer, err := newPeerWithContextAndDependencies(parent, cfg, rand.Reader, deps)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = peer.Close() })
			dialed := make(chan error, 1)
			go func() {
				session, err := peer.NewSession()
				if session != nil {
					err = errors.New("canceled startup returned a Session")
				}
				dialed <- err
			}()
			v2AwaitAsync(t, startup.entered, "Carrier startup did not block")
			cancel()
			startup.unblock()
			if err := receiveErrorWithin(t, dialed); !errors.Is(err, ErrClosed) && !errors.Is(err, context.Canceled) {
				t.Fatalf("parent-canceled NewSession did not retire before Peer.Close: %v", err)
			}
			v2AwaitAsync(t, peer.workerDone, "canceled driver did not drain construction cleanup")
			v2AssertReleased(t, peer)
			if lateCarrier {
				carrier.mu.Lock()
				calls := carrier.closeCall
				carrier.mu.Unlock()
				if calls != 1 {
					t.Fatalf("late Carrier was not joined before NewSession returned: %d closes", calls)
				}
			}
			if err := peer.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestV2AsyncRuntimeParentCancellationAllowsSessionClose(t *testing.T) {
	server, listener := v2LoopbackListener(t, "127.0.0.1:0", true)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := v2LoopbackConfig(true)
	cfg.Carriers = []string{server.listenerSocket.LocalAddr().String()}
	client, err := newPeerWithContextAndDependencies(parent, cfg, rand.Reader, defaultRuntimeDependencies())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	public, err := client.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	sender := public.(DatagramSession)
	v2AcceptReady(t, listener, sender)
	cancel()
	v2AwaitAsync(t, client.workerDone, "parent cancellation did not drain the Session")
	closed := make(chan error, 1)
	go func() { closed <- sender.Close() }()
	if err := receiveErrorWithin(t, closed); err != nil {
		t.Fatalf("Session.Close after parent cancellation required Peer.Close: %v", err)
	}
	v2AssertReleased(t, client)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestV2AsyncRuntimeParentCancellationAllowsListenerClose(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := v2LoopbackConfig(true)
	cfg.Listen = "127.0.0.1:9"
	deps := defaultRuntimeDependencies()
	deps.openListener = func(ctx context.Context, network, _ string, options transport.ListenerOptions) (runtimePacketListener, error) {
		return transport.OpenListener(ctx, network, "127.0.0.1:0", options)
	}
	server, err := newPeerWithContextAndDependencies(parent, cfg, rand.Reader, deps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	listener, err := server.Listener()
	if err != nil {
		t.Fatal(err)
	}
	_, _, accepted := v2LoopbackDial(t, server, listener, []string{"127.0.0.1"}, true)
	cancel()
	v2AwaitAsync(t, server.workerDone, "parent cancellation did not drain listener ownership")
	closedSession, closedListener := make(chan error, 1), make(chan error, 1)
	go func() { closedSession <- accepted.Close() }()
	go func() { closedListener <- listener.Close() }()
	if err := receiveErrorWithin(t, closedSession); err != nil {
		t.Fatalf("accepted Session.Close required Peer.Close: %v", err)
	}
	if err := receiveErrorWithin(t, closedListener); err != nil {
		t.Fatalf("Listener.Close after parent cancellation required Peer.Close: %v", err)
	}
	v2AssertReleased(t, server)
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
}
