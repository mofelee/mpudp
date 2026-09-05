package sessionv2

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/handshakev2"
	"github.com/mofelee/mpudp/internal/transport"
)

type ownedNativeConn struct {
	local, remote net.Addr
	closed        chan struct{}
	once          sync.Once
	writes        atomic.Int32
}

func (c *ownedNativeConn) Read([]byte) (int, error) { <-c.closed; return 0, io.EOF }
func (c *ownedNativeConn) Write(packet []byte) (int, error) {
	c.writes.Add(1)
	return len(packet), nil
}
func (c *ownedNativeConn) Close() error                     { c.once.Do(func() { close(c.closed) }); return nil }
func (c *ownedNativeConn) LocalAddr() net.Addr              { return c.local }
func (c *ownedNativeConn) RemoteAddr() net.Addr             { return c.remote }
func (c *ownedNativeConn) SetDeadline(time.Time) error      { return nil }
func (c *ownedNativeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *ownedNativeConn) SetWriteDeadline(time.Time) error { return nil }

func ownedNativeCarrier(t *testing.T, binding handshakev2.Binding) (*transport.Carrier, []*ownedNativeConn) {
	t.Helper()
	conns := make([]*ownedNativeConn, 2)
	for i := range conns {
		// net.IPv4 deliberately returns a16-byte IPv4-mapped representation.
		conns[i] = &ownedNativeConn{local: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: int(binding.Local.Port())}, remote: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: int(binding.Remote.Port())}, closed: make(chan struct{})}
	}
	next := 0
	carrier, err := transport.OpenCarrier(context.Background(), "owned-native", "test", transport.CarrierOptions{Dial: func(context.Context, string) (net.Conn, error) {
		conn := conns[next]
		next++
		return conn, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = carrier.Close() })
	return carrier, conns
}

func TestOwnedNativeBootstrapPinnedBeforeAndAfterDispatch(t *testing.T) {
	for _, dispatched := range []bool{false, true} {
		t.Run(map[bool]string{false: "waiting", true: "dispatched"}[dispatched], func(t *testing.T) {
			cfg := configFor(profile(1), false)
			ownedConfig(&cfg)
			setup, cfg, _ := scratchSetup(t, cfg, 0)
			carrier, conns := ownedNativeCarrier(t, setup.Binding)
			cfg.BootstrapPath = carrier
			c, err := New(setup, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.Close)
			if !c.paths[0].nativeTiming || c.paths[0].transportGeneration != 1 || c.paths[0].sender == carrier {
				t.Fatal("bootstrap did not capture native capability and generation at construction")
			}
			now := time.Unix(1000, 0)
			if _, err := c.Start(now); err != nil {
				t.Fatal(err)
			}
			var intent *SendIntent
			if dispatched {
				intent, _, err = c.TakeSend(now)
				if err != nil || intent == nil || !intent.NativeTiming {
					t.Fatalf("native intent: %+v %v", intent, err)
				}
			}
			if err := carrier.Rebuild(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !dispatched {
				var result Result
				intent, result, err = c.TakeSend(now)
				if err != nil || intent != nil || len(result.Sends) != 1 || !errors.Is(result.Sends[0].Err, transport.ErrGenerationReplaced) {
					t.Fatalf("rebuild before dispatch recaptured sender: %+v %v", result, err)
				}
			} else {
				started, sendErr := transport.SendWithAttempt(context.Background(), intent.Sender, intent.Packet)
				if !started.IsZero() || !errors.Is(sendErr, transport.ErrGenerationReplaced) {
					t.Fatalf("stale sender reached replacement: %v %v", started, sendErr)
				}
				intent.Release()
				if _, err := c.CompleteSend(now, SendOutcome{Token: intent.Token, Invoked: true, AttemptKnown: true, FinishedAt: now, Err: sendErr}); err != nil || c.paths[0].budget.sends != 0 {
					t.Fatalf("stale native completion: %v", err)
				}
			}
			if conns[0].writes.Load()+conns[1].writes.Load() != 0 {
				t.Fatal("stale captured route wrote to a socket")
			}
		})
	}
}

func TestOwnedInboundJoinPinsReplyAtAuthentication(t *testing.T) {
	p := newOwnedPair(t, 2, nil)
	p.client.controller.controlCursor = 1
	join, _, err := p.client.controller.TakeSend(p.now)
	if err != nil || join == nil {
		t.Fatal(err)
	}
	binding := opposite(join.Binding, true)
	carrier, conns := ownedNativeCarrier(t, binding)
	c := p.server.controller
	if _, err := c.Receive(p.now, binding, carrier, join.Packet); err != nil {
		t.Fatal(err)
	}
	if c.paths[1].join.sender == carrier || c.paths[1].join.transportGeneration != 1 || !c.paths[1].join.nativeTiming {
		t.Fatal("authenticated join retained mutable native Carrier")
	}
	if err := carrier.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.controlCursor = 1
	intent, result, err := c.TakeSend(p.now)
	if err != nil || len(result.Sends) != 1 || !errors.Is(result.Sends[0].Err, transport.ErrGenerationReplaced) || conns[1].writes.Load() != 0 {
		t.Fatalf("join dispatch recaptured rebuilt native route: %+v %v", result, err)
	}
	if intent != nil && intent.PathID == 2 {
		t.Fatal("stale join acquired a new sender")
	}
}

func TestOwnedNativeBindingCaptureFailureRollsBackAllInitialOwners(t *testing.T) {
	cfg := configFor(profile(1), false)
	ownedConfig(&cfg)
	setup, cfg, peer := scratchSetup(t, cfg, 0)
	wrong := setup.Binding
	wrong.Local = wrong.Remote
	carrier, _ := ownedNativeCarrier(t, wrong)
	cfg.BootstrapPath = carrier
	if c, err := New(setup, cfg); c != nil || !errors.Is(err, transport.ErrGenerationReplaced) || peer.Snapshot().Bytes != 0 {
		t.Fatalf("binding failure retained consumed initial owners: %v", err)
	}
	for _, lease := range setup.Initial {
		if !lease.Snapshot().Released {
			t.Fatal("constructor rollback left an initial owner live")
		}
	}
}
