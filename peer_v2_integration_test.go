//go:build linux

package mpudp

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/mofelee/mpudp/config"
	"github.com/mofelee/mpudp/internal/sessionv2"
	"github.com/mofelee/mpudp/internal/transport"
)

const v2LoopbackTimeout = 8 * time.Second

func v2LoopbackConfig(aggregation bool) config.Config {
	cfg := config.DefaultV2(config.ProtocolDatagram)
	base := baseConfig()
	cfg.PSK, cfg.FEC = base.PSK, base.FEC
	cfg.Aggregation.Enabled = aggregation
	cfg.Aggregation.MaxDelay = 10 * time.Millisecond
	return cfg
}

func v2LoopbackListener(t *testing.T, bind string, aggregation bool) (*Peer, Listener) {
	t.Helper()
	cfg := v2LoopbackConfig(aggregation)
	host, _, err := net.SplitHostPort(bind)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Listen = net.JoinHostPort(host, "9")
	deps := defaultRuntimeDependencies()
	deps.openListener = func(ctx context.Context, network, _ string, options transport.ListenerOptions) (runtimePacketListener, error) {
		return transport.OpenListener(ctx, network, bind, options)
	}
	peer, err := newPeerWithDependencies(cfg, rand.Reader, deps)
	if err != nil {
		t.Fatalf("NewPeer(listener): %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	listener, err := peer.Listener()
	if err != nil {
		t.Fatalf("Listener(): %v", err)
	}
	return peer, listener
}

func v2LoopbackDial(t *testing.T, server *Peer, listener Listener, destinations []string, aggregation bool) (*Peer, DatagramSession, DatagramSession) {
	t.Helper()
	port := server.listenerSocket.LocalAddr().(*net.UDPAddr).Port
	cfg := v2LoopbackConfig(aggregation)
	for _, destination := range destinations {
		cfg.Carriers = append(cfg.Carriers, net.JoinHostPort(destination, strconv.Itoa(port)))
	}
	client, err := NewPeer(cfg)
	if err != nil {
		t.Fatalf("NewPeer(initiator): %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	outbound, err := client.NewSession()
	if err != nil {
		t.Fatalf("NewSession(): %v", err)
	}
	initiator, ok := outbound.(DatagramSession)
	if !ok {
		t.Fatal("v2 initiator does not implement DatagramSession")
	}
	ctx, cancel := context.WithTimeout(context.Background(), v2LoopbackTimeout)
	defer cancel()
	inbound, err := listener.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept(): %v", err)
	}
	accepted, ok := inbound.(DatagramSession)
	if !ok {
		t.Fatal("v2 accepted Session does not implement DatagramSession")
	}
	v2WaitReady(t, initiator, len(destinations))
	v2WaitReady(t, accepted, len(destinations))
	return client, initiator, accepted
}

func v2SessionSnapshot(t *testing.T, public DatagramSession) sessionv2.Snapshot {
	t.Helper()
	s, ok := public.(*v2Session)
	if !ok {
		t.Fatalf("unexpected v2 Session type %T", public)
	}
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if s.controller == nil {
		return sessionv2.Snapshot{Closed: s.closed}
	}
	return s.controller.Snapshot()
}

func v2WaitReady(t *testing.T, public DatagramSession, paths int) {
	t.Helper()
	deadline := time.Now().Add(v2LoopbackTimeout)
	for {
		snapshot := v2SessionSnapshot(t, public)
		active := 0
		for _, path := range snapshot.Paths {
			if path.Active && path.SendEpoch == 2 && path.ReceiveEpoch == 2 {
				active++
			}
		}
		if snapshot.Ready && active == paths {
			return
		}
		if snapshot.Closed || time.Now().After(deadline) {
			t.Fatalf("v2 readiness: want %d established paths, snapshot %+v", paths, snapshot)
		}
		time.Sleep(time.Millisecond)
	}
}

func v2Flush(t *testing.T, session DatagramSession) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), v2LoopbackTimeout)
	defer cancel()
	if err := session.Flush(ctx); err != nil {
		t.Fatalf("Flush(): %v", err)
	}
}

func v2ExchangeOwned(t *testing.T, sender, receiver DatagramSession, label string) [][]byte {
	t.Helper()
	want := [][]byte{
		[]byte(label + " small"),
		{},
		bytes.Repeat([]byte(label+" original "), 2048),
		[]byte(label + " tail"),
	}
	for _, payload := range want {
		input := bytes.Clone(payload)
		if err := sender.WritePacket(input); err != nil {
			t.Fatalf("WritePacket(%d): %v", len(input), err)
		}
		clear(input)
	}
	v2Flush(t, sender)
	remaining := make(map[string]int, len(want))
	for _, payload := range want {
		remaining[string(payload)]++
	}
	retained := make([][]byte, 0, len(want))
	for range want {
		got := readWithin(t, receiver)
		if got == nil {
			t.Fatal("empty Datagram was returned as nil")
		}
		key := string(got)
		if remaining[key] == 0 {
			t.Fatalf("unexpected, duplicated or mutated Datagram: length %d", len(got))
		}
		remaining[key]--
		retained = append(retained, got)
	}
	return retained
}

func TestV2LoopbackDatagramsAcrossCarriersAndAggregation(t *testing.T) {
	for _, count := range []int{1, 5} {
		for _, aggregate := range []bool{false, true} {
			t.Run(fmt.Sprintf("carriers_%d/aggregation_%t", count, aggregate), func(t *testing.T) {
				server, listener := v2LoopbackListener(t, "0.0.0.0:0", aggregate)
				destinations := make([]string, count)
				for i := range destinations {
					destinations[i] = fmt.Sprintf("127.0.0.%d", i+1)
				}
				_, initiator, accepted := v2LoopbackDial(t, server, listener, destinations, aggregate)
				for i, path := range v2SessionSnapshot(t, accepted).Paths[:count] {
					if path.Binding.Local.Addr() != netip.MustParseAddr(destinations[i]) || path.PathID != uint16(i+1) {
						t.Fatalf("path %d learned incorrect wildcard destination: %+v", i+1, path)
					}
				}
				retained := v2ExchangeOwned(t, initiator, accepted, "forward")
				copies := make([][]byte, len(retained))
				for i := range retained {
					copies[i] = bytes.Clone(retained[i])
				}
				v2ExchangeOwned(t, accepted, initiator, "reverse")
				for i := range retained {
					if !bytes.Equal(retained[i], copies[i]) {
						t.Fatal("later traffic overwrote caller-owned ReadPacket bytes")
					}
					clear(retained[i])
				}
				fresh := v2ExchangeOwned(t, initiator, accepted, "forward")
				freshCopies := make([][]byte, len(fresh))
				for i := range fresh {
					freshCopies[i] = bytes.Clone(fresh[i])
				}
				ctx, cancel := context.WithTimeout(context.Background(), v2LoopbackTimeout)
				defer cancel()
				if err := initiator.CloseGracefully(ctx); err != nil {
					t.Fatalf("CloseGracefully(): %v", err)
				}
				if err := accepted.Close(); err != nil {
					t.Fatalf("Close(): %v", err)
				}
				for i := range fresh {
					if !bytes.Equal(fresh[i], freshCopies[i]) {
						t.Fatal("Session close invalidated caller-owned ReadPacket bytes")
					}
				}
			})
		}
	}
}

func TestV2LoopbackWildcardBootstrapDestinations(t *testing.T) {
	server, listener := v2LoopbackListener(t, "0.0.0.0:0", true)
	for _, destination := range []string{"127.0.0.1", "127.0.0.2"} {
		t.Run(destination, func(t *testing.T) {
			_, initiator, accepted := v2LoopbackDial(t, server, listener, []string{destination}, true)
			path := v2SessionSnapshot(t, accepted).Paths[0]
			if path.Binding.Local.Addr() != netip.MustParseAddr(destination) {
				t.Fatalf("bootstrap destination = %v, want %s", path.Binding.Local, destination)
			}
			v2ExchangeOwned(t, initiator, accepted, "to wildcard")
			v2ExchangeOwned(t, accepted, initiator, "from wildcard")
		})
	}
}

func TestV2LoopbackIPv6(t *testing.T) {
	probe, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	for _, aggregate := range []bool{false, true} {
		t.Run(fmt.Sprintf("aggregation_%t", aggregate), func(t *testing.T) {
			server, listener := v2LoopbackListener(t, "[::]:0", aggregate)
			_, initiator, accepted := v2LoopbackDial(t, server, listener, []string{"::1"}, aggregate)
			if path := v2SessionSnapshot(t, accepted).Paths[0]; path.Binding.Local.Addr() != netip.IPv6Loopback() {
				t.Fatalf("IPv6 destination = %v", path.Binding.Local)
			}
			v2ExchangeOwned(t, initiator, accepted, "IPv6 forward")
			v2ExchangeOwned(t, accepted, initiator, "IPv6 reverse")
		})
	}
}

func TestV2LoopbackCloseUnblocksReadAndAccept(t *testing.T) {
	for _, scope := range []string{"session", "listener", "peer"} {
		t.Run(scope, func(t *testing.T) {
			server, listener := v2LoopbackListener(t, "127.0.0.1:0", false)
			_, _, accepted := v2LoopbackDial(t, server, listener, []string{"127.0.0.1"}, false)
			readResult := make(chan error, 1)
			go func() {
				_, err := accepted.ReadPacket()
				readResult <- err
			}()
			acceptResult := make(chan error, 1)
			if scope != "session" {
				go func() {
					_, err := listener.Accept(context.Background())
					acceptResult <- err
				}()
			}
			var err error
			switch scope {
			case "session":
				err = accepted.Close()
			case "listener":
				err = listener.Close()
			case "peer":
				err = server.Close()
			}
			if err != nil {
				t.Fatalf("%s Close(): %v", scope, err)
			}
			if err := receiveErrorWithin(t, readResult); !errors.Is(err, ErrClosed) {
				t.Fatalf("ReadPacket after %s Close() = %v", scope, err)
			}
			if scope != "session" {
				if err := receiveErrorWithin(t, acceptResult); !errors.Is(err, ErrClosed) {
					t.Fatalf("Accept after %s Close() = %v", scope, err)
				}
			}
			if err := accepted.WritePacket([]byte("late")); !errors.Is(err, ErrClosed) {
				t.Fatalf("WritePacket after close = %v", err)
			}
		})
	}
}
