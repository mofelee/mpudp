//go:build linux

package mpudp

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mofelee/mpudp/config"
	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func v2AdmissionClient(t *testing.T, server *Peer, cfg config.Config) (*Peer, DatagramSession) {
	t.Helper()
	port := server.listenerSocket.LocalAddr().(*net.UDPAddr).Port
	cfg.Carriers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(port))}
	client, err := NewPeer(cfg)
	if err != nil {
		t.Fatalf("NewPeer(initiator): %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	public, err := client.NewSession()
	if err != nil {
		t.Fatalf("NewSession(): %v", err)
	}
	session, ok := public.(DatagramSession)
	if !ok {
		t.Fatalf("v2 Session type %T lacks DatagramSession", public)
	}
	return client, session
}

func v2AcceptReady(t *testing.T, listener Listener, initiator DatagramSession) DatagramSession {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), v2LoopbackTimeout)
	defer cancel()
	public, err := listener.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept(): %v", err)
	}
	accepted, ok := public.(DatagramSession)
	if !ok {
		t.Fatalf("accepted v2 Session type %T lacks DatagramSession", public)
	}
	v2WaitReady(t, initiator, 1)
	v2WaitReady(t, accepted, 1)
	return accepted
}

func v2AssertNoAccept(t *testing.T, listener Listener) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	public, err := listener.Accept(ctx)
	if public != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected Accept() = %v, %v", public, err)
	}
}

func v2Usage(peer *Peer) creditv2.PeerSnapshot {
	peer.v2.mu.Lock()
	defer peer.v2.mu.Unlock()
	return peer.v2.credits.Snapshot()
}

func v2AssertReleased(t *testing.T, peer *Peer) {
	t.Helper()
	r := peer.v2
	r.mu.Lock()
	defer r.mu.Unlock()
	usage := r.credits.Snapshot()
	if usage.Usage != (creditv2.Usage{}) || usage.SessionSlots != 0 || usage.PendingHandshakes != 0 || usage.EstablishedSessions != 0 {
		t.Fatalf("retained v2 credits after cleanup: %+v", usage)
	}
	if len(r.sessions) != 0 || len(r.established) != 0 || len(r.dials) != 0 || len(r.routes) != 0 || len(r.sockets) != 0 {
		t.Fatalf("retained runtime state: sessions %d established %d dials %d routes %d sockets %d", len(r.sessions), len(r.established), len(r.dials), len(r.routes), len(r.sockets))
	}
	if r.listener != nil && len(r.listener.accept) != 0 {
		t.Fatal("retained pending accept after cleanup")
	}
}

func v2CloseAndAssertReleased(t *testing.T, peers ...*Peer) {
	t.Helper()
	for _, peer := range peers {
		if err := peer.Close(); err != nil {
			t.Fatalf("Peer.Close(): %v", err)
		}
		v2AssertReleased(t, peer)
		if !v2Usage(peer).Closed {
			t.Fatal("Peer.Close() did not close the ledger")
		}
	}
}

func TestV2AdmissionRejectsPSKAndFECMismatchWithoutListenerState(t *testing.T) {
	for _, mismatch := range []string{"PSK", "FEC"} {
		t.Run(mismatch, func(t *testing.T) {
			server, listener := v2LoopbackListener(t, "127.0.0.1:0", false)
			cfg := v2LoopbackConfig(false)
			if mismatch == "PSK" {
				cfg.PSK = config.NewSecret("different-v2-test-key")
			} else {
				cfg.FEC = config.FECConfig{DataShards: 2, ParityShards: 1}
			}
			client, attempt := v2AdmissionClient(t, server, cfg)
			v2AssertNoAccept(t, listener)
			if server.statistics.ingressAccepted.Load() == 0 {
				t.Fatal("mismatched handshake never reached listener transport")
			}
			v2AssertReleased(t, server)
			if err := attempt.Close(); err != nil {
				t.Fatalf("Close failed attempt: %v", err)
			}
			v2AssertReleased(t, client)
			v2CloseAndAssertReleased(t, client, server)
		})
	}
}

func TestV2AdmissionCorruptFramesDoNotRetainCreditsOrRoutes(t *testing.T) {
	server, listener := v2LoopbackListener(t, "127.0.0.1:0", false)
	conn, err := net.DialUDP("udp4", nil, server.listenerSocket.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	key, err := wirev2.DeriveHandshakeKey(server.config.PSK.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 32; i++ {
		packet, err := wirev2.AppendEnvelope(nil, wirev2.Header{Type: wirev2.TypeHello, SessionID: wirev2.SessionID{byte(i)}}, make([]byte, wirev2.HandshakeBodySize), key)
		if err != nil {
			t.Fatal(err)
		}
		packet[len(packet)-1] ^= 1
		if _, err := conn.Write(packet); err != nil {
			t.Fatal(err)
		}
		if err := receiveDiagnosticWithin(t, server); !errors.Is(err, ErrAuthentication) {
			t.Fatalf("corrupt packet diagnostic = %v", err)
		}
		v2AssertReleased(t, server)
	}
	v2AssertNoAccept(t, listener)
	client, initiator, accepted := v2LoopbackDial(t, server, listener, []string{"127.0.0.1"}, false)
	v2ExchangeOwned(t, initiator, accepted, "after corrupt")
	v2CloseAndAssertReleased(t, client, server)
}

func TestV2AdmissionSessionLimitAndReuseAfterClose(t *testing.T) {
	cfg := v2LoopbackConfig(false)
	cfg.Limits.MaxSessions = 1
	server, listener := v2LoopbackListenerConfig(t, "127.0.0.1:0", cfg)
	client, first := v2AdmissionClient(t, server, cfg)
	accepted := v2AcceptReady(t, listener, first)
	before := v2Usage(client)
	if extra, err := client.NewSession(); extra != nil || !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("initiator exceeded MaxSessions: %v, %v", extra, err)
	}
	if after := v2Usage(client); after != before {
		t.Fatalf("failed local admission changed credits: before %+v after %+v", before, after)
	}
	other, rejected := v2AdmissionClient(t, server, v2LoopbackConfig(false))
	if err := receiveDiagnosticWithin(t, server); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("listener session limit diagnostic: %v", err)
	}
	v2AssertNoAccept(t, listener)
	if usage := v2Usage(server); usage.SessionSlots != 1 || usage.EstablishedSessions != 1 || usage.PendingHandshakes != 0 || usage.PendingAccepts != 0 {
		t.Fatalf("listener exceeded MaxSessions: %+v", usage)
	}
	if err := rejected.Close(); err != nil {
		t.Fatal(err)
	}
	if err := accepted.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	v2AssertReleased(t, server)
	v2AssertReleased(t, client)
	second, err := client.NewSession()
	if err != nil {
		t.Fatalf("reusing released Session slot: %v", err)
	}
	next := second.(DatagramSession)
	newAccepted := v2AcceptReady(t, listener, next)
	v2ExchangeOwned(t, next, newAccepted, "reused slot")
	v2CloseAndAssertReleased(t, other, client, server)
}

func TestV2AdmissionPendingAcceptLimitReleasesOnAccept(t *testing.T) {
	cfg := v2LoopbackConfig(false)
	cfg.Limits.MaxSessions, cfg.Limits.MaxPendingAccepts = 3, 1
	server, listener := v2LoopbackListenerConfig(t, "127.0.0.1:0", cfg)
	firstPeer, first := v2AdmissionClient(t, server, v2LoopbackConfig(false))
	v2WaitReady(t, first, 1)
	if usage := v2Usage(server); usage.PendingAccepts != 1 || usage.EstablishedSessions != 1 {
		t.Fatalf("unaccepted Session lacks reserved accept credit: %+v", usage)
	}
	secondPeer, rejected := v2AdmissionClient(t, server, v2LoopbackConfig(false))
	if err := receiveDiagnosticWithin(t, server); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("pending accept limit diagnostic: %v", err)
	}
	if usage := v2Usage(server); usage.PendingAccepts != 1 || usage.SessionSlots != 1 {
		t.Fatalf("exceeded pending accept credits: %+v", usage)
	}
	if err := rejected.Close(); err != nil {
		t.Fatal(err)
	}
	firstAccepted := v2AcceptReady(t, listener, first)
	if usage := v2Usage(server); usage.PendingAccepts != 0 || usage.SessionSlots != 1 {
		t.Fatalf("Accept did not release only its pending accept credit: %+v", usage)
	}
	second, err := secondPeer.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	secondAccepted := v2AcceptReady(t, listener, second.(DatagramSession))
	if usage := v2Usage(server); usage.PendingAccepts != 0 || usage.EstablishedSessions != 2 || usage.SessionSlots != 2 {
		t.Fatalf("released accept slot was not reusable: %+v", usage)
	}
	v2ExchangeOwned(t, first, firstAccepted, "first accepted")
	v2ExchangeOwned(t, second.(DatagramSession), secondAccepted, "second accepted")
	v2CloseAndAssertReleased(t, firstPeer, secondPeer, server)
}

func TestV2AdmissionAsymmetricSendReceiveLimitsExchange(t *testing.T) {
	serverConfig := v2LoopbackConfig(true)
	serverConfig.Transport.MaxUDPPayload, serverConfig.Transport.MaxReceiveUDPPayload = 1000, 1400
	server, listener := v2LoopbackListenerConfig(t, "127.0.0.1:0", serverConfig)
	clientConfig := v2LoopbackConfig(true)
	clientConfig.Transport.MaxUDPPayload, clientConfig.Transport.MaxReceiveUDPPayload = 1200, 900
	client, initiator := v2AdmissionClient(t, server, clientConfig)
	accepted := v2AcceptReady(t, listener, initiator)
	forward := v2SessionSnapshot(t, initiator).Paths[0]
	reverse := v2SessionSnapshot(t, accepted).Paths[0]
	if forward.SendBudget != 1200 || forward.ReceiveBudget != 900 || reverse.SendBudget != 900 || reverse.ReceiveBudget != 1200 {
		t.Fatalf("directional budgets were conflated: initiator %+v listener %+v", forward, reverse)
	}
	v2ExchangeOwned(t, initiator, accepted, "larger receive")
	v2ExchangeOwned(t, accepted, initiator, "smaller receive")
	if server.statistics.listener.ReceiveOversizeDrops.Load() != 0 || client.statistics.carriers[0].ReceiveOversizeDrops.Load() != 0 {
		t.Fatal("valid directional packets hit the wrong transport receive limit")
	}
	v2CloseAndAssertReleased(t, client, server)
}

func TestV2AdmissionClosePendingDialReleasesAllState(t *testing.T) {
	sink, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	for _, scope := range []string{"session", "peer"} {
		t.Run(scope, func(t *testing.T) {
			cfg := v2LoopbackConfig(false)
			cfg.Carriers = []string{sink.LocalAddr().String()}
			peer, err := NewPeer(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = peer.Close() })
			session, err := peer.NewSession()
			if err != nil {
				t.Fatal(err)
			}
			if usage := v2Usage(peer); usage.SessionSlots != 1 || usage.PendingHandshakes != 1 || usage.Bytes == 0 || usage.Reservations == 0 {
				t.Fatalf("pending dial did not retain its initial credits: %+v", usage)
			}
			if scope == "session" {
				if err := session.Close(); err != nil {
					t.Fatal(err)
				}
				v2AssertReleased(t, peer)
			}
			v2CloseAndAssertReleased(t, peer)
			closed := session.(*v2Session)
			closed.owner.mu.Lock()
			retainedDelivery := closed.delivery != nil
			closed.owner.mu.Unlock()
			if retainedDelivery {
				t.Fatal("closed Session retained delivery backing after releasing its initial credits")
			}
			if err := session.WritePacket([]byte("late")); !errors.Is(err, ErrClosed) {
				t.Fatalf("closed pending dial WritePacket = %v", err)
			}
		})
	}
}

func TestV2AdmissionStartupResourceFailureHasNoDependencies(t *testing.T) {
	cfg := v2LoopbackConfig(false)
	cfg.Listen, cfg.Carriers = "127.0.0.1:9", []string{"127.0.0.1:10"}
	cfg.Limits.ReceiveQueueCapacity = 65536
	cfg.Limits.MaxPeerRetainedBytes, cfg.Limits.MaxSessionRetainedBytes = 1<<20, 1<<20
	cfg.Limits.MaxMigrationTransactionBytes, cfg.Repair.MaxCachedBytes = 1<<20, 1<<20
	if err := cfg.Validate(); err != nil {
		t.Fatalf("resource-failure fixture must be valid configuration: %v", err)
	}
	deps := defaultRuntimeDependencies()
	deps.openCarrier = func(context.Context, string, string, transport.CarrierOptions) (runtimeCarrier, error) {
		t.Fatal("resource failure opened a Carrier")
		return nil, errors.New("unexpected Carrier")
	}
	deps.openListener = func(context.Context, string, string, transport.ListenerOptions) (runtimePacketListener, error) {
		t.Fatal("resource failure opened a Listener")
		return nil, errors.New("unexpected Listener")
	}
	deps.newTimer = func() runtimeDeadlineTimer {
		t.Fatal("resource failure created a runtime timer")
		return nil
	}
	peer, err := newPeerWithDependencies(cfg, unusedPeerRandom{}, deps)
	if peer != nil || !errors.Is(err, ErrResourceLimit) || errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("startup resource failure = %v, %v", peer, err)
	}
}

func TestV2AdmissionWrongRoleHelloCannotCreateListenerState(t *testing.T) {
	server, serverListener := v2LoopbackListener(t, "127.0.0.1:0", false)
	cfg := v2LoopbackConfig(false)
	cfg.Carriers = []string{server.listenerSocket.LocalAddr().String()}
	dual, dualListener := v2LoopbackListenerConfig(t, "127.0.0.1:0", cfg)
	public, err := dual.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	initiator := public.(DatagramSession)
	accepted := v2AcceptReady(t, serverListener, initiator)
	before := v2Usage(dual)
	profile, err := v2ControllerConfig(dual.config, false)
	if err != nil {
		t.Fatal(err)
	}
	tlv, err := (negotiationv2.Advertisement{Profile: profile.LocalProfile, BootstrapPathID: 1}).TLVs()
	if err != nil {
		t.Fatal(err)
	}
	key, err := wirev2.DeriveHandshakeKey(dual.config.PSK.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	hello, err := wirev2.AppendHandshake(nil, wirev2.Handshake{
		Header:      wirev2.Header{Type: wirev2.TypeHello, SessionID: wirev2.SessionID{0xf0}},
		ClientNonce: wirev2.Nonce{0x91}, TLVs: tlv,
	}, key)
	if err != nil {
		t.Fatal(err)
	}
	server.v2.mu.Lock()
	path := server.v2.routes[accepted.(*v2Session).id].path
	server.v2.mu.Unlock()
	if path == nil {
		t.Fatal("established responder has no retained native reply path")
	}
	ctx, cancel := context.WithTimeout(context.Background(), runtimeTestTimeout)
	defer cancel()
	if err := path.Send(ctx, hello); err != nil {
		t.Fatal(err)
	}
	v2AssertNoAccept(t, dualListener)
	if after := v2Usage(dual); after != before {
		t.Fatalf("HELLO on an outbound Carrier allocated listener credits: before %+v after %+v", before, after)
	}
	dual.v2.mu.Lock()
	pending, routes, established := dual.v2.engine.Snapshot().Pending, len(dual.v2.routes), len(dual.v2.established)
	dual.v2.mu.Unlock()
	if pending != 0 || routes != 1 || established != 1 {
		t.Fatalf("wrong-role HELLO retained state: pending %d routes %d established %d", pending, routes, established)
	}
	v2ExchangeOwned(t, initiator, accepted, "original role")
	v2CloseAndAssertReleased(t, dual, server)
}

func TestV2AdmissionBlockedSecondDialDoesNotBlockExistingSession(t *testing.T) {
	server, listener := v2LoopbackListener(t, "127.0.0.1:0", false)
	cfg := v2LoopbackConfig(false)
	cfg.Carriers = []string{server.listenerSocket.LocalAddr().String()}
	started, release := make(chan struct{}), make(chan struct{})
	var opens atomic.Int32
	deps := defaultRuntimeDependencies()
	deps.openCarrier = func(ctx context.Context, id, remote string, options transport.CarrierOptions) (runtimeCarrier, error) {
		if opens.Add(1) == 2 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return transport.OpenCarrier(ctx, id, remote, options)
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
	first := public.(DatagramSession)
	accepted := v2AcceptReady(t, listener, first)
	type dialResult struct {
		session Session
		err     error
	}
	second := make(chan dialResult, 1)
	go func() {
		session, err := client.NewSession()
		second <- dialResult{session, err}
	}()
	select {
	case <-started:
	case <-time.After(runtimeTestTimeout):
		t.Fatal("second Carrier startup did not reach the blocking opener")
	}
	payload := []byte("existing Session remains usable during unrelated Carrier startup")
	if err := accepted.WritePacket(payload); err != nil {
		t.Fatal(err)
	}
	if got := readWithin(t, first); !bytes.Equal(got, payload) {
		t.Fatal("blocked Carrier startup prevented existing Session receive")
	}
	closed := make(chan error, 1)
	go func() { closed <- first.Close() }()
	if err := receiveErrorWithin(t, closed); err != nil {
		t.Fatalf("existing Session.Close during unrelated startup: %v", err)
	}
	close(release)
	select {
	case result := <-second:
		if result.err != nil || result.session == nil {
			t.Fatalf("resumed second startup = %v, %v", result.session, result.err)
		}
		if err := result.session.Close(); err != nil {
			t.Fatal(err)
		}
	case <-time.After(runtimeTestTimeout):
		t.Fatal("second Carrier startup did not finish after release")
	}
	v2CloseAndAssertReleased(t, client, server)
}

func TestV2AdmissionPeerCloseCancelsAndJoinsCarrierStartup(t *testing.T) {
	for _, lateSocket := range []bool{false, true} {
		name := "canceled opener"
		if lateSocket {
			name = "late socket"
		}
		t.Run(name, func(t *testing.T) {
			entered := make(chan struct{})
			lateCarrier := &testRuntimeCarrier{id: "carrier-0"}
			deps := defaultRuntimeDependencies()
			deps.openCarrier = func(ctx context.Context, _, _ string, _ transport.CarrierOptions) (runtimeCarrier, error) {
				close(entered)
				<-ctx.Done()
				if lateSocket {
					return lateCarrier, nil
				}
				return nil, ctx.Err()
			}
			cfg := v2LoopbackConfig(false)
			cfg.Carriers = []string{"127.0.0.1:9001"}
			peer, err := newPeerWithDependencies(cfg, rand.Reader, deps)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = peer.Close() })
			dialDone := make(chan error, 1)
			go func() {
				session, err := peer.NewSession()
				if session != nil {
					err = errors.New("canceled startup returned a Session")
				}
				dialDone <- err
			}()
			select {
			case <-entered:
			case <-time.After(runtimeTestTimeout):
				t.Fatal("Carrier startup did not reach the blocking opener")
			}
			usage := v2Usage(peer)
			if usage.SessionSlots != 1 || usage.PendingHandshakes != 1 || usage.Bytes == 0 {
				t.Fatalf("startup lacks its reserved obligation: %+v", usage)
			}
			closeDone := make(chan error, 1)
			go func() { closeDone <- peer.Close() }()
			if err := receiveErrorWithin(t, closeDone); err != nil {
				t.Fatalf("Peer.Close during Carrier startup: %v", err)
			}
			v2AssertReleased(t, peer)
			if lateSocket {
				lateCarrier.mu.Lock()
				closeCalls := lateCarrier.closeCall
				lateCarrier.mu.Unlock()
				if closeCalls != 1 {
					t.Fatalf("Peer.Close returned before late Carrier disposal: calls %d", closeCalls)
				}
			}
			if err := receiveErrorWithin(t, dialDone); !errors.Is(err, ErrClosed) && !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled NewSession: %v", err)
			}
		})
	}
}

func TestV2AdmissionRefusedFirstCarrierFallsBackWithoutRenumbering(t *testing.T) {
	server, listener := v2LoopbackListener(t, "127.0.0.1:0", false)
	unavailable, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	refused := unavailable.LocalAddr().String()
	if err := unavailable.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := v2LoopbackConfig(false)
	cfg.Carriers = []string{refused, server.listenerSocket.LocalAddr().String()}
	client, err := NewPeer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	public, err := client.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	initiator := public.(DatagramSession)
	accepted := v2AcceptReady(t, listener, initiator)
	for _, session := range []DatagramSession{initiator, accepted} {
		paths := v2SessionSnapshot(t, session).Paths
		if len(paths) < 2 || paths[0].Active || !paths[1].Active || paths[1].PathID != 2 {
			t.Fatalf("healthy winning Carrier was lost or renumbered: %+v", paths)
		}
	}
	v2ExchangeOwned(t, initiator, accepted, "healthy second")
	v2ExchangeOwned(t, accepted, initiator, "healthy reverse")
	v2CloseAndAssertReleased(t, client, server)
}
