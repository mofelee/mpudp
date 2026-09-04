package mpudp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mofelee/mpudp/config"
	"github.com/mofelee/mpudp/internal/fec"
	internalsession "github.com/mofelee/mpudp/internal/session"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wire"
	"go.uber.org/goleak"
)

const runtimeTestTimeout = 3 * time.Second

func TestPeerLoopbackBidirectionalDatagrams(t *testing.T) {
	listenerAddress := reserveUDPAddress(t)
	listenerConfig := runtimeTestConfig()
	listenerConfig.Listen = listenerAddress
	listenerPeer, err := NewPeer(listenerConfig)
	if err != nil {
		t.Fatalf("NewPeer(listener) error = %v", err)
	}
	defer listenerPeer.Close()
	publicListener, err := listenerPeer.Listener()
	if err != nil {
		t.Fatalf("Listener() error = %v", err)
	}

	initiatorConfig := runtimeTestConfig()
	initiatorConfig.Carriers = []string{listenerAddress}
	initiatorPeer, err := NewPeer(initiatorConfig)
	if err != nil {
		t.Fatalf("NewPeer(initiator) error = %v", err)
	}
	defer initiatorPeer.Close()
	initiator, err := initiatorPeer.NewSession()
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	acceptContext, cancelAccept := context.WithTimeout(context.Background(), runtimeTestTimeout)
	defer cancelAccept()
	accepted, err := publicListener.Accept(acceptContext)
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}

	initiatorPayloads := [][]byte{[]byte("one"), bytes.Repeat([]byte{0xa5}, 2048), {}}
	for _, payload := range initiatorPayloads {
		writeEventually(t, initiator, payload)
		got := readWithin(t, accepted)
		if len(payload) == 0 && got == nil {
			t.Fatal("empty Datagram was delivered as a nil slice")
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("listener ReadPacket() = %x, want %x", got, payload)
		}
	}

	listenerPayloads := [][]byte{[]byte("reverse-one"), bytes.Repeat([]byte{0x5a}, 1025)}
	for _, payload := range listenerPayloads {
		writeEventually(t, accepted, payload)
		if got := readWithin(t, initiator); !bytes.Equal(got, payload) {
			t.Fatalf("initiator ReadPacket() = %x, want %x", got, payload)
		}
	}
}

func TestPeerDualModeRunsBothDirections(t *testing.T) {
	addressA := reserveUDPAddress(t)
	addressB := reserveUDPAddress(t)
	configA := runtimeTestConfig()
	configA.Listen = addressA
	configA.Carriers = []string{addressB}
	configB := runtimeTestConfig()
	configB.Listen = addressB
	configB.Carriers = []string{addressA}

	peerA, err := NewPeer(configA)
	if err != nil {
		t.Fatalf("NewPeer(A) error = %v", err)
	}
	defer peerA.Close()
	peerB, err := NewPeer(configB)
	if err != nil {
		t.Fatalf("NewPeer(B) error = %v", err)
	}
	defer peerB.Close()
	if peerA.Mode() != ModeDual || peerB.Mode() != ModeDual {
		t.Fatalf("modes = %q/%q, want dual/dual", peerA.Mode(), peerB.Mode())
	}

	listenerA, _ := peerA.Listener()
	listenerB, _ := peerB.Listener()
	outboundA, err := peerA.NewSession()
	if err != nil {
		t.Fatalf("A NewSession() error = %v", err)
	}
	outboundB, err := peerB.NewSession()
	if err != nil {
		t.Fatalf("B NewSession() error = %v", err)
	}
	ctxA, cancelA := context.WithTimeout(context.Background(), runtimeTestTimeout)
	inboundA, err := listenerA.Accept(ctxA)
	cancelA()
	if err != nil {
		t.Fatalf("A Accept() error = %v", err)
	}
	ctxB, cancelB := context.WithTimeout(context.Background(), runtimeTestTimeout)
	inboundB, err := listenerB.Accept(ctxB)
	cancelB()
	if err != nil {
		t.Fatalf("B Accept() error = %v", err)
	}

	writeEventually(t, outboundA, []byte("A-to-B"))
	if got := string(readWithin(t, inboundB)); got != "A-to-B" {
		t.Fatalf("B inbound = %q, want A-to-B", got)
	}
	writeEventually(t, inboundB, []byte("B-reply"))
	if got := string(readWithin(t, outboundA)); got != "B-reply" {
		t.Fatalf("A outbound reply = %q, want B-reply", got)
	}

	writeEventually(t, outboundB, []byte("B-to-A"))
	if got := string(readWithin(t, inboundA)); got != "B-to-A" {
		t.Fatalf("A inbound = %q, want B-to-A", got)
	}
}

func TestPeerNegotiatesPayloadAndRejectsOversizeBeforeSend(t *testing.T) {
	const peerCapability = 1000
	key := []byte("negotiation-test-key")
	carrier := &handshakeRuntimeCarrier{psk: key, peerCapability: peerCapability}
	deps := defaultRuntimeDependencies()
	deps.openCarrier = func(_ context.Context, id, _ string, options transport.CarrierOptions) (runtimeCarrier, error) {
		carrier.id = id
		carrier.onPacket = options.OnPacket
		return carrier, nil
	}

	cfg := runtimeTestConfig()
	cfg.Carriers = []string{"127.0.0.1:9001"}
	cfg.PSK = config.NewSecret(string(key))
	cfg.Transport.MaxUDPPayload = 1200
	peer, err := newPeerWithDependencies(cfg, bytes.NewReader(append([]byte{1}, make([]byte, 15)...)), deps)
	if err != nil {
		t.Fatalf("newPeerWithDependencies() error = %v", err)
	}
	defer peer.Close()
	publicSession, err := peer.NewSession()
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	runtimeSession := publicSession.(*session)
	waitForCondition(t, func() bool {
		return runtimeSession.controller.Snapshot().State == internalsession.StateEstablished
	}, "initiator handshake establishment")
	snapshot := runtimeSession.controller.Snapshot()
	if snapshot.SendMaxUDPPayload != peerCapability || snapshot.ReceiveMaxUDPPayload != peerCapability {
		t.Fatalf("negotiated payloads = send %d receive %d, want %d", snapshot.SendMaxUDPPayload, snapshot.ReceiveMaxUDPPayload, peerCapability)
	}

	carrier.resetPackets()
	effectiveLimit := cfg.FEC.DataShards * (peerCapability - wire.DataShardOverhead)
	if err := publicSession.WritePacket(make([]byte, effectiveLimit+1)); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("oversize WritePacket() error = %v, want ErrMessageTooLarge", err)
	}
	if got := carrier.packetCount(); got != 0 {
		t.Fatalf("oversize WritePacket made %d sends, want zero", got)
	}
	if err := publicSession.WritePacket(make([]byte, effectiveLimit)); err != nil {
		t.Fatalf("exact-limit WritePacket() error = %v", err)
	}
	lengths := carrier.packetLengths()
	if len(lengths) != cfg.FEC.DataShards+cfg.FEC.ParityShards {
		t.Fatalf("exact-limit sends = %d, want %d", len(lengths), cfg.FEC.DataShards+cfg.FEC.ParityShards)
	}
	for index, length := range lengths {
		if length != peerCapability {
			t.Fatalf("packet %d length = %d, want %d", index, length, peerCapability)
		}
	}
}

func TestPeerLoopbackNegotiates1200And1000Bidirectionally(t *testing.T) {
	listenerAddress := reserveUDPAddress(t)
	listenerConfig := runtimeTestConfig()
	listenerConfig.Listen = listenerAddress
	listenerConfig.Transport.MaxUDPPayload = 1000
	listenerPeer, err := NewPeer(listenerConfig)
	if err != nil {
		t.Fatalf("NewPeer(listener) error = %v", err)
	}
	defer listenerPeer.Close()
	publicListener, err := listenerPeer.Listener()
	if err != nil {
		t.Fatalf("Listener() error = %v", err)
	}

	initiatorConfig := runtimeTestConfig()
	initiatorConfig.Carriers = []string{listenerAddress}
	initiatorConfig.Transport.MaxUDPPayload = 1200
	initiatorPeer, err := NewPeer(initiatorConfig)
	if err != nil {
		t.Fatalf("NewPeer(initiator) error = %v", err)
	}
	defer initiatorPeer.Close()
	initiator, err := initiatorPeer.NewSession()
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	acceptContext, cancelAccept := context.WithTimeout(context.Background(), runtimeTestTimeout)
	accepted, err := publicListener.Accept(acceptContext)
	cancelAccept()
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}

	waitForCondition(t, func() bool {
		return initiator.(*session).controller.Snapshot().State == internalsession.StateEstablished
	}, "1200/1000 initiator establishment")
	for name, current := range map[string]Session{"initiator": initiator, "listener": accepted} {
		snapshot := current.(*session).controller.Snapshot()
		if snapshot.SendMaxUDPPayload != 1000 || snapshot.ReceiveMaxUDPPayload != 1000 {
			t.Fatalf("%s negotiated payloads = send %d receive %d, want 1000/1000", name, snapshot.SendMaxUDPPayload, snapshot.ReceiveMaxUDPPayload)
		}
	}

	effectiveLimit := listenerConfig.FEC.DataShards * (1000 - wire.DataShardOverhead)
	forward := bytes.Repeat([]byte{0xa6}, effectiveLimit)
	writeEventually(t, initiator, forward)
	if got := readWithin(t, accepted); !bytes.Equal(got, forward) {
		t.Fatalf("listener received %d bytes, want %d", len(got), len(forward))
	}
	reverse := bytes.Repeat([]byte{0x5c}, effectiveLimit)
	writeEventually(t, accepted, reverse)
	if got := readWithin(t, initiator); !bytes.Equal(got, reverse) {
		t.Fatalf("initiator received %d bytes, want %d", len(got), len(reverse))
	}
}

func TestPeerWritePacketMapsPublicTransportErrors(t *testing.T) {
	tests := []struct {
		name      string
		configure func([]*handshakeRuntimeCarrier)
		want      []error
		dontWant  []error
	}{
		{
			name: "partial",
			configure: func(carriers []*handshakeRuntimeCarrier) {
				carriers[0].setDataError(&transport.PathError{PathID: carriers[0].id, Generation: 1, Operation: "write", Err: errors.New("partial-marker")})
			},
			want:     []error{ErrPartialSend},
			dontWant: []error{ErrAllSendsFailed},
		},
		{
			name: "all failed",
			configure: func(carriers []*handshakeRuntimeCarrier) {
				for _, carrier := range carriers {
					carrier.setDataError(&transport.PathError{PathID: carrier.id, Generation: 1, Operation: "write", Err: errors.New("all-marker")})
				}
			},
			want:     []error{ErrAllSendsFailed},
			dontWant: []error{ErrPartialSend},
		},
		{
			name: "partial with PMTU",
			configure: func(carriers []*handshakeRuntimeCarrier) {
				carriers[0].setDataError(&transport.PathError{
					PathID: carriers[0].id, Generation: 1, Operation: "write",
					Err: errors.Join(transport.ErrPathMTUExceeded, errors.New("pmtu-marker")),
				})
			},
			want:     []error{ErrPartialSend, ErrPathMTUExceeded},
			dontWant: []error{ErrAllSendsFailed},
		},
		{
			name: "no available paths",
			configure: func(carriers []*handshakeRuntimeCarrier) {
				for _, carrier := range carriers {
					carrier.setUnavailable(true)
				}
			},
			want:     []error{ErrNoAvailablePaths},
			dontWant: []error{ErrPartialSend, ErrAllSendsFailed},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, current, carriers := newEstablishedFakeSession(t, 2)
			for _, carrier := range carriers {
				carrier.resetPackets()
			}
			test.configure(carriers)
			err := current.WritePacket([]byte("classified send"))
			if err == nil {
				t.Fatal("WritePacket() error = nil")
			}
			for _, target := range test.want {
				if !errors.Is(err, target) {
					t.Fatalf("WritePacket() error = %v, want errors.Is(%v)", err, target)
				}
			}
			for _, target := range test.dontWant {
				if errors.Is(err, target) {
					t.Fatalf("WritePacket() error = %v, unexpectedly matches %v", err, target)
				}
			}
			if strings.Contains(err.Error(), "marker") {
				t.Fatalf("WritePacket() leaked injected error text: %q", err)
			}
		})
	}
}

func TestBoundedQueuesDropNewest(t *testing.T) {
	runtimeContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &Peer{ctx: runtimeContext, ingress: make(chan ingressEvent, 1)}
	firstEvent := ingressEvent{transportErr: errors.New("first")}
	p.enqueue(firstEvent)
	p.enqueue(ingressEvent{transportErr: errors.New("second")})
	if len(p.ingress) != 1 {
		t.Fatalf("ingress length = %d, want 1", len(p.ingress))
	}
	if got := <-p.ingress; got.transportErr.Error() != "first" {
		t.Fatalf("retained ingress = %v, want first", got.transportErr)
	}

	initDone := make(chan struct{})
	close(initDone)
	current := &session{
		delivery: make(chan []byte, 1),
		done:     make(chan struct{}),
		initDone: initDone,
	}
	current.deliver([]byte("first"))
	current.deliver([]byte("second"))
	if got := string(readWithin(t, current)); got != "first" {
		t.Fatalf("retained delivery = %q, want first", got)
	}

	publicListener := &listener{accept: make(chan Session, 1), done: make(chan struct{})}
	if !publicListener.offer(current) {
		t.Fatal("first Accept offer was rejected")
	}
	if publicListener.offer(current) {
		t.Fatal("second Accept offer was accepted into a full queue")
	}
}

func TestTerminalListenerErrorSurvivesFullIngressQueue(t *testing.T) {
	marker := errors.New("terminal listener marker")
	packetListener := &testRuntimePacketListener{address: udpTestAddress(43000)}
	deps := defaultRuntimeDependencies()
	deps.openListener = func(_ context.Context, _ string, _ string, options transport.ListenerOptions) (runtimePacketListener, error) {
		// NewPeer has not started its worker yet, so this deterministically fills
		// packet ingress before the listener's one terminal read failure arrives.
		options.OnPacket(transport.ReceivedPacket{Payload: []byte("queue filler")})
		options.OnError(&transport.PathError{PathID: "listener", Generation: 1, Operation: "read", Err: marker})
		return packetListener, nil
	}
	cfg := runtimeTestConfig()
	cfg.Listen = "127.0.0.1:43000"
	cfg.Limits.ReceiveQueueCapacity = 1
	peer, err := newPeerWithDependencies(cfg, bytes.NewReader(make([]byte, 16)), deps)
	if err != nil {
		t.Fatalf("newPeerWithDependencies() error = %v", err)
	}
	defer peer.Close()
	waitForCondition(t, func() bool {
		_, listenerErr := peer.Listener()
		return errors.Is(listenerErr, ErrClosed)
	}, "listener closure after saturated terminal failure")
	if got := packetListener.closeCount(); got != 1 {
		t.Fatalf("listener socket Close calls = %d, want 1", got)
	}
}

func TestPeerDeliversDuplicatePacketIDOnlyOnce(t *testing.T) {
	cfg := runtimeTestConfig()
	cfg.Listen = "127.0.0.1:43000"
	cfg.FEC = config.FECConfig{DataShards: 1, ParityShards: 1}
	packetListener := &testRuntimePacketListener{address: udpTestAddress(43000)}
	deps := defaultRuntimeDependencies()
	deps.openListener = func(context.Context, string, string, transport.ListenerOptions) (runtimePacketListener, error) {
		return packetListener, nil
	}
	peer, err := newPeerWithDependencies(cfg, bytes.NewReader(make([]byte, 16)), deps)
	if err != nil {
		t.Fatalf("newPeerWithDependencies() error = %v", err)
	}
	defer peer.Close()
	publicListener, err := peer.Listener()
	if err != nil {
		t.Fatalf("Listener() error = %v", err)
	}

	id := wire.SessionID{0: 0x7d, 15: 1}
	reply := &staticReplyPath{id: "listener/duplicate", local: udpTestAddress(43000), remote: udpTestAddress(43001)}
	hello, err := wire.NewHello(id, 1, 1, uint16(cfg.Transport.MaxUDPPayload))
	if err != nil {
		t.Fatalf("NewHello() error = %v", err)
	}
	encodedHello, err := wire.AppendAuthenticated(nil, hello, cfg.PSK.Bytes(), cfg.Transport.MaxUDPPayload)
	if err != nil {
		t.Fatalf("AppendAuthenticated(HELLO) error = %v", err)
	}
	peer.handleListenerPacket(transport.ReceivedPacket{Payload: encodedHello, Reply: reply})
	acceptContext, cancelAccept := context.WithTimeout(context.Background(), runtimeTestTimeout)
	accepted, err := publicListener.Accept(acceptContext)
	cancelAccept()
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}

	encoder, err := fec.NewEncoder(fec.Params{DataShards: 1, ParityShards: 1}, fec.Budget{
		MaxUDPPayload: cfg.Transport.MaxUDPPayload, DataShardWireOverhead: wire.DataShardOverhead,
		MaxDatagramSize: cfg.Limits.MaxDatagramSize,
	})
	if err != nil {
		t.Fatalf("NewEncoder() error = %v", err)
	}
	payload := []byte("deliver exactly once")
	block, err := encoder.Encode(payload)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	message, err := wire.NewDataShard(id, block.PacketID, 1, 1, 0, uint32(block.OriginalLength), block.Shards[0])
	if err != nil {
		t.Fatalf("NewDataShard() error = %v", err)
	}
	packet, err := wire.AppendAuthenticated(nil, message, cfg.PSK.Bytes(), cfg.Transport.MaxUDPPayload)
	if err != nil {
		t.Fatalf("AppendAuthenticated(DATA_SHARD) error = %v", err)
	}
	received := transport.ReceivedPacket{Payload: packet, Reply: reply}
	peer.handleListenerPacket(received)
	peer.handleListenerPacket(received)
	runtimeSession := accepted.(*session)
	if got := len(runtimeSession.delivery); got != 1 {
		t.Fatalf("delivery queue length after duplicate = %d, want 1", got)
	}
	if got := readWithin(t, accepted); !bytes.Equal(got, payload) {
		t.Fatalf("ReadPacket() = %q, want %q", got, payload)
	}
}

func TestSlowSessionDoesNotBlockAnotherSession(t *testing.T) {
	listenerAddress := reserveUDPAddress(t)
	listenerConfig := runtimeTestConfig()
	listenerConfig.Listen = listenerAddress
	listenerConfig.FEC = config.FECConfig{DataShards: 1, ParityShards: 1}
	listenerConfig.Limits.DeliveryQueueCapacity = 1
	listenerPeer, err := NewPeer(listenerConfig)
	if err != nil {
		t.Fatalf("NewPeer(listener) error = %v", err)
	}
	defer listenerPeer.Close()
	publicListener, _ := listenerPeer.Listener()

	initiatorConfig := runtimeTestConfig()
	initiatorConfig.Carriers = []string{listenerAddress}
	initiatorConfig.FEC = config.FECConfig{DataShards: 1, ParityShards: 1}
	initiatorPeer, err := NewPeer(initiatorConfig)
	if err != nil {
		t.Fatalf("NewPeer(initiator) error = %v", err)
	}
	defer initiatorPeer.Close()

	slowSender, err := initiatorPeer.NewSession()
	if err != nil {
		t.Fatalf("slow NewSession() error = %v", err)
	}
	acceptContext, cancelAccept := context.WithTimeout(context.Background(), runtimeTestTimeout)
	slowReceiver, err := publicListener.Accept(acceptContext)
	cancelAccept()
	if err != nil {
		t.Fatalf("Accept(slow) error = %v", err)
	}
	fastSender, err := initiatorPeer.NewSession()
	if err != nil {
		t.Fatalf("fast NewSession() error = %v", err)
	}
	acceptContext, cancelAccept = context.WithTimeout(context.Background(), runtimeTestTimeout)
	fastReceiver, err := publicListener.Accept(acceptContext)
	cancelAccept()
	if err != nil {
		t.Fatalf("Accept(fast) error = %v", err)
	}

	writeEventually(t, slowSender, []byte("slow-retained"))
	waitForCondition(t, func() bool { return len(slowReceiver.(*session).delivery) == 1 }, "slow Session queue saturation")
	writeEventually(t, slowSender, []byte("slow-dropped"))
	writeEventually(t, fastSender, []byte("fast-delivered"))
	if got := string(readWithin(t, fastReceiver)); got != "fast-delivered" {
		t.Fatalf("fast Session payload = %q, want fast-delivered", got)
	}
	if got := string(readWithin(t, slowReceiver)); got != "slow-retained" {
		t.Fatalf("slow Session retained payload = %q, want slow-retained", got)
	}
}

func TestPeerCarrierErrorsAreIsolated(t *testing.T) {
	peer, current, carriers := newEstablishedFakeSession(t, 2)
	for _, carrier := range carriers {
		carrier.resetPackets()
	}

	temporaryCause := &timeoutRuntimeError{cause: errors.New("temporary-marker")}
	carriers[0].emitError(&transport.PathError{PathID: carriers[0].id, Generation: 1, Operation: "read", Err: temporaryCause})
	diagnostic := receiveDiagnosticWithin(t, peer)
	if !errors.Is(diagnostic, temporaryCause) {
		t.Fatal("temporary diagnostic did not preserve its cause")
	}
	if err := current.WritePacket([]byte("after temporary failure")); err != nil {
		t.Fatalf("WritePacket() after temporary error = %v", err)
	}
	if carriers[0].packetCount() == 0 || carriers[1].packetCount() == 0 {
		t.Fatalf("temporary error removed a Carrier: sends = %d/%d", carriers[0].packetCount(), carriers[1].packetCount())
	}

	for _, carrier := range carriers {
		carrier.resetPackets()
	}
	permanentCause := errors.New("permanent-marker")
	carriers[0].emitError(&transport.PathError{PathID: carriers[0].id, Generation: 1, Operation: "read", Err: permanentCause})
	diagnostic = receiveDiagnosticWithin(t, peer)
	if !errors.Is(diagnostic, permanentCause) {
		t.Fatal("permanent diagnostic did not preserve its cause")
	}
	waitForCondition(t, func() bool {
		return len(current.controller.SendPaths()) == 1
	}, "permanently failed Carrier removal")
	if err := current.WritePacket([]byte("after permanent failure")); err != nil {
		t.Fatalf("WritePacket() with surviving Carrier = %v", err)
	}
	if got := carriers[0].packetCount(); got != 0 {
		t.Fatalf("permanently failed Carrier received %d DATA sends, want zero", got)
	}
	if got := carriers[1].packetCount(); got != runtimeTestConfig().FEC.DataShards+runtimeTestConfig().FEC.ParityShards {
		t.Fatalf("surviving Carrier sends = %d, want all shards", got)
	}
}

func TestOversizeTransportErrorsAreRecoverable(t *testing.T) {
	t.Run("listener", func(t *testing.T) {
		listenerAddress := reserveUDPAddress(t)
		cfg := runtimeTestConfig()
		cfg.Listen = listenerAddress
		peer, err := NewPeer(cfg)
		if err != nil {
			t.Fatalf("NewPeer(listener) error = %v", err)
		}
		defer peer.Close()
		publicListener, _ := peer.Listener()

		connection, err := net.Dial("udp4", listenerAddress)
		if err != nil {
			t.Fatalf("dial listener: %v", err)
		}
		if _, err := connection.Write(make([]byte, cfg.Transport.MaxUDPPayload+1)); err != nil {
			_ = connection.Close()
			t.Fatalf("send oversize UDP packet: %v", err)
		}
		if err := connection.Close(); err != nil {
			t.Fatalf("close oversize sender: %v", err)
		}
		diagnostic := receiveDiagnosticWithin(t, peer)
		if !errors.Is(diagnostic, ErrMessageTooLarge) {
			t.Fatalf("oversize diagnostic = %v, want ErrMessageTooLarge", diagnostic)
		}
		if _, err := peer.Listener(); err != nil {
			t.Fatalf("Listener() after oversize packet = %v", err)
		}

		initiatorConfig := runtimeTestConfig()
		initiatorConfig.Carriers = []string{listenerAddress}
		initiatorPeer, err := NewPeer(initiatorConfig)
		if err != nil {
			t.Fatalf("NewPeer(initiator) error = %v", err)
		}
		defer initiatorPeer.Close()
		if _, err := initiatorPeer.NewSession(); err != nil {
			t.Fatalf("NewSession() error = %v", err)
		}
		acceptContext, cancelAccept := context.WithTimeout(context.Background(), runtimeTestTimeout)
		accepted, err := publicListener.Accept(acceptContext)
		cancelAccept()
		if err != nil {
			t.Fatalf("Accept() after oversize packet = %v", err)
		}
		if accepted == nil {
			t.Fatal("Accept() after oversize packet returned a nil Session")
		}
	})

	t.Run("Carrier", func(t *testing.T) {
		peer, current, carriers := newEstablishedFakeSession(t, 2)
		for _, carrier := range carriers {
			carrier.resetPackets()
		}
		carriers[0].emitError(&transport.PathError{
			PathID: carriers[0].id, Generation: 1, Operation: "read",
			Err: &transport.PayloadSizeError{Size: 1201, Limit: 1200},
		})
		diagnostic := receiveDiagnosticWithin(t, peer)
		if !errors.Is(diagnostic, ErrMessageTooLarge) {
			t.Fatalf("oversize Carrier diagnostic = %v, want ErrMessageTooLarge", diagnostic)
		}
		if got := len(current.controller.SendPaths()); got != 2 {
			t.Fatalf("send paths after oversize Carrier packet = %d, want 2", got)
		}
		if err := current.WritePacket([]byte("after oversize Carrier packet")); err != nil {
			t.Fatalf("WritePacket() after oversize Carrier packet = %v", err)
		}
		if carriers[0].packetCount() == 0 || carriers[1].packetCount() == 0 {
			t.Fatalf("oversize packet disabled a Carrier: sends = %d/%d", carriers[0].packetCount(), carriers[1].packetCount())
		}
	})
}

func TestAcceptQueueSaturationClosesNewSession(t *testing.T) {
	listenerAddress := reserveUDPAddress(t)
	listenerConfig := runtimeTestConfig()
	listenerConfig.Listen = listenerAddress
	listenerConfig.FEC = config.FECConfig{DataShards: 1, ParityShards: 1}
	listenerConfig.Limits.ReceiveQueueCapacity = 1
	listenerConfig.Limits.MaxSessions = 4
	listenerPeer, err := NewPeer(listenerConfig)
	if err != nil {
		t.Fatalf("NewPeer(listener) error = %v", err)
	}
	defer listenerPeer.Close()
	publicListener, _ := listenerPeer.Listener()

	initiatorConfig := runtimeTestConfig()
	initiatorConfig.Carriers = []string{listenerAddress}
	initiatorConfig.FEC = config.FECConfig{DataShards: 1, ParityShards: 1}
	initiatorConfig.Limits.MaxSessions = 4
	initiatorPeer, err := NewPeer(initiatorConfig)
	if err != nil {
		t.Fatalf("NewPeer(initiator) error = %v", err)
	}
	defer initiatorPeer.Close()
	first, err := initiatorPeer.NewSession()
	if err != nil {
		t.Fatalf("first NewSession() error = %v", err)
	}
	waitForCondition(t, func() bool { return len(publicListener.(*listener).accept) == 1 }, "first queued Accept")
	second, err := initiatorPeer.NewSession()
	if err != nil {
		t.Fatalf("second NewSession() error = %v", err)
	}
	waitForCondition(t, func() bool {
		return errors.Is(second.WritePacket([]byte("probe")), ErrClosed)
	}, "drop-newest Session close")
	if got := listenerPeer.listenerState.Stats().Sessions; got != 1 {
		t.Fatalf("listener retained Sessions = %d, want 1", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), runtimeTestTimeout)
	accepted, err := publicListener.Accept(ctx)
	cancel()
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	writeEventually(t, first, []byte("retained"))
	if got := string(readWithin(t, accepted)); got != "retained" {
		t.Fatalf("retained Session payload = %q, want retained", got)
	}
}

func TestGlobalSessionLimitCountsInboundAndOutbound(t *testing.T) {
	addressA := reserveUDPAddress(t)
	addressB := reserveUDPAddress(t)
	configA := runtimeTestConfig()
	configA.Listen = addressA
	configA.Carriers = []string{addressB}
	configA.Limits.MaxSessions = 1
	peerA, err := NewPeer(configA)
	if err != nil {
		t.Fatalf("NewPeer(A) error = %v", err)
	}
	defer peerA.Close()

	configB := runtimeTestConfig()
	configB.Listen = addressB
	peerB, err := NewPeer(configB)
	if err != nil {
		t.Fatalf("NewPeer(B) error = %v", err)
	}
	defer peerB.Close()
	if _, err := peerA.NewSession(); err != nil {
		t.Fatalf("A outbound NewSession() error = %v", err)
	}

	configC := runtimeTestConfig()
	configC.Carriers = []string{addressA}
	peerC, err := NewPeer(configC)
	if err != nil {
		t.Fatalf("NewPeer(C) error = %v", err)
	}
	defer peerC.Close()
	inboundAttempt, err := peerC.NewSession()
	if err != nil {
		t.Fatalf("C NewSession() error = %v", err)
	}
	waitForCondition(t, func() bool {
		return errors.Is(inboundAttempt.WritePacket([]byte("probe")), ErrClosed)
	}, "inbound rejection at global Session limit")
	peerA.mu.RLock()
	retained := len(peerA.sessions)
	inbound := len(peerA.inbound)
	peerA.mu.RUnlock()
	if retained != 1 || inbound != 0 {
		t.Fatalf("A retained Sessions/inbound = %d/%d, want 1/0", retained, inbound)
	}
}

func TestPartialCarrierStartupClosesOpenedSockets(t *testing.T) {
	marker := errors.New("second Carrier open failed")
	first := &testRuntimeCarrier{id: "carrier-0"}
	deps := defaultRuntimeDependencies()
	openCalls := 0
	deps.openCarrier = func(_ context.Context, _ string, _ string, _ transport.CarrierOptions) (runtimeCarrier, error) {
		openCalls++
		if openCalls == 1 {
			return first, nil
		}
		return nil, marker
	}
	cfg := runtimeTestConfig()
	cfg.Carriers = []string{"127.0.0.1:9001", "127.0.0.1:9002"}
	peer, err := newPeerWithDependencies(cfg, bytes.NewReader(append([]byte{1}, make([]byte, 15)...)), deps)
	if err != nil {
		t.Fatalf("newPeerWithDependencies() error = %v", err)
	}
	defer peer.Close()
	if current, err := peer.NewSession(); current != nil || !errors.Is(err, marker) {
		t.Fatalf("NewSession() = (%v, %v), want nil marker", current, err)
	}
	first.mu.Lock()
	closeCalls := first.closeCall
	first.mu.Unlock()
	if closeCalls == 0 {
		t.Fatal("first Carrier was not closed after partial startup")
	}
	peer.mu.RLock()
	retained := len(peer.sessions)
	peer.mu.RUnlock()
	if retained != 0 {
		t.Fatalf("partial startup retained %d Sessions, want zero", retained)
	}
}

func TestPeerCloseStopsWorkerTimerAndSockets(t *testing.T) {
	carrier := &testRuntimeCarrier{id: "carrier-0"}
	packetListener := &testRuntimePacketListener{address: udpTestAddress(43000)}
	timer := &testDeadlineTimer{channel: make(chan time.Time)}
	deps := defaultRuntimeDependencies()
	deps.openCarrier = func(_ context.Context, _ string, _ string, _ transport.CarrierOptions) (runtimeCarrier, error) {
		return carrier, nil
	}
	deps.openListener = func(_ context.Context, _ string, _ string, _ transport.ListenerOptions) (runtimePacketListener, error) {
		return packetListener, nil
	}
	createdTimers := 0
	deps.newTimer = func() runtimeDeadlineTimer {
		createdTimers++
		return timer
	}

	cfg := runtimeTestConfig()
	cfg.Listen = "127.0.0.1:43000"
	cfg.Carriers = []string{"127.0.0.1:43001"}
	peer, err := newPeerWithDependencies(cfg, bytes.NewReader(append([]byte{1}, make([]byte, 15)...)), deps)
	if err != nil {
		t.Fatalf("newPeerWithDependencies() error = %v", err)
	}
	if _, err := peer.NewSession(); err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	waitForCondition(t, func() bool { return timer.resetCount() > 0 }, "deadline timer reset")
	if err := peer.Close(); err != nil {
		t.Fatalf("Peer.Close() error = %v", err)
	}
	select {
	case <-peer.workerDone:
	default:
		t.Fatal("Peer.Close returned before the worker exited")
	}
	if createdTimers != 1 {
		t.Fatalf("created deadline timers = %d, want 1", createdTimers)
	}
	if timer.stopCount() < 2 {
		t.Fatalf("deadline timer Stop calls = %d, want initial and final stops", timer.stopCount())
	}
	carrier.mu.Lock()
	carrierCloseCalls := carrier.closeCall
	carrier.mu.Unlock()
	if carrierCloseCalls == 0 {
		t.Fatal("Peer.Close did not close the Carrier")
	}
	if packetListener.closeCount() != 1 {
		t.Fatalf("listener socket Close calls = %d, want 1", packetListener.closeCount())
	}
}

func TestPeerCloseReleasesListenerSocket(t *testing.T) {
	address := reserveUDPAddress(t)
	cfg := runtimeTestConfig()
	cfg.Listen = address
	peer, err := NewPeer(cfg)
	if err != nil {
		t.Fatalf("NewPeer() error = %v", err)
	}
	if err := peer.Close(); err != nil {
		t.Fatalf("Peer.Close() error = %v", err)
	}
	probe, err := net.ListenPacket("udp4", address)
	if err != nil {
		t.Fatalf("listener address remains bound after Close: %v", err)
	}
	if err := probe.Close(); err != nil {
		t.Fatalf("close listener reuse probe: %v", err)
	}
}

func TestPeerValidLifecycleDoesNotLeakWorkersOrSockets(t *testing.T) {
	ignoreCurrent := goleak.IgnoreCurrent()
	defer goleak.VerifyNone(t, ignoreCurrent)

	listenerAddress := reserveUDPAddress(t)
	listenerConfig := runtimeTestConfig()
	listenerConfig.Listen = listenerAddress
	listenerPeer, err := NewPeer(listenerConfig)
	if err != nil {
		t.Fatalf("NewPeer(listener) error = %v", err)
	}
	defer listenerPeer.Close()
	publicListener, err := listenerPeer.Listener()
	if err != nil {
		t.Fatalf("Listener() error = %v", err)
	}

	initiatorConfig := runtimeTestConfig()
	initiatorConfig.Carriers = []string{listenerAddress}
	initiatorPeer, err := NewPeer(initiatorConfig)
	if err != nil {
		t.Fatalf("NewPeer(initiator) error = %v", err)
	}
	defer initiatorPeer.Close()
	initiator, err := initiatorPeer.NewSession()
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	runtimeInitiator := initiator.(*session)
	runtimeInitiator.mu.Lock()
	runtimeCarrier := runtimeInitiator.carriers[0]
	runtimeInitiator.mu.Unlock()
	carrier, ok := runtimeCarrier.(*transport.Carrier)
	if !ok {
		t.Fatalf("runtime Carrier type = %T, want *transport.Carrier", runtimeCarrier)
	}
	carrierAddress := carrier.LocalAddr().String()

	acceptContext, cancelAccept := context.WithTimeout(context.Background(), runtimeTestTimeout)
	accepted, err := publicListener.Accept(acceptContext)
	cancelAccept()
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	writeEventually(t, initiator, []byte("lifecycle-forward"))
	if got := string(readWithin(t, accepted)); got != "lifecycle-forward" {
		t.Fatalf("forward payload = %q", got)
	}
	writeEventually(t, accepted, []byte("lifecycle-reverse"))
	if got := string(readWithin(t, initiator)); got != "lifecycle-reverse" {
		t.Fatalf("reverse payload = %q", got)
	}

	if err := initiatorPeer.Close(); err != nil {
		t.Fatalf("initiator Peer.Close() error = %v", err)
	}
	if err := listenerPeer.Close(); err != nil {
		t.Fatalf("listener Peer.Close() error = %v", err)
	}
	for name, peer := range map[string]*Peer{"initiator": initiatorPeer, "listener": listenerPeer} {
		select {
		case <-peer.workerDone:
		default:
			t.Fatalf("%s worker remained active after Close", name)
		}
	}
	for name, address := range map[string]string{"listener": listenerAddress, "Carrier": carrierAddress} {
		probe, err := net.ListenPacket("udp4", address)
		if err != nil {
			t.Fatalf("%s socket %q remains bound after Close: %v", name, address, err)
		}
		if err := probe.Close(); err != nil {
			t.Fatalf("close %s socket reuse probe: %v", name, err)
		}
	}
}

func TestCloseBoundsUncooperativePartialStartup(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	lateCarrier := &testRuntimeCarrier{id: "carrier-0"}
	deps := defaultRuntimeDependencies()
	deps.openCarrier = func(context.Context, string, string, transport.CarrierOptions) (runtimeCarrier, error) {
		close(entered)
		<-release
		return lateCarrier, nil
	}
	cfg := runtimeTestConfig()
	cfg.Carriers = []string{"127.0.0.1:9001"}
	peer, err := newPeerWithDependencies(cfg, bytes.NewReader(append([]byte{1}, make([]byte, 15)...)), deps)
	if err != nil {
		t.Fatalf("newPeerWithDependencies() error = %v", err)
	}
	newSessionDone := make(chan error, 1)
	go func() {
		_, startErr := peer.NewSession()
		newSessionDone <- startErr
	}()
	<-entered

	started := time.Now()
	closeErr := peer.Close()
	if elapsed := time.Since(started); elapsed > 2*runtimeCloseTimeout {
		t.Fatalf("Peer.Close blocked for %s on partial startup", elapsed)
	}
	if !errors.Is(closeErr, context.DeadlineExceeded) {
		t.Fatalf("Peer.Close() error = %v, want context deadline", closeErr)
	}
	close(release)
	if err := receiveErrorWithin(t, newSessionDone); !errors.Is(err, ErrClosed) {
		t.Fatalf("NewSession() after late open error = %v, want ErrClosed", err)
	}
	waitForCondition(t, func() bool {
		lateCarrier.mu.Lock()
		defer lateCarrier.mu.Unlock()
		return lateCarrier.closeCall > 0
	}, "late Carrier cleanup")
	if second := peer.Close(); !errors.Is(second, context.DeadlineExceeded) {
		t.Fatalf("second Peer.Close() error = %v, want stable deadline", second)
	}
}

func TestPeerContextCancelsCarrierStartup(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	entered := make(chan struct{})
	deps := defaultRuntimeDependencies()
	deps.openCarrier = func(ctx context.Context, _ string, _ string, _ transport.CarrierOptions) (runtimeCarrier, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	cfg := runtimeTestConfig()
	cfg.Carriers = []string{"127.0.0.1:9001"}
	peer, err := newPeerWithContextAndDependencies(parent, cfg, bytes.NewReader(append([]byte{1}, make([]byte, 15)...)), deps)
	if err != nil {
		t.Fatalf("newPeerWithContextAndDependencies() error = %v", err)
	}
	defer peer.Close()
	result := make(chan error, 1)
	go func() {
		_, startErr := peer.NewSession()
		result <- startErr
	}()
	<-entered
	cancelParent()
	if err := receiveErrorWithin(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("NewSession() error = %v, want context.Canceled", err)
	}
}

func TestRuntimeDiagnosticsAreBoundedRedactedAndObservable(t *testing.T) {
	address := reserveUDPAddress(t)
	cfg := runtimeTestConfig()
	cfg.Listen = address
	peer, err := NewPeer(cfg)
	if err != nil {
		t.Fatalf("NewPeer() error = %v", err)
	}
	defer peer.Close()
	marker := errors.New("PAYLOAD-DO-NOT-LOG-c48e")
	peer.enqueue(ingressEvent{listener: true, transportErr: marker})

	var diagnostic error
	select {
	case diagnostic = <-peer.Errors():
	case <-time.After(runtimeTestTimeout):
		t.Fatal("runtime transport failure produced no diagnostic")
	}
	if strings.Contains(diagnostic.Error(), marker.Error()) {
		t.Fatalf("diagnostic leaked underlying text: %q", diagnostic)
	}
	if !errors.Is(diagnostic, marker) {
		t.Fatal("diagnostic did not preserve its errors.Is cause")
	}
	waitForCondition(t, func() bool {
		_, listenerErr := peer.Listener()
		return errors.Is(listenerErr, ErrClosed)
	}, "listener closure after permanent transport failure")

	peer.reportRuntimeError("first diagnostic", errors.New("first cause"))
	peer.reportRuntimeError("second diagnostic", errors.New("second cause"))
	select {
	case got := <-peer.Errors():
		if got.Error() != "first diagnostic" {
			t.Fatalf("retained diagnostic = %q, want first", got)
		}
	default:
		t.Fatal("bounded diagnostic queue was unexpectedly empty")
	}
}

func TestListenerCloseRejectsAlreadyQueuedPacket(t *testing.T) {
	address := reserveUDPAddress(t)
	cfg := runtimeTestConfig()
	cfg.Listen = address
	peer, err := NewPeer(cfg)
	if err != nil {
		t.Fatalf("NewPeer() error = %v", err)
	}
	defer peer.Close()
	publicListener, _ := peer.Listener()
	if err := publicListener.Close(); err != nil {
		t.Fatalf("Listener.Close() error = %v", err)
	}

	id := wire.SessionID{0: 1}
	hello, err := wire.NewHello(id, uint8(cfg.FEC.DataShards), uint8(cfg.FEC.ParityShards), uint16(cfg.Transport.MaxUDPPayload))
	if err != nil {
		t.Fatalf("NewHello() error = %v", err)
	}
	encoded, err := wire.AppendAuthenticated(nil, hello, cfg.PSK.Bytes(), cfg.Transport.MaxUDPPayload)
	if err != nil {
		t.Fatalf("AppendAuthenticated() error = %v", err)
	}
	reply := &staticReplyPath{id: "listener/queued", local: udpTestAddress(9000), remote: udpTestAddress(9001)}
	peer.handleListenerPacket(transport.ReceivedPacket{Payload: encoded, Reply: reply})
	if got := peer.listenerState.Stats().Sessions; got != 0 {
		t.Fatalf("closed listener recreated %d Sessions, want zero", got)
	}
	if got := len(publicListener.(*listener).accept); got != 0 {
		t.Fatalf("closed listener queued %d accepts, want zero", got)
	}
}

func TestRuntimeDiagnosticsRedactSecretsPayloadsAndSessionIDs(t *testing.T) {
	const secret = "PSK-DO-NOT-LEAK-6f12"
	const payload = "PAYLOAD-DO-NOT-LEAK-a931"
	cfg := runtimeTestConfig()
	cfg.Carriers = []string{"127.0.0.1:9001"}
	cfg.PSK = config.NewSecret(secret)
	id := SessionID{0: 0xde, 1: 0xad, 2: 0xbe, 3: 0xef, 15: 1}
	initDone := make(chan struct{})
	close(initDone)
	peer := &Peer{config: cfg, sessions: make(map[*session]struct{})}
	current := &session{
		id:       id,
		wireID:   internalSessionID(id),
		owner:    peer,
		delivery: make(chan []byte, 1),
		done:     make(chan struct{}),
		initDone: initDone,
	}
	peer.sessions[current] = struct{}{}
	current.deliver([]byte(payload))
	publicListener := &listener{accept: make(chan Session, 1), done: make(chan struct{})}
	publicListener.accept <- current
	cause := errors.New(payload)
	diagnostic := fmt.Sprintf("%v %#v %+v %s %q | %v | %#v | %v", current, current, current, current, current, peer, publicListener, classifyRuntimeError(ErrResourceLimit, cause))
	for _, forbidden := range []string{secret, payload, id.String()} {
		if strings.Contains(diagnostic, forbidden) {
			t.Fatalf("diagnostic leaked %q: %q", forbidden, diagnostic)
		}
	}
	if !errors.Is(classifyRuntimeError(ErrResourceLimit, cause), cause) {
		t.Fatal("redacted classified error did not retain errors.Is cause")
	}
}

func TestPeerMultipleConcurrentSessions(t *testing.T) {
	listenerAddress := reserveUDPAddress(t)
	listenerConfig := runtimeTestConfig()
	listenerConfig.Listen = listenerAddress
	listenerPeer, err := NewPeer(listenerConfig)
	if err != nil {
		t.Fatalf("NewPeer(listener) error = %v", err)
	}
	defer listenerPeer.Close()
	publicListener, err := listenerPeer.Listener()
	if err != nil {
		t.Fatalf("Listener() error = %v", err)
	}

	initiatorConfig := runtimeTestConfig()
	initiatorConfig.Carriers = []string{listenerAddress}
	initiatorPeer, err := NewPeer(initiatorConfig)
	if err != nil {
		t.Fatalf("NewPeer(initiator) error = %v", err)
	}
	defer initiatorPeer.Close()

	const sessionCount = 4
	initiators := make([]Session, sessionCount)
	for index := range initiators {
		initiators[index], err = initiatorPeer.NewSession()
		if err != nil {
			t.Fatalf("NewSession(%d) error = %v", index, err)
		}
	}
	accepted := make([]Session, sessionCount)
	for index := range accepted {
		ctx, cancel := context.WithTimeout(context.Background(), runtimeTestTimeout)
		accepted[index], err = publicListener.Accept(ctx)
		cancel()
		if err != nil {
			t.Fatalf("Accept(%d) error = %v", index, err)
		}
	}

	for index, current := range initiators {
		writeEventually(t, current, []byte(fmt.Sprintf("session-%d", index)))
	}
	acceptedByPayload := make(map[string]Session, sessionCount)
	for _, current := range accepted {
		payload := readWithin(t, current)
		acceptedByPayload[string(payload)] = current
	}
	for index, current := range initiators {
		key := fmt.Sprintf("session-%d", index)
		inbound := acceptedByPayload[key]
		if inbound == nil {
			t.Fatalf("no accepted Session delivered %q", key)
		}
		writeEventually(t, inbound, []byte("reply-"+key))
		if got, want := string(readWithin(t, current)), "reply-"+key; got != want {
			t.Fatalf("reply for Session %d = %q, want %q", index, got, want)
		}
	}
}

func TestPeerConcurrentCloseUnblocksReadAndAccept(t *testing.T) {
	sink, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("open inert UDP sink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	listenerAddress := reserveUDPAddress(t)

	cfg := runtimeTestConfig()
	cfg.Listen = listenerAddress
	cfg.Carriers = []string{sink.LocalAddr().String()}
	peer, err := NewPeer(cfg)
	if err != nil {
		t.Fatalf("NewPeer() error = %v", err)
	}
	current, err := peer.NewSession()
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	publicListener, err := peer.Listener()
	if err != nil {
		t.Fatalf("Listener() error = %v", err)
	}

	readDone := make(chan error, 1)
	go func() {
		_, readErr := current.ReadPacket()
		readDone <- readErr
	}()
	acceptDone := make(chan error, 1)
	go func() {
		_, acceptErr := publicListener.Accept(context.Background())
		acceptDone <- acceptErr
	}()

	const callers = 16
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- peer.Close()
		}()
	}
	wait.Wait()
	close(results)
	for closeErr := range results {
		if closeErr != nil {
			t.Fatalf("concurrent Close() error = %v", closeErr)
		}
	}
	if err := receiveErrorWithin(t, readDone); !errors.Is(err, ErrClosed) {
		t.Fatalf("blocked ReadPacket error = %v, want ErrClosed", err)
	}
	if err := receiveErrorWithin(t, acceptDone); !errors.Is(err, ErrClosed) {
		t.Fatalf("blocked Accept error = %v, want ErrClosed", err)
	}
	if err := peer.Close(); err != nil {
		t.Fatalf("final Close() error = %v", err)
	}
}

func runtimeTestConfig() config.Config {
	cfg := config.Default()
	cfg.FEC = config.FECConfig{DataShards: 3, ParityShards: 2}
	cfg.PSK = config.NewSecret("runtime-test-key")
	cfg.Timers.HandshakeRetryInterval = 100 * time.Millisecond
	return cfg
}

func reserveUDPAddress(t *testing.T) string {
	t.Helper()
	connection, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve UDP address: %v", err)
	}
	address := connection.LocalAddr().String()
	if err := connection.Close(); err != nil {
		t.Fatalf("release UDP address: %v", err)
	}
	return address
}

func writeEventually(t *testing.T, current Session, payload []byte) {
	t.Helper()
	deadline := time.Now().Add(runtimeTestTimeout)
	for {
		err := current.WritePacket(payload)
		if err == nil {
			return
		}
		if !errors.Is(err, ErrNotReady) {
			t.Fatalf("WritePacket() error = %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("WritePacket() remained not ready for %s", runtimeTestTimeout)
		}
		time.Sleep(time.Millisecond)
	}
}

func readWithin(t *testing.T, current Session) []byte {
	t.Helper()
	type result struct {
		payload []byte
		err     error
	}
	resultChannel := make(chan result, 1)
	go func() {
		payload, err := current.ReadPacket()
		resultChannel <- result{payload: payload, err: err}
	}()
	select {
	case received := <-resultChannel:
		if received.err != nil {
			t.Fatalf("ReadPacket() error = %v", received.err)
		}
		return received.payload
	case <-time.After(runtimeTestTimeout):
		t.Fatalf("ReadPacket() did not return within %s", runtimeTestTimeout)
		return nil
	}
}

func receiveErrorWithin(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(runtimeTestTimeout):
		t.Fatalf("operation did not return within %s", runtimeTestTimeout)
		return nil
	}
}

func receiveDiagnosticWithin(t *testing.T, peer *Peer) error {
	t.Helper()
	select {
	case err := <-peer.Errors():
		return err
	case <-time.After(runtimeTestTimeout):
		t.Fatalf("runtime diagnostic did not arrive within %s", runtimeTestTimeout)
		return nil
	}
}

func newEstablishedFakeSession(t *testing.T, carrierCount int) (*Peer, *session, []*handshakeRuntimeCarrier) {
	t.Helper()
	cfg := runtimeTestConfig()
	cfg.Carriers = make([]string, carrierCount)
	carriers := make([]*handshakeRuntimeCarrier, carrierCount)
	for index := range carrierCount {
		cfg.Carriers[index] = fmt.Sprintf("127.0.0.1:%d", 44000+index)
		carriers[index] = &handshakeRuntimeCarrier{psk: cfg.PSK.Bytes(), peerCapability: cfg.Transport.MaxUDPPayload}
	}
	deps := defaultRuntimeDependencies()
	nextCarrier := 0
	deps.openCarrier = func(_ context.Context, id, _ string, options transport.CarrierOptions) (runtimeCarrier, error) {
		carrier := carriers[nextCarrier]
		nextCarrier++
		carrier.id = id
		carrier.onPacket = options.OnPacket
		carrier.onError = options.OnError
		return carrier, nil
	}
	randomBytes := make([]byte, 16)
	randomBytes[0] = 1
	peer, err := newPeerWithDependencies(cfg, bytes.NewReader(randomBytes), deps)
	if err != nil {
		t.Fatalf("newPeerWithDependencies() error = %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	publicSession, err := peer.NewSession()
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	current := publicSession.(*session)
	waitForCondition(t, func() bool {
		snapshot := current.controller.Snapshot()
		return snapshot.State == internalsession.StateEstablished && snapshot.AcknowledgedCarriers == carrierCount
	}, "fake Session handshake establishment on every Carrier")
	return peer, current, carriers
}

func waitForCondition(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(runtimeTestTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(time.Millisecond)
	}
}

func udpTestAddress(port int) *net.UDPAddr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}
}

type testRuntimeCarrier struct {
	mu        sync.Mutex
	id        string
	closed    bool
	closeCall int
	sendCall  int
}

func (c *testRuntimeCarrier) PathID() string {
	if c.id == "" {
		return "test-carrier"
	}
	return c.id
}

func (c *testRuntimeCarrier) Available() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed
}

func (c *testRuntimeCarrier) Send(context.Context, []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return transport.ErrClosed
	}
	c.sendCall++
	return nil
}

func (c *testRuntimeCarrier) Close() error {
	c.mu.Lock()
	c.closed = true
	c.closeCall++
	c.mu.Unlock()
	return nil
}

type handshakeRuntimeCarrier struct {
	mu             sync.Mutex
	id             string
	psk            []byte
	peerCapability int
	onPacket       transport.PacketHandler
	onError        transport.ErrorHandler
	closed         bool
	unavailable    bool
	dataErr        error
	packetSizes    []int
}

func (c *handshakeRuntimeCarrier) PathID() string { return c.id }

func (c *handshakeRuntimeCarrier) Available() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && !c.unavailable
}

func (c *handshakeRuntimeCarrier) Send(_ context.Context, packet []byte) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return transport.ErrClosed
	}
	c.packetSizes = append(c.packetSizes, len(packet))
	handler := c.onPacket
	id := c.id
	key := append([]byte(nil), c.psk...)
	capability := c.peerCapability
	dataErr := c.dataErr
	c.mu.Unlock()

	message, err := wire.DecodeAuthenticated(packet, key, wire.MaxUDPPayload)
	if err != nil {
		return err
	}
	if message.Header.Type == wire.TypeDataShard {
		return dataErr
	}
	if message.Header.Type != wire.TypeHello || handler == nil {
		return nil
	}
	ack, err := wire.NewHelloAck(message.Header.SessionID, message.Handshake.DataShards, message.Handshake.ParityShards, uint16(capability))
	if err != nil {
		return err
	}
	encoded, err := wire.AppendAuthenticated(nil, ack, key, wire.MaxUDPPayload)
	if err != nil {
		return err
	}
	reply := &staticReplyPath{
		id:      id,
		local:   udpTestAddress(41000),
		remote:  udpTestAddress(42000),
		carrier: c,
	}
	handler(transport.ReceivedPacket{
		Payload:    encoded,
		PathID:     id,
		Generation: 1,
		LocalAddr:  reply.local,
		RemoteAddr: reply.remote,
		Reply:      reply,
		Context:    context.Background(),
	})
	return nil
}

func (c *handshakeRuntimeCarrier) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *handshakeRuntimeCarrier) resetPackets() {
	c.mu.Lock()
	c.packetSizes = nil
	c.mu.Unlock()
}

func (c *handshakeRuntimeCarrier) setDataError(err error) {
	c.mu.Lock()
	c.dataErr = err
	c.mu.Unlock()
}

func (c *handshakeRuntimeCarrier) setUnavailable(unavailable bool) {
	c.mu.Lock()
	c.unavailable = unavailable
	c.mu.Unlock()
}

func (c *handshakeRuntimeCarrier) emitError(err error) {
	c.mu.Lock()
	handler := c.onError
	c.mu.Unlock()
	if handler != nil {
		handler(err)
	}
}

func (c *handshakeRuntimeCarrier) packetCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.packetSizes)
}

func (c *handshakeRuntimeCarrier) packetLengths() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.packetSizes...)
}

type staticReplyPath struct {
	id      string
	local   net.Addr
	remote  net.Addr
	carrier *handshakeRuntimeCarrier
}

func (p *staticReplyPath) PathID() string     { return p.id }
func (p *staticReplyPath) Generation() uint64 { return 1 }
func (p *staticReplyPath) LocalAddr() net.Addr {
	return p.local
}
func (p *staticReplyPath) RemoteAddr() net.Addr {
	return p.remote
}
func (p *staticReplyPath) Available() bool {
	return p.carrier == nil || p.carrier.Available()
}
func (p *staticReplyPath) Send(ctx context.Context, payload []byte) error {
	if p.carrier == nil {
		return nil
	}
	return p.carrier.Send(ctx, payload)
}

type testRuntimePacketListener struct {
	mu         sync.Mutex
	address    net.Addr
	closeCalls int
}

func (l *testRuntimePacketListener) Close() error {
	l.mu.Lock()
	l.closeCalls++
	l.mu.Unlock()
	return nil
}

func (l *testRuntimePacketListener) LocalAddr() net.Addr { return l.address }

func (l *testRuntimePacketListener) closeCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closeCalls
}

type testDeadlineTimer struct {
	mu      sync.Mutex
	channel chan time.Time
	resets  int
	stops   int
}

func (t *testDeadlineTimer) Channel() <-chan time.Time { return t.channel }

func (t *testDeadlineTimer) Reset(time.Duration) bool {
	t.mu.Lock()
	t.resets++
	t.mu.Unlock()
	return false
}

func (t *testDeadlineTimer) Stop() bool {
	t.mu.Lock()
	t.stops++
	t.mu.Unlock()
	return true
}

func (t *testDeadlineTimer) resetCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.resets
}

func (t *testDeadlineTimer) stopCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stops
}

type timeoutRuntimeError struct{ cause error }

func (e *timeoutRuntimeError) Error() string { return e.cause.Error() }
func (e *timeoutRuntimeError) Unwrap() error { return e.cause }
func (e *timeoutRuntimeError) Timeout() bool { return true }
func (e *timeoutRuntimeError) Temporary() bool {
	return true
}
