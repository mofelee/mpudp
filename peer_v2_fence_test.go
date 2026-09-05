//go:build linux

package mpudp

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mofelee/mpudp/config"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
)

type v2Gate struct {
	seen      atomic.Int64
	failAfter atomic.Int64
	failure   error
	stall     atomic.Bool
	started   chan struct{}
}

func (g *v2Gate) send(ctx context.Context, path transport.ReplyPath, packet []byte) error {
	if len(packet) > 5 && wirev2.PacketType(packet[5]) == wirev2.TypeFECBundle {
		count := g.seen.Add(1)
		if g.stall.Load() {
			select {
			case g.started <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		}
		if threshold := g.failAfter.Load(); threshold >= 0 && count > threshold {
			return g.failure
		}
	}
	return path.Send(ctx, packet)
}

type v2GatedReply struct {
	transport.ReplyPath
	gate *v2Gate
}

func (p *v2GatedReply) Send(ctx context.Context, packet []byte) error {
	return p.gate.send(ctx, p.ReplyPath, packet)
}

type v2GatedCarrier struct {
	*v2GatedReply
	carrier *transport.Carrier
}

func (p *v2GatedCarrier) Close() error { return p.carrier.Close() }

func v2FencePair(t *testing.T, gate *v2Gate, rate int64) (*Peer, DatagramSession, DatagramSession) {
	t.Helper()
	server, listener := v2LoopbackListener(t, "127.0.0.1:0", true)
	cfg := v2LoopbackConfig(true)
	cfg.Carriers = []string{server.listenerSocket.LocalAddr().String()}
	cfg.Scheduler.OutboundPathRatesBPS = map[int]int64{1: rate}
	deps := defaultRuntimeDependencies()
	deps.openCarrier = func(ctx context.Context, id, remote string, options transport.CarrierOptions) (runtimeCarrier, error) {
		receive := options.OnPacket
		options.OnPacket = func(packet transport.ReceivedPacket) {
			packet.Reply = &v2GatedReply{ReplyPath: packet.Reply, gate: gate}
			receive(packet)
		}
		carrier, err := transport.OpenCarrier(ctx, id, remote, options)
		if err != nil {
			return nil, err
		}
		return &v2GatedCarrier{v2GatedReply: &v2GatedReply{ReplyPath: carrier, gate: gate}, carrier: carrier}, nil
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
	v2WaitReady(t, sender, 1)
	v2WaitReady(t, receiver, 1)
	return client, sender, receiver
}

func TestV2FlushFailureSurvivesFullDiagnostics(t *testing.T) {
	failure := errors.New("injected send failure")
	gate := &v2Gate{failure: failure}
	client, sender, _ := v2FencePair(t, gate, config.DefaultPathRateBPS)
	client.diagnostics <- errors.New("occupied diagnostic slot")
	if err := sender.WritePacket([]byte("admitted whole original")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), v2LoopbackTimeout)
	defer cancel()
	for i := 0; i < 2; i++ {
		if err := sender.Flush(ctx); !errors.Is(err, failure) {
			t.Fatalf("Flush %d = %v, want sticky failure", i, err)
		}
	}
	if count := gate.seen.Load(); count != 5 {
		t.Fatalf("Flush finished before all five shard attempts: %d", count)
	}
	if err := sender.CloseGracefully(ctx); !errors.Is(err, failure) {
		t.Fatalf("graceful close lost send failure: %v", err)
	}
	if err := sender.CloseGracefully(ctx); !errors.Is(err, failure) {
		t.Fatalf("repeated graceful close lost its result: %v", err)
	}
}

func TestV2CancelledFlushPreservesAdmittedOriginal(t *testing.T) {
	gate := &v2Gate{}
	gate.failAfter.Store(-1)
	_, sender, receiver := v2FencePair(t, gate, 1000000)
	payload := bytes.Repeat([]byte("retained"), 2048)
	if err := sender.WritePacket(payload); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := sender.Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("short Flush = %v, want deadline", err)
	}
	v2Flush(t, sender)
	if got := readWithin(t, receiver); !bytes.Equal(got, payload) {
		t.Fatal("cancelled Flush lost or changed admitted work")
	}
}

func TestV2FlushFrontierExcludesLaterOriginalAndFailure(t *testing.T) {
	gate := &v2Gate{failure: errors.New("later original failed")}
	gate.failAfter.Store(5)
	_, sender, receiver := v2FencePair(t, gate, 1000000)
	if err := sender.WritePacket([]byte("first")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), v2LoopbackTimeout)
	defer cancel()
	flushed := make(chan error, 1)
	go func() { flushed <- sender.Flush(ctx) }()
	deadline := time.Now().Add(time.Second)
	for gate.seen.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Flush did not begin sending its frontier")
		}
		time.Sleep(time.Millisecond)
	}
	if err := sender.WritePacket(bytes.Repeat([]byte("later"), 6000)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-flushed:
		if err != nil {
			t.Fatalf("earlier frontier absorbed a later failure: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("earlier Flush waited for later original")
	}
	if snapshot := v2SessionSnapshot(t, sender); snapshot.CompletedThrough != 1 || snapshot.AcceptedThrough != 2 {
		t.Fatalf("Flush did not preserve its original frontier: %+v", snapshot)
	}
	if got := readWithin(t, receiver); string(got) != "first" {
		t.Fatalf("first delivery = %q", got)
	}
	if err := sender.Flush(ctx); !errors.Is(err, gate.failure) {
		t.Fatalf("later Flush lost failure: %v", err)
	}
}

func TestV2CloseCancelsPendingFlushAndReleasesStorage(t *testing.T) {
	gate := &v2Gate{}
	gate.failAfter.Store(-1)
	client, sender, _ := v2FencePair(t, gate, 1000000)
	if err := sender.WritePacket(bytes.Repeat([]byte("queued"), 9000)); err != nil {
		t.Fatal(err)
	}
	flushed := make(chan error, 1)
	go func() { flushed <- sender.Flush(context.Background()) }()
	if err := sender.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-flushed:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("pending Flush = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close left Flush blocked")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if usage := client.v2.credits.Snapshot(); usage.Bytes != 0 || usage.Reservations != 0 || usage.SessionSlots != 0 {
		t.Fatalf("Close retained owned storage: %+v", usage)
	}
}

func TestV2CloseInterruptsSocketAttempt(t *testing.T) {
	gate := &v2Gate{started: make(chan struct{}, 1)}
	gate.failAfter.Store(-1)
	client, sender, _ := v2FencePair(t, gate, config.DefaultPathRateBPS)
	gate.stall.Store(true)
	if err := sender.WritePacket([]byte("stalled")); err != nil {
		t.Fatal(err)
	}
	flushed := make(chan error, 1)
	go func() { flushed <- sender.Flush(context.Background()) }()
	select {
	case <-gate.started:
	case <-time.After(time.Second):
		t.Fatal("socket attempt did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- sender.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not interrupt the socket attempt")
	}
	select {
	case err := <-flushed:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("interrupted Flush = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("interrupted Flush did not exit")
	}
	if usage := client.v2.credits.Snapshot(); usage.Bytes != 0 || usage.SessionSlots != 0 {
		t.Fatalf("socket interruption retained state: %+v", usage)
	}
}
