package sessionv2

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/negotiationv2"
	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func TestOwnedMultiSlotSendProgressAtFullCredit(t *testing.T) {
	for _, pressure := range []string{"session", "peer"} {
		t.Run(pressure, func(t *testing.T) {
			payload := bytes.Repeat([]byte{91}, 10000)
			var limit uint64
			p := newOwnedPair(t, 3, func(role negotiationv2.Role, cfg Config, limits *creditv2.Limits) {
				if role != negotiationv2.Initiator {
					return
				}
				claims, err := RequiredInitialClaims(cfg)
				if err != nil {
					t.Fatal(err)
				}
				for _, claim := range claims {
					limit += claim.Bytes
				}
				limit += uint64(len(payload))
				limits.MaxSessionBytes, limits.MaxPeerBytes = limit, limit+1024
				if pressure == "peer" {
					limits.MaxSessionBytes = limits.MaxPeerBytes
				}
			})
			p.pump(t, p.ready)
			c := p.client.controller
			if pressure == "peer" {
				other, lease, err := p.client.peer.BeginSession(creditv2.Claim{Bytes: 1024})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(other.Close)
				t.Cleanup(lease.Release)
			}
			var held []*creditv2.Lease
			t.Cleanup(func() {
				for _, lease := range held {
					lease.Release()
				}
			})
			emit := c.cfg.Emit
			c.cfg.Emit = func(path transport.ReplyPath, packet []byte) error {
				if wirev2.PacketType(packet[5]) == wirev2.TypeFECBundle {
					if free := limit - c.setup.Scope.Snapshot().Bytes; free > 0 {
						lease, err := c.setup.Scope.Reserve(creditv2.Claim{Bytes: free})
						if err != nil {
							t.Fatal(err)
						}
						held = append(held, lease)
					}
				}
				return emit(path, packet)
			}
			if _, _, err := c.Write(p.now, payload); err != nil || c.outThrough != 0 || c.queue.Snapshot().RetainedBytes != uint64(len(payload)) || c.setup.Scope.Snapshot().Bytes != limit {
				t.Fatalf("partial original did not fill available credit: %v", err)
			}
			p.pump(t, func() bool { return c.completed == 1 && len(p.server.deliveries) == 1 })
			if !bytes.Equal(p.server.deliveries[0].Payload(), payload) || c.sticky != nil || c.setup.Scope.Snapshot().Bytes != limit || c.PendingSends() != 0 {
				t.Fatal("owned send progress required spare admission credit")
			}
		})
	}
}

func TestOwnedPartialOriginalFailureKeepsFenceBoundary(t *testing.T) {
	p := newOwnedPair(t, 3, nil)
	p.pump(t, p.ready)
	p.now = p.now.Add(time.Millisecond)
	c := p.client.controller
	if _, _, err := c.Write(p.now, bytes.Repeat([]byte{2}, 10000)); err != nil {
		t.Fatal(err)
	}
	if c.outThrough != 0 {
		t.Fatal("partial original advanced group frontier")
	}
	a, b, d := takeData(t, p), takeData(t, p), takeData(t, p)
	completeIntent(t, p, d, io.ErrClosedPipe)
	completeIntent(t, p, a, nil)
	completeIntent(t, p, b, nil)
	if c.completed != 0 || c.failedFrom != 1 || !errors.Is(c.sticky, io.ErrClosedPipe) {
		t.Fatal("partial failure advanced or lost the original boundary")
	}
	p.pump(t, func() bool { return c.completed == 1 })
	if c.failedFrom != 1 || !errors.Is(c.sticky, io.ErrClosedPipe) {
		t.Fatal("later successful groups replaced the first failure")
	}
}

func TestOwnedFullCapacityConstructionAndInvalidLimits(t *testing.T) {
	cfg := configFor(profile(3), false)
	ownedConfig(&cfg)
	setup, cfg, peer := scratchSetup(t, cfg, 0)
	cfg.Emit = nil
	before := peer.Snapshot()
	c, err := New(setup, cfg)
	if err != nil || peer.Snapshot() != before || len(c.sends.slots) != 3 {
		t.Fatalf("owned construction reserved late or required Emit: %v", err)
	}
	c.Close()
	if peer.Snapshot().Bytes != 0 {
		t.Fatal("constructor cleanup retained prepaid bytes")
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.MaxInFlightSends = 0 },
		func(c *Config) { c.MaxInFlightSends = 33 },
		func(c *Config) { c.MaxPathQueuedPackets = 0 },
		func(c *Config) { c.MaxPathQueuedBytes = 511 },
		func(c *Config) { c.MaxQueueResidence = 0 },
		func(c *Config) { c.MaxQueueResidence = 101 * time.Millisecond },
	} {
		bad := cfg
		change(&bad)
		if _, err := RequiredInitialClaims(bad); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid owned limit accepted")
		}
	}
	cfg = configFor(profile(1), false)
	ownedConfig(&cfg)
	cfg.MaxPathQueuedBytes = 1199
	setup, cfg, _ = scratchSetup(t, cfg, 0)
	if c, err := New(setup, cfg); c != nil || !errors.Is(err, ErrInvalid) {
		t.Fatal("negotiated DATA cannot fit path queue allowance")
	}
}
