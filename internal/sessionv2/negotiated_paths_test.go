package sessionv2

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/mofelee/mpudp/internal/handshakev2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func TestNegotiatedPathStoragePreservesAsymmetricContract(t *testing.T) {
	for _, counts := range [][2]int{{5, 256}, {256, 5}, {256, 256}} {
		for _, aggregate := range []bool{false, true} {
			t.Run(fmt.Sprintf("client_%d/server_%d/aggregate_%t", counts[0], counts[1], aggregate), func(t *testing.T) {
				paths := min(counts[0], counts[1])
				p := newPairWithProfiles(t, profile(counts[0]), profile(counts[1]), paths, aggregate, nil)
				t.Cleanup(func() { p.close(t) })
				p.pump(t, p.ready)
				for _, e := range []*endpoint{p.client, p.server} {
					c := e.controller
					if len(c.paths) != paths || c.setup.PathID != uint16(paths) {
						t.Fatalf("allocated %d paths, want negotiated %d; bootstrap %d", len(c.paths), paths, c.setup.PathID)
					}
					claims, err := RequiredInitialClaims(c.cfg)
					if err != nil || c.controlLease.Snapshot().Bytes != claims[InitialControl].Bytes {
						t.Fatalf("local prepaid control obligation changed: %v", err)
					}
					for i, path := range c.paths {
						if path.id != uint16(i+1) || !path.active || path.sendEpoch != 2 || path.receiveEpoch != 2 {
							t.Fatalf("negotiated path %d did not complete join/budget publication", i+1)
						}
					}
				}
				payload := bytes.Repeat([]byte("negotiated-path-payload"), 300)
				for _, e := range []*endpoint{p.client, p.server} {
					if _, _, err := e.controller.Write(p.now, payload); err != nil {
						t.Fatal(err)
					}
					if _, _, err := e.controller.Flush(p.now); err != nil {
						t.Fatal(err)
					}
				}
				p.pump(t, func() bool { return len(p.client.deliveries) == 1 && len(p.server.deliveries) == 1 })
				for _, e := range []*endpoint{p.client, p.server} {
					if !bytes.Equal(e.deliveries[0].Payload(), payload) {
						t.Fatal("asymmetric path contract changed payload")
					}
				}
				if paths == 256 {
					return
				}
				c := p.client.controller
				outside := uint16(paths + 1)
				if _, err := c.Join(p.now, Carrier{Carrier: handshakev2.Carrier{PathID: outside, Binding: clientBinding(int(outside))}, Sender: &testPath{clientBinding(int(outside))}}); !errors.Is(err, ErrInvalid) {
					t.Fatalf("Join accepted path outside negotiated bound: %v", err)
				}
				data, err := wirev2.AppendEstablished(nil, wirev2.Header{Type: wirev2.TypePathJoin, SessionID: c.setup.ID}, wirev2.Route{PathID: uint32(outside), Generation: 1}, make([]byte, 440), c.sendKey)
				if err != nil {
					t.Fatal(err)
				}
				binding := opposite(clientBinding(int(outside)), true)
				if _, err := p.server.controller.Receive(p.now, binding, &testPath{binding}, data); !errors.Is(err, ErrProtocol) {
					t.Fatalf("Receive accepted authenticated path outside negotiated bound: %v", err)
				}
			})
		}
	}
}

func TestNegotiatedPathStorageRejectsAlteredBoundsBeforeBindingCredit(t *testing.T) {
	for _, mutate := range []struct {
		name  string
		apply func(*handshakev2.Setup)
	}{
		{"zero_maximum", func(s *handshakev2.Setup) { s.Contract.MaxPaths = 0 }},
		{"larger_maximum", func(s *handshakev2.Setup) { s.Contract.MaxPaths = 6 }},
		{"smaller_maximum", func(s *handshakev2.Setup) { s.Contract.MaxPaths = 4 }},
		{"zero_bootstrap", func(s *handshakev2.Setup) { s.PathID, s.Contract.BootstrapPathID = 0, 0 }},
		{"large_bootstrap", func(s *handshakev2.Setup) { s.PathID, s.Contract.BootstrapPathID = 6, 6 }},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			setup, cfg, peer := scratchSetup(t, configFor(profile(5), false), 0)
			original := setup
			before := peer.Snapshot()
			mutate.apply(&setup)
			if c, err := New(setup, cfg); c != nil || !errors.Is(err, ErrInvalid) || peer.Snapshot() != before {
				t.Fatalf("altered contract changed ownership or was accepted: %v", err)
			}
			c, err := New(original, cfg)
			if err != nil {
				t.Fatalf("rejected setup consumed a one-time lease binding: %v", err)
			}
			c.Close()
		})
	}
}
