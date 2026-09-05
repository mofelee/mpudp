//go:build linux

package transport_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/transport"
)

func TestRequiredDestinationWildcardRepliesAndOwnership(t *testing.T) {
	for _, test := range []struct {
		name, network, bind, clientNetwork, clientIP string
		destinations                                 []string
	}{
		{"IPv4", "udp4", "0.0.0.0:0", "udp4", "127.0.0.1", []string{"127.0.0.1", "127.0.0.2"}},
		{"dual stack IPv4", "udp", "[::]:0", "udp4", "127.0.0.1", []string{"127.0.0.1", "127.0.0.2"}},
		{"IPv6", "udp6", "[::]:0", "udp6", "::1", []string{"::1", "::1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			packets := make(chan transport.ReceivedPacket, 3)
			errorsCh := make(chan error, 3)
			counters := new(transport.Counters)
			listener, err := transport.OpenListener(context.Background(), test.network, test.bind, transport.ListenerOptions{
				PathID: "destination", MaxPayload: 32, MaxReceivePayload: 8, RequireDestination: true,
				OnPacket: func(packet transport.ReceivedPacket) { packets <- packet },
				OnError:  func(err error) { errorsCh <- err }, Statistics: counters,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			bound := listener.LocalAddr().(*net.UDPAddr)
			if !bound.IP.IsUnspecified() {
				t.Fatalf("test did not bind wildcard: %v", bound)
			}
			client, err := net.ListenUDP(test.clientNetwork, &net.UDPAddr{IP: net.ParseIP(test.clientIP)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			var retained []transport.ReceivedPacket
			for i, destination := range test.destinations {
				payload := []byte{byte(i), 'r', 'e', 'q'}
				if _, err := client.WriteToUDP(payload, &net.UDPAddr{IP: net.ParseIP(destination), Port: bound.Port}); err != nil {
					t.Fatal(err)
				}
				var packet transport.ReceivedPacket
				select {
				case packet = <-packets:
				case err := <-errorsCh:
					t.Fatalf("capture error: %v: %v", err, errors.Unwrap(err))
				case <-time.After(2 * time.Second):
					t.Fatal("timed out capturing destination")
				}
				local := packet.LocalAddr.(*net.UDPAddr)
				if !local.IP.Equal(net.ParseIP(destination)) || local.Port != bound.Port || !bytes.Equal(packet.Payload, payload) {
					t.Fatalf("packet destination/payload = %v / %v", local, packet.Payload)
				}
				retained = append(retained, packet)
			}

			pathCounters := new(transport.Counters)
			for i := len(retained) - 1; i >= 0; i-- {
				packet := retained[i]
				expectedLocal := packet.LocalAddr.String()
				expectedRemote := packet.RemoteAddr.String()
				for _, address := range []net.Addr{packet.LocalAddr, packet.RemoteAddr, packet.Reply.LocalAddr(), packet.Reply.RemoteAddr()} {
					udp := address.(*net.UDPAddr)
					clear(udp.IP)
					udp.Port = 1
				}
				if packet.Reply.LocalAddr().String() != expectedLocal || packet.Reply.RemoteAddr().String() != expectedRemote {
					t.Fatal("mutable packet or getter addresses altered retained reply")
				}
				if packet.Payload[0] != byte(i) {
					t.Fatal("later read overwrote retained payload")
				}
				reply := transport.WithReplyStatistics(packet.Reply, pathCounters)
				if err := reply.Send(context.Background(), []byte("response")); err != nil {
					t.Fatalf("reply: %v: %v", err, errors.Unwrap(err))
				}
				if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
					t.Fatal(err)
				}
				buffer := make([]byte, 32)
				n, source, err := client.ReadFromUDP(buffer)
				if err != nil {
					t.Fatal(err)
				}
				if string(buffer[:n]) != "response" || source.Port != bound.Port || !source.IP.Equal(net.ParseIP(test.destinations[i])) {
					t.Fatalf("reply source/payload = %v / %q", source, buffer[:n])
				}
			}
			if pathCounters.SentPackets.Load() != 2 || counters.SentPackets.Load() != 2 {
				t.Fatal("reply statistics wrapper lost source-aware sends")
			}
			if _, err := client.WriteToUDP(make([]byte, 64), &net.UDPAddr{IP: net.ParseIP(test.destinations[0]), Port: bound.Port}); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-errorsCh:
				if !errors.Is(err, transport.ErrPayloadTooLarge) {
					t.Fatalf("oversize capture error = %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("oversize captured packet was not rejected")
			}
			if counters.ReceiveOversizeDrops.Load() != 1 {
				t.Fatal("capture did not use the receive limit for oversize statistics")
			}
			start := make(chan struct{})
			var workers sync.WaitGroup
			for range 4 {
				workers.Add(1)
				go func() {
					defer workers.Done()
					<-start
					err := retained[0].Reply.Send(context.Background(), []byte("closing"))
					if err != nil && !errors.Is(err, transport.ErrClosed) && !errors.Is(err, net.ErrClosed) {
						t.Errorf("concurrent reply: %v", err)
					}
				}()
				workers.Add(1)
				go func() {
					defer workers.Done()
					<-start
					if err := listener.Close(); err != nil {
						t.Errorf("concurrent close: %v", err)
					}
				}()
			}
			close(start)
			workers.Wait()
			if retained[0].Reply.Available() || retained[0].Context.Err() == nil {
				t.Fatal("closed capture retained an active reply")
			}
			if err := retained[0].Reply.Send(context.Background(), []byte("late")); !errors.Is(err, transport.ErrClosed) {
				t.Fatalf("closed reply error = %v", err)
			}
		})
	}
}
