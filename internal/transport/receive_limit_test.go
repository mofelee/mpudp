package transport_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/transport"
)

func TestIndependentReceivePayloadLimits(t *testing.T) {
	for _, kind := range []string{"carrier", "listener"} {
		for _, test := range []struct {
			name                string
			send, receive, want int
		}{
			{"receive larger", 4, 8, 8},
			{"send larger", 8, 4, 4},
			{"receive defaults to send", 4, 0, 4},
			{"both default", 0, 0, transport.MaxUDPPayload},
		} {
			t.Run(kind+"/"+test.name, func(t *testing.T) {
				packets := make(chan transport.ReceivedPacket, 2)
				errorsCh := make(chan error, 1)
				counters := new(transport.Counters)
				onPacket := func(packet transport.ReceivedPacket) { packets <- packet }
				onError := func(err error) { errorsCh <- err }
				var inject func([]byte)
				if kind == "carrier" {
					conn := newFakeConnectedConn("local", "remote")
					carrier, err := transport.OpenCarrier(context.Background(), "carrier", "remote", transport.CarrierOptions{
						Dial:       func(context.Context, string) (net.Conn, error) { return conn, nil },
						MaxPayload: test.send, MaxReceivePayload: test.receive,
						OnPacket: onPacket, OnError: onError, Statistics: counters,
					})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = carrier.Close() })
					inject = func(payload []byte) { conn.reads <- streamRead{payload: payload} }
				} else {
					conn := newFakePacketConn("local")
					listener, err := transport.ServePacketConn(conn, transport.ListenerOptions{
						PathID: "listener", MaxPayload: test.send, MaxReceivePayload: test.receive,
						OnPacket: onPacket, OnError: onError, Statistics: counters,
					})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = listener.Close() })
					inject = func(payload []byte) { conn.reads <- packetRead{payload: payload, remote: fakeAddr("remote")} }
				}

				payload := bytes.Repeat([]byte{'r'}, test.want)
				inject(payload)
				packet := receivePacket(t, packets)
				if !bytes.Equal(packet.Payload, payload) {
					t.Fatalf("receive length = %d, want %d", len(packet.Payload), test.want)
				}
				sendLimit := test.send
				if sendLimit == 0 {
					sendLimit = transport.MaxUDPPayload
				}
				if err := packet.Reply.Send(context.Background(), make([]byte, sendLimit)); err != nil {
					t.Fatalf("send at independent limit: %v", err)
				}
				if err := packet.Reply.Send(context.Background(), make([]byte, sendLimit+1)); !errors.Is(err, transport.ErrPayloadTooLarge) {
					t.Fatalf("oversized send error = %v", err)
				}
				inject(make([]byte, test.want+9))
				select {
				case err := <-errorsCh:
					var size *transport.PayloadSizeError
					if !errors.As(err, &size) || size.Limit != test.want || size.Size != test.want+1 {
						t.Fatalf("receive limit error = %v, size = %+v", err, size)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("missing receive limit error")
				}
				inject([]byte("ok"))
				if packet := receivePacket(t, packets); string(packet.Payload) != "ok" {
					t.Fatal("oversize packet reached callback or subsequent read failed")
				}
				if counters.ReceiveOversizeDrops.Load() != 1 || counters.ReceivedPackets.Load() != 3 || counters.ReceivedBytes.Load() != uint64(2*test.want+3) {
					t.Fatalf("receive counters = packets %d bytes %d drops %d", counters.ReceivedPackets.Load(), counters.ReceivedBytes.Load(), counters.ReceiveOversizeDrops.Load())
				}
				if counters.SentPackets.Load() != 1 || counters.SentBytes.Load() != uint64(sendLimit) {
					t.Fatal("send counters changed with receive limit")
				}
			})
		}
	}
}

func TestInvalidReceiveLimitDoesNotAcquireConnection(t *testing.T) {
	for _, limit := range []int{-1, transport.MaxUDPPayload + 1} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			_, err := transport.OpenCarrier(context.Background(), "carrier", "remote", transport.CarrierOptions{
				MaxReceivePayload: limit,
				Dial: func(context.Context, string) (net.Conn, error) {
					t.Error("invalid receive limit reached dialer")
					return nil, errors.New("unexpected dial")
				},
			})
			if !errors.Is(err, transport.ErrPayloadTooLarge) {
				t.Fatalf("carrier error = %v", err)
			}
			conn := newFakePacketConn("local")
			t.Cleanup(func() { _ = conn.Close() })
			_, err = transport.ServePacketConn(conn, transport.ListenerOptions{PathID: "listener", MaxReceivePayload: limit})
			if !errors.Is(err, transport.ErrPayloadTooLarge) {
				t.Fatalf("listener error = %v", err)
			}
			select {
			case <-conn.closed:
				t.Fatal("failed listener constructor took connection ownership")
			default:
			}
		})
	}
}
