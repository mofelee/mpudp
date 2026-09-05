package session

import (
	"context"
	"testing"

	"github.com/mofelee/mpudp/internal/wire"
)

type contextBenchmarkPath struct{}

func (contextBenchmarkPath) PathID() string  { return "carrier" }
func (contextBenchmarkPath) Available() bool { return true }
func (contextBenchmarkPath) Send(ctx context.Context, _ []byte) error {
	return ctx.Err()
}

func BenchmarkSessionOperationContext(b *testing.B) {
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &Session{lifetime: lifetime}
	for _, test := range []struct {
		name string
		ctx  context.Context
	}{{"background", context.Background()}, {"cancelable", lifetime}} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				ctx, finish := s.operationContextWithCancel(test.ctx)
				if err := ctx.Err(); err != nil {
					b.Fatal(err)
				}
				finish()
			}
		})
	}
}

func BenchmarkSessionWritePacketContext(b *testing.B) {
	s, err := NewInitiator(testSessionID, testConfig(newFakeClock(), 1200), []Path{contextBenchmarkPath{}})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close(context.Background())
	if _, err := s.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	ack, err := wire.NewHelloAck(testSessionID, 3, 2, 1200)
	if err != nil {
		b.Fatal(err)
	}
	packet, err := wire.AppendAuthenticated(nil, ack, []byte(testPSK), 1200)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := s.HandlePacket(context.Background(), ReceivedPacket{Payload: packet, Reply: newFakePath("carrier", "198.51.100.1:9000")}); err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 1400)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		if _, err := s.WritePacket(context.Background(), payload); err != nil {
			b.Fatal(err)
		}
	}
}
