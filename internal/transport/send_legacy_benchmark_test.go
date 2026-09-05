package transport

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// This deterministic connection excludes kernel/network cost and retained test
// payload copies, isolating transport locking, cancellation and diagnostics.
type sendBenchmarkConn struct{}

func (sendBenchmarkConn) Read([]byte) (int, error)                  { return 0, io.EOF }
func (sendBenchmarkConn) ReadFrom([]byte) (int, net.Addr, error)    { return 0, nil, io.EOF }
func (sendBenchmarkConn) Write(p []byte) (int, error)               { return len(p), nil }
func (sendBenchmarkConn) WriteTo(p []byte, _ net.Addr) (int, error) { return len(p), nil }
func (sendBenchmarkConn) Close() error                              { return nil }
func (sendBenchmarkConn) LocalAddr() net.Addr                       { return nil }
func (sendBenchmarkConn) RemoteAddr() net.Addr                      { return nil }
func (sendBenchmarkConn) SetDeadline(time.Time) error               { return nil }
func (sendBenchmarkConn) SetReadDeadline(time.Time) error           { return nil }
func (sendBenchmarkConn) SetWriteDeadline(time.Time) error          { return nil }

func benchmarkTransportSend(b *testing.B, send func(context.Context, ReplyPath, []byte) error) {
	for _, kind := range []string{"carrier", "captured-carrier", "listener"} {
		for _, diagnostics := range []bool{false, true} {
			mode := "diagnostics-off"
			if diagnostics {
				mode = "diagnostics-on"
			}
			b.Run(kind+"/"+mode, func(b *testing.B) {
				var enabled atomic.Bool
				enabled.Store(diagnostics)
				counters := &Counters{DiagnosticsEnabled: &enabled}
				conn := sendBenchmarkConn{}
				var path ReplyPath
				if kind == "listener" {
					listener := &Listener{id: "bench", conn: conn, generation: 1, maxPayload: 1200, statistics: counters}
					path = listenerReplyPath{listener: listener, generation: 1}
				} else {
					generation := &carrierGeneration{number: 1, conn: conn}
					generation.alive.Store(true)
					carrier := &Carrier{id: "bench", current: generation, maxPayload: 1200, statistics: counters}
					path = carrier
					if kind == "captured-carrier" {
						path = carrierReplyPath{carrier: carrier, generation: 1}
					}
				}
				payload := make([]byte, 1200)
				ctx := context.Background()
				b.ReportAllocs()
				b.SetBytes(int64(len(payload)))
				b.ResetTimer()
				for b.Loop() {
					if err := send(ctx, path, payload); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkTransportSendLegacy(b *testing.B) {
	benchmarkTransportSend(b, func(ctx context.Context, path ReplyPath, payload []byte) error {
		return path.Send(ctx, payload)
	})
}
