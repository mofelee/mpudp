package transport_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/transport"
)

type nativeBenchmarkWrapper struct{ *net.UDPConn }

func (c nativeBenchmarkWrapper) ReadFrom(payload []byte) (int, net.Addr, error) {
	return c.UDPConn.ReadFrom(payload)
}

func runNativeBenchmarkSender() error {
	remote, err := net.ResolveUDPAddr("udp", os.Getenv("MPUDP_NATIVE_BENCH_ADDRESS"))
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp", nil, remote)
	if err != nil {
		return err
	}
	defer conn.Close()
	payload := make([]byte, 551)
	var request [16]byte
	ack := []byte{1}
	for {
		if _, err := io.ReadFull(os.Stdin, request[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		sequence := binary.LittleEndian.Uint64(request[:8])
		count := binary.LittleEndian.Uint64(request[8:])
		if count == 0 || count > 32 {
			return errors.New("invalid benchmark burst")
		}
		for i := range count {
			binary.LittleEndian.PutUint64(payload, sequence+i)
			if n, err := conn.Write(payload); err != nil {
				return err
			} else if n != len(payload) {
				return io.ErrShortWrite
			}
		}
		if _, err := os.Stdout.Write(ack); err != nil {
			return err
		}
	}
}

func TestNativeListenerBenchmarkSender(t *testing.T) {
	if os.Getenv("MPUDP_NATIVE_BENCH_SENDER") != "1" {
		return
	}
	if err := runNativeBenchmarkSender(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

// Each operation completes one bounded burst through the actual transport
// Listener. Sender CPU/allocation lives in a separate process; receiver pipe
// coordination, callback delivery and sequence validation remain in the cost.
func BenchmarkNativeListenerReceive(b *testing.B) {
	for _, test := range []struct {
		name    string
		network string
		bind    string
		wrapped bool
	}{
		{"native_ipv4", "udp4", "127.0.0.1:0", false},
		{"native_dual_stack", "udp", "[::]:0", false},
		{"injected_ipv4", "udp4", "127.0.0.1:0", true},
	} {
		for _, burst := range []int{1, 32} {
			b.Run(fmt.Sprintf("%s/burst%d", test.name, burst), func(b *testing.B) {
				bind, err := net.ResolveUDPAddr(test.network, test.bind)
				if err != nil {
					b.Fatal(err)
				}
				conn, err := net.ListenUDP(test.network, bind)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = conn.Close() })
				if err := conn.SetReadBuffer(256 * 1024); err != nil {
					b.Fatal(err)
				}
				var packetConn net.PacketConn = conn
				if test.wrapped {
					packetConn = nativeBenchmarkWrapper{UDPConn: conn}
				}
				packets := make(chan transport.ReceivedPacket, 32)
				listener, err := transport.ServePacketConn(packetConn, transport.ListenerOptions{
					PathID: "benchmark", MaxPayload: 1200,
					OnPacket: func(packet transport.ReceivedPacket) { packets <- packet },
				})
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = listener.Close() })
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				address := net.JoinHostPort("127.0.0.1", strconv.Itoa(conn.LocalAddr().(*net.UDPAddr).Port))
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNativeListenerBenchmarkSender$")
				cmd.Env = append(os.Environ(), "MPUDP_NATIVE_BENCH_SENDER=1", "MPUDP_NATIVE_BENCH_ADDRESS="+address)
				cmd.Stderr = os.Stderr
				input, err := cmd.StdinPipe()
				if err != nil {
					cancel()
					b.Fatal(err)
				}
				output, err := cmd.StdoutPipe()
				if err != nil {
					_ = input.Close()
					cancel()
					b.Fatal(err)
				}
				if err := cmd.Start(); err != nil {
					_ = input.Close()
					_ = output.Close()
					cancel()
					b.Fatal(err)
				}
				b.Cleanup(func() {
					defer cancel()
					_ = input.Close()
					if b.Failed() {
						_ = cmd.Process.Kill()
					}
					if err := cmd.Wait(); err != nil && !b.Failed() {
						b.Error(err)
					}
					_ = output.Close()
				})
				var request [16]byte
				var ack [1]byte
				var retained [32]transport.ReceivedPacket
				var sequence uint64
				transfer := func() {
					binary.LittleEndian.PutUint64(request[:8], sequence)
					binary.LittleEndian.PutUint64(request[8:], uint64(burst))
					if n, err := input.Write(request[:]); err != nil || n != len(request) {
						b.Fatalf("sender request: bytes=%d error=%v", n, err)
					}
					if _, err := io.ReadFull(output, ack[:]); err != nil || ack[0] != 1 {
						b.Fatalf("sender acknowledgement: %v", err)
					}
					for i := range burst {
						select {
						case packet := <-packets:
							if len(packet.Payload) != 551 || binary.LittleEndian.Uint64(packet.Payload) != sequence {
								b.Fatal("benchmark packet lost, reordered or corrupted")
							}
							retained[i] = packet
							sequence++
						case <-ctx.Done():
							b.Fatal(ctx.Err())
						}
					}
				}
				transfer()
				b.ReportAllocs()
				b.SetBytes(int64(551 * burst))
				for b.Loop() {
					transfer()
				}
				runtime.KeepAlive(retained)
				b.ReportMetric(float64(burst), "packets/op")
			})
		}
	}
}
