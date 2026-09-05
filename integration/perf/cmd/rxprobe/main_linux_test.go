//go:build linux

package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

func testOptions() options {
	return options{mode: "scalar", batch: 16, burst: 8, payload: 32, packets: 37, warmup: 3, timeout: 10 * time.Second}
}

func TestOptionBounds(t *testing.T) {
	for _, args := range [][]string{
		{"-mode", "unknown"}, {"-batch", "0"}, {"-batch", "33"},
		{"-burst", "0"}, {"-burst", "33"}, {"-payload", "7"}, {"-payload", "1201"},
		{"-packets", "0"}, {"-packets", "10000001"}, {"-warmup", "-1"},
		{"-warmup", "100001"}, {"-timeout", "0s"}, {"-timeout", "6m"}, {"extra"},
	} {
		if _, err := parseOptions(args); err == nil {
			t.Fatalf("accepted invalid options %v", args)
		}
	}
}

func receiverPair(t *testing.T, mode string, capacity, payload int) (*packetReceiver, *net.UDPConn) {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	sender, err := net.DialUDP("udp4", nil, conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sender.Close() })
	return newPacketReceiver(conn, mode, capacity, payload), sender
}

func testPayload(sequence uint64, size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}
	binary.LittleEndian.PutUint64(payload, sequence)
	return payload
}

func TestReceivesOwnPayloadAndAddressSnapshotsAcrossReuse(t *testing.T) {
	for _, mode := range []string{"scalar", "batch"} {
		t.Run(mode, func(t *testing.T) {
			receiver, sender := receiverPair(t, mode, 4, 32)
			var retained []ownedPacket
			accept := func(packet ownedPacket) error { retained = append(retained, packet); return nil }
			for burst := range 2 {
				for i := range 3 {
					if _, err := sender.Write(testPayload(uint64(burst*3+i), 32)); err != nil {
						t.Fatal(err)
					}
				}
				if err := receiver.drain(3, accept); err != nil {
					t.Fatal(err)
				}
			}
			for i, packet := range retained {
				if binary.LittleEndian.Uint64(packet.payload) != uint64(i) {
					t.Fatal("later receive overwrote a retained payload")
				}
			}
			local, remote := retained[1].local.String(), retained[1].remote.String()
			retained[0].local.IP[0], retained[0].remote.IP[0] = 0, 0
			if retained[1].local.String() != local || retained[1].remote.String() != remote || receiver.local.String() != local {
				t.Fatal("mutating one packet changed another address snapshot or the socket")
			}
		})
	}
}

func TestOversizeAndTruncationDoNotPreventLaterReceive(t *testing.T) {
	for _, mode := range []string{"scalar", "batch"} {
		for _, size := range []int{33, 256} {
			t.Run(fmt.Sprintf("%s/%d", mode, size), func(t *testing.T) {
				receiver, sender := receiverPair(t, mode, 4, 32)
				if _, err := sender.Write(testPayload(0, size)); err != nil {
					t.Fatal(err)
				}
				if err := receiver.drain(1, func(ownedPacket) error { t.Fatal("oversize packet delivered"); return nil }); err == nil {
					t.Fatal("accepted oversized or truncated packet")
				}
				if _, err := sender.Write(testPayload(0, 32)); err != nil {
					t.Fatal(err)
				}
				collector := &collector{payload: 32}
				if err := receiver.drain(1, collector.accept); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestCloseUnblocksScalarAndBatchReceive(t *testing.T) {
	for _, mode := range []string{"scalar", "batch"} {
		t.Run(mode, func(t *testing.T) {
			receiver, _ := receiverPair(t, mode, 4, 32)
			result := make(chan error, 1)
			go func() { _, err := receiver.read(); result <- err }()
			if err := receiver.conn.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-result:
				if !errors.Is(err, net.ErrClosed) {
					t.Fatalf("receive error = %v, want closed socket", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close did not release receive")
			}
		})
	}
}

func helperCommand(ctx context.Context, args []string) *exec.Cmd {
	commandArgs := append([]string{"-test.run=^TestSenderHelperProcess$", "--"}, args...)
	cmd := exec.CommandContext(ctx, os.Args[0], commandArgs...)
	cmd.Env = append(os.Environ(), "MPUDP_RX_TEST_SENDER=1")
	return cmd
}

func TestSenderHelperProcess(t *testing.T) {
	if os.Getenv("MPUDP_RX_TEST_SENDER") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg != "--" {
			continue
		}
		switch os.Getenv("MPUDP_RX_TEST_BEHAVIOR") {
		case "fail":
			os.Exit(3)
		case "stall":
			time.Sleep(time.Minute)
			os.Exit(4)
		}
		o, err := parseOptions(os.Args[i+1:])
		if err == nil {
			err = runSender(o, os.Stdin, os.Stdout)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(2)
}

func TestIndependentSenderCountsSinglePacketAndTailBursts(t *testing.T) {
	for _, mode := range []string{"scalar", "batch"} {
		for _, count := range []int{1, 37} {
			t.Run(fmt.Sprintf("%s/%d", mode, count), func(t *testing.T) {
				o := testOptions()
				o.mode, o.packets = mode, count
				readyCalls := 0
				result, err := runReceiver(o, helperCommand, func(_ context.Context, value readyRecord) error {
					readyCalls++
					if value.Kind != "ready" || value.ReceiverPID != os.Getpid() || value.SenderPID == os.Getpid() || value.ReceiverFD < 0 {
						t.Fatal("invalid readiness metadata")
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				wantCalls := count
				if mode == "batch" {
					wantCalls = (count + o.burst - 1) / o.burst
				}
				if result.ReceivedPackets != uint64(count) || result.ReceiveCalls != uint64(wantCalls) || result.SyscallCountKnown || readyCalls != 1 {
					t.Fatalf("incorrect receive counts/attribution: %+v", result)
				}
				var histogramPackets, histogramCalls uint64
				for size, calls := range result.CallSizes {
					histogramPackets += uint64(size) * calls
					histogramCalls += calls
				}
				if histogramPackets != result.ReceivedPackets || histogramCalls != result.ReceiveCalls || result.SenderPID == result.ReceiverPID {
					t.Fatal("histogram totals or independent-process identity do not match")
				}
			})
		}
	}
}

func TestCollectorRejectsCorruptionAndSequenceErrors(t *testing.T) {
	for _, mutation := range []func([]byte){
		func(payload []byte) { payload[0]++ },
		func(payload []byte) { payload[8]++ },
	} {
		collector := &collector{payload: 32}
		payload := testPayload(0, 32)
		mutation(payload)
		if err := collector.accept(ownedPacket{payload: payload}); err == nil || collector.expected != 0 {
			t.Fatal("invalid packet advanced the accepted sequence")
		}
	}
}

func TestReceiverFailureAndReadyTimeoutJoinSender(t *testing.T) {
	for _, behavior := range []string{"fail", "stall", "ready-timeout"} {
		t.Run(behavior, func(t *testing.T) {
			o := testOptions()
			o.timeout = time.Second
			var child *exec.Cmd
			factory := func(ctx context.Context, args []string) *exec.Cmd {
				child = helperCommand(ctx, args)
				child.Env = append(child.Env, "MPUDP_RX_TEST_BEHAVIOR="+behavior)
				return child
			}
			var ready func(context.Context, readyRecord) error
			if behavior == "ready-timeout" {
				ready = func(ctx context.Context, _ readyRecord) error { <-ctx.Done(); return ctx.Err() }
			}
			started := time.Now()
			if _, err := runReceiver(o, factory, ready); err == nil {
				t.Fatal("failed or stalled child/ready barrier did not fail the receiver")
			}
			if time.Since(started) > 3*time.Second || child == nil || child.ProcessState == nil {
				t.Fatal("receiver did not join its failed child within the bounded timeout")
			}
		})
	}
}
