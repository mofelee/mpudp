//go:build linux

// rxprobe isolates scalar and batched UDP receive costs from the MPUDP pipeline.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"golang.org/x/net/ipv4"
	"golang.org/x/sys/unix"
)

var sourceSHA = "unknown"

const maxBurst = 32

type options struct {
	mode    string
	batch   int
	burst   int
	payload int
	packets int
	warmup  int
	timeout time.Duration
	sender  bool
	address string
	ready   bool
}

func parseOptions(args []string) (options, error) {
	var o options
	f := flag.NewFlagSet("rxprobe", flag.ContinueOnError)
	f.StringVar(&o.mode, "mode", "scalar", "scalar or batch UDP receive")
	f.IntVar(&o.batch, "batch", 32, "batch receive capacity, 1..32")
	f.IntVar(&o.burst, "burst", 32, "prequeued packets per sender burst, 1..32")
	f.IntVar(&o.payload, "payload", 551, "synthetic UDP payload bytes, 8..1200")
	f.IntVar(&o.packets, "packets", 262144, "measured packets, 1..10000000")
	f.IntVar(&o.warmup, "warmup", 1024, "untimed warmup packets, 0..100000")
	f.DurationVar(&o.timeout, "timeout", 30*time.Second, "whole-process timeout, 1s..5m")
	f.BoolVar(&o.ready, "ready", false, "emit receiver PID/FD after warmup and wait for a newline")
	f.BoolVar(&o.sender, "sender", false, "internal child sender mode")
	f.StringVar(&o.address, "address", "", "internal child loopback destination")
	if err := f.Parse(args); err != nil {
		return o, err
	}
	if f.NArg() != 0 {
		return o, errors.New("unexpected positional arguments")
	}
	if o.mode != "scalar" && o.mode != "batch" {
		return o, errors.New("mode must be scalar or batch")
	}
	if o.batch < 1 || o.batch > maxBurst || o.burst < 1 || o.burst > maxBurst || o.payload < 8 || o.payload > 1200 {
		return o, errors.New("batch/burst must be 1..32 and payload 8..1200")
	}
	if o.packets < 1 || o.packets > 10000000 || o.warmup < 0 || o.warmup > 100000 || o.timeout < time.Second || o.timeout > 5*time.Minute {
		return o, errors.New("packets, warmup or timeout outside bounds")
	}
	return o, nil
}

type ownedPacket struct {
	payload []byte
	local   *net.UDPAddr
	remote  *net.UDPAddr
}

func cloneUDP(addr *net.UDPAddr) *net.UDPAddr {
	copyAddr := *addr
	copyAddr.IP = append(net.IP(nil), addr.IP...)
	return &copyAddr
}

type packetReceiver struct {
	conn     *net.UDPConn
	batch    *ipv4.PacketConn
	local    *net.UDPAddr
	messages []ipv4.Message
	payload  int
	calls    uint64
	sizes    [maxBurst + 1]uint64
}

func newPacketReceiver(conn *net.UDPConn, mode string, capacity, payload int) *packetReceiver {
	if mode == "scalar" {
		capacity = 1
	}
	r := &packetReceiver{
		conn: conn, local: conn.LocalAddr().(*net.UDPAddr), payload: payload,
		messages: make([]ipv4.Message, capacity),
	}
	for i := range r.messages {
		r.messages[i].Buffers = [][]byte{make([]byte, payload+1)}
	}
	if mode == "batch" {
		r.batch = ipv4.NewPacketConn(conn)
	}
	return r
}

func (r *packetReceiver) read() (int, error) {
	r.calls++
	if r.batch != nil {
		return r.batch.ReadBatch(r.messages, 0)
	}
	message := &r.messages[0]
	n, remote, err := r.conn.ReadFrom(message.Buffers[0])
	if err != nil {
		return 0, err
	}
	message.N, message.Addr, message.Flags = n, remote, 0
	return 1, nil
}

func (r *packetReceiver) drain(count int, receive func(ownedPacket) error) error {
	for count > 0 {
		n, err := r.read()
		if err != nil {
			return err
		}
		if n < 1 || n > count || n > len(r.messages) {
			return fmt.Errorf("unexpected receive batch size %d with %d packets remaining", n, count)
		}
		r.sizes[n]++
		for i := range n {
			message := &r.messages[i]
			if message.N > r.payload || message.Flags&unix.MSG_TRUNC != 0 {
				return errors.New("oversize or truncated datagram")
			}
			remote, ok := message.Addr.(*net.UDPAddr)
			if !ok || remote == nil {
				return errors.New("receive did not return a UDP source address")
			}
			// Match transport's owned packet and per-packet address snapshots.
			packet := ownedPacket{
				payload: append([]byte(nil), message.Buffers[0][:message.N]...),
				local:   cloneUDP(r.local), remote: cloneUDP(remote),
			}
			if err := receive(packet); err != nil {
				return err
			}
		}
		count -= n
	}
	return nil
}

type collector struct {
	expected uint64
	payload  int
	remote   *net.UDPAddr
	retained [maxBurst]ownedPacket
}

func (c *collector) accept(packet ownedPacket) error {
	if len(packet.payload) != c.payload || binary.LittleEndian.Uint64(packet.payload) != c.expected {
		return fmt.Errorf("datagram length, loss, duplication or ordering error at packet %d", c.expected)
	}
	for i, value := range packet.payload[8:] {
		if value != byte(i+8) {
			return fmt.Errorf("corrupt packet %d", c.expected)
		}
	}
	if c.remote == nil {
		c.remote = packet.remote
	}
	if !packet.remote.IP.Equal(c.remote.IP) || packet.remote.Port != c.remote.Port || packet.remote.Zone != c.remote.Zone {
		return errors.New("sender endpoint changed")
	}
	c.retained[c.expected%maxBurst] = packet
	c.expected++
	return nil
}

type senderProcess struct {
	cmd     *exec.Cmd
	input   io.WriteCloser
	output  io.ReadCloser
	request [16]byte
	ack     [1]byte
}

type commandFactory func(context.Context, []string) *exec.Cmd

func startSender(ctx context.Context, o options, address string, command commandFactory) (*senderProcess, error) {
	cmd := command(ctx, []string{"-sender", "-address", address, "-payload", strconv.Itoa(o.payload)})
	cmd.Stderr = os.Stderr
	input, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		_ = input.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = input.Close()
		_ = output.Close()
		return nil, err
	}
	return &senderProcess{cmd: cmd, input: input, output: output}, nil
}

func (s *senderProcess) burst(sequence uint64, count int) error {
	binary.LittleEndian.PutUint64(s.request[:8], sequence)
	binary.LittleEndian.PutUint64(s.request[8:], uint64(count))
	if n, err := s.input.Write(s.request[:]); err != nil {
		return err
	} else if n != len(s.request) {
		return io.ErrShortWrite
	}
	if _, err := io.ReadFull(s.output, s.ack[:]); err != nil {
		return err
	}
	if s.ack[0] != 1 {
		return errors.New("invalid sender acknowledgement")
	}
	return nil
}

func runSender(o options, input io.Reader, output io.Writer) error {
	address, err := net.ResolveUDPAddr("udp4", o.address)
	if err != nil {
		return err
	}
	if !address.IP.IsLoopback() {
		return errors.New("sender destination must be loopback")
	}
	conn, err := net.DialUDP("udp4", nil, address)
	if err != nil {
		return err
	}
	defer conn.Close()
	payload := make([]byte, o.payload)
	for i := range payload {
		payload[i] = byte(i)
	}
	var request [16]byte
	ack := []byte{1}
	for {
		if _, err := io.ReadFull(input, request[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		sequence, count := binary.LittleEndian.Uint64(request[:8]), binary.LittleEndian.Uint64(request[8:])
		if count < 1 || count > maxBurst {
			return errors.New("invalid sender burst count")
		}
		for i := range count {
			binary.LittleEndian.PutUint64(payload, sequence+i)
			if n, err := conn.Write(payload); err != nil {
				return err
			} else if n != len(payload) {
				return io.ErrShortWrite
			}
		}
		if _, err := output.Write(ack); err != nil {
			return err
		}
	}
}

type readyRecord struct {
	Kind        string `json:"kind"`
	ReceiverPID int    `json:"receiver_pid"`
	ReceiverFD  int    `json:"receiver_fd"`
	SenderPID   int    `json:"sender_pid"`
}

type report struct {
	Kind                string               `json:"kind"`
	SourceSHA           string               `json:"source_sha"`
	GoVersion           string               `json:"go_version"`
	OS                  string               `json:"os"`
	Arch                string               `json:"arch"`
	GOMAXPROCS          int                  `json:"gomaxprocs"`
	XNetVersion         string               `json:"x_net_version"`
	Mode                string               `json:"mode"`
	BatchCapacity       int                  `json:"batch_capacity"`
	BurstPackets        int                  `json:"burst_packets"`
	PayloadBytes        int                  `json:"payload_bytes"`
	SocketReceiveBytes  int                  `json:"socket_receive_buffer_bytes"`
	WarmupPackets       int                  `json:"warmup_packets"`
	ReceivedPackets     uint64               `json:"received_packets"`
	ReceiveCalls        uint64               `json:"receive_calls"`
	PacketsPerCall      float64              `json:"packets_per_receive_call"`
	CallSizes           [maxBurst + 1]uint64 `json:"receive_call_sizes"`
	ActiveReceiveSecs   float64              `json:"active_receive_seconds"`
	ActiveReceivePPS    float64              `json:"active_receive_pps"`
	WallSecs            float64              `json:"wall_seconds"`
	WallPPS             float64              `json:"wall_pps"`
	ReceiverUserSecs    float64              `json:"receiver_user_seconds"`
	ReceiverSystemSecs  float64              `json:"receiver_system_seconds"`
	ReceiverCPUNsPacket float64              `json:"receiver_cpu_ns_per_packet"`
	AllocatedBytes      uint64               `json:"allocated_bytes"`
	Allocations         uint64               `json:"allocations"`
	BytesPerPacket      float64              `json:"allocated_bytes_per_packet"`
	AllocsPerPacket     float64              `json:"allocations_per_packet"`
	SyscallCountKnown   bool                 `json:"syscall_count_available"`
	ReceiverPID         int                  `json:"receiver_pid"`
	SenderPID           int                  `json:"sender_pid"`
}

func cpuSeconds(value unix.Timeval) float64 {
	return float64(value.Sec) + float64(value.Usec)/1e6
}

func runReceiver(o options, command commandFactory, ready func(context.Context, readyRecord) error) (result report, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), o.timeout)
	defer cancel()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return result, err
	}
	defer conn.Close()
	if err := conn.SetReadBuffer(256 * 1024); err != nil {
		return result, err
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return result, err
	}
	var fd, socketReceiveBytes int
	var socketErr error
	if err := raw.Control(func(value uintptr) {
		fd = int(value)
		socketReceiveBytes, socketErr = unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF)
	}); err != nil {
		return result, err
	}
	if socketErr != nil {
		return result, socketErr
	}
	deadline, _ := ctx.Deadline()
	if err := conn.SetReadDeadline(deadline); err != nil {
		return result, err
	}
	sender, err := startSender(ctx, o, conn.LocalAddr().String(), command)
	if err != nil {
		return result, err
	}
	defer func() {
		_ = sender.input.Close()
		if err != nil {
			_ = sender.cmd.Process.Kill()
		}
		waitErr := sender.cmd.Wait()
		_ = sender.output.Close()
		if err == nil {
			err = waitErr
		}
	}()
	receiver := newPacketReceiver(conn, o.mode, o.batch, o.payload)
	collector := &collector{payload: o.payload}
	accept := collector.accept
	phase := func(packets int) (time.Duration, error) {
		var active time.Duration
		for remaining := packets; remaining > 0; {
			count := min(remaining, o.burst)
			if err := sender.burst(collector.expected, count); err != nil {
				return 0, err
			}
			started := time.Now()
			if err := receiver.drain(count, accept); err != nil {
				return 0, err
			}
			active += time.Since(started)
			remaining -= count
		}
		return active, nil
	}
	if _, err := phase(o.warmup); err != nil {
		return result, err
	}
	if ready != nil {
		if err := ready(ctx, readyRecord{Kind: "ready", ReceiverPID: os.Getpid(), ReceiverFD: fd, SenderPID: sender.cmd.Process.Pid}); err != nil {
			return result, err
		}
	}
	receiver.calls, receiver.sizes = 0, [maxBurst + 1]uint64{}
	var before, after runtime.MemStats
	var cpuBefore, cpuAfter unix.Rusage
	runtime.ReadMemStats(&before)
	if err := unix.Getrusage(unix.RUSAGE_SELF, &cpuBefore); err != nil {
		return result, err
	}
	started := time.Now()
	active, err := phase(o.packets)
	wall := time.Since(started)
	if err != nil {
		return result, err
	}
	if err := unix.Getrusage(unix.RUSAGE_SELF, &cpuAfter); err != nil {
		return result, err
	}
	runtime.ReadMemStats(&after)
	user := cpuSeconds(cpuAfter.Utime) - cpuSeconds(cpuBefore.Utime)
	system := cpuSeconds(cpuAfter.Stime) - cpuSeconds(cpuBefore.Stime)
	result = report{
		Kind: "rx_summary", SourceSHA: sourceSHA, GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH,
		GOMAXPROCS: runtime.GOMAXPROCS(0), XNetVersion: "unknown", Mode: o.mode,
		BatchCapacity: len(receiver.messages), BurstPackets: o.burst, PayloadBytes: o.payload, WarmupPackets: o.warmup,
		SocketReceiveBytes: socketReceiveBytes,
		ReceivedPackets:    uint64(o.packets), ReceiveCalls: receiver.calls,
		PacketsPerCall: float64(o.packets) / float64(receiver.calls), CallSizes: receiver.sizes,
		ActiveReceiveSecs: active.Seconds(), ActiveReceivePPS: float64(o.packets) / active.Seconds(),
		WallSecs: wall.Seconds(), WallPPS: float64(o.packets) / wall.Seconds(),
		ReceiverUserSecs: user, ReceiverSystemSecs: system, ReceiverCPUNsPacket: (user + system) * 1e9 / float64(o.packets),
		AllocatedBytes: after.TotalAlloc - before.TotalAlloc, Allocations: after.Mallocs - before.Mallocs,
		ReceiverPID: os.Getpid(), SenderPID: sender.cmd.Process.Pid,
	}
	result.BytesPerPacket = float64(result.AllocatedBytes) / float64(o.packets)
	result.AllocsPerPacket = float64(result.Allocations) / float64(o.packets)
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if dep.Path == "golang.org/x/net" {
				result.XNetVersion = dep.Version
			}
		}
	}
	return result, nil
}

func main() {
	o, err := parseOptions(os.Args[1:])
	if err == nil && o.sender {
		err = runSender(o, os.Stdin, os.Stdout)
	} else if err == nil {
		var executable string
		executable, err = os.Executable()
		if err == nil {
			factory := func(ctx context.Context, args []string) *exec.Cmd {
				return exec.CommandContext(ctx, executable, args...)
			}
			var ready func(context.Context, readyRecord) error
			if o.ready {
				ready = func(ctx context.Context, value readyRecord) error {
					stop := context.AfterFunc(ctx, func() { _ = os.Stdin.Close() })
					defer stop()
					if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
						return err
					}
					var input [1]byte
					if _, err := io.ReadFull(os.Stdin, input[:]); err != nil {
						return err
					}
					if input[0] != '\n' {
						return errors.New("ready barrier requires one newline")
					}
					return nil
				}
			}
			var result report
			result, err = runReceiver(o, factory, ready)
			if err == nil {
				err = json.NewEncoder(os.Stdout).Encode(result)
			}
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
