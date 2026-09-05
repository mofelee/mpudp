// perfprobe is an intentionally separate benchmark module. Its experimental
// kcp-go adapter is not a product dependency or a reliable-stream API promise.
package main

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

var sourceSHA = "unknown"

var output io.Writer = os.Stdout
var outputMu sync.Mutex

type options struct {
	Mode          string  `json:"-"`
	Protocol      string  `json:"protocol"`
	Address       string  `json:"-"`
	Control       string  `json:"-"`
	Bind          string  `json:"-"`
	Config        string  `json:"-"`
	ID            string  `json:"run_id"`
	Direction     string  `json:"direction"`
	Flows         int     `json:"flows"`
	Seconds       int     `json:"seconds"`
	Warmup        int     `json:"warmup_seconds"`
	Payload       int     `json:"message_bytes"`
	KCPMTU        int     `json:"kcp_mtu"`
	KCPWindow     int     `json:"kcp_window"`
	ACKNoDelay    bool    `json:"kcp_ack_no_delay"`
	Diagnostics   bool    `json:"diagnostics"`
	ProfilePrefix string  `json:"-"`
	RateMbps      float64 `json:"offered_mbps_per_flow"`
	Nonce         uint64  `json:"-"`
}

func flags() options {
	var o options
	flag.StringVar(&o.Mode, "mode", "client", "client or one-shot server")
	flag.StringVar(&o.Protocol, "protocol", "kcp-mpudp", "tcp, udp, kcp, mpudp, or kcp-mpudp")
	flag.StringVar(&o.Address, "address", "127.0.0.1:19000", "native data address; UDP reserves one port per flow")
	flag.StringVar(&o.Control, "control", "127.0.0.1:18999", "separate TCP control address, reachable in both directions")
	flag.StringVar(&o.Bind, "bind", "", "native client source IP")
	flag.StringVar(&o.Config, "config", "", "local MPUDP YAML; never included in output")
	flag.StringVar(&o.ID, "id", "manual", "run identifier")
	flag.StringVar(&o.Direction, "direction", "download", "upload or download, always client-initiated")
	flag.IntVar(&o.Flows, "flows", 1, "application flows; one connection or MPUDP Session per flow")
	flag.IntVar(&o.Seconds, "seconds", 300, "receiver steady-state window in seconds (1..3600)")
	flag.IntVar(&o.Warmup, "warmup", 10, "receiver warmup window in seconds (0..300)")
	flag.IntVar(&o.Payload, "payload", 1400, "application message bytes, including the 40-byte verifier header")
	flag.IntVar(&o.KCPMTU, "kcp-mtu", 1400, "KCP complete UDP payload or MPUDP Datagram budget")
	flag.IntVar(&o.KCPWindow, "kcp-window", 1024, "KCP send and receive window")
	flag.BoolVar(&o.ACKNoDelay, "ack-no-delay", false, "KCP immediate ACKs")
	flag.BoolVar(&o.Diagnostics, "diagnostics", false, "optional MPUDP timing and length histograms")
	flag.StringVar(&o.ProfilePrefix, "profile-prefix", "", "opt-in local private CPU/alloc/heap/mutex/block profile prefix")
	flag.Float64Var(&o.RateMbps, "rate-mbps", 0, "application message offered Mbit/s per flow; zero unlimited")
	flag.Parse()
	return o
}

func (o options) validate() error {
	if o.Mode != "client" && o.Mode != "server" {
		return errors.New("mode must be client or server")
	}
	switch o.Protocol {
	case "tcp", "udp", "kcp", "mpudp", "kcp-mpudp":
	default:
		return errors.New("invalid protocol")
	}
	if o.Direction != "upload" && o.Direction != "download" {
		return errors.New("direction must be upload or download")
	}
	if o.Flows < 1 || o.Flows > 64 || o.Seconds < 1 || o.Seconds > 3600 || o.Warmup < 0 || o.Warmup > 300 {
		return errors.New("flows must be 1..64, seconds 1..3600, warmup 0..300")
	}
	if o.Payload < 64 || o.Payload > 16*1024*1024 {
		return errors.New("payload must be 64..16777216 bytes")
	}
	if o.Protocol == "udp" && o.Payload > 65507 {
		return errors.New("native UDP payload cannot exceed 65507")
	}
	if o.KCPMTU < 64 || o.KCPMTU > 1500 || o.KCPWindow < 1 || o.KCPWindow > 65536 {
		return errors.New("KCP MTU must be 64..1500 (kcp-go hard limit), window 1..65536")
	}
	if math.IsNaN(o.RateMbps) || math.IsInf(o.RateMbps, 0) || o.RateMbps < 0 || o.RateMbps > 1000000 {
		return errors.New("rate-mbps must be 0..1000000")
	}
	if o.RateMbps > 0 && float64(o.Payload)*8/o.RateMbps*1000 >= float64(math.MaxInt64) {
		return errors.New("rate-mbps is too small for a representable message pacing interval")
	}
	if int64(o.Payload)*int64(o.Flows) > 64*1024*1024 {
		return errors.New("payload times flows must not exceed 64 MiB")
	}
	if len(o.ID) == 0 || len(o.ID) > 128 || strings.ContainsAny(o.ID, "\n\r\x00") {
		return errors.New("invalid run ID")
	}
	if o.Bind != "" && net.ParseIP(o.Bind) == nil {
		return errors.New("bind must be a source IP")
	}
	return nil
}

type controlMessage struct {
	Kind    string   `json:"kind"`
	Options *options `json:"options,omitempty"`
	Nonce   uint64   `json:"nonce,omitempty"`
	Flow    int      `json:"flow,omitempty"`
	Paths   int      `json:"path_count,omitempty"`
	Summary *summary `json:"summary,omitempty"`
}

func controlWrite(c net.Conn, v controlMessage) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > 4*1024*1024 {
		return errors.New("control message exceeds bound")
	}
	h := make([]byte, 4)
	binary.BigEndian.PutUint32(h, uint32(len(b)))
	_, err = (framedStream{c}).Write(append(h, b...))
	return err
}

func controlRead(c net.Conn, want string) (controlMessage, error) {
	var v controlMessage
	h := make([]byte, 4)
	if _, err := io.ReadFull(c, h); err != nil {
		return v, err
	}
	n := binary.BigEndian.Uint32(h)
	if n > 4*1024*1024 {
		return v, errors.New("control message exceeds bound")
	}
	b := make([]byte, int(n))
	if _, err := io.ReadFull(c, b); err != nil {
		return v, err
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return v, err
	}
	if v.Kind != want {
		return v, fmt.Errorf("expected control %s, received %s", want, v.Kind)
	}
	return v, nil
}

func emit(v any) error {
	outputMu.Lock()
	defer outputMu.Unlock()
	return json.NewEncoder(output).Encode(v)
}

func run(o options) (runErr error) {
	if err := o.validate(); err != nil {
		return err
	}
	var control net.Conn
	if o.Mode == "server" {
		l, err := net.Listen("tcp", o.Control)
		if err != nil {
			return err
		}
		defer l.Close()
		_ = l.(*net.TCPListener).SetDeadline(time.Now().Add(60 * time.Second))
		control, err = l.Accept()
		if err != nil {
			return err
		}
	} else {
		var err error
		until := time.Now().Add(30 * time.Second)
		for {
			control, err = net.DialTimeout("tcp", o.Control, time.Second)
			if err == nil {
				break
			}
			if time.Now().After(until) {
				return err
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	defer control.Close()
	_ = control.SetDeadline(time.Now().Add(60 * time.Second))
	if o.Mode == "client" {
		b := make([]byte, 8)
		if _, err := rand.Read(b); err != nil {
			return err
		}
		o.Nonce = binary.BigEndian.Uint64(b)
		if err := controlWrite(control, controlMessage{Kind: "request", Options: &o, Nonce: o.Nonce}); err != nil {
			return err
		}
	} else {
		request, err := controlRead(control, "request")
		if err != nil {
			return err
		}
		if request.Options == nil || request.Options.Protocol != o.Protocol {
			return errors.New("client/server protocol mismatch")
		}
		local := o
		o = *request.Options
		o.Mode, o.Address, o.Control, o.Bind, o.Config, o.ProfilePrefix = local.Mode, local.Address, local.Control, local.Bind, local.Config, local.ProfilePrefix
		o.Nonce = request.Nonce
		if err := o.validate(); err != nil {
			return err
		}
	}
	t, err := openTransports(o)
	if err != nil {
		return err
	}
	defer t.close()
	if err := connectFlows(control, t, o); err != nil {
		return err
	}
	build, _ := debug.ReadBuildInfo()
	if err := emit(map[string]any{"type": "metadata", "side": o.Mode, "source_sha": sourceSHA, "options": o, "path_count": t.paths, "config": t.configMetadata, "build": build, "verification_header_bytes": headerSize, "dedup_window_packets": dedupSize}); err != nil {
		return err
	}
	finishProfiles, err := startProfiles(o.ProfilePrefix)
	if err != nil {
		return err
	}
	defer func() {
		if err := finishProfiles(); runErr == nil {
			runErr = err
		}
	}()
	_ = control.SetDeadline(time.Now().Add(time.Duration(o.Warmup+o.Seconds+30) * time.Second))
	receiver := (o.Mode == "client" && o.Direction == "download") || (o.Mode == "server" && o.Direction == "upload")
	var result summary
	if receiver {
		start := time.Now().Add(200 * time.Millisecond)
		if err := controlWrite(control, controlMessage{Kind: "start"}); err != nil {
			return err
		}
		result, err = receive(t, o, start)
		if err != nil {
			return err
		}
		if err := controlWrite(control, controlMessage{Kind: "result", Summary: &result}); err != nil {
			return err
		}
		peerResult, err := controlRead(control, "sender_result")
		if err != nil {
			return err
		}
		if peerResult.Summary == nil {
			return errors.New("missing sender summary")
		}
		if err := emit(map[string]any{"type": "remote_summary", "summary": peerResult.Summary}); err != nil {
			return err
		}
		return nil
	}
	if _, err := controlRead(control, "start"); err != nil {
		return err
	}
	result, err = send(t, o, control)
	if err != nil {
		return err
	}
	return controlWrite(control, controlMessage{Kind: "sender_result", Summary: &result})
}

func connectFlows(control net.Conn, t *transports, o options) error {
	if o.Mode == "server" {
		accept, err := t.listen(o)
		if err != nil {
			return err
		}
		if err := controlWrite(control, controlMessage{Kind: "listening"}); err != nil {
			return err
		}
		for i := 0; i < o.Flows; i++ {
			c, err := accept(i)
			if err != nil {
				return err
			}
			if o.Protocol != "udp" {
				t.conns = append(t.conns, c)
			}
			timer := time.AfterFunc(30*time.Second, func() { _ = c.Close() })
			want, b := payload(o.Payload, o.Nonce, i), make([]byte, o.Payload)
			n, err := c.Read(b)
			timer.Stop()
			if err != nil {
				return err
			}
			if !validPayload(b[:n], want) || binary.BigEndian.Uint32(b[20:]) != kindHello {
				return errors.New("invalid flow hello")
			}
			if err := controlWrite(control, controlMessage{Kind: "flow_ready", Flow: i}); err != nil {
				return err
			}
		}
		connected, err := controlRead(control, "connected")
		if err != nil {
			return err
		}
		if connected.Paths < 1 || connected.Paths > 1024 {
			return errors.New("invalid configured path count")
		}
		if t.peer == nil && connected.Paths != 1 {
			return errors.New("native connection cannot claim multiple paths")
		}
		t.paths = connected.Paths
		return nil
	}
	if _, err := controlRead(control, "listening"); err != nil {
		return err
	}
	for i := 0; i < o.Flows; i++ {
		c, err := t.dial(o, i)
		if err != nil {
			return err
		}
		t.conns = append(t.conns, c)
		hello := payload(o.Payload, o.Nonce, i)
		stamp(hello, kindHello, 0, 0)
		stop := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			for {
				if _, err := c.Write(hello); err != nil && !recoverableSend(err) {
					done <- err
					_ = control.Close()
					return
				}
				if o.Protocol != "udp" && o.Protocol != "mpudp" {
					done <- nil
					return
				}
				select {
				case <-stop:
					done <- nil
					return
				case <-time.After(100 * time.Millisecond):
				}
			}
		}()
		ready, err := controlRead(control, "flow_ready")
		close(stop)
		if err != nil {
			_ = c.Close()
		}
		if writeErr := <-done; writeErr != nil {
			return writeErr
		}
		if err != nil {
			return err
		}
		if ready.Flow != i {
			return errors.New("flow handshake mismatch")
		}
	}
	return controlWrite(control, controlMessage{Kind: "connected", Paths: t.paths})
}

func main() {
	if err := run(flags()); err != nil {
		fmt.Fprintln(os.Stderr, "perfprobe:", err)
		os.Exit(1)
	}
}
