package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mofelee/mpudp"
	"github.com/mofelee/mpudp/config"
)

func TestRunValidatesConfiguration(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, `carriers: ["127.0.0.1:9"]
fec: {data_shards: 3, parity_shards: 2}
psk: "test-key"
`)
	peer := &fakeRuntimePeer{mode: mpudp.ModeInitiator}
	var decoded config.Config
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stdout := newSignalBuffer()
	var stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- runContextWithPeerFactory(ctx, []string{"-config", path}, stdout, &stderr, func(_ context.Context, cfg config.Config) (runtimePeer, error) {
			decoded = cfg
			return peer, nil
		})
	}()

	select {
	case <-stdout.written:
	case <-time.After(2 * time.Second):
		t.Fatal("runContext did not report its running state")
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("runContext() code = %d, stderr = %q", code, stderr.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runContext did not stop after cancellation")
	}
	if got, want := stdout.String(), "mpudp: running mode=initiator\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	if !decoded.InitiatorEnabled() || decoded.ListenerEnabled() {
		t.Fatalf("decoded mode = carriers %v listen %q, want initiator", decoded.Carriers, decoded.Listen)
	}
	if peer.newSessionCalls != 1 || peer.closeCalls != 1 {
		t.Fatalf("NewSession/Close calls = %d/%d, want 1/1", peer.newSessionCalls, peer.closeCalls)
	}
}

func TestRunContextPreCanceledDoesNotBind(t *testing.T) {
	t.Parallel()
	address := reserveUDPAddress(t)
	path := writeConfig(t, fmt.Sprintf(`listen: %q
fec: {data_shards: 3, parity_shards: 2}
psk: "test-key"
`, address))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	if code := runContext(ctx, []string{"-config", path}, &stdout, &stderr); code != 0 {
		t.Fatalf("runContext() code = %d, stderr = %q", code, stderr.String())
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("pre-canceled output: stdout = %q, stderr = %q", stdout.String(), stderr.String())
	}
	probe, err := net.ListenPacket("udp4", address)
	if err != nil {
		t.Fatalf("pre-canceled run bound listener address: %v", err)
	}
	if err := probe.Close(); err != nil {
		t.Fatalf("close listener probe: %v", err)
	}
}

func TestMainHandlesSIGINTAndSIGTERM(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	tests := []struct {
		name   string
		signal os.Signal
	}{
		{name: "SIGINT", signal: os.Interrupt},
		{name: "SIGTERM", signal: syscall.SIGTERM},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			address := reserveUDPAddress(t)
			path := writeConfig(t, fmt.Sprintf(`listen: %q
fec: {data_shards: 3, parity_shards: 2}
psk: "test-key"
`, address))
			command := exec.Command(executable, "-test.run=^TestMainSignalHelper$")
			command.Env = append(os.Environ(),
				"MPUDP_TEST_MAIN_SIGNAL_HELPER=1",
				"MPUDP_TEST_MAIN_SIGNAL_CONFIG="+path,
			)
			stdout := newSignalBuffer()
			stderr := newSignalBuffer()
			command.Stdout = stdout
			command.Stderr = stderr
			if err := command.Start(); err != nil {
				t.Fatalf("start CLI subprocess: %v", err)
			}

			waitDone := make(chan error, 1)
			go func() { waitDone <- command.Wait() }()
			finished := false
			defer func() {
				if !finished {
					_ = command.Process.Kill()
					<-waitDone
				}
			}()

			select {
			case <-stdout.written:
			case waitErr := <-waitDone:
				finished = true
				t.Fatalf("CLI exited before signal: %v, stdout = %q, stderr = %q", waitErr, stdout.String(), stderr.String())
			case <-time.After(3 * time.Second):
				t.Fatalf("CLI did not reach running state; stderr = %q", stderr.String())
			}
			if err := command.Process.Signal(test.signal); err != nil {
				t.Fatalf("send %s: %v", test.name, err)
			}
			select {
			case waitErr := <-waitDone:
				finished = true
				if waitErr != nil {
					t.Fatalf("CLI exit after %s: %v, stdout = %q, stderr = %q", test.name, waitErr, stdout.String(), stderr.String())
				}
			case <-time.After(3 * time.Second):
				t.Fatalf("CLI did not exit after %s", test.name)
			}
			if got, want := stdout.String(), "mpudp: running mode=listener\n"; got != want {
				t.Fatalf("stdout = %q, want %q", got, want)
			}
			if stderr.String() != "" {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
			probe, err := net.ListenPacket("udp4", address)
			if err != nil {
				t.Fatalf("listener address remains bound after %s: %v", test.name, err)
			}
			if err := probe.Close(); err != nil {
				t.Fatalf("close listener probe: %v", err)
			}
		})
	}
}

func TestMainSignalHelper(t *testing.T) {
	if os.Getenv("MPUDP_TEST_MAIN_SIGNAL_HELPER") != "1" {
		return
	}
	configPath := os.Getenv("MPUDP_TEST_MAIN_SIGNAL_CONFIG")
	if configPath == "" {
		fmt.Fprintln(os.Stderr, "missing helper configuration")
		os.Exit(2)
	}
	os.Args = []string{"mpudp", "-config", configPath}
	main()
}

func TestRunRequiresConfigPath(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if code := runContext(context.Background(), nil, &stdout, &stderr); code != 2 {
		t.Fatalf("runContext() code = %d, want 2", code)
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "-config is required") {
		t.Fatalf("stdout = %q, stderr = %q", stdout.String(), stderr.String())
	}
}

func TestRunDoesNotLeakPSKOnConfigurationFailure(t *testing.T) {
	t.Parallel()
	const marker = "cli-PSK-DO-NOT-LEAK-a44b"
	path := writeConfig(t, `carriers: ["not-an-address"]
fec: {data_shards: 3, parity_shards: 2}
psk: "cli-PSK-DO-NOT-LEAK-a44b"
`)
	var stdout, stderr bytes.Buffer
	if code := runContext(context.Background(), []string{"-config", path}, &stdout, &stderr); code != 1 {
		t.Fatalf("runContext() code = %d, want 1", code)
	}
	if strings.Contains(stdout.String(), marker) || strings.Contains(stderr.String(), marker) {
		t.Fatalf("PSK leaked: stdout = %q, stderr = %q", stdout.String(), stderr.String())
	}
}

func TestRunBoundsOversizeConfigWithoutLeakingPSK(t *testing.T) {
	t.Parallel()
	const marker = "oversize-cli-PSK-DO-NOT-LEAK-c913"
	base := `carriers: ["example.com:4000"]
fec: {data_shards: 3, parity_shards: 2}
psk: "oversize-cli-PSK-DO-NOT-LEAK-c913"
`
	wantSize := config.MaxConfigBytes + 1
	contents := base + "#" + strings.Repeat("x", wantSize-len(base)-1)
	if len(contents) != wantSize {
		t.Fatalf("oversize fixture length = %d, want %d", len(contents), wantSize)
	}
	path := writeConfig(t, contents)
	var stdout, stderr bytes.Buffer
	if code := runContext(context.Background(), []string{"-config", path}, &stdout, &stderr); code != 1 {
		t.Fatalf("runContext() code = %d, want 1", code)
	}
	if strings.Contains(stdout.String(), marker) || strings.Contains(stderr.String(), marker) {
		t.Fatalf("PSK leaked: stdout = %q, stderr = %q", stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "maximum size") {
		t.Fatalf("stderr = %q, want size-limit diagnosis", stderr.String())
	}
}

func TestRunContextWaitsForCancellation(t *testing.T) {
	path := writeConfig(t, fmt.Sprintf(`listen: %q
fec: {data_shards: 3, parity_shards: 2}
psk: "test-key"
`, reserveUDPAddress(t)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stdout := newSignalBuffer()
	var stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- runContext(ctx, []string{"-config", path}, stdout, &stderr)
	}()

	select {
	case <-stdout.written:
	case <-time.After(2 * time.Second):
		t.Fatal("runContext did not report its running state")
	}
	select {
	case code := <-done:
		t.Fatalf("runContext returned before cancellation with code %d", code)
	default:
	}

	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("runContext() code = %d, stderr = %q", code, stderr.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runContext did not stop after cancellation")
	}
	if got, want := stdout.String(), "mpudp: running mode=listener\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunContextStartsInitiatorInDualMode(t *testing.T) {
	path := writeConfig(t, `carriers: ["127.0.0.1:9001"]
listen: "127.0.0.1:9002"
fec: {data_shards: 3, parity_shards: 2}
psk: "test-key"
`)
	peer := &fakeRuntimePeer{mode: mpudp.ModeDual}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	code := runContextWithPeerFactory(ctx, []string{"-config", path}, &stdout, &stderr, func(context.Context, config.Config) (runtimePeer, error) {
		return peer, nil
	})
	if code != 0 {
		t.Fatalf("runContextWithPeerFactory() code = %d, stderr = %q", code, stderr.String())
	}
	if peer.newSessionCalls != 1 {
		t.Fatalf("NewSession calls = %d, want 1", peer.newSessionCalls)
	}
	if peer.closeCalls != 1 {
		t.Fatalf("Close calls = %d, want 1", peer.closeCalls)
	}
}

func TestRunContextClosesPeerWhenInitiatorStartupFails(t *testing.T) {
	path := writeConfig(t, `carriers: ["127.0.0.1:9001"]
fec: {data_shards: 3, parity_shards: 2}
psk: "test-key"
`)
	marker := errors.New("injected startup failure")
	peer := &fakeRuntimePeer{mode: mpudp.ModeInitiator, newSessionErr: marker}
	var stdout, stderr bytes.Buffer
	code := runContextWithPeerFactory(context.Background(), []string{"-config", path}, &stdout, &stderr, func(context.Context, config.Config) (runtimePeer, error) {
		return peer, nil
	})
	if code != 1 {
		t.Fatalf("runContextWithPeerFactory() code = %d, want 1", code)
	}
	if peer.newSessionCalls != 1 || peer.closeCalls != 1 {
		t.Fatalf("NewSession/Close calls = %d/%d, want 1/1", peer.newSessionCalls, peer.closeCalls)
	}
	if !strings.Contains(stderr.String(), "start initiator Session") {
		t.Fatalf("stderr = %q, want startup diagnosis", stderr.String())
	}
}

func reserveUDPAddress(t *testing.T) string {
	t.Helper()
	connection, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve UDP address: %v", err)
	}
	address := connection.LocalAddr().String()
	if err := connection.Close(); err != nil {
		t.Fatalf("release UDP address: %v", err)
	}
	return address
}

type fakeRuntimePeer struct {
	mode            mpudp.Mode
	newSessionErr   error
	newSessionCalls int
	closeCalls      int
}

func (p *fakeRuntimePeer) Mode() mpudp.Mode { return p.mode }

func (p *fakeRuntimePeer) NewSession() (mpudp.Session, error) {
	p.newSessionCalls++
	return nil, p.newSessionErr
}

func (p *fakeRuntimePeer) Errors() <-chan error { return nil }

func (p *fakeRuntimePeer) Close() error {
	p.closeCalls++
	return nil
}

type signalBuffer struct {
	mu      sync.Mutex
	buffer  bytes.Buffer
	once    sync.Once
	written chan struct{}
}

func newSignalBuffer() *signalBuffer {
	return &signalBuffer{written: make(chan struct{})}
}

func (b *signalBuffer) Write(payload []byte) (int, error) {
	b.mu.Lock()
	written, err := b.buffer.Write(payload)
	b.mu.Unlock()
	b.once.Do(func() { close(b.written) })
	return written, err
}

func (b *signalBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "mpudp.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
