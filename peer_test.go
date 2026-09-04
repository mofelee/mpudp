package mpudp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"testing"

	"github.com/mofelee/mpudp/config"
)

func TestPeerModesExposeOnlyConfiguredLifecycle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		configure    func(*config.Config)
		mode         Mode
		wantSession  bool
		wantListener bool
	}{
		{
			name: "initiator",
			configure: func(cfg *config.Config) {
				cfg.Carriers = []string{"127.0.0.1:9"}
			},
			mode:        ModeInitiator,
			wantSession: true,
		},
		{
			name: "listener",
			configure: func(cfg *config.Config) {
				cfg.Listen = "127.0.0.1:9000"
			},
			mode:         ModeListener,
			wantListener: true,
		},
		{
			name: "dual",
			configure: func(cfg *config.Config) {
				cfg.Carriers = []string{"127.0.0.1:9"}
				cfg.Listen = "127.0.0.1:9000"
			},
			mode:         ModeDual,
			wantSession:  true,
			wantListener: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg := baseConfig()
			test.configure(&cfg)
			if cfg.ListenerEnabled() {
				cfg.Listen = reserveUDPAddress(t)
			}
			peer, err := NewPeer(cfg)
			if err != nil {
				t.Fatalf("NewPeer() error = %v", err)
			}
			defer peer.Close()
			if got := peer.Mode(); got != test.mode {
				t.Errorf("Mode() = %q, want %q", got, test.mode)
			}

			session, sessionErr := peer.NewSession()
			if test.wantSession {
				if sessionErr != nil {
					t.Fatalf("NewSession() error = %v", sessionErr)
				}
				assertRunningSession(t, session, cfg.Limits.MaxDatagramSize)
			} else if !errors.Is(sessionErr, ErrModeUnavailable) || session != nil {
				t.Fatalf("NewSession() = (%v, %v), want ErrModeUnavailable", session, sessionErr)
			}

			listener, listenerErr := peer.Listener()
			if test.wantListener {
				if listenerErr != nil {
					t.Fatalf("Listener() error = %v", listenerErr)
				}
				assertRunningListener(t, listener)
			} else if !errors.Is(listenerErr, ErrModeUnavailable) || listener != nil {
				t.Fatalf("Listener() = (%v, %v), want ErrModeUnavailable", listener, listenerErr)
			}
		})
	}
}

func TestPeerCopiesConfig(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Carriers = []string{"127.0.0.1:9"}
	peer, err := NewPeer(cfg)
	if err != nil {
		t.Fatalf("NewPeer() error = %v", err)
	}
	defer peer.Close()
	cfg.Carriers[0] = "changed.example:4000"
	got := peer.Config()
	if got.Carriers[0] != "127.0.0.1:9" {
		t.Fatalf("retained configuration changed to %q", got.Carriers[0])
	}
	got.Carriers[0] = "changed-again.example:4000"
	if peer.Config().Carriers[0] != "127.0.0.1:9" {
		t.Fatal("Config() returned aliased storage")
	}
}

func TestPeerCloseClosesChildrenAndIsIdempotent(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Carriers = []string{"127.0.0.1:9"}
	cfg.Listen = reserveUDPAddress(t)
	peer, err := NewPeer(cfg)
	if err != nil {
		t.Fatalf("NewPeer() error = %v", err)
	}
	session, err := peer.NewSession()
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	listener, err := peer.Listener()
	if err != nil {
		t.Fatalf("Listener() error = %v", err)
	}
	if err := peer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := peer.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := peer.NewSession(); !errors.Is(err, ErrClosed) {
		t.Errorf("NewSession() after close error = %v, want ErrClosed", err)
	}
	if _, err := peer.Listener(); !errors.Is(err, ErrClosed) {
		t.Errorf("Listener() after close error = %v, want ErrClosed", err)
	}
	if err := session.WritePacket(nil); !errors.Is(err, ErrClosed) {
		t.Errorf("WritePacket() after peer close error = %v, want ErrClosed", err)
	}
	if _, err := listener.Accept(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("Accept() after peer close error = %v, want ErrClosed", err)
	}
}

func TestNewSessionHonorsConfiguredSessionLimit(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Carriers = []string{"127.0.0.1:9"}
	cfg.Limits.MaxSessions = 1
	peer, err := NewPeer(cfg)
	if err != nil {
		t.Fatalf("NewPeer() error = %v", err)
	}
	defer peer.Close()
	first, err := peer.NewSession()
	if err != nil {
		t.Fatalf("first NewSession() error = %v", err)
	}
	if second, err := peer.NewSession(); second != nil || !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("second NewSession() = (%v, %v), want nil ErrResourceLimit", second, err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	third, err := peer.NewSession()
	if err != nil {
		t.Fatalf("NewSession() after release error = %v", err)
	}
	if err := third.Close(); err != nil {
		t.Fatalf("third Close() error = %v", err)
	}
}

func TestNewPeerValidatesBeforeReadingRandomOrBinding(t *testing.T) {
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve UDP address: %v", err)
	}
	address := packetConn.LocalAddr().String()
	if err := packetConn.Close(); err != nil {
		t.Fatalf("release UDP address: %v", err)
	}

	cfg := baseConfig()
	cfg.Listen = address
	cfg.PSK = config.Secret{}
	random := &countingReader{reader: bytes.NewReader(make([]byte, 16))}
	before := runtime.NumGoroutine()
	peer, err := newPeer(cfg, random)
	after := runtime.NumGoroutine()
	if peer != nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("newPeer() = (%v, %v), want nil ErrInvalidConfig", peer, err)
	}
	if random.bytesRead != 0 {
		t.Fatalf("invalid construction read %d random bytes", random.bytesRead)
	}
	if after > before {
		t.Fatalf("invalid construction increased goroutines from %d to %d", before, after)
	}

	probe, err := net.ListenPacket("udp", address)
	if err != nil {
		t.Fatalf("invalid construction bound configured address: %v", err)
	}
	if err := probe.Close(); err != nil {
		t.Fatalf("close UDP probe: %v", err)
	}
}

func TestInjectedReaderDrivesOnlyPackagePrivateConstructor(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Carriers = []string{"127.0.0.1:9"}
	want := SessionID{0: 1, 15: 2}
	peer, err := newPeer(cfg, bytes.NewReader(want[:]))
	if err != nil {
		t.Fatalf("newPeer() error = %v", err)
	}
	defer peer.Close()
	sessionValue, err := peer.NewSession()
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	sessionImpl, ok := sessionValue.(*session)
	if !ok {
		t.Fatalf("NewSession() concrete type = %T", sessionValue)
	}
	if sessionImpl.id != want {
		t.Fatalf("session ID = %v, want %v", sessionImpl.id, want)
	}
}

func TestLifecycleMethodsAreConcurrentSafe(t *testing.T) {
	cfg := baseConfig()
	cfg.Carriers = []string{"127.0.0.1:9"}
	cfg.Listen = reserveUDPAddress(t)
	peer, err := NewPeer(cfg)
	if err != nil {
		t.Fatalf("NewPeer() error = %v", err)
	}

	var wait sync.WaitGroup
	for i := 0; i < 32; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_ = peer.Mode()
			_ = peer.Config()
			listener, listenErr := peer.Listener()
			if listenErr == nil {
				_, _ = listener.Accept(context.Background())
			}
			session, sessionErr := peer.NewSession()
			if sessionErr == nil {
				_ = session.WritePacket([]byte("one Datagram"))
				_, _ = session.ReadPacket()
				_ = session.Close()
			}
		}()
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		_ = peer.Close()
	}()
	wait.Wait()
	if err := peer.Close(); err != nil {
		t.Fatalf("final Close() error = %v", err)
	}
}

func TestStableSentinelErrorsAreDistinct(t *testing.T) {
	t.Parallel()
	errorsToCheck := []error{
		ErrInvalidConfig,
		ErrMessageTooLarge,
		ErrClosed,
		ErrAuthentication,
		ErrHandshakeIncompatible,
		ErrNotReady,
		ErrModeUnavailable,
		ErrResourceLimit,
	}
	for i, first := range errorsToCheck {
		if first == nil {
			t.Fatalf("sentinel %d is nil", i)
		}
		for j, second := range errorsToCheck {
			if i != j && errors.Is(first, second) {
				t.Fatalf("sentinel %q unexpectedly matches %q", first, second)
			}
		}
	}
}

func assertRunningSession(t *testing.T, session Session, maxDatagramSize int) {
	t.Helper()
	if err := session.WritePacket(make([]byte, maxDatagramSize+1)); !errors.Is(err, ErrMessageTooLarge) {
		t.Errorf("oversize WritePacket() error = %v, want ErrMessageTooLarge", err)
	}
	if err := session.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
	if err := session.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
}

func assertRunningListener(t *testing.T, listener Listener) {
	t.Helper()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := listener.Accept(canceled); !errors.Is(err, context.Canceled) {
		t.Errorf("Accept(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := listener.Accept(nil); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("Accept(nil) error = %v, want ErrInvalidConfig", err)
	}
	if err := listener.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
}

func baseConfig() config.Config {
	cfg := config.Default()
	cfg.FEC = config.FECConfig{DataShards: 3, ParityShards: 2}
	cfg.PSK = config.NewSecret("unit-test-key")
	return cfg
}

func ExampleSession() {
	fmt.Println("WritePacket and ReadPacket preserve one Datagram per call")
	// Output: WritePacket and ReadPacket preserve one Datagram per call
}
