package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type captureTestConn struct {
	*attemptTestConn
	local, remote net.Addr
	localLookup   func()
}

func newCaptureTestConn(port int) *captureTestConn {
	return &captureTestConn{
		attemptTestConn: &attemptTestConn{closed: make(chan struct{})},
		local:           &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port},
		remote:          &net.UDPAddr{IP: net.IPv4(127, 0, 0, 2), Port: port + 1000},
	}
}

func (c *captureTestConn) LocalAddr() net.Addr {
	if c.localLookup != nil {
		c.localLookup()
	}
	return c.local
}

func (c *captureTestConn) RemoteAddr() net.Addr { return c.remote }

func newCaptureTestCarrier(t *testing.T, dial DialFunc) *Carrier {
	t.Helper()
	carrier, err := OpenCarrier(context.Background(), "capture", "remote", CarrierOptions{Dial: dial})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = carrier.Close() })
	return carrier
}

func TestCaptureSendPathKeepsCarrierGenerationAndAddresses(t *testing.T) {
	first, second := newCaptureTestConn(10001), newCaptureTestConn(10002)
	var dials atomic.Int32
	carrier := newCaptureTestCarrier(t, func(context.Context, string) (net.Conn, error) {
		if dials.Add(1) == 1 {
			return first, nil
		}
		return second, nil
	})
	captured, native, err := CaptureSendPath(carrier)
	if err != nil || !native || captured.Generation() != 1 || captured.PathID() != carrier.PathID() {
		t.Fatalf("capture native=%t err=%v path=%v", native, err, captured)
	}
	local, remote := captured.LocalAddr().String(), captured.RemoteAddr().String()
	for _, addr := range []net.Addr{first.local, first.remote, captured.LocalAddr(), captured.RemoteAddr()} {
		udp := addr.(*net.UDPAddr)
		clear(udp.IP)
		udp.Port = 1
	}
	if captured.LocalAddr().String() != local || captured.RemoteAddr().String() != remote {
		t.Fatal("mutable connection/getter addresses changed the captured tuple")
	}
	if at, err := SendWithAttempt(context.Background(), captured, []byte("first")); at.IsZero() || err != nil {
		t.Fatalf("captured send time=%v err=%v", at, err)
	}
	if err := carrier.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	stale, native, err := CaptureSendPath(captured)
	if err != nil || !native || stale.Generation() != 1 || stale.Available() ||
		stale.LocalAddr().String() != local || stale.RemoteAddr().String() != remote {
		t.Fatalf("recapture changed the stale binding: native=%t err=%v", native, err)
	}
	if err := stale.Send(context.Background(), []byte("stale")); !errors.Is(err, ErrGenerationReplaced) {
		t.Fatalf("stale ordinary send = %v", err)
	}
	if at, err := SendWithAttempt(context.Background(), stale, []byte("stale")); !at.IsZero() || !errors.Is(err, ErrGenerationReplaced) {
		t.Fatalf("stale timed send time=%v err=%v", at, err)
	}
	if first.writes.Load() != 1 || second.writes.Load() != 0 {
		t.Fatal("stale captured send reached a socket")
	}
	fresh, native, err := CaptureSendPath(carrier)
	if err != nil || !native || fresh.Generation() != 2 ||
		fresh.LocalAddr().String() != second.local.String() || fresh.RemoteAddr().String() != second.remote.String() {
		t.Fatalf("fresh capture native=%t err=%v", native, err)
	}
	if err := fresh.Send(context.Background(), []byte("fresh")); err != nil {
		t.Fatal(err)
	}
	if err := carrier.Send(context.Background(), []byte("current")); err != nil {
		t.Fatal(err)
	}
	if second.writes.Load() != 2 {
		t.Fatal("fresh capture or ordinary Carrier Send lost the current generation")
	}
	if err := carrier.Close(); err != nil {
		t.Fatal(err)
	}
	if at, err := SendWithAttempt(context.Background(), fresh, nil); !at.IsZero() || !errors.Is(err, ErrClosed) {
		t.Fatalf("closed captured path time=%v err=%v", at, err)
	}
}

func TestCaptureSendPathLocksGenerationAndAddressSnapshot(t *testing.T) {
	first, second := newCaptureTestConn(10001), newCaptureTestConn(10002)
	dialingReplacement := make(chan struct{})
	var dials atomic.Int32
	carrier := newCaptureTestCarrier(t, func(context.Context, string) (net.Conn, error) {
		if dials.Add(1) == 1 {
			return first, nil
		}
		close(dialingReplacement)
		return second, nil
	})
	lookingUp, continueLookup := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(continueLookup) }) }
	defer release()
	first.localLookup = func() {
		close(lookingUp)
		<-continueLookup
	}
	type result struct {
		path   ReplyPath
		native bool
		err    error
	}
	captured := make(chan result, 1)
	go func() {
		path, native, err := CaptureSendPath(carrier)
		captured <- result{path, native, err}
	}()
	select {
	case <-lookingUp:
	case <-time.After(time.Second):
		t.Fatal("capture did not reach the address lookup")
	}
	if carrier.mu.TryLock() {
		carrier.mu.Unlock()
		t.Fatal("capture did not keep the Carrier lock across its address lookup")
	}
	rebuilt := make(chan error, 1)
	go func() { rebuilt <- carrier.Rebuild(context.Background()) }()
	select {
	case <-dialingReplacement:
	case <-time.After(time.Second):
		t.Fatal("rebuild did not dial its replacement")
	}
	release()
	got := <-captured
	if got.err != nil || !got.native || got.path.Generation() != 1 ||
		got.path.LocalAddr().String() != first.local.String() || got.path.RemoteAddr().String() != first.remote.String() {
		t.Fatalf("capture mixed generations: native=%t err=%v", got.native, got.err)
	}
	if err := <-rebuilt; err != nil {
		t.Fatal(err)
	}
	if at, err := SendWithAttempt(context.Background(), got.path, nil); !at.IsZero() || !errors.Is(err, ErrGenerationReplaced) {
		t.Fatalf("replaced snapshot time=%v err=%v", at, err)
	}
	if first.writes.Load() != 0 || second.writes.Load() != 0 {
		t.Fatal("capture or rejected send wrote a packet")
	}
}

func TestCaptureSendPathRejectsMissingAndUnavailableCarrier(t *testing.T) {
	var nilCarrier *Carrier
	for name, path := range map[string]ReplyPath{"nil-interface": nil, "nil-carrier": nilCarrier} {
		t.Run(name, func(t *testing.T) {
			captured, native, err := CaptureSendPath(path)
			if captured != nil || native || !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("capture=%v native=%t err=%v", captured, native, err)
			}
		})
	}
	for _, state := range []string{"no-generation", "unavailable", "closed"} {
		t.Run(state, func(t *testing.T) {
			f := newAttemptFixture(t, "carrier")
			carrier, expected := f.path.(*Carrier), ErrPathUnavailable
			switch state {
			case "no-generation":
				carrier = new(Carrier)
			case "unavailable":
				carrier.current.alive.Store(false)
			case "closed":
				if err := carrier.Close(); err != nil {
					t.Fatal(err)
				}
				expected = ErrClosed
			}
			captured, native, err := CaptureSendPath(carrier)
			if captured != nil || native || !errors.Is(err, expected) || f.conn.writes.Load() != 0 {
				t.Fatalf("capture=%v native=%t err=%v", captured, native, err)
			}
		})
	}
}

func TestCaptureSendPathPreservesListenerSourceAndStatistics(t *testing.T) {
	f := newAttemptFixture(t, "listener")
	path := f.path.(listenerReplyPath)
	path.sourceControl = []byte{1, 2, 3}
	path.statistics = new(Counters)
	for _, closed := range []bool{false, true} {
		if closed {
			if err := f.close(); err != nil {
				t.Fatal(err)
			}
		}
		captured, native, err := CaptureSendPath(path)
		if err != nil || !native {
			t.Fatalf("listener capture native=%t err=%v", native, err)
		}
		got := captured.(listenerReplyPath)
		if got.listener != path.listener || got.generation != path.generation || got.pathID != path.pathID ||
			got.local.String() != path.local.String() || got.remote.String() != path.remote.String() || got.statistics != path.statistics ||
			len(got.sourceControl) != len(path.sourceControl) || &got.sourceControl[0] != &path.sourceControl[0] {
			t.Fatal("listener capture changed its socket, endpoint, source control or statistics")
		}
		if got.local == path.local || got.remote == path.remote {
			t.Fatal("listener capture retained mutable address backing")
		}
		if closed {
			if at, err := SendWithAttempt(context.Background(), captured, nil); !at.IsZero() || !errors.Is(err, ErrClosed) {
				t.Fatalf("closed listener time=%v err=%v", at, err)
			}
		}
	}
}

type mutableCaptureAddr struct{ value string }

func (*mutableCaptureAddr) Network() string  { return "udp" }
func (a *mutableCaptureAddr) String() string { return a.value }

func TestCaptureSendPathRejectsUnsupportedNativeAddresses(t *testing.T) {
	var nilUDP *net.UDPAddr
	var nilIP *net.IPAddr
	for name, unsupported := range map[string]net.Addr{
		"nil": nil, "nil-udp": nilUDP, "nil-ip": nilIP,
		"mutable-custom": &mutableCaptureAddr{value: "mutable"},
	} {
		for _, kind := range []string{"carrier", "captured-carrier", "listener"} {
			for _, side := range []string{"local", "remote"} {
				t.Run(name+"/"+kind+"/"+side, func(t *testing.T) {
					conn := newCaptureTestConn(10001)
					if side == "local" {
						conn.local = unsupported
					} else {
						conn.remote = unsupported
					}
					var path ReplyPath
					if kind == "listener" {
						listener, err := ServePacketConn(conn, ListenerOptions{PathID: "capture"})
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = listener.Close() })
						path = listenerReplyPath{listener: listener, generation: listener.Generation(), local: conn.local, remote: conn.remote}
					} else {
						carrier := newCaptureTestCarrier(t, func(context.Context, string) (net.Conn, error) { return conn, nil })
						path = carrier
						if kind == "captured-carrier" {
							path = carrierReplyPath{carrier: carrier, generation: carrier.Generation(), local: conn.local, remote: conn.remote}
						}
					}
					captured, native, err := CaptureSendPath(path)
					if captured != nil || native || !errors.Is(err, ErrDestinationUnsupported) || conn.writes.Load() != 0 {
						t.Fatalf("unsupported capture=%v native=%t err=%v", captured, native, err)
					}
				})
			}
		}
	}
}

func TestCaptureSendPathCopiesIPAddrBackingOnRecapture(t *testing.T) {
	f := newAttemptFixture(t, "captured-carrier")
	path := f.path.(carrierReplyPath)
	path.local = &net.IPAddr{IP: net.ParseIP("fe80::1"), Zone: "local-zone"}
	path.remote = &net.IPAddr{IP: net.ParseIP("fe80::2"), Zone: "remote-zone"}
	local, remote := path.local.String(), path.remote.String()
	captured, native, err := CaptureSendPath(path)
	if err != nil || !native {
		t.Fatalf("IPAddr capture native=%t err=%v", native, err)
	}
	for _, address := range []net.Addr{path.local, path.remote, captured.LocalAddr(), captured.RemoteAddr()} {
		ip := address.(*net.IPAddr)
		clear(ip.IP)
		ip.Zone = "changed"
	}
	if captured.LocalAddr().String() != local || captured.RemoteAddr().String() != remote {
		t.Fatal("IPAddr capture retained mutable address backing")
	}
}

func TestCaptureSendPathCustomFallbackDoesNotInvokeOrUnwrap(t *testing.T) {
	f := newAttemptFixture(t, "carrier")
	for _, path := range []ReplyPath{&untimedAttemptReply{err: io.ErrUnexpectedEOF}, &adaptedAttemptCarrier{Carrier: f.path.(*Carrier)}} {
		captured, native, err := CaptureSendPath(path)
		if captured != path || native || err != nil {
			t.Fatalf("custom capture changed the path: native=%t err=%v", native, err)
		}
		at, err := SendWithAttempt(context.Background(), captured, nil)
		if !at.IsZero() || err != io.ErrUnexpectedEOF || f.conn.writes.Load() != 0 {
			t.Fatalf("custom timed send time=%v err=%v", at, err)
		}
		switch path := path.(type) {
		case *untimedAttemptReply:
			if path.called != 1 {
				t.Fatalf("custom Send calls=%d", path.called)
			}
		case *adaptedAttemptCarrier:
			if path.called != 1 {
				t.Fatalf("adapter Send calls=%d", path.called)
			}
		}
	}
}
