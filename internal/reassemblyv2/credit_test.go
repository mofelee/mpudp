package reassemblyv2

import (
	"bytes"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/fecv2"
)

func TestCompletionReleasesRangeCreditWhileSourceAndDeliveryStayCharged(t *testing.T) {
	r, p, scope := fixture(t, func(l *Limits) { l.MaxFragments = 256 })
	base := p.Snapshot()
	payload := bytes.Repeat([]byte("x"), 1400)
	source, err := scope.Reserve(creditv2.Claim{Bytes: uint64(len(payload))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(source.Release)
	if done := add(t, r, start, fecv2.Fragment{DatagramID: 1, TotalBytes: 1400, Payload: payload[:700]}); len(done) != 0 {
		t.Fatal("incomplete original delivered")
	}
	if got, want := p.Snapshot().Bytes, base.Bytes+2*1400+256*8+deadlineLinkBytes; got != want {
		t.Fatalf("simultaneous source and reassembly ownership = %d, want %d", got, want)
	}
	done := add(t, r, start.Add(time.Millisecond), fecv2.Fragment{DatagramID: 1, TotalBytes: 1400, Offset: 700, Payload: payload[700:]}, fragment(2, 0, 0, ""))
	if len(done) != 2 || !bytes.Equal(done[0].Payload(), payload) || len(done[1].Payload()) != 0 {
		t.Fatal("completion lost source or empty-original boundaries")
	}
	for _, delivery := range done {
		t.Cleanup(delivery.Release)
	}
	if got := p.Snapshot(); got.Bytes != base.Bytes+2800 || got.Reservations != base.Reservations+3 {
		t.Fatalf("completed ranges remained charged or source/empty lease was released: %+v", got)
	}
	clear(payload)
	source.Release()
	r.Close()
	if got := p.Snapshot(); got.Bytes != 1400 || got.Reservations != 2 || done[0].Payload()[0] != 'x' {
		t.Fatalf("source/receiver teardown changed delivery ownership: %+v", got)
	}
	copyOfDelivery := *done[0]
	copyOfDelivery.Release()
	done[0].Release()
	if got := p.Snapshot(); got.Bytes != 0 || got.Reservations != 1 {
		t.Fatal("payload release changed the independently owned empty delivery")
	}
	done[1].Release()
	if p.Snapshot().Usage != (creditv2.Usage{}) {
		t.Fatal("empty delivery retained lease metadata after release")
	}
}
