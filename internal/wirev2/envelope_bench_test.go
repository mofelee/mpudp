package wirev2

import (
	"fmt"
	"testing"
)

var benchmarkAuthenticatedEnvelope AuthenticatedEnvelope

func BenchmarkEnvelopeAuthenticate(b *testing.B) {
	for _, size := range []int{512, 1200, MaxUDPPayload} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			key := Key{1}
			packet, err := AppendEnvelope(nil, Header{Type: TypeFECBundle, SessionID: SessionID{1}}, make([]byte, size-EnvelopeOverhead), key)
			if err != nil {
				b.Fatal(err)
			}
			envelope, err := ParseEnvelope(packet)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				authenticated, err := envelope.Authenticate(key)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkAuthenticatedEnvelope = authenticated
			}
		})
	}
}
