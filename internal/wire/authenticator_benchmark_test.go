package wire

import (
	"fmt"
	"testing"
)

func BenchmarkAuthenticatorCodec(b *testing.B) {
	for _, size := range []int{480, 1129} {
		b.Run(fmt.Sprintf("shard%d", size), func(b *testing.B) {
			authenticator := newTestAuthenticator(b, []byte(testKey))
			benchmarkCodec(b, benchmarkCodecMessage(b, size), authenticator.Append, authenticator.Decode)
		})
	}
}

func BenchmarkAuthenticatorCacheMiss(b *testing.B) {
	authenticator := newTestAuthenticator(b, []byte(testKey))
	message := benchmarkCodecMessage(b, 480)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := authenticator.Append(nil, message, 1200); err != nil {
			b.Fatal(err)
		}
		// Discard the returned state to force a fresh fallback on every call.
		<-authenticator.states
	}
}
