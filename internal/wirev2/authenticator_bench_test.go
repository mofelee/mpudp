package wirev2

import (
	"fmt"
	"testing"
)

func BenchmarkDirectionalAuthenticator(b *testing.B) {
	for _, size := range []int{512, 1200, MaxUDPPayload} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			a := newTestAuthenticator(b, Key{1})
			context := EncodingContext{Epoch: 7, LayoutID: 1, ProtectionID: 1, DataShards: 3, ParityShards: 2,
				ShardBytes: uint16(size - 94), MaxDescriptors: 1, MaxLogicalBytes: uint32(3 * (size - 94))}
			lookup := func(epoch uint32) (EncodingContext, bool) { return context, epoch == context.Epoch }
			bundle := FECBundle{Header: Header{Type: TypeFECBundle, SessionID: SessionID{1}}, Route: Route{1, 1, 1},
				Records: []FECRecord{{GroupID: 1, EncodingEpoch: 7, LogicalBytes: 24, ShardIndex: 3, Payload: make([]byte, size-94)}}}
			packet, err := a.AppendFECBundle(nil, bundle, lookup, size)
			if err != nil {
				b.Fatal(err)
			}
			envelope, err := ParseEnvelope(packet)
			if err != nil {
				b.Fatal(err)
			}
			for _, reuse := range []bool{false, true} {
				b.Run(fmt.Sprintf("encode_reused_%t", reuse), func(b *testing.B) {
					var dst []byte
					if reuse {
						dst = make([]byte, 0, size)
					}
					b.ReportAllocs()
					b.SetBytes(int64(size))
					for b.Loop() {
						packet, err := a.AppendFECBundle(dst, bundle, lookup, size)
						if err != nil {
							b.Fatal(err)
						}
						benchmarkFECBundlePacket = packet
					}
				})
			}
			b.Run("authenticate", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(size))
				for b.Loop() {
					authenticated, err := a.Authenticate(envelope)
					if err != nil {
						b.Fatal(err)
					}
					benchmarkAuthenticatedEnvelope = authenticated
				}
			})
		})
	}
}

func BenchmarkDirectionalAuthenticatorConstruction(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		a, err := NewAuthenticator(Key{1})
		if err != nil {
			b.Fatal(err)
		}
		a.Close()
	}
}
