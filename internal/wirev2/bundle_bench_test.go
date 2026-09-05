package wirev2

import (
	"fmt"
	"testing"
)

var benchmarkFECBundlePacket []byte

func BenchmarkFECBundleAppend(b *testing.B) {
	for _, shape := range []struct {
		name                string
		records, shardBytes int
		budget              int
	}{
		{"one_512", 1, 418, 512},
		{"one_1200", 1, 1106, 1200},
		{"one_maximum", 1, MaxFECShardBytes, MaxUDPPayload},
		{"sixteen_1472", MaxFECRecords, 64, 1472},
	} {
		b.Run(shape.name, func(b *testing.B) {
			context := EncodingContext{Epoch: 7, LayoutID: 1, ProtectionID: 1, DataShards: 3, ParityShards: 2,
				ShardBytes: uint16(shape.shardBytes), MaxDescriptors: 1, MaxLogicalBytes: uint32(3 * shape.shardBytes)}
			lookup := func(epoch uint32) (EncodingContext, bool) { return context, epoch == context.Epoch }
			bundle := FECBundle{Header: Header{Type: TypeFECBundle, SessionID: SessionID{1}}, Route: Route{1, 1, 1}}
			for i := range shape.records {
				bundle.Records = append(bundle.Records, FECRecord{GroupID: uint64(i + 1), EncodingEpoch: 7,
					LogicalBytes: MinimumFECLogicalBytes, ShardIndex: 3, Payload: make([]byte, shape.shardBytes)})
			}
			packetBytes := TypedBodyOverhead + FECBundlePrefixSize + shape.records*(FECRecordHeaderSize+shape.shardBytes)
			for _, prefix := range []int{-1, 0, 8} {
				name := "nil"
				if prefix >= 0 {
					name = fmt.Sprintf("reused_prefix_%d", prefix)
				}
				b.Run(name, func(b *testing.B) {
					var dst []byte
					if prefix >= 0 {
						dst = make([]byte, prefix, prefix+packetBytes)
					}
					b.ReportAllocs()
					b.SetBytes(int64(packetBytes))
					for b.Loop() {
						packet, err := AppendFECBundle(dst, bundle, lookup, Key{1}, shape.budget)
						if err != nil {
							b.Fatal(err)
						}
						benchmarkFECBundlePacket = packet
					}
				})
			}
		})
	}
}
