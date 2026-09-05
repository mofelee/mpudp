package wire

import (
	"fmt"
	"testing"
)

func benchmarkCodecMessage(b *testing.B, size int) Message {
	b.Helper()
	message, err := NewDataShard(testSessionID, 1, 3, 2, 0, uint32(3*size), make([]byte, size))
	if err != nil {
		b.Fatal(err)
	}
	return message
}

func benchmarkCodec(b *testing.B, message Message,
	appendPacket func([]byte, Message, int) ([]byte, error),
	decodePacket func([]byte, int) (Message, error),
) {
	b.Helper()
	packet, err := appendPacket(nil, message, 1200)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := decodePacket(packet, 1200); err != nil {
		b.Fatal(err)
	}
	b.Run("encode_fresh", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := appendPacket(nil, message, 1200); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("encode_reused", func(b *testing.B) {
		buffer := make([]byte, 0, len(packet))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := appendPacket(buffer, message, 1200); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := decodePacket(packet, 1200); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decode_parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := decodePacket(packet, 1200); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}

func BenchmarkCodecStateless(b *testing.B) {
	key := []byte(testKey)
	for _, size := range []int{480, 1129} {
		b.Run(fmt.Sprintf("shard%d", size), func(b *testing.B) {
			benchmarkCodec(b, benchmarkCodecMessage(b, size),
				func(dst []byte, message Message, budget int) ([]byte, error) {
					return AppendAuthenticated(dst, message, key, budget)
				},
				func(packet []byte, limit int) (Message, error) {
					return DecodeAuthenticated(packet, key, limit)
				})
		})
	}
}
