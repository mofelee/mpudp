package wire

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"testing"
)

func FuzzDecodeArbitrary(f *testing.F) {
	for _, encoded := range goldenHexSeeds() {
		packet, _ := hex.DecodeString(encoded)
		f.Add(packet, []byte(testKey), uint32(MaxUDPPayload))
	}
	f.Add([]byte("not an MPUDP packet"), []byte("key"), uint32(MinUDPPayload))
	f.Fuzz(func(t *testing.T, packet, key []byte, rawLimit uint32) {
		limit := int(rawLimit % (MaxUDPPayload + 2))
		message, err := DecodeAuthenticated(packet, key, limit)
		if err != nil {
			return
		}
		length, err := EncodedLen(message)
		if err != nil {
			t.Fatalf("decoded message does not revalidate: %v", err)
		}
		if length != len(packet) {
			t.Fatalf("decoded length=%d, packet length=%d", length, len(packet))
		}
	})
}

func FuzzRoundTripBounded(f *testing.F) {
	f.Add(uint8(0), uint64(1), uint8(3), uint8(2), uint8(0), uint16(1200), []byte("payload"))
	f.Add(uint8(2), uint64(0), uint8(1), uint8(1), uint8(0), uint16(72), []byte{})
	f.Add(uint8(5), uint64(9), uint8(8), uint8(4), uint8(3), uint16(1000), []byte("close"))
	f.Fuzz(func(t *testing.T, rawType uint8, packetID uint64, rawData, rawParity, rawIndex uint8, rawBudget uint16, input []byte) {
		packetType := PacketType(rawType%6 + 1)
		dataShards := rawData%16 + 1
		parityShards := rawParity%uint8(17-dataShards) + 1
		total := uint16(dataShards) + uint16(parityShards)
		index := uint8(uint16(rawIndex) % total)
		budget := uint16(MinUDPPayload + int(rawBudget)%(MaxUDPPayload-MinUDPPayload+1))
		hasher := sha256.New()
		writeHash(hasher, []byte{rawType})
		writeHash(hasher, input)
		sessionHash := hasher.Sum(nil)
		var sessionID SessionID
		copy(sessionID[:], sessionHash[:16])
		if sessionID == (SessionID{}) {
			sessionID[15] = 1
		}

		var message Message
		var err error
		switch packetType {
		case TypeHello:
			message, err = NewHello(sessionID, dataShards, parityShards, budget)
		case TypeHelloAck:
			message, err = NewHelloAck(sessionID, dataShards, parityShards, budget)
		case TypeDataShard:
			payloadSize := len(input)%128 + 1
			payload := make([]byte, payloadSize)
			for i := range payload {
				if len(input) != 0 {
					payload[i] = input[i%len(input)]
				}
			}
			originalLength := uint32((payloadSize-1)*int(dataShards) + 1 + int(packetID%uint64(dataShards)))
			message, err = NewDataShard(sessionID, packetID, dataShards, parityShards, index, originalLength, payload)
		case TypePing:
			if packetID == 0 {
				packetID = 1
			}
			message, err = NewPing(sessionID, packetID, uint64(rawBudget))
		case TypePong:
			if packetID == 0 {
				packetID = 1
			}
			message, err = NewPong(sessionID, packetID, uint64(rawBudget))
		case TypeClose:
			message, err = NewClose(sessionID)
		}
		if err != nil {
			t.Fatalf("construct generated message: %v", err)
		}
		encoded, err := AppendAuthenticated(nil, message, []byte(testKey), MaxUDPPayload)
		if err != nil {
			t.Fatalf("encode generated message: %v", err)
		}
		decoded, err := DecodeAuthenticated(encoded, []byte(testKey), MaxUDPPayload)
		if err != nil {
			t.Fatalf("decode generated message: %v", err)
		}
		assertMessageEqual(t, decoded, message)
	})
}

func FuzzSingleBitTamper(f *testing.F) {
	for _, encoded := range goldenHexSeeds() {
		packet, _ := hex.DecodeString(encoded)
		f.Add(packet, uint32(0), uint8(1))
	}
	f.Fuzz(func(t *testing.T, packet []byte, rawIndex uint32, rawBit uint8) {
		if len(packet) == 0 {
			return
		}
		if _, err := DecodeAuthenticated(packet, []byte(testKey), MaxUDPPayload); err != nil {
			return
		}
		mutated := append([]byte(nil), packet...)
		index := int(uint64(rawIndex) % uint64(len(mutated)))
		mutated[index] ^= byte(1 << (rawBit % 8))
		if _, err := DecodeAuthenticated(mutated, []byte(testKey), MaxUDPPayload); err == nil {
			t.Fatalf("single-bit mutation was accepted at byte %d", index)
		}
	})
}

func writeHash(hasher hash.Hash, value []byte) {
	_, _ = hasher.Write(value)
}

func goldenHexSeeds() []string {
	return []string{
		"4d50554401010004000102030405060708090a0b0c0d0e0f030204b0833beb8bf2e1c9333c198a9e265f55f5a5e52156d02326e595b6a0f3aa6595e5",
		"4d50554401020004000102030405060708090a0b0c0d0e0f030203e854d2f8c301403ac388e15c5e30037a4a7db06e34b6776bba51fcd860cc642e36",
		"4d50554401030013000102030405060708090a0b0c0d0e0f01020304050607080302040000000adeadbeef08fb48f166b97aac758410baa7de3e0525f0c3c40682b45b290b1d45427cc4d2",
		"4d50554401040010000102030405060708090a0b0c0d0e0f1122334455667788010203040506070872d4e392c109689b758f6724a01afb3f3a2948dd5600852aa4690fa9769f2234",
		"4d50554401050010000102030405060708090a0b0c0d0e0f112233445566778801020304050607086ce02b78cb7aeaffd3c823ef0575d19c029f59bd0b6d1a0f37bf50fdd3444c3e",
		"4d50554401060000000102030405060708090a0b0c0d0e0f03108370ef4ec9963e864767fe5d57556de161e8b4bfd19577ee6f9d7324e943",
	}
}
