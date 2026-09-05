package fecv2

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"math/bits"
	"reflect"
	"sync"
	"testing"
)

func testCodec(t testing.TB) *Codec {
	t.Helper()
	c, err := New(Parameters{DataShards: 3, ParityShards: 2, ShardBytes: 32, MaxDescriptors: 4, MaxLogicalBytes: 96, MaxDatagramBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestManifestVectorAndEveryRecoverySubset(t *testing.T) {
	c := testCodec(t)
	fragments := []Fragment{
		{DatagramID: 1, TotalBytes: 0},
		{DatagramID: 7, TotalBytes: 9, Offset: 2, Payload: []byte("abc")},
		{DatagramID: math.MaxUint64, TotalBytes: 2, Payload: []byte("XY")},
	}
	group, err := c.Encode(fragments)
	if err != nil {
		t.Fatal(err)
	}
	// Literal big-endian manifest, independent of encoder/decoder helpers.
	want, err := hex.DecodeString("00010003" +
		"0000000000000001000000000000000000000000" +
		"0000000000000007000000090000000200000003" +
		"ffffffffffffffff000000020000000000000002" + "6162635859")
	if err != nil {
		t.Fatal(err)
	}
	logical := bytes.Join(group.Shards[:3], nil)
	if group.LogicalBytes != uint32(len(want)) || !bytes.Equal(logical[:group.LogicalBytes], want) {
		t.Fatalf("manifest = %x, want %x", logical[:group.LogicalBytes], want)
	}
	if !bytes.Equal(logical[group.LogicalBytes:], make([]byte, len(logical)-len(want))) {
		t.Fatal("tail was not zero padded")
	}
	for _, shard := range group.Shards {
		if len(shard) != 32 || cap(shard) != 32 {
			t.Fatal("shard boundary differs from context")
		}
	}
	for mask := 0; mask < 1<<len(group.Shards); mask++ {
		input := make([][]byte, len(group.Shards))
		for i := range input {
			if mask&(1<<i) != 0 {
				input[i] = bytes.Clone(group.Shards[i])
			}
		}
		before := cloneShards(input)
		got, err := c.Decode(group.LogicalBytes, input)
		if !reflect.DeepEqual(input, before) {
			t.Fatalf("mask %05b: input mutated", mask)
		}
		if bits.OnesCount(uint(mask)) < 3 {
			if got != nil || !errors.Is(err, ErrInsufficientShards) {
				t.Fatalf("mask %05b: %v, %v", mask, got, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("mask %05b: %v", mask, err)
		}
		assertFragments(t, got, fragments)
		if got[0].Payload == nil {
			t.Fatal("empty Datagram decoded as absent payload")
		}
		for _, f := range got {
			if cap(f.Payload) != len(f.Payload) {
				t.Fatal("payload can overwrite another fragment")
			}
		}
		for _, shard := range input {
			clear(shard)
		}
		assertFragments(t, got, fragments)
	}
	fragments[1].Payload[0] = 'z'
	if !bytes.Equal(bytes.Join(group.Shards[:3], nil)[:len(want)], want) {
		t.Fatal("Encode retained caller payload")
	}
}

func cloneShards(input [][]byte) [][]byte {
	result := make([][]byte, len(input))
	for i := range input {
		result[i] = bytes.Clone(input[i])
	}
	return result
}

func assertFragments(t testing.TB, got, want []Fragment) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("fragment count %d, want %d", len(got), len(want))
	}
	for i, f := range got {
		w := want[i]
		if f.DatagramID != w.DatagramID || f.TotalBytes != w.TotalBytes || f.Offset != w.Offset || !bytes.Equal(f.Payload, w.Payload) {
			t.Fatalf("fragment %d = %+v, want %+v", i, f, w)
		}
	}
}

func TestInvalidParameters(t *testing.T) {
	base := testCodec(t).params
	for _, change := range []func(*Parameters){
		func(p *Parameters) { p.DataShards = 0 }, func(p *Parameters) { p.DataShards = 256 },
		func(p *Parameters) { p.ParityShards = 0 }, func(p *Parameters) { p.ParityShards = 256 },
		func(p *Parameters) { p.DataShards = 255; p.ParityShards = 2 },
		func(p *Parameters) { p.ShardBytes = 0 }, func(p *Parameters) { p.ShardBytes = MaxShardBytes + 1 },
		func(p *Parameters) { p.MaxDescriptors = 0 }, func(p *Parameters) { p.MaxDescriptors = 257 },
		func(p *Parameters) { p.MaxLogicalBytes = 23 }, func(p *Parameters) { p.MaxLogicalBytes = 97 },
		func(p *Parameters) { p.MaxLogicalBytes = MaxLogicalBytes + 1 },
		func(p *Parameters) { p.MaxDatagramBytes = 0 }, func(p *Parameters) { p.MaxDatagramBytes = MaxDatagramBytes + 1 },
	} {
		p := base
		change(&p)
		if c, err := New(p); c != nil || !errors.Is(err, ErrInvalid) {
			t.Fatalf("accepted %+v: %v", p, err)
		}
	}
}

func TestEncodeRejectionAndExactCapacity(t *testing.T) {
	c := testCodec(t)
	for _, fragments := range [][]Fragment{
		nil,
		{{DatagramID: 0}},
		{{DatagramID: 2}, {DatagramID: 1}},
		{{DatagramID: 1}, {DatagramID: 1}},
		{{DatagramID: 1, TotalBytes: 1025, Payload: []byte{1}}},
		{{DatagramID: 1, Offset: 1}},
		{{DatagramID: 1, TotalBytes: 1}},
		{{DatagramID: 1, TotalBytes: 2, Offset: 2, Payload: []byte{1}}},
		{{DatagramID: 1, TotalBytes: math.MaxUint32, Offset: math.MaxUint32, Payload: []byte{1}}},
		{{DatagramID: 1, TotalBytes: 73, Payload: make([]byte, 73)}},
		{{DatagramID: 1}, {DatagramID: 2}, {DatagramID: 3}, {DatagramID: 4}, {DatagramID: 5}},
	} {
		group, err := c.Encode(fragments)
		if group.Shards != nil || group.LogicalBytes != 0 || !errors.Is(err, ErrInvalid) {
			t.Fatalf("accepted %+v: %v", fragments, err)
		}
	}
	fragments := []Fragment{{DatagramID: 1, TotalBytes: 72, Payload: bytes.Repeat([]byte{9}, 72)}}
	group, err := c.Encode(fragments)
	if err != nil || group.LogicalBytes != 96 {
		t.Fatalf("exact capacity: %+v %v", group, err)
	}
	got, err := c.Decode(group.LogicalBytes, group.Shards)
	if err != nil {
		t.Fatal(err)
	}
	assertFragments(t, got, fragments)
}

// Construct a consistent codeword with malformed logical bytes or padding.
// This ensures semantic rejection is tested after successful RS verification.
func recode(t testing.TB, c *Codec, data []byte) [][]byte {
	t.Helper()
	backing := make([]byte, (c.params.DataShards+c.params.ParityShards)*c.params.ShardBytes)
	copy(backing, data)
	shards := shardSlices(backing, c.params.ShardBytes)
	if err := c.rs.Encode(shards); err != nil {
		t.Fatal(err)
	}
	return shards
}

func TestDecodeRejectsMalformedManifestAndPadding(t *testing.T) {
	c := testCodec(t)
	group, err := c.Encode([]Fragment{{DatagramID: 1, TotalBytes: 3, Payload: []byte("abc")}, {DatagramID: 2}})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Join(group.Shards[:3], nil)
	for name, change := range map[string]func([]byte){
		"version":             func(b []byte) { b[1] = 2 },
		"zero count":          func(b []byte) { b[3] = 0 },
		"large count":         func(b []byte) { binary.BigEndian.PutUint16(b[2:], 257) },
		"zero ID":             func(b []byte) { clear(b[4:12]) },
		"duplicate ID":        func(b []byte) { binary.BigEndian.PutUint64(b[24:], 1) },
		"reordered ID":        func(b []byte) { binary.BigEndian.PutUint64(b[4:], 3) },
		"large total":         func(b []byte) { binary.BigEndian.PutUint32(b[12:], 1025) },
		"overflow offset":     func(b []byte) { binary.BigEndian.PutUint32(b[16:], math.MaxUint32) },
		"overflow length":     func(b []byte) { binary.BigEndian.PutUint32(b[20:], math.MaxUint32) },
		"payload after empty": func(b []byte) { binary.BigEndian.PutUint32(b[32:], 1) },
		"unconsumed bytes":    func(b []byte) { binary.BigEndian.PutUint32(b[20:], 2) },
		"tail padding":        func(b []byte) { b[group.LogicalBytes] = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			malformed := bytes.Clone(data)
			change(malformed)
			got, err := c.Decode(group.LogicalBytes, recode(t, c, malformed))
			if got != nil || !errors.Is(err, ErrInvalid) {
				t.Fatalf("malformed decode: %+v %v", got, err)
			}
		})
	}
	for _, size := range []uint32{0, 23, 44, group.LogicalBytes + 1, 97, math.MaxUint32} {
		if got, err := c.Decode(size, group.Shards); got != nil || !errors.Is(err, ErrInvalid) {
			t.Fatalf("logical size %d accepted: %v", size, err)
		}
	}
	for _, size := range []int{0, 31, 33} {
		shards := cloneShards(group.Shards)
		shards[0] = make([]byte, size)
		if got, err := c.Decode(group.LogicalBytes, shards); got != nil || !errors.Is(err, ErrInvalid) {
			t.Fatalf("shard size %d accepted: %v", size, err)
		}
	}
	shards := cloneShards(group.Shards)
	shards[4][0] ^= 1
	if got, err := c.Decode(group.LogicalBytes, shards); got != nil || !errors.Is(err, ErrInvalid) {
		t.Fatal("inconsistent parity accepted")
	}
	if got, err := c.Decode(group.LogicalBytes, group.Shards[:4]); got != nil || !errors.Is(err, ErrInvalid) {
		t.Fatal("wrong shard count accepted")
	}
}

func TestConcurrentCodecAndMaximumShardCount(t *testing.T) {
	c := testCodec(t)
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 20 {
				fragments := []Fragment{{DatagramID: 1, TotalBytes: 3, Payload: []byte("abc")}}
				group, err := c.Encode(fragments)
				if err != nil {
					t.Error(err)
					return
				}
				group.Shards[0] = nil
				group.Shards[4] = nil
				got, err := c.Decode(group.LogicalBytes, group.Shards)
				if err != nil {
					t.Error(err)
					return
				}
				assertFragments(t, got, fragments)
			}
		}()
	}
	workers.Wait()
	for _, p := range []Parameters{
		{DataShards: 1, ParityShards: 1, ShardBytes: 24, MaxDescriptors: 1, MaxLogicalBytes: 24, MaxDatagramBytes: 1},
		{DataShards: 255, ParityShards: 1, ShardBytes: 1, MaxDescriptors: 1, MaxLogicalBytes: 24, MaxDatagramBytes: 1},
	} {
		c, err := New(p)
		if err != nil {
			t.Fatal(err)
		}
		group, err := c.Encode([]Fragment{{DatagramID: 1}})
		if err != nil {
			t.Fatal(err)
		}
		group.Shards[0] = nil
		got, err := c.Decode(group.LogicalBytes, group.Shards)
		if err != nil {
			t.Fatal(err)
		}
		assertFragments(t, got, []Fragment{{DatagramID: 1}})
	}
}

func FuzzManifest(f *testing.F) {
	f.Add([]byte{0, 1, 0, 1})
	f.Add([]byte{0, 1, 255, 255})
	for _, vector := range []string{
		"000100010000000000000001000000000000000000000000",
		"000100010000000000000001000000030000000000000003616263",
	} {
		seed, err := hex.DecodeString(vector)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 96 {
			return
		}
		c := testCodec(t)
		fragments, err := c.parseManifest(bytes.Clone(data))
		if err != nil {
			return
		}
		group, err := c.Encode(fragments)
		if err != nil {
			t.Fatalf("parsed manifest cannot re-encode: %v", err)
		}
		if !bytes.Equal(bytes.Join(group.Shards[:3], nil)[:group.LogicalBytes], data) {
			t.Fatal("accepted noncanonical manifest")
		}
	})
}
