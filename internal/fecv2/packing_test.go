package fecv2

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestPrefixPackingCapacityAndContinuation(t *testing.T) {
	c := testCodec(t)
	input := []Fragment{
		{DatagramID: 1, TotalBytes: 40, Payload: bytes.Repeat([]byte{1}, 40)},
		{DatagramID: 2, TotalBytes: 40, Payload: bytes.Repeat([]byte{2}, 40)},
	}
	group, cursor, err := c.EncodePrefix(input)
	if err != nil {
		t.Fatal(err)
	}
	if group.LogicalBytes != 96 || cursor != (Cursor{Next: 1, Bytes: 12}) {
		t.Fatalf("first group length=%d cursor=%+v", group.LogicalBytes, cursor)
	}
	got, err := c.Decode(group.LogicalBytes, group.Shards)
	if err != nil {
		t.Fatal(err)
	}
	assertFragments(t, got, []Fragment{input[0], {DatagramID: 2, TotalBytes: 40, Payload: input[1].Payload[:12]}})
	next := input[cursor.Next]
	next.Offset += uint32(cursor.Bytes)
	next.Payload = next.Payload[cursor.Bytes:]
	group, cursor, err = c.EncodePrefix([]Fragment{next})
	if err != nil {
		t.Fatal(err)
	}
	if cursor != (Cursor{Next: 1}) || group.LogicalBytes != 52 {
		t.Fatalf("tail length=%d cursor=%+v", group.LogicalBytes, cursor)
	}
	got, err = c.Decode(group.LogicalBytes, group.Shards)
	if err != nil {
		t.Fatal(err)
	}
	assertFragments(t, got, []Fragment{next})
	if input[1].Offset != 0 || len(input[1].Payload) != 40 {
		t.Fatal("packing mutated input cursor")
	}
}

func TestPrefixPackingEmptyLimitsAndFailureAtomicity(t *testing.T) {
	p := testCodec(t).params
	p.MaxLogicalBytes, p.MaxDescriptors = 24, 1
	c, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	input := []Fragment{{DatagramID: 1}, {DatagramID: 2, TotalBytes: 1, Payload: []byte{1}}}
	group, cursor, err := c.EncodePrefix(input)
	if err != nil || cursor != (Cursor{Next: 1}) || group.LogicalBytes != 24 {
		t.Fatalf("empty admission: %+v %+v %v", group, cursor, err)
	}
	group, cursor, err = c.EncodePrefix(input[1:])
	if !errors.Is(err, ErrNoPayloadCapacity) || cursor != (Cursor{}) || group.Shards != nil {
		t.Fatalf("no progress: %+v %+v %v", group, cursor, err)
	}
	for _, bad := range [][]Fragment{nil, make([]Fragment, 257), {{DatagramID: 1}, {DatagramID: 1}}, {{DatagramID: 1}, {DatagramID: 2, TotalBytes: 1}}} {
		group, cursor, err = c.EncodePrefix(bad)
		if !errors.Is(err, ErrInvalid) || cursor != (Cursor{}) || group.Shards != nil {
			t.Fatalf("invalid packing advanced: %+v %+v %v", group, cursor, err)
		}
	}
	p.MaxLogicalBytes = 96
	c, err = New(p)
	if err != nil {
		t.Fatal(err)
	}
	group, cursor, err = c.EncodePrefix([]Fragment{{DatagramID: 1}, {DatagramID: 2}})
	if err != nil || cursor != (Cursor{Next: 1}) || group.LogicalBytes != 24 {
		t.Fatalf("descriptor limit: %+v %+v %v", group, cursor, err)
	}
	got, err := c.Decode(group.LogicalBytes, group.Shards)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Payload == nil {
		t.Fatal("empty Datagram lost its payload identity")
	}
}

func TestFullStreamPackingPreservesOriginalsAndCapacityModel(t *testing.T) {
	const originals = 1000
	p := Parameters{DataShards: 3, ParityShards: 2, ShardBytes: 1378, MaxDescriptors: 32, MaxLogicalBytes: 4134, MaxDatagramBytes: 65536}
	c, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	input := make([]Fragment, originals)
	for i := range input {
		input[i] = Fragment{DatagramID: uint64(i + 1), TotalBytes: 1400, Payload: bytes.Repeat([]byte{byte(i)}, 1400)}
	}
	initial := append([]Fragment(nil), input...)
	decoded := make([][]byte, originals)
	groups, padding, descriptors, useful := 0, 0, 0, 0
	for len(input) > 0 {
		prefix := input[:min(len(input), MaxDescriptors)]
		group, cursor, err := c.EncodePrefix(prefix)
		if err != nil {
			t.Fatal(err)
		}
		if cursor == (Cursor{}) {
			t.Fatal("packer made no progress")
		}
		group.Shards[groups%5] = nil
		group.Shards[(groups+1)%5] = nil
		fragments, err := c.Decode(group.LogicalBytes, group.Shards)
		if err != nil {
			t.Fatal(err)
		}
		groups++
		padding += p.DataShards*p.ShardBytes - int(group.LogicalBytes)
		descriptors += len(fragments)
		for _, fragment := range fragments {
			index := int(fragment.DatagramID - 1)
			if fragment.Offset != uint32(len(decoded[index])) {
				t.Fatal("packing changed fragment order or overlap")
			}
			decoded[index] = append(decoded[index], fragment.Payload...)
			useful += len(fragment.Payload)
		}
		input = input[cursor.Next:]
		if cursor.Bytes > 0 {
			input = append([]Fragment(nil), input...)
			input[0].Offset += uint32(cursor.Bytes)
			input[0].Payload = input[0].Payload[cursor.Bytes:]
		}
	}
	for i, want := range initial {
		if !bytes.Equal(decoded[i], want.Payload) || want.Offset != 0 {
			t.Fatalf("original %d changed", i)
		}
	}
	if useful+ManifestBytes*groups+DescriptorBytes*descriptors+padding != groups*4134 {
		t.Fatal("group byte accounting does not balance")
	}
	// Include the final padded tail. This is codec arithmetic, not throughput.
	ceiling := 500 * float64(useful) / float64(groups*7500)
	if ceiling < 269 || ceiling > 271 {
		t.Fatalf("unexpected long-stream codec ceiling: %.3f", ceiling)
	}
	t.Logf("%d originals, %d groups, %d descriptors, %d padding bytes, %.3f Mbit/s arithmetic ceiling", originals, groups, descriptors, padding, ceiling)
}

func FuzzPrefixPacking(f *testing.F) {
	f.Add([]byte("short payload"), uint16(96), uint8(4))
	f.Add([]byte{}, uint16(0), uint8(1))
	f.Add([]byte("no room"), uint16(0), uint8(1))
	f.Fuzz(func(t *testing.T, data []byte, limit uint16, records uint8) {
		if len(data) > 1024 {
			return
		}
		p := Parameters{DataShards: 3, ParityShards: 2, ShardBytes: 64, MaxDescriptors: int(records)%8 + 1, MaxLogicalBytes: int(limit)%169 + 24, MaxDatagramBytes: 1024}
		c, err := New(p)
		if err != nil {
			t.Fatal(err)
		}
		input := []Fragment{{DatagramID: 1, TotalBytes: uint32(len(data)), Payload: bytes.Clone(data)}}
		before := append([]Fragment(nil), input...)
		var result []byte
		for len(input) > 0 {
			group, cursor, err := c.EncodePrefix(input)
			if errors.Is(err, ErrNoPayloadCapacity) {
				if p.MaxLogicalBytes != 24 || len(data) == 0 {
					t.Fatal("unexpected inability to progress")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			group.Shards[0], group.Shards[4] = nil, nil
			fragments, err := c.Decode(group.LogicalBytes, group.Shards)
			if err != nil {
				t.Fatal(err)
			}
			if len(fragments) != 1 || fragments[0].Offset != uint32(len(result)) {
				t.Fatal("invalid continuation")
			}
			result = append(result, fragments[0].Payload...)
			if cursor.Next == 1 {
				input = nil
			} else {
				if cursor.Bytes < 1 {
					t.Fatal("no progress")
				}
				next := input[0]
				next.Offset += uint32(cursor.Bytes)
				next.Payload = next.Payload[cursor.Bytes:]
				input = []Fragment{next}
			}
		}
		if !bytes.Equal(result, data) || !reflect.DeepEqual(before[0].Payload, data) {
			t.Fatal("packing changed original bytes")
		}
	})
}
