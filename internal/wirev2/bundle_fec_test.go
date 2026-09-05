package wirev2

import (
	"bytes"
	"fmt"
	"math/bits"
	"testing"

	"github.com/mofelee/mpudp/internal/fecv2"
)

func shardSelections(total, required int) [][]int {
	var result [][]int
	for mask := uint(0); mask < 1<<total; mask++ {
		if bits.OnesCount(mask) != required {
			continue
		}
		var selection []int
		for index := 0; index < total; index++ {
			if mask&(1<<index) != 0 {
				selection = append(selection, index)
			}
		}
		result = append(result, selection)
	}
	return result
}

func TestRealFECMixedEpochBundlesRecoverEveryAnyKSelection(t *testing.T) {
	contexts := []EncodingContext{testContext(7), testContext(8)}
	contexts[1].DataShards, contexts[1].ParityShards, contexts[1].ShardBytes = 2, 1, 96
	fragments := [][]fecv2.Fragment{
		{{DatagramID: 1}, {DatagramID: 4, TotalBytes: 100, Payload: bytes.Repeat([]byte{0x4a}, 100)}},
		{{DatagramID: 2, TotalBytes: 80, Offset: 12, Payload: bytes.Repeat([]byte{0x8b}, 50)}},
	}
	codecs := make([]*fecv2.Codec, len(contexts))
	groups := make([]fecv2.Group, len(contexts))
	for index, context := range contexts {
		codec, err := fecv2.New(fecv2.Parameters{
			DataShards: int(context.DataShards), ParityShards: int(context.ParityShards),
			ShardBytes: int(context.ShardBytes), MaxDescriptors: int(context.MaxDescriptors),
			MaxLogicalBytes: int(context.MaxLogicalBytes), MaxDatagramBytes: 65536,
		})
		if err != nil {
			t.Fatal(err)
		}
		group, err := codec.Encode(fragments[index])
		if err != nil {
			t.Fatal(err)
		}
		codecs[index], groups[index] = codec, group
	}
	lookup := func(epoch uint32) (EncodingContext, bool) {
		for _, context := range contexts {
			if context.Epoch == epoch {
				return context, true
			}
		}
		return EncodingContext{}, false
	}
	for _, first := range shardSelections(5, 3) {
		for _, second := range shardSelections(3, 2) {
			t.Run(fmt.Sprintf("%v/%v", first, second), func(t *testing.T) {
				selections := [][]int{first, second}
				received := [][][]byte{make([][]byte, 5), make([][]byte, 3)}
				for path := 0; path < len(first); path++ {
					bundle := FECBundle{Header: Header{Type: TypeFECBundle, SessionID: SessionID{1}}, Route: Route{PathID: uint32(path + 1), Generation: 1, BudgetEpoch: uint32(path + 1)}}
					for groupIndex, selection := range selections {
						if path >= len(selection) {
							continue
						}
						shardIndex := selection[path]
						bundle.Records = append(bundle.Records, FECRecord{
							GroupID: uint64(groupIndex + 100), EncodingEpoch: contexts[groupIndex].Epoch,
							LogicalBytes: groups[groupIndex].LogicalBytes, ShardIndex: uint8(shardIndex), Payload: groups[groupIndex].Shards[shardIndex],
						})
					}
					packet, err := AppendFECBundle(nil, bundle, lookup, Key{1}, 512)
					if err != nil {
						t.Fatal(err)
					}
					decoded, err := DecodeFECBundle(authenticate(t, packet, Key{1}), lookup, 512)
					if err != nil {
						t.Fatal(err)
					}
					for _, record := range decoded.Records {
						received[record.GroupID-100][record.ShardIndex] = record.Payload
					}
					clear(packet)
				}
				for groupIndex, codec := range codecs {
					actual, err := codec.Decode(groups[groupIndex].LogicalBytes, received[groupIndex])
					if err != nil {
						t.Fatal(err)
					}
					expected := fragments[groupIndex]
					if len(actual) != len(expected) {
						t.Fatalf("fragment count=%d want=%d", len(actual), len(expected))
					}
					for index, fragment := range actual {
						want := expected[index]
						if fragment.DatagramID != want.DatagramID || fragment.TotalBytes != want.TotalBytes || fragment.Offset != want.Offset || !bytes.Equal(fragment.Payload, want.Payload) {
							t.Fatalf("group %d fragment %d changed", groupIndex, index)
						}
					}
				}
			})
		}
	}
}
