// Package fecv2 implements the proposed v2 equal-shard logical group format.
// It does not authenticate packets, admit epochs, assign IDs, retain pending
// groups, or deliver Datagrams. Callers must reserve ownership and authenticate
// shards before using this codec. The v1 runtime does not use this package.
package fecv2

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/klauspost/reedsolomon"
)

const (
	ManifestVersion  = 1
	ManifestBytes    = 4
	DescriptorBytes  = 20
	MaxDescriptors   = 256
	MaxLogicalBytes  = 16 * 1024 * 1024
	MaxDatagramBytes = 16 * 1024 * 1024
	// One established envelope, bundle prefix and shard record consume 94 bytes.
	MaxShardBytes = 65507 - 94
)

var (
	ErrInvalid            = errors.New("invalid v2 FEC group")
	ErrInsufficientShards = errors.New("insufficient v2 FEC shards")
)

// Parameters describe one immutable, already acknowledged encoding context.
// The runtime must further constrain these limits by its negotiated resources.
type Parameters struct {
	DataShards       int
	ParityShards     int
	ShardBytes       int
	MaxDescriptors   int
	MaxLogicalBytes  int
	MaxDatagramBytes int
}

func (p Parameters) validate() error {
	if p.DataShards < 1 || p.DataShards > 255 || p.ParityShards < 1 || p.ParityShards > 255 || p.DataShards+p.ParityShards > 256 {
		return invalid("invalid data/parity count")
	}
	if p.ShardBytes < 1 || p.ShardBytes > MaxShardBytes {
		return invalid("invalid shard size")
	}
	if p.MaxDescriptors < 1 || p.MaxDescriptors > MaxDescriptors || p.MaxLogicalBytes < ManifestBytes+DescriptorBytes || p.MaxLogicalBytes > MaxLogicalBytes || p.MaxLogicalBytes > p.DataShards*p.ShardBytes {
		return invalid("invalid manifest limits")
	}
	if p.MaxDatagramBytes < 1 || p.MaxDatagramBytes > MaxDatagramBytes {
		return invalid("invalid Datagram limit")
	}
	return nil
}

// Fragment describes one original Datagram range. A group contains at most
// one fragment per nonzero DatagramID, in ascending ID order. Empty Datagrams
// have TotalBytes=Offset=0 and an empty Payload; other fragments are nonempty.
type Fragment struct {
	DatagramID uint64
	TotalBytes uint32
	Offset     uint32
	Payload    []byte
}

// Group owns all shard bytes in a single allocation. Treat them as immutable.
// LogicalBytes excludes zero padding through DataShards*ShardBytes.
type Group struct {
	LogicalBytes uint32
	Shards       [][]byte
}

// Codec has no per-group state. Calls are safe concurrently; callers must not
// mutate their input slices until the call returns. Results never alias inputs.
type Codec struct {
	params Parameters
	mu     sync.Mutex
	rs     reedsolomon.Encoder
}

func New(p Parameters) (*Codec, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	// Runtime contexts are long-lived; missing-shard patterns must not create
	// an unbounded retained inversion cache or independent codec worker pool.
	rs, err := reedsolomon.New(p.DataShards, p.ParityShards, reedsolomon.WithInversionCache(false), reedsolomon.WithMaxGoroutines(1))
	if err != nil {
		return nil, fmt.Errorf("%w: Reed-Solomon parameters: %v", ErrInvalid, err)
	}
	return &Codec{params: p, rs: rs}, nil
}

func invalid(reason string) error { return fmt.Errorf("%w: %s", ErrInvalid, reason) }

func (c *Codec) logicalSize(fragments []Fragment) (int, error) {
	if len(fragments) < 1 || len(fragments) > c.params.MaxDescriptors {
		return 0, invalid("descriptor count outside context")
	}
	size := ManifestBytes + DescriptorBytes*len(fragments)
	if size > c.params.MaxLogicalBytes {
		return 0, invalid("manifest exceeds context")
	}
	var previous uint64
	for _, f := range fragments {
		if err := c.validateFragment(f, previous); err != nil {
			return 0, err
		}
		previous = f.DatagramID
		if len(f.Payload) > c.params.MaxLogicalBytes-size {
			return 0, invalid("logical bytes exceed context")
		}
		size += len(f.Payload)
	}
	return size, nil
}

func (c *Codec) validateFragment(f Fragment, previous uint64) error {
	if f.DatagramID == 0 || f.DatagramID <= previous {
		return invalid("Datagram IDs must be nonzero and strictly increasing")
	}
	if f.TotalBytes > uint32(c.params.MaxDatagramBytes) || f.Offset > f.TotalBytes || uint64(len(f.Payload)) > uint64(f.TotalBytes-f.Offset) {
		return invalid("fragment outside Datagram bounds")
	}
	if len(f.Payload) == 0 && f.TotalBytes != 0 {
		return invalid("empty fragment of nonempty Datagram")
	}
	return nil
}

// Encode protects the complete manifest and payloads together. Even a tail
// group uses the context's full shard size, with canonical zero padding.
// Invalid input is rejected before shard allocation or codec work.
func (c *Codec) Encode(fragments []Fragment) (Group, error) {
	size, err := c.logicalSize(fragments)
	if err != nil {
		return Group{}, err
	}
	p := c.params
	backing := make([]byte, (p.DataShards+p.ParityShards)*p.ShardBytes)
	shards := shardSlices(backing, p.ShardBytes)
	binary.BigEndian.PutUint16(backing, ManifestVersion)
	binary.BigEndian.PutUint16(backing[2:], uint16(len(fragments)))
	dataAt := ManifestBytes + DescriptorBytes*len(fragments)
	for i, f := range fragments {
		d := backing[ManifestBytes+i*DescriptorBytes:]
		binary.BigEndian.PutUint64(d, f.DatagramID)
		binary.BigEndian.PutUint32(d[8:], f.TotalBytes)
		binary.BigEndian.PutUint32(d[12:], f.Offset)
		binary.BigEndian.PutUint32(d[16:], uint32(len(f.Payload)))
		dataAt += copy(backing[dataAt:], f.Payload)
	}
	c.mu.Lock()
	err = c.rs.Encode(shards)
	c.mu.Unlock()
	if err != nil {
		return Group{}, fmt.Errorf("%w: encode: %v", ErrInvalid, err)
	}
	return Group{LogicalBytes: uint32(size), Shards: shards}, nil
}

func shardSlices(backing []byte, size int) [][]byte {
	shards := make([][]byte, len(backing)/size)
	for i := range shards {
		start, end := i*size, (i+1)*size
		shards[i] = backing[start:end:end]
	}
	return shards
}

// Decode accepts an index-addressed shard set; nil means absent. It requires
// at least k exact-size authenticated shards. Supplied parity is checked after
// reconstruction, then padding and the entire manifest are checked before any
// fragments are returned. Returned payloads share owned logical storage and
// have capped capacities. No caller shard is modified or retained.
func (c *Codec) Decode(logicalBytes uint32, input [][]byte) ([]Fragment, error) {
	p := c.params
	if logicalBytes < ManifestBytes+DescriptorBytes || logicalBytes > uint32(p.MaxLogicalBytes) || len(input) != p.DataShards+p.ParityShards {
		return nil, invalid("logical size or shard count outside context")
	}
	present := 0
	for _, shard := range input {
		if shard == nil {
			continue
		}
		if len(shard) != p.ShardBytes {
			return nil, invalid("shard size differs from context")
		}
		present++
	}
	if present < p.DataShards {
		return nil, ErrInsufficientShards
	}
	backing := make([]byte, len(input)*p.ShardBytes)
	shards := shardSlices(backing, p.ShardBytes)
	for i, shard := range input {
		if shard == nil {
			shards[i] = shards[i][:0]
		} else {
			copy(shards[i], shard)
		}
	}
	c.mu.Lock()
	err := c.rs.Reconstruct(shards)
	var valid bool
	if err == nil {
		valid, err = c.rs.Verify(shards)
	}
	c.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("%w: reconstruction: %v", ErrInvalid, err)
	}
	if !valid {
		return nil, invalid("inconsistent parity")
	}
	// Reconstruct may replace missing slices, so do not assume contiguous data.
	logical := make([]byte, int(logicalBytes))
	for i, shard := range shards[:p.DataShards] {
		start := i * p.ShardBytes
		used := min(max(len(logical)-start, 0), p.ShardBytes)
		if used > 0 {
			copy(logical[start:], shard[:used])
		}
		for _, value := range shard[used:] {
			if value != 0 {
				return nil, invalid("nonzero tail padding")
			}
		}
	}
	return c.parseManifest(logical)
}

func (c *Codec) parseManifest(logical []byte) ([]Fragment, error) {
	if len(logical) < ManifestBytes || binary.BigEndian.Uint16(logical) != ManifestVersion {
		return nil, invalid("unknown or truncated manifest")
	}
	count := int(binary.BigEndian.Uint16(logical[2:]))
	dataAt := ManifestBytes + count*DescriptorBytes
	if count < 1 || count > c.params.MaxDescriptors || dataAt > len(logical) {
		return nil, invalid("invalid descriptor count")
	}
	// Validate every descriptor before allocating the returned descriptor slice.
	var previous uint64
	for i := range count {
		d := logical[ManifestBytes+i*DescriptorBytes:]
		size := uint64(binary.BigEndian.Uint32(d[16:]))
		if size > uint64(len(logical)-dataAt) {
			return nil, invalid("truncated fragment payload")
		}
		f := Fragment{DatagramID: binary.BigEndian.Uint64(d), TotalBytes: binary.BigEndian.Uint32(d[8:]), Offset: binary.BigEndian.Uint32(d[12:]), Payload: logical[dataAt : dataAt+int(size)]}
		if err := c.validateFragment(f, previous); err != nil {
			return nil, err
		}
		previous = f.DatagramID
		dataAt += int(size)
	}
	if dataAt != len(logical) {
		return nil, invalid("trailing logical bytes")
	}
	fragments := make([]Fragment, count)
	dataAt = ManifestBytes + count*DescriptorBytes
	for i := range fragments {
		d := logical[ManifestBytes+i*DescriptorBytes:]
		end := dataAt + int(binary.BigEndian.Uint32(d[16:]))
		fragments[i] = Fragment{DatagramID: binary.BigEndian.Uint64(d), TotalBytes: binary.BigEndian.Uint32(d[8:]), Offset: binary.BigEndian.Uint32(d[12:]), Payload: logical[dataAt:end:end]}
		dataAt = end
	}
	return fragments, nil
}
