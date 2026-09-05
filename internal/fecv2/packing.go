package fecv2

import "errors"

// ErrNoPayloadCapacity means the context fits an empty descriptor but cannot
// make progress on a nonempty fragment. The caller must select another context
// or reject the original admission; retrying this same input cannot progress.
var ErrNoPayloadCapacity = errors.New("v2 FEC context cannot fit a payload byte")

// Cursor identifies input consumed by EncodePrefix. Next is the first input
// fragment not fully consumed. Bytes is its consumed payload prefix, or zero
// when no partial fragment remains. For that fragment, advance Offset and slice
// Payload by Bytes while retaining its original DatagramID and TotalBytes.
type Cursor struct {
	Next  int
	Bytes int
}

// EncodePrefix greedily packs one group from at most MaxDescriptors already
// admitted, canonically ordered fragments. It can split the last fragment to
// fill the context's logical capacity. Empty Datagrams consume one descriptor.
// Inputs are borrowed until return; the returned group owns its bytes. On any
// error no cursor advances and the input remains unchanged.
//
// This is a bounded packing primitive, not whole-Datagram queue admission. The
// caller retains every unconsumed input under its original ownership/deadline
// and decides when a low-rate tail must be sealed, cancelled, or flushed.
func (c *Codec) EncodePrefix(input []Fragment) (Group, Cursor, error) {
	if len(input) == 0 || len(input) > MaxDescriptors {
		return Group{}, Cursor{}, invalid("packing input count outside bounds")
	}
	var previous uint64
	for _, f := range input {
		if err := c.validateFragment(f, previous); err != nil {
			return Group{}, Cursor{}, err
		}
		previous = f.DatagramID
	}
	var planned [MaxDescriptors]Fragment
	count, used := 0, ManifestBytes
	cursor := Cursor{}
	for i, f := range input {
		room := c.params.MaxLogicalBytes - used - DescriptorBytes
		if count == c.params.MaxDescriptors || room < 0 || (room == 0 && len(f.Payload) > 0) {
			break
		}
		consumed := min(room, len(f.Payload))
		f.Payload = f.Payload[:consumed:consumed]
		planned[count] = f
		count++
		used += DescriptorBytes + consumed
		if consumed < len(input[i].Payload) {
			cursor = Cursor{Next: i, Bytes: consumed}
			break
		}
		cursor = Cursor{Next: i + 1}
	}
	if count == 0 {
		return Group{}, Cursor{}, ErrNoPayloadCapacity
	}
	group, err := c.Encode(planned[:count])
	if err != nil {
		return Group{}, Cursor{}, err
	}
	return group, cursor, nil
}
