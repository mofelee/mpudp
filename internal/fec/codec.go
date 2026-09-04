package fec

import (
	"fmt"

	"github.com/klauspost/reedsolomon"
)

func newCodec(params Params) (reedsolomon.Encoder, int, error) {
	total, err := validateParams(params)
	if err != nil {
		return nil, 0, err
	}
	codec, err := reedsolomon.New(
		params.DataShards,
		params.ParityShards,
		reedsolomon.WithMaxGoroutines(1),
		reedsolomon.WithInversionCache(false),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: codec rejected validated parameters: %v", ErrInvalidParameters, err)
	}
	return codec, total, nil
}
