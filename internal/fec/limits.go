package fec

import "fmt"

// MaxTotalShards keeps v0.1 on the codec's regular GF(2^8) implementation.
const MaxTotalShards = 256

const maxWireOriginalLength = uint64(1<<32 - 1)

// Params defines RS(n, k), where DataShards is k and n is the sum of both
// fields.
type Params struct {
	DataShards   int
	ParityShards int
}

// Budget contains the already negotiated, direction-specific UDP payload
// budget and the independently configured process resource limit.
type Budget struct {
	MaxUDPPayload         int
	DataShardWireOverhead int
	MaxDatagramSize       int
}

// Limits is the checked result of applying Params to a Budget.
type Limits struct {
	MaxUDPPayload           int
	DataShardWireOverhead   int
	ShardCapacity           int
	FECDerivedDatagramLimit int
	EffectiveDatagramLimit  int
}

// DeriveLimits calculates shard and Datagram limits without unchecked integer
// addition or multiplication. DataShardWireOverhead must come from the wire
// codec; this package deliberately does not duplicate that constant.
func DeriveLimits(params Params, budget Budget) (Limits, error) {
	total, err := validateParams(params)
	if err != nil {
		return Limits{}, err
	}
	if budget.DataShardWireOverhead < 0 {
		return Limits{}, fmt.Errorf("%w: DATA_SHARD overhead must not be negative", ErrInvalidBudget)
	}
	if budget.MaxUDPPayload <= budget.DataShardWireOverhead {
		return Limits{}, fmt.Errorf("%w: max UDP payload must leave room for at least one shard byte", ErrInvalidBudget)
	}
	if budget.MaxDatagramSize <= 0 {
		return Limits{}, fmt.Errorf("%w: max Datagram size must be greater than zero", ErrInvalidBudget)
	}
	if uint64(budget.MaxDatagramSize) > maxWireOriginalLength {
		return Limits{}, fmt.Errorf("%w: max Datagram size exceeds the uint32 wire length", ErrInvalidBudget)
	}

	capacity := budget.MaxUDPPayload - budget.DataShardWireOverhead
	maxInt := int(^uint(0) >> 1)
	if capacity > maxInt/params.DataShards {
		return Limits{}, fmt.Errorf("%w: data shard capacity multiplication overflows int", ErrInvalidBudget)
	}
	if capacity > maxInt/total {
		return Limits{}, fmt.Errorf("%w: total shard allocation multiplication overflows int", ErrInvalidBudget)
	}
	fecLimit := params.DataShards * capacity
	effective := min(fecLimit, budget.MaxDatagramSize)
	return Limits{
		MaxUDPPayload:           budget.MaxUDPPayload,
		DataShardWireOverhead:   budget.DataShardWireOverhead,
		ShardCapacity:           capacity,
		FECDerivedDatagramLimit: fecLimit,
		EffectiveDatagramLimit:  effective,
	}, nil
}

func validateParams(params Params) (int, error) {
	if params.DataShards <= 0 {
		return 0, fmt.Errorf("%w: data shards must be greater than zero", ErrInvalidParameters)
	}
	if params.ParityShards <= 0 {
		return 0, fmt.Errorf("%w: parity shards must be greater than zero", ErrInvalidParameters)
	}
	// Subtraction after the positivity checks avoids overflowing an unchecked
	// DataShards + ParityShards expression.
	if params.ParityShards > MaxTotalShards || params.DataShards > MaxTotalShards-params.ParityShards {
		return 0, fmt.Errorf("%w: total shards must not exceed %d", ErrInvalidParameters, MaxTotalShards)
	}
	return params.DataShards + params.ParityShards, nil
}

func shardSizeForDatagram(length, dataShards int) int {
	if length == 0 {
		return 1
	}
	result := length / dataShards
	if length%dataShards != 0 {
		result++
	}
	return result
}
