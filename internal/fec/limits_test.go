package fec

import (
	"errors"
	"testing"
)

func TestValidateParams(t *testing.T) {
	t.Parallel()

	valid := []struct {
		name   string
		params Params
		total  int
	}{
		{name: "minimum", params: Params{DataShards: 1, ParityShards: 1}, total: 2},
		{name: "maximum data", params: Params{DataShards: 255, ParityShards: 1}, total: MaxTotalShards},
		{name: "maximum parity", params: Params{DataShards: 1, ParityShards: 255}, total: MaxTotalShards},
		{name: "balanced maximum", params: Params{DataShards: 128, ParityShards: 128}, total: MaxTotalShards},
	}
	for _, test := range valid {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			total, err := validateParams(test.params)
			if err != nil {
				t.Fatalf("validateParams(%+v) returned error: %v", test.params, err)
			}
			if total != test.total {
				t.Fatalf("validateParams(%+v) total = %d, want %d", test.params, total, test.total)
			}
		})
	}

	maxInt := int(^uint(0) >> 1)
	invalid := []struct {
		name   string
		params Params
	}{
		{name: "zero data", params: Params{DataShards: 0, ParityShards: 1}},
		{name: "negative data", params: Params{DataShards: -1, ParityShards: 1}},
		{name: "zero parity", params: Params{DataShards: 1, ParityShards: 0}},
		{name: "negative parity", params: Params{DataShards: 1, ParityShards: -1}},
		{name: "total above field limit", params: Params{DataShards: 128, ParityShards: 129}},
		{name: "data above field limit", params: Params{DataShards: 256, ParityShards: 1}},
		{name: "parity above field limit", params: Params{DataShards: 1, ParityShards: 256}},
		{name: "addition cannot overflow", params: Params{DataShards: maxInt, ParityShards: maxInt}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			total, err := validateParams(test.params)
			if !errors.Is(err, ErrInvalidParameters) {
				t.Fatalf("validateParams(%+v) error = %v, want ErrInvalidParameters", test.params, err)
			}
			if total != 0 {
				t.Fatalf("validateParams(%+v) total = %d, want 0", test.params, total)
			}
		})
	}
}

func TestDeriveLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		budget Budget
		want   Limits
	}{
		{
			name: "FEC derived limit is lower",
			budget: Budget{
				MaxUDPPayload:         1472,
				DataShardWireOverhead: 72,
				MaxDatagramSize:       5000,
			},
			want: Limits{
				MaxUDPPayload:           1472,
				DataShardWireOverhead:   72,
				ShardCapacity:           1400,
				FECDerivedDatagramLimit: 4200,
				EffectiveDatagramLimit:  4200,
			},
		},
		{
			name: "resource limit is lower",
			budget: Budget{
				MaxUDPPayload:         1200,
				DataShardWireOverhead: 200,
				MaxDatagramSize:       2048,
			},
			want: Limits{
				MaxUDPPayload:           1200,
				DataShardWireOverhead:   200,
				ShardCapacity:           1000,
				FECDerivedDatagramLimit: 3000,
				EffectiveDatagramLimit:  2048,
			},
		},
		{
			name: "equal limits",
			budget: Budget{
				MaxUDPPayload:         101,
				DataShardWireOverhead: 1,
				MaxDatagramSize:       300,
			},
			want: Limits{
				MaxUDPPayload:           101,
				DataShardWireOverhead:   1,
				ShardCapacity:           100,
				FECDerivedDatagramLimit: 300,
				EffectiveDatagramLimit:  300,
			},
		},
	}
	params := Params{DataShards: 3, ParityShards: 2}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := DeriveLimits(params, test.budget)
			if err != nil {
				t.Fatalf("DeriveLimits() returned error: %v", err)
			}
			if got != test.want {
				t.Fatalf("DeriveLimits() = %+v, want %+v", got, test.want)
			}
		})
	}
}

func TestDeriveLimitsRejectsInvalidBudget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		budget Budget
	}{
		{
			name: "negative wire overhead",
			budget: Budget{
				MaxUDPPayload:         100,
				DataShardWireOverhead: -1,
				MaxDatagramSize:       100,
			},
		},
		{
			name: "overhead equals UDP payload",
			budget: Budget{
				MaxUDPPayload:         72,
				DataShardWireOverhead: 72,
				MaxDatagramSize:       100,
			},
		},
		{
			name: "overhead exceeds UDP payload",
			budget: Budget{
				MaxUDPPayload:         71,
				DataShardWireOverhead: 72,
				MaxDatagramSize:       100,
			},
		},
		{
			name: "zero maximum Datagram",
			budget: Budget{
				MaxUDPPayload:         100,
				DataShardWireOverhead: 10,
				MaxDatagramSize:       0,
			},
		},
		{
			name: "negative maximum Datagram",
			budget: Budget{
				MaxUDPPayload:         100,
				DataShardWireOverhead: 10,
				MaxDatagramSize:       -1,
			},
		},
	}
	maxInt := int(^uint(0) >> 1)
	if uint64(maxInt) > maxWireOriginalLength {
		tooLargeForWire := maxWireOriginalLength + 1
		tests = append(tests, struct {
			name   string
			budget Budget
		}{
			name: "maximum Datagram exceeds wire uint32",
			budget: Budget{
				MaxUDPPayload:         100,
				DataShardWireOverhead: 10,
				MaxDatagramSize:       int(tooLargeForWire),
			},
		})
	}

	params := Params{DataShards: 3, ParityShards: 2}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			limits, err := DeriveLimits(params, test.budget)
			if !errors.Is(err, ErrInvalidBudget) {
				t.Fatalf("DeriveLimits() error = %v, want ErrInvalidBudget", err)
			}
			if limits != (Limits{}) {
				t.Fatalf("DeriveLimits() limits = %+v, want zero value", limits)
			}
		})
	}
}

func TestDeriveLimitsAcceptsMaximumWireOriginalLength(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	if uint64(maxInt) < maxWireOriginalLength {
		t.Skip("uint32 wire maximum is not representable by int on this architecture")
	}
	wireMaximum := maxWireOriginalLength
	limits, err := DeriveLimits(Params{DataShards: 1, ParityShards: 1}, Budget{
		MaxUDPPayload:         2,
		DataShardWireOverhead: 1,
		MaxDatagramSize:       int(wireMaximum),
	})
	if err != nil {
		t.Fatalf("DeriveLimits() error = %v", err)
	}
	if limits.EffectiveDatagramLimit != 1 {
		t.Fatalf("EffectiveDatagramLimit = %d, want 1", limits.EffectiveDatagramLimit)
	}
}

func TestDeriveLimitsRejectsIntegerOverflow(t *testing.T) {
	t.Parallel()

	maxInt := int(^uint(0) >> 1)
	capacity := maxInt/2 + 1
	tests := []struct {
		name   string
		params Params
	}{
		{
			name:   "data capacity multiplication",
			params: Params{DataShards: 2, ParityShards: 1},
		},
		{
			name:   "total allocation multiplication",
			params: Params{DataShards: 1, ParityShards: 1},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			limits, err := DeriveLimits(test.params, Budget{
				MaxUDPPayload:         maxInt,
				DataShardWireOverhead: maxInt - capacity,
				MaxDatagramSize:       1,
			})
			if !errors.Is(err, ErrInvalidBudget) {
				t.Fatalf("DeriveLimits() error = %v, want ErrInvalidBudget", err)
			}
			if limits != (Limits{}) {
				t.Fatalf("DeriveLimits() limits = %+v, want zero value", limits)
			}
		})
	}
}

func TestDeriveLimitsClassifiesInvalidParams(t *testing.T) {
	t.Parallel()

	limits, err := DeriveLimits(Params{DataShards: 0, ParityShards: 1}, Budget{
		MaxUDPPayload:         100,
		DataShardWireOverhead: 10,
		MaxDatagramSize:       100,
	})
	if !errors.Is(err, ErrInvalidParameters) {
		t.Fatalf("DeriveLimits() error = %v, want ErrInvalidParameters", err)
	}
	if limits != (Limits{}) {
		t.Fatalf("DeriveLimits() limits = %+v, want zero value", limits)
	}
}

func TestShardSizeForDatagram(t *testing.T) {
	t.Parallel()

	tests := []struct {
		length     int
		dataShards int
		want       int
	}{
		{length: 0, dataShards: 5, want: 1},
		{length: 1, dataShards: 5, want: 1},
		{length: 5, dataShards: 5, want: 1},
		{length: 6, dataShards: 5, want: 2},
		{length: 10, dataShards: 5, want: 2},
	}
	for _, test := range tests {
		if got := shardSizeForDatagram(test.length, test.dataShards); got != test.want {
			t.Errorf("shardSizeForDatagram(%d, %d) = %d, want %d", test.length, test.dataShards, got, test.want)
		}
	}
}
