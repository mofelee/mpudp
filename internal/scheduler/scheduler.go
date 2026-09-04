// Package scheduler maps the shards of one FEC block onto the paths that are
// available when that block is sent.
package scheduler

import (
	"errors"
	"fmt"
)

// MPUDP v0.1 bounds both FEC shards and configured paths. Keeping the limits
// here makes Assign safe even when it is accidentally called before config
// validation.
const (
	MaxShards = 256
	MaxPaths  = 256
)

var (
	// ErrInvalidShardCount means a block has no shards to schedule.
	ErrInvalidShardCount = errors.New("MPUDP scheduler requires at least one shard")
	// ErrNoPaths means there is no path on which to schedule a shard.
	ErrNoPaths = errors.New("MPUDP scheduler has no available paths")
	// ErrTooManyShards means a block exceeds the v0.1 FEC shard limit.
	ErrTooManyShards = errors.New("MPUDP scheduler shard count exceeds limit")
	// ErrTooManyPaths means a path set exceeds the v0.1 carrier/endpoint limit.
	ErrTooManyPaths = errors.New("MPUDP scheduler path count exceeds limit")
)

// CountError reports an invalid scheduler dimension without allocating a plan.
type CountError struct {
	Field string
	Value int
	Min   int
	Max   int
	cause error
}

func (e *CountError) Error() string {
	return fmt.Sprintf("MPUDP scheduler %s %d outside [%d,%d]", e.Field, e.Value, e.Min, e.Max)
}

func (e *CountError) Unwrap() error { return e.cause }

// Assign returns one path index for every shard in a block. The first path is
// selected solely from packetID, so scheduling is independent of goroutine or
// I/O completion order. The returned slice is newly allocated.
//
// For S shards and M paths, assignment i is (packetID%M+i)%M. Consequently,
// paths carry either floor(S/M) or ceil(S/M) shards.
func Assign(packetID uint64, shardCount, pathCount int) ([]int, error) {
	if shardCount <= 0 {
		return nil, &CountError{Field: "shard count", Value: shardCount, Min: 1, Max: MaxShards, cause: ErrInvalidShardCount}
	}
	if shardCount > MaxShards {
		return nil, &CountError{Field: "shard count", Value: shardCount, Min: 1, Max: MaxShards, cause: ErrTooManyShards}
	}
	if pathCount <= 0 {
		return nil, &CountError{Field: "path count", Value: pathCount, Min: 1, Max: MaxPaths, cause: ErrNoPaths}
	}
	if pathCount > MaxPaths {
		return nil, &CountError{Field: "path count", Value: pathCount, Min: 1, Max: MaxPaths, cause: ErrTooManyPaths}
	}

	assignments := make([]int, shardCount)
	start := int(packetID % uint64(pathCount))
	for shard := range assignments {
		assignments[shard] = (start + shard) % pathCount
	}
	return assignments, nil
}
