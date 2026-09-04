package transport

import (
	"context"
	"fmt"

	"github.com/mofelee/mpudp/internal/scheduler"
)

// ShardAttempt records the deterministic path chosen for one shard. Err is nil
// on success. It contains no packet bytes.
type ShardAttempt struct {
	ShardIndex int
	// PathIndex indexes the paths slice passed to SendBlock, even when
	// unavailable entries were filtered before scheduling.
	PathIndex int
	PathID    string
	Err       error
}

// BlockSendResult describes every attempted shard send. PathsAvailable is the
// size of the available-path snapshot used for this block.
type BlockSendResult struct {
	PacketID       uint64
	PathsAvailable int
	Attempted      int
	Succeeded      int
	Attempts       []ShardAttempt
}

// BlockSendError classifies a partial or complete block send failure. Error
// output contains counts only; individual causes remain available through
// Unwrap and BlockSendResult.
type BlockSendError struct {
	Kind   error
	Result BlockSendResult
}

func (e *BlockSendError) Error() string {
	return fmt.Sprintf("%v: %d of %d shard sends succeeded", e.Kind, e.Result.Succeeded, e.Result.Attempted)
}

func (e *BlockSendError) Unwrap() []error {
	errorsList := make([]error, 0, len(e.Result.Attempts)+1)
	errorsList = append(errorsList, e.Kind)
	for _, attempt := range e.Result.Attempts {
		if attempt.Err != nil {
			errorsList = append(errorsList, attempt.Err)
		}
	}
	return errorsList
}

// SendBlock sends every already encoded shard exactly once. It snapshots the
// available paths at entry, computes a deterministic round-robin assignment,
// and continues after every per-shard error. It never retries DATA packets.
func SendBlock(ctx context.Context, packetID uint64, shards [][]byte, paths []Path) (BlockSendResult, error) {
	result := BlockSendResult{PacketID: packetID}
	if ctx == nil {
		return result, invalidArgument("nil send context")
	}

	type availablePath struct {
		path       Path
		inputIndex int
	}
	available := make([]availablePath, 0, len(paths))
	for inputIndex, path := range paths {
		if path != nil && path.Available() {
			available = append(available, availablePath{path: path, inputIndex: inputIndex})
		}
	}
	result.PathsAvailable = len(available)

	if len(available) == 0 {
		if len(shards) == 0 {
			_, err := scheduler.Assign(packetID, 0, 0)
			return result, err
		}
		return result, ErrNoAvailablePaths
	}
	assignments, err := scheduler.Assign(packetID, len(shards), len(available))
	if err != nil {
		return result, err
	}

	result.Attempts = make([]ShardAttempt, 0, len(shards))
	for shardIndex, availableIndex := range assignments {
		selected := available[availableIndex]
		attempt := ShardAttempt{
			ShardIndex: shardIndex,
			PathIndex:  selected.inputIndex,
			PathID:     selected.path.PathID(),
		}
		result.Attempted++
		if sendErr := selected.path.Send(ctx, shards[shardIndex]); sendErr != nil {
			attempt.Err = sendErr
		} else {
			result.Succeeded++
		}
		result.Attempts = append(result.Attempts, attempt)
	}

	if result.Succeeded == result.Attempted {
		return result, nil
	}
	kind := ErrPartialSend
	if result.Succeeded == 0 {
		kind = ErrAllSendsFailed
	}
	return result, &BlockSendError{Kind: kind, Result: result}
}
