package transport_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/mofelee/mpudp/internal/scheduler"
	"github.com/mofelee/mpudp/internal/transport"
)

type fakePath struct {
	id string

	mu        sync.Mutex
	available bool
	fail      bool
	sends     [][]byte
}

func (p *fakePath) PathID() string {
	return p.id
}

func (p *fakePath) Available() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.available
}

func (p *fakePath) Send(ctx context.Context, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sends = append(p.sends, append([]byte(nil), payload...))
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.fail {
		return fmt.Errorf("injected failure on %s", p.id)
	}
	return nil
}

func (p *fakePath) setAvailable(available bool) {
	p.mu.Lock()
	p.available = available
	p.mu.Unlock()
}

func (p *fakePath) sentStrings() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]string, len(p.sends))
	for i, payload := range p.sends {
		result[i] = string(payload)
	}
	return result
}

func shards(count int) [][]byte {
	result := make([][]byte, count)
	for i := range result {
		result[i] = []byte(fmt.Sprintf("shard-%d", i))
	}
	return result
}

func TestSendBlockContinuesAfterFailureAndReportsPartial(t *testing.T) {
	t.Parallel()

	failing := &fakePath{id: "A", available: true, fail: true}
	working := &fakePath{id: "B", available: true}
	result, err := transport.SendBlock(context.Background(), 100, shards(5), []transport.Path{failing, working})
	if !errors.Is(err, transport.ErrPartialSend) {
		t.Fatalf("SendBlock() error = %v, want ErrPartialSend", err)
	}
	if result.Attempted != 5 || result.Succeeded != 2 || result.PathsAvailable != 2 {
		t.Fatalf("SendBlock() result = %+v", result)
	}
	if got, want := failing.sentStrings(), []string{"shard-0", "shard-2", "shard-4"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("failing path sends = %v, want %v", got, want)
	}
	if got, want := working.sentStrings(), []string{"shard-1", "shard-3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("working path sends = %v, want %v", got, want)
	}
	var sendErr *transport.BlockSendError
	if !errors.As(err, &sendErr) || sendErr.Result.Attempted != 5 {
		t.Fatalf("SendBlock() error type/result = %#v", err)
	}
}

func TestSendBlockReportsAllFailedAndTriesEveryShard(t *testing.T) {
	t.Parallel()

	paths := []transport.Path{
		&fakePath{id: "A", available: true, fail: true},
		&fakePath{id: "B", available: true, fail: true},
	}
	result, err := transport.SendBlock(context.Background(), 101, shards(5), paths)
	if !errors.Is(err, transport.ErrAllSendsFailed) {
		t.Fatalf("SendBlock() error = %v, want ErrAllSendsFailed", err)
	}
	if errors.Is(err, transport.ErrPartialSend) {
		t.Fatalf("all-failed error was also classified partial: %v", err)
	}
	if result.Attempted != 5 || result.Succeeded != 0 || len(result.Attempts) != 5 {
		t.Fatalf("SendBlock() result = %+v", result)
	}
}

func TestSendBlockReportsNoAvailablePathWithoutAttempts(t *testing.T) {
	t.Parallel()

	unavailable := &fakePath{id: "down", available: false}
	result, err := transport.SendBlock(context.Background(), 0, shards(1), []transport.Path{nil, unavailable})
	if !errors.Is(err, transport.ErrNoAvailablePaths) {
		t.Fatalf("SendBlock() error = %v", err)
	}
	if result.Attempted != 0 || len(unavailable.sentStrings()) != 0 {
		t.Fatalf("SendBlock() made an attempt with no path: %+v", result)
	}
}

func TestSendBlockRecomputesAvailableSetForLaterBlocks(t *testing.T) {
	t.Parallel()

	a := &fakePath{id: "A", available: true}
	b := &fakePath{id: "B", available: true}
	c := &fakePath{id: "C", available: false}
	if _, err := transport.SendBlock(context.Background(), 0, shards(3), []transport.Path{a, b, c}); err != nil {
		t.Fatal(err)
	}
	a.setAvailable(false)
	c.setAvailable(true)
	result, err := transport.SendBlock(context.Background(), 1, shards(3), []transport.Path{a, b, c})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := []string{result.Attempts[0].PathID, result.Attempts[1].PathID, result.Attempts[2].PathID}, []string{"C", "B", "C"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("second block paths = %v, want %v", got, want)
	}
	if got, want := []int{result.Attempts[0].PathIndex, result.Attempts[1].PathIndex, result.Attempts[2].PathIndex}, []int{2, 1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("second block input indexes = %v, want %v", got, want)
	}
}

func TestSendBlockBoundsAvailablePathsBeforeScheduling(t *testing.T) {
	t.Parallel()

	paths := make([]transport.Path, scheduler.MaxPaths+1)
	for i := range paths {
		paths[i] = &fakePath{id: fmt.Sprintf("path-%d", i), available: true}
	}
	result, err := transport.SendBlock(context.Background(), 0, shards(1), paths)
	if !errors.Is(err, scheduler.ErrTooManyPaths) {
		t.Fatalf("SendBlock() error = %v, want scheduler.ErrTooManyPaths", err)
	}
	if result.Attempted != 0 {
		t.Fatalf("SendBlock() attempted %d sends before path bound", result.Attempted)
	}
}

func TestSendBlockCanceledContextStillVisitsEveryShard(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a := &fakePath{id: "A", available: true}
	b := &fakePath{id: "B", available: true}
	result, err := transport.SendBlock(ctx, 0, shards(5), []transport.Path{a, b})
	if !errors.Is(err, transport.ErrAllSendsFailed) || !errors.Is(err, context.Canceled) {
		t.Fatalf("SendBlock() error = %v", err)
	}
	if result.Attempted != 5 || len(a.sentStrings())+len(b.sentStrings()) != 5 {
		t.Fatalf("canceled block attempts = %+v", result)
	}
}

func TestSendBlockRejectsNilContextAndEmptyBlock(t *testing.T) {
	t.Parallel()

	path := &fakePath{id: "A", available: true}
	if _, err := transport.SendBlock(nil, 0, shards(1), []transport.Path{path}); !errors.Is(err, transport.ErrInvalidArgument) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := transport.SendBlock(context.Background(), 0, nil, []transport.Path{path}); !errors.Is(err, scheduler.ErrInvalidShardCount) {
		t.Fatalf("empty block error = %v", err)
	}
}
