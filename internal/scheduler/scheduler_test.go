package scheduler_test

import (
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/mofelee/mpudp/internal/scheduler"
)

func TestAssignRSFiveAcrossTwoPathsRotatesThreeTwo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		packetID uint64
		want     []int
	}{
		{packetID: 100, want: []int{0, 1, 0, 1, 0}},
		{packetID: 101, want: []int{1, 0, 1, 0, 1}},
	}
	for _, test := range tests {
		got, err := scheduler.Assign(test.packetID, 5, 2)
		if err != nil {
			t.Fatalf("Assign(%d, 5, 2) error = %v", test.packetID, err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Fatalf("Assign(%d, 5, 2) = %v, want %v", test.packetID, got, test.want)
		}
	}
}

func TestAssignShardAndPathCountBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		packetID uint64
		shards   int
		paths    int
		want     []int
	}{
		{name: "more shards", packetID: 3, shards: 7, paths: 3, want: []int{0, 1, 2, 0, 1, 2, 0}},
		{name: "equal", packetID: 7, shards: 5, paths: 5, want: []int{2, 3, 4, 0, 1}},
		{name: "more paths", packetID: 6, shards: 5, paths: 7, want: []int{6, 0, 1, 2, 3}},
		{name: "one shard", packetID: 11, shards: 1, paths: 7, want: []int{4}},
		{name: "one path", packetID: ^uint64(0), shards: 5, paths: 1, want: []int{0, 0, 0, 0, 0}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := scheduler.Assign(test.packetID, test.shards, test.paths)
			if err != nil {
				t.Fatalf("Assign() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("Assign() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestAssignIsBalanced(t *testing.T) {
	t.Parallel()

	for shards := 1; shards <= 256; shards++ {
		for paths := 1; paths <= 256; paths++ {
			got, err := scheduler.Assign(0xffffffffffffffff, shards, paths)
			if err != nil {
				t.Fatalf("Assign(%d, %d) error = %v", shards, paths, err)
			}
			counts := make([]int, paths)
			for _, path := range got {
				if path < 0 || path >= paths {
					t.Fatalf("path index %d outside [0,%d)", path, paths)
				}
				counts[path]++
			}
			minimum, maximum := counts[0], counts[0]
			for _, count := range counts[1:] {
				if count < minimum {
					minimum = count
				}
				if count > maximum {
					maximum = count
				}
			}
			if maximum-minimum > 1 {
				t.Fatalf("%d shards across %d paths are unbalanced: %v", shards, paths, counts)
			}
		}
	}
}

func TestAssignRotatesPathCoverage(t *testing.T) {
	t.Parallel()

	seen := make(map[int]bool)
	for packetID := uint64(0); packetID < 7; packetID++ {
		got, err := scheduler.Assign(packetID, 1, 7)
		if err != nil {
			t.Fatalf("Assign(%d, 1, 7) error = %v", packetID, err)
		}
		seen[got[0]] = true
	}
	if len(seen) != 7 {
		t.Fatalf("one-shard blocks covered %d paths, want 7", len(seen))
	}
}

func TestAssignRejectsInvalidCounts(t *testing.T) {
	t.Parallel()

	if _, err := scheduler.Assign(0, 0, 1); !errors.Is(err, scheduler.ErrInvalidShardCount) {
		t.Fatalf("zero shards error = %v", err)
	}
	if _, err := scheduler.Assign(0, -1, 1); !errors.Is(err, scheduler.ErrInvalidShardCount) {
		t.Fatalf("negative shards error = %v", err)
	}
	if _, err := scheduler.Assign(0, 1, 0); !errors.Is(err, scheduler.ErrNoPaths) {
		t.Fatalf("zero paths error = %v", err)
	}
	if _, err := scheduler.Assign(0, 1, -1); !errors.Is(err, scheduler.ErrNoPaths) {
		t.Fatalf("negative paths error = %v", err)
	}

	for _, test := range []struct {
		name   string
		shards int
		paths  int
		want   error
	}{
		{name: "257 shards", shards: 257, paths: 1, want: scheduler.ErrTooManyShards},
		{name: "MaxInt shards", shards: math.MaxInt, paths: 1, want: scheduler.ErrTooManyShards},
		{name: "257 paths", shards: 1, paths: 257, want: scheduler.ErrTooManyPaths},
		{name: "MaxInt paths", shards: 1, paths: math.MaxInt, want: scheduler.ErrTooManyPaths},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := scheduler.Assign(0, test.shards, test.paths)
			if got != nil {
				t.Fatalf("Assign() returned plan on failure: %v", got)
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("Assign() error = %v, want %v", err, test.want)
			}
			var countErr *scheduler.CountError
			if !errors.As(err, &countErr) {
				t.Fatalf("Assign() error type = %T, want *scheduler.CountError", err)
			}
		})
	}
}

func TestAssignReturnsIndependentSlices(t *testing.T) {
	t.Parallel()

	first, err := scheduler.Assign(9, 5, 3)
	if err != nil {
		t.Fatal(err)
	}
	second, err := scheduler.Assign(9, 5, 3)
	if err != nil {
		t.Fatal(err)
	}
	first[0] = 99
	if second[0] == 99 {
		t.Fatal("Assign() results alias each other")
	}
}

func TestAssignConcurrentDeterminism(t *testing.T) {
	t.Parallel()

	want, err := scheduler.Assign(math.MaxUint64, 256, 256)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errorsCh := make(chan []int, 64)
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				got, assignErr := scheduler.Assign(math.MaxUint64, 256, 256)
				if assignErr != nil || !reflect.DeepEqual(got, want) {
					errorsCh <- got
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errorsCh)
	for got := range errorsCh {
		t.Fatalf("concurrent Assign() = %v, want deterministic result", got)
	}
}
