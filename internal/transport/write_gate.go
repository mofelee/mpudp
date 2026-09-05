package transport

import (
	"context"
	"sync"
)

// writeGate serializes connection writes and deadline changes while allowing a
// waiting sender to cancel without changing the active sender's socket state.
// Its zero value is ready for use.
type writeGate struct {
	once sync.Once
	held chan struct{}
}

func (g *writeGate) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.once.Do(func() { g.held = make(chan struct{}, 1) })
	select {
	case g.held <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *writeGate) Lock() { _ = g.acquire(context.Background()) }

func (g *writeGate) Unlock() { <-g.held }
