package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/mofelee/mpudp"
)

const (
	admissionWaitLimit = time.Second
	admissionRetryWait = 100 * time.Microsecond
	localDrainLimit    = 3 * time.Second
)

// The optional interface keeps this probe buildable against the v1 baseline.
type localFlusher interface {
	Flush(context.Context) error
}

type admissionSnapshot struct {
	BackpressuredPackets uint64 `json:"backpressured_packets"`
	RejectedAttempts     uint64 `json:"rejected_attempts"`
	RetryAttempts        uint64 `json:"retry_attempts"`
	WaitNS               uint64 `json:"wait_ns"`
	CanceledPackets      uint64 `json:"canceled_packets"`
	TimeoutPackets       uint64 `json:"timeout_packets"`
}

type admissionCounters struct {
	backpressured, rejected, retries, waitNS, canceled, timeouts atomic.Uint64
}

type admissionWriter struct {
	session  mpudp.Session
	ctx      context.Context
	cancel   context.CancelFunc
	gate     chan struct{}
	counters admissionCounters
}

func newAdmissionWriter(session mpudp.Session) *admissionWriter {
	ctx, cancel := context.WithCancel(context.Background())
	w := &admissionWriter{session: session, ctx: ctx, cancel: cancel, gate: make(chan struct{}, 1)}
	w.gate <- struct{}{}
	return w
}

func (w *admissionWriter) writePacket(parent context.Context, packet []byte) error {
	ctx := parent
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.ctx.Done():
		return w.ctx.Err()
	case <-w.gate:
	}
	defer func() { w.gate <- struct{}{} }()
	var firstRejection time.Time
	defer func() {
		if !firstRejection.IsZero() {
			w.counters.waitNS.Add(uint64(time.Since(firstRejection)))
		}
	}()
	for {
		var interrupted error
		if err := ctx.Err(); err != nil {
			interrupted = err
		} else if err := w.ctx.Err(); err != nil {
			interrupted = err
		}
		if interrupted != nil {
			if firstRejection.IsZero() {
				return interrupted
			}
			if errors.Is(interrupted, context.DeadlineExceeded) {
				w.counters.timeouts.Add(1)
			} else {
				w.counters.canceled.Add(1)
			}
			return fmt.Errorf("MPUDP whole-Datagram admission: %w", errors.Join(mpudp.ErrResourceLimit, interrupted))
		}
		if !firstRejection.IsZero() {
			w.counters.retries.Add(1)
		}
		err := w.session.WritePacket(packet)
		if !errors.Is(err, mpudp.ErrResourceLimit) {
			return err
		}
		if firstRejection.IsZero() {
			firstRejection = time.Now()
			w.counters.backpressured.Add(1)
			retryContext, cancel := context.WithTimeout(parent, admissionWaitLimit)
			defer cancel()
			ctx = retryContext
		}
		w.counters.rejected.Add(1)
		timer := time.NewTimer(admissionRetryWait)
		select {
		case <-ctx.Done():
		case <-w.ctx.Done():
		case <-timer.C:
		}
		timer.Stop()
	}
}

func (w *admissionWriter) flush(ctx context.Context) error {
	flusher, ok := w.session.(localFlusher)
	if !ok {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.gate:
	}
	defer func() { w.gate <- struct{}{} }()
	return flusher.Flush(ctx)
}

func (w *admissionWriter) snapshot() admissionSnapshot {
	c := &w.counters
	return admissionSnapshot{c.backpressured.Load(), c.rejected.Load(), c.retries.Load(), c.waitNS.Load(), c.canceled.Load(), c.timeouts.Load()}
}

type localDrainSummary struct {
	Scope             string `json:"scope"`
	SupportedSessions int    `json:"supported_sessions"`
	CompletedSessions int    `json:"completed_sessions"`
	FailedSessions    int    `json:"failed_sessions"`
	DurationNS        int64  `json:"duration_ns"`
}

func (t *transports) stopAdmissions() {
	for _, w := range t.writers {
		w.cancel()
	}
}

func (t *transports) drain(ctx context.Context) (localDrainSummary, error) {
	started := time.Now()
	result := localDrainSummary{Scope: "admitted_mpudp_datagrams_local_socket_attempts"}
	var failures []error
	for _, w := range t.writers {
		if _, ok := w.session.(localFlusher); !ok {
			continue
		}
		result.SupportedSessions++
		if err := w.flush(ctx); err != nil {
			result.FailedSessions++
			failures = append(failures, err)
		} else {
			result.CompletedSessions++
		}
	}
	result.DurationNS = time.Since(started).Nanoseconds()
	return result, errors.Join(failures...)
}
