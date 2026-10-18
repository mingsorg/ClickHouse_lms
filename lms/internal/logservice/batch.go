package logservice

import (
	"context"
	"errors"
	"time"
)

var ErrBusy = errors.New("ingestion capacity is full")

type writeJob struct {
	rows []Event
	done chan error
}

// Batcher is bounded by the server's ingress slots and request body limit.
// Cancellation after enqueue does not imply rollback; callers must allow retries.
type Batcher struct {
	queue chan writeJob
	store Store
}

func NewBatcher(ctx context.Context, store Store) *Batcher {
	b := &Batcher{queue: make(chan writeJob, 8), store: store}
	go b.run(ctx)
	return b
}

func (b *Batcher) Write(ctx context.Context, rows []Event) error {
	j := writeJob{rows: rows, done: make(chan error, 1)}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	select {
	case b.queue <- j:
	default:
		return ErrBusy
	}
	select {
	case err := <-j.done:
		return err
	case <-ctx.Done():
		return errors.New("request interrupted; commit status may be unknown")
	}
}

func (b *Batcher) run(ctx context.Context) {
	for {
		var first writeJob
		select {
		case <-ctx.Done():
			return
		case first = <-b.queue:
		}
		jobs := []writeJob{first}
		timer := time.NewTimer(5 * time.Millisecond)
	collect:
		for len(jobs) < 8 {
			select {
			case j := <-b.queue:
				jobs = append(jobs, j)
			case <-timer.C:
				break collect
			case <-ctx.Done():
				break collect
			}
		}
		timer.Stop()
		var rows []Event
		for _, j := range jobs {
			rows = append(rows, j.rows...)
		}
		writeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := b.store.Insert(writeCtx, rows)
		cancel()
		for _, j := range jobs {
			j.done <- err
		}
	}
}
