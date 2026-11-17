package queue

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Source is anything that can block until an ID is available.
type Source interface {
	Dequeue(ctx context.Context) (string, error)
}

// Fanout reads from a Source with a few goroutines and hands IDs to
// any number of consumers through an unbuffered channel.
type Fanout struct {
	ch chan string
	wg sync.WaitGroup
}

func NewFanout(ctx context.Context, src Source, dequeuers int) *Fanout {
	f := &Fanout{ch: make(chan string)}
	for i := 0; i < dequeuers; i++ {
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			f.run(ctx, src)
		}()
	}
	return f
}

func (f *Fanout) run(ctx context.Context, src Source) {
	for {
		id, err := src.Dequeue(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("dequeue failed, retrying", "component", "fanout", "err", err)
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}
		select {
		case f.ch <- id:
		case <-ctx.Done():
			return // ID dropped; the row is still queued, so the orphan sweep recovers it
		}
	}
}

// Dequeue lets Fanout satisfy the worker's Queue interface.
func (f *Fanout) Dequeue(ctx context.Context) (string, error) {
	select {
	case id := <-f.ch:
		return id, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// Wait blocks until every dequeuer goroutine has exited.
func (f *Fanout) Wait() { f.wg.Wait() }
