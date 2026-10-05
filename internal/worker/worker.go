package worker

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/anandhubiju-dev/jobqueue/internal/job"
)

type Queue interface {
	Dequeue(ctx context.Context) (string, error)
}

type Store interface {
	Get(ctx context.Context, id string) (*job.Job, error)
	MarkProcessing(ctx context.Context, id string, now time.Time) error
	MarkCompleted(ctx context.Context, id string, now time.Time) error
	MarkFailed(ctx context.Context, id string, now time.Time, msg string) error
}

type Worker struct {
	queue Queue
	store Store
}

func New(q Queue, s Store) *Worker {
	return &Worker{queue: q, store: s}
}

func (w *Worker) Run(ctx context.Context) {
	for {
		id, err := w.queue.Dequeue(ctx)
		if err != nil {
			log.Printf("worker stopping: %v", err)
			return
		}
		w.handle(ctx, id)
	}
}

func (w *Worker) handle(ctx context.Context, id string) {
	if err := w.store.MarkProcessing(ctx, id, time.Now()); err != nil {
		if errors.Is(err, job.ErrNotClaimable) {
			log.Printf("job %s already claimed, skipping", id)
			return
		}
		log.Printf("claim job %s: %v", id, err)
		return
	}

	j, err := w.store.Get(ctx, id)
	if err != nil {
		log.Printf("load job %s: %v", id, err)
		return
	}

	if err := process(ctx, j); err != nil {
		log.Printf("job %s failed: %v", id, err)
		_ = w.store.MarkFailed(ctx, id, time.Now(), err.Error())
		return
	}

	if err := w.store.MarkCompleted(ctx, id, time.Now()); err != nil {
		log.Printf("complete job %s: %v", id, err)
	}
}
