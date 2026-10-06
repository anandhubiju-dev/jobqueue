package worker

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/anandhubiju-dev/jobqueue/internal/job"
)

const jobTimeout = 10 * time.Second

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
	id    int
	queue Queue
	store Store
}

func New(id int, q Queue, s Store) *Worker {
	return &Worker{id: id, queue: q, store: s}
}

func (w *Worker) Run(ctx context.Context) {
	for {
		id, err := w.queue.Dequeue(ctx)
		if err != nil {
			log.Printf("[worker %d] stopping: %v", w.id, err)
			return
		}
		w.handle(context.WithoutCancel(ctx), id)
	}
}

func (w *Worker) handle(ctx context.Context, id string) {
	if err := w.store.MarkProcessing(ctx, id, time.Now()); err != nil {
		if errors.Is(err, job.ErrNotClaimable) {
			log.Printf("[worker %d] job %s already claimed, skipping", w.id, id)
			return
		}
		log.Printf("[worker %d] claim job %s: %v", w.id, id, err)
		return
	}

	j, err := w.store.Get(ctx, id)
	if err != nil {
		log.Printf("[worker %d] load job %s: %v", w.id, id, err)
		return
	}

	log.Printf("[worker %d] started job %s (%s)", w.id, j.ID, j.Type)

	jobCtx, cancel := context.WithTimeout(ctx, jobTimeout)
	err = process(jobCtx, j)
	cancel()
	if err != nil {
		log.Printf("[worker %d] job %s failed: %v", w.id, id, err)
		w.fail(id, err)
		return
	}

	if err := w.store.MarkCompleted(ctx, id, time.Now()); err != nil {
		log.Printf("[worker %d] complete job %s: %v", w.id, id, err)
		return
	}

	log.Printf("[worker %d] finished job %s", w.id, id)
}

func (w *Worker) fail(id string, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.store.MarkFailed(ctx, id, time.Now(), cause.Error()); err != nil {
		log.Printf("[worker %d] mark failed %s: %v", w.id, id, err)
	}
}
