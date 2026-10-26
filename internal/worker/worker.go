package worker

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/anandhubiju-dev/jobqueue/internal/job"
)

const jobTimeout = 10 * time.Second
const heartbeatInterval = 2 * time.Second

type Queue interface {
	Dequeue(ctx context.Context) (string, error)
}

type Store interface {
	Get(ctx context.Context, id string) (*job.Job, error)
	MarkProcessing(ctx context.Context, id string, now time.Time) error
	MarkCompleted(ctx context.Context, id string, attempt int, now time.Time) error
	MarkFailed(ctx context.Context, id string, attempt int, now time.Time, msg string) error
	MarkRetrying(ctx context.Context, id string, attempt int, RunAt time.Time, msg string) error
	Heartbeat(ctx context.Context, id string, attempt int, now time.Time) error
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

	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		w.heartbeat(jobCtx, cancel, id, j.Attempts)
	}()

	err = process(jobCtx, j)
	cancel()
	<-hbDone
	if err != nil {
		log.Printf("[worker %d] job %s failed (permanent=%v): %v", w.id, id, IsPermanent(err), err)
		w.retryOrFail(j, err)
		return
	}

	if err := w.store.MarkCompleted(ctx, id, j.Attempts, time.Now()); err != nil {
		if errors.Is(err, job.ErrNotOwner) {
			log.Printf("[worker %d] job %s was taken over; discarding my result", w.id, id)
			return
		}
		log.Printf("[worker %d] complete job %s: %v", w.id, id, err)
		return
	}

	log.Printf("[worker %d] finished job %s", w.id, id)
}

func (w *Worker) fail(j *job.Job, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := w.store.MarkFailed(ctx, j.ID, j.Attempts, time.Now(), cause.Error())
	if errors.Is(err, job.ErrNotOwner) {
		log.Printf("[worker %d] job %s was taken over; discarding my failure", w.id, j.ID)
		return
	}
	if err != nil {
		log.Printf("[worker %d] mark failed %s: %v", w.id, j.ID, err)
	}
}

func (w *Worker) retryOrFail(j *job.Job, cause error) {
	if IsPermanent(cause) || j.Attempts >= j.MaxAttempts {
		w.fail(j, cause)
		return
	}

	delay := backoff(j.Attempts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := w.store.MarkRetrying(ctx, j.ID, j.Attempts, time.Now().Add(delay), cause.Error())
	if errors.Is(err, job.ErrNotOwner) {
		log.Printf("[worker %d] job %s was taken over; not scheduling a retry", w.id, j.ID)
		return
	}
	if err != nil {
		log.Printf("[worker %d] mark retrying %s: %v", w.id, j.ID, err)
		return
	}
	log.Printf("[worker %d] job %s will retry in %v (attempt %d/%d)",
		w.id, j.ID, delay.Round(time.Millisecond), j.Attempts, j.MaxAttempts)
}

func (w *Worker) heartbeat(ctx context.Context, cancelJob context.CancelFunc, id string, attempt int) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := w.store.Heartbeat(ctx, id, attempt, time.Now())
			if ctx.Err() != nil {
				return // job finished while the writr was in fight
			}
			if errors.Is(err, job.ErrNotOwner) {
				log.Printf("[worker %d] job %s was taken over, abondoning it", w.id, id)
				cancelJob()
				return
			}
			if err != nil {
				log.Printf("[worker %d] heartbeat %s: %v", w.id, id, err)
			}
		}
	}
}
