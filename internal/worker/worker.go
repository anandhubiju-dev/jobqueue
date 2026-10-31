package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/anandhubiju-dev/jobqueue/internal/job"
	"github.com/anandhubiju-dev/jobqueue/internal/metrics"
)

const jobTimeout = 10 * time.Second
const heartbeatInterval = 2 * time.Second

type Queue interface {
	Dequeue(ctx context.Context) (string, error)
}

type Store interface {
	Insert(ctx context.Context, j *job.Job) (bool, error)
	Get(ctx context.Context, id string) (*job.Job, error)
	GetByKey(ctx context.Context, key string) (*job.Job, error)
	MarkProcessing(ctx context.Context, id string, now time.Time) error
	MarkCompleted(ctx context.Context, id string, attempt int, now time.Time) error
	MarkFailed(ctx context.Context, id string, attempt int, now time.Time, msg string) error
	MarkRetrying(ctx context.Context, id string, attempt int, RunAt time.Time, msg string) error
	Heartbeat(ctx context.Context, id string, attempt int, now time.Time) error
	RecordCharge(ctx context.Context, jobID string, amountCents int) (bool, error)
}

type Worker struct {
	id    int
	queue Queue
	store Store
}

func New(id int, q Queue, s Store) *Worker {
	return &Worker{id: id, queue: q, store: s}
}

func (w *Worker) log() *slog.Logger {
	return slog.With("worker", w.id)
}

func (w *Worker) jobLog(j *job.Job) *slog.Logger {
	l := w.log().With("job_id", j.ID)
	if j.CreatedByRequestID != nil {
		l = l.With("request_id", *j.CreatedByRequestID)
	}
	return l
}

func (w *Worker) Run(ctx context.Context) {
	for {
		id, err := w.queue.Dequeue(ctx)
		if err != nil {
			w.log().Info("worker stopping", "reason", err)
			return
		}
		w.handle(context.WithoutCancel(ctx), id)
	}
}

func (w *Worker) handle(ctx context.Context, id string) {
	if err := w.store.MarkProcessing(ctx, id, time.Now()); err != nil {
		if errors.Is(err, job.ErrNotClaimable) {
			w.log().Info("job already claimed, skipping", "job_id", id)
			return
		}
		w.log().Error("claim job failed", "job_id", id, "err", err)
		return
	}

	j, err := w.store.Get(ctx, id)
	if err != nil {
		w.log().Error("load job failed", "job_id", id, "err", err)
		return
	}

	w.jobLog(j).Info("job started", "type", j.Type, "attempt", j.Attempts)

	metrics.JobsStarted.Add(1)
	start := time.Now()

	jobCtx, cancel := context.WithTimeout(ctx, jobTimeout)

	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		w.heartbeat(jobCtx, cancel, id, j.Attempts)
	}()

	err = w.process(jobCtx, j)
	cancel()
	<-hbDone
	metrics.ObserveDuration(time.Since(start))
	if err != nil {
		w.log().Warn("job failed", "job_id", id, "permanent", IsPermanent(err), "err", err)
		w.retryOrFail(j, err)
		return
	}

	if err := w.store.MarkCompleted(ctx, id, j.Attempts, time.Now()); err != nil {
		if errors.Is(err, job.ErrNotOwner) {
			w.log().Info("job taken over; discarding result", "job_id", id)
			return
		}
		w.log().Error("complete job failed", "job_id", id, "err", err)
		metrics.JobsTakenOver.Add(1)
		return
	}

	metrics.JobsCompleted.Add(1)
	w.jobLog(j).Info("job finished", "attempt", j.Attempts)
}

func (w *Worker) fail(j *job.Job, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := w.store.MarkFailed(ctx, j.ID, j.Attempts, time.Now(), cause.Error())
	if errors.Is(err, job.ErrNotOwner) {
		w.log().Info("job taken over; discarding failure", "job_id", j.ID)
		return
	}
	if err != nil {
		w.log().Error("mark failed over", "job_id", j.ID, "err", err)
	}
	metrics.JobsFailed.Add(1)
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
		w.log().Info("job was taken over; not scheduling retry", "job_id", j.ID)
		return
	}
	if err != nil {
		w.log().Error("mark retrying error", "job_id", j.ID, "err", err)
		return
	}
	metrics.JobsRetried.Add(1)
	w.log().Info("job will retry", "job_id", j.ID, "delay", delay.Round(time.Millisecond),
		"attempt", j.Attempts, "max_attempts", j.MaxAttempts)
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
				w.log().Info("job taken over, abondoning it", "job_id", id)
				cancelJob()
				return
			}
			if err != nil {
				w.log().Error("heartbeat error", "job_id", id, "err", err)
			}
		}
	}
}
