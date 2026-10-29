package scheduler

import (
	"context"
	"log/slog"
	"time"
)

type Store interface {
	ClaimDue(ctx context.Context, now time.Time, limit int) ([]string, error)
	Requeue(ctx context.Context, id string, runAt time.Time, msg string) error
	ReclaimStale(ctx context.Context, now, retryAt, staleBefore time.Time, limit int) ([]string, error)
	ListOrphanedQueued(ctx context.Context, now, olderThan time.Time, limit int) ([]string, error)
	ListDueQueued(ctx context.Context, now time.Time, limit int) ([]string, error)
}

type Queue interface {
	Enqueue(ctx context.Context, id string) error
}

type Scheduler struct {
	store    Store
	queue    Queue
	interval time.Duration
}

func New(s Store, q Queue, interval time.Duration) *Scheduler {
	return &Scheduler{store: s, queue: q, interval: interval}
}

func (s *Scheduler) log() *slog.Logger {
	return slog.With("component", "scheduler")
}

const staleAfter = 6 * time.Second // 3 missed heartbeats
const orphanGrace = 10 * time.Second

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.log().Info("scheduler stopping", "reason", ctx.Err())
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) {
	now := time.Now()
	lost, err := s.store.ReclaimStale(ctx, now, now, now.Add(-staleAfter), 50)
	if err != nil {
		s.log().Error("reclaim stale failed", "err", err)
	}
	for _, id := range lost {
		s.log().Info("reclaimed stale job", "job_id", id)
	}

	scheduled, err := s.store.ListDueQueued(ctx, time.Now(), 50)
	if err != nil {
		s.log().Error("list due scheduled failed", "err", err)
	}
	for _, id := range scheduled {
		if err := s.queue.Enqueue(ctx, id); err != nil {
			s.log().Warn("enqueue scheduled failed", "job_id", id, "err", err)
			break
		}
		s.log().Info("enqueued scheduled job", "job_id", id)
	}

	ids, err := s.store.ClaimDue(ctx, time.Now(), 50)
	if err != nil {
		s.log().Error("claim due failed", "err", err)
		return
	}
	for _, id := range ids {
		if err := s.queue.Enqueue(ctx, id); err != nil {
			s.log().Warn("enqueue failed back to retrying", "job_id", id, "err", err)
			_ = s.store.Requeue(context.WithoutCancel(ctx), id, time.Now().Add(time.Second), "queue full")
			continue
		}
		s.log().Info("re-enqueued job", "job_id", id)
	}

	orphans, err := s.store.ListOrphanedQueued(ctx, time.Now(), time.Now().Add(-orphanGrace), 50)
	if err != nil {
		s.log().Error("list orphaned failed", "err", err)
		return
	}
	for _, id := range orphans {
		if err := s.queue.Enqueue(ctx, id); err != nil {
			s.log().Warn("enqueue orphaned failed", "job_id", id, "err", err)
			break
		}
		s.log().Info("re-enqueued orphaned job", "job_id", id)
	}
}
