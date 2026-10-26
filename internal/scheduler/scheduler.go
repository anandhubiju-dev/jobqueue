package scheduler

import (
	"context"
	"log"
	"time"
)

type Store interface {
	ClaimDue(ctx context.Context, now time.Time, limit int) ([]string, error)
	Requeue(ctx context.Context, id string, runAt time.Time, msg string) error
	ReclaimStale(ctx context.Context, now, retryAt, staleBefore time.Time, limit int) ([]string, error)
	ListOrphanedQueued(ctx context.Context, now, olderThan time.Time, limit int) ([]string, error)
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

const staleAfter = 6 * time.Second // 3 missed heartbeats
const orphanGrace = 10 * time.Second

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[scheduler] stopping: %v", ctx.Err())
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
		log.Printf("[scheduler] reclaim stale: %v", err)
	}
	for _, id := range lost {
		log.Printf("[scheduler] reclaimed stale job %s", id)
	}

	ids, err := s.store.ClaimDue(ctx, time.Now(), 50)
	if err != nil {
		log.Printf("[scheduler] claim due: %v", err)
		return
	}
	for _, id := range ids {
		if err := s.queue.Enqueue(ctx, id); err != nil {
			log.Printf("[scheduler] enqueue %s: %v, back to retrying", id, err)
			_ = s.store.Requeue(context.WithoutCancel(ctx), id, time.Now().Add(time.Second), "queue full")
			continue
		}
		log.Printf("[scheduler] re-enqueued job %s", id)
	}

	orphans, err := s.store.ListOrphanedQueued(ctx, time.Now(), time.Now().Add(-orphanGrace), 50)
	if err != nil {
		log.Printf("[scheduler] list orphaned: %v", err)
		return
	}
	for _, id := range orphans {
		if err := s.queue.Enqueue(ctx, id); err != nil {
			log.Printf("[scheduler] enqueue orphan %s: %v", id, err)
			break
		}
		log.Printf("[scheduler] re-enqueued orphaned job %s", id)
	}
}
