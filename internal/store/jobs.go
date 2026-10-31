package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/anandhubiju-dev/jobqueue/internal/job"
	"github.com/jackc/pgx/v5"
)

const insertJobSQL = `
INSERT INTO jobs (id, type, payload, status, attempts, max_attempts, run_at, created_at, idempotency_key, created_by_request_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`

func (s *Store) Insert(ctx context.Context, j *job.Job) (created bool, err error) {
	tag, err := s.pool.Exec(ctx, insertJobSQL,
		j.ID, j.Type, j.Payload, j.Status,
		j.Attempts, j.MaxAttempts, j.RunAt, j.CreatedAt,
		j.IdempotencyKey, j.CreatedByRequestID,
	)
	if err != nil {
		return false, fmt.Errorf("insert job %s: %w", j.ID, err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) GetByKey(ctx context.Context, key string) (*job.Job, error) {
	var id string
	err := s.pool.QueryRow(ctx, `SELECT id FROM jobs WHERE idempotency_key = $1`, key).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, job.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get by key: %w", err)
	}
	return s.Get(ctx, id)
}

const getJobSQL = `
SELECT id, type, payload, status, attempts, max_attempts,
       run_at, created_at, started_at, completed_at, error,
	   created_by_request_id
FROM jobs
WHERE id = $1`

func (s *Store) Get(ctx context.Context, id string) (*job.Job, error) {
	var j job.Job
	err := s.pool.QueryRow(ctx, getJobSQL, id).Scan(
		&j.ID, &j.Type, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts,
		&j.RunAt, &j.CreatedAt, &j.StartedAt, &j.CompletedAt, &j.Error,
		&j.CreatedByRequestID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, job.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job %s: %w", id, err)
	}
	return &j, nil
}

const claimJobSQL = `
UPDATE jobs
SET status = 'processing', started_at = $2, heartbeat_at = $2, attempts = attempts + 1
WHERE id = $1 AND status = 'queued'`

func (s *Store) MarkProcessing(ctx context.Context, id string, now time.Time) error {
	tag, err := s.pool.Exec(ctx, claimJobSQL, id, now)
	if err != nil {
		return fmt.Errorf("mark processing %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return job.ErrNotClaimable
	}
	return nil
}

func (s *Store) MarkCompleted(ctx context.Context, id string, attempt int, now time.Time) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status = 'completed', completed_at = $2, error = NULL
		 WHERE id = $1 AND status = 'processing' AND attempts = $3`,
		id, now, attempt)
	return fencedResult("mark completed", id, tag.RowsAffected(), err)
}

func (s *Store) MarkFailed(ctx context.Context, id string, attempt int, now time.Time, msg string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status = 'failed', completed_at = $2, error = $3
		 WHERE id = $1 AND status = 'processing' AND attempts = $4`,
		id, now, msg, attempt)
	return fencedResult("mark failed", id, tag.RowsAffected(), err)
}

func (s *Store) MarkRetrying(ctx context.Context, id string, attempt int, runAt time.Time, msg string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status = 'retrying', run_at = $2, error = $3
		 WHERE id = $1 AND status = 'processing' AND attempts = $4`,
		id, runAt, msg, attempt)
	return fencedResult("mark retrying", id, tag.RowsAffected(), err)
}

func (s *Store) Requeue(ctx context.Context, id string, runAt time.Time, msg string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status = 'retrying', run_at = $2, error = $3
		 WHERE id = $1 AND status = 'queued'`,
		id, runAt, msg)
	if err != nil {
		return fmt.Errorf("requeue %s: %w", id, err)
	}
	return nil
}

func fencedResult(op, id string, rows int64, err error) error {
	if err != nil {
		return fmt.Errorf("%s %s: %w", op, id, err)
	}
	if rows == 0 {
		return job.ErrNotOwner
	}
	return nil
}

const claimDueSQL = `
UPDATE jobs SET status = 'queued'
WHERE id IN (
    SELECT id FROM jobs
    WHERE status = 'retrying' AND run_at <= $1
    ORDER BY run_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
RETURNING id`

func (s *Store) ClaimDue(ctx context.Context, now time.Time, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx, claimDueSQL, now, limit)
	if err != nil {
		return nil, fmt.Errorf("claim due: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan due id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim due: %w", err)
	}
	return ids, nil
}

func (s *Store) ListOrphanedQueued(ctx context.Context, now time.Time, olderThan time.Time, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id FROM jobs
		 WHERE status = 'queued' AND run_at <= $1 AND created_at < $2
		 ORDER BY created_at
		 LIMIT $3`,
		now, olderThan, limit)
	if err != nil {
		return nil, fmt.Errorf("list orphaned queued: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan orphaned id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list orphaned queued: %w", err)
	}
	return ids, nil
}

func (s *Store) ListDueQueued(ctx context.Context, now time.Time, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id FROM jobs
		 WHERE status = 'queued' AND run_at > created_at AND run_at <= $1
		 ORDER BY run_at
		 LIMIT $2`,
		now, limit)
	if err != nil {
		return nil, fmt.Errorf("list due queued: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan due id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list due queued: %w", err)
	}
	return ids, nil
}

func (s *Store) Heartbeat(ctx context.Context, id string, attempt int, now time.Time) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE jobs SET heartbeat_at = $2
		 WHERE id = $1 AND status = 'processing' AND attempts = $3`,
		id, now, attempt)
	if err != nil {
		return fmt.Errorf("heartbeat %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return job.ErrNotOwner
	}
	return nil
}

const reclaimStaleSQL = `
UPDATE jobs
SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'retrying' END,
    run_at = $2,
    completed_at = CASE WHEN attempts >= max_attempts THEN $1::timestamptz ELSE NULL END,
    error = 'worker lost (heartbeat stale)'
WHERE id IN (
    SELECT id FROM jobs
    WHERE status = 'processing' AND heartbeat_at < $3
    ORDER BY heartbeat_at
    LIMIT $4
    FOR UPDATE SKIP LOCKED
)
RETURNING id`

func (s *Store) ReclaimStale(ctx context.Context, now time.Time, retryAt time.Time, staleBefore time.Time, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx, reclaimStaleSQL, now, retryAt, staleBefore, limit)
	if err != nil {
		return nil, fmt.Errorf("reclaim stale: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan stale id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reclaim stale: %w", err)
	}
	return ids, nil
}

func (s *Store) RecordCharge(ctx context.Context, jobID string, amountCents int) (created bool, err error) {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO charges (job_id, amount_cents) VALUES ($1, $2)
		 ON CONFLICT (job_id) DO NOTHING`,
		jobID, amountCents)
	if err != nil {
		return false, fmt.Errorf("record charge %s: %w", jobID, err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) CountByStatus(ctx context.Context) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM jobs GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("count by status: %w", err)
	}
	defer rows.Close()

	out := make(map[string]int64)
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return nil, fmt.Errorf("scan status count: %w", err)
		}
		out[status] = n
	}
	return out, rows.Err()
}
