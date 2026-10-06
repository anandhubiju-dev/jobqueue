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
INSERT INTO jobs (id, type, payload, status, attempts, max_attempts, run_at, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

func (s *Store) Insert(ctx context.Context, j *job.Job) error {
	_, err := s.pool.Exec(ctx, insertJobSQL,
		j.ID, j.Type, j.Payload, j.Status,
		j.Attempts, j.MaxAttempts, j.RunAt, j.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert job %s: %w", j.ID, err)
	}
	return nil
}

const getJobSQL = `
SELECT id, type, payload, status, attempts, max_attempts,
       run_at, created_at, started_at, completed_at, error
FROM jobs
WHERE id = $1`

func (s *Store) Get(ctx context.Context, id string) (*job.Job, error) {
	var j job.Job
	err := s.pool.QueryRow(ctx, getJobSQL, id).Scan(
		&j.ID, &j.Type, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts,
		&j.RunAt, &j.CreatedAt, &j.StartedAt, &j.CompletedAt, &j.Error,
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
SET status = 'processing', started_at = $2, attempts = attempts + 1
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

func (s *Store) MarkCompleted(ctx context.Context, id string, now time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status = 'completed', completed_at = $2 WHERE id = $1`, id, now)
	if err != nil {
		return fmt.Errorf("mark completed %s: %w", id, err)
	}
	return nil
}

func (s *Store) MarkFailed(ctx context.Context, id string, now time.Time, msg string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status = 'failed', completed_at = $2, error = $3 WHERE id = $1`,
		id, now, msg)
	if err != nil {
		return fmt.Errorf("mark failed %s: %w", id, err)
	}
	return nil
}
