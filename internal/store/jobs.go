package store

import (
	"context"
	"errors"
	"fmt"

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
		return nil, fmt.Errorf("get job%s: %w", id, err)
	}
	return &j, nil
}
