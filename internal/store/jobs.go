package store

import (
	"context"
	"fmt"

	"github.com/anandhubiju-dev/jobqueue/internal/job"
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
