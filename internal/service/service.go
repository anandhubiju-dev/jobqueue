package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/anandhubiju-dev/jobqueue/internal/job"
)

type Store interface {
	Insert(ctx context.Context, j *job.Job) error
	Get(ctx context.Context, id string) (*job.Job, error)
}

type Service struct {
	store Store
}

func New(store Store) *Service {
	return &Service{store: store}
}

var ErrInvalidJob = errors.New("invalid job")

var validTypes = map[string]bool{
	"email": true,
	"report": true,
	"notification": true,
}

func (s *Service) Create(ctx context.Context, jobType string, payload json.RawMessage) (*job.Job, error) {
	if !validTypes[jobType] {
		return nil, fmt.Errorf("%w: unknown type %q", ErrInvalidJob, jobType)
	}
	if !json.Valid(payload) {
		return nil, fmt.Errorf("%w: payload is not valid JSON", ErrInvalidJob)
	}

	now := time.Now()
	j := &job.Job{
		ID: uuid.NewString(),
		Type: jobType,
		Payload: payload,
		Status: job.StatusQueued,
		MaxAttempts: 3,
		RunAt: now,
		CreatedAt: now,
	}

	if err := s.store.Insert(ctx, j); err != nil {
		return nil, err
	}
	return j, nil
}

func (s *Service) Get(ctx context.Context, id string) (*job.Job, error) {
	if _, err := uuid.Parse(id); err != nil {
		return nil, job.ErrNotFound
	}
	return s.store.Get(ctx, id)
}