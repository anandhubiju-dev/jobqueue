package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/anandhubiju-dev/jobqueue/internal/job"
)

type fakeStore struct{ inserted []*job.Job }

func (f *fakeStore) Insert(ctx context.Context, j *job.Job) (bool, error) {
	f.inserted = append(f.inserted, j)
	return true, nil
}
func (f *fakeStore) Get(ctx context.Context, id string) (*job.Job, error) {
	return nil, job.ErrNotFound
}
func (f *fakeStore) GetByKey(ctx context.Context, key string) (*job.Job, error) {
	return nil, job.ErrNotFound
}

type fakeQueue struct{ ids []string }

func (f *fakeQueue) Enqueue(ctx context.Context, id string) error {
	f.ids = append(f.ids, id)
	return nil
}

func TestCreateScheduling(t *testing.T) {
	ctx := context.Background()
	payload := json.RawMessage(`{}`)

	create := func(runAt time.Time) (*job.Job, *fakeQueue, error) {
		q := &fakeQueue{}
		svc := New(&fakeStore{}, q)
		j, err := svc.Create(ctx, "email", payload, "", runAt, "")
		return j, q, err
	}

	t.Run("unset runs immediately", func(t *testing.T) {
		_, q, err := create(time.Time{})
		if err != nil || len(q.ids) != 1 {
			t.Fatalf("err=%v enqueued=%d, want 1", err, len(q.ids))
		}
	})

	t.Run("past runs immediately", func(t *testing.T) {
		j, q, err := create(time.Now().Add(-time.Hour))
		if err != nil || len(q.ids) != 1 || !j.RunAt.Equal(j.CreatedAt) {
			t.Fatalf("err=%v enqueued=%d", err, len(q.ids))
		}
	})

	t.Run("future is not enqueued", func(t *testing.T) {
		j, q, err := create(time.Now().Add(time.Hour))
		if err != nil || len(q.ids) != 0 {
			t.Fatalf("err=%v enqueued=%d, want 0", err, len(q.ids))
		}
		if !j.RunAt.After(j.CreatedAt) {
			t.Fatal("scheduled job must have run_at after created_at")
		}
	})

	t.Run("too far ahead is rejected", func(t *testing.T) {
		_, _, err := create(time.Now().Add(2 * 365 * 24 * time.Hour))
		if !errors.Is(err, ErrInvalidJob) {
			t.Fatalf("want ErrInvalidJob, got %v", err)
		}
	})
}
