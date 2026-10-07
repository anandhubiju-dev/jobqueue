package store

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/anandhubiju-dev/jobqueue/internal/job"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()

	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	st, err := New(context.Background(), url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

func insertTestJob(t *testing.T, st *Store) string {
	t.Helper()

	now := time.Now()
	j := &job.Job{
		ID:          uuid.NewString(),
		Type:        "email",
		Payload:     json.RawMessage(`{}`),
		Status:      job.StatusQueued,
		MaxAttempts: 3,
		RunAt:       now,
		CreatedAt:   now,
	}
	if err := st.Insert(context.Background(), j); err != nil {
		t.Fatalf("insert: %v", err)
	}
	t.Cleanup(func() {
		_, _ = st.pool.Exec(context.Background(), "DELETE FROM jobs where id = $1,", j.ID)
	})
	return j.ID
}

func TestLifecycle(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	id := insertTestJob(t, st)

	// claim
	if err := st.MarkProcessing(ctx, id, time.Now()); err != nil {
		t.Fatalf("claim: %v", err)
	}
	j, err := st.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if j.Status != job.StatusProcessing || j.Attempts != 1 || j.StartedAt == nil {
		t.Fatalf("after claim: status=%s attempts=%d started=%v", j.Status, j.Attempts, j.StartedAt)
	}

	// a second claim must lose
	if err := st.MarkProcessing(ctx, id, time.Now()); err != job.ErrNotClaimable {
		t.Fatalf("second claim: want ErrNotClaimable, got %v", err)
	}

	// complete
	if err := st.MarkCompleted(ctx, id, time.Now()); err != nil {
		t.Fatalf("complete: %v", err)
	}
	j, _ = st.Get(ctx, id)
	if j.Status != job.StatusCompleted || j.CompletedAt == nil || j.Error != nil {
		t.Fatalf("after complete: status=%s completedAt=%v error=%v", j.Status, j.CompletedAt, j.Error)
	}
}

func TestMarkFailed(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	id := insertTestJob(t, st)

	if err := st.MarkProcessing(ctx, id, time.Now()); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := st.MarkFailed(ctx, id, time.Now(), "boom"); err != nil {
		t.Fatalf("fail: %v", err)
	}

	j, err := st.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if j.Status != job.StatusFailed {
		t.Fatalf("status = %s, want failed", j.Status)
	}
	if j.Error == nil || *j.Error != "boom" {
		t.Fatalf("error = %v, want boom", j.Error)
	}
	if j.CompletedAt == nil {
		t.Fatal("completed_at not set")
	}
}

func TestMarkRetrying(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	id := insertTestJob(t, st)

	if err := st.MarkProcessing(ctx, id, time.Now()); err != nil {
		t.Fatalf("claim: %v", err)
	}
	next := time.Now().Add(5 * time.Second)
	if err := st.MarkRetrying(ctx, id, next, "timeout"); err != nil {
		t.Fatalf("retrying: %v", err)
	}

	j, err := st.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if j.Status != job.StatusRetrying || j.Attempts != 1 {
		t.Fatalf("status=%s attempts=%d", j.Status, j.Attempts)
	}
	if j.Error == nil || *j.Error != "timeout" {
		t.Fatalf("error = %v", j.Error)
	}
	if j.RunAt.Sub(next).Abs() > time.Second {
		t.Fatalf("run_at = %v, want about %v", j.RunAt, next)
	}
}
