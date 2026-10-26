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
	if err := st.MarkCompleted(ctx, id, 1, time.Now()); err != nil {
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
	if err := st.MarkFailed(ctx, id, 1, time.Now(), "boom"); err != nil {
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
	if err := st.MarkRetrying(ctx, id, 1, next, "timeout"); err != nil {
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

func TestClaimDue(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	due := insertTestJob(t, st)
	later := insertTestJob(t, st)

	for id, at := range map[string]time.Time{
		due:   time.Now().Add(-time.Second),
		later: time.Now().Add(time.Hour),
	} {
		if err := st.MarkProcessing(ctx, id, time.Now()); err != nil {
			t.Fatalf("claim: %v", err)
		}
		if err := st.MarkRetrying(ctx, id, 1, at, "x"); err != nil {
			t.Fatalf("retrying: %v", err)
		}
	}

	ids, err := st.ClaimDue(ctx, time.Now(), 100)
	if err != nil {
		t.Fatalf("claim due: %v", err)
	}
	found := false
	for _, id := range ids {
		if id == later {
			t.Fatal("future job was claimed")
		}
		if id == due {
			found = true
		}
	}
	if !found {
		t.Fatal("due job was not claimed")
	}

	j, _ := st.Get(ctx, due)
	if j.Status != job.StatusQueued {
		t.Fatalf("status = %s, want queued", j.Status)
	}

	again, _ := st.ClaimDue(ctx, time.Now(), 100)
	for _, id := range again {
		if id == due {
			t.Fatal("job claimed twice")
		}
	}
}

func TestHeartbeat(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	id := insertTestJob(t, st)

	claimedAt := time.Now()
	if err := st.MarkProcessing(ctx, id, claimedAt); err != nil {
		t.Fatalf("claim: %v", err)
	}

	readHB := func() time.Time {
		var hb *time.Time
		err := st.pool.QueryRow(ctx, "SELECT heartbeat_at FROM jobs WHERE id = $1", id).Scan(&hb)
		if err != nil || hb == nil {
			t.Fatalf("heartbeat_at = %v, err = %v", hb, err)
		}
		return *hb
	}

	if hb := readHB(); hb.Sub(claimedAt).Abs() > time.Second {
		t.Fatalf("claim should set heartbeat, got %v", hb)
	}

	later := claimedAt.Add(time.Minute)
	if err := st.Heartbeat(ctx, id, 1, later); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if hb := readHB(); hb.Sub(later).Abs() > time.Second {
		t.Fatalf("heartbeat not advanced, got %v", hb)
	}

	// wrong attempt number: a replaced worker must be rejected
	if err := st.Heartbeat(ctx, id, 2, later); err != job.ErrNotOwner {
		t.Fatalf("stale attempt: want ErrNotOwner, got %v", err)
	}

	// once the job is finished, nobody owns it
	if err := st.MarkCompleted(ctx, id, 1, time.Now()); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := st.Heartbeat(ctx, id, 1, later); err != job.ErrNotOwner {
		t.Fatalf("after complete: want ErrNotOwner, got %v", err)
	}
}

func TestReclaimStale(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	retryable := insertTestJob(t, st)
	exhausted := insertTestJob(t, st)

	for _, id := range []string{retryable, exhausted} {
		if err := st.MarkProcessing(ctx, id, time.Now()); err != nil {
			t.Fatalf("claim: %v", err)
		}
	}
	// make 'exhausted' have used all its attempts
	_, err := st.pool.Exec(ctx, "UPDATE jobs SET attempts = max_attempts WHERE id = $1", exhausted)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	now := time.Now()
	ids, err := st.ReclaimStale(ctx, now, now, now.Add(time.Minute), 100)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	got := map[string]bool{}
	for _, id := range ids {
		got[id] = true
	}
	if !got[retryable] || !got[exhausted] {
		t.Fatalf("expected both reclaimed, got %v", ids)
	}

	j, _ := st.Get(ctx, retryable)
	if j.Status != job.StatusRetrying || j.Error == nil {
		t.Fatalf("retryable: status=%s error=%v", j.Status, j.Error)
	}
	j, _ = st.Get(ctx, exhausted)
	if j.Status != job.StatusFailed || j.CompletedAt == nil {
		t.Fatalf("exhausted: status=%s completedAt=%v", j.Status, j.CompletedAt)
	}

	// a job with a fresh heartbeat must be left alone
	fresh := insertTestJob(t, st)
	if err := st.MarkProcessing(ctx, fresh, time.Now()); err != nil {
		t.Fatalf("claim fresh: %v", err)
	}
	ids, _ = st.ReclaimStale(ctx, now, now, now.Add(-time.Minute), 100)
	for _, id := range ids {
		if id == fresh {
			t.Fatal("fresh job was reclaimed")
		}
	}
}

func TestFenceRejectsZombie(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	id := insertTestJob(t, st)

	// attempt 1 starts, then is "lost" and reclaimed
	if err := st.MarkProcessing(ctx, id, time.Now()); err != nil {
		t.Fatalf("claim 1: %v", err)
	}
	if err := st.MarkRetrying(ctx, id, 1, time.Now(), "lost"); err != nil {
		t.Fatalf("retrying: %v", err)
	}
	if _, err := st.ClaimDue(ctx, time.Now(), 100); err != nil {
		t.Fatalf("claim due: %v", err)
	}
	// attempt 2 starts
	if err := st.MarkProcessing(ctx, id, time.Now()); err != nil {
		t.Fatalf("claim 2: %v", err)
	}

	// the zombie from attempt 1 wakes up and tries to finish
	if err := st.MarkCompleted(ctx, id, 1, time.Now()); err != job.ErrNotOwner {
		t.Fatalf("zombie complete: want ErrNotOwner, got %v", err)
	}
	if err := st.MarkFailed(ctx, id, 1, time.Now(), "x"); err != job.ErrNotOwner {
		t.Fatalf("zombie fail: want ErrNotOwner, got %v", err)
	}

	j, _ := st.Get(ctx, id)
	if j.Status != job.StatusProcessing || j.Attempts != 2 {
		t.Fatalf("live attempt was disturbed: status=%s attempts=%d", j.Status, j.Attempts)
	}

	// the real owner can still finish
	if err := st.MarkCompleted(ctx, id, 2, time.Now()); err != nil {
		t.Fatalf("owner complete: %v", err)
	}
}

func TestRecordChargeIdempotent(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	id := insertTestJob(t, st)
	t.Cleanup(func() {
		_, _ = st.pool.Exec(context.Background(), "DELETE FROM charges WHERE job_id = $1", id)
	})

	created, err := st.RecordCharge(ctx, id, 500)
	if err != nil || !created {
		t.Fatalf("first charge: created=%v err=%v", created, err)
	}
	created, err = st.RecordCharge(ctx, id, 500)
	if err != nil || created {
		t.Fatalf("second charge: created=%v err=%v", created, err)
	}

	var n int
	if err := st.pool.QueryRow(ctx, "SELECT count(*) FROM charges WHERE job_id = $1", id).Scan(&n); err != nil || n != 1 {
		t.Fatalf("charge rows = %d, err = %v", n, err)
	}
}
