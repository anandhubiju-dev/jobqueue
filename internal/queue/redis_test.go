package queue

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

func newTestRedis(t *testing.T, size int) *Redis {
	t.Helper()

	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("TEST_REDIS_URL not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	q, err := NewRedis(ctx, url, size)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// unique key per test, so we never touch the app's real queue
	q.key = "test:" + uuid.NewString()

	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		if err := q.client.Del(cctx, q.key).Err(); err != nil {
			t.Errorf("cleanup: delete %s: %v", q.key, err)
		}
		if err := q.Close(); err != nil {
			t.Errorf("cleanup: close: %v", err)
		}
	})
	return q
}

func TestRedisFIFOAndFull(t *testing.T) {
	q := newTestRedis(t, 2)
	ctx := context.Background()

	for _, id := range []string{"a", "b"} {
		if err := q.Enqueue(ctx, id); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	if err := q.Enqueue(ctx, "c"); !errors.Is(err, ErrFull) {
		t.Fatalf("want ErrFull, got %v", err)
	}

	for _, want := range []string{"a", "b"} {
		got, err := q.Dequeue(ctx)
		if err != nil || got != want {
			t.Fatalf("want %s, got %q (err %v)", want, got, err)
		}
	}
}

func TestRedisDequeueCancel(t *testing.T) {
	q := newTestRedis(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if _, err := q.Dequeue(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
}
