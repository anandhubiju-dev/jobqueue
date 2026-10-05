package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestFIFOAndFull(t *testing.T) {
	q := NewInMemory(2)
	ctx := context.Background()

	_ = q.Enqueue(ctx, "a")
	_ = q.Enqueue(ctx, "b")

	if err := q.Enqueue(ctx, "c"); !errors.Is(err, ErrFull) {
		t.Fatalf("want ErrFull, got %v", err)
	}

	got, err := q.Dequeue(ctx)
	if err != nil || got != "a" {
		t.Fatalf("want a, got %q (err %v)", got, err)
	}
}

func TestDequeueCancel(t *testing.T) {
	q := NewInMemory(1)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := q.Dequeue(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
}
