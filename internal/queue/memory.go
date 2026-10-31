package queue

import (
	"context"
	"errors"
)

var ErrFull = errors.New("queue is full")

type InMemory struct {
	ch chan string
}

func NewInMemory(size int) *InMemory {
	return &InMemory{ch: make(chan string, size)}
}

func (q *InMemory) Len(ctx context.Context) (int64, error) {
	return int64(len(q.ch)), nil
}

func (q *InMemory) Enqueue(ctx context.Context, id string) error {
	select {
	case q.ch <- id:
		return nil
	default:
		return ErrFull
	}
}

func (q *InMemory) Dequeue(ctx context.Context) (string, error) {
	select {
	case id := <-q.ch:
		return id, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (q *InMemory) Ping(ctx context.Context) error {
	return nil // in-process channel: nothing external to check
}
