package queue

import "context"

type Queue interface {
	Enqueue(ctx context.Context, id string) error
	Dequeue(ctx context.Context) (string, error)
	Len(ctx context.Context) (int64, error)
}

var (
	_ Queue = (*InMemory)(nil)
	_ Queue = (*Redis)(nil)
)
