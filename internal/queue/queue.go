package queue

import "context"

type Queue interface {
	Enqueue(ctx context.Context, id string) error
	Dequeue(ctx context.Context) (string, error)
}

var (
	_ Queue = (*InMemory)(nil)
	_ Queue = (*Redis)(nil)
)
