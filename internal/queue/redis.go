package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const redisKey = "jobqueue:ids"

type Redis struct {
	client  *redis.Client
	maxSize int64
	key     string
}

func NewRedis(ctx context.Context, url string, maxSize int) (*Redis, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Redis{client: client, maxSize: int64(maxSize), key: redisKey}, nil
}

func (q *Redis) Close() error { return q.client.Close() }

func (q *Redis) Enqueue(ctx context.Context, id string) error {
	n, err := q.client.LLen(ctx, q.key).Result()
	if err != nil {
		return fmt.Errorf("redis llen: %w", err)
	}
	if n >= q.maxSize {
		return ErrFull
	}
	if err := q.client.LPush(ctx, q.key, id).Err(); err != nil {
		return fmt.Errorf("redis lpush: %w", err)
	}
	return nil
}

func (q *Redis) Dequeue(ctx context.Context) (string, error) {
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		res, err := q.client.BRPop(ctx, time.Second, q.key).Result()
		if errors.Is(err, redis.Nil) {
			continue // timed out with nothing to do; loop and re-check ctx
		}
		if err != nil {
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			return "", fmt.Errorf("redis brpop: %w", err)
		}
		return res[1], nil // res[0] is the key, res[1] is the value
	}
}
