package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/anandhubiju-dev/jobqueue/internal/job"
)

func process(ctx context.Context, j *job.Job) error {
	var d time.Duration
	switch j.Type {
	case "email":
		d = 2 * time.Second
	case "report":
		d = 4 * time.Second
	case "notification":
		d = 1 * time.Second
	default:
		return fmt.Errorf("no processor for type %q", j.Type)
	}

	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
