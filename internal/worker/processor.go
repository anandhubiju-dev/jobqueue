package worker

import (
	"context"
	"errors"
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
		return Permanent(fmt.Errorf("no processor for type %q", j.Type))
	}

	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type permanentError struct {
	err error
}

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as not worth retrying
func Permanent(err error) error {
	return &permanentError{err: err}
}

// Ispermanent reports whether err, or anything it wraps, is marked permanent
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
