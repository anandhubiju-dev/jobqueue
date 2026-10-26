package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/anandhubiju-dev/jobqueue/internal/job"
)

func (w *Worker) process(ctx context.Context, j *job.Job) error {
	var d time.Duration
	switch j.Type {
	case "email":
		d = 2 * time.Second
	case "report":
		d = 4 * time.Second
	case "notification":
		d = 1 * time.Second
	case "flaky":
		var p struct {
			FailTimes int `json:"fail_times"`
		}
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return Permanent(fmt.Errorf("bad flaky payload: %w", err))
		}
		if j.Attempts <= p.FailTimes {
			return fmt.Errorf("simulated failure on attempt %d", j.Attempts)
		}
		d = 500 * time.Millisecond
	case "payment":
		var p struct {
			AmountCents     int  `json:"amount_cents"`
			FailAfterCharge bool `json:"fail_after_charge"`
		}
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return Permanent(fmt.Errorf("bad payment payload: %w", err))
		}
		if err := w.store.RecordCharge(ctx, j.ID, p.AmountCents); err != nil {
			return err
		}
		if p.FailAfterCharge && j.Attempts == 1 {
			return fmt.Errorf("lost connection after charging")
		}
		d = 500 * time.Millisecond
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
