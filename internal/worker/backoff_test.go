package worker

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	for attempt := 1; attempt <= 10; attempt++ {
		for i := 0; i < 100; i++ {
			d := backoff(attempt)
			if d <= 0 || d > maxDelay {
				t.Fatalf("attempt %d: delay %v out of range", attempt, d)
			}
		}
	}

	// attempt 1 must fall in [0.5s, 1s]
	for i := 0; i < 100; i++ {
		d := backoff(1)
		if d < 500*time.Millisecond || d > time.Second {
			t.Fatalf("attempt 1: delay %v not in [0.5s, 1s]", d)
		}
	}

	// huge attempt numbers must not overflow
	if d := backoff(200); d <= 0 || d > maxDelay {
		t.Fatalf("attempt 200: delay %v", d)
	}
}
