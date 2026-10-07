package worker

import (
	"math/rand"
	"time"
)

const (
	baseDelay = 1 * time.Second
	maxDelay  = 1 * time.Minute
)

// backoff returns how long to wait before the next attempt.
// attempt is the number of attempts already made (1 after the first failure).
func backoff(attempt int) time.Duration {
	d := baseDelay << (attempt - 1) // 1s, 2s, 3s, 4s, 8s...
	if d > maxDelay || d <= 0 {
		d = maxDelay
	}
	// full jitter: pick a random point in [d/2, d]
	half := d / 2
	return half + time.Duration(rand.Int63n(int64(half)+1))
}
