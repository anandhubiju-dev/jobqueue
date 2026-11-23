package shutdown

import (
	"sync"
	"time"
)

// WaitTimeout waits for wg, but gives up after d.
// It reports whether the WaitGroup finished in time.
func WaitTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}
