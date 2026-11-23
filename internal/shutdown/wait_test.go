package shutdown

import (
	"sync"
	"testing"
	"time"
)

func TestWaitTimeoutFinishes(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		wg.Done()
	}()

	if !WaitTimeout(&wg, 2*time.Second) {
		t.Fatal("WaitGroup finished in time, but WaitTimeout reported a timeout")
	}
}

func TestWaitTimeoutGivesUp(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1) // never released: stands in for a hung worker
	defer wg.Done()

	start := time.Now()
	if WaitTimeout(&wg, 100*time.Millisecond) {
		t.Fatal("WaitGroup never finished, but WaitTimeout reported success")
	}
	if d := time.Since(start); d < 100*time.Millisecond || d > time.Second {
		t.Fatalf("returned after %v, want about 100ms", d)
	}
}
