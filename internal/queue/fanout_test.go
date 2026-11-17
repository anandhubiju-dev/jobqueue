package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type countingSource struct {
	in       chan string
	cur, max atomic.Int32
}

func (s *countingSource) Dequeue(ctx context.Context) (string, error) {
	n := s.cur.Add(1)
	defer s.cur.Add(-1)
	for {
		m := s.max.Load()
		if n <= m || s.max.CompareAndSwap(m, n) {
			break
		}
	}
	select {
	case id := <-s.in:
		return id, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func TestFanoutDeliversOnceWithFewReaders(t *testing.T) {
	const ids, consumers, dequeuers = 200, 50, 3

	src := &countingSource{in: make(chan string, ids)}
	ctx, cancel := context.WithCancel(context.Background())
	f := NewFanout(ctx, src, dequeuers)

	var mu sync.Mutex
	got := map[string]int{}
	var cwg sync.WaitGroup
	for i := 0; i < consumers; i++ {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for {
				id, err := f.Dequeue(ctx)
				if err != nil {
					return
				}
				mu.Lock()
				got[id]++
				mu.Unlock()
			}
		}()
	}

	for i := 0; i < ids; i++ {
		src.in <- string(rune('A'+i/26)) + string(rune('a'+i%26))
	}

	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == ids {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d of %d ids delivered", n, ids)
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	f.Wait()
	cwg.Wait()

	for id, c := range got {
		if c != 1 {
			t.Errorf("id %s delivered %d times", id, c)
		}
	}
	if m := src.max.Load(); m > dequeuers {
		t.Errorf("source read by %d goroutines at once, want at most %d", m, dequeuers)
	}
}
