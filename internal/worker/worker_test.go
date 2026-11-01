package worker

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/anandhubiju-dev/jobqueue/internal/job"
)

type fakeQueue struct{ ch chan string }

func (q *fakeQueue) Dequeue(ctx context.Context) (string, error) {
	select {
	case id := <-q.ch:
		return id, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

type fakeStore struct {
	mu        sync.Mutex
	jobs      map[string]*job.Job
	completed int
}

func (s *fakeStore) Get(ctx context.Context, id string) (*job.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *s.jobs[id]
	return &cp, nil
}

func (s *fakeStore) MarkProcessing(ctx context.Context, id string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[id]
	if j.Status != job.StatusQueued {
		return job.ErrNotClaimable
	}
	j.Status = job.StatusProcessing
	j.Attempts++
	return nil
}

func (s *fakeStore) MarkCompleted(ctx context.Context, id string, attempt int, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[id]
	if j.Status != job.StatusProcessing || j.Attempts != attempt {
		return job.ErrNotOwner
	}
	j.Status = job.StatusCompleted
	s.completed++
	return nil
}

func (s *fakeStore) MarkFailed(ctx context.Context, id string, attempt int, now time.Time, msg string) error {
	return nil
}
func (s *fakeStore) MarkRetrying(ctx context.Context, id string, attempt int, runAt time.Time, msg string) error {
	return nil
}
func (s *fakeStore) Heartbeat(ctx context.Context, id string, attempt int, now time.Time) error {
	return nil
}
func (s *fakeStore) RecordCharge(ctx context.Context, jobID string, amountCents int) (bool, error) {
	return true, nil
}

func TestPoolProcessesEachJobOnce(t *testing.T) {
	const n = 50
	st := &fakeStore{jobs: map[string]*job.Job{}}
	q := &fakeQueue{ch: make(chan string, n*2)}

	for i := 0; i < n; i++ {
		id := string(rune('A'+i/26)) + string(rune('a'+i%26))
		st.jobs[id] = &job.Job{ID: id, Type: "notification", Payload: json.RawMessage(`{}`),
			Status: job.StatusQueued, MaxAttempts: 3}
		q.ch <- id
		q.ch <- id // duplicate delivery: the claim must make this harmless
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 1; i <= 4; i++ {
		w := New(i, q, st)
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Run(ctx)
		}()
	}

	deadline := time.After(30 * time.Second)
	for {
		st.mu.Lock()
		done := st.completed
		st.mu.Unlock()
		if done == n {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d of %d jobs completed", done, n)
		case <-time.After(50 * time.Millisecond):
		}
	}
	cancel()
	wg.Wait()

	for id, j := range st.jobs {
		if j.Attempts != 1 {
			t.Errorf("job %s ran %d times, want exactly 1", id, j.Attempts)
		}
	}
}
