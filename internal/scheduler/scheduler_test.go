package scheduler

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

type fakeStore struct {
	mu sync.Mutex

	reclaimed, scheduled, due, orphans []string
	reclaimErr, claimDueErr            error

	requeued     []string
	reclaimCalls int
}

func (s *fakeStore) ReclaimStale(ctx context.Context, now, retryAt, staleBefore time.Time, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reclaimCalls++
	return s.reclaimed, s.reclaimErr
}

func (s *fakeStore) ListDueQueued(ctx context.Context, now time.Time, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scheduled, nil
}

func (s *fakeStore) ClaimDue(ctx context.Context, now time.Time, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.due, s.claimDueErr
}

func (s *fakeStore) ListOrphanedQueued(ctx context.Context, now, olderThan time.Time, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.orphans, nil
}

func (s *fakeStore) Requeue(ctx context.Context, id string, runAt time.Time, msg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requeued = append(s.requeued, id)
	return nil
}

func (s *fakeStore) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reclaimCalls
}

type fakeQueue struct {
	mu       sync.Mutex
	enqueued []string
	failFor  map[string]bool
}

func (q *fakeQueue) Enqueue(ctx context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.failFor[id] {
		return errors.New("queue full")
	}
	q.enqueued = append(q.enqueued, id)
	return nil
}

func TestTickEnqueuesEverySource(t *testing.T) {
	st := &fakeStore{scheduled: []string{"s1"}, due: []string{"d1"}, orphans: []string{"o1"}}
	q := &fakeQueue{}
	New(st, q, time.Second).tick(context.Background())

	want := []string{"s1", "d1", "o1"}
	if !reflect.DeepEqual(q.enqueued, want) {
		t.Fatalf("enqueued %v, want %v", q.enqueued, want)
	}
}

func TestTickFullQueueRequeuesRetryAndKeepsGoing(t *testing.T) {
	st := &fakeStore{due: []string{"d1"}, orphans: []string{"o1"}}
	q := &fakeQueue{failFor: map[string]bool{"d1": true}}
	New(st, q, time.Second).tick(context.Background())

	if !reflect.DeepEqual(st.requeued, []string{"d1"}) {
		t.Fatalf("requeued %v, want [d1]", st.requeued)
	}
	if !reflect.DeepEqual(q.enqueued, []string{"o1"}) {
		t.Fatalf("enqueued %v, want [o1]: a failed retry must not block orphans", q.enqueued)
	}
}

func TestTickReclaimErrorDoesNotStopTheTick(t *testing.T) {
	st := &fakeStore{reclaimErr: errors.New("db down"), scheduled: []string{"s1"}, orphans: []string{"o1"}}
	q := &fakeQueue{}
	New(st, q, time.Second).tick(context.Background())

	want := []string{"s1", "o1"}
	if !reflect.DeepEqual(q.enqueued, want) {
		t.Fatalf("enqueued %v, want %v", q.enqueued, want)
	}
}

// Pins current behaviour: an error from ClaimDue returns early, so the orphan
// sweep is skipped for that tick. Change both code and test if that is revised.
func TestTickClaimDueErrorSkipsOrphanSweep(t *testing.T) {
	st := &fakeStore{claimDueErr: errors.New("db down"), orphans: []string{"o1"}}
	q := &fakeQueue{}
	New(st, q, time.Second).tick(context.Background())

	if len(q.enqueued) != 0 {
		t.Fatalf("enqueued %v, want none", q.enqueued)
	}
}

func TestRunTicksAndStopsOnCancel(t *testing.T) {
	st := &fakeStore{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		New(st, &fakeQueue{}, 10*time.Millisecond).Run(ctx)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for st.calls() < 3 {
		select {
		case <-deadline:
			t.Fatalf("only %d ticks in 2s", st.calls())
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
