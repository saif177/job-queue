package queue

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestEnqueueDequeue(t *testing.T) {
	q := New()

	id := q.Enqueue(func() error { return nil })
	if id != 1 {
		t.Fatalf("expected first job ID to be 1, got %d", id)
	}

	if q.Len() != 1 {
		t.Fatalf("expected queue length 1, got %d", q.Len())
	}

	job, ok := q.Dequeue()
	if !ok {
		t.Fatal("expected a job to be dequeued")
	}
	if job.ID != id {
		t.Fatalf("expected dequeued job ID %d, got %d", id, job.ID)
	}

	if _, ok := q.Dequeue(); ok {
		t.Fatal("expected empty queue to return ok=false")
	}
}

func TestWorkerPoolProcessesJobs(t *testing.T) {
	q := New()

	var processed atomic.Int32
	for i := 0; i < 5; i++ {
		q.Enqueue(func() error {
			processed.Add(1)
			return nil
		})
	}

	pool := NewWorkerPool(q, 2, 5*time.Millisecond)
	pool.Start()
	defer pool.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for processed.Load() < 5 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if got := processed.Load(); got != 5 {
		t.Fatalf("expected 5 jobs processed, got %d", got)
	}
}

func TestWorkerPoolHandlesFailure(t *testing.T) {
	q := New()
	q.Enqueue(func() error { return errors.New("boom") })

	pool := NewWorkerPool(q, 1, 5*time.Millisecond)
	pool.Start()
	defer pool.Stop()

	deadline := time.Now().Add(1 * time.Second)
	for q.Len() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if q.Len() != 0 {
		t.Fatal("expected failing job to still be dequeued, not stuck in queue")
	}
}

func TestStatusLifecycle(t *testing.T) {
	q := New()
	okID := q.Enqueue(func() error { return nil })
	badID := q.Enqueue(func() error { return errors.New("boom") })

	if job, ok := q.Get(okID); !ok || job.Status != StatusPending {
		t.Fatalf("expected pending job, got %+v ok=%v", job, ok)
	}

	job, _ := q.Dequeue()
	if got, _ := q.Get(job.ID); got.Status != StatusRunning || got.StartedAt.IsZero() {
		t.Fatalf("expected running with start time, got %+v", got)
	}
	q.Complete(job.ID, job.Payload())
	if got, _ := q.Get(okID); got.Status != StatusSucceeded || got.FinishedAt.IsZero() {
		t.Fatalf("expected succeeded, got %+v", got)
	}

	job, _ = q.Dequeue()
	q.Complete(job.ID, job.Payload())
	got, _ := q.Get(badID)
	if got.Status != StatusFailed || got.Err == nil || got.Err.Error() != "boom" {
		t.Fatalf("expected failed with error, got %+v", got)
	}
}

func TestGetUnknownID(t *testing.T) {
	q := New()
	if _, ok := q.Get(42); ok {
		t.Fatal("expected unknown ID to return ok=false")
	}
	q.Complete(42, nil) // must not panic
}

func TestWorkerPoolUpdatesStatus(t *testing.T) {
	q := New()
	okID := q.Enqueue(func() error { return nil })
	badID := q.Enqueue(func() error { return errors.New("boom") })

	pool := NewWorkerPool(q, 2, 5*time.Millisecond)
	pool.Start()
	defer pool.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		a, _ := q.Get(okID)
		b, _ := q.Get(badID)
		if a.Status == StatusSucceeded && b.Status == StatusFailed {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	a, _ := q.Get(okID)
	b, _ := q.Get(badID)
	t.Fatalf("statuses not settled: ok=%s bad=%s", a.Status, b.Status)
}

func TestBackoffGrowsExponentiallyAndCaps(t *testing.T) {
	q := New(WithRetry(10, 100*time.Millisecond, 500*time.Millisecond))
	want := []time.Duration{100, 200, 400, 500, 500}
	for i, w := range want {
		if got := q.backoff(i + 1); got != w*time.Millisecond {
			t.Fatalf("attempt %d: expected %v, got %v", i+1, w*time.Millisecond, got)
		}
	}
}

func TestRetryThenSucceed(t *testing.T) {
	q := New(WithRetry(2, time.Second, 0))
	clock := time.Now()
	q.now = func() time.Time { return clock }

	id := q.Enqueue(func() error { return nil })
	boom := errors.New("boom")

	job, _ := q.Dequeue()
	q.Complete(job.ID, boom)

	got, _ := q.Get(id)
	if got.Status != StatusPending || got.Attempts != 1 || !errors.Is(got.Err, boom) {
		t.Fatalf("expected pending after 1 attempt with error, got %+v", got)
	}
	if _, ok := q.Dequeue(); ok {
		t.Fatal("job should not be ready during backoff")
	}

	clock = clock.Add(time.Second)
	job, ok := q.Dequeue()
	if !ok || job.Attempts != 2 {
		t.Fatalf("expected retry as attempt 2, got ok=%v job=%+v", ok, job)
	}
	q.Complete(job.ID, nil)

	got, _ = q.Get(id)
	if got.Status != StatusSucceeded || got.Err != nil {
		t.Fatalf("expected succeeded with cleared error, got %+v", got)
	}
}

func TestRetriesExhaustedMarksFailed(t *testing.T) {
	q := New(WithRetry(1, time.Second, 0))
	clock := time.Now()
	q.now = func() time.Time { return clock }

	id := q.Enqueue(func() error { return nil })
	boom := errors.New("boom")

	for attempt := 1; attempt <= 2; attempt++ {
		job, ok := q.Dequeue()
		if !ok {
			t.Fatalf("attempt %d: expected job", attempt)
		}
		q.Complete(job.ID, boom)
		clock = clock.Add(10 * time.Second)
	}

	got, _ := q.Get(id)
	if got.Status != StatusFailed || got.Attempts != 2 || !errors.Is(got.Err, boom) {
		t.Fatalf("expected failed after 2 attempts, got %+v", got)
	}
	if _, ok := q.Dequeue(); ok {
		t.Fatal("exhausted job must not be re-queued")
	}
}

func TestBackoffDoesNotBlockOtherJobs(t *testing.T) {
	q := New(WithRetry(1, time.Hour, 0))
	first := q.Enqueue(func() error { return nil })
	second := q.Enqueue(func() error { return nil })

	job, _ := q.Dequeue()
	q.Complete(job.ID, errors.New("boom"))

	next, ok := q.Dequeue()
	if !ok || next.ID != second || first == second {
		t.Fatalf("expected second job to run while first backs off, got %+v ok=%v", next, ok)
	}
}
