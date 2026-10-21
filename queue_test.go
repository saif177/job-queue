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
