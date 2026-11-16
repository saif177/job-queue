package queue

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrencyLimitBlocksUntilComplete(t *testing.T) {
	q := New(WithConcurrencyLimit("report", 2))
	for i := 0; i < 3; i++ {
		q.EnqueueJob("report", nil)
	}
	a, ok1 := q.Dequeue()
	_, ok2 := q.Dequeue()
	if !ok1 || !ok2 {
		t.Fatal("first two dequeues should succeed")
	}
	if _, ok := q.Dequeue(); ok {
		t.Fatal("third dequeue should hit the concurrency limit")
	}
	if q.Len() != 1 || q.Running("report") != 2 {
		t.Fatalf("Len=%d Running=%d", q.Len(), q.Running("report"))
	}
	q.Complete(a.ID, nil)
	if _, ok := q.Dequeue(); !ok {
		t.Fatal("slot should be free after Complete")
	}
}

func TestConcurrencyLimitDoesNotBlockOtherTypes(t *testing.T) {
	q := New(WithConcurrencyLimit("slow", 1))
	q.EnqueueJob("slow", nil)
	q.EnqueueJob("slow", nil)
	q.EnqueueJob("fast", nil)
	q.Dequeue()
	j, ok := q.Dequeue()
	if !ok || j.Type != "fast" {
		t.Fatalf("expected the fast job to run, got %+v ok=%v", j, ok)
	}
}

func TestConcurrencyLimitReleasedOnFailure(t *testing.T) {
	q := New(WithConcurrencyLimit("x", 1))
	q.EnqueueJob("x", nil)
	j, _ := q.Dequeue()
	q.Complete(j.ID, errors.New("boom"))
	if q.Running("x") != 0 {
		t.Fatalf("Running = %d after failure", q.Running("x"))
	}
	q.Complete(j.ID, errors.New("boom")) // double complete must not underflow
	if q.Running("x") != 0 {
		t.Fatal("running count changed on double complete")
	}
}

func TestConcurrencyLimitInvalidIgnored(t *testing.T) {
	q := New(WithConcurrencyLimit("x", 0), WithConcurrencyLimit("", 3))
	if len(q.concLimits) != 0 {
		t.Fatal("invalid limits should be ignored")
	}
}

func TestConcurrencyLimitWithWorkerPool(t *testing.T) {
	q := New(WithConcurrencyLimit("job", 2))
	var cur, peak, done int32
	q.Register("job", func([]byte) error {
		n := atomic.AddInt32(&cur, 1)
		for {
			p := atomic.LoadInt32(&peak)
			if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt32(&cur, -1)
		atomic.AddInt32(&done, 1)
		return nil
	})
	for i := 0; i < 8; i++ {
		q.EnqueueJob("job", nil)
	}
	pool := NewWorkerPool(q, 6, 2*time.Millisecond)
	pool.Start()
	deadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt32(&done) < 8 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	pool.Stop()
	if atomic.LoadInt32(&done) != 8 {
		t.Fatalf("done = %d", done)
	}
	if p := atomic.LoadInt32(&peak); p > 2 {
		t.Fatalf("peak concurrency %d exceeded limit 2", p)
	}
}
