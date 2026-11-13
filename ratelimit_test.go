package queue

import (
	"testing"
	"time"
)

func newClockQueue(opts ...Option) (*Queue, *time.Time) {
	q := New(opts...)
	t := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	q.now = func() time.Time { return t }
	return q, &t
}

func TestRateLimitBurstThenRefill(t *testing.T) {
	q, clock := newClockQueue(WithRateLimit("email", 1, 2))
	for i := 0; i < 4; i++ {
		q.EnqueueJob("email", nil)
	}
	for i := 0; i < 2; i++ {
		if _, ok := q.Dequeue(); !ok {
			t.Fatalf("burst dequeue %d should succeed", i)
		}
	}
	if _, ok := q.Dequeue(); ok {
		t.Fatal("third dequeue should be rate limited")
	}
	if q.Len() != 2 {
		t.Fatalf("limited jobs must stay pending, Len=%d", q.Len())
	}
	*clock = clock.Add(500 * time.Millisecond)
	if _, ok := q.Dequeue(); ok {
		t.Fatal("half a token is not enough")
	}
	*clock = clock.Add(500 * time.Millisecond)
	if _, ok := q.Dequeue(); !ok {
		t.Fatal("one token should have refilled after 1s")
	}
	if _, ok := q.Dequeue(); ok {
		t.Fatal("only one token was refilled")
	}
}

func TestRateLimitDoesNotBlockOtherTypes(t *testing.T) {
	q, _ := newClockQueue(WithRateLimit("slow", 1, 1))
	q.EnqueueJobWithPriority("slow", nil, PriorityHigh)
	q.EnqueueJobWithPriority("slow", nil, PriorityHigh)
	other, _ := q.EnqueueJob("other", nil)
	fn := q.Enqueue(func() error { return nil })

	if j, ok := q.Dequeue(); !ok || j.Type != "slow" {
		t.Fatalf("expected first slow job, got %+v ok=%v", j, ok)
	}
	// second slow job is limited; lower-priority jobs still run.
	if j, ok := q.Dequeue(); !ok || j.ID != other {
		t.Fatalf("expected other job %d, got %+v", other, j)
	}
	if j, ok := q.Dequeue(); !ok || j.ID != fn {
		t.Fatalf("expected func job %d, got %+v", fn, j)
	}
	if _, ok := q.Dequeue(); ok {
		t.Fatal("remaining slow job should be limited")
	}
}

func TestRateLimitIgnoresInvalidConfig(t *testing.T) {
	q, _ := newClockQueue(WithRateLimit("a", 0, 1), WithRateLimit("b", 1, 0), WithRateLimit("", 1, 1))
	if len(q.limits) != 0 {
		t.Fatalf("invalid limits should be ignored, got %d", len(q.limits))
	}
}

func TestRateLimitRetryDoesNotBypass(t *testing.T) {
	q, clock := newClockQueue(WithRateLimit("t", 1, 1), WithRetry(2, 0, 0))
	id, _ := q.EnqueueJob("t", nil)
	j, ok := q.Dequeue()
	if !ok || j.ID != id {
		t.Fatal("first run should dequeue")
	}
	q.Complete(id, errTest)
	if _, ok := q.Dequeue(); ok {
		t.Fatal("retry must wait for a token")
	}
	*clock = clock.Add(time.Second)
	if _, ok := q.Dequeue(); !ok {
		t.Fatal("retry should run once a token is available")
	}
}
