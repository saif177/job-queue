package queue

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMiddlewareOrder(t *testing.T) {
	var trace []string
	mk := func(name string) Middleware {
		return func(job Job, next func() error) error {
			trace = append(trace, name+":before")
			err := next()
			trace = append(trace, name+":after")
			return err
		}
	}
	q := New(WithMiddleware(mk("a"), mk("b")))
	id := q.Enqueue(func() error { trace = append(trace, "job"); return nil })
	job, _ := q.Dequeue()
	if job.ID != id {
		t.Fatal("wrong job")
	}
	if err := q.Execute(job); err != nil {
		t.Fatal(err)
	}
	got := strings.Join(trace, ",")
	want := "a:before,b:before,job,b:after,a:after"
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestMiddlewareShortCircuit(t *testing.T) {
	boom := errors.New("blocked")
	ran := false
	q := New(WithMiddleware(func(Job, func() error) error { return boom }))
	q.Enqueue(func() error { ran = true; return nil })
	job, _ := q.Dequeue()
	if err := q.Execute(job); err != boom || ran {
		t.Fatalf("err=%v ran=%v", err, ran)
	}
}

func TestRecover(t *testing.T) {
	q := New(WithMiddleware(Recover()))
	q.Enqueue(func() error { panic("oops") })
	job, _ := q.Dequeue()
	err := q.Execute(job)
	if err == nil || !strings.Contains(err.Error(), "oops") {
		t.Fatalf("err=%v", err)
	}
}

func TestTimeout(t *testing.T) {
	q := New(WithMiddleware(Timeout(20 * time.Millisecond)))
	release := make(chan struct{})
	defer close(release)
	q.Enqueue(func() error { <-release; return nil })
	q.Enqueue(func() error { return nil })
	slow, _ := q.Dequeue()
	if err := q.Execute(slow); !errors.Is(err, ErrTimeout) {
		t.Fatalf("err=%v", err)
	}
	fast, _ := q.Dequeue()
	if err := q.Execute(fast); err != nil {
		t.Fatal(err)
	}
}

func TestTimeoutPanicAndDisabled(t *testing.T) {
	q := New(WithMiddleware(Timeout(time.Second)))
	q.Enqueue(func() error { panic("x") })
	job, _ := q.Dequeue()
	if err := q.Execute(job); err == nil {
		t.Fatal("expected panic error")
	}
	q2 := New(WithMiddleware(Timeout(0)))
	q2.Enqueue(func() error { return nil })
	job, _ = q2.Dequeue()
	if err := q2.Execute(job); err != nil {
		t.Fatal(err)
	}
}

func TestLogging(t *testing.T) {
	var mu sync.Mutex
	var lines []string
	logf := func(f string, a ...any) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, f)
	}
	q := New(WithMiddleware(Logging(logf)))
	q.Enqueue(func() error { return errors.New("bad") })
	job, _ := q.Dequeue()
	_ = q.Execute(job)
	if len(lines) != 1 || !strings.Contains(lines[0], "failed") {
		t.Fatalf("lines=%v", lines)
	}
}

func TestMetricsWithWorkerPool(t *testing.T) {
	m := NewMetrics()
	q := New(WithMiddleware(Recover(), m.Middleware()))
	q.Register("ok", func([]byte) error { return nil })
	q.Register("bad", func([]byte) error { return errors.New("no") })
	for i := 0; i < 3; i++ {
		q.EnqueueJob("ok", nil)
	}
	q.EnqueueJob("bad", nil)
	pool := NewWorkerPool(q, 2, time.Millisecond)
	pool.Start()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := m.Snapshot()
		if s["ok"].Runs == 3 && s["bad"].Runs == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	pool.Stop()
	s := m.Snapshot()
	if s["ok"].Runs != 3 || s["ok"].Failures != 0 || s["bad"].Runs != 1 || s["bad"].Failures != 1 {
		t.Fatalf("stats=%+v", s)
	}
}
