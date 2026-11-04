package queue

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestShutdownDrainsInFlightJob(t *testing.T) {
	q := New()
	started := make(chan struct{})
	release := make(chan struct{})
	id := q.Enqueue(func() error {
		close(started)
		<-release
		return nil
	})
	pool := NewWorkerPool(q, 1, time.Millisecond)
	pool.Start()
	<-started

	done := make(chan error, 1)
	go func() { done <- pool.Shutdown(context.Background()) }()
	select {
	case <-done:
		t.Fatal("Shutdown returned while a job was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if j, _ := q.Get(id); j.Status != StatusSucceeded {
		t.Fatalf("status = %s, want succeeded", j.Status)
	}
}

func TestShutdownStopsPickingUpNewJobs(t *testing.T) {
	q := New()
	pool := NewWorkerPool(q, 2, time.Millisecond)
	pool.Start()
	if err := pool.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	id := q.Enqueue(func() error { return nil })
	time.Sleep(30 * time.Millisecond)
	if j, _ := q.Get(id); j.Status != StatusPending {
		t.Fatalf("job ran after shutdown: %s", j.Status)
	}
}

func TestShutdownTimeout(t *testing.T) {
	q := New()
	started := make(chan struct{})
	release := make(chan struct{})
	q.Enqueue(func() error { close(started); <-release; return nil })
	pool := NewWorkerPool(q, 1, time.Millisecond)
	pool.Start()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := pool.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	close(release)
	if err := pool.Shutdown(context.Background()); err != nil { // second call is safe
		t.Fatal(err)
	}
}

func TestStopIsIdempotent(t *testing.T) {
	pool := NewWorkerPool(New(), 2, time.Millisecond)
	pool.Start()
	pool.Stop()
	pool.Stop()
}

func TestRunUntilSignal(t *testing.T) {
	q := New()
	started := make(chan struct{})
	id := q.Enqueue(func() error {
		close(started)
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	pool := NewWorkerPool(q, 1, time.Millisecond)
	sig := make(chan os.Signal, 1)
	errc := make(chan error, 1)
	go func() { errc <- pool.runUntil(sig, time.Second) }()
	<-started
	sig <- syscall.SIGTERM
	if err := <-errc; err != nil {
		t.Fatalf("runUntil: %v", err)
	}
	if j, _ := q.Get(id); j.Status != StatusSucceeded {
		t.Fatalf("in-flight job status = %s, want succeeded", j.Status)
	}
}
