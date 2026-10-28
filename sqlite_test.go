package queue

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func openTemp(t *testing.T, path string, opts ...Option) *Queue {
	t.Helper()
	s, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	q, err := Open(s, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")

	q := openTemp(t, path)
	id1, err := q.EnqueueJob("email", []byte("a@example.com"))
	if err != nil {
		t.Fatal(err)
	}
	id2, _ := q.EnqueueJob("email", []byte("b@example.com"))
	id3, _ := q.EnqueueJob("email", []byte("c@example.com"))

	var sent []string
	q.Register("email", func(d []byte) error { sent = append(sent, string(d)); return nil })

	// id1 succeeds; id2 is left running (simulated crash); id3 stays pending.
	j, _ := q.Dequeue()
	if err := j.Payload(); err != nil {
		t.Fatal(err)
	}
	q.Complete(j.ID, nil)
	if j, _ := q.Dequeue(); j.ID != id2 {
		t.Fatalf("expected job %d running, got %d", id2, j.ID)
	}
	q.Close()

	q = openTemp(t, path)
	defer q.Close()
	if got, _ := q.Get(id1); got.Status != StatusSucceeded {
		t.Fatalf("job 1 status = %s, want succeeded", got.Status)
	}
	if got, _ := q.Get(id2); got.Status != StatusPending || got.Attempts != 1 {
		t.Fatalf("job 2 = %s/%d attempts, want pending/1", got.Status, got.Attempts)
	}
	if q.Len() != 2 {
		t.Fatalf("Len = %d, want 2", q.Len())
	}

	q.Register("email", func(d []byte) error { sent = append(sent, string(d)); return nil })
	for _, want := range []int{id2, id3} {
		j, ok := q.Dequeue()
		if !ok || j.ID != want {
			t.Fatalf("dequeued %v (%v), want %d", j.ID, ok, want)
		}
		if err := j.Payload(); err != nil {
			t.Fatal(err)
		}
		q.Complete(j.ID, nil)
	}
	if got := sent[len(sent)-1]; got != "c@example.com" {
		t.Fatalf("last sent = %q", got)
	}

	// IDs continue after the recovered maximum.
	if id, _ := q.EnqueueJob("email", nil); id != id3+1 {
		t.Fatalf("next ID = %d, want %d", id, id3+1)
	}
}

func TestPersistedRetryAndFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	q := openTemp(t, path, WithRetry(1, time.Millisecond, 0))
	q.Register("boom", func([]byte) error { return errors.New("kaboom") })
	id, _ := q.EnqueueJob("boom", nil)

	for i := 0; i < 2; i++ {
		var j Job
		for ok := false; !ok; {
			j, ok = q.Dequeue()
			time.Sleep(2 * time.Millisecond)
		}
		q.Complete(j.ID, j.Payload())
	}
	q.Close()

	q = openTemp(t, path)
	defer q.Close()
	got, ok := q.Get(id)
	if !ok || got.Status != StatusFailed || got.Attempts != 2 || got.Err == nil || got.Err.Error() != "kaboom" {
		t.Fatalf("recovered job = %+v", got)
	}
	if q.Len() != 0 {
		t.Fatalf("Len = %d, want 0", q.Len())
	}
}

func TestUnregisteredTypeFails(t *testing.T) {
	q := openTemp(t, ":memory:")
	defer q.Close()
	q.EnqueueJob("nope", nil)
	j, _ := q.Dequeue()
	if err := j.Payload(); err == nil {
		t.Fatal("expected error for unregistered handler")
	}
}

func TestFuncJobsNotPersisted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	q := openTemp(t, path)
	q.Enqueue(func() error { return nil })
	q.Close()

	q = openTemp(t, path)
	defer q.Close()
	if q.Len() != 0 {
		t.Fatalf("func jobs should not survive restart, Len = %d", q.Len())
	}
}

func TestEnqueueJobRequiresType(t *testing.T) {
	q := New()
	if _, err := q.EnqueueJob("", nil); err == nil {
		t.Fatal("expected error for empty type")
	}
}
