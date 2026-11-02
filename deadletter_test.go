package queue

import (
	"path/filepath"
	"testing"
)

func exhaust(t *testing.T, q *Queue, id int) {
	t.Helper()
	for i := 0; i < 2; i++ { // maxRetries=1 -> 2 runs
		j, ok := q.Dequeue()
		if !ok || j.ID != id {
			t.Fatalf("run %d: dequeue got %d ok=%v", i, j.ID, ok)
		}
		q.Complete(id, errTest)
	}
}

func TestExhaustedJobsGoToDeadLetter(t *testing.T) {
	q := New(WithRetry(1, 0, 0), WithDeadLetter())
	id := q.Enqueue(func() error { return errTest })
	exhaust(t, q, id)
	ok1 := q.Enqueue(noop)

	j, _ := q.Get(id)
	if j.Status != StatusDead || j.Err != errTest {
		t.Fatalf("status=%s err=%v", j.Status, j.Err)
	}
	dl := q.DeadLetters()
	if len(dl) != 1 || dl[0].ID != id {
		t.Fatalf("dead letters = %+v", dl)
	}
	if g, _ := q.Get(ok1); g.Status != StatusPending {
		t.Fatal("other job affected")
	}
}

func TestWithoutDeadLetterStaysFailed(t *testing.T) {
	q := New(WithRetry(1, 0, 0))
	id := q.Enqueue(noop)
	exhaust(t, q, id)
	if j, _ := q.Get(id); j.Status != StatusFailed || len(q.DeadLetters()) != 0 {
		t.Fatalf("status=%s", j.Status)
	}
}

func TestRequeueDeadJob(t *testing.T) {
	q := New(WithRetry(1, 0, 0), WithDeadLetter())
	id := q.Enqueue(noop)
	exhaust(t, q, id)

	if err := q.Requeue(id); err != nil {
		t.Fatal(err)
	}
	j, _ := q.Get(id)
	if j.Status != StatusPending || j.Attempts != 0 || j.Err != nil {
		t.Fatalf("after requeue: %+v", j)
	}
	if got, ok := q.Dequeue(); !ok || got.ID != id {
		t.Fatal("requeued job not dequeued")
	}
	if err := q.Requeue(id); err == nil {
		t.Fatal("requeue of running job should fail")
	}
	if err := q.Requeue(999); err == nil {
		t.Fatal("requeue of unknown job should fail")
	}
}

func TestDiscardDeadJob(t *testing.T) {
	q := New(WithRetry(0, 0, 0), WithDeadLetter())
	id := q.Enqueue(noop)
	q.Dequeue()
	q.Complete(id, errTest)
	if err := q.Discard(id); err != nil {
		t.Fatal(err)
	}
	if _, ok := q.Get(id); ok || len(q.DeadLetters()) != 0 {
		t.Fatal("job should be gone")
	}
	if err := q.Discard(id); err == nil {
		t.Fatal("second discard should fail")
	}
}

func TestDeadLetterPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	store, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	q, _ := Open(store, WithRetry(0, 0, 0), WithDeadLetter())
	q.Register("bad", func([]byte) error { return errTest })
	id, _ := q.EnqueueJob("bad", nil)
	q.Dequeue()
	q.Complete(id, errTest)
	q.Close()

	store, _ = OpenSQLite(path)
	q, _ = Open(store, WithDeadLetter())
	defer q.Close()
	if q.Len() != 0 {
		t.Fatal("dead job must not be re-queued on restart")
	}
	dl := q.DeadLetters()
	if len(dl) != 1 || dl[0].ID != id {
		t.Fatalf("dead letters = %+v", dl)
	}
	if err := q.Requeue(id); err != nil {
		t.Fatal(err)
	}
	if q.Len() != 1 {
		t.Fatal("requeued job should be pending")
	}
}
