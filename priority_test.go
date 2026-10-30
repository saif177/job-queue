package queue

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func noop() error { return nil }

var errTest = errors.New("boom")

func TestDequeueOrdersByPriorityThenFIFO(t *testing.T) {
	q := New()
	low, _ := q.EnqueueWithPriority(noop, PriorityLow)
	n1 := q.Enqueue(noop)
	high1, _ := q.EnqueueWithPriority(noop, PriorityHigh)
	n2 := q.Enqueue(noop)
	high2, _ := q.EnqueueWithPriority(noop, PriorityHigh)

	want := []int{high1, high2, n1, n2, low}
	for i, id := range want {
		j, ok := q.Dequeue()
		if !ok || j.ID != id {
			t.Fatalf("dequeue %d: got id %d ok=%v, want %d", i, j.ID, ok, id)
		}
	}
	if _, ok := q.Dequeue(); ok {
		t.Fatal("queue should be empty")
	}
}

func TestInvalidPriorityRejected(t *testing.T) {
	q := New()
	if _, err := q.EnqueueWithPriority(noop, Priority(5)); err == nil {
		t.Fatal("expected error for invalid priority")
	}
	if _, err := q.EnqueueJobWithPriority("x", nil, Priority(-3)); err == nil {
		t.Fatal("expected error for invalid priority")
	}
	if q.Len() != 0 {
		t.Fatal("rejected jobs must not be queued")
	}
}

func TestBackingOffHighPriorityDoesNotBlockLower(t *testing.T) {
	now := time.Now()
	q := New(WithRetry(1, time.Minute, 0))
	q.now = func() time.Time { return now }

	hi, _ := q.EnqueueWithPriority(noop, PriorityHigh)
	lo, _ := q.EnqueueWithPriority(noop, PriorityLow)

	j, _ := q.Dequeue()
	if j.ID != hi {
		t.Fatalf("got %d, want high job %d", j.ID, hi)
	}
	q.Complete(hi, errTest) // backs off for a minute

	j, ok := q.Dequeue()
	if !ok || j.ID != lo {
		t.Fatalf("got %d ok=%v, want low job %d while high backs off", j.ID, ok, lo)
	}

	now = now.Add(2 * time.Minute)
	q.Complete(lo, nil)
	j, ok = q.Dequeue()
	if !ok || j.ID != hi {
		t.Fatalf("got %d ok=%v, want retried high job %d", j.ID, ok, hi)
	}
}

func TestPriorityPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	q := openTemp(t, path)
	q.EnqueueJobWithPriority("t", nil, PriorityLow)
	hi, _ := q.EnqueueJobWithPriority("t", nil, PriorityHigh)
	q.Close()

	q = openTemp(t, path)
	defer q.Close()
	j, ok := q.Dequeue()
	if !ok || j.ID != hi || j.Priority != PriorityHigh {
		t.Fatalf("got %+v, want high-priority job %d first", j, hi)
	}
}

func TestMigratesDatabaseWithoutPriorityColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE jobs (
		id INTEGER PRIMARY KEY, type TEXT NOT NULL, data BLOB, status TEXT NOT NULL,
		err TEXT NOT NULL DEFAULT '', enqueued_at INTEGER NOT NULL, started_at INTEGER NOT NULL,
		finished_at INTEGER NOT NULL, attempts INTEGER NOT NULL, run_at INTEGER NOT NULL);
		INSERT INTO jobs VALUES (1, 'old', NULL, 'pending', '', 1, 0, 0, 0, 0)`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	q := openTemp(t, path)
	defer q.Close()
	j, ok := q.Get(1)
	if !ok || j.Priority != PriorityNormal || j.Status != StatusPending {
		t.Fatalf("migrated job = %+v ok=%v", j, ok)
	}
}

func TestPriorityString(t *testing.T) {
	if PriorityHigh.String() != "high" || PriorityLow.String() != "low" || PriorityNormal.String() != "normal" {
		t.Fatal("unexpected Priority strings")
	}
}
