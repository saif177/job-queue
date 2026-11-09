package queue

import (
	"testing"
	"time"
)

func fakeClock(q *Queue) *time.Time {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	q.now = func() time.Time { return now }
	return &now
}

func TestEnqueueInDelaysDequeue(t *testing.T) {
	q := New()
	now := fakeClock(q)
	id, err := q.EnqueueIn(func() error { return nil }, PriorityNormal, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := q.Dequeue(); ok {
		t.Fatal("scheduled job dequeued early")
	}
	*now = now.Add(time.Minute)
	j, ok := q.Dequeue()
	if !ok || j.ID != id {
		t.Fatalf("expected job %d ready, got %v %v", id, j.ID, ok)
	}
}

func TestScheduledDoesNotBlockReadyJobs(t *testing.T) {
	q := New()
	now := fakeClock(q)
	q.EnqueueAt(func() error { return nil }, PriorityHigh, now.Add(time.Hour))
	low, _ := q.EnqueueWithPriority(func() error { return nil }, PriorityLow)
	j, ok := q.Dequeue()
	if !ok || j.ID != low {
		t.Fatalf("expected ready low job %d, got %+v %v", low, j, ok)
	}
}

func TestEnqueueAtPastIsImmediate(t *testing.T) {
	q := New()
	now := fakeClock(q)
	q.EnqueueAt(func() error { return nil }, PriorityNormal, now.Add(-time.Hour))
	if _, ok := q.Dequeue(); !ok {
		t.Fatal("past run-at job should be ready")
	}
	if _, err := q.EnqueueAt(nil, Priority(9), time.Time{}); err == nil {
		t.Fatal("expected invalid priority error")
	}
}

func TestScheduledJobPersistsAcrossRestart(t *testing.T) {
	path := t.TempDir() + "/jobs.db"
	s, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	q, _ := Open(s)
	runAt := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	id, err := q.EnqueueJobAt("x", nil, PriorityNormal, runAt)
	if err != nil {
		t.Fatal(err)
	}
	q.Close()

	s2, _ := OpenSQLite(path)
	q2, err := Open(s2)
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()
	j, ok := q2.Get(id)
	if !ok || !j.RunAt.Equal(runAt) {
		t.Fatalf("RunAt not recovered: %+v", j)
	}
	if _, ok := q2.Dequeue(); ok {
		t.Fatal("scheduled job ran before its time after restart")
	}
}

func TestHTTPSubmitScheduled(t *testing.T) {
	q := New()
	fakeClock(q)
	q.Register("t", func([]byte) error { return nil })
	h := q.HTTPHandler()

	rec := doReq(t, h, "POST", "/jobs", `{"type":"t","delay":"90s"}`)
	if rec.Code != 201 || !contains(rec.Body.String(), `"run_at":"2026-01-01T12:01:30Z"`) {
		t.Fatalf("delay: %d %s", rec.Code, rec.Body)
	}
	rec = doReq(t, h, "POST", "/jobs", `{"type":"t","run_at":"2030-05-01T00:00:00Z"}`)
	if rec.Code != 201 || !contains(rec.Body.String(), `"run_at":"2030-05-01T00:00:00Z"`) {
		t.Fatalf("run_at: %d %s", rec.Code, rec.Body)
	}
	for _, body := range []string{
		`{"type":"t","run_at":"nope"}`,
		`{"type":"t","delay":"-5s"}`,
		`{"type":"t","delay":"abc"}`,
		`{"type":"t","delay":"1s","run_at":"2030-05-01T00:00:00Z"}`,
	} {
		if rec := doReq(t, h, "POST", "/jobs", body); rec.Code != 400 {
			t.Errorf("%s: code %d", body, rec.Code)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
