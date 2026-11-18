package queue

import (
	"errors"
	"os"
	"testing"
	"time"
)

// storeFactories returns every Store implementation to run the shared
// contract tests against. Postgres is included only when
// JOBQUEUE_POSTGRES_DSN is set.
func storeFactories(t *testing.T) map[string]func(t *testing.T) Store {
	f := map[string]func(t *testing.T) Store{
		"sqlite": func(t *testing.T) Store {
			s, err := OpenSQLite(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			return s
		},
	}
	if dsn := os.Getenv("JOBQUEUE_POSTGRES_DSN"); dsn != "" {
		f["postgres"] = func(t *testing.T) Store {
			s, err := OpenPostgres(dsn)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(`TRUNCATE jobs`); err != nil {
				t.Fatal(err)
			}
			return s
		}
	}
	return f
}

func TestStoreContract(t *testing.T) {
	for name, mk := range storeFactories(t) {
		t.Run(name, func(t *testing.T) {
			s := mk(t)
			defer s.Close()

			now := time.Now()
			j := Job{ID: 1, Type: "email", Data: []byte("a@example.com"), Status: StatusPending,
				EnqueuedAt: now, RunAt: now.Add(time.Minute), Priority: PriorityHigh}
			if err := s.Save(j); err != nil {
				t.Fatal(err)
			}
			// Upsert: update the same ID.
			j.Status = StatusFailed
			j.Err = errors.New("boom")
			j.Attempts = 2
			j.FinishedAt = now.Add(time.Second)
			if err := s.Save(j); err != nil {
				t.Fatal(err)
			}
			if err := s.Save(Job{ID: 2, Type: "x", Status: StatusPending, EnqueuedAt: now}); err != nil {
				t.Fatal(err)
			}

			got, err := s.Load()
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 2 || got[0].ID != 1 || got[1].ID != 2 {
				t.Fatalf("unexpected jobs: %+v", got)
			}
			g := got[0]
			if g.Status != StatusFailed || g.Err == nil || g.Err.Error() != "boom" ||
				g.Attempts != 2 || g.Priority != PriorityHigh || string(g.Data) != "a@example.com" ||
				g.Type != "email" {
				t.Fatalf("round trip mismatch: %+v", g)
			}
			if !g.EnqueuedAt.Equal(now) || !g.RunAt.Equal(now.Add(time.Minute)) || !g.FinishedAt.Equal(now.Add(time.Second)) {
				t.Fatalf("time mismatch: %+v", g)
			}
			if !got[1].StartedAt.IsZero() || got[1].Err != nil {
				t.Fatalf("zero values not preserved: %+v", got[1])
			}
		})
	}
}

// TestStoreQueuePersists checks that a queue writes typed jobs through to each store.
func TestStoreQueuePersists(t *testing.T) {
	for name, mk := range storeFactories(t) {
		t.Run(name, func(t *testing.T) {
			s := mk(t)
			q, err := Open(s)
			if err != nil {
				t.Fatal(err)
			}
			q.Register("noop", func([]byte) error { return nil })
			id, err := q.EnqueueJob("noop", []byte("hi"))
			if err != nil {
				t.Fatal(err)
			}
			jobs, err := s.Load()
			if err != nil || len(jobs) != 1 || jobs[0].ID != id {
				t.Fatalf("persisted jobs = %+v, err %v", jobs, err)
			}
		})
	}
}

func TestOpenPostgresBadDSN(t *testing.T) {
	if _, err := OpenPostgres("postgres://nobody@127.0.0.1:1/none?connect_timeout=1"); err == nil {
		t.Fatal("expected error connecting to unreachable server")
	}
}
