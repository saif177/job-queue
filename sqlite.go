package queue

import (
	"database/sql"
	"errors"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver
)

// SQLiteStore is a Store backed by a SQLite database file.
type SQLiteStore struct {
	db *sql.DB
}

const schema = `CREATE TABLE IF NOT EXISTS jobs (
	id          INTEGER PRIMARY KEY,
	type        TEXT NOT NULL,
	data        BLOB,
	status      TEXT NOT NULL,
	err         TEXT NOT NULL DEFAULT '',
	enqueued_at INTEGER NOT NULL,
	started_at  INTEGER NOT NULL,
	finished_at INTEGER NOT NULL,
	attempts    INTEGER NOT NULL,
	run_at      INTEGER NOT NULL
)`

// OpenSQLite opens (creating if needed) the SQLite database at path.
// Use ":memory:" for a throwaway database.
func OpenSQLite(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// A single connection serializes writes and keeps ":memory:" coherent.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	return &SQLiteStore{db: db}, nil
}

// zero time is stored as 0 so it round-trips as time.Time{}.
func toNanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func fromNanos(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

func (s *SQLiteStore) Save(j Job) error {
	errText := ""
	if j.Err != nil {
		errText = j.Err.Error()
	}
	_, err := s.db.Exec(`INSERT INTO jobs
		(id, type, data, status, err, enqueued_at, started_at, finished_at, attempts, run_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			status=excluded.status, err=excluded.err, started_at=excluded.started_at,
			finished_at=excluded.finished_at, attempts=excluded.attempts, run_at=excluded.run_at`,
		j.ID, j.Type, j.Data, string(j.Status), errText, toNanos(j.EnqueuedAt),
		toNanos(j.StartedAt), toNanos(j.FinishedAt), j.Attempts, toNanos(j.RunAt))
	return err
}

func (s *SQLiteStore) Load() ([]Job, error) {
	rows, err := s.db.Query(`SELECT id, type, data, status, err, enqueued_at,
		started_at, finished_at, attempts, run_at FROM jobs ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var (
			j                           Job
			status, errText             string
			enq, started, finished, run int64
		)
		if err := rows.Scan(&j.ID, &j.Type, &j.Data, &status, &errText, &enq, &started, &finished, &j.Attempts, &run); err != nil {
			return nil, err
		}
		j.Status = Status(status)
		if errText != "" {
			j.Err = errors.New(errText)
		}
		j.EnqueuedAt, j.StartedAt, j.FinishedAt, j.RunAt = fromNanos(enq), fromNanos(started), fromNanos(finished), fromNanos(run)
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func (s *SQLiteStore) Close() error { return s.db.Close() }
