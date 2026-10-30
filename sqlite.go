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
	run_at      INTEGER NOT NULL,
	priority    INTEGER NOT NULL DEFAULT 0
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
	if err := migratePriority(db); err != nil {
		db.Close()
		return nil, err
	}
	return &SQLiteStore{db: db}, nil
}

// migratePriority adds the priority column to databases created before
// priorities existed.
func migratePriority(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(jobs)`)
	if err != nil {
		return err
	}
	has := false
	for rows.Next() {
		var (
			cid, notnull, pk int
			name, typ        string
			dflt             sql.NullString
		)
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			rows.Close()
			return err
		}
		if name == "priority" {
			has = true
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if has {
		return nil
	}
	_, err = db.Exec(`ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0`)
	return err
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
		(id, type, data, status, err, enqueued_at, started_at, finished_at, attempts, run_at, priority)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			status=excluded.status, err=excluded.err, started_at=excluded.started_at,
			finished_at=excluded.finished_at, attempts=excluded.attempts, run_at=excluded.run_at,
			priority=excluded.priority`,
		j.ID, j.Type, j.Data, string(j.Status), errText, toNanos(j.EnqueuedAt),
		toNanos(j.StartedAt), toNanos(j.FinishedAt), j.Attempts, toNanos(j.RunAt), int(j.Priority))
	return err
}

func (s *SQLiteStore) Load() ([]Job, error) {
	rows, err := s.db.Query(`SELECT id, type, data, status, err, enqueued_at,
		started_at, finished_at, attempts, run_at, priority FROM jobs ORDER BY id`)
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
			prio                        int
		)
		if err := rows.Scan(&j.ID, &j.Type, &j.Data, &status, &errText, &enq, &started, &finished, &j.Attempts, &run, &prio); err != nil {
			return nil, err
		}
		j.Priority = Priority(prio)
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
