package queue

import (
	"database/sql"
	"errors"

	_ "github.com/jackc/pgx/v5/stdlib" // Postgres driver (database/sql name "pgx")
)

// PostgresStore is a Store backed by a PostgreSQL database. It is a drop-in
// replacement for SQLiteStore: pass it to Open or WithStore. Unlike SQLite it
// uses a normal connection pool, so several processes can share one database
// (note: the queue itself still keeps its working set in memory per process).
type PostgresStore struct {
	db *sql.DB
}

const pgSchema = `CREATE TABLE IF NOT EXISTS jobs (
	id          BIGINT PRIMARY KEY,
	type        TEXT NOT NULL,
	data        BYTEA,
	status      TEXT NOT NULL,
	err         TEXT NOT NULL DEFAULT '',
	enqueued_at BIGINT NOT NULL,
	started_at  BIGINT NOT NULL,
	finished_at BIGINT NOT NULL,
	attempts    INTEGER NOT NULL,
	run_at      BIGINT NOT NULL,
	priority    INTEGER NOT NULL DEFAULT 0
)`

// OpenPostgres connects to the database identified by dsn (a URL such as
// "postgres://user:pass@host:5432/db" or a key=value string) and creates the
// jobs table if needed. Timestamps are stored as Unix nanoseconds, matching
// the SQLite store.
func OpenPostgres(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(pgSchema); err != nil {
		db.Close()
		return nil, err
	}
	return &PostgresStore{db: db}, nil
}

func (s *PostgresStore) Save(j Job) error {
	errText := ""
	if j.Err != nil {
		errText = j.Err.Error()
	}
	_, err := s.db.Exec(`INSERT INTO jobs
		(id, type, data, status, err, enqueued_at, started_at, finished_at, attempts, run_at, priority)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (id) DO UPDATE SET
			status=EXCLUDED.status, err=EXCLUDED.err, started_at=EXCLUDED.started_at,
			finished_at=EXCLUDED.finished_at, attempts=EXCLUDED.attempts, run_at=EXCLUDED.run_at,
			priority=EXCLUDED.priority`,
		j.ID, j.Type, j.Data, string(j.Status), errText, toNanos(j.EnqueuedAt),
		toNanos(j.StartedAt), toNanos(j.FinishedAt), j.Attempts, toNanos(j.RunAt), int(j.Priority))
	return err
}

func (s *PostgresStore) Load() ([]Job, error) {
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

func (s *PostgresStore) Close() error { return s.db.Close() }
