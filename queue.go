package queue

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

// Status is the lifecycle state of a job.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

// Priority orders pending jobs: higher priorities are dequeued first.
// The zero value is PriorityNormal, so jobs enqueued without a priority
// behave as before.
type Priority int

const (
	PriorityLow    Priority = -1
	PriorityNormal Priority = 0
	PriorityHigh   Priority = 1
)

func (p Priority) String() string {
	switch p {
	case PriorityLow:
		return "low"
	case PriorityNormal:
		return "normal"
	case PriorityHigh:
		return "high"
	}
	return fmt.Sprintf("priority(%d)", int(p))
}

// Valid reports whether p is one of the defined priorities.
func (p Priority) Valid() bool { return p >= PriorityLow && p <= PriorityHigh }

// Job is a unit of work submitted to the queue.
type Job struct {
	ID         int
	Payload    func() error
	Status     Status
	Err        error // set when Status is StatusFailed
	EnqueuedAt time.Time
	StartedAt  time.Time
	FinishedAt time.Time
	Attempts   int       // number of times the job has been dequeued to run
	RunAt      time.Time // earliest time the job may be dequeued (set when awaiting a retry)
	Priority   Priority  // higher priority jobs are dequeued first; FIFO within a priority

	// Type and Data identify a persistable job: Type names a registered Handler
	// and Data is its opaque argument. Jobs enqueued with a bare func have an
	// empty Type and live in memory only.
	Type string
	Data []byte
}

// Handler runs a persistable job of a registered type.
type Handler func(data []byte) error

// Store persists job records so a Queue can recover after a restart.
type Store interface {
	// Save inserts or updates a job record.
	Save(Job) error
	// Load returns all persisted jobs.
	Load() ([]Job, error)
	Close() error
}

// Option configures a Queue.
type Option func(*Queue)

// WithRetry makes failed jobs retry up to maxRetries times (so a job runs at
// most maxRetries+1 times). The delay before retry n is base * 2^(n-1),
// capped at max (no cap if max <= 0).
func WithRetry(maxRetries int, base, max time.Duration) Option {
	return func(q *Queue) {
		q.maxRetries = maxRetries
		q.baseDelay = base
		q.maxDelay = max
	}
}

// WithStore enables write-through persistence for typed jobs. Use Open to
// also recover previously persisted jobs.
func WithStore(s Store) Option {
	return func(q *Queue) { q.store = s }
}

// Queue is a thread-safe job queue: highest priority first, FIFO within a priority.
type Queue struct {
	mu   sync.Mutex
	jobs []int // IDs of pending jobs, in FIFO order
	all  map[int]*Job
	next int

	store    Store
	handlers map[string]Handler

	maxRetries int
	baseDelay  time.Duration
	maxDelay   time.Duration
	now        func() time.Time // overridable clock for tests
}

func New(opts ...Option) *Queue {
	q := &Queue{all: make(map[int]*Job), handlers: make(map[string]Handler), now: time.Now}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// Open creates a Queue backed by store and recovers its persisted jobs.
// Jobs that were running when the previous process stopped are put back to
// pending so they run again (at-least-once).
func Open(store Store, opts ...Option) (*Queue, error) {
	q := New(append(opts, WithStore(store))...)
	jobs, err := store.Load()
	if err != nil {
		return nil, fmt.Errorf("load jobs: %w", err)
	}
	for _, j := range jobs {
		job := j
		if job.Status == StatusRunning {
			job.Status = StatusPending
		}
		q.all[job.ID] = &job
		if job.ID > q.next {
			q.next = job.ID
		}
		if job.Status == StatusPending {
			q.jobs = append(q.jobs, job.ID)
		}
	}
	sort.Ints(q.jobs)
	return q, nil
}

// Register associates a handler with a job type. Register handlers before
// workers start; jobs of an unregistered type fail when run.
func (q *Queue) Register(typ string, h Handler) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.handlers[typ] = h
}

// save writes a job through to the store, if any. Func-only jobs are skipped.
// Callers hold q.mu.
func (q *Queue) save(job *Job) error {
	if q.store == nil || job.Type == "" {
		return nil
	}
	return q.store.Save(*job)
}

func (q *Queue) mustSave(job *Job) {
	if err := q.save(job); err != nil {
		log.Printf("queue: persist job %d: %v", job.ID, err)
	}
}

// Close closes the underlying store, if any.
func (q *Queue) Close() error {
	if q.store == nil {
		return nil
	}
	return q.store.Close()
}

// EnqueueJob adds a persistable job of a registered-by-name type and returns
// its ID. The job is written to the store before it becomes visible.
func (q *Queue) EnqueueJob(typ string, data []byte) (int, error) {
	return q.EnqueueJobWithPriority(typ, data, PriorityNormal)
}

// EnqueueJobWithPriority is EnqueueJob with an explicit priority.
func (q *Queue) EnqueueJobWithPriority(typ string, data []byte, p Priority) (int, error) {
	if typ == "" {
		return 0, errors.New("queue: job type must not be empty")
	}
	if !p.Valid() {
		return 0, fmt.Errorf("queue: invalid priority %d", int(p))
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	job := &Job{ID: q.next + 1, Type: typ, Data: data, Priority: p, Status: StatusPending, EnqueuedAt: q.now()}
	if err := q.save(job); err != nil {
		return 0, fmt.Errorf("persist job: %w", err)
	}
	q.next++
	q.all[job.ID] = job
	q.jobs = append(q.jobs, job.ID)
	return job.ID, nil
}

// backoff returns the delay before the retry following the given failed attempt (1-based).
func (q *Queue) backoff(attempt int) time.Duration {
	d := q.baseDelay
	for i := 1; i < attempt; i++ {
		d *= 2
		if q.maxDelay > 0 && d >= q.maxDelay {
			return q.maxDelay
		}
	}
	if q.maxDelay > 0 && d > q.maxDelay {
		return q.maxDelay
	}
	return d
}

// Enqueue adds a job to the queue and returns its assigned ID.
func (q *Queue) Enqueue(payload func() error) int {
	id, _ := q.EnqueueWithPriority(payload, PriorityNormal)
	return id
}

// EnqueueWithPriority adds a job with an explicit priority. It returns an
// error if p is not a defined priority.
func (q *Queue) EnqueueWithPriority(payload func() error, p Priority) (int, error) {
	if !p.Valid() {
		return 0, fmt.Errorf("queue: invalid priority %d", int(p))
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	q.next++
	job := &Job{ID: q.next, Payload: payload, Priority: p, Status: StatusPending, EnqueuedAt: q.now()}
	q.all[job.ID] = job
	q.jobs = append(q.jobs, job.ID)
	return job.ID, nil
}

// Dequeue removes and returns the next ready job, marking it running.
// The highest-priority ready job wins; ties go to the job queued earliest.
// Jobs waiting out a retry backoff are skipped until their RunAt passes.
// It returns ok=false if no job is ready. The returned Job is a snapshot.
func (q *Queue) Dequeue() (Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.now()
	best := -1
	for i, id := range q.jobs {
		job := q.all[id]
		if job.RunAt.After(now) {
			continue
		}
		if best < 0 || job.Priority > q.all[q.jobs[best]].Priority {
			best = i
		}
	}
	if best >= 0 {
		job := q.all[q.jobs[best]]
		q.jobs = append(q.jobs[:best], q.jobs[best+1:]...)
		job.Status = StatusRunning
		job.StartedAt = now
		job.Attempts++
		q.mustSave(job)
		snap := *job
		if job.Type != "" {
			snap.Payload = q.payloadFor(job)
		}
		return snap, true
	}
	return Job{}, false
}

// payloadFor builds the runnable for a typed job. Callers hold q.mu.
func (q *Queue) payloadFor(job *Job) func() error {
	h, ok := q.handlers[job.Type]
	if !ok {
		typ := job.Type
		return func() error { return fmt.Errorf("no handler registered for job type %q", typ) }
	}
	data := job.Data
	return func() error { return h(data) }
}

// Complete records the outcome of a running job: succeeded if err is nil.
// On error the job is re-queued with exponential backoff while retries
// remain; otherwise it is marked failed. It is a no-op for unknown IDs.
func (q *Queue) Complete(id int, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.all[id]
	if !ok {
		return
	}
	now := q.now()
	switch {
	case err == nil:
		job.FinishedAt = now
		job.Status = StatusSucceeded
		job.Err = nil
	case job.Attempts <= q.maxRetries:
		job.Status = StatusPending
		job.Err = err // last error, visible while awaiting retry
		job.RunAt = now.Add(q.backoff(job.Attempts))
		q.jobs = append(q.jobs, job.ID)
	default:
		job.FinishedAt = now
		job.Status = StatusFailed
		job.Err = err
	}
	q.mustSave(job)
}

// Get looks up a job by ID and returns a snapshot of its current state.
func (q *Queue) Get(id int) (Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.all[id]
	if !ok {
		return Job{}, false
	}
	return *job, true
}

// Len returns the number of jobs currently waiting in the queue.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobs)
}
