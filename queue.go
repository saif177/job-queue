package queue

import (
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

// Queue is a simple thread-safe FIFO job queue.
type Queue struct {
	mu   sync.Mutex
	jobs []int // IDs of pending jobs, in FIFO order
	all  map[int]*Job
	next int

	maxRetries int
	baseDelay  time.Duration
	maxDelay   time.Duration
	now        func() time.Time // overridable clock for tests
}

func New(opts ...Option) *Queue {
	q := &Queue{all: make(map[int]*Job), now: time.Now}
	for _, opt := range opts {
		opt(q)
	}
	return q
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
	q.mu.Lock()
	defer q.mu.Unlock()

	q.next++
	job := &Job{ID: q.next, Payload: payload, Status: StatusPending, EnqueuedAt: q.now()}
	q.all[job.ID] = job
	q.jobs = append(q.jobs, job.ID)
	return job.ID
}

// Dequeue removes and returns the next ready job, marking it running.
// Jobs waiting out a retry backoff are skipped until their RunAt passes.
// It returns ok=false if no job is ready. The returned Job is a snapshot.
func (q *Queue) Dequeue() (Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.now()
	for i, id := range q.jobs {
		job := q.all[id]
		if job.RunAt.After(now) {
			continue
		}
		q.jobs = append(q.jobs[:i], q.jobs[i+1:]...)
		job.Status = StatusRunning
		job.StartedAt = now
		job.Attempts++
		return *job, true
	}
	return Job{}, false
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
