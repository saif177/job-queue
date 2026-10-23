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
}

// Queue is a simple thread-safe FIFO job queue.
type Queue struct {
	mu   sync.Mutex
	jobs []int // IDs of pending jobs, in FIFO order
	all  map[int]*Job
	next int
}

func New() *Queue {
	return &Queue{all: make(map[int]*Job)}
}

// Enqueue adds a job to the queue and returns its assigned ID.
func (q *Queue) Enqueue(payload func() error) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.next++
	job := &Job{ID: q.next, Payload: payload, Status: StatusPending, EnqueuedAt: time.Now()}
	q.all[job.ID] = job
	q.jobs = append(q.jobs, job.ID)
	return job.ID
}

// Dequeue removes and returns the next job, marking it running.
// It returns ok=false if the queue is empty. The returned Job is a snapshot.
func (q *Queue) Dequeue() (Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.jobs) == 0 {
		return Job{}, false
	}

	job := q.all[q.jobs[0]]
	q.jobs = q.jobs[1:]
	job.Status = StatusRunning
	job.StartedAt = time.Now()
	return *job, true
}

// Complete records the outcome of a running job: succeeded if err is nil,
// failed otherwise. It is a no-op for unknown IDs.
func (q *Queue) Complete(id int, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.all[id]
	if !ok {
		return
	}
	job.FinishedAt = time.Now()
	if err != nil {
		job.Status = StatusFailed
		job.Err = err
	} else {
		job.Status = StatusSucceeded
		job.Err = nil
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
