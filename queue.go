package queue

import "sync"

// Job is a unit of work submitted to the queue.
type Job struct {
	ID      int
	Payload func() error
}

// Queue is a simple thread-safe FIFO job queue.
type Queue struct {
	mu   sync.Mutex
	jobs []Job
	next int
}

func New() *Queue {
	return &Queue{}
}

// Enqueue adds a job to the queue and returns its assigned ID.
func (q *Queue) Enqueue(payload func() error) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.next++
	job := Job{ID: q.next, Payload: payload}
	q.jobs = append(q.jobs, job)
	return job.ID
}

// Dequeue removes and returns the next job, or ok=false if the queue is empty.
func (q *Queue) Dequeue() (Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.jobs) == 0 {
		return Job{}, false
	}

	job := q.jobs[0]
	q.jobs = q.jobs[1:]
	return job, true
}

// Len returns the number of jobs currently waiting in the queue.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobs)
}
