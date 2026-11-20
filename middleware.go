package queue

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// ErrTimeout is returned by the Timeout middleware when a job exceeds its limit.
var ErrTimeout = errors.New("job timed out")

// Middleware wraps the execution of a job. It receives the job being run and
// next, which runs the rest of the chain and finally the job itself. A
// middleware may run code before/after next, short-circuit it, or alter the
// returned error.
type Middleware func(job Job, next func() error) error

// WithMiddleware installs middleware that wraps every job run by a
// WorkerPool. The first middleware is the outermost: WithMiddleware(a, b)
// runs a, then b, then the job, then unwinds back through b and a.
// Calls accumulate.
func WithMiddleware(mw ...Middleware) Option {
	return func(q *Queue) { q.middleware = append(q.middleware, mw...) }
}

// Execute runs the job's payload through the queue's middleware chain and
// returns its error. Worker pools use it; the result still has to be passed
// to Complete.
func (q *Queue) Execute(job Job) error {
	next := job.Payload
	if next == nil {
		return errors.New("job has no payload")
	}
	for i := len(q.middleware) - 1; i >= 0; i-- {
		mw, inner := q.middleware[i], next
		next = func() error { return mw(job, inner) }
	}
	return next()
}

// Recover converts a panic in the job (or inner middleware) into an error so a
// misbehaving job cannot crash a worker.
func Recover() Middleware {
	return func(job Job, next func() error) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("job panicked: %v", r)
			}
		}()
		return next()
	}
}

// Logging logs the start, outcome and duration of every job using logf
// (log.Printf if nil).
func Logging(logf func(format string, args ...any)) Middleware {
	if logf == nil {
		logf = log.Printf
	}
	return func(job Job, next func() error) error {
		start := time.Now()
		err := next()
		if err != nil {
			logf("job %d type=%q attempt=%d failed after %v: %v", job.ID, job.Type, job.Attempts, time.Since(start), err)
		} else {
			logf("job %d type=%q attempt=%d ok in %v", job.ID, job.Type, job.Attempts, time.Since(start))
		}
		return err
	}
}

// Timeout fails a job with ErrTimeout if it runs longer than d. Handlers
// cannot be interrupted in Go, so a timed-out job's goroutine keeps running
// in the background until it returns; its eventual result is discarded. A
// non-positive d disables the limit.
func Timeout(d time.Duration) Middleware {
	return func(job Job, next func() error) error {
		if d <= 0 {
			return next()
		}
		done := make(chan error, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					done <- fmt.Errorf("job panicked: %v", r)
				}
			}()
			done <- next()
		}()
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case err := <-done:
			return err
		case <-t.C:
			return fmt.Errorf("%w after %v", ErrTimeout, d)
		}
	}
}

// Metrics accumulates per-type run counts and durations. Use Middleware() to
// install it.
type Metrics struct {
	mu    sync.Mutex
	stats map[string]*TypeStats
}

// TypeStats are the totals for one job type (func jobs use the empty type).
type TypeStats struct {
	Runs      int
	Failures  int
	TotalTime time.Duration
}

// NewMetrics returns an empty collector.
func NewMetrics() *Metrics { return &Metrics{stats: make(map[string]*TypeStats)} }

// Middleware returns the collecting middleware.
func (m *Metrics) Middleware() Middleware {
	return func(job Job, next func() error) error {
		start := time.Now()
		err := next()
		d := time.Since(start)
		m.mu.Lock()
		defer m.mu.Unlock()
		s := m.stats[job.Type]
		if s == nil {
			s = &TypeStats{}
			m.stats[job.Type] = s
		}
		s.Runs++
		s.TotalTime += d
		if err != nil {
			s.Failures++
		}
		return err
	}
}

// Snapshot returns a copy of the current stats keyed by job type.
func (m *Metrics) Snapshot() map[string]TypeStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]TypeStats, len(m.stats))
	for k, v := range m.stats {
		out[k] = *v
	}
	return out
}
