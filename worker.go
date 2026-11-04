package queue

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// WorkerPool pulls jobs from a Queue and runs them concurrently.
type WorkerPool struct {
	queue *Queue
	size  int
	poll  time.Duration
	stop  chan struct{}
	once  sync.Once
	wg    sync.WaitGroup
}

// NewWorkerPool creates a pool of `size` workers polling `q` every `poll` interval.
func NewWorkerPool(q *Queue, size int, poll time.Duration) *WorkerPool {
	return &WorkerPool{
		queue: q,
		size:  size,
		poll:  poll,
		stop:  make(chan struct{}),
	}
}

// Start launches the worker goroutines.
func (p *WorkerPool) Start() {
	for i := 0; i < p.size; i++ {
		p.wg.Add(1)
		go p.run(i)
	}
}

// Stop signals all workers to exit and waits for them to finish. Jobs already
// running are allowed to complete; no new jobs are picked up. It is safe to
// call more than once.
func (p *WorkerPool) Stop() {
	_ = p.Shutdown(context.Background())
}

// Shutdown gracefully stops the pool: workers stop dequeuing new jobs and
// in-flight jobs are drained. It returns nil once every worker has exited, or
// ctx.Err() if ctx ends first (workers still finish in the background; a job
// left running is re-run after a restart when using a persistent store).
// It is safe to call more than once.
func (p *WorkerPool) Shutdown(ctx context.Context) error {
	p.once.Do(func() { close(p.stop) })
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RunUntilSignal starts the pool, blocks until one of sigs is received
// (SIGINT and SIGTERM if none are given), then shuts down gracefully, waiting
// up to timeout for in-flight jobs (no limit if timeout <= 0).
func (p *WorkerPool) RunUntilSignal(timeout time.Duration, sigs ...os.Signal) error {
	if len(sigs) == 0 {
		sigs = []os.Signal{os.Interrupt, syscall.SIGTERM}
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, sigs...)
	defer signal.Stop(ch)
	return p.runUntil(ch, timeout)
}

func (p *WorkerPool) runUntil(sig <-chan os.Signal, timeout time.Duration) error {
	p.Start()
	s := <-sig
	log.Printf("pool: received %v, draining in-flight jobs", s)
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return p.Shutdown(ctx)
}

func (p *WorkerPool) run(id int) {
	defer p.wg.Done()

	ticker := time.NewTicker(p.poll)
	defer ticker.Stop()

	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			select {
			case <-p.stop: // select picks randomly when both are ready
				return
			default:
			}
			job, ok := p.queue.Dequeue()
			if !ok {
				continue
			}
			err := job.Payload()
			p.queue.Complete(job.ID, err)
			if err != nil {
				log.Printf("worker %d: job %d failed: %v", id, job.ID, err)
			} else {
				log.Printf("worker %d: job %d succeeded", id, job.ID)
			}
		}
	}
}
