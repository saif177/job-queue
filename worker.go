package queue

import (
	"log"
	"sync"
	"time"
)

// WorkerPool pulls jobs from a Queue and runs them concurrently.
type WorkerPool struct {
	queue *Queue
	size  int
	poll  time.Duration
	stop  chan struct{}
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

// Stop signals all workers to exit and waits for them to finish.
func (p *WorkerPool) Stop() {
	close(p.stop)
	p.wg.Wait()
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
			job, ok := p.queue.Dequeue()
			if !ok {
				continue
			}
			if err := job.Payload(); err != nil {
				log.Printf("worker %d: job %d failed: %v", id, job.ID, err)
			} else {
				log.Printf("worker %d: job %d succeeded", id, job.ID)
			}
		}
	}
}
