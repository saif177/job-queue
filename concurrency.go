package queue

// WithConcurrencyLimit caps how many jobs of the given type may run at the
// same time, regardless of how many workers are available. Jobs over the cap
// stay pending (they do not fail or consume retries) and never block ready
// jobs of other types. The slot is released when the job completes, whether it
// succeeds, fails, or is scheduled for a retry. Only typed jobs (EnqueueJob)
// can be limited. Non-positive values are ignored.
func WithConcurrencyLimit(typ string, max int) Option {
	return func(q *Queue) {
		if typ == "" || max < 1 {
			return
		}
		if q.concLimits == nil {
			q.concLimits = make(map[string]int)
		}
		q.concLimits[typ] = max
	}
}

// concurrencyOK reports whether another job of typ may start. Callers hold q.mu.
func (q *Queue) concurrencyOK(typ string) bool {
	max, ok := q.concLimits[typ]
	return !ok || q.running[typ] < max
}

// startRunning records a started typed job. Callers hold q.mu.
func (q *Queue) startRunning(typ string) {
	if typ == "" {
		return
	}
	if q.running == nil {
		q.running = make(map[string]int)
	}
	q.running[typ]++
}

// stopRunning releases a running slot for typ. Callers hold q.mu.
func (q *Queue) stopRunning(typ string) {
	if q.running[typ] > 0 {
		q.running[typ]--
	}
}

// Running returns the number of jobs of the given type currently running.
func (q *Queue) Running(typ string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.running[typ]
}
