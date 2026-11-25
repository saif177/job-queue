package queue

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// WithMetrics attaches a Metrics collector to the queue: its middleware is
// installed (outermost) and its counters are exported by GET /metrics.
func WithMetrics(m *Metrics) Option {
	return func(q *Queue) {
		q.metrics = m
		q.middleware = append([]Middleware{m.Middleware()}, q.middleware...)
	}
}

var allStatuses = []Status{
	StatusPending, StatusRunning, StatusSucceeded, StatusFailed,
	StatusDead, StatusCancelled,
}

// escapeLabel escapes a Prometheus label value.
func escapeLabel(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(s)
}

// WritePrometheus renders queue state in the Prometheus text exposition
// format (version 0.0.4): job counts by status, running jobs per type and,
// when a collector is attached with WithMetrics, per-type run counters.
func (q *Queue) WritePrometheus(w interface{ Write([]byte) (int, error) }) error {
	var b strings.Builder

	counts := make(map[Status]int, len(allStatuses))
	for _, j := range q.List("") {
		counts[j.Status]++
	}
	b.WriteString("# HELP jobqueue_jobs Number of jobs known to the queue, by status.\n")
	b.WriteString("# TYPE jobqueue_jobs gauge\n")
	for _, s := range allStatuses {
		fmt.Fprintf(&b, "jobqueue_jobs{status=\"%s\"} %d\n", s, counts[s])
	}

	q.mu.Lock()
	types := make([]string, 0, len(q.running))
	running := make(map[string]int, len(q.running))
	for t, n := range q.running {
		if n > 0 {
			types = append(types, t)
			running[t] = n
		}
	}
	q.mu.Unlock()
	sort.Strings(types)
	b.WriteString("# HELP jobqueue_running_jobs Jobs currently running, by job type.\n")
	b.WriteString("# TYPE jobqueue_running_jobs gauge\n")
	for _, t := range types {
		fmt.Fprintf(&b, "jobqueue_running_jobs{type=\"%s\"} %d\n", escapeLabel(t), running[t])
	}

	if q.metrics != nil {
		snap := q.metrics.Snapshot()
		keys := make([]string, 0, len(snap))
		for k := range snap {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteString("# HELP jobqueue_job_runs_total Job executions, by job type.\n")
		b.WriteString("# TYPE jobqueue_job_runs_total counter\n")
		for _, k := range keys {
			fmt.Fprintf(&b, "jobqueue_job_runs_total{type=\"%s\"} %d\n", escapeLabel(k), snap[k].Runs)
		}
		b.WriteString("# HELP jobqueue_job_failures_total Failed job executions, by job type.\n")
		b.WriteString("# TYPE jobqueue_job_failures_total counter\n")
		for _, k := range keys {
			fmt.Fprintf(&b, "jobqueue_job_failures_total{type=\"%s\"} %d\n", escapeLabel(k), snap[k].Failures)
		}
		b.WriteString("# HELP jobqueue_job_duration_seconds_total Total time spent executing jobs, by job type.\n")
		b.WriteString("# TYPE jobqueue_job_duration_seconds_total counter\n")
		for _, k := range keys {
			fmt.Fprintf(&b, "jobqueue_job_duration_seconds_total{type=\"%s\"} %g\n", escapeLabel(k), snap[k].TotalTime.Seconds())
		}
	}

	_, err := w.Write([]byte(b.String()))
	return err
}

func (q *Queue) httpMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_ = q.WritePrometheus(w)
}
