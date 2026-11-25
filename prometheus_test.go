package queue

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestMetricsEndpoint(t *testing.T) {
	m := NewMetrics()
	q := New(WithMetrics(m))
	q.Register("email", func([]byte) error { return nil })
	q.Register("bad", func([]byte) error { return errors.New("boom") })
	h := q.HTTPHandler()

	for _, typ := range []string{"email", "email", "bad"} {
		if rec := doReq(t, h, "POST", "/jobs", `{"type":"`+typ+`"}`); rec.Code != http.StatusCreated {
			t.Fatalf("submit: %d %s", rec.Code, rec.Body)
		}
	}
	// Run two jobs by hand through the middleware chain.
	for i := 0; i < 2; i++ {
		job, ok := q.Dequeue()
		if !ok {
			t.Fatal("expected a job")
		}
		err := q.Execute(job)
		q.Complete(job.ID, err)
	}

	rec := doReq(t, h, "GET", "/metrics", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Errorf("content-type = %q", ct)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"# TYPE jobqueue_jobs gauge",
		`jobqueue_jobs{status="pending"} 1`,
		`jobqueue_jobs{status="succeeded"} 2`,
		`jobqueue_jobs{status="dead"} 0`,
		`jobqueue_job_runs_total{type="email"} 2`,
		"# TYPE jobqueue_job_failures_total counter",
		`jobqueue_job_failures_total{type="email"} 0`,
		"jobqueue_job_duration_seconds_total{type=\"email\"}",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
}

func TestMetricsEndpointWithoutCollector(t *testing.T) {
	q := New()
	rec := doReq(t, q.HTTPHandler(), "GET", "/metrics", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `jobqueue_jobs{status="pending"} 0`) {
		t.Errorf("body:\n%s", body)
	}
	if strings.Contains(body, "job_runs_total") {
		t.Errorf("unexpected collector metrics:\n%s", body)
	}
	if rec := doReq(t, q.HTTPHandler(), "POST", "/metrics", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST code = %d", rec.Code)
	}
}

func TestEscapeLabel(t *testing.T) {
	if got := escapeLabel("a\"b\\c\nd"); got != `a\"b\\c\nd` {
		t.Errorf("got %q", got)
	}
}
