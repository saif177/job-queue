package queue

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func doReq(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHTTPSubmitGetList(t *testing.T) {
	q := New()
	q.Register("email", func([]byte) error { return nil })
	h := q.HTTPHandler()

	rec := doReq(t, h, "POST", "/jobs", `{"type":"email","data":"a@b.c","priority":"high"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("submit code = %d, body %s", rec.Code, rec.Body)
	}
	var v jobView
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	if v.ID != 1 || v.Status != StatusPending || v.Priority != "high" || v.Data != "a@b.c" {
		t.Fatalf("unexpected view %+v", v)
	}
	if loc := rec.Header().Get("Location"); loc != "/jobs/1" {
		t.Fatalf("Location = %q", loc)
	}

	rec = doReq(t, h, "GET", "/jobs/1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get code = %d", rec.Code)
	}

	// Run it, then filter by status.
	job, _ := q.Dequeue()
	q.Complete(job.ID, nil)
	doReq(t, h, "POST", "/jobs", `{"type":"email"}`)

	var list struct{ Jobs []jobView }
	rec = doReq(t, h, "GET", "/jobs", "")
	json.Unmarshal(rec.Body.Bytes(), &list)
	if len(list.Jobs) != 2 {
		t.Fatalf("want 2 jobs, got %d", len(list.Jobs))
	}
	rec = doReq(t, h, "GET", "/jobs?status=succeeded", "")
	list.Jobs = nil
	json.Unmarshal(rec.Body.Bytes(), &list)
	if len(list.Jobs) != 1 || list.Jobs[0].ID != 1 || list.Jobs[0].FinishedAt == nil {
		t.Fatalf("unexpected filtered list %+v", list.Jobs)
	}
}

func TestHTTPErrors(t *testing.T) {
	q := New()
	q.Register("email", func([]byte) error { return nil })
	h := q.HTTPHandler()

	tests := []struct {
		name, method, path, body string
		want                     int
	}{
		{"bad json", "POST", "/jobs", `{`, 400},
		{"unknown field", "POST", "/jobs", `{"type":"email","x":1}`, 400},
		{"missing type", "POST", "/jobs", `{}`, 400},
		{"unregistered type", "POST", "/jobs", `{"type":"nope"}`, 400},
		{"bad priority", "POST", "/jobs", `{"type":"email","priority":"urgent"}`, 400},
		{"bad id", "GET", "/jobs/abc", "", 400},
		{"not found", "GET", "/jobs/99", "", 404},
		{"bad status", "GET", "/jobs?status=wat", "", 400},
		{"wrong method", "DELETE", "/jobs", "", 405},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if rec := doReq(t, h, tc.method, tc.path, tc.body); rec.Code != tc.want {
				t.Fatalf("code = %d, want %d (%s)", rec.Code, tc.want, rec.Body)
			}
		})
	}
}

var errBoom = errors.New("boom")

func TestHTTPFailedJobShowsError(t *testing.T) {
	q := New()
	q.Register("boom", func([]byte) error { return errBoom })
	h := q.HTTPHandler()
	doReq(t, h, "POST", "/jobs", `{"type":"boom"}`)
	job, _ := q.Dequeue()
	q.Complete(job.ID, job.Payload())
	var v jobView
	json.Unmarshal(doReq(t, h, "GET", "/jobs/1", "").Body.Bytes(), &v)
	if v.Status != StatusFailed || v.Error != "boom" {
		t.Fatalf("unexpected %+v", v)
	}
}

func TestHTTPCancelRetryAndDashboard(t *testing.T) {
	q := New(WithDeadLetter())
	q.Register("x", func([]byte) error { return errors.New("boom") })
	h := q.HTTPHandler()

	doReq(t, h, "POST", "/jobs", `{"type":"x"}`)
	rec := doReq(t, h, "POST", "/jobs/1/cancel", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"cancelled"`) {
		t.Fatalf("cancel: %d %s", rec.Code, rec.Body)
	}
	if q.Len() != 0 {
		t.Fatalf("cancelled job still queued")
	}
	if rec := doReq(t, h, "POST", "/jobs/1/cancel", ""); rec.Code != http.StatusConflict {
		t.Fatalf("double cancel code = %d", rec.Code)
	}
	if rec := doReq(t, h, "POST", "/jobs/99/cancel", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown cancel code = %d", rec.Code)
	}
	if rec := doReq(t, h, "POST", "/jobs/abc/retry", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad id code = %d", rec.Code)
	}
	if rec := doReq(t, h, "GET", "/jobs?status=cancelled", ""); !strings.Contains(rec.Body.String(), `"id":1`) {
		t.Fatalf("cancelled filter: %s", rec.Body)
	}

	// Retry a cancelled job; it runs, fails and dead-letters; retry again.
	if rec := doReq(t, h, "POST", "/jobs/1/retry", ""); rec.Code != 200 || q.Len() != 1 {
		t.Fatalf("retry: %d %s", rec.Code, rec.Body)
	}
	if rec := doReq(t, h, "POST", "/jobs/1/retry", ""); rec.Code != http.StatusConflict {
		t.Fatalf("retry of pending code = %d", rec.Code)
	}
	job, _ := q.Dequeue()
	q.Complete(job.ID, errors.New("boom"))
	if got, _ := q.Get(1); got.Status != StatusDead {
		t.Fatalf("status = %s", got.Status)
	}
	if rec := doReq(t, h, "POST", "/jobs/1/retry", ""); rec.Code != 200 {
		t.Fatalf("retry dead: %d", rec.Code)
	}
	if got, _ := q.Get(1); got.Status != StatusPending || got.Attempts != 0 {
		t.Fatalf("after retry: %+v", got)
	}

	rec = doReq(t, h, "GET", "/", "")
	if rec.Code != 200 || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") || !strings.Contains(rec.Body.String(), "Job Queue") {
		t.Fatalf("dashboard: %d %s", rec.Code, rec.Header().Get("Content-Type"))
	}
	if rec := doReq(t, h, "GET", "/nope", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown path code = %d", rec.Code)
	}
}

func TestCancelRunningRejected(t *testing.T) {
	q := New()
	id := q.Enqueue(func() error { return nil })
	q.Dequeue()
	if err := q.Cancel(id); err == nil {
		t.Fatal("expected error cancelling running job")
	}
	if err := q.Retry(id); err == nil {
		t.Fatal("expected error retrying running job")
	}
}
