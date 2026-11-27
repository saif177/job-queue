package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	queue "github.com/saif177/job-queue"
)

func newServer(t *testing.T) (*queue.Queue, string) {
	t.Helper()
	q := queue.New()
	q.Register("email", func([]byte) error { return nil })
	srv := httptest.NewServer(q.HTTPHandler())
	t.Cleanup(srv.Close)
	return q, srv.URL
}

func cli(t *testing.T, args ...string) (code int, out, errOut string) {
	t.Helper()
	var o, e bytes.Buffer
	code = run(args, &o, &e)
	return code, o.String(), e.String()
}

func TestSubmitGetList(t *testing.T) {
	_, url := newServer(t)

	code, out, errOut := cli(t, "submit", "-server", url, "-priority", "high", "email", "a@example.com")
	if code != 0 {
		t.Fatalf("submit: code %d, stderr %q", code, errOut)
	}
	for _, want := range []string{"ID:", "email", "pending", "high", "a@example.com"} {
		if !strings.Contains(out, want) {
			t.Errorf("submit output missing %q:\n%s", want, out)
		}
	}

	code, out, _ = cli(t, "get", "-server", url, "1")
	if code != 0 || !strings.Contains(out, "Status:") {
		t.Fatalf("get: code %d, out %q", code, out)
	}

	code, out, _ = cli(t, "list", "-server", url)
	if code != 0 || !strings.Contains(out, "STATUS") || !strings.Contains(out, "email") {
		t.Fatalf("list: code %d, out %q", code, out)
	}

	code, out, _ = cli(t, "list", "-server", url, "-status", "failed")
	if code != 0 || strings.TrimSpace(out) != "no jobs" {
		t.Fatalf("empty list: code %d, out %q", code, out)
	}
}

func TestJSONOutput(t *testing.T) {
	_, url := newServer(t)
	code, out, _ := cli(t, "submit", "-server", url, "-json", "-delay", "1h", "email")
	if code != 0 {
		t.Fatalf("code %d", code)
	}
	var j struct {
		ID     int    `json:"id"`
		Status string `json:"status"`
		RunAt  string `json:"run_at"`
	}
	if err := json.Unmarshal([]byte(out), &j); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, out)
	}
	if j.ID != 1 || j.Status != "pending" || j.RunAt == "" {
		t.Errorf("unexpected job %+v", j)
	}

	code, out, _ = cli(t, "list", "-server", url, "-json")
	if code != 0 || !strings.Contains(out, `"jobs"`) {
		t.Errorf("list -json: code %d out %q", code, out)
	}
}

func TestCancelAndRetry(t *testing.T) {
	_, url := newServer(t)
	cli(t, "submit", "-server", url, "email")

	code, out, errOut := cli(t, "cancel", "-server", url, "1")
	if code != 0 || !strings.Contains(out, "cancelled") {
		t.Fatalf("cancel: code %d out %q err %q", code, out, errOut)
	}
	// Cancelling again is a conflict -> runtime error (exit 1).
	if code, _, errOut = cli(t, "cancel", "-server", url, "1"); code != 1 || !strings.Contains(errOut, "409") {
		t.Errorf("second cancel: code %d err %q", code, errOut)
	}
	code, out, _ = cli(t, "retry", "-server", url, "1")
	if code != 0 || !strings.Contains(out, "pending") {
		t.Fatalf("retry: code %d out %q", code, out)
	}
}

func TestErrors(t *testing.T) {
	_, url := newServer(t)

	if code, _, errOut := cli(t, "submit", "-server", url, "nope"); code != 1 || !strings.Contains(errOut, "no handler registered") {
		t.Errorf("unregistered type: code %d err %q", code, errOut)
	}
	if code, _, errOut := cli(t, "get", "-server", url, "99"); code != 1 || !strings.Contains(errOut, "not found") {
		t.Errorf("unknown id: code %d err %q", code, errOut)
	}
	if code, _, errOut := cli(t, "get", "-server", url, "abc"); code != 2 || !strings.Contains(errOut, "invalid job id") {
		t.Errorf("bad id: code %d err %q", code, errOut)
	}
	if code, _, _ := cli(t, "submit", "-server", url); code != 2 {
		t.Errorf("missing type: code %d, want 2", code)
	}
	if code, _, _ := cli(t, "bogus"); code != 2 {
		t.Errorf("unknown command: code %d, want 2", code)
	}
	if code, _, _ := cli(t); code != 2 {
		t.Errorf("no args: code %d, want 2", code)
	}
	if code, out, _ := cli(t, "help"); code != 0 || !strings.Contains(out, "usage:") {
		t.Errorf("help: code %d", code)
	}
	if code, _, errOut := cli(t, "list", "-server", "127.0.0.1:1"); code != 1 || !strings.Contains(errOut, "cannot reach server") {
		t.Errorf("unreachable: code %d err %q", code, errOut)
	}
}

func TestServerFromEnv(t *testing.T) {
	_, url := newServer(t)
	t.Setenv("JOBQUEUE_ADDR", url)
	if code, _, errOut := cli(t, "list"); code != 0 {
		t.Fatalf("code %d err %q", code, errOut)
	}
}
