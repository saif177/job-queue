// Command jobqueue is a CLI for the job queue's HTTP API, plus a small
// "serve" command that runs a queue server backed by SQLite.
//
//	jobqueue serve  [-addr :8080] [-db jobs.db] [-workers 4]
//	jobqueue submit [-server URL] [-priority p] [-delay d | -run-at t] [-json] <type> [data]
//	jobqueue get    [-server URL] [-json] <id>
//	jobqueue list   [-server URL] [-status s] [-json]
//	jobqueue cancel [-server URL] [-json] <id>
//	jobqueue retry  [-server URL] [-json] <id>
//
// The server address defaults to $JOBQUEUE_ADDR or http://localhost:8080.
// Flags must come before positional arguments.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

const usage = `usage: jobqueue <command> [flags] [args]

commands:
  serve    run a queue server (SQLite-backed, with a built-in "echo" job type)
  submit   submit a job:      submit [-priority low|normal|high] [-delay 30s | -run-at RFC3339] <type> [data]
  get      show one job:      get <id>
  list     list jobs:         list [-status pending|running|succeeded|failed|dead|cancelled]
  cancel   cancel a pending job
  retry    requeue a failed, dead or cancelled job

common flags: -server URL (default $JOBQUEUE_ADDR or http://localhost:8080), -json for raw JSON
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// jobView mirrors the JSON returned by the HTTP API.
type jobView struct {
	ID         int        `json:"id"`
	Type       string     `json:"type"`
	Data       string     `json:"data"`
	Status     string     `json:"status"`
	Priority   string     `json:"priority"`
	Attempts   int        `json:"attempts"`
	Error      string     `json:"error"`
	EnqueuedAt time.Time  `json:"enqueued_at"`
	StartedAt  *time.Time `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
	RunAt      *time.Time `json:"run_at"`
}

// run executes the CLI and returns the process exit code: 0 on success,
// 1 for runtime/API errors, 2 for usage errors.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return 0
	case "serve":
		return report(stderr, cmdServe(rest, stdout))
	case "submit", "get", "list", "cancel", "retry":
		return report(stderr, cmdClient(cmd, rest, stdout, stderr))
	}
	fmt.Fprintf(stderr, "jobqueue: unknown command %q\n\n%s", cmd, usage)
	return 2
}

type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

func report(stderr io.Writer, err error) int {
	if err == nil {
		return 0
	}
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	fmt.Fprintln(stderr, "jobqueue:", err)
	var ue usageError
	if errors.As(err, &ue) {
		return 2
	}
	return 1
}

func defaultServer() string {
	if s := os.Getenv("JOBQUEUE_ADDR"); s != "" {
		return s
	}
	return "http://localhost:8080"
}

func cmdClient(cmd string, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	server := fs.String("server", defaultServer(), "server base URL")
	asJSON := fs.Bool("json", false, "print raw JSON")
	var priority, delay, runAt, status *string
	switch cmd {
	case "submit":
		priority = fs.String("priority", "", "low, normal or high")
		delay = fs.String("delay", "", "run after this duration, e.g. 30s")
		runAt = fs.String("run-at", "", "run at this RFC 3339 time")
	case "list":
		status = fs.String("status", "", "filter by status")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	pos := fs.Args()
	base := strings.TrimRight(*server, "/")
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	c := &client{base: base, http: &http.Client{Timeout: 15 * time.Second}}

	switch cmd {
	case "submit":
		if len(pos) < 1 || len(pos) > 2 {
			return usageError{"submit needs <type> and optional [data]"}
		}
		req := map[string]string{"type": pos[0]}
		if len(pos) == 2 {
			req["data"] = pos[1]
		}
		if *priority != "" {
			req["priority"] = *priority
		}
		if *delay != "" {
			req["delay"] = *delay
		}
		if *runAt != "" {
			req["run_at"] = *runAt
		}
		body, err := c.do("POST", "/jobs", req)
		if err != nil {
			return err
		}
		return printJob(stdout, body, *asJSON)
	case "get", "cancel", "retry":
		if len(pos) != 1 {
			return usageError{cmd + " needs exactly one <id>"}
		}
		id := pos[0]
		if _, err := fmt.Sscanf(id, "%d", new(int)); err != nil {
			return usageError{fmt.Sprintf("invalid job id %q", id)}
		}
		method, path := "GET", "/jobs/"+id
		if cmd != "get" {
			method, path = "POST", path+"/"+cmd
		}
		body, err := c.do(method, path, nil)
		if err != nil {
			return err
		}
		return printJob(stdout, body, *asJSON)
	default: // list
		if len(pos) != 0 {
			return usageError{"list takes no arguments"}
		}
		path := "/jobs"
		if *status != "" {
			path += "?status=" + url.QueryEscape(*status)
		}
		body, err := c.do("GET", path, nil)
		if err != nil {
			return err
		}
		if *asJSON {
			_, err = fmt.Fprintln(stdout, strings.TrimSpace(string(body)))
			return err
		}
		var out struct {
			Jobs []jobView `json:"jobs"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			return fmt.Errorf("decoding response: %w", err)
		}
		return printTable(stdout, out.Jobs)
	}
}

type client struct {
	base string
	http *http.Client
}

// do performs a request and returns the response body, turning non-2xx
// responses into errors that carry the server's message.
func (c *client) do(method, path string, payload any) ([]byte, error) {
	var rd io.Reader
	if payload != nil {
		b, _ := json.Marshal(payload)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.base+path, rd)
	if err != nil {
		return nil, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cannot reach server at %s: %w", c.base, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(body, &e) == nil && e.Error != "" {
			return nil, fmt.Errorf("%s (HTTP %d)", e.Error, resp.StatusCode)
		}
		return nil, fmt.Errorf("unexpected HTTP %d", resp.StatusCode)
	}
	return body, nil
}

func printJob(w io.Writer, body []byte, asJSON bool) error {
	if asJSON {
		_, err := fmt.Fprintln(w, strings.TrimSpace(string(body)))
		return err
	}
	var j jobView
	if err := json.Unmarshal(body, &j); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "ID:\t%d\n", j.ID)
	if j.Type != "" {
		fmt.Fprintf(tw, "Type:\t%s\n", j.Type)
	}
	fmt.Fprintf(tw, "Status:\t%s\n", j.Status)
	fmt.Fprintf(tw, "Priority:\t%s\n", j.Priority)
	fmt.Fprintf(tw, "Attempts:\t%d\n", j.Attempts)
	if j.Data != "" {
		fmt.Fprintf(tw, "Data:\t%s\n", j.Data)
	}
	if j.Error != "" {
		fmt.Fprintf(tw, "Error:\t%s\n", j.Error)
	}
	fmt.Fprintf(tw, "Enqueued:\t%s\n", j.EnqueuedAt.Format(time.RFC3339))
	for _, t := range []struct {
		label string
		at    *time.Time
	}{{"Run at", j.RunAt}, {"Started", j.StartedAt}, {"Finished", j.FinishedAt}} {
		if t.at != nil {
			fmt.Fprintf(tw, "%s:\t%s\n", t.label, t.at.Format(time.RFC3339))
		}
	}
	return tw.Flush()
}

func printTable(w io.Writer, jobs []jobView) error {
	if len(jobs) == 0 {
		_, err := fmt.Fprintln(w, "no jobs")
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tTYPE\tSTATUS\tPRIORITY\tATTEMPTS\tERROR")
	for _, j := range jobs {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%d\t%s\n", j.ID, j.Type, j.Status, j.Priority, j.Attempts, truncate(j.Error, 40))
	}
	return tw.Flush()
}

func truncate(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n-1]) + "…"
	}
	return s
}
