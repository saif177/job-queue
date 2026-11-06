package queue

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// jobView is the JSON representation of a Job.
type jobView struct {
	ID         int        `json:"id"`
	Type       string     `json:"type,omitempty"`
	Data       string     `json:"data,omitempty"`
	Status     Status     `json:"status"`
	Priority   string     `json:"priority"`
	Attempts   int        `json:"attempts"`
	Error      string     `json:"error,omitempty"`
	EnqueuedAt time.Time  `json:"enqueued_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	RunAt      *time.Time `json:"run_at,omitempty"`
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func toView(j Job) jobView {
	v := jobView{
		ID: j.ID, Type: j.Type, Data: string(j.Data), Status: j.Status,
		Priority: j.Priority.String(), Attempts: j.Attempts, EnqueuedAt: j.EnqueuedAt,
		StartedAt: timePtr(j.StartedAt), FinishedAt: timePtr(j.FinishedAt), RunAt: timePtr(j.RunAt),
	}
	if j.Err != nil {
		v.Error = j.Err.Error()
	}
	return v
}

type submitRequest struct {
	Type     string `json:"type"`
	Data     string `json:"data"`
	Priority string `json:"priority"` // "low", "normal" (default), "high"
}

// HTTPHandler returns an http.Handler exposing the queue over a JSON API:
//
//	POST /jobs        {"type","data","priority"} -> 201 job
//	GET  /jobs/{id}   -> job
//	GET  /jobs        ?status=pending|running|... -> {"jobs":[...]}
//
// Only typed jobs (registered handlers) can be submitted over HTTP.
func (q *Queue) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", q.httpSubmit)
	mux.HandleFunc("GET /jobs/{id}", q.httpGet)
	mux.HandleFunc("GET /jobs", q.httpList)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func parsePriority(s string) (Priority, bool) {
	switch s {
	case "", "normal":
		return PriorityNormal, true
	case "low":
		return PriorityLow, true
	case "high":
		return PriorityHigh, true
	}
	return 0, false
}

func (q *Queue) httpSubmit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req submitRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	p, ok := parsePriority(req.Priority)
	if !ok {
		writeError(w, http.StatusBadRequest, "priority must be low, normal or high")
		return
	}
	q.mu.Lock()
	_, registered := q.handlers[req.Type]
	q.mu.Unlock()
	if !registered {
		writeError(w, http.StatusBadRequest, "no handler registered for job type "+strconv.Quote(req.Type))
		return
	}
	id, err := q.EnqueueJobWithPriority(req.Type, []byte(req.Data), p)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	job, _ := q.Get(id)
	w.Header().Set("Location", "/jobs/"+strconv.Itoa(id))
	writeJSON(w, http.StatusCreated, toView(job))
}

func (q *Queue) httpGet(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	job, ok := q.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, toView(job))
}

func (q *Queue) httpList(w http.ResponseWriter, r *http.Request) {
	status := Status(r.URL.Query().Get("status"))
	switch status {
	case "", StatusPending, StatusRunning, StatusSucceeded, StatusFailed, StatusDead:
	default:
		writeError(w, http.StatusBadRequest, "unknown status "+strconv.Quote(string(status)))
		return
	}
	jobs := q.List(status)
	views := make([]jobView, len(jobs))
	for i, j := range jobs {
		views[i] = toView(j)
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": views})
}
