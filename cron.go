package queue

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// CronSchedule is a parsed 5-field cron expression:
//
//	minute hour day-of-month month day-of-week
//
// Each field accepts "*", a number, a range "a-b", a list "a,b,c", and steps
// ("*/n", "a-b/n", "a/n"). Day-of-week is 0-7 (0 and 7 are Sunday). As in
// classic cron, when both day-of-month and day-of-week are restricted a time
// matches if either does. Times are evaluated in the location of the time
// passed to Next. The shortcuts @hourly, @daily and @weekly are supported.
type CronSchedule struct {
	minute, hour, dom, month, dow uint64 // bitmasks
	domStar, dowStar              bool
}

var cronShortcuts = map[string]string{
	"@hourly": "0 * * * *",
	"@daily":  "0 0 * * *",
	"@weekly": "0 0 * * 0",
}

// ParseCron parses a cron expression.
func ParseCron(spec string) (*CronSchedule, error) {
	spec = strings.TrimSpace(spec)
	if s, ok := cronShortcuts[spec]; ok {
		spec = s
	}
	f := strings.Fields(spec)
	if len(f) != 5 {
		return nil, fmt.Errorf("cron: expected 5 fields, got %d in %q", len(f), spec)
	}
	var (
		c   CronSchedule
		err error
	)
	if c.minute, err = parseCronField(f[0], 0, 59, "minute"); err != nil {
		return nil, err
	}
	if c.hour, err = parseCronField(f[1], 0, 23, "hour"); err != nil {
		return nil, err
	}
	if c.dom, err = parseCronField(f[2], 1, 31, "day-of-month"); err != nil {
		return nil, err
	}
	if c.month, err = parseCronField(f[3], 1, 12, "month"); err != nil {
		return nil, err
	}
	if c.dow, err = parseCronField(f[4], 0, 7, "day-of-week"); err != nil {
		return nil, err
	}
	if c.dow&(1<<7) != 0 { // 7 is Sunday too
		c.dow |= 1
	}
	c.dow &^= 1 << 7
	c.domStar = strings.HasPrefix(f[2], "*")
	c.dowStar = strings.HasPrefix(f[4], "*")
	return &c, nil
}

func parseCronField(field string, lo, hi int, name string) (uint64, error) {
	var mask uint64
	for _, part := range strings.Split(field, ",") {
		rng, stepStr, hasStep := strings.Cut(part, "/")
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepStr)
			if err != nil || n < 1 {
				return 0, fmt.Errorf("cron: invalid step %q in %s field", stepStr, name)
			}
			step = n
		}
		start, end := lo, hi
		switch {
		case rng == "*":
		case strings.Contains(rng, "-"):
			a, b, _ := strings.Cut(rng, "-")
			var err1, err2 error
			start, err1 = strconv.Atoi(a)
			end, err2 = strconv.Atoi(b)
			if err1 != nil || err2 != nil {
				return 0, fmt.Errorf("cron: invalid range %q in %s field", rng, name)
			}
		default:
			n, err := strconv.Atoi(rng)
			if err != nil {
				return 0, fmt.Errorf("cron: invalid value %q in %s field", rng, name)
			}
			start, end = n, n
			if hasStep { // "a/n" means a through the max, every n
				end = hi
			}
		}
		if start < lo || end > hi || start > end {
			return 0, fmt.Errorf("cron: %q out of range %d-%d in %s field", part, lo, hi, name)
		}
		for v := start; v <= end; v += step {
			mask |= 1 << uint(v)
		}
	}
	return mask, nil
}

func (c *CronSchedule) dayMatches(t time.Time) bool {
	domOK := c.dom&(1<<uint(t.Day())) != 0
	dowOK := c.dow&(1<<uint(t.Weekday())) != 0
	switch {
	case c.domStar && c.dowStar:
		return true
	case c.domStar:
		return dowOK
	case c.dowStar:
		return domOK
	}
	return domOK || dowOK
}

// Next returns the first matching minute strictly after t, or the zero time if
// none occurs within five years (e.g. "0 0 31 2 *").
func (c *CronSchedule) Next(t time.Time) time.Time {
	t = t.Truncate(time.Minute).Add(time.Minute)
	limit := t.AddDate(5, 0, 0)
	for t.Before(limit) {
		switch {
		case c.month&(1<<uint(t.Month())) == 0:
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
		case !c.dayMatches(t):
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
		case c.hour&(1<<uint(t.Hour())) == 0:
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, t.Location())
		case c.minute&(1<<uint(t.Minute())) == 0:
			t = t.Add(time.Minute)
		default:
			return t
		}
	}
	return time.Time{}
}

// cronEntry is a recurring job registration.
type cronEntry struct {
	name     string
	spec     string
	sched    *CronSchedule
	typ      string
	data     []byte
	priority Priority
	next     time.Time
	lastID   int // ID of the most recently enqueued job
}

// CronInfo describes a registered recurring job.
type CronInfo struct {
	Name     string
	Spec     string
	Type     string
	Priority Priority
	Next     time.Time // next time a job will be enqueued
	LastID   int       // ID of the last job enqueued by this entry (0 if none yet)
}

// AddCron registers a recurring typed job: each time spec fires, a job of type
// typ with data is enqueued. Names are unique. Missed firings (e.g. while the
// process was down or workers were slow) are skipped, not replayed. Schedules
// live in memory only: re-register them on startup. Nothing fires until
// RunCron (or Tick) is called.
func (q *Queue) AddCron(name, spec, typ string, data []byte, p Priority) error {
	if name == "" {
		return errors.New("queue: cron name must not be empty")
	}
	if typ == "" {
		return errors.New("queue: job type must not be empty")
	}
	if !p.Valid() {
		return fmt.Errorf("queue: invalid priority %d", int(p))
	}
	sched, err := ParseCron(spec)
	if err != nil {
		return err
	}
	next := sched.Next(q.now())
	if next.IsZero() {
		return fmt.Errorf("cron: %q never fires", spec)
	}
	q.cronMu.Lock()
	defer q.cronMu.Unlock()
	if _, dup := q.crons[name]; dup {
		return fmt.Errorf("queue: cron %q already exists", name)
	}
	if q.crons == nil {
		q.crons = make(map[string]*cronEntry)
	}
	q.crons[name] = &cronEntry{name: name, spec: spec, sched: sched, typ: typ, data: data, priority: p, next: next}
	return nil
}

// RemoveCron unregisters a recurring job. Already-enqueued jobs are unaffected.
func (q *Queue) RemoveCron(name string) error {
	q.cronMu.Lock()
	defer q.cronMu.Unlock()
	if _, ok := q.crons[name]; !ok {
		return fmt.Errorf("queue: cron %q not found", name)
	}
	delete(q.crons, name)
	return nil
}

// Crons lists registered recurring jobs ordered by name.
func (q *Queue) Crons() []CronInfo {
	q.cronMu.Lock()
	defer q.cronMu.Unlock()
	out := make([]CronInfo, 0, len(q.crons))
	for _, e := range q.crons {
		out = append(out, CronInfo{Name: e.name, Spec: e.spec, Type: e.typ, Priority: e.priority, Next: e.next, LastID: e.lastID})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Tick enqueues a job for every cron entry that is due and returns how many
// were enqueued. An entry fires at most once per tick, however many firings
// were missed, then advances to its next future time.
func (q *Queue) Tick() int {
	now := q.now()
	q.cronMu.Lock()
	defer q.cronMu.Unlock()
	n := 0
	for _, e := range q.crons {
		if e.next.After(now) {
			continue
		}
		id, err := q.enqueueJob(e.typ, e.data, e.priority, time.Time{})
		if err != nil {
			continue // retry on the next tick
		}
		e.lastID = id
		e.next = e.sched.Next(now)
		if e.next.IsZero() {
			delete(q.crons, e.name)
		}
		n++
	}
	return n
}

// RunCron calls Tick every interval until ctx is cancelled.
func (q *Queue) RunCron(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			q.Tick()
		}
	}
}
