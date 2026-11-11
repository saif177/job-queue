package queue

import (
	"context"
	"testing"
	"time"
)

func TestParseCronErrors(t *testing.T) {
	for _, spec := range []string{"", "* * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * * 13 *", "*/0 * * * *", "a * * * *", "5-1 * * * *", "* * * * 8"} {
		if _, err := ParseCron(spec); err == nil {
			t.Errorf("ParseCron(%q): expected error", spec)
		}
	}
}

func TestCronNext(t *testing.T) {
	utc := time.UTC
	d := func(y int, m time.Month, day, h, min int) time.Time { return time.Date(y, m, day, h, min, 0, 0, utc) }
	tests := []struct {
		spec string
		from time.Time
		want time.Time
	}{
		{"* * * * *", d(2026, 1, 1, 12, 0), d(2026, 1, 1, 12, 1)},
		{"*/15 * * * *", d(2026, 1, 1, 12, 0), d(2026, 1, 1, 12, 15)},
		{"*/15 * * * *", d(2026, 1, 1, 12, 50), d(2026, 1, 1, 13, 0)},
		{"30 9 * * *", d(2026, 1, 1, 10, 0), d(2026, 1, 2, 9, 30)},
		{"0 0 1 * *", d(2026, 1, 15, 0, 0), d(2026, 2, 1, 0, 0)},
		{"0 0 29 2 *", d(2026, 1, 1, 0, 0), d(2028, 2, 29, 0, 0)},
		{"0 9 * * 1-5", d(2026, 1, 2, 10, 0), d(2026, 1, 5, 9, 0)}, // Fri -> Mon
		{"0 9 * * 7", d(2026, 1, 1, 0, 0), d(2026, 1, 4, 9, 0)},    // 7 == Sunday
		{"0,30 8-9 * * *", d(2026, 1, 1, 8, 30), d(2026, 1, 1, 9, 0)},
		{"10/20 * * * *", d(2026, 1, 1, 0, 0), d(2026, 1, 1, 0, 10)},
		{"@hourly", d(2026, 1, 1, 12, 30), d(2026, 1, 1, 13, 0)},
		{"@daily", d(2026, 1, 1, 12, 30), d(2026, 1, 2, 0, 0)},
		// dom and dow both restricted: either matches. Jan 13 2026 is a Tuesday.
		{"0 0 15 * 2", d(2026, 1, 10, 0, 0), d(2026, 1, 13, 0, 0)},
	}
	for _, tc := range tests {
		c, err := ParseCron(tc.spec)
		if err != nil {
			t.Fatalf("%q: %v", tc.spec, err)
		}
		if got := c.Next(tc.from); !got.Equal(tc.want) {
			t.Errorf("%q from %v: got %v want %v", tc.spec, tc.from, got, tc.want)
		}
	}
}

func TestCronNeverFires(t *testing.T) {
	c, _ := ParseCron("0 0 31 2 *")
	if got := c.Next(time.Now()); !got.IsZero() {
		t.Fatalf("expected zero, got %v", got)
	}
	q := New()
	if err := q.AddCron("x", "0 0 31 2 *", "t", nil, PriorityNormal); err == nil {
		t.Fatal("expected error for schedule that never fires")
	}
}

func TestTickEnqueuesWhenDue(t *testing.T) {
	q := New()
	now := fakeClock(q) // 12:00:00
	if err := q.AddCron("report", "*/5 * * * *", "report", []byte("d"), PriorityHigh); err != nil {
		t.Fatal(err)
	}
	if n := q.Tick(); n != 0 {
		t.Fatalf("nothing due yet, got %d", n)
	}
	*now = now.Add(5 * time.Minute)
	if n := q.Tick(); n != 1 {
		t.Fatalf("want 1 enqueued, got %d", n)
	}
	if n := q.Tick(); n != 0 {
		t.Fatalf("must not fire twice for the same slot, got %d", n)
	}
	infos := q.Crons()
	if len(infos) != 1 || infos[0].LastID == 0 || !infos[0].Next.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("unexpected info %+v", infos)
	}
	j, ok := q.Get(infos[0].LastID)
	if !ok || j.Type != "report" || string(j.Data) != "d" || j.Priority != PriorityHigh {
		t.Fatalf("unexpected job %+v", j)
	}
}

func TestTickSkipsMissedFirings(t *testing.T) {
	q := New()
	now := fakeClock(q)
	q.AddCron("m", "* * * * *", "t", nil, PriorityNormal)
	*now = now.Add(time.Hour)
	if n := q.Tick(); n != 1 {
		t.Fatalf("missed firings should collapse to one job, got %d", n)
	}
	if q.Len() != 1 {
		t.Fatalf("want 1 pending job, got %d", q.Len())
	}
}

func TestAddRemoveCron(t *testing.T) {
	q := New()
	fakeClock(q)
	if err := q.AddCron("a", "bad", "t", nil, PriorityNormal); err == nil {
		t.Fatal("expected parse error")
	}
	if err := q.AddCron("", "* * * * *", "t", nil, PriorityNormal); err == nil {
		t.Fatal("expected empty name error")
	}
	if err := q.AddCron("a", "* * * * *", "", nil, PriorityNormal); err == nil {
		t.Fatal("expected empty type error")
	}
	if err := q.AddCron("a", "* * * * *", "t", nil, Priority(9)); err == nil {
		t.Fatal("expected priority error")
	}
	if err := q.AddCron("a", "* * * * *", "t", nil, PriorityNormal); err != nil {
		t.Fatal(err)
	}
	if err := q.AddCron("a", "* * * * *", "t", nil, PriorityNormal); err == nil {
		t.Fatal("expected duplicate error")
	}
	if err := q.RemoveCron("a"); err != nil {
		t.Fatal(err)
	}
	if err := q.RemoveCron("a"); err == nil {
		t.Fatal("expected not found")
	}
	if len(q.Crons()) != 0 {
		t.Fatal("expected no crons")
	}
}

func TestRunCronEndToEnd(t *testing.T) {
	q := New()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	q.now = func() time.Time { return now }
	ran := make(chan struct{}, 1)
	q.Register("ping", func([]byte) error { ran <- struct{}{}; return nil })
	q.AddCron("p", "* * * * *", "ping", nil, PriorityNormal)
	now = now.Add(time.Minute) // make it due

	pool := NewWorkerPool(q, 1, 5*time.Millisecond)
	pool.Start()
	defer pool.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q.RunCron(ctx, 5*time.Millisecond)

	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("cron job never ran")
	}
}
