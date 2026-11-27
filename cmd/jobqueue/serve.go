package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	queue "github.com/saif177/job-queue"
)

// cmdServe runs an HTTP queue server backed by SQLite with a worker pool.
// Only a built-in "echo" job type is registered (it prints its data), which
// makes the CLI usable end to end; real deployments register their own handlers.
func cmdServe(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", ":8080", "listen address")
	db := fs.String("db", "jobs.db", "SQLite database path")
	workers := fs.Int("workers", 4, "number of workers")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *workers < 1 {
		return usageError{"-workers must be at least 1"}
	}

	store, err := queue.OpenSQLite(*db)
	if err != nil {
		return err
	}
	q, err := queue.Open(store, queue.WithRetry(3, time.Second, 30*time.Second), queue.WithDeadLetter())
	if err != nil {
		return err
	}
	defer q.Close()
	q.Register("echo", func(data []byte) error {
		fmt.Fprintf(stdout, "echo: %s\n", data)
		return nil
	})

	pool := queue.NewWorkerPool(q, *workers, 50*time.Millisecond)
	pool.Start()
	srv := &http.Server{Addr: *addr, Handler: q.HTTPHandler()}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	fmt.Fprintf(stdout, "jobqueue listening on %s (db %s, %d workers)\n", *addr, *db, *workers)

	select {
	case err := <-errc:
		pool.Stop()
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return pool.Shutdown(sctx)
}
