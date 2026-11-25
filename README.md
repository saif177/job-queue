# job-queue

A backend job queue / task scheduler, built incrementally as a daily practice project. Starts as a simple in-memory queue + worker pool and grows feature by feature — see [ROADMAP.md](ROADMAP.md) for what's done and what's next.

## Current features

- In-memory FIFO queue (`Queue`)
- Worker pool that polls the queue and runs jobs concurrently (`WorkerPool`)
- Job status tracking (pending/running/succeeded/failed) and lookup by ID (`Queue.Get`)
- Priority queues: high/normal/low via `EnqueueWithPriority` / `EnqueueJobWithPriority`, FIFO within a priority
- Configurable retries with exponential backoff (`queue.WithRetry`)
- Dead-letter queue (`WithDeadLetter`): inspect, requeue, or discard jobs that exhausted retries
- Graceful shutdown: `WorkerPool.Shutdown(ctx)` drains in-flight jobs; `RunUntilSignal` handles SIGINT/SIGTERM
- HTTP API (`Queue.HTTPHandler`): submit jobs, get status by ID, list jobs (filter by status)
- Web dashboard at `GET /` (filter jobs, cancel pending, retry failed/dead) plus `POST /jobs/{id}/cancel|retry`; `Queue.Cancel` / `Queue.Retry`
- Scheduled/delayed jobs: `EnqueueAt` / `EnqueueIn` / `EnqueueJobAt` / `EnqueueJobIn`, or `run_at` / `delay` over HTTP
- Cron-style recurring jobs: `AddCron(name, "*/5 * * * *", type, data, priority)` + `RunCron(ctx, interval)`
- Per-type rate limiting (`WithRateLimit(type, perSecond, burst)`): token bucket applied at dequeue
- Per-type concurrency limits (`WithConcurrencyLimit(type, max)`): cap simultaneously running jobs of a type
- Job middleware (`WithMiddleware`): `Logging`, `Recover`, `Timeout(d)` and a `Metrics` collector, or your own wrappers
- Prometheus metrics at `GET /metrics`: job counts by status, running jobs per type, and run/failure/duration counters via `WithMetrics`
- Postgres persistence (`OpenPostgres(dsn)`): same `Store` interface, swap-in for SQLite
- SQLite persistence: typed jobs (`EnqueueJob` + `Register`) survive restarts via `queue.Open(store)`

## Usage

```go
q := queue.New()
q.Enqueue(func() error {
    // do work
    return nil
})

pool := queue.NewWorkerPool(q, 3, 20*time.Millisecond)
pool.Start()
defer pool.Stop()
```

### Persistent jobs

```go
store, _ := queue.OpenSQLite("jobs.db")
q, _ := queue.Open(store) // recovers pending/interrupted jobs
defer q.Close()
q.Register("email", func(data []byte) error { /* send */ return nil })
q.EnqueueJob("email", []byte("a@example.com"))
```

## Run the demo

```
go run ./cmd/demo
```

## Test

```
go test ./...
```

### HTTP API

```go
http.ListenAndServe(":8080", q.HTTPHandler())
```

```
curl -X POST localhost:8080/jobs -d '{"type":"email","data":"a@example.com","priority":"high"}'
curl -X POST localhost:8080/jobs -d '{"type":"email","data":"a@example.com","delay":"30s"}'
curl localhost:8080/jobs/1
curl 'localhost:8080/jobs?status=pending'
```

### Postgres

```go
store, _ := queue.OpenPostgres("postgres://user:pass@localhost:5432/jobs")
q, _ := queue.Open(store)
```

Run the Postgres tests with `JOBQUEUE_POSTGRES_DSN=... go test ./...` (skipped otherwise).
