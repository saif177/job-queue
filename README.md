# job-queue

A backend job queue / task scheduler, built incrementally as a daily practice project. Starts as a simple in-memory queue + worker pool and grows feature by feature — see [ROADMAP.md](ROADMAP.md) for what's done and what's next.

## Current features

- In-memory FIFO queue (`Queue`)
- Worker pool that polls the queue and runs jobs concurrently (`WorkerPool`)
- Job status tracking (pending/running/succeeded/failed) and lookup by ID (`Queue.Get`)
- Priority queues: high/normal/low via `EnqueueWithPriority` / `EnqueueJobWithPriority`, FIFO within a priority
- Configurable retries with exponential backoff (`queue.WithRetry`)
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
