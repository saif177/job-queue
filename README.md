# job-queue

A backend job queue / task scheduler, built incrementally as a daily practice project. Starts as a simple in-memory queue + worker pool and grows feature by feature — see [ROADMAP.md](ROADMAP.md) for what's done and what's next.

## Current features

- In-memory FIFO queue (`Queue`)
- Worker pool that polls the queue and runs jobs concurrently (`WorkerPool`)
- Job status tracking (pending/running/succeeded/failed) and lookup by ID (`Queue.Get`)

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

## Run the demo

```
go run ./cmd/demo
```

## Test

```
go test ./...
```
