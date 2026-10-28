# Roadmap

Incremental features, roughly in order. Pick the next unchecked item each session — doesn't have to be strictly sequential if something else makes more sense that day.

- [x] Day 1: Core in-memory queue + worker pool (enqueue, dequeue, process)
- [x] Job status tracking (pending/running/succeeded/failed) + lookup by ID
- [x] Retries with exponential backoff on failure
- [x] Persistence: swap in-memory store for SQLite
- [ ] Priority queues (high/normal/low)
- [ ] Dead-letter queue for jobs that exhaust retries
- [ ] Graceful shutdown (drain in-flight jobs on SIGTERM)
- [ ] HTTP API: submit job, get status, list jobs
- [ ] Scheduled/delayed jobs (run-at timestamp)
- [ ] Cron-style recurring jobs
- [ ] Rate limiting per job type
- [ ] Concurrency limits per job type / worker pool tuning
- [ ] Postgres backend option (swap-able storage interface)
- [ ] Job middleware (logging, metrics, timeout enforcement)
- [ ] Simple web dashboard (list jobs, retry/cancel from UI)
- [ ] Structured metrics endpoint (Prometheus format)
- [ ] CLI tool to submit/inspect jobs
- [ ] Horizontal scaling: multiple worker processes pulling from shared store
- [ ] At-least-once vs exactly-once semantics doc + idempotency key support
- [ ] Load test + README with benchmarks

## Done

- Day 1 (2026-10-22): scaffolded repo, basic in-memory queue + worker pool.
- Day 2 (2026-10-23): job status tracking (pending/running/succeeded/failed) with timestamps and error, `Queue.Get` lookup by ID, `Queue.Complete` called by workers.
- Day 3 (2026-10-26): retries with exponential backoff — `queue.New(queue.WithRetry(maxRetries, base, max))`; failed jobs go back to pending with a `RunAt` delay (base·2^(n-1), capped), `Attempts` tracked on `Job`, `Dequeue` skips jobs still backing off; marked failed once retries are exhausted.
- Day 4 (2026-10-28): SQLite persistence — `Store` interface + pure-Go `SQLiteStore` (modernc.org/sqlite); `queue.Open(store)` recovers jobs after restart (running jobs revert to pending); persistable jobs are enqueued via `EnqueueJob(type, data)` and run by handlers added with `Register` (bare-func jobs stay in-memory only); state changes are written through on enqueue/dequeue/complete.
