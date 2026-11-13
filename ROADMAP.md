# Roadmap

Incremental features, roughly in order. Pick the next unchecked item each session — doesn't have to be strictly sequential if something else makes more sense that day.

- [x] Day 1: Core in-memory queue + worker pool (enqueue, dequeue, process)
- [x] Job status tracking (pending/running/succeeded/failed) + lookup by ID
- [x] Retries with exponential backoff on failure
- [x] Persistence: swap in-memory store for SQLite
- [x] Priority queues (high/normal/low)
- [x] Dead-letter queue for jobs that exhaust retries
- [x] Graceful shutdown (drain in-flight jobs on SIGTERM)
- [x] HTTP API: submit job, get status, list jobs
- [x] Scheduled/delayed jobs (run-at timestamp)
- [x] Cron-style recurring jobs
- [x] Rate limiting per job type
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
- Day 5 (2026-10-30): priority queues — `Priority` (low/normal/high, zero value = normal); `EnqueueWithPriority` / `EnqueueJobWithPriority`; `Dequeue` returns the highest-priority ready job, FIFO within a priority, and a backing-off job never blocks lower-priority ready jobs; priority persisted in SQLite with automatic migration of older databases. No aging yet, so sustained high-priority load can starve low.
- Day 6 (2026-11-02): dead-letter queue — `queue.WithDeadLetter()` moves jobs that exhaust retries to `StatusDead` instead of `StatusFailed`; `DeadLetters()` lists them, `Requeue(id)` gives a dead job a fresh set of retries, `Discard(id)` drops it from memory (persisted record kept for audit). Dead jobs persist in SQLite and are not re-run on restart.
- Day 7 (2026-11-04): graceful shutdown — `WorkerPool.Shutdown(ctx)` stops dequeuing and drains in-flight jobs (returns `ctx.Err()` on timeout); `Stop()` is now idempotent; `RunUntilSignal(timeout, sigs...)` starts the pool and shuts down on SIGINT/SIGTERM. Fixed a race where a worker could dequeue one more job after stop was signalled. Jobs still running when the timeout hits revert to pending on restart with a persistent store.
- Day 8 (2026-11-06): HTTP API — `Queue.HTTPHandler()` serves `POST /jobs` (type, data, priority; 201 + Location), `GET /jobs/{id}`, and `GET /jobs?status=` with JSON responses and validation (400 for bad body/priority/unregistered type/status, 404 unknown job, 405 wrong method). Added `Queue.List(status)`. Only typed (registered-handler) jobs can be submitted over HTTP; no auth yet.
- Day 9 (2026-11-09): scheduled/delayed jobs — `EnqueueAt`/`EnqueueIn` (func jobs) and `EnqueueJobAt`/`EnqueueJobIn` (typed jobs) set a `RunAt` before which `Dequeue` skips the job (reusing the retry-backoff mechanism, so scheduled jobs never block ready ones); HTTP `POST /jobs` accepts `run_at` (RFC 3339) or `delay` (e.g. "30s"), mutually exclusive, validated with 400s; RunAt persists in SQLite so schedules survive restarts (overdue jobs run immediately on recovery). No timer wakeups — workers still poll.
- Day 10 (2026-11-11): cron-style recurring jobs — `ParseCron` (5-field: lists, ranges, steps, `@hourly/@daily/@weekly`, dom/dow OR semantics, Sunday as 0 or 7) with `Next(t)`; `Queue.AddCron(name, spec, type, data, priority)` / `RemoveCron` / `Crons`; `Tick()` enqueues a typed job for each due entry and `RunCron(ctx, interval)` drives it. Missed firings collapse into one job rather than replaying. Schedules are in-memory only (re-register on startup) and evaluated in the clock's timezone; no HTTP endpoint yet.
- Day 11 (2026-11-13): per-type rate limiting — `queue.WithRateLimit(type, perSecond, burst)` adds a token bucket per typed job type; `Dequeue` skips jobs whose type has no token (they stay pending, don't fail or use retries, and never block other types or lower priorities), consuming a token only when a job actually starts, so retries are limited too. Limits are in-memory config set at construction; func jobs can't be limited; no HTTP visibility yet.
