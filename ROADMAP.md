# Roadmap

Incremental features, roughly in order. Pick the next unchecked item each session — doesn't have to be strictly sequential if something else makes more sense that day.

- [x] Day 1: Core in-memory queue + worker pool (enqueue, dequeue, process)
- [ ] Job status tracking (pending/running/succeeded/failed) + lookup by ID
- [ ] Retries with exponential backoff on failure
- [ ] Persistence: swap in-memory store for SQLite
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
