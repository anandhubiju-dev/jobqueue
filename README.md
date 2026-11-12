# jobqueue

A background job processing system in Go. It moves slow or non-critical work out of the HTTP request path, so the API answers in milliseconds while workers do the work.

## Contents

1. [Problem](#problem)
2. [Architecture](#architecture)
3. [Job lifecycle](#job-lifecycle)
4. [API](#api)
5. [Database design](#database-design)
6. [Concurrency model](#concurrency-model)
7. [Failure handling and idempotency](#failure-handling-and-idempotency)
8. [Why Redis](#why-redis)
9. [Observability](#observability)
10. [Performance results](#performance-results)
11. [Running locally](#running-locally)
12. [Testing](#testing)
13. [Limitations](#limitations)
14. [Lessons learned](#lessons-learned)

## Problem

Many requests trigger work that is slow, expensive, or does not need to finish before the user gets a response: sending email, generating reports, calling webhooks, processing files.

Doing it inline makes every request as slow as its slowest step:

```text
Client -> API -> do everything synchronously -> slow response
```

This project does this instead:

```text
Client -> API -> store job -> queue -> respond (202)
                                 |
                                 v
                              worker -> process in background
```

In local load tests, `POST /jobs` had a p95 latency of 7 to 10 ms with 2 to 5 concurrent clients and 34 to 97 ms with 10 to 50 clients, while background processing took seconds per job. These runs were on one laptop with Docker Desktop, so the high-concurrency figures partly measure the test machine. See [Performance results](#performance-results).

The project is also a study of what Go offers for this: goroutines, channels, `context`, graceful shutdown, and the race detector. Each technology was added only after a measured or observed limitation justified it.

## Architecture

```text
Client
  |
  v
nginx (proxy, port 8080)
  |
  v
+------------------------------------------------------------+
| app (one or more replicas)                                 |
|                                                            |
|  Gin handlers --> Service --+--> Store ---------> PostgreSQL
|  (validation,      (rules,  |    (all SQL)        (source of truth)
|   request IDs)      create) |
|                             +--> Queue ---------> Redis list
|                                  (job IDs only)    (or in-memory channel)
|                                        |
|                                        v
|                              Worker pool (N goroutines)
|                              claim -> process -> finish
|                                                            |
|  Scheduler (one goroutine per app instance, ticks every    |
|  second)                                                   |
|   - reclaims jobs whose worker died (stale heartbeat)      |
|   - enqueues scheduled jobs that are now due               |
|   - moves due retries back to the queue                    |
|   - re-enqueues orphaned queued jobs                       |
+------------------------------------------------------------+
```

**Several instances, one scheduler each.** Every replica runs its own scheduler, so they coordinate through PostgreSQL:

- Reclaiming stale jobs and moving due retries use `FOR UPDATE SKIP LOCKED`, so each row is handled by exactly one scheduler.
- Listing due scheduled jobs and orphaned `queued` jobs are plain reads, so two instances may enqueue the same ID. That is harmless: the worker's claim (`UPDATE ... WHERE status = 'queued'`) lets only one copy win.

This is the design. It has not been load tested with several replicas. Two replicas were run once, for a kill-one-instance recovery experiment.

**Packages**

| Package | Responsibility |
|---|---|
| `cmd/server` | Wiring only: config, start goroutines, graceful shutdown |
| `cmd/loadtest` | Load test client (separate from the production binary) |
| `internal/job` | Domain model: `Job`, `Status`, sentinel errors |
| `internal/api` | HTTP handlers, request-ID middleware, health endpoints |
| `internal/service` | Validation and job creation rules |
| `internal/store` | All PostgreSQL access (pgx, plain SQL) |
| `internal/queue` | `Queue` interface with `InMemory` and `Redis` implementations |
| `internal/worker` | Worker loop, processors, retry and backoff logic |
| `internal/scheduler` | Polling loop for retries, schedules, reclaim and orphans |
| `internal/metrics` | `expvar` counters and gauges |

**Key design decisions**

- **PostgreSQL is the source of truth.** The queue carries only job IDs. Losing or duplicating a queue message cannot lose a job or change its state.
- **The queue is an interface.** The in-memory channel came first. Redis was added behind the same interface (`Enqueue`, `Dequeue`, `Len`, `Ping`). The service only needs `Enqueue` and the workers only need `Dequeue`, so each declares its own narrow interface and neither depends on a particular queue implementation. Only `cmd/server` chooses one.
- **Each consumer defines the interface it needs.** The service, worker, and scheduler each declare a small `Store` or `Queue` interface. That keeps dependencies narrow and lets tests use fakes without a database.
- **Ownership is decided by the database, not by Go locks.** Claiming a job is one atomic SQL update, which is why the design works across several app instances.