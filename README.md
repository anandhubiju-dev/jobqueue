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


## Job lifecycle

```text
                    +--------------------------------------+
                    |  run_at in the future (scheduled)    |
                    |  stays queued until due              |
                    +------------------+-------------------+
                                       |
  POST /jobs --> queued --claim--> processing --success--> completed
                   ^                   |
                   |                   +--transient error, attempts left--> retrying
                   |                   |                                       |
                   |                   +--permanent error, or attempts used--> failed
                   |                   |
                   |                   +--worker died (stale heartbeat)------> retrying / failed
                   |                                                           |
                   +---------------- scheduler: run_at reached ----------------+
```

| Status | Meaning |
|---|---|
| `queued` | Waiting for a worker (or for its `run_at`, if scheduled) |
| `processing` | A worker claimed it; `attempts` was incremented and `heartbeat_at` is refreshed every 2 s |
| `retrying` | An attempt failed or was lost; waiting until `run_at` (backoff) |
| `completed` | Finished; `error` is cleared |
| `failed` | Permanent error, or `attempts >= max_attempts` (default 3); last error kept |

**Rules**

- **Claiming is atomic.** `queued -> processing` is one `UPDATE ... WHERE status = 'queued'`. If it matches no row, another worker already has the job and this copy of the ID is skipped.
- **Transient vs permanent.** An error is transient unless the processor marks it permanent (unknown job type, malformed payload). Timeouts and ordinary failures are transient.
- **Backoff.** Retry delay is 1 s, 2 s, 4 s, ... capped at 1 minute, with random jitter in the upper half of each delay, so many failures do not retry in lockstep.
- **Lost workers.** If a `processing` job's heartbeat is more than 6 s old, the scheduler moves it to `retrying` (or `failed` if attempts are used up). The dead attempt counts toward the limit, so a job that kills its worker every time cannot loop forever.
- **Fencing.** Every finishing write (`completed`, `failed`, `retrying`, heartbeat) includes the attempt number and matches only if `status = 'processing' AND attempts = N`. A worker that was merely paused, and replaced by a newer attempt, cannot overwrite the newer attempt's state.
- **Rejected submissions leave nothing behind.** If the queue is full, the just-inserted row is deleted before the client gets its 503.

## API

### `POST /jobs`

```json
{
  "type": "email",
  "payload": { "to": "user@example.com", "subject": "Welcome" },
  "delay_seconds": 30
}
```

| Field | Required | Notes |
|---|---|---|
| `type` | yes | `email` (simulated 2 s), `report` (4 s), `notification` (1 s), `flaky`, `payment` |
| `payload` | yes | Any valid JSON. `flaky` reads `{"fail_times": N}`; `payment` reads `{"amount_cents": N, "fail_after_charge": bool}` |
| `delay_seconds` | no | 0 to 31536000. Mutually exclusive with `run_at` |
| `run_at` | no | RFC 3339 timestamp, at most one year ahead. A past time runs immediately |

| Header | Purpose |
|---|---|
| `Idempotency-Key` | Same key returns the original job instead of creating a second one |
| `X-Request-ID` | Accepted if 1 to 64 characters from `A-Za-z0-9._-`; otherwise a UUID is generated. Echoed in the response and stored with the job |

**Responses**

| Status | Body | When |
|---|---|---|
| `202` | `{"id":"...","status":"queued"}` | Accepted |
| `400` | `{"error":"malformed JSON body"}` | Body is not parseable JSON |
| `400` | `{"error":"invalid job: unknown type \"bogus\""}` | Validation failed (type, payload, `run_at` too far ahead) |
| `400` | `{"error":"use either delay_seconds or run_at, not both"}` | Both scheduling fields sent |
| `400` | `{"error":"delay_seconds must be between 0 and 31536000"}` | Delay out of range |
| `503` | `{"error":"server busy, retry later"}` | Queue full (backpressure). No job was created |
| `500` | `{"error":"internal error","request_id":"..."}` | Unexpected failure; quote the request ID |

Notes:

- `202` means "accepted", not "done". Poll `GET /jobs/:id` for the outcome.
- With a repeated `Idempotency-Key`, the `status` in the `202` body is the original job's current status (it may already be `completed`). Reusing a key with a different body silently returns the original job; it is not rejected.
- A `503` response carries no job ID, so retry with an `Idempotency-Key` to avoid duplicates.

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/jobs `
  -ContentType "application/json" `
  -Headers @{ "Idempotency-Key" = "order-123" } `
  -Body '{"type":"email","payload":{},"delay_seconds":10}'
```

### `GET /jobs/:id`

Returns the job:

```json
{
  "id": "3242b4f3-0d67-4bc3-9864-bf4bbdba0980",
  "type": "email",
  "payload": {},
  "status": "completed",
  "attempts": 1,
  "max_attempts": 3,
  "run_at": "2026-10-29T06:21:30.572Z",
  "created_at": "2026-10-29T06:21:30.572Z",
  "started_at": "2026-10-29T06:21:30.576Z",
  "completed_at": "2026-10-29T06:21:32.605Z",
  "error": null
}
```

`404 {"error":"job not found"}` for an unknown ID, or one that is not a valid UUID. The idempotency key and creating request ID are stored but not returned.

### Operational endpoints

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Liveness: `200 {"status":"ok"}` whenever the process can answer |
| `GET /readyz` | Readiness: pings PostgreSQL and the queue. `200`, or `503` such as `{"checks":{"postgres":"ok","queue":"unavailable"},"ready":false}`. Names the failing dependency, not the error text |
| `GET /debug/vars` | `expvar` counters and gauges (see Observability) |

`/debug/vars` is currently reachable through the proxy. Restrict it before any real deployment.