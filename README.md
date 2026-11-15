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
| `processing` | A worker claimed it; `attempts` was incremented and `heartbeat_at` is refreshed every 2 s while the attempt is running |
| `retrying` | An attempt failed or was lost; waiting until `run_at` (backoff) |
| `completed` | Finished; `error` is cleared |
| `failed` | Permanent error, or `attempts >= max_attempts` (default 3); last error kept |

**Rules**

- **Claiming is atomic.** `queued -> processing` is one `UPDATE ... WHERE status = 'queued'`. If it matches no row, another worker already has the job and this copy of the ID is skipped.
- **Transient vs permanent.** An error is transient unless the processor marks it permanent (unknown job type, malformed payload). Timeouts and ordinary failures are transient.
- **Per-attempt timeout.** Each attempt runs under a 10 s deadline. It bounds one attempt, not the job's lifetime, which can span several attempts and backoff delays. A timeout is a transient error, so the job is retried with backoff.
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

## Database design

PostgreSQL holds all durable state. The schema is built by numbered migrations in `migrations/`, which are append-only: an applied file is never edited, so each schema change is a new file.

| Migration | Change |
|---|---|
| `001_create_jobs.sql` | `jobs` table, `CHECK` on status, partial index on queued jobs |
| `002_add_retrying_status.sql` | Adds `retrying` to the status constraint, partial index on retrying jobs |
| `003_add_heartbeat.sql` | `heartbeat_at`, partial index on processing jobs |
| `004_create_charges.sql` | `charges` table (stands in for an external side effect) |
| `005_charges_unique_job.sql` | `UNIQUE (job_id)` on `charges` |
| `006_add_idempotency_key.sql` | `idempotency_key`, partial unique index |
| `007_add_created_by_request_id.sql` | `created_by_request_id` |

### `jobs`

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid` PK | Generated in Go before the insert, so the service knows it before enqueueing |
| `type` | `text` | |
| `payload` | `jsonb` | Passed through as `json.RawMessage`; only the processor for that type decodes it |
| `status` | `text` | `CHECK` in (`queued`, `processing`, `completed`, `failed`, `retrying`) |
| `attempts` | `integer` | Incremented in SQL when a worker claims the job. Doubles as the fencing token |
| `max_attempts` | `integer` | Default 3 |
| `run_at` | `timestamptz` | When the job becomes due: now, a delay, a schedule, or a retry time |
| `created_at`, `started_at`, `completed_at` | `timestamptz` | The last two are NULL until set |
| `error` | `text` | Last error; cleared on completion |
| `heartbeat_at` | `timestamptz` | Refreshed every 2 s while the attempt is running |
| `idempotency_key` | `text` | Optional |
| `created_by_request_id` | `text` | The request that created the job. Not a key, and a request could in principle create several jobs |

Design notes:

- **`status` is `text` plus a `CHECK`, not a PostgreSQL enum.** Adding `retrying` was a one-line constraint change (migration 002). Altering an enum is more awkward.
- **`timestamptz` stores an absolute instant** (UTC internally), so a session time zone cannot change which moment a value means.
- **Nullable columns map to pointers in Go** (`*time.Time`, `*string`), which pgx scans directly. A plain `time.Time` fails at runtime on NULL.

### Indexes

All four are partial, so they stay small however many completed jobs accumulate.

| Index | Serves |
|---|---|
| `(run_at) WHERE status = 'queued'` | Due scheduled jobs, orphan sweep, oldest-queued-age gauge |
| `(run_at) WHERE status = 'retrying'` | "Which retries are due?" |
| `(heartbeat_at) WHERE status = 'processing'` | "Which running jobs have gone quiet?" |
| unique `(idempotency_key) WHERE idempotency_key IS NOT NULL` | Duplicate-submission protection. Jobs without a key are unconstrained |

### `charges`

`id bigserial`, `job_id uuid UNIQUE`, `amount_cents`, `created_at`. It simulates a side effect that lives outside the job's own row, such as a payment provider. See [Failure handling and idempotency](#failure-handling-and-idempotency).

## Concurrency model

```text
main
 |-- worker 1..N   loop: Dequeue -> claim -> process -> finish
 |     \-- heartbeat goroutine (one per running job)
 |-- scheduler     loop: tick every second
 \-- HTTP server   handlers run in per-request goroutines
```

**Worker pool.** `WORKERS` (default 4) goroutines each loop on `Dequeue`. An idle worker blocks and uses no CPU. One queue delivery goes to exactly one receiver, so the queue spreads jobs across workers with no extra code. The same ID can still be delivered more than once (see below), which is why the claim is atomic.

**Nothing mutable is shared between workers.** A `Worker` holds its ID and two references. Each worker loads its own `*job.Job` from the database. The shared state is the database row, and ownership of that row is decided by SQL (see Job lifecycle).

**One attempt, step by step.**

1. Claim (`queued` to `processing`, `attempts + 1`). If it matches no row, skip.
2. Load the job.
3. Start a heartbeat goroutine on the attempt's context, which has a 10 s deadline.
4. Run the processor.
5. Cancel the context, then wait for the heartbeat goroutine to exit. Without the wait, a heartbeat write could still be in flight during the final status write.
6. Write the outcome, fenced by the attempt number.

**Heartbeat.** The goroutine ticks every 2 s while the attempt is running. If its write matches no row (`ErrNotOwner`), another attempt owns the job, and it cancels the attempt's context so the worker stops wasting work.

**Final writes use their own context.** When a deadline fires, the attempt's context is already dead. Reusing it would cancel the status write and strand the row in `processing`, so failures and retries are written with a fresh 5 s context.

**Graceful shutdown** (`SIGTERM` or Ctrl+C):

1. `signal.NotifyContext` cancels the root context.
2. Workers stop taking new jobs. A job already dequeued runs under `context.WithoutCancel`, so shutdown stops the waiting, not the working.
3. `http.Server.Shutdown` stops accepting requests and waits up to 5 s for in-flight ones.
4. `wg.Wait()` blocks until every worker and the scheduler have returned.
5. Deferred closes run last, so the database pool is closed after workers finish writing.

Observed: `docker compose stop` with a job mid-flight let it finish, wrote its final status, and exited in under 3 s.

Already-dequeued jobs are allowed to finish during shutdown. There is no separate drain deadline: `wg.Wait()` is unbounded. Each attempt is capped by its 10 s deadline, but only if the processor honors its context. A processor that ignores its context can delay termination until the container runtime kills the process (10 s by default in Docker Compose), which strands the job in `processing` until the heartbeat goes stale.

**Redis queue details.**

- `Dequeue` uses `BRPOP` with a 1 s timeout in a loop, so a cancelled context is noticed within a second.
- A worker that gets a dequeue error logs it and retries after a second. It exits only on shutdown.
- Goroutines are cheap; Redis connections are not. Each idle worker holds one blocked `BRPOP` connection. If the pool is smaller than the number of simultaneously blocked dequeuers, workers wait for connections instead of consuming jobs. The client default is `10 x GOMAXPROCS` (160 here). At 256 workers the run hung until `pool_size=400` was set. The cause (pool exhaustion plus workers exiting on dequeue errors) was inferred from the fix working; I did not capture a log of the failure.
- A better design is a few dequeuer goroutines feeding workers over an internal channel, which needs a handful of connections:

```text
Redis -> few dequeuer goroutines -> internal channel -> worker pool
```

  Not implemented.

**Race detector.** The `store`, `queue`, `api`, `metrics`, `service` and `worker` packages pass `go test -race ./...`, run inside Docker because `-race` needs cgo. The worker test runs 4 workers over 50 jobs, every ID delivered twice, and asserts each job ran exactly once. The scheduler and `cmd/server` have no tests, so their concurrency has not been checked under `-race`.

## Failure handling and idempotency

The system provides **at-least-once execution**. Exactly-once is not achievable across a crash, because a side effect can happen before the status write that records it. The strategy is to make repeats harmless.

### Failure paths

| What fails | What happens |
|---|---|
| Processor returns an error | Transient: `retrying` with backoff, until `max_attempts`, then `failed`. Permanent (marked by the processor): `failed` immediately |
| Attempt exceeds 10 s | Context deadline, a transient error |
| Worker process dies mid-job | Heartbeat goes stale (6 s). The scheduler moves the job to `retrying` (or `failed` if attempts are used up) |
| Process restarts | `retrying` and scheduled jobs live in PostgreSQL, so the new scheduler finds them. Observed: a job retried across a restart (one run) |
| A queued ID is lost (restart with the in-memory queue, crash between insert and enqueue) | The orphan sweep re-enqueues `queued` rows older than 10 s |
| Queue full | 503, and the just-inserted row is deleted |
| Redis down | `/readyz` returns 503 and `/healthz` stays 200. Workers log and retry |
| A paused worker wakes after being replaced | Its writes match no row (`ErrNotOwner`) and are discarded |

The orphan sweep may enqueue an ID that is already waiting in Redis, because the scheduler cannot tell a lost message from one that is simply queued. This is intentional: duplicate delivery is safe because a worker must claim the job atomically from PostgreSQL before processing it. The sweep only lists `queued` rows older than 10 s whose `run_at` has passed, and it does not change them, so under a backlog deeper than 10 s it re-enqueues IDs already in Redis on every tick (up to 50 per tick). That overhead was not measured.

Retrying jobs wake through the scheduler's polling rather than an in-memory timer, because a timer is lost on restart. The cost is up to one polling interval (1 s) of extra delay: a retry due in 959 ms was re-enqueued about 2 s after it failed.

### Duplicate execution, and what protects against it

| Source of duplicates | Protection | Limit |
|---|---|---|
| Same ID delivered to two workers | Atomic claim: only one `UPDATE` matches | |
| Replaced worker finishes late | Fencing on `attempts` | Protects database writes only. It cannot stop an old worker from performing an external side effect unless that system has its own idempotency or fencing |
| Retried attempt repeats a side effect | Idempotent effect (below) | Each effect must be made idempotent |
| Client retries `POST /jobs` | `Idempotency-Key` header, partial unique index, `ON CONFLICT DO NOTHING` | Only if the client sends a key |

**The double charge, reproduced.** A `payment` job inserts into `charges` and then fails once with "lost connection after charging". The retry charged again, leaving two rows for one job, while the retry, the fence and the final status all behaved correctly. Fencing cannot help, because the charge happened outside the job's row before any status write.

**The fix.** `charges.job_id` is `UNIQUE`, and the insert is `ON CONFLICT (job_id) DO NOTHING`. The job ID is stable across attempts, so attempt 2 inserts nothing and skips. The check and the insert are one atomic statement, so two racing attempts cannot both charge. A real payment provider takes an idempotency key in the request itself, which plays the same role.

**Not covered.**

- A crash between inserting a job and cleaning up after a rejected enqueue can leave a row, and the orphan sweep will then run it.
- A `503` gives the client no job ID. Without an `Idempotency-Key`, a retry creates a second job.
- Reusing an `Idempotency-Key` with a different body returns the original job silently instead of an error.
- Only the `payment` demo has an idempotent side effect. Other job types simulate work with a sleep.

## Why Redis

The queue started as a buffered Go channel. That was deliberate: it made the hand-off, blocking and backpressure visible. It has three limits once there is more than one process:

1. **It is not shared.** A job created on instance A sits in A's channel, so B's idle workers cannot see it. B would only find it through the orphan sweep, after its 10 s grace.
2. **It is volatile.** A restart drops every queued ID. Nothing is lost, since PostgreSQL still has the rows, but recovery depends on the sweep and adds latency.
3. **Backpressure is per process.** The queue-full limit applies to each instance, not to the system.

Redis fixes these by being one shared list that every instance pushes to and blocks on. It carries only job IDs, and PostgreSQL stays the source of truth. If Redis drops or duplicates a message, the orphan sweep and the atomic claim cover it.

Redis is not strictly required. PostgreSQL with `FOR UPDATE SKIP LOCKED` can serve as the queue by itself, and many systems do that. Redis buys lower hand-off latency and less polling load on the database, at the cost of another moving part.

**Measured cost** (`go test -bench`, one run, inside Docker on Windows):

| Queue | Enqueue + dequeue | Allocations |
|---|---|---|
| In-memory channel | 42.87 ns | 0 B, 0 allocs |
| Redis | 605,286 ns (about 0.6 ms) | 760 B, 23 allocs |

Redis is roughly 14,000 times slower per hand-off, because `Enqueue` makes two round trips (`LLEN` for the size limit, then `LPUSH`) and `Dequeue` makes one. Docker Desktop adds overhead, so treat the ratio as the lesson, not the absolute figure.

That is invisible for jobs lasting seconds (about 0.03% of a 1 s job). It would dominate for jobs lasting microseconds, where Redis would be the wrong choice.

Observed benefit: with 20 jobs queued and the app restarted, 12 IDs were still in Redis afterwards and the new process drained them immediately. The in-memory queue would have dropped them.

## Observability


**Structured logs** use `log/slog`. Fields are key-value pairs, so one search follows a job:

```text
level=INFO msg="job started" instance=2671253fc928 worker=2 job_id=3242b4f3-... request_id=demo-req-2 type=email attempt=1
```

Standard fields: `instance` (hostname), `worker`, `job_id`, `request_id`, `component=scheduler`. The text handler is used for readability; switching to JSON is a one-line change in `cmd/server/main.go`.

**Request IDs.** Each HTTP request gets an ID, validated before use (1 to 64 characters from `A-Za-z0-9._-`, otherwise a fresh UUID), so clients cannot inject huge or multi-line values into logs. It is returned in `X-Request-ID`, stored as `created_by_request_id`, and logged by the worker. One search by request ID shows the HTTP call and its background execution. The request ID is not the job ID: it names a request, and a request could create several jobs. This is correlation, not distributed tracing.

**Metrics** use the standard library `expvar`, served at `/debug/vars`:

| Name | Kind | Meaning |
|---|---|---|
| `jobs_started`, `jobs_completed`, `jobs_failed`, `jobs_retried`, `jobs_taken_over` | counter (per process) | Counted after the database write succeeds |
| `job_attempt_count`, `job_attempt_ms_total` | counter | Average attempt time is the ratio |
| `queue_depth` | gauge | IDs waiting in the queue |
| `jobs_in_status_{queued,processing,retrying,completed,failed}` | gauge | Row counts from PostgreSQL |
| `oldest_queued_age_seconds` | gauge | Age of the oldest due `queued` job |

Reading them:

- **Queue depth is not outstanding work.** At 20 jobs on 4 workers, `queue_depth` was 16 while `jobs_in_status_processing` was 4. Jobs held by workers have left the queue.
- **Age beats count.** During a 30-job backlog, the oldest queued age climbed 1, 4, 7, 10 s while the counts moved in steps. Age says whether workers keep up.
- **Counters are per process.** With several replicas, each has its own, and a request through the proxy returns whichever answered. Scrape each replica separately.
- `expvar` has no labels or histograms, which is the honest reason to move to Prometheus later.
- The metric names are plain strings, so a typo compiles fine. We had four such typos. `internal/metrics/metrics_test.go` now checks that the names exist.

**Health checks.** `/healthz` (liveness) answers whenever the process can. `/readyz` (readiness) pings PostgreSQL and the queue with a 2 s timeout. With Redis stopped, `/healthz` stayed 200 and `/readyz` returned 503 with `"queue":"unavailable"`. The two are separate on purpose: restarting the app does not fix a dead dependency, but sending it traffic does not help either.

**Profiling.** `net/http/pprof` listens on `127.0.0.1:6060` inside the container only, not through the proxy. Capture with `docker compose exec app wget ...`, and analyze inside the `test` container because Windows Application Control blocks `go tool pprof` on the host.


## Performance results

All runs: one Windows laptop (16 logical CPUs), Docker Desktop, one app replica, Redis queue, `notification` jobs (simulated 1 s). Load test: `go run ./cmd/loadtest`. Jobs are tracked by ID, so wait and execution times come from each job's own timestamps.

**Scaling with workers**

| Workers | Jobs | Throughput | Efficiency | Queue wait p95 | Execution p95 |
|---|---|---|---|---|---|
| 4 | 100 | 3.9 jobs/s | 98% | 23.3 s | 1.016 s |
| 16 | 100 | 13.9 jobs/s | 87% | 5.0 s | 1.008 s |
| 64 | 3000 | 59.3 jobs/s | 93% | 42.8 s | 1.053 s |
| 128 | 3000 | 122.1 jobs/s | 95% | 18.9 s | 1.006 s |
| 128 | 6000 | 119.2 jobs/s | 93% | 38.6 s | 1.081 s |
| 256 | 6000 | 234.5 jobs/s | 92% | 15.1 s | 1.139 s |
| 256 | 6000 | 230.1 jobs/s | 90% | 14.5 s | 1.059 s |

Efficiency is throughput divided by workers (the ideal is one job per worker per second). Short runs (100 jobs) are dominated by ramp-up and tail effects. The 32-worker run (24.0 jobs/s, 100 jobs) is omitted because its worker count was inferred, not logged.

What the data supports:

- **Throughput scales almost linearly up to 256 workers**, at about 90% efficiency. It is worker-bound, not API-bound.
- **The API is never the limit.** Submit p95 was 8 to 9 ms with 4 clients. Higher client counts raised it (34 to 97 ms with 10 to 50 clients), partly the test machine.
- **Backpressure works.** With a 100-ID queue, 500 submissions gave about 105 `202`s and about 395 `503`s within about 300 ms, and the system stayed up.
- **Two replicas were run once** (kill-one-instance recovery). Heartbeat reclaim and fencing recovered the dead instance's jobs. Throughput with two replicas was not measured.

What the data does not support:

- **Execution p95 is noisy.** Identical 256-worker runs gave 1.139 s and 1.059 s, and 128 workers gave 1.006 s and 1.081 s. The ranges overlap, so there is no evidence of contention starting at any worker count. The p99 of one run was 1.612 s with a 1.851 s max; the cause was not found.
- **The pgx pool was predicted to cap throughput near 16 connections. It did not.** Workers hold a connection only for brief claim and finish queries, not during the job. This holds for simulated jobs. Real jobs that hold a connection throughout would differ.

**CPU profiles** (pprof, 12 to 20 s windows):

- Idle with 256 workers: 1.4 to 2.25% CPU. About 76% of it is `Redis.Dequeue`, the parked `BRPOP` calls waking once a second.
- Under submission load: 19% CPU, about 62% in syscalls and futex (network I/O, goroutine parking), about 32% in HTTP serving. No application function was a hotspot.
- A CPU profile only shows running code, so it cannot explain the p99 tail. A block profile would be the next tool.

**Micro-benchmark:** `backoff` costs 11 ns and allocates nothing, so it is not worth optimizing.

**Load test defects found and fixed.** Three results came from bugs we then fixed, not from the system: ghost rows left by `503` responses (the row is now deleted), the worker count label printing before the environment variable was read, and a 5000-ID queue size set on a stray second `NewRedis` call so it had no effect. The README numbers above are from after those fixes.

**Open questions.** The p99 tail, the cost of the orphan sweep under deep backlogs, and throughput with several replicas.

## Running locally

_To be written._

## Testing

_To be written._

## Limitations

- Metrics are per process, and `expvar` has no histograms or labels.
- `/debug/vars` is exposed through the proxy.
- Job work is simulated with sleeps, so jobs that hold a database connection for their whole duration would behave differently.
- Request IDs give correlation across API, database and worker logs. They are not distributed tracing (no spans, timing breakdown or parent-child structure).
- Migrations are applied by hand.
- Several recorded numbers came from single runs.
- The orphan sweep duplicates enqueues under deep backlogs; the overhead is unmeasured.
- The shutdown drain is unbounded (see Concurrency model).
- The scheduler and `cmd/server` have no tests, and the scheduler's coordination across replicas has not been load tested.
- One blocked Redis connection per worker does not scale past the pool size.

## Lessons learned

_To be written._