# internal/metrics

Prometheus metrics. Each binary builds its own `Metrics` (its own `prometheus.Registry`), so there is **no global default registry**: tests are isolated and duplicate registration cannot panic.

## Pieces

| Piece | Purpose |
|---|---|
| `New()` | registry with Go runtime and process collectors plus the application metrics |
| `Handler()` | the `/metrics` handler |
| `WorkerObserver()`, `APIObserver()`, `SchedulerObserver()` | adapters that satisfy the `Observer` interfaces defined by `worker`, `api`, `scheduler`, so those packages never import Prometheus |
| `QueueCollector` | database-derived gauges, computed on scrape |

## Metrics (all prefixed `jobscheduler_`)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `queue_depth` | gauge | queue, status | jobs by status (pending, retrying, running, dead) |
| `oldest_ready_job_age_seconds` | gauge | queue | the best single "are workers keeping up?" signal |
| `dlq_size` | gauge | queue | jobs in `dead` |
| `job_pickup_latency_seconds` | histogram | queue | `run_at` to claim, measured on the database clock |
| `job_duration_seconds` | histogram | queue, type, outcome | handler time by outcome |
| `job_retries_total` | counter | queue, type | attempts scheduled for retry |
| `jobs_claimed_total`, `jobs_reaped_total`, `jobs_retention_deleted_total` | counters | | throughput, crash recoveries, sweeps |
| `jobs_submitted_total`, `submissions_rejected_total` | counters | queue, created / reason | API intake and 429 backpressure |
| `http_requests_total`, `http_request_duration_seconds` | counter, histogram | route, method, code class | status is bucketed to 2xx/3xx/4xx/5xx |
| `scheduler_is_leader`, `scheduler_fires_total` | gauge, counter | result | leadership; `duplicate_prevented` means the idempotent key caught a double-fire |

## Design points

- `QueueCollector` queries the database **lazily on scrape** with a short cache, so many scrapers cost one query. It only collects while `enabled()` is true: the scheduler enables it for the **leader only**, giving exactly one series per queue instead of one per replica.
- It emits a **zero** for every known queue and status, so alerts like `dlq_size > 0` evaluate against a real series; "absent" never means "healthy". It exports nothing until a query has succeeded rather than lie.
- HTTP status codes are bucketed to classes to bound label cardinality.
- **Fixed-label counters are created at zero in `New()`** (`jobs_reaped_total{result}`, `scheduler_fires_total{result}`). Prometheus's `rate()`/`increase()` cannot see the jump from "no series" to a first value, so a counter born at its first increment hides that event: this once made the first crash recovery on every worker invisible to `WorkerCrashesDetected`. Dynamic-label counters (retries per job type, rejections per queue) cannot be pre-created; alert on sustained rates, never on a single event. `TestFixedLabelCountersExistAtZero` guards this (mutation-tested).

## Tests

[metrics_test.go](metrics_test.go): each observer, cardinality bounds, the collector's zero-filling, caching (one query for two scrapes), disabled and failing sources.
