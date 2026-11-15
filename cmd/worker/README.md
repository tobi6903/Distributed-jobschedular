# cmd/worker

The job-executing process. `main` wires config, the built-in demo handlers, metrics and the ops endpoint, then calls `worker.Run`. The behaviour lives in [internal/worker](../../internal/worker/README.md).

## Run

```bash
DATABASE_URL=postgres://... WORKER_CONCURRENCY=20 ./bin/worker
```

Key variables (full table: [docs/OPERATIONS.md](../../docs/OPERATIONS.md#components-and-configuration)): `WORKER_QUEUES` (default `default`), `WORKER_CONCURRENCY` (10), `WORKER_LEASE` (30s; heartbeat is lease/3), `WORKER_DEFAULT_TIMEOUT` (5m), `WORKER_SHUTDOWN_GRACE` (30s), `WORKER_REAP_INTERVAL` (10s), `RETENTION` (168h), `BACKOFF_BASE`/`BACKOFF_CAP`, `DB_MAX_CONNS` (Compose/Helm: 20).

## Ops endpoint (`METRICS_ADDR`, default `:9100`)

- `GET /metrics`: Prometheus.
- `GET /healthz`: liveness. It only checks that the process is up; if it also checked Postgres, a database blip would restart every worker at once and kill in-flight jobs.

## Shutdown

SIGTERM cancels the context passed to `Run`, which stops claiming and drains in-flight jobs for up to `WORKER_SHUTDOWN_GRACE` (details in the package README). Kubernetes must allow longer than that: the Helm chart sets `terminationGracePeriodSeconds = worker.maxJobTimeoutSeconds + 15`.

## Adding your own handlers

This binary registers only the demo handlers from [internal/handlers](../../internal/handlers/README.md). For real work, build your own `main` that calls `Registry.Register` for your job types and reuse everything else.
