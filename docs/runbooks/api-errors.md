# API errors or latency

**Alerts:** `APIAvailabilitySLOFastBurn`, `APIHighLatency`

## What it means
The API is returning 5xx or `POST /jobs` is slow. 4xx responses never trigger these alerts: they are client mistakes.

## First checks
- Dashboard **API** row: request rate by code, p50/p99 of `POST /jobs`.
- Pod health: `kubectl get pods -l app.kubernetes.io/component=api`; `kubectl top pods`.
- `curl -s $API/readyz`: `database unavailable` means the API cannot reach Postgres.

## Diagnose
- **All 5xx with `database unavailable`** -> [database-down.md](database-down.md).
- **Latency up, errors flat** -> Postgres is slow (enqueue is one INSERT). Check lock waits and connection saturation:
  ```sql
  SELECT state, wait_event_type, wait_event, count(*) FROM pg_stat_activity GROUP BY 1,2,3 ORDER BY 4 DESC;
  ```
  Pool exhaustion shows as many `Client: ClientRead` / `idle in transaction` and request latency growing with load: raise `DB_MAX_CONNS` (within `max_connections`) or add API replicas (HPA does this on CPU).
- **A deploy just happened** -> roll back.
- **One tenant hammering** -> identify from logs (`tenant_id` field). Limit what it can queue with `jobctl limits set -queue Q -tenant T -max-backlog N` (it then gets 429 + `Retry-After`); the API has no request-rate limit, so cap request rate at the ingress if needed.

## Mitigate
Scale the API (`kubectl scale deploy/<release>-api --replicas=N`), roll back, or shed load at the ingress. Submitting clients should retry with the same `idempotency_key`, so a retry never duplicates a job.
