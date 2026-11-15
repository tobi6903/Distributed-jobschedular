# migrations

Versioned SQL, applied by [internal/migrate](../internal/migrate/README.md) with `golang-migrate`. The files are compiled into the binaries (`embed.go`), so nothing needs to be mounted at runtime. **Every `up` has a `down`, and the pair is tested** (`TestMigrationsUpDownUp` applies, fully rolls back, and re-applies).

| # | Migration | What it does |
|---|---|---|
| 1 | `create_jobs` | `jobs` (state, lease, idempotency key, `lease_iff_running` CHECK) with the partial indexes for claiming, lease expiry and retention; `api_keys` |
| 2 | `job_timeout` | `jobs.timeout_seconds`: per-job timeout |
| 3 | `schedules` | cron schedules: expression, IANA timezone, misfire policy, job template, `next_fire_at` |
| 4 | `trace_context` | `jobs.traceparent`: lets a worker continue the trace that submitted the job |
| 5 | `dead_index` | partial index on dead jobs, so the DLQ-size metric does not scan finished rows |
| 6 | `autovacuum_tuning` | aggressive per-table autovacuum on `jobs` (it is high-churn: insert, claim, complete, heartbeats) |
| 7 | `queue_limits` | per-tenant concurrency / rate / backlog limits and the index that makes counting running jobs cheap |

## Conventions

- **Backward compatible with the previous app version.** Rolling deploys run old and new code side by side: add columns and tables in one release, start using them in the next, drop in a third (expand/contract).
- Status is `text` with a `CHECK`, not an enum: `ALTER TYPE ... ADD VALUE` is awkward in migrations and a `CHECK` is trivial to change.
- Constraints encode invariants the application must not violate (a `running` job must hold a lease; idempotency keys are unique per tenant).
- Indexes are **partial** wherever the hot query only touches a subset (claimable rows, running rows, dead rows), so they stay small however many finished rows exist.

See [docs/DESIGN.md](../docs/DESIGN.md#4-schema) section 4 for the schema reasoning.
