# internal/queue

The **Postgres-backed job store**: every durable state transition in the system is a method here, and each is a single SQL statement or one short transaction. There is no in-memory state to lose.

## Files

| File | Contents |
|---|---|
| [store.go](store.go) | `Store`: `Enqueue`, `Get`, `Claim`, `Heartbeat`, `Complete`, `CompleteBatch`, `Fail`, `Release`, `Reap`, `Replay`, `DeleteFinished`, `Depth`, `OldestReady` |
| [limits.go](limits.go) | Per-tenant limits: `SetLimit`, `DeleteLimit`, `ListLimits`, `Admit` (backpressure), the governed claim path, rule and backlog caches |
| [apikeys.go](apikeys.go) | `CreateAPIKey`, `TenantForKey` (keys are 256-bit random values stored as SHA-256 hashes) |
| [pool.go](pool.go) | `Connect`: opens a `pgxpool` and waits for the database to accept connections |

## The claim

`claimSQLFor` is the heart of the system:

```sql
WITH picked AS (
  SELECT id FROM jobs
  WHERE queue = $1 AND status IN ('pending','retrying') AND run_at <= now()
  ORDER BY priority DESC, run_at, id
  LIMIT $2
  FOR UPDATE SKIP LOCKED
)
UPDATE jobs SET status='running', locked_by=$3, locked_until=now()+lease, attempts=attempts+1
FROM picked WHERE jobs.id = picked.id RETURNING ...
```

`SKIP LOCKED` gives concurrent workers disjoint batches without blocking. The row lock lives only for this one statement; the durable claim is `status='running'` plus the lease columns, so no transaction is held open while a job runs.

## Invariants

- **Batched completion.** `CompleteBatch` completes many jobs in one statement (`UPDATE ... FROM unnest(...)`), applying the same fence per row and returning which refs were completed; see [DESIGN.md section 19](../../docs/DESIGN.md#19-completion-batching-group-commit).
- **Fencing.** `Heartbeat`, `Complete`, `Fail` and `Release` all use `WHERE id AND status='running' AND locked_by AND attempts`. Zero rows affected becomes `ErrLeaseLost`: a worker that lost its lease can never overwrite the current owner's result.
- **`attempts` is counted at claim time**, so a job that crashes its worker still burns an attempt. `Release` refunds it (an interrupted deploy is not the job's fault).
- **All time comes from the database** (`now()`), including delays and lease deadlines.
- **`Reap`** returns expired leases to `retrying` (or `dead` when out of attempts) and is safe to run from every worker at once.

## Limits (see DESIGN.md section 18)

A queue with no limiting rule uses the plain claim. A queue with rules splits tenants into disjoint groups; a limited group claims inside a transaction holding `SELECT ... FOR UPDATE SKIP LOCKED` on its rule row, which makes the concurrency cap and the token bucket exact. **Never weaken that lock**: `TestConcurrencyCapIsExactUnderContention` and `TestRateLimitHoldsAcrossConcurrentClaimers` guard it (and were mutation-tested).

`Admit` implements backpressure: it returns `ErrBacklogFull` when a scope's pending backlog has reached `max_backlog`. Counts are cached for 1 s (approximate by design).

## Errors

`ErrNotFound`, `ErrLeaseLost`, `ErrNotReplayable`, `ErrUnauthorized`, `ErrBacklogFull`, `ErrInvalidLimit`, `ErrLimitNotFound`. Check them with `errors.Is`.

## Tests

Integration tests against a real Postgres ([store_test.go](store_test.go), [limits_test.go](limits_test.go)): claim exclusivity under 10 concurrent claimers, fencing, lease expiry and reclaim, idempotent submit (including 20 concurrent enqueues), priority ordering, every limit behaviour. Run via `make test` (see [../testdb](../testdb/README.md)).
