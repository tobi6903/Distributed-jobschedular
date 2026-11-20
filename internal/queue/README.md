# internal/queue

The **Postgres-backed job store**: every durable state transition in the system is a method here, and each is a single SQL statement or one short transaction. There is no in-memory state to lose.

## Files

| File | Contents |
|---|---|
| [store.go](store.go) | `Store`: `Enqueue`, `Get`, `Claim`, `Heartbeat`, `Complete`, `CompleteBatch`, `Fail`, `Release`, `Reap`, `Replay`, `DeleteFinished`, `Depth`, `OldestReady` |
| [limits.go](limits.go) | Per-tenant limits: `SetLimit`, `DeleteLimit`, `ListLimits`, `Admit` (backpressure), the governed claim path, rule and backlog caches |
| [apikeys.go](apikeys.go) | `CreateAPIKey`, `TenantForKey` (keys are 256-bit random values stored as SHA-256 hashes) |
| [notify.go](notify.go) | `Notifier` (publishes "queue X has runnable work", coalesced: one per queue per window) and `Listener` (a dedicated `LISTEN` connection that reconnects and turns hints into a wake-up signal); see [DESIGN.md section 21](../../docs/DESIGN.md#21-wake-up-notifications-listennotify) |
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

## Wake-up notifications

`Store.SetNotifier` makes `Enqueue` and `Replay` announce a job that is runnable now, after the row has committed. A notification is **only a hint**: it wakes an idle worker earlier than its next poll, and the worker still claims with the same `FOR UPDATE SKIP LOCKED` query. Hints are lost when nobody listens, when a connection drops, or when a process dies between commit and notify, so polling is never removed. Jobs that are not due yet (delays, retries) are not announced. `Notifier` sends at most one `pg_notify` per queue per window from its own statement, because `NOTIFY` takes a lock at commit that serializes every transaction that sends one. See [DESIGN.md section 21](../../docs/DESIGN.md#21-wake-up-notifications-listennotify).

## Invariants

- **Plan cache mode.** `Connect` runs every pooled connection with `plan_cache_mode=force_custom_plan`; a generic plan cached while `jobs` was empty made a 60,000-job burst 2 to 3 times slower on fresh workers. See [DESIGN.md section 20](../../docs/DESIGN.md#20-the-cached-plan-trap-found-by-investigating-uneven-burst-claims).
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
