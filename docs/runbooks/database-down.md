# Postgres unreachable

**Alert:** `PostgresUnreachableFromAPI` (also seen as worker `claim failed`/`heartbeat failed` logs)

## What it means
Postgres is the only stateful dependency. If it is down: the API returns 503, workers cannot claim, heartbeat, or record results, and schedulers cannot fire.

## What happens to jobs
- **Nothing committed is lost.** Every state transition is a committed UPDATE.
- **Running jobs:** workers keep executing handlers but cannot record results. They retry the final write a few times; after the lease expires another worker may own the job. When the database returns, leases that expired are reclaimed by the reaper and the jobs run again (at-least-once). Handlers must be idempotent.
- **Not-yet-submitted jobs:** clients get 503 and should retry with the same `idempotency_key`.
- **Cron:** schedules whose slot passed during the outage are handled by the misfire policy once a scheduler leader exists again.

## First checks
```bash
kubectl logs deploy/<release>-api --tail=20 | grep -i 'enqueue failed\|auth lookup'
pg_isready -h <host>
```
Managed Postgres: check the provider's status, failover events, storage-full, and `max_connections` exhaustion (too many workers x `DB_MAX_CONNS`).

## Recovery
Restore Postgres (failover/restart/storage). Components reconnect on their own (pgxpool retries; schedulers re-run the election). Then:
1. Watch **Jobs reclaimed from expired leases**: expect a burst.
2. Watch **Oldest ready job age** return to normal.
3. Check the DLQ for jobs that used attempts during the outage (`lease expired` errors), and replay if needed ([dead-letter-queue.md](dead-letter-queue.md)).

## Connection exhaustion (a common cause)
Total connections = (API pods + worker pods + scheduler pods) x `DB_MAX_CONNS` (+1 per scheduler for the election). If that exceeds `max_connections`, new connections fail. Lower `DB_MAX_CONNS`, or put PgBouncer in front for the API and workers in **transaction** mode (but not for the scheduler, which needs a session for its advisory lock).
