# Queue backlog / jobs waiting too long

**Alerts:** `QueueOldestJobTooOld`, `QueueOldestJobCritical`, `JobPickupSLOFastBurn`, `JobPickupSLOSlowBurn`, `NoWorkersUp`

## What it means
Runnable jobs are waiting longer than they should. Either workers are not running, are too few, or are busy with slow jobs. Jobs are not lost: they are durable in Postgres and will run when capacity returns.

## First checks (2 minutes)
1. Dashboard "Job Scheduler": **Oldest ready job age**, **Queue depth**, **Throughput by outcome**.
2. Are workers up? `kubectl get pods -l app.kubernetes.io/component=worker` (or `docker compose ps worker`). `sum(up{job="worker"})`.
3. Is KEDA scaling? `kubectl get scaledobject,hpa`. At `maxReplicaCount`?

## Diagnose
```sql
-- What is waiting, by queue and type, and for how long?
SELECT queue, type, count(*) AS waiting, now() - min(run_at) AS oldest
FROM jobs WHERE status IN ('pending','retrying') AND run_at <= now()
GROUP BY 1,2 ORDER BY waiting DESC;

-- What is occupying workers? (long-running jobs hogging concurrency)
SELECT type, count(*) AS running, max(now() - updated_at) AS since_last_heartbeat
FROM jobs WHERE status = 'running' GROUP BY 1 ORDER BY running DESC;
```
- **Workers all busy, backlog growing** -> capacity problem: scale out.
- **Workers idle but backlog exists** -> they cannot claim: check worker logs for `claim failed` (Postgres connectivity, pool exhaustion), and that the queue name is in `WORKER_QUEUES`.
- **Backlog concentrated in one type** -> a slow or hung handler. Check **Handler duration p95** and per-job timeouts.
- **One huge batch just arrived** -> a burst; confirm it is draining.

## Mitigate
- Scale workers: `kubectl scale deploy/<release>-worker --replicas=N` (KEDA will take it back on its next evaluation; raise `keda.maxReplicaCount` in values for a permanent change).
- Raise `WORKER_CONCURRENCY` if CPU and memory are idle (handlers are I/O bound).
- Do **not** raise concurrency so far that total connections exceed Postgres `max_connections` (workers x `DB_MAX_CONNS`). Check the database before scaling past ~50 workers (see OPERATIONS.md "What breaks first").
- A hung handler: its per-job timeout will cancel it. If a type has no sensible timeout, set `timeout_seconds` on submit.

## Follow up
Was it capacity (adjust KEDA target) or a slow job type (add a dedicated queue)? File the change; consider isolating that type on its own queue so it cannot starve others.

## If the cause is limits (throttling, backpressure) rather than capacity

`SubmissionsRejectedByBackpressure`, or a backlog that is large while workers sit idle, can be **deliberate**: per-tenant limits are doing their job. Check the rules first:

```bash
jobctl limits list        # needs DATABASE_URL
```
```sql
SELECT queue, tenant_id, max_running, rate_per_second, burst, max_backlog FROM queue_limits ORDER BY 1,2;
```

- **429s with a queue that is draining normally** -> a tenant's `max_backlog` is lower than its normal burst. Raise it (`jobctl limits set -queue Q -tenant T -max-backlog N`; applies within about 2s) or have the producer honour `Retry-After`.
- **Workers idle, backlog high, one tenant dominating** -> its `max_running` or `rate_per_second` is the bottleneck by design. Confirm the downstream it protects can take more, then raise it.
- **Remove a limit** with `jobctl limits delete -queue Q -tenant T`.
