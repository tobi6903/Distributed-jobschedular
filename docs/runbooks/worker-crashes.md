# Worker crashes (jobs reclaimed from expired leases)

**Alert:** `WorkerCrashesDetected`

## What it means
`jobscheduler_jobs_reaped_total` increased: a job's lease (`locked_until`) expired without a heartbeat, and the reaper put it back in the queue. Causes: the worker was OOM-killed or crashed, the pod was evicted, the node died, the worker lost connectivity to Postgres for longer than the lease (default 30s), or the handler blocked the whole process.

**Impact:** those jobs run again (at-least-once). Handlers must be idempotent. If a handler had already caused a side effect before the crash, it can happen twice.

## First checks
```bash
kubectl get pods -l app.kubernetes.io/component=worker      # RESTARTS column
kubectl describe pod <pod> | grep -A3 'Last State'          # OOMKilled? Evicted?
kubectl get events --sort-by=.lastTimestamp | tail -20
```
```sql
-- Which jobs were reclaimed, and are any repeatedly crashing their worker?
SELECT id, type, attempts, max_attempts, last_error
FROM jobs WHERE last_error LIKE 'lease expired%' ORDER BY updated_at DESC LIMIT 20;
```

## Diagnose
- **OOMKilled** -> a handler uses more memory than the limit. Often a single poison job: look at `attempts` close to `max_attempts` for the same job id. Raise the memory limit or fix the handler.
- **Evicted / node drain** -> expected during maintenance; confirm the PodDisruptionBudget is honoured and `terminationGracePeriodSeconds` exceeds job timeouts so jobs drain instead of being killed.
- **Many pods at once** -> Postgres or the network. Check [database-down.md](database-down.md). Workers that lose the database stop heartbeating, so their leases expire even though the process is fine: it is *correct* behaviour, as another worker may now own the job.
- **No restarts but leases expire** -> heartbeat starvation: CPU limits too low (throttling) or the pool exhausted. Check `container_cpu_cfs_throttled_seconds_total` and `DB_MAX_CONNS`.

## Mitigate
- Poison job: it will dead-letter after `max_attempts` crashes by itself (attempts count at claim time). To stop it sooner: `UPDATE jobs SET status='dead', finished_at=now(), locked_by=NULL, locked_until=NULL, last_error='manually killed' WHERE id='...';`
- Raise the lease if jobs legitimately block the process for long: `WORKER_LEASE` (heartbeat is lease/3).

## Follow up
Make the offending handler idempotent and memory-bounded; add a per-job `timeout_seconds`.
