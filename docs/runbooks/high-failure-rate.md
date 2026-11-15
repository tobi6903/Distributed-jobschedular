# High retry or failure rate

**Alerts:** `HighRetryRate`, `JobSuccessSLOFastBurn`

## What it means
A large share of attempts fail. Retries are absorbing it for now (that is what they are for), but if it continues, jobs will dead-letter and burn the success SLO.

## First checks
- Dashboard: **Retries per second by type** (which type?), **Throughput by outcome**.
- Worker logs for that type: `kubectl logs -l app.kubernetes.io/component=worker | grep '"type":"<type>"' | grep -i fail`. Each log line has `job_id`, `tenant_id`, `trace_id`.
- Grafana Explore (Loki): `{service="worker"} | json | outcome=~"retry|dead|failed" | type="<type>"`. Click a `trace_id` to see the trace.

## Diagnose
```sql
SELECT left(last_error, 150) AS error, count(*)
FROM jobs WHERE status='retrying' GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
```
- **Downstream dependency erroring** -> fix or wait; backoff with jitter prevents us from hammering it. Consider pausing submission upstream.
- **Started right after a deploy** -> roll back.
- **Timeouts** -> dependency slow; raise `timeout_seconds` or reduce concurrency.

## Mitigate
- Roll back a bad deploy.
- Reduce load on a struggling dependency: lower `WORKER_CONCURRENCY` or scale workers down. Retries back off exponentially (base 5s, cap 1h, jittered), so the load decays on its own.
- If a type is entirely broken and jobs are better held than failed, you can push their `run_at` into the future: `UPDATE jobs SET run_at = now() + interval '30 min' WHERE type='x' AND status IN ('pending','retrying');`

## Follow up
Is the backoff base or `max_attempts` right for that dependency's typical outage length? If outages last an hour but jobs die after 5 attempts in 2 minutes, raise `max_attempts` for that type at submit time.
