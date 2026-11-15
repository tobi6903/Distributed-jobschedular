# Dead-letter queue (DLQ) not empty or growing

**Alerts:** `DeadLetterQueueNotEmpty`, `DeadLetterQueueGrowing`

## What it means
Jobs used all `max_attempts` and were moved to `dead`, or failed permanently (`failed`). They will not run again until someone replays them. Nothing is lost: the payload and `last_error` are kept (until the retention period, default 7 days, after which finished rows are deleted).

## First checks
```sql
SELECT type, queue, count(*), min(finished_at) AS first, max(finished_at) AS last
FROM jobs WHERE status IN ('dead','failed') GROUP BY 1,2 ORDER BY 3 DESC;

-- Why? group by error
SELECT type, left(last_error, 120) AS error, count(*)
FROM jobs WHERE status IN ('dead','failed') GROUP BY 1,2 ORDER BY 3 DESC LIMIT 20;
```

## Diagnose by pattern
- **All one type, same error, started at one time** -> a bad deploy or a dependency outage that began then. Check the deploy history and the dependency.
- **`no handler registered for job type`** -> `failed` by design: a worker without that handler claimed it (version skew during a rolling deploy, or a typo in `type`). Finish the rollout, then replay.
- **`invalid payload`** -> producer bug (permanent). Fix the producer; replaying will fail again until payloads are corrected.
- **`lease expired (worker crashed or stalled)`** -> the job repeatedly crashed its worker (OOM?). See [worker-crashes.md](worker-crashes.md). This is the poison-pill case: it stopped after `max_attempts` rather than crash-looping forever.
- **`job timed out`** -> handler too slow or hung; raise `timeout_seconds` or fix the handler.

## Mitigate: replay after the cause is fixed
Replay one: `jobctl replay <job-id>` or `POST /jobs/{id}/replay`. Replay resets `attempts` to 0 and re-queues immediately.

Replay many (operator, directly in SQL, same effect as the API):
```sql
UPDATE jobs SET status='pending', attempts=0, run_at=now(), finished_at=NULL, updated_at=now()
WHERE status='dead' AND type='email.send' AND finished_at > now() - interval '2 hours';
```
Replay in batches if the downstream was the problem, so you do not recreate the outage.

## Follow up
Add an alert-specific note to the owning team. If the job is not worth keeping, let retention delete it.
