# Scheduler leadership problems

**Alerts:** `NoSchedulerLeader`, `MultipleSchedulerLeaders`, `CronDuplicatePrevented`

## Background
Schedulers run as 2+ replicas. One holds a Postgres session advisory lock and fires due cron schedules; the others wait. If the leader dies, Postgres frees the lock and a standby takes over within `SCHEDULER_ELECTION_RETRY` (default 5s). Missed slots are handled by each schedule's misfire policy (`fire_once`, `skip`, `fire_all`).

Safety does **not** depend on the election: each fire is also protected by row locks (`FOR UPDATE SKIP LOCKED`) and a unique idempotency key `cron:<schedule>:<slot>`.

## `NoSchedulerLeader`: cron is not firing
```bash
kubectl get pods -l app.kubernetes.io/component=scheduler
kubectl logs -l app.kubernetes.io/component=scheduler --tail=50 | grep -E 'election|leader|connect'
```
```sql
-- Who holds the lock? (empty = nobody)
SELECT l.pid, a.application_name, a.client_addr, a.state, a.backend_start
FROM pg_locks l JOIN pg_stat_activity a USING (pid)
WHERE l.locktype='advisory' AND l.classid = 1785684595;
```
- **No scheduler pods running** -> start them / check the rollout.
- **Pods run but cannot connect** -> `election: cannot connect`: DSN, NetworkPolicy, or Postgres down.
- **Behind a transaction-mode PgBouncer** -> advisory locks are session-scoped and do not work through it. Point `DATABASE_URL` for the scheduler at Postgres directly or at a session-mode pooler.

**Recovery is automatic once a leader exists.** Slots missed meanwhile are handled by the misfire policy. Check `SELECT name, next_fire_at, last_fired_at FROM schedules WHERE enabled AND next_fire_at < now();`: it should drain within seconds.

## `MultipleSchedulerLeaders`
A brief overlap (< 5s) during failover is expected: a killed session frees the lock instantly while the old leader notices on its next check (about 1s). A **sustained** overlap means two schedulers hold different locks, which should be impossible on one database. Check they use the same `DATABASE_URL` (a stale replica pointing at another database, or a restored copy). Double-fires are still blocked by idempotency keys (see below), but fix the overlap.

## `CronDuplicatePrevented`
The idempotency key stopped a second insert for the same slot, so the row-lock layer was bypassed, for example by an operator resetting `next_fire_at` or restoring a backup. No duplicate job ran. Find what rewound `next_fire_at`; no action is needed beyond that.
