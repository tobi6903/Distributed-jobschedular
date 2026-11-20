# internal/scheduler

Turns **cron schedules into jobs**, runs as 2+ replicas, and guarantees a schedule slot never fires twice.

## Files

| File | Contents |
|---|---|
| [cron.go](cron.go) | `ParseSpec`, `Spec.Next` (timezone-aware), `Plan` (the misfire policy logic, a pure function), `MisfirePolicy` |
| [store.go](store.go) | `Store`: schedule CRUD + `SetEnabled`, and `FireDue` (the fire transaction), `FireKey` |
| [election.go](election.go) | `Elector`: leader election with a Postgres session advisory lock |
| [scheduler.go](scheduler.go) | (see also `WithNotifier`: after a fire commits, the queues that received jobs are announced so idle workers wake at once) |
| [scheduler.go](scheduler.go) | `Scheduler`: the loop that ties election and `FireDue` together; `Observer` for metrics |

## Misfire policies

If the scheduler was down and slots were missed, `Plan` decides what to do:

| Policy | Behaviour |
|---|---|
| `fire_once` (default) | one run for the most recent missed slot, then resume |
| `skip` | fire only if the latest slot is within the grace window, otherwise wait for the next |
| `fire_all` | one run per missed slot, bounded per tick (default 1000) |

Time zones use an IANA name (`America/New_York`); `0 9 * * *` stays 09:00 local across daylight-saving changes. Zone data is embedded (`time/tzdata`) because distroless images have none. Cron parsing is `robfig/cron/v3`; the schedule maths and policies are ours.

## Never double-fires: two independent layers

1. **Row locks.** `FireDue` selects due schedules `FOR UPDATE SKIP LOCKED` and, **in one transaction**, inserts the jobs and advances `next_fire_at`. Two schedulers cannot process the same schedule.
2. **Idempotent key.** Each job gets `idempotency_key = cron:<schedule>:<slot>` (`FireKey`) under the `UNIQUE (tenant_id, idempotency_key)` constraint, so even a bypass of layer 1 (operator rewinds `next_fire_at`, restored backup, split brain) cannot create a second job.

**Leader election is an optimisation, not the guarantee.** `Elector` holds `pg_try_advisory_lock` on a **dedicated** connection (the lock is per session, so it cannot come from the pool and does not work behind a transaction-mode PgBouncer). When a session is killed Postgres frees the lock at once, a standby takes over, and the old leader only notices on its next check (about 1 s), so two replicas can briefly both believe they lead. That is exactly why safety rests on layers 1 and 2.

## Tests

[cron_test.go](cron_test.go): DST transitions, every policy, "next always advances", a pathological multi-year gap. [scheduler_test.go](scheduler_test.go) (real Postgres): CRUD, pause/resume, **6 schedulers with election disabled still produce exactly one job per slot**, the idempotent-key backstop, single-leader failover, leadership loss when the session is killed.

See: [docs/DESIGN.md](../../docs/DESIGN.md#7-scheduling-phase-4-preview) section 7, [runbook](../../docs/runbooks/scheduler-leader.md).
