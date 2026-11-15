# Operations Guide

How to run, size, scale, and debug the scheduler, and an honest account of **what breaks first**.

## Components and configuration

| Binary | Role | Scale |
|---|---|---|
| `api` | HTTP API: submit, status, replay, schedules | Horizontally, stateless (HPA on CPU) |
| `worker` | Claims and executes jobs; also runs the lease reaper and the retention sweeper | Horizontally (KEDA on backlog) |
| `scheduler` | Fires cron schedules | 2+ replicas for HA; **one leads**, the rest are standbys |
| `jobctl` | Migrations, API keys, CLI | One-shot (migration Job) |

All configuration is environment variables, validated at startup (all errors reported at once).

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | required | Postgres connection string |
| `DB_MAX_CONNS` | 10 | Pool size **per process**. Compose/Helm set API 40, worker 20, scheduler 5 (see sizing below) |
| `LOG_LEVEL` | info | debug, info, warn, error |
| `HTTP_ADDR` (api) | `:8080` | |
| `SHUTDOWN_DELAY` / `SHUTDOWN_TIMEOUT` (api) | 5s / 20s | Stay unready, then drain connections |
| `METRICS_ADDR` | `:9100` worker, `:9101` scheduler | `/metrics` and `/healthz` |
| `WORKER_QUEUES` | `default` | Comma list, polled in order (strict queue priority) |
| `WORKER_CONCURRENCY` | 10 | Jobs executing at once per worker |
| `WORKER_BATCH_SIZE` | = concurrency | Jobs claimed per round trip |
| `WORKER_POLL_INTERVAL` | 1s | Idle sleep (jittered +/-20%). This bounds pickup latency when idle. |
| `WORKER_LEASE` | 30s | Visibility timeout. Heartbeat is lease/3. |
| `WORKER_DEFAULT_TIMEOUT` | 5m | Per-job timeout when the job sets none |
| `WORKER_SHUTDOWN_GRACE` | 30s | Drain time on SIGTERM before cancelling stragglers |
| `WORKER_REAP_INTERVAL` | 10s | How often expired leases are recovered |
| `RETENTION` | 168h | Finished jobs are deleted after this (also the idempotency-key dedup window) |
| `BACKOFF_BASE` / `BACKOFF_CAP` | 5s / 1h | Retry delay: `min(cap, base*2^(n-1))` with equal jitter |
| `SCHEDULER_TICK` | 1s | How often the leader looks for due schedules |
| `SCHEDULER_ELECTION_RETRY` | 5s | Standby retry interval (about the worst-case cron failover time) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | Enables trace export when set |

### The shutdown contract (get this right)

`terminationGracePeriodSeconds` must be **greater than** `WORKER_SHUTDOWN_GRACE`, which should be **at least the longest job timeout you allow**. The Helm chart derives all three from `worker.maxJobTimeoutSeconds` (grace period = max timeout + 15s). If you set the Kubernetes value lower, the kubelet SIGKILLs a draining worker: its jobs are not lost (leases expire and the reaper re-queues them) but they run again and consume an attempt.

## Sizing

### Database connections: the first thing to budget

```
total connections = (api pods + worker pods + scheduler pods) x their DB_MAX_CONNS  +  1 per scheduler (election session)
```

This must stay under Postgres `max_connections` (minus headroom for migrations, `psql`, monitoring). The shipped defaults are **per component** (Compose and the Helm chart): API 40, worker 20, scheduler 5 (the process default, for anything else, is 10). With 2 API pods, 2 workers and 2 schedulers that is 80 + 40 + 12 = 132 connections, within the Compose Postgres's `max_connections=200`. With 40 workers and 10 API pods it is 1,200: above any default Postgres. Options, in order of preference: lower the pool sizes (but see the next paragraph before cutting them), raise `max_connections` with enough RAM, or put **PgBouncer in transaction mode** in front of the API and workers. **Never put the scheduler behind a transaction-mode pooler**: its advisory-lock leader election needs a real session.

**The pool is also a throughput cap, so do not size it only to save connections.** A request holds a connection for the whole of its durable commit, so a pod can do at most about `pool size / commit latency` requests per second. **It only bites when the database is busy.** Measured on the Compose stack by `scripts/perf-collect.sh` (400 req/s for 20 s, k6; see the chart in the README):

| API `DB_MAX_CONNS` | idle database: p95 | database under commit pressure*: p95 | achieved req/s under pressure | iterations dropped |
|---|---|---|---|---|
| 10 | 17 ms | **1,637 ms** | 395 | 1,163 |
| 40 (shipped default) | 18 ms | **146 ms** | 463 (1.2% of requests failed) | 117 |

\* a 60,000-job backlog draining on 8 workers throughout, so every completion is a commit competing with the API's.

On a quiet database both pool sizes are identical, which is why this is easy to miss in a light test and expensive to discover in production. Size the pool for the *busy* case.

The same applies to workers, where every finished job needs a connection to record its result: a worker with a small pool serialises completions and cannot use its concurrency slots. Under pressure Postgres showed 16 backends waiting on `LWLock:WALWrite`, the group-commit ceiling described under "What breaks first". The real limit is the database; the pool must simply not be the *first* limit you hit.

### Worker concurrency and replicas

Handlers are usually I/O bound, so `WORKER_CONCURRENCY` can be much higher than CPU count. Size replicas so that `replicas x concurrency` covers your peak arrival rate times mean job duration (Little's law: in-flight = rate x duration). Example: 200 jobs/s of 2s jobs needs 400 slots, i.e. 20 workers at concurrency 20. KEDA's `jobsPerReplica` target (default 100) should be roughly `concurrency x (seconds of backlog you tolerate) / mean job seconds`.

### Lease length

Longer leases mean slower crash recovery (a dead worker's jobs wait up to lease + reap interval); shorter leases mean more heartbeat traffic and more false reclaims under GC pauses or CPU throttling. 30s is a sane default. Keep `heartbeat <= lease/3` (enforced at startup).

## What breaks first

In the order you will hit them as load grows. The measured numbers come from the README's Performance section (`scripts/perf-collect.sh`, one 12-CPU WSL2 box, durable Postgres); they vary between runs.

1. **Postgres commit rate (about 1,000 to 3,300 jobs/s on that box, peaking at 4 worker containers and then falling).** Each job costs three durable writes: INSERT, claim UPDATE (batched), completion UPDATE (one per job). Completion is the dominant cost: throughput tracked the commit rate, and adding workers past 4 gave diminishing returns. **Mitigations, in order:** batch completions (collect finished jobs and `UPDATE ... WHERE id = ANY(...)` once per tick, not implemented to keep fencing simple); faster storage and a bigger instance; `synchronous_commit=off` for this database if you can tolerate losing the last few ms of commits (acceptable here because loss of a *completion* only causes a re-run, never a lost job, but a lost *enqueue* would be a lost job: use it only after weighing that); then sharding queues or tenants across databases.
2. **Dead tuples and vacuum.** Every job produces about 3 row versions, plus one per heartbeat. `status` and `locked_until` are in index predicates or keys, so these updates are not HOT. If autovacuum falls behind, the partial claim index fills with dead entries and claim latency climbs. Migration `000006` already makes autovacuum aggressive on `jobs`. Watch `n_dead_tup` and `last_autovacuum`; keep `RETENTION` short. This is the failure that looks like "the queue got slow for no reason".
3. **Connections** (see sizing). Symptoms: `too many clients`, API 503s, workers logging `claim failed`.
4. **Claim contention.** `SKIP LOCKED` avoids blocking, but every worker scans the head of the same index. Batch claiming amortises round trips. At very high worker counts, split queues (each queue has its own index range) or tenants across databases.
5. **Poll latency vs idle load.** Pickup latency when idle is up to `WORKER_POLL_INTERVAL`. Lowering it raises idle query load (workers x 1/interval). `LISTEN/NOTIFY` as a wake-up hint (polling stays the source of truth) is the standard fix and a natural next step.
6. **Strict priority starvation.** Higher priority always runs first, so a steady stream of high-priority jobs starves low-priority ones. Priority aging is not implemented.
7. **Table size.** Retention keeps it bounded. If you need long retention, partition by `finished_at` (note the cost: unique idempotency keys across partitions, and `SKIP LOCKED` on a partitioned table needs care).

### Limits of the design (by choice)

- Throughput is bounded by **one Postgres primary**. This is the price of the simplicity and the transactional guarantees. If you need hundreds of thousands of jobs/s, use a log-based system (Kafka) and accept losing queryable state.
- **At-least-once only.** Duplicate execution is possible after crashes and lost leases; handlers must be idempotent.
- Handlers are **in-process Go functions**. There is no sandboxing; a handler can OOM its worker (the attempt budget stops a poison job from crash-looping).
- The advisory-lock election needs a **session-mode** connection.
- **Fairness is per-queue rules, not weighted shares.** Per-tenant concurrency caps, dispatch rate limits and submit backpressure exist (see "Limits and backpressure"), but groups get an equal chance, not configurable weights, and the API itself has no request-rate limit.

## Day-2 operations

### Deploying

Migrations run first (Helm hook Job; `PreSync` under Argo CD) and must be **backward compatible with the previous app version** (rolling deploys run old and new code side by side): add columns and tables in one release, start using them in the next, drop in a third (expand/contract). The migration Job failing blocks the upgrade, by design.

Rollback is `git revert` of the tag commit (Argo CD) or `helm rollback`. Down migrations exist and are tested (`TestMigrationsUpDownUp`), but rolling the schema back under load is rarely what you want; prefer rolling forward with a fix.

During a rolling worker deploy, a new job `type` can briefly be claimed by an old worker that does not know it. That job goes to `failed` ("no handler registered") and is replayable. If you add job types often, deploy workers before producers.

### Backups and restore

Postgres is the only state; back it up with your normal tooling (base backups plus WAL for point-in-time recovery). After a restore to an earlier point: jobs finished after the restore point run again (at-least-once; idempotent handlers make this safe), cron `next_fire_at` is restored with the data, and `fire_all`/`fire_once` schedules catch up per their policy. The idempotent fire key stops any slot fired both before and after the restore point from creating a second job, as long as the earlier job row still exists.

### Everyday queries

```sql
-- Backlog and oldest waiter per queue
SELECT queue, count(*), now() - min(run_at) AS oldest FROM jobs
WHERE status IN ('pending','retrying') AND run_at <= now() GROUP BY 1;

-- What is running, and for how long?
SELECT id, type, locked_by, attempts, now() - updated_at AS since_heartbeat FROM jobs WHERE status='running' ORDER BY updated_at;

-- Dead jobs by error
SELECT type, left(last_error,100), count(*) FROM jobs WHERE status IN ('dead','failed') GROUP BY 1,2 ORDER BY 3 DESC;

-- Vacuum health
SELECT n_live_tup, n_dead_tup, last_autovacuum FROM pg_stat_user_tables WHERE relname='jobs';
```

### Security notes

API keys are 256-bit random values stored as SHA-256 hashes; the plaintext is shown once. The tenant always comes from the key. Pods run non-root with read-only filesystems, dropped capabilities, no service-account token, and default-deny NetworkPolicies. Submission payloads are capped at 1 MiB. There is no API request-rate limit and no key expiry yet (queue backlog limits bound queue growth, see below). Rotate by creating a new key and revoking the old (`UPDATE api_keys SET revoked_at = now() WHERE ...`).

## Failure matrix

| Failure | What happens | Recovery |
|---|---|---|
| Worker crash / OOM / node loss | Its jobs wait out the lease, then another worker's reaper re-queues them. Attempt consumed. | Automatic (lease + reap interval, about 30-40s by default) |
| Worker paused, then resumes after losing its lease | Heartbeat gets "lease lost", the handler context is cancelled, its result is discarded | Automatic; the new owner's result stands |
| Handler hangs | Per-job timeout cancels its context, then retry | Automatic |
| Poison job that kills the process | Stops after `max_attempts`, then `dead` | Fix the handler, replay |
| Postgres down | API 503; workers cannot claim or record; running handlers continue but results may be lost to lease expiry | Restore Postgres; jobs re-run (see [database-down.md](runbooks/database-down.md)) |
| Scheduler leader dies | A standby takes over within the election retry interval; missed slots follow the misfire policy | Automatic |
| Two schedulers overlap | Row locks and idempotent keys still give one job per slot | Automatic |
| Rolling deploy | SIGTERM drains jobs; API drains via readiness | Automatic |
| Clock skew between machines | No effect: all comparisons use the database clock | n/a |

## Metrics caveats

Found by looking at real dashboards during chaos runs:

- **Create fixed-label counters at zero.** `rate()` and `increase()` cannot see the jump from "series does not exist" to its first value. A counter born at its first increment hides that first event: this once made the first crash recovery on every worker invisible, so `WorkerCrashesDetected` would have missed it. `metrics.New()` now pre-creates `jobs_reaped_total` and `scheduler_fires_total`. Counters with **dynamic labels** (retries per job type, rejections per queue) cannot be pre-created; alert on sustained rates, never on a single event.
- **Scrape-based metrics are blind in a crash storm.** A container that lives less than one scrape interval is never scraped, so worker-side counters under-report while containers are killed every few seconds. The Compose stack scrapes the app targets every 5 s; production at 15 to 30 s is fine for normal crashes.
- **Histogram quantiles above the top finite bucket are interpolated.** The pickup-latency buckets end at 30, 60 and 300 s, so a 47 s wait shows as a "60 s p99". The SLO is defined at 30 s, where the buckets are exact; use `oldest_ready_job_age_seconds` for long waits.
- **Database-derived gauges are sampled coarsely.** `queue_depth` and `oldest_ready_job_age_seconds` come from the scheduler leader's collector (cached 5 s) and are scraped every 5 s, so they appear in steps about 10 s wide.

## Kubernetes: verified behaviour and caveats

Everything here was observed on a real cluster (`make k8s-verify`, `make argocd-verify`); the caveats are the ones that surprised me.

**Install and migrations**
- With a real database the migration is a `pre-install`/`pre-upgrade` hook Job: it runs before the new pods roll out and blocks the upgrade if it fails (Argo CD runs it as `PreSync`).
- With the in-cluster **demo** Postgres it is a plain per-revision Job instead. A post-install hook *deadlocks* `helm install --wait` there (KEDA cannot be Ready until the table exists, and the hook only starts after the wait succeeds).
- `jobctl migrate up` waits up to 90 s for the database, so a migration that starts before Postgres accepts connections retries instead of failing.

**KEDA**
- Scale-up doubles replicas every 30 s up to the max (observed 2 -> 4 -> 8); scale-down removes 2 pods per minute after a 5-minute stabilization window (observed 8 -> 6 -> 4 -> 2). Both are the HPA behavior in `templates/keda.yaml`.
- `keda.cooldownSeconds` only applies when `minReplicaCount` is 0. With a minimum above zero KEDA hands scaling to an HPA and ignores it (and warns), so the chart only renders it for scale-to-zero.
- **The database host in the connection string must be resolvable from KEDA's namespace** (use the namespace-qualified service name, as the demo secret does). KEDA's operator runs elsewhere and queries the database itself.
- **KEDA caches the resolved secret when it builds the scaler.** After rotating `DATABASE_URL`, restart the `keda-operator` deployment (or touch the ScaledObject); it does not notice the change on its own.

**Rollouts and shutdown**
- `kubectl rollout status` returns as soon as the *replacement* pods are Ready (it returned in 5 s while 12 jobs of 40 s were still draining). Old pods stay `Terminating` for up to `terminationGracePeriodSeconds`. Do not use it to conclude that draining finished.
- `kubectl delete pod --force --grace-period=0` is **not** a crash: the kubelet still sends SIGTERM and waits about 2 s, which absorbed short jobs completely. To simulate an OOM kill, stop the container immediately (`crictl stop -t 0`).
- A job whose `timeout_seconds` exceeds `worker.maxJobTimeoutSeconds` cannot finish across a deploy: at the grace deadline the old worker cancels it and **releases** it (attempt refunded, nothing lost), and it restarts from the beginning on a new pod. Keep job timeouts at or below the maximum, or such jobs restart on every deploy.

**Node maintenance**
- `kubectl drain` waits on the PodDisruptionBudgets (`Cannot evict pod as it would violate the pod's disruption budget`, retried every 5 s). That is the budget working, not a fault.
- After `uncordon`, pods do **not** move back; the topology-spread constraint is soft and only applies at scheduling time. Use the descheduler, or the next rollout, to rebalance.
- The demo Postgres uses a node-local volume, so its node cannot be drained. A real database is outside the cluster.

**NetworkPolicy**
- Default-deny covers every pod carrying the release labels, including the demo Postgres, which therefore needs an explicit allow (the chart adds one, including KEDA's namespace). A policy that is *missing* such an allow looks healthy until the first restart: **existing connections survive a new policy and new ones are refused**. Always test by restarting the pods (`make k8s-verify` does).
- With an external database the only database rule is the egress CIDR; whatever protects the database itself is outside the chart.

**GitOps (Argo CD)**
- The `AppProject` whitelist covers every kind the chart renders (verified). A new kind in the chart needs a matching entry or the sync is rejected.
- `ignoreDifferences` on `/spec/replicas` is what stops Argo CD fighting the HPA and KEDA. Manual edits of anything else are reverted within seconds (self-heal), and rollback is `git revert`.

## Limits and backpressure

Protect the system from a noisy tenant, protect a downstream service from being overrun, and stop a backlog growing without bound. Rules are per `(queue, tenant)`; `*` means "every tenant on this queue without a rule of its own" (a shared allowance).

```bash
# Tenant "acme" on queue "default": at most 20 running at once, 50 jobs/second (burst 100),
# and reject submissions with 429 once 10,000 jobs are waiting.
jobctl limits set -queue default -tenant acme -max-running 20 -rate 50 -burst 100 -max-backlog 10000

# Protect a third-party API: everyone on the "emails" queue shares 10 jobs/second.
jobctl limits set -queue emails -rate 10 -burst 10

jobctl limits list
jobctl limits delete -queue emails            # (-tenant defaults to "*")
```

`jobctl limits` talks to the database (`DATABASE_URL`), like `create-key`. Changes apply within about 2 seconds.

**Choosing values**

- `max_running`: the downstream's safe concurrency for that tenant, or the share of your total worker slots you are willing to give them. It cannot exceed what workers can actually run.
- `rate_per_second` / `burst`: the downstream's published rate limit. `burst` is how much a quiet tenant may do at once; the bucket never holds more than `burst`, so idling does not bank unlimited credit. A retried job spends a token again, because it hits the downstream again.
- `max_backlog`: roughly `rate you can drain x the delay you accept`. Producers must honour `Retry-After` (5s) and retry with the **same** `idempotency_key`; a retry of an already accepted job returns it instead of 429.

**How to tell throttling from a capacity problem.** A deliberate limit shows as a large backlog for one tenant while workers have idle slots (`running` below total concurrency), and `jobscheduler_submissions_rejected_total` rising for 429s. See [queue-backlog.md](runbooks/queue-backlog.md#if-the-cause-is-limits-throttling-or-backpressure-rather-than-capacity).

**Costs to know about**

- Claims for a *limited* scope are serial (one short transaction at a time per rule). Fine for restraining a tenant; not a way to describe your entire capacity.
- Backlog counts are cached for 1s, so with N API replicas the real backlog can overshoot `max_backlog` by what the others admit within a second.
- Jobs whose lease expired but are not yet reaped still count as running, so a crashed worker briefly lowers a capped tenant's effective concurrency.
- Cron fires are not subject to `max_backlog`.
