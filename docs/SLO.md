# Service Level Objectives

SLOs are defined from the user's point of view: *do my jobs start promptly, do they finish, can I submit them?* Each SLI is computed from the metrics in `internal/metrics`, recorded by `deploy/prometheus/rules.yml`, and alerted on by multi-window burn-rate alerts in `deploy/prometheus/alerts.yml`.

## The three SLOs

| # | SLI (good events / valid events) | Objective | Window | Error budget |
|---|---|---|---|---|
| 1 | **Pickup latency**: jobs claimed within 30s of becoming runnable (`run_at`) / all claimed jobs | **99%** | 30 days | 1% of jobs may wait longer than 30s |
| 2 | **Job success**: attempts that did not end `dead` or `failed` / attempts that ended in `succeeded`, `dead` or `failed` | **99.5%** | 30 days | 0.5% |
| 3 | **API availability**: requests not answered with 5xx / all requests | **99.9%** | 30 days | 0.1% (about 43 minutes per 30 days) |

### Why these SLIs

- **Pickup latency** is the property a scheduler sells. It is measured on the database clock (`now() - run_at` inside the claim statement), so a delayed or cron job that is *meant* to wait does not count against it: the clock starts at `run_at`, not at submission.
- **Job success** deliberately excludes retried-then-succeeded attempts from "bad": a transient failure that retry fixes is the system working as designed. Only jobs that exhaust retries (`dead`) or fail permanently (`failed`) burn budget. Permanent failures caused by a client's bad payload are counted too, because we cannot tell them apart from handler bugs. If that proves noisy, exclude them by adding a `reason` label.
- **API availability** counts only 5xx. A 4xx (bad request, unauthorized, not found) is the client's error and is not an availability failure.

### What the SLOs do not cover (and why)

- **Job duration.** That is a property of the handler, not the platform. Per-type duration is on the dashboard for the job owners.
- **Exactly-once.** The system is at-least-once by design. Duplicate execution is bounded by `jobs_reaped_total` and surfaced by `WorkerCrashesDetected`, but is not an SLO.

## Alerting: multi-window, multi-burn-rate

Alerting on a raw threshold either pages too late or flaps. Instead we alert on how fast the error budget is being consumed, following the Google SRE workbook.

Burn rate = (observed error ratio) / (error budget). A burn rate of 1 uses exactly the whole budget in the SLO window.

| Alert | Burn rate | Long window | Short window | Budget consumed if sustained | Severity |
|---|---|---|---|---|---|
| Fast burn | 14.4x | 1h | 5m | 2% in 1 hour | critical (page) |
| Slow burn | 6x | 6h | 30m | 5% in 6 hours | warning (ticket) |

The short window makes an alert **reset quickly** once the problem is fixed, and the long window stops a brief blip from paging anyone. Both must be true to fire.

Worked example, pickup SLO (budget 1%): the fast-burn alert fires when more than 14.4% of jobs waited over 30s, in both the last hour and the last 5 minutes.

## Cause-based alerts (supplement the SLOs)

SLO alerts say *users are hurting*. These say *why*, and catch problems earlier:

| Alert | Meaning |
|---|---|
| `QueueOldestJobTooOld` / `Critical` | The longest-waiting runnable job is 2 / 15 minutes old: workers are behind or down |
| `DeadLetterQueueNotEmpty` / `Growing` | Jobs exhausted retries: a bug, a bad deploy, or a dead dependency |
| `HighRetryRate` | More than 20% of attempts of one job type are retried |
| `WorkerCrashesDetected` | Leases expired: a worker crashed or lost the database mid-job |
| `NoWorkersUp`, `NoSchedulerLeader`, `MultipleSchedulerLeaders` | Capacity and coordination failures |
| `CronDuplicatePrevented` | The idempotent key blocked a double-fire (the first layer was bypassed) |

Each alert links to a runbook in [docs/runbooks/](runbooks/).

The alert and SLO logic is unit-tested with `promtool test rules deploy/prometheus/tests.yml` (run by `make rules-test` and CI): synthetic series assert which alerts fire and, just as important, which do not (a 5-minute blip does not page; a service at 99.9% does not alert).

## Reviewing the SLOs

Revisit the thresholds after the first month of real traffic. A 30s pickup target is a default; a tenant with latency-sensitive work may need a dedicated queue (and its own, tighter SLO) rather than a tighter global one.
