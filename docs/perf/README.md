# docs/perf

Raw performance measurements and how the charts in the main [README](../../README.md#performance) are produced. Nothing is typed in: [scripts/perf-collect.sh](../../scripts/perf-collect.sh) measures, [scripts/perf-charts.py](../../scripts/perf-charts.py) draws.

```bash
make up                                   # the Compose stack (add the renderer for screenshots, see below)
./scripts/perf-collect.sh                 # about 15 minutes; needs k6
./scripts/perf-batching.sh                # about 15 minutes: completion batching off vs on
./scripts/perf-notify.sh                 # about 25 minutes: LISTEN/NOTIFY wake-ups vs polling (idle pickup latency, idle load, submit throughput)
./scripts/perf-notify-retry.sh           # about 4 minutes: how late retried jobs start under each fallback poll
./scripts/perf-cold-burst.sh              # about 12 minutes: bursts on freshly started workers, before and after the plan-cache fix
~/sdk/venv/bin/python scripts/perf-charts.py        # or any Python with matplotlib
./scripts/grafana-screenshots.sh $(cut -d' ' -f1 docs/perf/data/window.txt) $(cut -d' ' -f2 docs/perf/data/window.txt)
```

## Data files ([data/](data/))

| File | Columns / contents | Experiment |
|---|---|---|
| `scale.csv` | `workers, run, jobs, seconds, jobs_per_second` | a backlog of no-op jobs is preloaded and drained by 1, 2, 4 and 8 **worker containers** (20 execution slots each), several runs per point |
| `load.csv` | `offered_rps, p50_ms, p95_ms, p99_ms, achieved_rps, dropped_iterations, failed_pct` | k6 submits jobs at a fixed *arrival rate* of 100 to 500 req/s for 20 s each; latency is `POST /jobs` only |
| `pool.csv` | `scenario, pool, p50_ms, p95_ms, p99_ms, achieved_rps, ...` | the API at 400 req/s with its connection pool at 10 and 40, on an idle database and while 8 workers drain a 60,000-job backlog |
| `burst.json`, `burst.window` | Prometheus `query_range` results (queue depth, completed jobs/s, p50 and p99 pickup latency, oldest runnable job age) and the start/end epoch | **a burst**: 60,000 no-op jobs inserted at once and drained by 4 workers, after a 60 s quiet period so rate windows contain only this run |
| `chaos.csv`, `chaos.summary.json`, `chaos.window` | `t, succeeded, kills` every ~2 s; totals; start/end epoch | the container chaos run: `docker kill` a worker or scheduler container every ~4 s for 40 s while jobs and a 2-second cron schedule run. Computed from the **database**, not Prometheus (see below) |
| `batching.csv` | `mode, workers, run, jobs, seconds, jobs_per_second, commits_per_job` | completion batching: the same drain as `scale.csv` with batching off, with group commit, and with group commit plus a 5 ms linger, 5 runs per cell, plus Postgres commits per job from `pg_stat_database.xact_commit`. Written by [perf-batching.sh](../../scripts/perf-batching.sh) |
| `cold-burst.csv`, `cold-burst-series.csv` | `mode, run, drain_seconds, first_1000_per_s_at, worker_min, worker_max` and `mode, run, t, jobs_per_second` | the cold-start burst: 4 freshly started workers, 60,000 jobs inserted at once, with the plan cache left at the old behaviour (`auto`) and at the shipped default (`custom`), alternating. Written by [perf-cold-burst.sh](../../scripts/perf-cold-burst.sh) from the workers' own log lines, not from Prometheus |
| `notify-latency.csv`, `notify-idle.csv`, `notify-submit.csv`, `notify-retry.csv` | `mode, workers, job, pickup_ms`; `mode, workers, empty_polls_per_s, wakeups_claimed, wakeups_empty, jobs`; `mode, run, req_per_s, p50_ms, p99_ms`; `mode, job, attempt2_late_ms` | LISTEN/NOTIFY: per-job pickup latency from the worker's own log (database clock), idle polling load and the wake-up herd, submit throughput over 8 alternating pairs, and the lateness of retried jobs |
| `window.txt` | `start_epoch end_epoch` | the whole collection, used to time the Grafana screenshots |

## Reading the numbers honestly

- **One shared machine.** The stack, Postgres, k6 and the load generator all run on one WSL2 machine (12 logical CPUs, durable Postgres with `fsync=on`). These show the *shape* of the system, not a capacity claim.
- **Throughput is a Postgres commit rate.** Every job costs one durable commit when it completes, so a single database sets the ceiling; adding worker containers beyond a point adds little. Batching completion writes cut commits per job by 3.5x and raised 8-worker throughput by about half ([OPERATIONS.md](../OPERATIONS.md#completion-batching)); it did not remove the ceiling, so commit rate is one limit among several.
- **Results vary with host load.** Runs on a quiet machine and on a busy one differed by an order of magnitude in tail latency; the repository's earlier numbers were taken on a busier machine than the ones here. Compare runs taken under similar conditions, and rerun on your own hardware.
- **New series are under-reported at first.** A series that does not exist until its first increment hides that increment from `rate()`, and a 20 s window needs 20 s to fill. In a burst the per-worker panel showed the least busy worker at 41 to 60% of the busiest for 14 s while the workers' logs showed identical rates. When a metric disagrees with what you expect, count log lines or database rows before believing either.
- **Prometheus is blind during a crash storm.** A container that lives less than one scrape interval is never scraped, so worker-side counters (throughput, pickup latency, reaps) under-report badly while containers are being killed every few seconds. The app targets are scraped every 5 s here (15 s globally would be worse), the burst experiment is deliberately kill-free, and the chaos chart is drawn from the database, which does not have this problem. Real crashes are rarer and pods live longer, but it is a real limit of scrape-based metrics.
- **No-op jobs** isolate the queue's own cost. Real handlers spend their time in handlers, and then worker count scales much further.
