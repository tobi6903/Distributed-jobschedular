# scripts

Developer and verification scripts. Each sources a small library and stops at the first failed check.

| Script | Needs | Purpose |
|---|---|---|
| [dev.sh](dev.sh) | a Postgres, `make build` | run API, worker and two schedulers as plain local processes (`make dev-up`) |
| [bench.sh](bench.sh) | a Postgres admin URL | the original single-machine benchmark (submit and drain tables) |
| [smoke.sh](smoke.sh) | `make up` | end-to-end check of the Compose stack: retrying job, cron, Prometheus scrapes every replica, exactly one scheduler leads, one trace spans the API and worker containers |
| [chaos-docker.sh](chaos-docker.sh) | `make up` | `docker kill` random worker and scheduler containers while jobs and cron run; fails if anything is lost or double-fired |
| [perf-collect.sh](perf-collect.sh) | `make up`, k6 | measures drain throughput vs worker count, latency vs offered load, the connection-pool effect (idle database and under commit pressure), a 60,000-job burst with a Prometheus time series, and a chaos run; writes [docs/perf/data](../docs/perf/data) |
| [perf-batching.sh](perf-batching.sh) | `make up` | measures completion batching: the same backlog drained with batching off, with group commit and with a 5 ms linger at 1, 2, 4 and 8 workers, with Postgres commits per job; writes `docs/perf/data/batching.csv` |
| [perf-notify.sh](perf-notify.sh) | `make up` | LISTEN/NOTIFY against polling in three modes (poll, notify, notify + 10 s fallback): pickup latency of a job submitted to an idle fleet (per-job `pickup_ms` from the worker log), idle polling load and wake-up herd, and a submit-throughput check (`PHASES=submit` re-runs just that one); refuses to measure unless every worker is actually listening |
| [perf-notify-retry.sh](perf-notify-retry.sh) | `make up` | the cost side of the fallback poll: how late a retried job starts under each mode (retries are never announced) |
| [perf-cold-burst.sh](perf-cold-burst.sh) | `make up` | the burst on freshly started workers, with the plan cache at the old behaviour and at the fix, alternating; per-worker numbers come from the workers' own log lines; writes `docs/perf/data/cold-burst*.csv` |
| [perf-charts.py](perf-charts.py) | matplotlib | draws the charts in [docs/images](../docs/images) from that data |
| [grafana-screenshots.sh](grafana-screenshots.sh) | `make up` with the renderer | real Grafana dashboard and panel screenshots for a time window |
| [kind-up.sh](kind-up.sh) | kind, kubectl, helm, built images | kind cluster + metrics-server + KEDA + the chart |
| [hpa-verify.sh](hpa-verify.sh) | the kind cluster | API HPA scales out under in-cluster load and the new replicas take traffic (not via port-forward, which pins one pod) |
| [k8s-verify.sh](k8s-verify.sh) | the kind cluster | KEDA scaling, container crashes, rolling deploy, forced release (`FULL=1`), node drain, NetworkPolicy |
| [argocd-verify.sh](argocd-verify.sh) | the kind cluster | Argo CD deploys from git, heals drift, rolls out, rolls back (`PUBLISHED=1`: with the GHCR images from `origin/main`) |
| [lib-compose.sh](lib-compose.sh), [lib-k8s.sh](lib-k8s.sh) | | shared helpers (`psql_`, `mint_key`, `wait_eq`, `check`, ...) |

## Conventions

- **Never `wait_for N test "$(cmd)" = x`.** The `$(...)` is evaluated once, before polling, so it polls a frozen value. Use `wait_eq` (re-runs the command on every poll).
- Checks print `PASS`/`FAIL` and the script exits non-zero if any failed; a check that cannot fail proves nothing, so each was run once against a deliberately broken case.
- Scripts depend only on `docker`, `kubectl`, `helm`, `curl` and `python3`; no `psql` on the host (SQL runs inside the Postgres container or pod).
