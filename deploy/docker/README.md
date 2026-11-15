# deploy/docker

## Dockerfile

One Dockerfile builds all four binaries; pick one with `--build-arg APP=api|worker|scheduler|jobctl`.

```bash
make docker-build IMAGE_TAG=dev REGISTRY=jobscheduler     # needs Docker with the buildx plugin (BuildKit)
```

- **Stage 1** compiles a static binary (`CGO_ENABLED=0`, `-trimpath`, stripped) with cached module downloads.
- **Stage 2** is `gcr.io/distroless/static-debian12:nonroot`: no shell, no package manager, uid 65532. The exec-form `ENTRYPOINT` makes the process PID 1 so it receives SIGTERM directly.
- **The build uses the newest Go**, not the oldest the module allows: the compiler's standard library is compiled into the binary, and building with an end-of-life toolchain shipped dozens of fixable HIGH/CRITICAL CVEs (Trivy). Images are 22 to 35 MB with **0 fixable HIGH/CRITICAL findings**.

## docker-compose.yml

The whole stack for local use (`make up`): Postgres 16, a one-shot `migrate` job (everything else waits for it), the API, 2 workers, 2 schedulers, Prometheus, Alertmanager, Grafana, Loki, Promtail and Tempo.

```bash
make up                  # build and start
export JOBCTL_API_KEY=$(make -s key)
make smoke               # end-to-end check of the running stack
make chaos-docker        # docker kill workers and schedulers mid-run
make down                # stop and delete volumes
```

| Detail | Why |
|---|---|
| Published ports bind to `127.0.0.1` and are configurable (`API_PORT`, `POSTGRES_PORT`, `PROMETHEUS_PORT`, `ALERTMANAGER_PORT`, `GRAFANA_PORT`) | Postgres defaults to **55432**, not 5432, because most developers already run a local Postgres |
| Per-component pool sizes (API 40, worker 20, scheduler 5) | the API holds a connection per durable commit; 10 connections gave p95 1,637 ms vs 146 ms with 40 once the database was busy committing (no difference on an idle one) |
| `stop_grace_period: 45s` on workers | must exceed `WORKER_SHUTDOWN_GRACE` (30s) so SIGTERM drains instead of SIGKILL |
| `renderer` service behind the `render` profile | headless Chromium for dashboard screenshots; opt-in because the image is large |

`docker kill` counts as a manual stop, so Docker does **not** apply `restart: unless-stopped` to it (a real crash or OOM kill is restarted). The chaos script restarts what it kills.
