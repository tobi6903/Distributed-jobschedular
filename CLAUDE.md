# CLAUDE.md

Portfolio project for SDE-2 interviews: a Distributed Job Scheduler in Go (mini Sidekiq/Temporal-lite). The owner must be able to explain every line, so favour clarity over cleverness.

## Autonomy (standing instruction from the owner)

**Do not ask for permission, approval or review. Work autonomously through all phases (1 to 7) until the project is complete.** Pick sensible defaults, record non-obvious choices in `docs/DESIGN.md`, and keep going. Only stop if something is genuinely impossible, and say so plainly in the final summary (for example, tooling that cannot be installed without sudo).

## Current phase

**Phases 0 to 7: complete. Phase 8 item done: rate-limited queues with backpressure** (per-tenant concurrency caps, token-bucket dispatch rate, 429 backpressure; see DESIGN.md section 18). Remaining Phase 8 options, none started: DAG workflows, outbox pattern, K8s Operator. Also done: investigation of the uneven burst claims (a Prometheus new-series artifact plus a cached-generic-plan trap on fresh workers, fixed with `plan_cache_mode=force_custom_plan`; DESIGN.md section 20). Also done: LISTEN/NOTIFY wake-ups for idle workers (opt-in via `NOTIFY` on the API and scheduler and `WORKER_NOTIFY` on workers, Helm `notify.enabled`; a hint only, polling stays as the safety net; DESIGN.md section 21; idle pickup p50 377 ms to 9 ms at 2 workers, but only 69 to 7 ms at 8, no idle-query reduction without a longer fallback poll, and a 10 s fallback delays retried jobs by up to 8.9 s). Also done: completion-write batching (group commit, opt-in via `WORKER_COMPLETE_BATCH`; DESIGN.md section 19; +51% at 8 workers, no measurable gain at 1 to 4).

Environment notes:
- Go lives in `~/sdk/go/bin` and CLI tools in `~/sdk/tools` (add both to PATH): helm v4.3.1, kubeconform v0.8.0, hadolint v2.15.1, actionlint v1.7.12, k6 v2.3.0, promtool 3.15.1, amtool 0.34.2, golangci-lint v2.15.0, each downloaded at a pinned version and sha256-verified. The docker buildx plugin is installed per-user in `~/.docker/cli-plugins`.
- Docker is installed. A shell started before the user was added to the `docker` group needs `sg docker -c '...'`; a fresh login does not.
- Tests: `make test` sets `REQUIRE_TEST_DB=1` and starts Postgres via testcontainers (or uses `TEST_DATABASE_URL`). Run Go tooling with `GOTOOLCHAIN=go1.26.9` to match `go.mod` and CI.
- `make validate` (no container runtime needed), `make up` then `make smoke` / `make chaos-docker` / `make load-test` (Docker).
- kind v0.33.0 and kubectl v1.37.2 are in `~/sdk/tools`. Kubernetes: `make docker-build IMAGE_TAG=dev REGISTRY=jobscheduler && make kind-up`, then `make k8s-verify` (FULL=1 for the long test) and `make argocd-verify`; cluster context is `kind-js`. See deploy/kind/README.md.
- GitHub Actions has run for real (repo `tobi6903/Distributed-jobschedular`, `gh` is authenticated): `test`, `lint`, `chart`, `smoke` and the Kubernetes workflow passed. The `images` job first failed on a misconfigured Trivy gate; after the fix the whole pipeline passed on `main` (GHCR push of four public images, `promote` bot commit). Still never run: anything on a managed cloud cluster. The published GHCR images were rolled out through Argo CD on kind (`PUBLISHED=1 make argocd-verify`), and the API HPA scale-out was checked with `make hpa-verify`.
- When reading CI: `gh run list -R tobi6903/Distributed-jobschedular`, `gh run view <id> --json jobs`, and `gh api repos/<repo>/actions/jobs/<job_id>/logs` for a finished job inside a running workflow. Trivy SARIF findings are in code scanning (`gh api repos/<repo>/code-scanning/alerts`), not in the log. Scan images locally with a FRESH `--cache-dir`: a stale vulnerability DB gives false negatives.
- The Go module path is still the placeholder `github.com/vasistamagam/distributed-jobscheduler` (27 files import it); deployment-facing names (registry, Argo repoURL) use the real repo. Renaming the module is optional.
- Metrics: create fixed-label counters at zero in `metrics.New()` (a counter born at its first increment is invisible to `rate()`/`increase()`); quantiles above the top finite histogram bucket are interpolated, so do not chart or alert on them.
- `PHASES=submit ./scripts/perf-notify.sh` re-runs only the submit-throughput check; that check swings about +/-30% between runs on this machine, so compare paired alternating runs, never single ones.
- Performance numbers in docs come from `scripts/perf-collect.sh` (data in docs/perf/data, charts by `scripts/perf-charts.py`, screenshots by `scripts/grafana-screenshots.sh`). Never type a number into a chart or doc; look at every rendered chart before committing it; results vary between runs, so state that.
- **Tests share one Postgres server in CI** (all packages run in parallel against a single service container; locally every test binary gets its own testcontainer, which hides this). A test must never touch cluster-wide state: scope `pg_terminate_backend`/`pg_stat_activity` queries with `datname = current_database()`. An unscoped kill of `LISTEN` sessions in the queue tests once failed the integration notify test on CI only (its listeners waited out the 1 s reconnect backoff: mean 415 ms, worst 1.02 s). To reproduce CI's topology locally: `docker run -d -p 55433:5432 -e POSTGRES_PASSWORD=postgres postgres:16 -c max_connections=300 -c fsync=off`, then `TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55433/postgres?sslmode=disable REQUIRE_TEST_DB=1 go test -race ./...`.
- Shell scripts: never write `wait_for N test "$(cmd)" = x` (the `$(...)` is evaluated once); use `wait_eq` from scripts/lib-k8s.sh. Do not run an acceptance check you have not seen fail: mutate something and confirm it catches it.

## Working agreement

1. Work one phase at a time, in order. At the end of each phase: run tests, and note what was built, why, and the tradeoffs (in the final summary and `docs/DESIGN.md`). Do not pause for review.
2. No phase is done without passing tests (`go test -race ./...`).
3. Explain non-obvious decisions in short comments and in `docs/DESIGN.md`.
4. Keep dependencies minimal and justify each one in DESIGN.md section 13.
5. Keep this file's "Current phase" up to date.

## Conventions

- Go 1.26 minimum (see `go.mod`); images are built with the newest Go. Standard library first: `net/http` with the new `ServeMux`, `log/slog`, `flag`, stdlib `testing`.
- Postgres 16 via `pgx/v5`. Migrations via `golang-migrate`, embedded. Integration tests via `testcontainers-go`.
- Small interfaces defined by the consumer. `context.Context` is the first parameter everywhere. Wrap errors with `%w`. No global state. Inject the clock and the RNG.
- Table-driven tests. Run with `-race`.
- All time comparisons in SQL use the database `now()`, never worker clocks.
- Every transition out of `running` is fenced on `(id, status='running', locked_by, attempts)`.
- Tenant always comes from the API key, never from the request body.
- Limit enforcement lives in `internal/queue/limits.go`: a limited claim holds the rule's row lock (`FOR UPDATE SKIP LOCKED`) so count-and-claim is atomic. Never weaken that lock; `TestConcurrencyCapIsExactUnderContention` guards it (it was mutation-tested).

## Decisions made

- Handlers are in-process Go funcs in a registry keyed by job `type`.
- Tenants are first-class: `tenant_id` column plus an API key per tenant (SHA-256 hashed).
- Finished rows are kept, and a batched TTL reaper deletes them.
- Kubernetes target: Helm chart (not Kustomize), KEDA for worker autoscaling, Argo CD for GitOps; kind or k3d locally.
- Leader election uses a Postgres session advisory lock, but double-fire safety does NOT depend on it: row locks plus a per-slot idempotency key are the guarantee.
- At-least-once delivery only; handlers must be idempotent.
- `go.mod` is at `go 1.26`, **raised from 1.23 on purpose**. Pinning old dependencies to hold 1.23 left CRITICAL/HIGH CVEs in the shipped binaries (pgx 5.7, x/crypto 0.35, otel sdk 1.34, grpc 1.69; Trivy found them). Keep dependencies patched, not pinned old; the CI Trivy gate enforces it. Build images with a current Go for the same reason (the stdlib is compiled in).
- Connection pools are sized per component (API 40, worker 20, scheduler 5): pool size x commit latency caps throughput.
- Never trust an `ok`: a skipped test prints `ok`. Run with `REQUIRE_TEST_DB=1`, and check `-v` for SKIP counts. A validation step that passes on empty input proves nothing (it must fail when nothing was validated).

## Layout

`cmd/{api,worker,scheduler,jobctl}`, `internal/{job,queue,worker,scheduler,api,metrics,telemetry,config,migrate,handlers,testdb}`, `migrations/`, `deploy/{docker,helm,argocd,prometheus,alertmanager,grafana,loki,promtail,tempo}`, `docs/` (DESIGN, OPERATIONS, SLO, runbooks), `test/{integration,chaos,load,helm}`.

## Phases

0 Plan, 1 Core, 2 Concurrency and correctness, 3 Retries and failure handling, 4 Scheduling, 5 Observability, 6 Containers and Kubernetes, 7 CI/CD and proof, 8 Advanced (choose later).
