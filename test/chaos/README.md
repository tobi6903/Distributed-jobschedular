# test/chaos

`main.go` runs the **real `worker` and `scheduler` binaries** as OS processes against a throwaway database and `SIGKILL`s them at random while work flows. SIGKILL gives the process no chance to clean up, which is what a crash or an OOM kill looks like.

```bash
make build
ADMIN_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable make chaos
go run ./test/chaos -help        # -workers, -jobs, -job-ms, -kill-every, -sched-kill-every, -timeout
```

## What it does

1. Creates a fresh database, migrates it, inserts N jobs and a per-second cron schedule (`fire_all`).
2. Starts the worker processes and two schedulers; every `-kill-every` it SIGKILLs a random worker (and a scheduler on a slower cadence), then respawns it.
3. Waits until every job succeeded, then checks:
   - **no job lost** (every job reached `succeeded`),
   - **no cron slot missed or duplicated** (consecutive one-second slots, no gaps, no duplicate keys).

Exit status is non-zero if either fails. Sample run: 52 SIGKILLs, 20,000 of 20,000 jobs succeeded, 484 recovered after losing their worker, 46 cron slots with no gaps or duplicates.

This is the process-level chaos test. The container-level equivalent is [scripts/chaos-docker.sh](../../scripts/chaos-docker.sh) and the cluster-level one is section 2 of [scripts/k8s-verify.sh](../../scripts/k8s-verify.sh).
