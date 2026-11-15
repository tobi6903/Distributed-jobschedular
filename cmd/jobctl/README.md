# cmd/jobctl

The operator CLI: run migrations, mint API keys, manage limits, and talk to the API. `cli.go` holds a testable `run(ctx, args, out, getenv)`; `main.go` only wires signals and exit codes.

## Commands

| Command | Talks to | Purpose |
|---|---|---|
| `migrate up\|down [n]\|version` | database (`DATABASE_URL`) | apply or roll back migrations; `up` waits up to 90 s for the database |
| `create-key -tenant T [-name N]` | database | mint an API key; printed **once**, only its hash is stored |
| `limits set\|list\|delete` | database | per-tenant concurrency caps, dispatch rate limits, backlog limits |
| `submit -type T [-payload JSON] [-queue Q] [-priority N] [-delay S] [-max-attempts N] [-timeout S] [-key K]` | API | submit a job |
| `get [-wait] ID` | API | show a job; `-wait` blocks until it is terminal |
| `replay ID` | API | re-queue a `failed` or `dead` job |
| `schedule create\|list\|get\|pause\|resume\|delete` | API | manage cron schedules |

API commands read `JOBCTL_URL` (default `http://localhost:8080`) and `JOBCTL_API_KEY`.

```bash
./bin/jobctl submit -type flaky -payload '{"succeed_on_attempt":3}'
./bin/jobctl get -wait <id>
./bin/jobctl schedule create -name nightly -cron '0 2 * * *' -tz Asia/Kolkata -type report
./bin/jobctl limits set -queue default -tenant acme -max-running 20 -rate 50 -max-backlog 10000
```

`migrate`, `create-key` and `limits` are admin operations and need database access; that is why they are not API endpoints. The same image is the Kubernetes migration Job.

## Tests

[cli_test.go](cli_test.go): argument errors table, and submit/get/replay against a stub server (including `get -wait` polling and server errors surfacing).
