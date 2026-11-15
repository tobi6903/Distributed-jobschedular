# test/integration

End-to-end tests that run the **real** API, store and workers against a real Postgres (provisioned by [internal/testdb](../../internal/testdb/README.md)). Mocks cannot reproduce `SKIP LOCKED` semantics, so none are used for the database.

| File | Tests |
|---|---|
| [worker_test.go](worker_test.go) | end-to-end over HTTP; retries until success; **dead-letter and replay**; permanent errors skip retries; unknown job type; per-job timeout; **crashed worker's job recovered** (attempts = 2); heartbeats keep a long job alive against an aggressive reaper; **graceful shutdown** finishes in-flight jobs; **forced shutdown** releases (attempt refunded); the retention sweeper; a rate-limited queue throttled end to end while another queue is unaffected |
| [scale_test.go](scale_test.go) | **50 workers x 100,000 jobs: zero duplicates, zero lost** (`-short` uses 5,000); **workers crash at random** (a `crashable` store wrapper kills 12 of 20, leaving zombies) and nothing is lost, with the at-least-once contract stated exactly: a job executes more than once only if it was claimed more than once; 50 concurrent identical idempotent submits create one job |
| [observability_test.go](observability_test.go) | one trace spans submit, enqueue, claim, execute and complete with the right parent/child chain; JSON log lines carry `job_id`, `tenant_id`, `worker_id` and the same `trace_id` |
| [helpers_test.go](helpers_test.go) | `fastCfg` (millisecond timers), `startWorker`, `eventually`, a concurrency-safe execution counter |

## Running

```bash
make test                       # everything, with -race (about 3 minutes)
make test-short                 # smaller scale
TEST_DATABASE_URL=postgres://postgres@localhost:5432/postgres?sslmode=disable go test -race ./test/integration/
```

`make test` sets `REQUIRE_TEST_DB=1`, so a missing database fails the run instead of silently skipping every test.

The 100k test reports its own throughput (about 1,500 to 1,800 jobs/s under the race detector on the development machine); the numbers in the main README come from [scripts/perf-collect.sh](../../scripts/perf-collect.sh), not from here.
