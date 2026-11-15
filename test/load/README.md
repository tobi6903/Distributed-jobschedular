# test/load

| File | |
|---|---|
| [k6.js](k6.js) | k6 load test with thresholds, for the Compose stack and Kubernetes |
| [loadgen/main.go](loadgen/main.go) | a small Go load generator that needs no k6, used by `scripts/bench.sh` |

## k6

```bash
k6 run -e API_URL=http://localhost:8080 -e API_KEY=$JOBCTL_API_KEY test/load/k6.js     # or: make load-test
# tunables: RATE (jobs/s, default 200), DURATION (default 2m), JOB_TYPE (default noop)
```

Two scenarios:

- **submit**: a constant *arrival rate* (open model, so slow responses do not reduce the offered load, which is how real traffic behaves).
- **e2e**: one job per second is submitted and polled until it finishes, measuring end-to-end completion latency (`job_end_to_end_ms`).

Thresholds: submit p95 < 250 ms and p99 < 750 ms, submit failures < 1%, at least 99% of sampled jobs complete, end-to-end p95 < 30 s (the pickup SLO).

## loadgen

```bash
go run ./test/load/loadgen -api http://localhost:8080 -key $KEY -n 50000 -c 64 -db $DATABASE_URL -tenant bench
```

Measures submit throughput and latency; with `-db` it also waits for every job to finish and reports end-to-end throughput.

Results and charts: the main [README](../../README.md#performance); raw data in [docs/perf](../../docs/perf/README.md).
