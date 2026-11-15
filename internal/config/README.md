# internal/config

Environment-variable configuration (12-factor), loaded through a `getenv func(string) string` argument rather than `os.Getenv`, so tests need no global state.

| Function | Returns |
|---|---|
| `LoadAPI` | `API` (`Common` + address, shutdown delay and timeout) |
| `LoadWorker` | `Worker` (`Common` + queues, concurrency, lease, timeouts, retention, backoff) |
| `LoadScheduler` | `Scheduler` (`Common` + tick, election retry, misfire grace) |

`Common` carries `DATABASE_URL` (required), `DB_MAX_CONNS`, `LOG_LEVEL`, `METRICS_ADDR`.

## Behaviour

- **All problems are reported at once.** Errors are accumulated and joined, so an operator fixes every bad variable in one pass instead of one restart per mistake.
- **Cross-field validation**: concurrency must be at least 1, and `WORKER_HEARTBEAT_INTERVAL` must be below `WORKER_LEASE` (a heartbeat at or beyond the lease would let it expire between beats).
- Durations use `time.ParseDuration` (`30s`, `5m`); negative values are rejected.

The full variable table with defaults is in [docs/OPERATIONS.md](../../docs/OPERATIONS.md#components-and-configuration).

## Tests

[config_test.go](config_test.go): defaults, overrides, and a table of error cases including "reports all errors at once".
