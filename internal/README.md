# internal

The application's packages. Dependencies point downward: `job` imports nothing; `queue` and `scheduler` use it; `worker` and `api` define the small interfaces they need and depend on neither the concrete store nor Prometheus.

```
cmd/*  ──►  api, worker, scheduler, config, telemetry, metrics, migrate, handlers
api    ──►  job, queue (errors), scheduler (types)       worker ──► job, queue (types)
queue, scheduler ──► job                                 metrics ──► job, queue
job    ──►  (nothing)                                    testdb ──► migrate
```

| Package | Responsibility |
|---|---|
| [job](job/README.md) | domain model: states, fencing token, backoff (no I/O) |
| [queue](queue/README.md) | the Postgres store: claim, heartbeat, fenced writes, reaper, limits, API keys |
| [worker](worker/README.md) | poll loop, handler registry, heartbeats, outcomes, graceful shutdown |
| [scheduler](scheduler/README.md) | cron, misfire policies, leader election, never double-fires |
| [api](api/README.md) | HTTP: submit, status, replay, schedules, health, backpressure |
| [metrics](metrics/README.md) | Prometheus metrics and observer adapters |
| [telemetry](telemetry/README.md) | JSON logging and optional OpenTelemetry tracing |
| [config](config/README.md) | environment configuration, validated at startup |
| [migrate](migrate/README.md) | embedded SQL migrations |
| [handlers](handlers/README.md) | demo job handlers |
| [testdb](testdb/README.md) | isolated, migrated databases for tests |

Conventions: small interfaces defined by the consumer, `context.Context` first, errors wrapped with `%w`, no global state, the clock and RNG injected, table-driven tests (see [../CLAUDE.md](../CLAUDE.md)).
