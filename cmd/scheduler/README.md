# cmd/scheduler

The cron process. Run **2 or more replicas**: one is elected leader and fires due schedules, the others are hot standbys. Behaviour lives in [internal/scheduler](../../internal/scheduler/README.md).

## Run

```bash
DATABASE_URL=postgres://... ./bin/scheduler    # point it at Postgres directly, not a transaction-mode pooler
```

| Variable | Default | |
|---|---|---|
| `SCHEDULER_TICK` | 1s | how often the leader looks for due schedules |
| `SCHEDULER_ELECTION_RETRY` | 5s | how often a standby retries (about the worst-case cron failover time) |
| `SCHEDULER_MISFIRE_GRACE` | 1m | how stale a slot may be and still fire under the `skip` policy |
| `METRICS_ADDR` | `:9101` | `/metrics` and `/healthz` |
| `DB_MAX_CONNS` | 10 (Compose/Helm: 5) | plus one dedicated connection for the election |

## Notes

- The election opens its **own** session from `DATABASE_URL`: an advisory lock belongs to a session, so it cannot come from the pool.
- The queue-depth gauges (`queue_depth`, `oldest_ready_job_age_seconds`, `dlq_size`) are exported **only by the leader**, so Prometheus sees one series per queue rather than one per replica.
- Safety does not depend on the election. See the package README for the row-lock and idempotent-key guarantees.
