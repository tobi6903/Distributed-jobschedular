# deploy/prometheus

| File | Purpose |
|---|---|
| [prometheus.yml](prometheus.yml) | scrape config for the Compose stack; `dns_sd` discovers every replica behind a service name, so `--scale worker=N` is picked up automatically (in Kubernetes the chart's `ServiceMonitor` does this). The app targets are scraped every **5 s** rather than the global 15 s: at 15 s, a container that lives less than one interval (a crash storm) is never scraped at all. Use 15 to 30 s in production |
| [rules.yml](rules.yml) | recording rules: the three SLIs precomputed over the windows the burn-rate alerts need |
| [alerts.yml](alerts.yml) | 17 alerts, each with a `runbook_url` that points at a real file in [docs/runbooks](../../docs/runbooks/) |
| [tests.yml](tests.yml) | **unit tests for the rules**: synthetic series in, assertions about which alerts fire (and which do not) |

## Alert families

- **SLO burn rate** (multi-window, multi-burn-rate): fast burn 14.4x over 1 h and 5 m pages; slow burn 6x over 6 h and 30 m tickets. Definitions and the budget maths: [docs/SLO.md](../../docs/SLO.md).
- **Queue**: oldest ready job too old (warning 2 min, critical 15 min), dead-letter queue not empty or growing, high retry rate, submissions rejected by backpressure.
- **Workers and scheduler**: no workers up, crash recoveries detected, no scheduler leader, multiple leaders, a cron double-fire prevented.
- **API**: high latency, Postgres unreachable.

## Testing the alerts

```bash
promtool check rules alerts.yml rules.yml
promtool test rules tests.yml      # 12 test cases; `make rules-test` runs both
```

The tests cover, among others: an alert that fires only after its `for` duration, a healthy queue that raises nothing, **a 5-minute blip that must not page** (the point of multi-window alerting), and a 99.9%-good service that must not alert. A mutation check (raising a threshold in a copy) was confirmed to make them fail.
