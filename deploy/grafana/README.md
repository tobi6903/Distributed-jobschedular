# deploy/grafana

**Dashboards as code.** The dashboard JSON is generated from [gen/gen_dashboard.py](gen/gen_dashboard.py), committed, and provisioned read-only (`allowUiUpdates: false`), so UI edits are not persisted. Change the generator (or the JSON), commit, and Grafana picks it up.

```bash
python3 deploy/grafana/gen/gen_dashboard.py     # rewrites dashboards/jobscheduler.json
```

| Path | Purpose |
|---|---|
| [dashboards/jobscheduler.json](dashboards/jobscheduler.json) | the "Job Scheduler" dashboard: 28 panels in six rows |
| [provisioning/datasources/](provisioning/datasources/datasources.yml) | Prometheus, Loki (with a derived field turning `trace_id` into a link) and Tempo (traces to logs) |
| [provisioning/dashboards/](provisioning/dashboards/dashboards.yml) | the file provider |

## Rows

Health at a glance (oldest ready job age, backlog, running, DLQ, scheduler leaders, pickup SLO) / Queue / Execution / Scheduler / API / Logs and traces.

## Verified

Against the live Compose stack: the datasource health checks pass, the dashboard is provisioned (read-only), and **every Prometheus query in all 28 panels executes without error** against live data.

Screenshots of the dashboard under load and chaos are in the main [README](../../README.md#performance); regenerate them with `scripts/grafana-screenshots.sh`.
