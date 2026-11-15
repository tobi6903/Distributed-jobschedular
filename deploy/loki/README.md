# deploy/loki

[loki.yml](loki.yml): a single-node Loki with filesystem storage and the TSDB schema, for the Compose stack. Logs arrive from [Promtail](../promtail/README.md); Grafana queries them with LogQL.

Our logs are JSON, so fields are parsed at query time: `{service="worker"} | json | outcome="retry"`. Every job log line carries `job_id`, `tenant_id`, `worker_id` and `trace_id`; the Grafana Loki datasource turns `trace_id` into a link to the trace in [Tempo](../tempo/README.md).

For anything beyond local use, run a real Loki deployment; this config is single-node on purpose.
