#!/usr/bin/env python3
"""Generates deploy/grafana/dashboards/jobscheduler.json.

Dashboards are code: edit this file (or the JSON), commit, and Grafana provisioning picks it
up (allowUiUpdates is false, so UI edits are not persisted). Run from the repo root:

    python3 deploy/grafana/gen/gen_dashboard.py
"""
import json
import pathlib

PROM = {"type": "prometheus", "uid": "prometheus"}
LOKI = {"type": "loki", "uid": "loki"}
_id = 0


def _next():
    global _id
    _id += 1
    return _id


def target(expr, legend="", ref="A", ds=PROM):
    return {"datasource": ds, "expr": expr, "legendFormat": legend, "refId": ref}


def panel(kind, title, targets, x, y, w, h, unit="short", desc="", thresholds=None, extra=None):
    p = {
        "id": _next(), "type": kind, "title": title, "description": desc,
        "datasource": targets[0]["datasource"], "targets": targets,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "palette-classic"}}, "overrides": []},
        "options": {},
    }
    if kind == "timeseries":
        p["fieldConfig"]["defaults"]["custom"] = {"lineWidth": 1, "fillOpacity": 10, "showPoints": "never"}
        p["options"] = {"legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull", "max"]},
                        "tooltip": {"mode": "multi"}}
    if thresholds:
        p["fieldConfig"]["defaults"]["thresholds"] = {"mode": "absolute", "steps": thresholds}
        p["fieldConfig"]["defaults"]["color"] = {"mode": "thresholds"}
    if extra:
        p.update(extra)
    return p


def row(title, y):
    return {"id": _next(), "type": "row", "title": title, "collapsed": False,
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


def stat(title, expr, x, y, w, unit="short", thresholds=None, desc=""):
    return panel("stat", title, [target(expr)], x, y, w, 4, unit, desc, thresholds,
                 {"options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background", "graphMode": "none"}})


green, amber, red = "green", "orange", "red"
steps = lambda *s: [{"color": c, "value": v} for c, v in s]

panels = []
y = 0
panels.append(row("Health at a glance", y)); y += 1
panels += [
    stat("Oldest ready job age", 'max(jobscheduler_oldest_ready_job_age_seconds)', 0, y, 4, "s",
         steps((green, None), (amber, 60), (red, 300)), "How long the longest-waiting runnable job has waited. The single best 'are workers keeping up?' signal."),
    stat("Pending + retrying", 'sum(max by (queue, status) (jobscheduler_queue_depth{status=~"pending|retrying"}))', 4, y, 4, "short",
         steps((green, None), (amber, 1000), (red, 10000))),
    stat("Running", 'sum(max by (queue, status) (jobscheduler_queue_depth{status="running"}))', 8, y, 4),
    stat("Dead-letter queue", 'sum(max by (queue) (jobscheduler_dlq_size))', 12, y, 4, "short",
         steps((green, None), (red, 1)), "Jobs that exhausted retries. Should be 0; see runbooks/dead-letter-queue.md."),
    stat("Scheduler leaders", 'sum(jobscheduler_scheduler_is_leader)', 16, y, 4, "short",
         steps((red, None), (green, 1), (red, 2)), "Exactly 1 is healthy. 0 = cron not firing."),
    stat("Pickup SLO (30s), 1h", 'jobscheduler:pickup_within_30s:ratio_rate1h', 20, y, 4, "percentunit",
         steps((red, None), (amber, 0.98), (green, 0.99)), "Target 99% of jobs claimed within 30s of becoming runnable."),
]
y += 4
panels.append(row("Queue", y)); y += 1
panels += [
    panel("timeseries", "Queue depth by status",
          [target('sum by (queue, status) (max by (queue, status) (jobscheduler_queue_depth))', "{{queue}} / {{status}}")],
          0, y, 12, 8, desc="max by() de-duplicates when more than one replica exports the series."),
    panel("timeseries", "Oldest ready job age",
          [target('max by (queue) (jobscheduler_oldest_ready_job_age_seconds)', "{{queue}}")],
          12, y, 12, 8, "s"),
]
y += 8
panels += [
    panel("timeseries", "Pickup latency (run_at to claim)",
          [target('histogram_quantile(0.5, sum by (le) (rate(jobscheduler_job_pickup_latency_seconds_bucket[5m])))', "p50", "A"),
           target('histogram_quantile(0.95, sum by (le) (rate(jobscheduler_job_pickup_latency_seconds_bucket[5m])))', "p95", "B"),
           target('histogram_quantile(0.99, sum by (le) (rate(jobscheduler_job_pickup_latency_seconds_bucket[5m])))', "p99", "C")],
          0, y, 12, 8, "s", "Measured on the database clock, so worker clock skew cannot distort it."),
    panel("timeseries", "Throughput by outcome (jobs/s)",
          [target('sum by (outcome) (rate(jobscheduler_job_duration_seconds_count[1m]))', "{{outcome}}")],
          12, y, 12, 8, "ops"),
]
y += 8
panels.append(row("Execution", y)); y += 1
panels += [
    panel("timeseries", "Handler duration p95 by type",
          [target('histogram_quantile(0.95, sum by (le, type) (rate(jobscheduler_job_duration_seconds_bucket{outcome="succeeded"}[5m])))', "{{type}}")],
          0, y, 8, 8, "s"),
    panel("timeseries", "Retries per second by type",
          [target('sum by (type) (rate(jobscheduler_job_retries_total[5m]))', "{{type}}")],
          8, y, 8, 8, "ops"),
    panel("timeseries", "Jobs reclaimed from expired leases",
          [target('sum by (result) (rate(jobscheduler_jobs_reaped_total[5m]))', "{{result}}")],
          16, y, 8, 8, "ops", "Non-zero means a worker crashed, was killed, or lost the database mid-job."),
]
y += 8
panels += [
    panel("timeseries", "Claim rate by worker pod",
          [target('sum by (instance) (rate(jobscheduler_jobs_claimed_total[1m]))', "{{instance}}")],
          0, y, 12, 8, "ops", "Even distribution across workers shows SKIP LOCKED is spreading load."),
    panel("timeseries", "Retention sweeper deletions",
          [target('sum(rate(jobscheduler_jobs_retention_deleted_total[5m]))', "deleted/s")],
          12, y, 12, 8, "ops"),
]
y += 8
panels.append(row("Scheduler", y)); y += 1
panels += [
    panel("timeseries", "Leadership per replica",
          [target('jobscheduler_scheduler_is_leader{job="scheduler"}', "{{instance}}")], 0, y, 8, 7),
    panel("timeseries", "Cron fires",
          [target('sum by (result) (rate(jobscheduler_scheduler_fires_total[5m]))', "{{result}}")], 8, y, 8, 7, "ops",
          "duplicate_prevented > 0 means the idempotent key caught a double-fire attempt."),
]
y += 7
panels.append(row("API", y)); y += 1
panels += [
    panel("timeseries", "Request rate by route",
          [target('sum by (route, code) (rate(jobscheduler_http_requests_total[1m]))', "{{route}} {{code}}")],
          0, y, 8, 8, "reqps"),
    panel("timeseries", "POST /jobs latency",
          [target('histogram_quantile(0.5, sum by (le) (rate(jobscheduler_http_request_duration_seconds_bucket{route="POST /jobs"}[5m])))', "p50", "A"),
           target('histogram_quantile(0.99, sum by (le) (rate(jobscheduler_http_request_duration_seconds_bucket{route="POST /jobs"}[5m])))', "p99", "B")],
          8, y, 8, 8, "s"),
    panel("timeseries", "Jobs submitted",
          [target('sum by (created) (rate(jobscheduler_jobs_submitted_total[1m]))', "created={{created}}")],
          16, y, 4, 8, "ops", "created=false are idempotency-key hits (client retries)."),
    panel("timeseries", "Rejected by backpressure (429)",
          [target('sum by (queue, reason) (rate(jobscheduler_submissions_rejected_total[1m]))', "{{queue}} {{reason}}")],
          20, y, 4, 8, "ops", "Non-zero means a tenant hit its max_backlog: producers are being told to slow down."),
]
y += 8
panels.append(row("Logs and traces", y)); y += 1
panels += [
    panel("logs", "Worker errors and warnings",
          [target('{service="worker"} | json | level=~"WARN|ERROR"', "", "A", LOKI)], 0, y, 24, 9,
          extra={"options": {"showTime": True, "wrapLogMessage": True, "enableLogDetails": True}}),
]

dashboard = {
    "uid": "jobscheduler", "title": "Job Scheduler", "tags": ["jobscheduler"],
    "timezone": "browser", "schemaVersion": 39, "version": 1, "refresh": "10s",
    "time": {"from": "now-30m", "to": "now"}, "panels": panels, "templating": {"list": []},
    "annotations": {"list": []},
}

out = pathlib.Path(__file__).resolve().parents[1] / "dashboards" / "jobscheduler.json"
out.write_text(json.dumps(dashboard, indent=2) + "\n")
print(f"wrote {out} ({len(panels)} panels)")
