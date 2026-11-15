#!/usr/bin/env python3
"""Fetches the Prometheus time series behind the burst chart.

    python3 scripts/perf-prom-range.py http://localhost:9090 <start_epoch> <end_epoch> docs/perf/data/burst.json

Standalone so the series can be re-queried for a past window (Prometheus keeps the data) without rerunning
the experiment. Rate windows are 20 s, i.e. four 5 s scrapes.
"""
import json
import sys
import urllib.parse
import urllib.request

prom, start, end, out = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
queries = {
    # database-derived gauge, exported by the scheduler leader (scraped every 5 s, cached 5 s)
    "backlog": 'sum(max by (queue,status) (jobscheduler_queue_depth{status=~"pending|retrying"}))',
    "throughput": 'sum(rate(jobscheduler_job_duration_seconds_count{outcome="succeeded"}[20s]))',
    # does SKIP LOCKED share the work? the busiest and the least busy worker, claims per second
    "claims_max_per_worker": "max(rate(jobscheduler_jobs_claimed_total[20s]))",
    "claims_min_per_worker": "min(rate(jobscheduler_jobs_claimed_total[20s]))",
    "oldest_ready": "max(jobscheduler_oldest_ready_job_age_seconds)",
}
res = {"start": start, "end": end, "series": {}}
for name, q in queries.items():
    url = prom + "/api/v1/query_range?" + urllib.parse.urlencode({"query": q, "start": start, "end": end, "step": 2})
    r = json.load(urllib.request.urlopen(url))["data"]["result"]
    res["series"][name] = [[float(t), float(v)] for t, v in (r[0]["values"] if r else []) if v != "NaN"]
    print(f"  {name}: {len(res['series'][name])} points")
json.dump(res, open(out, "w"))
