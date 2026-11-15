#!/usr/bin/env bash
# Measures the platform on the running Compose stack and writes raw data to docs/perf/data/ (CSV/JSON), which
# scripts/perf-charts.py turns into the charts in docs/images/. Everything is measured, nothing is typed in.
#
#   make up && ./scripts/perf-collect.sh          # about 8 minutes
#
# Needs: docker, curl, python3, k6. Numbers depend on the machine (the README states which one).
source "$(dirname "$0")/lib-compose.sh"
OUT="$ROOT/docs/perf/data"; mkdir -p "$OUT"
command -v k6 >/dev/null || { echo "k6 is required" >&2; exit 1; }
WORKERS_LIST=${WORKERS_LIST:-"1 2 4 8"}; BACKLOG=${BACKLOG:-20000}; RUNS=${RUNS:-3}
RATES=${RATES:-"100 200 300 400 500"}; LOAD_SECONDS=${LOAD_SECONDS:-20}
wait_ready
KEY=$(mint_key perf); export KEY
T_ALL_START=$(date +%s)
mkdir -p /tmp/perf.$$; trap 'rm -rf /tmp/perf.$$' EXIT

drain_pending() { for _ in $(seq 120); do [ "$(psql_ -c "select count(*) from jobs where status in ('pending','retrying','running')")" = 0 ] && return 0; sleep 1; done; }
workers_running() { "${COMPOSE[@]}" ps worker --status running -q | wc -l; }

echo "== 1. drain throughput vs number of worker containers ($BACKLOG no-op jobs each run, 20 slots per worker)"
echo "workers,run,jobs,seconds,jobs_per_second" > "$OUT/scale.csv"
for n in $WORKERS_LIST; do
  "${COMPOSE[@]}" up -d --no-deps --scale worker="$n" worker >/dev/null 2>&1
  for _ in $(seq 60); do [ "$(workers_running)" = "$n" ] && break; sleep 1; done; sleep 6   # let them start polling
  for run in $(seq "$RUNS"); do
    psql_ -c "truncate jobs; delete from schedules"
    psql_ -c "insert into jobs (tenant_id, type) select 'perf', 'noop' from generate_series(1, $BACKLOG)"
    t0=$(date +%s.%N)
    while [ "$(psql_ -c "select count(*) from jobs where status <> 'succeeded'")" != 0 ]; do sleep 0.25; done
    t1=$(date +%s.%N)
    python3 -c "
n,run,j,a,b=$n,$run,$BACKLOG,$t0,$t1
print(f'{n},{run},{j},{b-a:.2f},{j/(b-a):.0f}')" | tee -a "$OUT/scale.csv" | sed 's/^/  workers,run,jobs,seconds,jobs\/s = /'
  done
done
"${COMPOSE[@]}" up -d --no-deps --scale worker=2 worker >/dev/null 2>&1; sleep 8

k6_run() { # rate seconds outfile
  K6_NO_USAGE_REPORT=true k6 run -q -e API_URL="$API" -e API_KEY="$KEY" -e RATE="$1" -e DURATION="${2}s" \
    --summary-trend-stats="avg,med,p(90),p(95),p(99),max" --summary-export "$3" "$ROOT/test/load/k6.js" >/dev/null 2>&1 || true; }
k6_row() { python3 - "$1" "$2" <<'PY'
import json,sys
d=json.load(open(sys.argv[1])); m=d['metrics']; get=lambda k:(m[k].get('values',m[k]) if k in m else {})
lat=get('http_req_duration{scenario:submit}') or get('http_req_duration')
reqs=get('http_reqs'); drop=get('dropped_iterations'); fail=get('http_req_failed{scenario:submit}') or get('http_req_failed')
secs=float(sys.argv[2])
# k6 counts all iterations (submit scenario + 1/s end-to-end sampler); achieved submit rate = total requests / duration.
achieved=reqs.get('count',0)/secs
print(f"{lat.get('med',0):.1f},{lat.get('p(95)',0):.1f},{lat.get('p(99)',0):.1f},{achieved:.0f},{int(drop.get('count',0))},{fail.get('value',fail.get('rate',0))*100:.2f}")
PY
}

echo "== 2. submit latency vs offered load (k6, $LOAD_SECONDS s per rate, 2 workers, API pool 40)"
echo "offered_rps,p50_ms,p95_ms,p99_ms,achieved_rps,dropped_iterations,failed_pct" > "$OUT/load.csv"
for r in $RATES; do
  drain_pending; psql_ -c "truncate jobs" ; sleep 2
  k6_run "$r" "$LOAD_SECONDS" /tmp/perf.$$/k6-$r.json
  row=$(k6_row /tmp/perf.$$/k6-$r.json "$LOAD_SECONDS"); echo "$r,$row" | tee -a "$OUT/load.csv" | sed 's/^/  offered,p50,p95,p99,achieved,dropped,failed% = /'
done

echo "== 3. API connection pool size at 400 req/s: on an idle database, and while workers hammer it with commits"
echo "scenario,pool,p50_ms,p95_ms,p99_ms,achieved_rps,dropped_iterations,failed_pct" > "$OUT/pool.csv"
for scenario in idle pressure; do
  for c in 10 40; do
    API_DB_MAX_CONNS=$c "${COMPOSE[@]}" up -d --no-deps --force-recreate api >/dev/null 2>&1; wait_ready 60; sleep 3
    if [ "$scenario" = pressure ]; then "${COMPOSE[@]}" up -d --no-deps --scale worker=8 worker >/dev/null 2>&1; sleep 10; fi
    drain_pending; psql_ -c "truncate jobs"; sleep 2
    if [ "$scenario" = pressure ]; then   # a big backlog drains throughout the measurement: every completion is a commit
      psql_ -c "insert into jobs (tenant_id, type) select 'bg', 'noop' from generate_series(1, 60000)"; fi
    k6_run 400 "$LOAD_SECONDS" /tmp/perf.$$/k6-pool-$scenario-$c.json
    row=$(k6_row /tmp/perf.$$/k6-pool-$scenario-$c.json "$LOAD_SECONDS"); echo "$scenario,$c,$row" | tee -a "$OUT/pool.csv" | sed 's/^/  scenario,pool,p50,p95,p99,achieved,dropped,failed% = /'
    if [ "$scenario" = pressure ]; then "${COMPOSE[@]}" up -d --no-deps --scale worker=2 worker >/dev/null 2>&1; sleep 5; fi
  done
done
"${COMPOSE[@]}" up -d --no-deps --force-recreate api >/dev/null 2>&1; wait_ready 60   # back to the shipped default

echo "== 4. burst: 60,000 jobs arrive at once and 4 workers drain them (clean, no kills)"
"${COMPOSE[@]}" up -d --no-deps --scale worker=4 worker >/dev/null 2>&1
for _ in $(seq 60); do [ "$(workers_running)" = 4 ] && break; sleep 1; done
drain_pending; psql_ -c "truncate jobs; delete from schedules"
echo "  letting metrics settle for 60 s so rate windows contain only this run..."; sleep 60
BS=$(date +%s)
psql_ -c "insert into jobs (tenant_id, type) select 'burst', 'noop' from generate_series(1, 60000)"
while [ "$(psql_ -c "select count(*) from jobs where status <> 'succeeded'")" != 0 ]; do sleep 0.5; done
BE=$(date +%s); sleep 25   # a tail of quiet, so the recovery is visible
echo "  drained 60000 jobs in $((BE-BS))s"; echo "$BS $BE" > "$OUT/burst.window"
python3 "$ROOT/scripts/perf-prom-range.py" "$PROM" "$((BS-20))" "$((BE+25))" "$OUT/burst.json"

echo "== 5. chaos run with a recorded time series (a container SIGKILLed every ~4s for 40s)"
"${COMPOSE[@]}" up -d --no-deps --scale worker=2 worker >/dev/null 2>&1; sleep 8
JOBS=${CHAOS_JOBS:-6000} KILL_SECONDS=40 CSV="$OUT/chaos.csv" "$ROOT/scripts/chaos-docker.sh" | tail -8
echo "$T_ALL_START $(date +%s)" > "$OUT/window.txt"
echo "done: $(ls "$OUT" | tr '\n' ' ')"
