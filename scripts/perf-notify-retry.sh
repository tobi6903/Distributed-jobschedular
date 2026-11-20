#!/usr/bin/env bash
# The cost side of the LISTEN/NOTIFY fallback poll: retried jobs are not announced (nothing can announce a moment in the
# future), so while workers are listening and the fallback poll is long, a retry is found only by the poll. This measures
# how late. A job that fails once is retried after its backoff; the second attempt's pickup_ms (claim time minus run_at,
# database clock, from the worker's own log line) is exactly how late the retry started.
#
#   make up && ./scripts/perf-notify-retry.sh        # about 4 minutes
#
# Variables: MODES ("poll notify notify10s"), JOBS (12), WORKERS (2). Writes docs/perf/data/notify-retry.csv.
source "$(dirname "$0")/lib-compose.sh"
OUT="$ROOT/docs/perf/data"; mkdir -p "$OUT"
MODES=${MODES:-"poll notify notify10s"}; JOBS=${JOBS:-12}; WORKERS=${WORKERS:-2}
wait_ready
workers_running() { "${COMPOSE[@]}" ps worker --status running -q | wc -l; }
KEY=$(mint_key perf-retry); export KEY
echo "mode,job,attempt2_late_ms" > "$OUT/notify-retry.csv"
for mode in $MODES; do
  notify=false; fb=0s
  case $mode in notify) notify=true;; notify10s) notify=true; fb=10s;; esac
  "${COMPOSE[@]}" rm -sf api scheduler worker >/dev/null 2>&1
  NOTIFY=$notify WORKER_NOTIFY_FALLBACK_POLL=$fb BACKOFF_BASE=${BACKOFF_BASE:-2s} "${COMPOSE[@]}" up -d --no-deps --scale worker="$WORKERS" --scale scheduler=2 api scheduler worker >/dev/null 2>&1
  for _ in $(seq 90); do [ "$(workers_running)" = "$WORKERS" ] && break; sleep 1; done
  wait_ready 90
  if [ "$notify" = true ]; then
    for _ in $(seq 60); do
      up=$(psql_ -c "select count(*) from pg_stat_activity where query like 'LISTEN%' and pid <> pg_backend_pid()")
      [ "$up" -ge "$WORKERS" ] && break; sleep 1
    done
    [ "$up" -ge "$WORKERS" ] || { echo "ERROR: only $up of $WORKERS workers are listening" >&2; exit 1; }
  fi
  sleep 10
  psql_ -c "truncate jobs"
  T0=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  for i in $(seq "$JOBS"); do
    api POST /jobs '{"type":"flaky","payload":{"succeed_on_attempt":2}}' >/dev/null
    sleep "$(python3 -c "import random; print(round(random.uniform(0.3, 1.1), 2))")"
  done
  for _ in $(seq 90); do [ "$(psql_ -c "select count(*) from jobs where status <> 'succeeded'")" = 0 ] && break; sleep 1; done
  sleep 3
  "${COMPOSE[@]}" logs worker --no-log-prefix --since "$T0" 2>&1 | python3 -c "
import sys,json
mode='$mode'; i=0
for line in sys.stdin:
    if '\"job finished\"' not in line: continue
    try: d=json.loads(line[line.index('{'):])
    except ValueError: continue
    if d.get('tenant_id')!='perf-retry' or d.get('attempt')!=2 or 'pickup_ms' not in d: continue
    i+=1; print(f'{mode},{i},{d[\"pickup_ms\"]}')" | tee -a "$OUT/notify-retry.csv" >/dev/null
  python3 - "$OUT/notify-retry.csv" "$mode" <<'PY'
import csv,sys,statistics as st
v=sorted(float(r['attempt2_late_ms']) for r in csv.DictReader(open(sys.argv[1])) if r['mode']==sys.argv[2])
print(f"  {sys.argv[2]}: retried jobs n={len(v)}  started late by p50={st.median(v):.0f} ms  max={v[-1]:.0f} ms")
PY
done
"${COMPOSE[@]}" rm -sf api scheduler worker >/dev/null 2>&1
"${COMPOSE[@]}" up -d --no-deps --scale worker=2 --scale scheduler=2 api scheduler worker >/dev/null 2>&1
echo "wrote $OUT/notify-retry.csv"
