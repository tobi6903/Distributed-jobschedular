#!/usr/bin/env bash
# Measures LISTEN/NOTIFY wake-ups against plain polling on the Compose stack, in three modes:
#   poll       NOTIFY=false                         workers poll every ~1s (the default)
#   notify     NOTIFY=true                          idle workers are woken by a notification; the poll stays at 1s
#   notify10s  NOTIFY=true + 10s fallback poll      the poll is only a safety net: fewer idle queries, but delayed
#                                                   and retried jobs (which nothing announces) can wait up to 10s
#
# Three things are measured, and each can contradict the idea:
#   1. Pickup latency of a job submitted to an IDLE fleet. Ground truth is pickup_ms in the worker's own
#      "job finished" log line (database clock: claim time minus run_at), one value per job, not a histogram.
#   2. Idle load: empty claim polls per second (jobscheduler_claim_polls_total), and the wake-up outcomes at 8
#      workers (how many woken workers lose the race for the one job: the thundering herd).
#   3. Submit throughput and p99. NOTIFY takes a lock at commit, so if it were sent inside the insert path it would
#      slow every submit. Here it is sent by a separate coalesced statement, and this checks that it really costs nothing.
#
#   make up && ./scripts/perf-notify.sh        # about 25 minutes
#
# Variables: MODES ("poll notify notify10s"), WORKERS_LIST ("2 8"), JOBS (40), IDLE (70), SUBMIT_RUNS (3), SUBMIT_JOBS (20000),
# PHASES ("latency submit"; "submit" alone re-measures just the throughput check, which needs more runs on a noisy host).
# Writes docs/perf/data/notify-{latency,idle,submit}.csv, which scripts/perf-charts.py draws.
source "$(dirname "$0")/lib-compose.sh"
OUT="$ROOT/docs/perf/data"; mkdir -p "$OUT"
MODES=${MODES:-"poll notify notify10s"}; WORKERS_LIST=${WORKERS_LIST:-"2 8"}
JOBS=${JOBS:-40}; IDLE=${IDLE:-70}; SUBMIT_RUNS=${SUBMIT_RUNS:-3}; SUBMIT_JOBS=${SUBMIT_JOBS:-20000}
PHASES=${PHASES:-"latency submit"}
phase() { [[ " $PHASES " == *" $1 "* ]]; }
export PATH="$HOME/sdk/go/bin:$PATH"
wait_ready
workers_running() { "${COMPOSE[@]}" ps worker --status running -q | wc -l; }
prom() { curl -fsS --data-urlencode "query=$1" "$PROM/api/v1/query" | python3 -c "
import sys,json
r=json.load(sys.stdin)['data']['result']
print(r[0]['value'][1] if r else 0)"; }

# set_mode <mode> <workers>: recreate the producers and the workers with that mode's settings. (rm, not
# --force-recreate: that keeps renaming containers, which breaks chaos-docker.sh.)
set_mode() {
  local notify=false fb=0s
  case $1 in notify) notify=true;; notify10s) notify=true; fb=10s;; esac
  "${COMPOSE[@]}" rm -sf api scheduler worker >/dev/null 2>&1
  NOTIFY=$notify WORKER_NOTIFY_FALLBACK_POLL=$fb "${COMPOSE[@]}" up -d --no-deps --scale worker="$2" --scale scheduler=2 api scheduler worker >/dev/null 2>&1
  for _ in $(seq 90); do [ "$(workers_running)" = "$2" ] && break; sleep 1; done
  wait_ready 90; sleep 10   # listeners connect and every worker settles into its idle wait
  if [ "$notify" = true ]; then
    # Count LISTEN sessions in Postgres itself (immediate and exact; the Prometheus gauge lags by a scrape). A fleet that
    # is only partly listening would make a "notify" result look worse than it is, so refuse to measure.
    local up=0
    for _ in $(seq 60); do
      up=$(psql_ -c "select count(*) from pg_stat_activity where query like 'LISTEN%' and pid <> pg_backend_pid()")
      [ "$up" -ge "$2" ] && break; sleep 1
    done
    [ "$up" -ge "$2" ] || { echo "ERROR: only $up of $2 workers are listening; not measuring" >&2; exit 1; }
  fi
}

KEY=$(mint_key perf-notify); export KEY
phase latency && echo "mode,workers,job,pickup_ms" > "$OUT/notify-latency.csv"
phase latency && echo "mode,workers,empty_polls_per_s,wakeups_claimed,wakeups_empty,jobs" > "$OUT/notify-idle.csv"
phase submit && echo "mode,run,req_per_s,p50_ms,p99_ms" > "$OUT/notify-submit.csv"

phase latency && echo "== 1 + 2: idle pickup latency and idle load"
for n in $(phase latency && echo $WORKERS_LIST); do
  for mode in $MODES; do
    set_mode "$mode" "$n"
    psql_ -c "truncate jobs; delete from schedules"
    T0=$(date -u +%Y-%m-%dT%H:%M:%SZ); S0=$(date +%s)
    for i in $(seq "$JOBS"); do
      sleep "$(python3 -c "import random; print(round(random.uniform(0.7, 1.7), 2))")"   # random phase against every poll timer
      api POST /jobs '{"type":"noop"}' >/dev/null
    done
    sleep 4
    "${COMPOSE[@]}" logs worker --no-log-prefix --since "$T0" 2>&1 | python3 -c "
import sys,json
mode,n='$mode',$n
i=0
for line in sys.stdin:
    if '\"job finished\"' not in line: continue
    try: d=json.loads(line[line.index('{'):])
    except ValueError: continue
    if d.get('tenant_id')!='perf-notify' or 'pickup_ms' not in d: continue
    i+=1; print(f'{mode},{n},{i},{d[\"pickup_ms\"]}')" >> "$OUT/notify-latency.csv"
    sleep "$IDLE"                                  # nothing is queued now: this is pure idle polling
    sleep 20                                       # let Prometheus scrape the last values
    W=$(( $(date +%s) - S0 + 5 ))
    polls=$(prom 'sum(rate(jobscheduler_claim_polls_total{result="empty"}[60s]))')
    wc=$(prom "sum(increase(jobscheduler_worker_wakeups_total{outcome=\"claimed\"}[${W}s]))")
    we=$(prom "sum(increase(jobscheduler_worker_wakeups_total{outcome=\"empty\"}[${W}s]))")
    got=$(psql_ -c "select count(*) from jobs where tenant_id='perf-notify' and status='succeeded'")
    python3 -c "print('$mode,$n,%.2f,%.0f,%.0f,$got' % ($polls, $wc, $we))" | tee -a "$OUT/notify-idle.csv" | sed 's/^/  mode,workers,empty polls\/s,wakeups claimed,wakeups empty,jobs = /'
    python3 - "$OUT/notify-latency.csv" "$mode" "$n" <<'EOF'
import csv,sys,statistics as st
rows=[float(r['pickup_ms']) for r in csv.DictReader(open(sys.argv[1])) if r['mode']==sys.argv[2] and r['workers']==sys.argv[3]]
rows.sort()
print(f"  pickup ms: n={len(rows)} p50={st.median(rows):.0f} p95={rows[int(len(rows)*.95)-1]:.0f} max={rows[-1]:.0f}")
EOF
  done
done

phase submit && echo "== 3: submit throughput with and without notifications (modes alternate within each run)"
BIN=$(mktemp -d)/loadgen; phase submit && (cd "$ROOT" && go build -o "$BIN" ./test/load/loadgen)
for run in $(phase submit && seq "$SUBMIT_RUNS"); do
  for mode in poll notify; do
    set_mode "$mode" 2
    psql_ -c "truncate jobs; delete from schedules"
    out=$("$BIN" -api "$API" -key "$KEY" -n "$SUBMIT_JOBS" -c 64 2>&1)
    echo "$out" | python3 -c "
import sys,re
t=sys.stdin.read()
rps=re.search(r'=>\s+(\d+) req/s',t).group(1); p50=re.search(r'p50=([\d.]+)ms',t).group(1); p99=re.search(r'p99=([\d.]+)(ms|s)',t)
v=float(p99.group(1))*(1000 if p99.group(2)=='s' else 1)
print(f'$mode,$run,{rps},{p50},{v:.1f}')" | tee -a "$OUT/notify-submit.csv" | sed 's/^/  mode,run,req\/s,p50 ms,p99 ms = /'
    for _ in $(seq 300); do [ "$(psql_ -c "select count(*) from jobs where status <> 'succeeded'")" = 0 ] && break; sleep 1; done
  done
done
set_mode poll 2   # leave the stack as it started
echo "wrote $OUT/notify-{latency,idle,submit}.csv"
