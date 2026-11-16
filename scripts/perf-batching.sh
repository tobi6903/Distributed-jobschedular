#!/usr/bin/env bash
# Measures completion batching: drain a backlog of no-op jobs with batching off, with group commit, and with group commit
# plus a 5 ms linger, at several worker counts, and record Postgres commits per job (the mechanism) next to jobs/s (the
# effect). Writes docs/perf/data/batching.csv, which scripts/perf-charts.py draws. The modes are measured back to back
# for each worker count so drift on a shared machine hits all of them alike.
#
#   make up && ./scripts/perf-batching.sh          # about 8 minutes
#
# Variables: WORKERS_LIST ("1 2 4 8"), BACKLOG (20000), RUNS (5), MODES ("off group linger5ms").
source "$(dirname "$0")/lib-compose.sh"
OUT="$ROOT/docs/perf/data"; mkdir -p "$OUT"
WORKERS_LIST=${WORKERS_LIST:-"1 2 4 8"}; BACKLOG=${BACKLOG:-20000}; RUNS=${RUNS:-5}; MODES=${MODES:-"off group linger5ms"}
wait_ready
workers_running() { "${COMPOSE[@]}" ps worker --status running -q | wc -l; }
commits() { psql_ -c "select xact_commit from pg_stat_database where datname = 'jobs'"; }

echo "mode,workers,run,jobs,seconds,jobs_per_second,commits_per_job" > "$OUT/batching.csv"
for n in $WORKERS_LIST; do
  for mode in $MODES; do
    batch=false; linger=0s
    case $mode in group) batch=true;; linger5ms) batch=true; linger=5ms;; esac
    # The environment changes between modes, so the containers are removed and recreated. (rm, not --force-recreate:
    # that keeps renaming them worker-18, worker-22..., which breaks chaos-docker.sh, which expects worker-1/2.)
    "${COMPOSE[@]}" rm -sf worker >/dev/null 2>&1
    WORKER_COMPLETE_BATCH=$batch WORKER_COMPLETE_BATCH_LINGER=$linger "${COMPOSE[@]}" up -d --no-deps --scale worker="$n" worker >/dev/null 2>&1
    for _ in $(seq 60); do [ "$(workers_running)" = "$n" ] && break; sleep 1; done; sleep 6
    for run in $(seq "$RUNS"); do
      psql_ -c "truncate jobs; delete from schedules"
      psql_ -c "insert into jobs (tenant_id, type) select 'perf', 'noop' from generate_series(1, $BACKLOG)"
      sleep 1
      c0=$(commits); t0=$(date +%s.%N)
      while [ "$(psql_ -c "select count(*) from jobs where status <> 'succeeded'")" != 0 ]; do sleep 0.25; done
      t1=$(date +%s.%N); c1=$(commits)
      python3 -c "
m,n,run,j,a,b,c0,c1=('$mode',$n,$run,$BACKLOG,$t0,$t1,$c0,$c1)
print(f'{m},{n},{run},{j},{b-a:.2f},{j/(b-a):.0f},{(c1-c0)/j:.2f}')" | tee -a "$OUT/batching.csv" | sed 's/^/  mode,workers,run,jobs,seconds,jobs\/s,commits\/job = /'
    done
  done
done
"${COMPOSE[@]}" rm -sf worker >/dev/null 2>&1
WORKER_COMPLETE_BATCH=false WORKER_COMPLETE_BATCH_LINGER=0s "${COMPOSE[@]}" up -d --no-deps --scale worker=2 worker >/dev/null 2>&1
