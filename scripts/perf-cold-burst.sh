#!/usr/bin/env bash
# Reproduces the slow start of a burst on freshly started workers, and shows the fix.
#
# Each run: remove the workers, empty the jobs table, start 4 fresh workers, let them idle (they poll the empty table),
# then insert 60,000 jobs at once and time the drain. The per-worker numbers and the per-second throughput come from the
# workers' own "job finished" log lines, not from Prometheus (see docs/DESIGN.md section 20 for why).
#
#   mode "auto"    DATABASE_URL carries plan_cache_mode=auto: the behaviour before the fix (an explicit DSN setting wins)
#   mode "custom"  the shipped default (force_custom_plan)
#
# Modes alternate within each run index, so slow drift on a shared machine hits both alike.
#   make up && ./scripts/perf-cold-burst.sh          # about 12 minutes
# Variables: RUNS (4), JOBS (60000), SETTLE (30, idle seconds before the burst).
source "$(dirname "$0")/lib-compose.sh"
OUT="$ROOT/docs/perf/data"; mkdir -p "$OUT"
RUNS=${RUNS:-4}; JOBS=${JOBS:-60000}; SETTLE=${SETTLE:-30}
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
cat > "$TMP/auto.yml" <<'YML'
services:
  worker:
    environment:
      DATABASE_URL: postgres://jobs:jobs@postgres:5432/jobs?sslmode=disable&plan_cache_mode=auto
YML
wait_ready
echo "mode,run,drain_seconds,first_1000_per_s_at,worker_min,worker_max" > "$OUT/cold-burst.csv"
echo "mode,run,t,jobs_per_second" > "$OUT/cold-burst-series.csv"
for run in $(seq "$RUNS"); do
  for mode in auto custom; do
    C=("${COMPOSE[@]}"); [ "$mode" = auto ] && C+=(-f "$TMP/auto.yml")
    "${C[@]}" rm -sf worker >/dev/null 2>&1
    psql_ -c "truncate jobs; delete from schedules"
    "${C[@]}" up -d --no-deps --scale worker=4 worker >/dev/null 2>&1
    for _ in $(seq 60); do [ "$("${C[@]}" ps worker --status running -q | wc -l)" = 4 ] && break; sleep 1; done
    sleep "$SETTLE"
    BS=$(date +%s); BSN=$(date -u +%Y-%m-%dT%H:%M:%S)
    psql_ -c "insert into jobs (tenant_id, type) select 'burst', 'noop' from generate_series(1, $JOBS)"
    while [ "$(psql_ -c "select count(*) from jobs where status <> 'succeeded'")" != 0 ]; do sleep 0.5; done
    BE=$(date +%s); sleep 1
    for n in 1 2 3 4; do docker logs --timestamps --since "$BSN" "jobscheduler-worker-$n" > "$TMP/w$n.log" 2>&1; done
    python3 - "$mode" "$run" "$BS" "$BE" "$TMP" "$OUT" <<'PY'
import collections, datetime as dt, json, sys
mode, run, bs, be, tmp, out = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5], sys.argv[6]
tot, per = collections.Counter(), []
for n in (1, 2, 3, 4):
    k = 0
    for line in open(f"{tmp}/w{n}.log"):
        i = line.find("{")
        try: e = json.loads(line[i:])
        except ValueError: continue
        if e.get("msg") == "job finished":
            t = int(dt.datetime.fromisoformat(e["time"].replace("Z", "+00:00")).timestamp()) - bs
            tot[t] += 1; k += 1
    per.append(k)
span = be - bs + 1
first = next((s for s in range(span + 1) if tot[s] >= 1000), -1)
with open(f"{out}/cold-burst.csv", "a") as f: f.write(f"{mode},{run},{be-bs},{first},{min(per)},{max(per)}\n")
with open(f"{out}/cold-burst-series.csv", "a") as f:
    for s in range(span + 1): f.write(f"{mode},{run},{s},{tot[s]}\n")
print(f"  {mode:6} run {run}: drained in {be-bs:3d}s, 1,000 jobs/s reached at t={first}s, per-worker {min(per)}..{max(per)}", file=sys.stderr)
PY
  done
done
"${COMPOSE[@]}" rm -sf worker >/dev/null 2>&1
"${COMPOSE[@]}" up -d --no-deps --scale worker=2 worker >/dev/null 2>&1
