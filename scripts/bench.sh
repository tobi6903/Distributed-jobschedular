#!/usr/bin/env bash
# Reproduces the benchmark table in the README.
#
# Runs on one machine: Postgres (you provide), one API process, N worker processes. For each
# worker count it submits JOBS no-op jobs through the HTTP API with the Go load generator and
# waits for all of them to finish.
#
#   ADMIN_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable make bench
#
# "noop" jobs make this a measurement of the queue itself (claim + complete), not of handlers.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${ADMIN_DATABASE_URL:?set ADMIN_DATABASE_URL (a Postgres admin connection string)}"
JOBS=${JOBS:-50000}
WORKER_COUNTS=${WORKER_COUNTS:-"1 2 4 8"}
CONCURRENCY=${CONCURRENCY:-32}
BIN=./bin
DB="bench_$$"
APIPORT=${APIPORT:-18480}
PIDS=()

psql_admin() { psql "$ADMIN_DATABASE_URL" -v ON_ERROR_STOP=1 -Atq "$@"; }
cleanup() {
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill -TERM "$p" 2>/dev/null || true; done
  sleep 1
  psql_admin -c "DROP DATABASE IF EXISTS $DB WITH (FORCE)" >/dev/null 2>&1 || true
}
trap cleanup EXIT

[ -x "$BIN/api" ] || { echo "run 'make build' first"; exit 1; }
export DATABASE_URL="${ADMIN_DATABASE_URL%/*}/$DB?${ADMIN_DATABASE_URL#*\?}"
psql_admin -c "CREATE DATABASE $DB"
"$BIN/jobctl" migrate up >/dev/null
KEY=$("$BIN/jobctl" create-key -tenant bench -name bench)

echo "environment: $(nproc) CPUs, $(uname -sr), $(psql_admin -c 'show server_version'), fsync=$(psql_admin -c 'show fsync'), synchronous_commit=$(psql_admin -c 'show synchronous_commit')"
echo
echo "| workers | concurrency/worker | jobs | submit req/s | submit p50 | submit p99 | end-to-end jobs/s |"
echo "|---|---|---|---|---|---|---|"

HTTP_ADDR=":$APIPORT" SHUTDOWN_DELAY=0s LOG_LEVEL=error DB_MAX_CONNS=40 "$BIN/api" >/dev/null 2>&1 &
PIDS+=($!)
for _ in $(seq 50); do curl -fsS "localhost:$APIPORT/readyz" >/dev/null 2>&1 && break; sleep 0.2; done

for W in $WORKER_COUNTS; do
  psql "$DATABASE_URL" -Atqc "TRUNCATE jobs"
  WPIDS=()
  for i in $(seq "$W"); do
    WORKER_CONCURRENCY=$CONCURRENCY WORKER_POLL_INTERVAL=50ms LOG_LEVEL=error DB_MAX_CONNS=20 METRICS_ADDR=":0" "$BIN/worker" >/dev/null 2>&1 &
    WPIDS+=($!)
  done
  sleep 1
  OUT=$(go run ./test/load/loadgen -api "http://localhost:$APIPORT" -key "$KEY" -n "$JOBS" -c 64 -db "$DATABASE_URL" -tenant bench)
  RPS=$(sed -n 's/.*=>  \([0-9]*\) req\/s.*/\1/p' <<<"$OUT")
  P50=$(sed -n 's/.*p50=\([^ ]*\).*/\1/p' <<<"$OUT")
  P99=$(sed -n 's/.*p99=\([^ ]*\).*/\1/p' <<<"$OUT")
  E2E=$(sed -n 's/.*=>  \([0-9]*\) jobs\/s.*/\1/p' <<<"$OUT")
  echo "| $W | $CONCURRENCY | $JOBS | $RPS | $P50 | $P99 | $E2E |"
  for p in "${WPIDS[@]}"; do kill -TERM "$p" 2>/dev/null || true; done
  wait "${WPIDS[@]}" 2>/dev/null || true
done

# ---- Table 2: drain a preloaded backlog (isolates worker claim+complete throughput) ----
echo
echo "| workers | concurrency/worker | backlog | drain time | jobs/s |"
echo "|---|---|---|---|---|"
for W in $WORKER_COUNTS; do
  psql "$DATABASE_URL" -Atqc "TRUNCATE jobs" 
  psql "$DATABASE_URL" -Atqc "INSERT INTO jobs (tenant_id, type) SELECT 'bench', 'noop' FROM generate_series(1, $JOBS)"
  START=$(date +%s.%N)
  WPIDS=()
  for i in $(seq "$W"); do
    WORKER_CONCURRENCY=$CONCURRENCY WORKER_POLL_INTERVAL=50ms LOG_LEVEL=error DB_MAX_CONNS=20 METRICS_ADDR=":0" "$BIN/worker" >/dev/null 2>&1 &
    WPIDS+=($!)
  done
  while [ "$(psql "$DATABASE_URL" -Atqc "SELECT count(*) FROM jobs WHERE status <> 'succeeded'")" != "0" ]; do sleep 0.1; done
  END=$(date +%s.%N)
  SECS=$(python3 -c "print(round($END-$START,2))")
  echo "| $W | $CONCURRENCY | $JOBS | ${SECS}s | $(python3 -c "print(round($JOBS/($END-$START)))") |"
  for p in "${WPIDS[@]}"; do kill -TERM "$p" 2>/dev/null || true; done
  wait "${WPIDS[@]}" 2>/dev/null || true
done
