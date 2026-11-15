#!/usr/bin/env bash
# Run the API, a worker and two scheduler replicas as plain local processes (no Docker).
# Requires DATABASE_URL pointing at a Postgres you can write to, and `make build` first.
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=.dev
case "${1:-}" in
  up)
    : "${DATABASE_URL:?set DATABASE_URL, e.g. postgres://postgres@localhost:5432/jobs?sslmode=disable}"
    mkdir -p "$RUN"
    ./bin/jobctl migrate up
    start() { # name, env..., binary
      local name=$1; shift
      env "$@" >"$RUN/$name.log" 2>&1 &
      echo $! >"$RUN/$name.pid"
      echo "started $name (pid $(cat "$RUN/$name.pid"), log $RUN/$name.log)"
    }
    start api       HTTP_ADDR=:8080 SHUTDOWN_DELAY=0s ./bin/api
    start worker    METRICS_ADDR=:9100 ./bin/worker
    start scheduler1 METRICS_ADDR=:9101 ./bin/scheduler
    start scheduler2 METRICS_ADDR=:9102 ./bin/scheduler
    echo "create a key:  ./bin/jobctl create-key -tenant demo"
    echo "then:          export JOBCTL_API_KEY=<key>; ./bin/jobctl submit -type noop"
    ;;
  down)
    for f in "$RUN"/*.pid; do [ -e "$f" ] && kill -TERM "$(cat "$f")" 2>/dev/null || true; done
    rm -rf "$RUN"
    ;;
  *) echo "usage: $0 up|down" >&2; exit 2 ;;
esac
