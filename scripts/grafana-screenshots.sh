#!/usr/bin/env bash
# Renders the Grafana dashboard (and individual panels) to PNG with the Grafana image renderer.
#   docker compose -f deploy/docker/docker-compose.yml --profile render up -d renderer
#   ./scripts/grafana-screenshots.sh <from_epoch_seconds> <to_epoch_seconds> [name_prefix]
# Output: docs/images/grafana-[<prefix>-]dashboard.png and grafana-[<prefix>-]panel-*.png
# FULL=0 skips the whole-dashboard image, PANELS=0 skips the individual panels.
source "$(dirname "$0")/lib-compose.sh"
FROM=${1:?from epoch seconds}; TO=${2:?to epoch seconds}; PFX=${3:+$3-}
G="${GRAFANA_URL:-http://admin:admin@localhost:${GRAFANA_PORT:-3000}}"; OUT="$ROOT/docs/images"; mkdir -p "$OUT"
q="orgId=1&from=$(( FROM * 1000 ))&to=$(( TO * 1000 ))&theme=light&kiosk"

render() { # name url-path extra-query width height
  local f="$OUT/grafana-$PFX$1.png"
  if curl -fsS -m 180 -o "$f" "$G/render/$2?$q&$3&width=$4&height=$5" && file "$f" | grep -q PNG; then
    echo "  $1: $(du -h "$f" | cut -f1)"; else echo "  $1: FAILED" >&2; rm -f "$f"; return 1; fi
}
# the renderer needs a moment to start, and Grafana has to know about it
for _ in $(seq 30); do curl -fsS -m 5 "$G/api/health" >/dev/null 2>&1 && break; sleep 2; done

[ "${FULL:-1}" = 1 ] && { echo "full dashboard"; render dashboard "d/jobscheduler/job-scheduler" "" 1500 2500; }
# individual panels by title -> panel id (read from the provisioned dashboard)
[ "${PANELS:-1}" = 1 ] || exit 0
ids=$(curl -fsS "$G/api/dashboards/uid/jobscheduler" | python3 -c "
import sys,json
d=json.load(sys.stdin)['dashboard']
want=['Queue depth by status','Pickup latency (run_at to claim)','Throughput by outcome (jobs/s)','Jobs reclaimed from expired leases','Claim rate by worker pod','Oldest ready job age','Cron fires']
t={p['title']:p['id'] for p in d['panels']}
for w in want:
    if w in t: print(t[w], w.replace(' ','-').replace('(','').replace(')','').replace('/','').lower())")
echo "panels"
while read -r id name; do [ -n "$id" ] && render "panel-$name" "d-solo/jobscheduler/job-scheduler" "panelId=$id" 900 380; done <<<"$ids"
