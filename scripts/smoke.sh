#!/usr/bin/env bash
# End-to-end smoke test of the running Compose stack (make up first). It checks the things that
# only a real stack can prove: containers talk to each other, Prometheus scrapes every replica,
# exactly one scheduler leads, a retrying job completes, cron fires, and one trace spans the API
# and worker containers in Tempo.
source "$(dirname "$0")/lib-compose.sh"
FAILED=0
wait_ready
KEY=$(mint_key smoke); export KEY

# 1. A job that fails twice then succeeds: exercises API -> Postgres -> worker -> retry/backoff.
ID=$(api POST /jobs '{"type":"flaky","payload":{"succeed_on_attempt":3}}' | jget "d['id']")
for _ in $(seq 60); do ST=$(api GET /jobs/$ID | jget "d['status']"); [ "$ST" = succeeded ] && break; sleep 1; done
ATT=$(api GET /jobs/$ID | jget "d['attempts']")
[ "$ST" = succeeded ] && [ "$ATT" = 3 ] && pass "retrying job succeeded on attempt 3" || fail "flaky job: status=$ST attempts=$ATT"

# 2. Cron: a every-2s schedule must create jobs, with unique per-slot keys.
api POST /schedules '{"name":"smoke-cron","cron_expr":"@every 2s","misfire_policy":"fire_all","type":"noop"}' >/dev/null
sleep 12
N=$(psql_ -c "select count(*)||' '||count(distinct idempotency_key) from jobs where idempotency_key like 'cron:%'")
set -- $N
[ "$1" -ge 3 ] && [ "$1" = "$2" ] && pass "cron fired $1 slots, all distinct" || fail "cron slots: total=$1 distinct=$2"

# 3. Prometheus scraped every replica (api + 2 workers + 2 schedulers) and exactly one scheduler leads.
sleep 15
UP=$(curl -fsS --data-urlencode 'query=count(up{job=~"api|worker|scheduler"}==1)' "$PROM/api/v1/query" | jget "d['data']['result'][0]['value'][1]")
[ "$UP" -ge 5 ] && pass "Prometheus scrapes $UP/5 replicas" || fail "only $UP/5 targets up"
LEAD=$(curl -fsS --data-urlencode 'query=sum(jobscheduler_scheduler_is_leader)' "$PROM/api/v1/query" | jget "d['data']['result'][0]['value'][1]")
[ "$LEAD" = 1 ] && pass "exactly one scheduler leads" || fail "scheduler leaders = $LEAD"

# 4. One distributed trace spanning the api and worker containers.
ID=$(api POST /jobs '{"type":"noop"}' | jget "d['id']"); sleep 10
TRACE=$("${COMPOSE[@]}" logs worker --no-log-prefix 2>&1 | grep "\"job_id\":\"$ID\"" | grep -o '"trace_id":"[0-9a-f]*"' | head -1 | cut -d'"' -f4)
SVCS=$("${COMPOSE[@]}" exec -T grafana wget -qO- "http://tempo:3200/api/traces/$TRACE" 2>/dev/null | python3 -c "
import sys,json
d=json.load(sys.stdin); print(','.join(sorted({a['value']['stringValue'] for b in d['batches'] for a in b['resource']['attributes'] if a['key']=='service.name'})))" 2>/dev/null || true)
[ "$SVCS" = "jobscheduler-api,jobscheduler-worker" ] && pass "one trace spans the api and worker containers" || fail "trace services: '${SVCS}'"

[ "$FAILED" = 0 ] && echo "SMOKE PASSED" || { echo "SMOKE FAILED"; exit 1; }
