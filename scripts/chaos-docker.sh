#!/usr/bin/env bash
# Container-level chaos against the Compose stack (make up first): SIGKILL random worker and
# scheduler containers while 4,000 jobs run and an every-2-seconds cron schedule fires. Passes only
# if every job succeeded and no cron slot was missed or duplicated.
#   JOBS=4000 KILL_SECONDS=40 ./scripts/chaos-docker.sh
# Set CSV=path to also record "t,succeeded,kills" every 2 seconds (for charts); the wall-clock window is
# written next to it as <path>.window ("start_epoch end_epoch").
source "$(dirname "$0")/lib-compose.sh"
JOBS=${JOBS:-4000}; KILL_SECONDS=${KILL_SECONDS:-40}; DEADLINE=${DEADLINE:-300}
wait_ready
KEY=$(mint_key chaos); export KEY
psql_ -c "truncate jobs; delete from schedules"
api POST /schedules '{"name":"chaos-every-2s","cron_expr":"@every 2s","misfire_policy":"fire_all","type":"noop"}' >/dev/null
psql_ -c "insert into jobs (tenant_id,type,payload,max_attempts) select 'chaos','sleep','{\"seconds\":0.1}',20 from generate_series(1,$JOBS)"
echo "chaos: $JOBS jobs + cron every 2s; SIGKILL one container every 2s for ${KILL_SECONDS}s, then recovery"

# `docker kill` counts as a manual stop, so Docker does not apply restart: unless-stopped to it (a
# real crash or OOM kill is restarted). Play the orchestrator: restart what we killed.
restart_later() { sleep 1; docker start "$1" >/dev/null 2>&1; }
START=$(date +%s); kills=0
[ -n "${CSV:-}" ] && echo "t,succeeded,kills" > "$CSV"
while true; do
  ok=$(psql_ -c "select count(*) from jobs where type='sleep' and status='succeeded'")
  el=$(( $(date +%s) - START ))
  [ -n "${CSV:-}" ] && echo "$el,$ok,$kills" >> "$CSV"   # before the break, so the final point is recorded
  [ "$ok" = "$JOBS" ] && break
  [ "$el" -gt "$DEADLINE" ] && { echo "TIMEOUT after ${DEADLINE}s"; break; }
  if [ "$el" -lt "$KILL_SECONDS" ]; then
    if [ $((kills % 4)) = 3 ]; then t="scheduler-$((RANDOM%2+1))"; else t="worker-$((RANDOM%2+1))"; fi
    if docker kill "jobscheduler-$t" >/dev/null 2>&1; then kills=$((kills+1)); restart_later "jobscheduler-$t" & fi
    printf '  t=%3ds succeeded=%-5s killed=%s (last: %s)\n' "$el" "$ok" "$kills" "$t"
  else
    printf '  t=%3ds succeeded=%-5s (recovering; no more kills)\n' "$el" "$ok"
  fi
  sleep 2
done
wait
sleep 8   # let the surviving scheduler catch up any cron backlog
[ -n "${CSV:-}" ] && echo "$START $(date +%s)" > "${CSV%.csv}.window"

echo "=== chaos report ==="
echo "containers SIGKILLed: $kills"
psql_ -F' ' -c "select 'jobs succeeded: '||count(*) filter (where status='succeeded')||'   dead/failed: '||count(*) filter (where status in ('dead','failed'))||'   not finished: '||count(*) filter (where status not in ('succeeded','dead','failed'))||'   recovered (attempts>1): '||count(*) filter (where attempts>1) from jobs where type='sleep'" | tee /tmp/chaos-jobs.$$
psql_ -c "select idempotency_key from jobs where idempotency_key like 'cron:%' order by 1" | python3 -c "
import sys,datetime
keys=[l.strip() for l in sys.stdin if l.strip()]
slots=sorted(datetime.datetime.fromisoformat(k.split(':',2)[2].replace('Z','+00:00')) for k in keys)
gaps=[(a,b) for a,b in zip(slots,slots[1:]) if (b-a).total_seconds()!=2]
print(f'cron slots fired: {len(keys)}   duplicates: {len(keys)-len(set(keys))}   gaps: {len(gaps)}')
sys.exit(1 if (gaps or len(keys)!=len(set(keys)) or len(keys)<5) else 0)" | tee /tmp/chaos-cron.$$
cron_rc=${PIPESTATUS[1]}
grep -q "succeeded: $JOBS   dead/failed: 0   not finished: 0" /tmp/chaos-jobs.$$ && jobs_ok=1 || jobs_ok=0
rm -f /tmp/chaos-jobs.$$ /tmp/chaos-cron.$$
if [ -n "${CSV:-}" ]; then   # machine-readable summary of this run, for the charts
  read -r succ rec <<<"$(psql_ -F' ' -c "select count(*) filter (where status='succeeded'), count(*) filter (where attempts>1) from jobs where type='sleep'")"
  read -r slots dup <<<"$(psql_ -F' ' -c "select count(*), count(*)-count(distinct idempotency_key) from jobs where idempotency_key like 'cron:%'")"
  printf '{"jobs": %s, "succeeded": %s, "recovered": %s, "kills": %s, "cron_slots": %s, "cron_duplicates": %s}\n' \
    "$JOBS" "$succ" "$rec" "$kills" "$slots" "$dup" > "${CSV%.csv}.summary.json"
fi
if [ "$jobs_ok" = 1 ] && [ "$cron_rc" = 0 ]; then echo "PASS: nothing lost, nothing double-fired"; else echo "FAIL"; exit 1; fi
