#!/usr/bin/env bash
# Verifies the behaviour the chart promises, on a real cluster (scripts/kind-up.sh first):
# KEDA autoscaling, container crashes, rolling deploys draining in-flight jobs, node drains
# respecting PodDisruptionBudgets, and NetworkPolicies that both block and still let the app work.
# FULL=1 adds the 2.5 minute forced-release test.
source "$(dirname "$0")/lib-k8s.sh"
D="${REL}-jobscheduler"
PSQL -c "truncate jobs; delete from schedules" >/dev/null

echo "== 1. KEDA scales workers on queue depth (min 2, max 8, 50 jobs per replica)"
min=$(K get scaledobject "$D-worker" -o jsonpath='{.spec.minReplicaCount}')
PSQL -c "insert into jobs (tenant_id,type,payload,max_attempts) select 'v','sleep','{\"seconds\":0.5}',20 from generate_series(1,4000)"
peak=0; for _ in $(seq 120); do
  r=$(K get deploy "$D-worker" -o jsonpath='{.status.replicas}'); [ "${r:-0}" -gt "$peak" ] && peak=$r
  [ "$(count_succeeded)" = 4000 ] && break; sleep 2; done
check "4000 jobs drained" test "$(count_succeeded)" = 4000
check "workers scaled out from $min to $peak under load" test "$peak" -gt "$min"

echo "== 2. Container crashes (SIGKILL via crictl, like an OOM kill) lose nothing"
PSQL -c "truncate jobs"
PSQL -c "insert into schedules (tenant_id,name,cron_expr,misfire_policy,type,next_fire_at) values ('v','chaos-2s','@every 2s','fire_all','noop', date_trunc('second', now()) + interval '2 seconds')"
PSQL -c "insert into jobs (tenant_id,type,payload,max_attempts) select 'v','sleep','{\"seconds\":2}',20 from generate_series(1,4000)"
start=$(date +%s); kills=0
while [ "$(count_succeeded sleep)" != 4000 ] && [ $(( $(date +%s)-start )) -lt 300 ]; do
  if [ $(( $(date +%s)-start )) -lt 40 ]; then
    comp=worker; [ $((kills%4)) = 3 ] && comp=scheduler
    read -r node cid <<<"$(K get pods -l app.kubernetes.io/component=$comp --field-selector=status.phase=Running \
      -o jsonpath='{range .items[*]}{.spec.nodeName} {.status.containerStatuses[0].containerID}{"\n"}{end}' | shuf -n1)"
    docker exec "$node" crictl stop -t 0 "${cid#containerd://}" >/dev/null 2>&1 && kills=$((kills+1))
  fi; sleep 3; done
sleep 8
check "all 4000 jobs succeeded after $kills container kills" test "$(count_succeeded sleep)" = 4000
check "some jobs were recovered after losing their worker (attempts>1)" test "$(PSQL -c "select count(*) from jobs where attempts>1")" -gt 0
dups=$(PSQL -c "select count(*)-count(distinct idempotency_key) from jobs where idempotency_key like 'cron:%'")
gaps=$(PSQL -c "select count(*) from (select extract(epoch from (substr(idempotency_key, 43)::timestamptz - lag(substr(idempotency_key, 43)::timestamptz) over (order by idempotency_key))) d from jobs where idempotency_key like 'cron:%') s where d is not null and d <> 2")
check "cron fired with 0 duplicate and 0 missed slots (dups=$dups gaps=$gaps)" test "$dups$gaps" = 00
PSQL -c "delete from schedules" >/dev/null

echo "== 3. A rolling deploy lets in-flight jobs finish (grace period = max job timeout + 15s)"
PSQL -c "truncate jobs"
PSQL -c "insert into jobs (tenant_id,type,payload,max_attempts) select 'v','sleep','{\"seconds\":40}',5 from generate_series(1,12)"
wait_eq 30 12 count_status running || true
K rollout restart "deploy/$D-worker" >/dev/null
wait_eq 60 12 count_succeeded || true
check "12 jobs mid-flight during the rollout all finished" test "$(count_succeeded)" = 12
check "none was interrupted (max attempts = 1)" test "$(PSQL -c "select max(attempts) from jobs")" = 1

if [ "${FULL:-0}" = 1 ]; then
echo "== 3b. A job longer than the shutdown grace is RELEASED (attempt refunded), not lost"
PSQL -c "truncate jobs"
PSQL -c "insert into jobs (tenant_id,type,payload,max_attempts,timeout_seconds) select 'v','sleep','{\"seconds\":100}',5,300 from generate_series(1,4)"
wait_eq 30 4 count_status running || true
K rollout restart "deploy/$D-worker" >/dev/null
wait_eq 90 4 count_succeeded || true
check "jobs outlived the old worker and finished on the new ones" test "$(count_succeeded)" = 4
check "released jobs kept attempts = 1 (a reap would make it 2)" test "$(PSQL -c "select max(attempts) from jobs")" = 1
fi

echo "== 4. Draining a node respects the PodDisruptionBudgets and loses no work"
pgnode=$(K get pod "$D-postgres-0" -o jsonpath='{.spec.nodeName}')
target=$(kubectl --context "$CTX" get nodes -l '!node-role.kubernetes.io/control-plane' -o name | sed 's|node/||' | grep -vx "$pgnode" | head -1)
PSQL -c "truncate jobs"
PSQL -c "insert into jobs (tenant_id,type,payload,max_attempts) select 'v','sleep','{\"seconds\":1}',20 from generate_series(1,3000)"
ready_log=$(mktemp); trap 'rm -f "$ready_log"' EXIT
( while true; do r=$(K get deploy "$D-api" -o jsonpath='{.status.readyReplicas}'); echo "${r:-0}" >> "$ready_log"; sleep 1; done ) & sampler=$!
check "node $target drained" kubectl --context "$CTX" drain "$target" --ignore-daemonsets --delete-emptydir-data --timeout=240s
kill $sampler 2>/dev/null || true
minready=$(sort -n "$ready_log" | head -1)
check "the API always had at least 1 ready replica during the drain (min=$minready)" test "${minready:-0}" -ge 1
wait_eq 150 3000 count_succeeded || true
check "3000 jobs finished across the drain" test "$(count_succeeded)" = 3000
kubectl --context "$CTX" uncordon "$target" >/dev/null

echo "== 5. NetworkPolicies block what they should and the app still works with NEW connections"
helm upgrade "$REL" "$ROOT/deploy/helm/jobscheduler" --kube-context "$CTX" -n "$NS" --wait --timeout 300s \
  -f "$ROOT/deploy/kind/values-kind.yaml" --set networkPolicy.enabled=true \
  --set 'networkPolicy.postgres.podSelector.app\.kubernetes\.io/component=postgres' \
  --set "networkPolicy.postgres.podSelector.app\.kubernetes\.io/instance=$REL" >/dev/null
kubectl --context "$CTX" create namespace ingress-nginx >/dev/null 2>&1 || true
sleep 5
check "a pod in the default namespace is BLOCKED from the API" test "$(probe default "http://$D-api.$NS.svc/healthz")" = BLOCKED
check "a pod in the allowed ingress namespace can reach the API" test "$(probe ingress-nginx "http://$D-api.$NS.svc/healthz")" = REACHABLE
for d in api worker scheduler; do K rollout restart "deploy/$D-$d" >/dev/null; done
for d in api worker scheduler; do K rollout status "deploy/$D-$d" --timeout=180s >/dev/null; done
sleep 15
PSQL -c "truncate jobs"
PSQL -c "insert into jobs (tenant_id,type,payload,max_attempts) select 'v','noop','{}',5 from generate_series(1,1000)"
wait_eq 60 1000 count_succeeded || true
check "1000 jobs ran on brand-new database connections under default-deny" test "$(count_succeeded)" = 1000
check "no pod is crash-looping" test "$(K get pods --no-headers | grep -vE 'Completed|postgres' | awk '$3!="Running"' | wc -l)" = 0
check "KEDA can still read the queue" test "$(K get scaledobject "$D-worker" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')" = True

[ "$FAILED" = 0 ] && echo "K8S VERIFY PASSED" || { echo "K8S VERIFY FAILED"; exit 1; }
