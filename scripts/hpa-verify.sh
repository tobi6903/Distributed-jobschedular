#!/usr/bin/env bash
# Does the API autoscaler actually scale out, and do the new replicas take traffic? Run after kind-up.sh.
# Load comes from pods INSIDE the cluster and goes through the Service. Do not use `kubectl port-forward` for
# this: it pins every connection to one pod, so the HPA scales out but the new pods sit idle (we measured
# 1117m on one pod and 1m on the other three) and the test would prove nothing about load balancing.
source "$(dirname "$0")/lib-k8s.sh"
HPA="${REL}-jobscheduler-api"; SVC="http://${HPA}/readyz"; LOADERS="${LOADERS:-4}"
cleanup() { for i in $(seq "$LOADERS"); do K delete pod "hpa-load-$i" --now --ignore-not-found >/dev/null 2>&1 || true; done; }
trap cleanup EXIT

replicas() { K get hpa "$HPA" -o jsonpath='{.status.currentReplicas}'; }
cpu_pct()  { K get hpa "$HPA" -o jsonpath='{.status.currentMetrics[0].resource.current.averageUtilization}'; }
MIN=$(K get hpa "$HPA" -o jsonpath='{.spec.minReplicas}'); MAX=$(K get hpa "$HPA" -o jsonpath='{.spec.maxReplicas}')
TARGET=$(K get hpa "$HPA" -o jsonpath='{.spec.metrics[0].resource.target.averageUtilization}')
echo "== API HPA: min=$MIN max=$MAX target=${TARGET}% cpu; starting at $(replicas) replicas, cpu $(cpu_pct)%"
check "starts at the minimum ($MIN replicas)" test "$(replicas)" = "$MIN"

echo "== $LOADERS in-cluster load pods hit $SVC (each runs 6 parallel loops)"
cleanup
for i in $(seq "$LOADERS"); do
  K run "hpa-load-$i" --restart=Never -q --image=busybox:1.36 --command -- sh -c \
    "for j in 1 2 3 4 5 6; do (while true; do wget -qO- -T 3 $SVC >/dev/null 2>&1; done) & done; wait" >/dev/null
done
scaled() { [ "$(replicas)" -gt "$MIN" ]; }
wait_for 90 scaled || true
PEAK_CPU=$(cpu_pct)
check "scaled out above $MIN replicas (now $(replicas), cpu ${PEAK_CPU}% vs target ${TARGET}%)" scaled
check "stays within maxReplicas ($(replicas) <= $MAX)" test "$(replicas)" -le "$MAX"

echo "== do the new replicas take traffic?"
ready() { [ "$(K get deploy "$HPA" -o jsonpath='{.status.readyReplicas}')" = "$(replicas)" ]; }
wait_for 60 ready || true
sleep 45   # metrics-server refreshes about every 15s; let CPU settle after the new pods warm up
mapfile -t CPUS < <(K top pods --no-headers | grep -- '-api-' | awk '{gsub("m","",$2); print $2}')
printf '  per-pod CPU (m): %s\n' "${CPUS[*]}"
IDLE=0; for c in "${CPUS[@]}"; do [ "$c" -lt 50 ] && IDLE=$((IDLE+1)); done
check "all ${#CPUS[@]} API pods are busy (none below 50m): load is spread by the Service" test "$IDLE" = 0 -a "${#CPUS[@]}" -gt "$MIN"

[ "$FAILED" = 0 ] && echo "HPA VERIFIED (scale-down is not waited for: the HPA holds replicas 5 minutes after load stops)" || { echo "HPA CHECK FAILED"; exit 1; }
