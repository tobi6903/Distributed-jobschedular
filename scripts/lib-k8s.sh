#!/usr/bin/env bash
# Shared helpers for the Kubernetes scripts. Needs kubectl, helm, docker and a kind cluster.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER="${CLUSTER:-js}"; CTX="kind-$CLUSTER"; NS="${NS:-jobs}"; REL="${REL:-js}"
K() { kubectl --context "$CTX" -n "$NS" "$@"; }
# SQL against the demo in-cluster Postgres (runs inside its pod: no port-forward, no host psql).
PSQL() { K exec "${REL}-jobscheduler-postgres-0" -- psql -U jobs -d jobs -Atq "$@"; }
FAILED=0
pass() { printf '  PASS  %s\n' "$*"; }
fail() { printf '  FAIL  %s\n' "$*" >&2; FAILED=1; }
check() { local msg=$1; shift; if "$@"; then pass "$msg"; else fail "$msg"; fi; }
# wait_for <tries> <command...>: polls every 2s until the command succeeds.
wait_for() { local n=$1; shift; for _ in $(seq "$n"); do "$@" >/dev/null 2>&1 && return 0; sleep 2; done; return 1; }
# wait_eq <tries> <expected> <command...>: polls until the command's output equals <expected>. The command
# runs again on EVERY poll. (Writing `wait_for 30 test "$(cmd)" = x` is a trap: the $(...) is evaluated once,
# before polling starts, so it would poll a frozen value.)
wait_eq() { local n=$1 want=$2; shift 2; for _ in $(seq "$n"); do [ "$("$@" 2>/dev/null)" = "$want" ] && return 0; sleep 2; done; return 1; }
# count_status <status> [type]: jobs in that status (optionally of one type; cron adds noop jobs).
count_status() { PSQL -c "select count(*) from jobs where status='$1'${2:+ and type='$2'}"; }
count_succeeded() { count_status succeeded "${1:-}"; }
# probe <namespace> <url>: REACHABLE / BLOCKED, from a throwaway pod in that namespace.
probe() { kubectl --context "$CTX" -n "$1" run "probe-$RANDOM" --rm -i --restart=Never --image=busybox:1.36 -q --command -- \
  sh -c "wget -qO- -T 4 $2 >/dev/null 2>&1 && echo REACHABLE || echo BLOCKED" 2>/dev/null | tail -1; }
