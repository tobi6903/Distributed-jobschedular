#!/usr/bin/env bash
# Shared helpers for the scripts that drive the Docker Compose stack. Needs only docker, curl, python3.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/deploy/docker/docker-compose.yml")
API="${API_URL:-http://localhost:${API_PORT:-8080}}"
PROM="${PROM_URL:-http://localhost:${PROMETHEUS_PORT:-9090}}"

# SQL against the compose Postgres, with no psql needed on the host.
psql_() { "${COMPOSE[@]}" exec -T postgres psql -U jobs -d jobs -Atq "$@"; }

# mint_key <tenant>: prints a fresh API key.
mint_key() { "${COMPOSE[@]}" run --rm --no-deps -T jobctl create-key -tenant "$1" -name "script-$$" | tail -1; }

# api <method> <path> [json-body]: calls the API with $KEY, prints the body.
api() { curl -fsS -X "$1" "$API$2" -H "Authorization: Bearer $KEY" ${3:+-d "$3"}; }

jget() { python3 -c "import sys,json; d=json.load(sys.stdin); print(eval(sys.argv[1], {'d': d}))" "$1"; }

wait_ready() { for _ in $(seq "${1:-60}"); do curl -fsS "$API/readyz" >/dev/null 2>&1 && return 0; sleep 2; done; echo "API never became ready" >&2; return 1; }

pass() { printf '  PASS  %s\n' "$*"; }
fail() { printf '  FAIL  %s\n' "$*" >&2; FAILED=1; }
