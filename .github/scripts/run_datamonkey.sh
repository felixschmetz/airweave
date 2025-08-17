#!/usr/bin/env bash
set -euo pipefail

# Required env vars:
# - DM_API_BASE (e.g., https://dm.example.com)
# - DM_CONFIG (e.g., configs/github.yaml)
# Optional:
# - POLL_INTERVAL (default 5)
# - DM_UI_URL (for printing a link with ?run=...)

DM_API_BASE="${DM_API_BASE:?DM_API_BASE is required}"
DM_CONFIG="${DM_CONFIG:?DM_CONFIG is required}"
POLL_INTERVAL="${POLL_INTERVAL:-5}"

json_value() {
  # Prefer jq if present; otherwise use Python
  if command -v jq >/dev/null 2>&1; then
    jq -r "$2" <<<"$1"
  else
    python3 - "$2" <<<"$1" <<'PY'
import sys, json
data=sys.stdin.read()
key=sys.argv[1]
print(json.loads(data)[key.strip('.').split('.')[-1]])
PY
  fi
}

resp=$(curl -fsSL -X POST "$DM_API_BASE/api/run" \
  -H 'Content-Type: application/json' \
  -d "{\"config\":\"$DM_CONFIG\"}")
run_id="$(json_value "$resp" '.run_id')"

echo "Run started: $run_id"
if [[ -n "${DM_UI_URL:-}" ]]; then
  echo "UI: ${DM_UI_URL%/}/?run=$run_id"
fi

while true; do
  info=$(curl -fsSL "$DM_API_BASE/api/runs/$run_id")
  status="$(json_value "$info" '.status')"
  echo "status=$status"
  if [[ "$status" == "passed" ]]; then
    exit 0
  elif [[ "$status" == "failed" ]]; then
    exit 1
  fi
  sleep "$POLL_INTERVAL"
done


