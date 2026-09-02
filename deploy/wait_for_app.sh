#!/usr/bin/env bash
# Ensure a Databricks App's compute is running before `databricks apps deploy`.
# A newly created app is STOPPED, and apps deploy rejects it with
# "Cannot deploy app <name> as it is not in RUNNING state."
set -uo pipefail

APP_NAME="${1:?usage: wait_for_app.sh <app-name> [timeout-seconds]}"
TIMEOUT="${2:-600}"

app_state() {
  databricks apps get "$APP_NAME" -o json 2>/dev/null \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print((d.get('compute_status') or {}).get('state',''))" 2>/dev/null \
    || echo ""
}

STATE="$(app_state)"
if [ -z "$STATE" ]; then
  echo "App '$APP_NAME' not found yet; skipping start (bundle deploy should create it)."
  exit 0
fi
echo "App '$APP_NAME' compute state: ${STATE:-unknown}"

case "$STATE" in
  ACTIVE|RUNNING) echo "Already running."; exit 0 ;;
esac

echo "Starting app '$APP_NAME'..."
databricks apps start "$APP_NAME" >/dev/null 2>&1 || true

DEADLINE=$(( $(date +%s) + TIMEOUT ))
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  STATE="$(app_state)"
  case "$STATE" in
    ACTIVE|RUNNING) echo "App '$APP_NAME' is running."; exit 0 ;;
    ERROR) echo "App '$APP_NAME' entered ERROR state."; exit 1 ;;
  esac
  echo "  state=${STATE:-unknown} - waiting..."
  sleep 10
done

echo "Timed out after ${TIMEOUT}s waiting for '$APP_NAME' (last state: ${STATE:-unknown})."
exit 1
