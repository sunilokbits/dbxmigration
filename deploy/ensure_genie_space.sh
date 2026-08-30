#!/usr/bin/env bash
# ensure_genie_space.sh — idempotent Genie Space creation via CLI
set -euo pipefail

pip install -q databricks-sdk >/dev/null 2>&1 || true

# Check if a migration-related Genie Space already exists
EXISTING=$(databricks genie list-spaces -o json 2>/dev/null | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    for sp in data.get('spaces', []):
        t = sp.get('title', '').lower()
        if 'migration' in t or 'dbx' in t:
            print(sp.get('space_id', ''))
            sys.exit(0)
except: pass
print('')
" 2>/dev/null || echo "")

if [ -n "$EXISTING" ]; then
    echo "Genie Space already exists: $EXISTING"
    exit 0
fi

# Find a running SQL warehouse
WH_ID=$(python3 -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
for wh in w.warehouses.list():
    s = str(getattr(wh.state, 'value', wh.state)).upper()
    if 'RUNNING' in s:
        print(wh.id); break
else:
    whs = list(w.warehouses.list())
    if whs: print(whs[0].id)
" 2>/dev/null || echo "")

if [ -z "$WH_ID" ]; then
    echo "SKIP: no SQL warehouse available"
    exit 0
fi

# Create using --json with the correct serialized_space format
TMPFILE=$(mktemp /tmp/genie_create.XXXXXX.json)
cat > "$TMPFILE" <<EOF
{"title":"DBX Migration — Full Workspace","description":"Auto-created by CI/CD. Query migration metadata, reconciliation, and execution logs.","warehouse_id":"$WH_ID","serialized_space":"{\"version\":2}"}
EOF
databricks genie create-space --json "@$TMPFILE" -o json 2>&1 && echo "Genie Space created successfully" || echo "WARN: Genie Space auto-create not supported in this workspace (non-blocking)"
rm -f "$TMPFILE"
