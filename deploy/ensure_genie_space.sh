#!/usr/bin/env bash
# ensure_genie_space.sh — idempotent Genie Space creation with full config
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
pip install -q databricks-sdk >/dev/null 2>&1 || true

# Render the instructions/description templates with whatever catalogs are
# actually configured in this deployment's Settings (falls back to the
# static defaults if that can't be read -- non-blocking either way).
python3 "$SCRIPT_DIR/render_genie_instructions.py" 2>&1 || echo "WARN: Genie template rendering failed (using static defaults)"

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
    SPACE_ID="$EXISTING"
else
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

    # Build the create payload with full description from template
    SPACE_ID=$(python3 -c "
import json, sys, os

desc_file = '/tmp/genie_space_description.txt'
if not os.path.isfile(desc_file):
    desc_file = os.path.join('$SCRIPT_DIR', 'genie_space_description.txt')
if os.path.isfile(desc_file):
    with open(desc_file) as f:
        desc = f.read().strip()
else:
    desc = 'DBX Migration Studio — query migration metadata, reconciliation, and execution logs.'

payload = {
    'title': 'DBX Migration \u2014 Full Workspace',
    'description': desc,
    'warehouse_id': '$WH_ID',
    'serialized_space': json.dumps({'version': 2}),
}
tmpfile = '/tmp/genie_create.json'
with open(tmpfile, 'w') as f:
    json.dump(payload, f)
print(tmpfile)
" 2>/dev/null || echo "")

    if [ -z "$SPACE_ID" ] || [ ! -f "$SPACE_ID" ]; then
        echo "WARN: could not build create payload"
        exit 0
    fi

    TMPFILE="$SPACE_ID"
    RESULT=$(databricks genie create-space --json "@$TMPFILE" -o json 2>&1) || {
        echo "WARN: Genie Space auto-create not supported in this workspace (non-blocking)"
        echo "$RESULT"
        rm -f "$TMPFILE"
        exit 0
    }
    rm -f "$TMPFILE"

    SPACE_ID=$(echo "$RESULT" | python3 -c "import json,sys; print(json.load(sys.stdin).get('space_id',''))" 2>/dev/null || echo "")
    echo "Created Genie Space: $SPACE_ID"
fi

# Update the space with instructions (configure tab) if we have a space ID
if [ -n "$SPACE_ID" ]; then
    python3 -c "
import json, os
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
space_id = '$SPACE_ID'
inst_file = '/tmp/genie_space_instructions.txt'
if not os.path.isfile(inst_file):
    inst_file = os.path.join('$SCRIPT_DIR', 'genie_space_instructions.txt')

if os.path.isfile(inst_file):
    with open(inst_file) as f:
        instructions = f.read().strip()
else:
    instructions = ''

if instructions:
    try:
        w.api_client.do('PATCH', f'/api/2.0/genie/spaces/{space_id}', body={
            'instructions': instructions,
            'sample_questions': [
                'What recent actions were taken in the audit log?',
                'Which jobs have the highest failure rate?',
                'Which tables have reconciliation mismatches?',
                'Show me the last 10 pipeline runs with their status and duration',
                'How many tables have been migrated successfully vs failed?',
            ],
        })
        print(f'Updated Genie Space {space_id} with instructions and sample questions')
    except Exception as e:
        print(f'WARN: Could not update space instructions: {e}')
else:
    print('No instructions file found, skipping update')
" 2>/dev/null || echo "WARN: Could not update Genie Space instructions (non-blocking)"
fi
