#!/usr/bin/env bash
# ensure_genie_space.sh — idempotent Genie Space creation
# Called from CI/CD after secrets are scaffolded and before bundle deploy.
# Requires: DATABRICKS_HOST, DATABRICKS_TOKEN
set -euo pipefail

pip install -q databricks-sdk >/dev/null 2>&1 || true

python3 - <<'PYEOF'
import os, json

def main():
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()

    # Check if a Genie Space already exists in this workspace
    try:
        spaces = w.api_client.do("GET", "/api/2.0/genie/spaces")
        existing = (spaces or {}).get("spaces", []) if isinstance(spaces, dict) else []
        for sp in existing:
            title = sp.get("title", "")
            if "migration" in title.lower() or "dbx" in title.lower():
                print(f"Genie Space already exists: {sp.get('space_id', '')} ({title})")
                return
    except Exception:
        existing = []

    # Find a SQL warehouse to attach
    wh_id = os.environ.get("SQL_WAREHOUSE_ID", "")
    if not wh_id:
        try:
            warehouses = list(w.warehouses.list())
            running = [wh for wh in warehouses if "RUNNING" in str(getattr(wh.state, 'value', wh.state)).upper()]
            wh = running[0] if running else (warehouses[0] if warehouses else None)
            if wh:
                wh_id = wh.id
        except Exception:
            pass

    if not wh_id:
        print("SKIP: no SQL warehouse available — Genie Space needs one to attach to")
        return

    # Create the space
    try:
        import json as _json
        serialized = _json.dumps({
            "title": "DBX Migration — Full Workspace",
            "description": "# DBX Migration Studio Genie Space\nQuery migration metadata, reconciliation, and execution logs.",
            "warehouse_id": wh_id,
            "table_identifiers": [],
        })
        resp = w.api_client.do("POST", "/api/2.0/genie/spaces", body={
            "title": "DBX Migration — Full Workspace",
            "description": "# DBX Migration Studio Genie Space\nQuery migration metadata, reconciliation, and execution logs.",
            "warehouse_id": wh_id,
            "serialized_space": serialized,
        })
        space_id = resp.get("space_id", "") if isinstance(resp, dict) else ""
        if space_id:
            print(f"Created Genie Space: {space_id}")
        else:
            print(f"WARN: Genie API returned no space_id: {resp}")
    except Exception as e:
        print(f"WARN: Genie Space auto-create not supported in this workspace: {e}")

main()
PYEOF
