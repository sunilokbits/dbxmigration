#!/usr/bin/env bash
# resolve_sql_warehouse.sh — prints a usable SQL warehouse ID to stdout (or
# nothing if the workspace has none). Prefers a RUNNING warehouse, falls
# back to the first available one. Used by CI/CD to populate
# DATABRICKS_SQL_WAREHOUSE_ID for the app so the "SQL Warehouse
# Connectivity" pre-flight check passes without any manual step.
set -euo pipefail

pip install -q databricks-sdk >/dev/null 2>&1 || true

python3 -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
whs = list(w.warehouses.list())
for wh in whs:
    s = str(getattr(wh.state, 'value', wh.state)).upper()
    if 'RUNNING' in s:
        print(wh.id)
        break
else:
    if whs:
        print(whs[0].id)
" 2>/dev/null || echo ""
