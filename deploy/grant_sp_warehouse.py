#!/usr/bin/env python3
"""Grant the Databricks App's SP CAN_USE on a SQL warehouse."""
import os, sys, requests
from databricks.sdk import WorkspaceClient

wh_id = os.environ.get("SQL_WAREHOUSE_ID", "")
if not wh_id:
    print("No SQL_WAREHOUSE_ID set, skipping")
    sys.exit(0)

w = WorkspaceClient()
app = next((a for a in w.apps.list() if a.name == "dbxmigrationapp"), None)
if not app or not app.service_principal_client_id:
    print("WARN: could not find app SP (non-blocking)")
    sys.exit(0)

sp_app_id = app.service_principal_client_id
host = os.environ["DATABRICKS_HOST"].rstrip("/")
token = os.environ["DATABRICKS_TOKEN"]
r = requests.patch(
    f"{host}/api/2.0/permissions/sql/warehouses/{wh_id}",
    headers={"Authorization": f"Bearer {token}"},
    json={"access_control_list": [{"service_principal_name": sp_app_id, "permission_level": "CAN_USE"}]},
)
print(f"Granted CAN_USE to {sp_app_id}: {r.status_code}")
