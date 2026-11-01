"""Show the username (OAuth client ID) stored on existing ServiceNow REST pipeline groups.

    python inspect_servicenow_rest_groups.py
"""
import json

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
q = w.statement_execution.execute_statement(
    warehouse_id="d01073f7104f07ff", wait_timeout="50s",
    statement="SELECT group_id, full_table, source_config FROM dbx_admin_source.configtables.wf_pipeline_metadata "
              "WHERE full_table LIKE 'servicenow.%'")
if q.status.error:
    raise SystemExit(q.status.error.message)
for gid, ft, sc in q.result.data_array or []:
    s = json.loads(sc or "{}")
    if s.get("ingestion_method") != "rest_api":
        continue
    u = (s.get("username") or "").strip()
    print(f"{gid} {ft:28s} username={u[:4] + '...' if u else '(empty)'} len={len(u)} "
          f"oauth_client_id={'set' if s.get('oauth_client_id') else 'missing'}")
