"""Check the ServiceNow Client ID lives in app config + REST pipeline metadata, not in secrets."""
import json

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def q(stmt):
    r = w.statement_execution.execute_statement(warehouse_id="d01073f7104f07ff", statement=stmt, wait_timeout="50s")
    if r.status.error:
        raise RuntimeError(r.status.error.message)
    return (r.result.data_array if r.result else None) or []


src = json.loads(q("SELECT config_value FROM admin_source.migration_app.app_config WHERE config_key = 'source'")[0][0])
cid = src.get("username") or ""
print("app config source:", src.get("source_type"), src.get("ingestion_method"), src.get("api_auth_type"),
      "| client id:", f"{len(cid)} chars" if cid else "MISSING")

ok = missing = 0
for _gid, sc in q("SELECT group_id, source_config FROM dbx_admin_source.configtables.wf_pipeline_metadata "
                  "WHERE full_table LIKE 'servicenow.%'"):
    s = json.loads(sc or "{}")
    if s.get("ingestion_method") == "rest_api":
        ok, missing = (ok + 1, missing) if s.get("oauth_client_id") else (ok, missing + 1)
print("REST pipeline groups with oauth_client_id:", ok, "| missing:", missing)
print("servicenow secrets:", sorted(x.key for x in w.secrets.list_secrets("migration-studio") if "servicenow" in x.key))
