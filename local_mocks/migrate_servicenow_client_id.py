"""One-off move of the ServiceNow OAuth Client ID out of the secret scope:
  1. copy it into the app's saved source config (shown in Settings as Client ID),
  2. stamp it into existing ServiceNow REST pipeline metadata (oauth_client_id),
  3. delete the source-servicenow-client-id secret.

    python migrate_servicenow_client_id.py
"""
import base64
import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

SCOPE, KEY = "migration-studio", "source-servicenow-client-id"
WAREHOUSE = "d01073f7104f07ff"
CONFIG_TBL = "admin_source.migration_app.app_config"
META = "dbx_admin_source.configtables"

w = WorkspaceClient()


def sql(stmt, params=None):
    params = [StatementParameterListItem(name=k, value=v) for k, v in (params or {}).items()] or None
    r = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE, statement=stmt, parameters=params,
                                                wait_timeout="50s")
    if r.status.error:
        raise RuntimeError(r.status.error.message)
    return (r.result.data_array if r.result else None) or []


keys = {s.key for s in w.secrets.list_secrets(SCOPE)}
if KEY not in keys:
    raise SystemExit(f"{KEY} already removed")
cid = base64.b64decode(w.secrets.get_secret(SCOPE, KEY).value).decode().strip()

# 1) app config: source.username = client id
rows = sql(f"SELECT config_value FROM {CONFIG_TBL} WHERE config_key = 'source'")
src = json.loads(rows[0][0]) if rows else {}
if src.get("source_type") == "servicenow":
    src["username"] = cid
    sql(f"UPDATE {CONFIG_TBL} SET config_value = :v WHERE config_key = 'source'", {"v": json.dumps(src)})
    print("app config: ServiceNow Client ID saved to source.username")

# 2) existing REST pipeline metadata
for tbl, id_col in (("wf_pipeline_metadata", "group_id"), ("wf_job_metadata", "job_id")):
    for rid, sc in sql(f"SELECT {id_col}, source_config FROM {META}.{tbl} WHERE full_table LIKE 'servicenow.%'"):
        s = json.loads(sc or "{}")
        if s.get("ingestion_method") != "rest_api" or s.get("api_auth_type") != "oauth2":
            continue
        s["oauth_client_id"] = cid
        s["username"] = s.get("username") or cid
        s.pop("oauth_client_id_key", None)
        sql(f"UPDATE {META}.{tbl} SET source_config = :v WHERE {id_col} = :id", {"v": json.dumps(s), "id": rid})
        print(f"{tbl}: {rid} stamped with oauth_client_id")

# 3) remove the secret
w.secrets.delete_secret(SCOPE, KEY)
print(f"deleted secret {SCOPE}/{KEY}")
