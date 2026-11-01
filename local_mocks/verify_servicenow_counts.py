"""Compare ServiceNow row counts (live API) with Lakeflow-ingested Delta tables,
then check the app's Discover Tables output.

    python verify_servicenow_counts.py
"""
import base64

import requests
from databricks.sdk import WorkspaceClient
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

TABLES = ("incident", "change_request", "cmdb_ci", "problem", "sc_request", "sc_req_item", "task",
          "sys_user", "sys_user_group", "cmdb_ci_server", "cmdb_ci_computer")
WAREHOUSE = "d01073f7104f07ff"

w = WorkspaceClient()
sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(total=4, connect=4, read=4, backoff_factor=3)))


def s(k):
    return base64.b64decode(w.secrets.get_secret("migration-studio", k).value).decode()


url = s("source-servicenow-instance-url")
tok = sess.post(url + "/oauth_token.do", timeout=(30, 90), data={
    "grant_type": "client_credentials", "client_id": s("source-servicenow-client-id"),
    "client_secret": s("source-servicenow-client-secret"), "scope": "useraccount"}).json()["access_token"]
hdr = {"Authorization": "Bearer " + tok, "Accept": "application/json"}

print(f"{'table':18s} {'ServiceNow':>11s} {'Delta (Lakeflow)':>17s}  match")
for t in TABLES:
    r = sess.get(f"{url}/api/now/stats/{t}", params={"sysparm_count": "true"}, headers=hdr, timeout=(30, 120))
    sn = int(r.json()["result"]["stats"]["count"]) if r.ok else -1
    q = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE, statement=f"SELECT COUNT(*) FROM dbx_bronze.servicenow.{t}", wait_timeout="50s")
    dl = int(q.result.data_array[0][0]) if "SUCCEEDED" in str(q.status.state) else -1
    print(f"{t:18s} {sn:>11,} {dl:>17,}  {'OK' if sn == dl else 'DIFF'}")

app = requests.post("http://localhost:5000/api/v1/workflow/list-tables", timeout=900,
                    json={"source_type": "servicenow", "database": "", "username": ""}).json()
print(f"\nApp Discover Tables: {app.get('total')} tables -> " + ", ".join(t["table"] for t in app.get("tables", [])))
