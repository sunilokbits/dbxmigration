"""Check the app's ServiceNow Test Connection + table discovery against the real instance."""
import time

import requests

BASE = "http://localhost:5000"
CFG = {"source_type": "servicenow", "ingestion_method": "lakeflow_connect",
       "server": "servicenow", "objects": "incident, change_request, cmdb_ci",
       "database": "", "username": ""}

for _ in range(60):
    try:
        requests.get(BASE + "/api/v1/sources/registry.js", timeout=3)
        break
    except requests.RequestException:
        time.sleep(2)

t = requests.post(BASE + "/api/v1/source/test-connection", json=CFG, timeout=300).json()
print("Test Connection:", t.get("success"), "|", t.get("server_version") or t.get("error"))

d = requests.post(BASE + "/api/v1/workflow/list-tables", json=CFG, timeout=600).json()
print("Discover Tables:", d.get("total"), "tables")
for tb in d.get("tables", []):
    print(f"  {tb['full_name']:28s} {tb['row_estimate']:>6,} rows  {tb['col_count']:>4} cols")
