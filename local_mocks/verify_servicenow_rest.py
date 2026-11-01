"""Check the app's ServiceNow REST API (OAuth via secrets) Test Connection + Discover Tables."""
import time

import requests

BASE = "http://localhost:5000/api/v1"
CFG = {"source_type": "servicenow", "ingestion_method": "rest_api",
       "server": "https://dev405660.service-now.com", "api_auth_type": "oauth2",
       "objects": "incident, change_request, cmdb_ci, problem, sc_request, sc_req_item, task, "
                  "sys_user, sys_user_group, cmdb_ci_server, cmdb_ci_computer",
       "database": "", "username": "", "password": ""}

for _ in range(60):
    try:
        requests.get(BASE + "/sources/registry.js", timeout=3)
        break
    except requests.RequestException:
        time.sleep(2)

t = requests.post(BASE + "/source/test-connection", json=CFG, timeout=300).json()
print("Test Connection:", t.get("success"), "|", t.get("server_version") or t.get("error"))

d = requests.post(BASE + "/workflow/list-tables", json=CFG, timeout=900).json()
print("Discover Tables:", d.get("total"), "tables", "" if d.get("success") else d.get("error"))
for tb in d.get("tables", []):
    print(f"  {tb['full_name']:28s} {tb['row_estimate']:>6,} rows  {tb['col_count']:>4} cols")
