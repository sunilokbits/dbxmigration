"""Verify ServiceNow OAuth works with the Client ID from config (no client-id secret):
Settings GET shows it, Test Connection passes, and one REST pipeline runs on Databricks.

    python verify_client_id_from_config.py [group_id_to_run]
"""
import sys
import time

import requests
from databricks.sdk import WorkspaceClient

BASE = "http://localhost:5000/api/v1"
w = WorkspaceClient()

for _ in range(90):
    try:
        requests.get(BASE + "/sources/registry.js", timeout=3)
        break
    except requests.RequestException:
        time.sleep(2)

keys = sorted(s.key for s in w.secrets.list_secrets("migration-studio") if "servicenow" in s.key)
print("ServiceNow secrets:", keys)

c = requests.get(BASE + "/deploy-config", timeout=120).json()["config"]
src = c.get("source") or {}
cid = src.get("username") or ""
print(f"Settings GET -> source={src.get('source_type')}/{src.get('ingestion_method')} auth={src.get('api_auth_type')} "
      f"client_id={'shown (' + str(len(cid)) + ' chars)' if cid else 'MISSING'} client_secret={src.get('password') or '(empty)'}")

t = requests.post(BASE + "/source/test-connection", timeout=300, json={
    "source_type": "servicenow", "ingestion_method": "rest_api", "server": "https://dev405660.service-now.com",
    "api_auth_type": "oauth2", "username": cid, "password": src.get("password", ""), "database": ""}).json()
print("Test Connection:", t.get("success"), "|", t.get("server_version") or t.get("error"))

dep = requests.post(BASE + "/workflow/notebooks/deploy", timeout=600, json={
    "host": c.get("databricks_host", ""), "catalog": c.get("metadata_catalog", ""),
    "schema": c.get("metadata_schema", ""), "landing_path": c.get("volume_path", "/mnt/landing"),
    "workspace_path": "/Shared/DBX/MetadataPipeline", "pipeline_mode": "dlt"}).json()
print("deploy notebooks:", dep.get("message") or dep.get("error"))

if len(sys.argv) > 1:
    gid = sys.argv[1]
    r = requests.post(f"{BASE}/workflow/pipelines/{gid}/run-databricks", timeout=600, json={
        "host": c.get("databricks_host", ""), "catalog": c.get("metadata_catalog", ""),
        "schema": c.get("metadata_schema", ""), "workspace_path": "/Shared/DBX/MetadataPipeline",
        "landing_path": c.get("volume_path", "/mnt/landing")}).json()
    print("run:", r.get("run_id") if r.get("success") else r.get("error"))
    if r.get("success"):
        rid = int(r["run_id"])
        while True:
            run = w.jobs.get_run(rid)
            lc = run.state.life_cycle_state.value
            if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                break
            time.sleep(20)
        print("result:", lc, run.state.result_state.value if run.state.result_state else "",
              {t.task_key: (t.state.result_state.value if t.state.result_state else "?") for t in run.tasks or []})
