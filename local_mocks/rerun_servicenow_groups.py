"""Redeploy notebooks, run ServiceNow pipeline groups by id, wait, and report task results + row counts.

    python rerun_servicenow_groups.py <group_id> [<group_id> ...]
"""
import sys
import time

import requests
from databricks.sdk import WorkspaceClient

BASE = "http://localhost:5000/api/v1"
WAREHOUSE = "d01073f7104f07ff"
w = WorkspaceClient()

for _ in range(90):
    try:
        requests.get(BASE + "/sources/registry.js", timeout=3)
        break
    except requests.RequestException:
        time.sleep(2)

c = requests.get(BASE + "/deploy-config", timeout=120).json()["config"]
dep = requests.post(BASE + "/workflow/notebooks/deploy", timeout=600, json={
    "host": c.get("databricks_host", ""), "catalog": c.get("metadata_catalog", ""),
    "schema": c.get("metadata_schema", ""), "landing_path": c.get("volume_path", "/mnt/landing"),
    "workspace_path": "/Shared/DBX/MetadataPipeline", "pipeline_mode": "dlt"}).json()
print("deploy:", dep.get("message") or dep.get("error"))

groups = {g["group_id"]: g for g in (requests.get(BASE + "/workflow/pipelines", timeout=300).json().get("groups") or [])}
runs = {}
for gid in sys.argv[1:]:
    r = requests.post(f"{BASE}/workflow/pipelines/{gid}/run-databricks", timeout=600, json={
        "host": c.get("databricks_host", ""), "catalog": c.get("metadata_catalog", ""),
        "schema": c.get("metadata_schema", ""), "workspace_path": "/Shared/DBX/MetadataPipeline",
        "landing_path": c.get("volume_path", "/mnt/landing")}).json()
    name = groups.get(gid, {}).get("full_table", gid)
    print(f"submit {name:28s} ({gid}) ->", r.get("run_id") if r.get("success") else r.get("error"))
    if r.get("success"):
        runs[gid] = (name, int(r["run_id"]))

pending = dict(runs)
while pending:
    for gid, (name, rid) in list(pending.items()):
        run = w.jobs.get_run(rid)
        lc = run.state.life_cycle_state.value if run.state.life_cycle_state else ""
        if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            rs = run.state.result_state.value if run.state.result_state else ""
            tasks = {t.task_key: (t.state.result_state.value if t.state and t.state.result_state else "?")
                     for t in run.tasks or []}
            print(f"\n{name}: {lc} {rs}  {tasks}")
            for t in run.tasks or []:
                if t.state and t.state.result_state and t.state.result_state.value == "FAILED":
                    try:
                        out = w.jobs.get_run_output(t.run_id)
                        print(f"   {t.task_key} error:", (out.error or "")[:400])
                    except Exception:
                        pass
            pending.pop(gid)
    if pending:
        time.sleep(20)

print("\n--- row counts ---")
for gid, (name, _) in runs.items():
    t = name.split(".", 1)[1].lower()
    for layer in ("dbx_bronze", "dbx_silver"):
        for sch in ("servicenow_rest", "sales"):
            q = w.statement_execution.execute_statement(
                warehouse_id=WAREHOUSE, statement=f"SELECT COUNT(*) FROM {layer}.{sch}.{t}", wait_timeout="50s")
            if "SUCCEEDED" in str(q.status.state):
                print(f"{layer}.{sch}.{t:20s} -> {q.result.data_array[0][0]}")
