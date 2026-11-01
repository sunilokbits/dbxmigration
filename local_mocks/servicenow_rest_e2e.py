"""End-to-end: ServiceNow via REST API (OAuth) -> app Extract (landing) -> SDP Bronze -> Silver.

    python servicenow_rest_e2e.py [table ...]      # default: incident problem sys_user
"""
import sys
import time

import requests
from databricks.sdk import WorkspaceClient

BASE = "http://localhost:5000/api/v1"
INSTANCE = "https://dev405660.service-now.com"
OBJECTS = ("incident, change_request, cmdb_ci, problem, sc_request, sc_req_item, task, "
           "sys_user, sys_user_group, cmdb_ci_server, cmdb_ci_computer")
TARGET_SCHEMA = "servicenow_rest"  # keeps REST outputs apart from the Lakeflow-based groups
WAREHOUSE = "d01073f7104f07ff"
RUN_TABLES = sys.argv[1:] or ["incident", "problem", "sys_user"]

w = WorkspaceClient()


def sql(stmt):
    r = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE, statement=stmt, wait_timeout="50s")
    if "SUCCEEDED" not in str(r.status.state):
        raise RuntimeError(r.status.error.message if r.status.error else str(r.status.state))
    return r.result.data_array if r.result else []


for _ in range(60):
    try:
        requests.get(BASE + "/sources/registry.js", timeout=3)
        break
    except requests.RequestException:
        time.sleep(2)

# 1) Settings: ServiceNow + REST API + OAuth 2.0
cfg = requests.get(BASE + "/deploy-config", timeout=60).json()["config"]
src = {"source_type": "servicenow", "ingestion_method": "rest_api", "server": INSTANCE,
       "api_auth_type": "oauth2", "api_key_header": "", "objects": OBJECTS,
       "database": "", "username": "", "password": cfg.get("source", {}).get("password", "")}
cfg["source"] = src
print("1. save config      :", requests.post(BASE + "/deploy-config", json=cfg, timeout=120).json().get("success"))

# 2) Test Connection
t = requests.post(BASE + "/source/test-connection", json=src, timeout=300).json()
print("2. test connection  :", t.get("success"), "|", t.get("server_version") or t.get("error"))

# 3) Discover Tables
d = requests.post(BASE + "/workflow/list-tables", json=src, timeout=900).json()
print(f"3. discover tables  : {d.get('total')} tables")
for tb in d.get("tables", []):
    print(f"     {tb['full_name']:30s} {tb['row_estimate']:>6,} rows {tb['col_count']:>4} cols")

# 4) Target schemas for the REST outputs
for cat in ("dbx_bronze", "dbx_silver"):
    sql(f"CREATE SCHEMA IF NOT EXISTS {cat}.{TARGET_SCHEMA}")
print(f"4. target schemas   : dbx_bronze/dbx_silver.{TARGET_SCHEMA} ready")

# 5) Deploy notebooks + create pipelines (targets copied from an existing SDP group)
gs = requests.get(BASE + "/workflow/pipelines", timeout=120).json()
gs = gs.get("groups") or gs.get("pipelines") or []
ref = next(g for g in gs if g.get("pipeline_mode") == "dlt" and str(g.get("full_table", "")).startswith("adobe."))
tc = dict(ref["target_config"], target_schema=TARGET_SCHEMA, schema=TARGET_SCHEMA)
dep = requests.post(BASE + "/workflow/notebooks/deploy", timeout=600, json={
    "host": cfg.get("databricks_host", ""), "catalog": cfg.get("metadata_catalog", ""),
    "schema": cfg.get("metadata_schema", ""), "landing_path": cfg.get("volume_path", "/mnt/landing"),
    "workspace_path": "/Shared/DBX/MetadataPipeline", "pipeline_mode": "dlt"}).json()
print("5. deploy notebooks :", dep.get("message") or dep.get("error"))
cr = requests.post(BASE + "/workflow/create-pipelines-bulk", timeout=900, json={
    "tables": [{"schema": "servicenow", "table": t, "load_type": "full", "watermark_column": ""} for t in RUN_TABLES],
    "source_config": src, "target_config": tc, "pipeline_mode": "dlt", "cdc_mode": "watermark",
    "primary_keys": [], "use_layer_mapping": False}).json()
print("   create pipelines :", cr.get("success"), "| created:", cr.get("created"), "|", cr.get("error") or "")
groups = {}
for g in cr.get("groups") or []:
    grp = g.get("group") or g
    groups[grp["table_name"]] = g["group_id"]

# 6) Run on Databricks and wait
runs = {}
for t, gid in groups.items():
    r = requests.post(f"{BASE}/workflow/pipelines/{gid}/run-databricks", timeout=300, json={
        "host": cfg.get("databricks_host", ""), "catalog": cfg.get("metadata_catalog", ""),
        "schema": cfg.get("metadata_schema", ""), "workspace_path": "/Shared/DBX/MetadataPipeline",
        "landing_path": cfg.get("volume_path", "/mnt/landing")}).json()
    runs[t] = r.get("run_id")
    print(f"6. run {t:15s}: submitted run {r.get('run_id')} {r.get('error') or ''}")

pending = {t: rid for t, rid in runs.items() if rid}
while pending:
    for t, rid in list(pending.items()):
        r = w.jobs.get_run(rid)
        lc = r.state.life_cycle_state.value if r.state.life_cycle_state else ""
        if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            rs = r.state.result_state.value if r.state.result_state else ""
            bad = [k.task_key for k in (r.tasks or []) if k.state and k.state.result_state
                   and k.state.result_state.value != "SUCCESS"]
            print(f"   {t:15s} -> {lc} {rs}" + (f" | failed: {', '.join(bad)}" if bad else ""))
            if bad:
                out = w.jobs.get_run_output(next(k.run_id for k in r.tasks if k.task_key == bad[0]))
                print("     ", (out.error or "")[:400])
            pending.pop(t)
    if pending:
        time.sleep(20)

# 7) Row counts per layer
print("7. counts (ServiceNow REST -> Bronze -> Silver)")
for t in groups:
    b = sql(f"SELECT COUNT(*) FROM dbx_bronze.{TARGET_SCHEMA}.{t}")[0][0]
    s = sql(f"SELECT COUNT(*) FROM dbx_silver.{TARGET_SCHEMA}.{t}")[0][0]
    print(f"   {t:15s} bronze={b:>6}  silver={s:>6}")
