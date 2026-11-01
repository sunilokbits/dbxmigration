"""End-to-end check of MS Dynamics CRM via REST API (OData v4) through the running
app, using the public Northwind service with No Auth. Does not change saved Settings.

    python verify_dynamics_app.py discover     # Test Connection + Discover Tables
    python verify_dynamics_app.py create       # deploy notebooks + create SDP pipelines
    python verify_dynamics_app.py run          # run every ms_dynamics_crm group on Databricks
    python verify_dynamics_app.py status
"""
import sys
import time

import requests

BASE = "http://localhost:5000/api/v1"
TABLES = ("Customers", "Orders", "Products", "Order_Details")
CFG = {"source_type": "ms_dynamics_crm", "ingestion_method": "rest_api",
       "server": "https://services.odata.org/V4/Northwind/Northwind.svc", "api_auth_type": "none",
       "api_key_header": "", "objects": ", ".join(TABLES), "database": "", "username": "", "password": ""}
TARGET_SCHEMA = "dynamics_crm"


def wait_app():
    for _ in range(60):
        try:
            requests.get(BASE + "/sources/registry.js", timeout=3)
            return
        except requests.RequestException:
            time.sleep(2)
    sys.exit("app not reachable on :5000")


def groups():
    d = requests.get(BASE + "/workflow/pipelines", timeout=120).json()
    return d.get("groups") or d.get("pipelines") or []


def cfg():
    return requests.get(BASE + "/deploy-config", timeout=60).json()["config"]


def discover():
    t = requests.post(BASE + "/source/test-connection", json=CFG, timeout=300).json()
    print("Test Connection:", t.get("success"), "|", t.get("server_version") or t.get("error"))
    o = requests.post(BASE + "/source/load-objects", json=CFG, timeout=300).json()
    print("Load Objects   :", o.get("success"), o.get("total"), "objects", o.get("error") or "")
    d = requests.post(BASE + "/workflow/list-tables", json=CFG, timeout=900).json()
    print("Discover Tables:", d.get("total"), "tables", "" if d.get("success") else d.get("error"))
    for tb in d.get("tables", []):
        print(f"  {tb['full_name']:34s} {tb['row_estimate']:>6,} rows  {tb['col_count']:>4} cols")


def create():
    c = cfg()
    ref = next((g for g in groups() if str(g.get("full_table", "")).startswith("servicenow.")
                and g.get("pipeline_mode") == "dlt"
                and (g.get("source_config") or {}).get("connector") == "api"), None)
    if not ref:
        sys.exit("No reference ServiceNow REST SDP group found to copy targets from")
    tc = dict(ref["target_config"], target_schema=TARGET_SCHEMA)
    for k in ("schema",):
        if k in tc and tc[k] == ref["target_config"].get("target_schema"):
            tc[k] = TARGET_SCHEMA
    print("targets:", {k: tc.get(k) for k in ("volumes_catalog", "bronze_catalog", "silver_catalog", "target_schema")})
    r = requests.post(BASE + "/workflow/notebooks/deploy", timeout=600, json={
        "host": c.get("databricks_host", ""), "catalog": c.get("metadata_catalog", ""),
        "schema": c.get("metadata_schema", ""), "landing_path": c.get("volume_path", "/mnt/landing"),
        "workspace_path": "/Shared/DBX/MetadataPipeline", "pipeline_mode": "dlt"}).json()
    print("deploy notebooks:", r.get("message") or r.get("error"))
    r = requests.post(BASE + "/workflow/create-pipelines-bulk", timeout=900, json={
        "tables": [{"schema": "ms_dynamics_crm", "table": t, "load_type": "full", "watermark_column": ""} for t in TABLES],
        "source_config": CFG, "target_config": tc, "pipeline_mode": "dlt",
        "cdc_mode": "watermark", "primary_keys": [], "use_layer_mapping": False}).json()
    print("create:", r.get("success"), "| created:", r.get("created"), "|", r.get("error") or "")
    for e in r.get("errors") or []:
        print("  error:", e)


def run():
    c = cfg()
    for g in groups():
        if not str(g.get("full_table", "")).startswith("ms_dynamics_crm."):
            continue
        r = requests.post(f"{BASE}/workflow/pipelines/{g['group_id']}/run-databricks", timeout=300, json={
            "host": c.get("databricks_host", ""), "catalog": c.get("metadata_catalog", ""),
            "schema": c.get("metadata_schema", ""), "workspace_path": "/Shared/DBX/MetadataPipeline",
            "landing_path": c.get("volume_path", "/mnt/landing")}).json()
        print(f"run {g['full_table']:34s}", r.get("success"), "| run_id:", r.get("run_id"), "|", r.get("error") or "")


def status():
    for g in groups():
        if str(g.get("full_table", "")).startswith("ms_dynamics_crm."):
            print(f"{g['full_table']:34s} {g.get('status')}  group={g['group_id']}")


if __name__ == "__main__":
    wait_app()
    {"discover": discover, "create": create, "run": run, "status": status}[sys.argv[1]]()
