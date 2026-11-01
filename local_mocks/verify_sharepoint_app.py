"""End-to-end check of SharePoint via REST (No Auth) against the public Northwind
OData V3 service (SharePoint REST JSON format). Does not change saved Settings.

    python verify_sharepoint_app.py local      # notebook paging helpers vs live service
    python verify_sharepoint_app.py discover   # app Test Connection + Discover Tables
    python verify_sharepoint_app.py create     # deploy notebooks + create SDP pipelines
    python verify_sharepoint_app.py run        # run every sharepoint.* group on Databricks
    python verify_sharepoint_app.py counts     # Bronze / Silver row counts
"""
import base64
import os
import re
import sys
import time

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "migration_utility"))

APP = "http://localhost:5000/api/v1"
SERVICE = "https://services.odata.org/V3/Northwind/Northwind.svc"
TABLES = ("Customers", "Orders", "Products", "Order_Details")
EXPECTED = {"Customers": 91, "Orders": 830, "Products": 77, "Order_Details": 2155}
CFG = {"source_type": "sharepoint", "server": SERVICE, "api_auth_type": "none", "api_key_header": "",
       "objects": ", ".join(TABLES), "database": "", "username": "", "password": "", "tenant_id": ""}
TARGET_SCHEMA = "sharepoint_src"


def local():
    import source_registry
    from metadata_notebooks import generate_metadata_notebooks
    nbs = generate_metadata_notebooks(catalog="c", schema="s", landing_path="/Volumes/x/y/landing",
                                      pipeline_mode="dlt")["notebooks"]
    text = next(n for n in nbs if n["name"] == "01_Meta_Extract")["code"]
    block = text[text.index("if IS_API_SOURCE:\n    import requests as _requests"):text.index("def _read_source(")]
    prof = source_registry.rest_profile("sharepoint", "none")
    for t, exp in EXPECTED.items():
        g = {"IS_API_SOURCE": True, "SERVER": SERVICE, "USERNAME": "", "PASSWORD": "", "TABLE_NAME": t,
             "base64": base64, "_re": re, "dbutils": None,
             "source_config": {"api_auth_type": "none", "api_profile": prof, "api_base": SERVICE}}
        exec(block, g)
        n = len(g["_api_records"](g["_api_endpoint_for_table"]()))
        print(f"EXTRACT: {t:14s} records={n:5d} expected={exp:5d} {'OK' if n == exp else 'MISMATCH'}")


def wait_app():
    for _ in range(60):
        try:
            requests.get(APP + "/sources/registry.js", timeout=3)
            return
        except requests.RequestException:
            time.sleep(2)
    sys.exit("app not reachable on :5000")


def groups():
    d = requests.get(APP + "/workflow/pipelines", timeout=120).json()
    return [g for g in (d.get("groups") or d.get("pipelines") or [])
            if str(g.get("full_table", "")).startswith("sharepoint.")]


def cfg():
    return requests.get(APP + "/deploy-config", timeout=60).json()["config"]


def discover():
    t = requests.post(APP + "/source/test-connection", json=CFG, timeout=300).json()
    print("Test Connection:", t.get("success"), "|", t.get("server_version") or t.get("error"))
    o = requests.post(APP + "/source/load-objects", json=CFG, timeout=300).json()
    print("Load Objects   :", o.get("success"), o.get("total"), "objects", o.get("error") or "")
    d = requests.post(APP + "/workflow/list-tables", json=CFG, timeout=900).json()
    print("Discover Tables:", d.get("total"), "tables", "" if d.get("success") else d.get("error"))
    for tb in d.get("tables", []):
        print(f"  {tb['full_name']:28s} {tb['row_estimate']:>6,} rows  {tb['col_count']:>4} cols")


def create():
    c = cfg()
    all_groups = requests.get(APP + "/workflow/pipelines", timeout=120).json()
    all_groups = all_groups.get("groups") or all_groups.get("pipelines") or []
    ref = next((g for g in all_groups if str(g.get("full_table", "")).startswith("ms_dynamics_crm.")
                and g.get("pipeline_mode") == "dlt"), None)
    if not ref:
        sys.exit("No reference SDP group (ms_dynamics_crm.*) to copy targets from")
    tc = dict(ref["target_config"], target_schema=TARGET_SCHEMA)
    if tc.get("schema") == ref["target_config"].get("target_schema"):
        tc["schema"] = TARGET_SCHEMA
    r = requests.post(APP + "/workflow/notebooks/deploy", timeout=600, json={
        "host": c.get("databricks_host", ""), "catalog": c.get("metadata_catalog", ""),
        "schema": c.get("metadata_schema", ""), "landing_path": c.get("volume_path", "/mnt/landing"),
        "workspace_path": "/Shared/DBX/MetadataPipeline", "pipeline_mode": "dlt"}).json()
    print("deploy notebooks:", r.get("message") or r.get("error"))
    r = requests.post(APP + "/workflow/create-pipelines-bulk", timeout=900, json={
        "tables": [{"schema": "sharepoint", "table": t, "load_type": "full", "watermark_column": ""} for t in TABLES],
        "source_config": CFG, "target_config": tc, "pipeline_mode": "dlt",
        "cdc_mode": "watermark", "primary_keys": [], "use_layer_mapping": False}).json()
    print("create:", r.get("success"), "| created:", r.get("created"), "|", r.get("error") or "")
    for e in r.get("errors") or []:
        print("  error:", e)


def run():
    c = cfg()
    for g in groups():
        r = requests.post(f"{APP}/workflow/pipelines/{g['group_id']}/run-databricks", timeout=300, json={
            "host": c.get("databricks_host", ""), "catalog": c.get("metadata_catalog", ""),
            "schema": c.get("metadata_schema", ""), "workspace_path": "/Shared/DBX/MetadataPipeline",
            "landing_path": c.get("volume_path", "/mnt/landing")}).json()
        print(f"run {g['full_table']:28s}", r.get("success"), "| run_id:", r.get("run_id"), "|", r.get("error") or "")


def counts():
    from dbsql_client import execute_query
    for cat in ("dbx_bronze", "dbx_silver"):
        names = {r.get("tableName") or r.get("tablename") for r in execute_query(f"SHOW TABLES IN {cat}.{TARGET_SCHEMA}")}
        for t, exp in EXPECTED.items():
            t = t.lower()
            if t not in names:
                print(f"{cat}.{TARGET_SCHEMA}.{t:14s} MISSING")
                continue
            n = int(next(iter(execute_query(f"SELECT COUNT(*) AS c FROM {cat}.{TARGET_SCHEMA}.{t}")[0].values())))
            print(f"{cat}.{TARGET_SCHEMA}.{t:14s} {n:>5}  expected {exp:>5}  {'OK' if n == exp else 'MISMATCH'}")


if __name__ == "__main__":
    step = sys.argv[1]
    if step in ("discover", "create", "run"):
        wait_app()
    {"local": local, "discover": discover, "create": create, "run": run, "counts": counts}[step]()
