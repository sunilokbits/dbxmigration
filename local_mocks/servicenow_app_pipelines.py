"""Create + run app pipelines (Extract -> Bronze -> Silver) for the ServiceNow
tables that Lakeflow Connect ingested, mirroring an existing successful group's targets.

    python servicenow_app_pipelines.py create
    python servicenow_app_pipelines.py run incident
    python servicenow_app_pipelines.py status
"""
import sys
import time

import requests

BASE = "http://localhost:5000/api/v1"
TABLES = ("incident", "change_request", "cmdb_ci")


def wait_app():
    for _ in range(60):
        try:
            requests.get(BASE + "/sources/registry.js", timeout=3)
            return
        except requests.RequestException:
            time.sleep(2)


def groups():
    d = requests.get(BASE + "/workflow/pipelines", timeout=120).json()
    return d.get("groups") or d.get("pipelines") or []


def cfg():
    return requests.get(BASE + "/deploy-config", timeout=60).json()["config"]


def deploy_notebooks(c, mode):
    r = requests.post(BASE + "/workflow/notebooks/deploy", timeout=600, json={
        "host": c.get("databricks_host", ""), "catalog": c.get("metadata_catalog", ""),
        "schema": c.get("metadata_schema", ""), "landing_path": c.get("volume_path", "/mnt/landing"),
        "workspace_path": "/Shared/DBX/MetadataPipeline", "pipeline_mode": mode}).json()
    print("deploy notebooks:", r.get("message") or r.get("error"))


def create():
    c = cfg()
    ref = next((g for g in groups() if str(g.get("full_table", "")).startswith(("anaplan.", "sap_ariba.", "adobe."))
                and g.get("pipeline_mode") == "dlt"), None)
    if not ref:
        sys.exit("No reference SDP pipeline group found to copy targets from")
    tc = dict(ref["target_config"])
    print("reference group:", ref["full_table"], "| bronze:", tc.get("bronze_catalog"), "| schema:", tc.get("target_schema"))
    deploy_notebooks(c, "dlt")
    r = requests.post(BASE + "/workflow/create-pipelines-bulk", timeout=900, json={
        "tables": [{"schema": "servicenow", "table": t, "load_type": "full", "watermark_column": ""} for t in TABLES],
        "source_config": c["source"], "target_config": tc, "pipeline_mode": "dlt",
        "cdc_mode": "watermark", "primary_keys": [], "use_layer_mapping": False}).json()
    print("create:", r.get("success"), "| created:", r.get("created"), "|", r.get("error") or "")
    for g in r.get("groups") or []:
        print("  ", g.get("full_table") or g.get("group", {}).get("full_table"), "->",
              g.get("group_id"), "| lakeflow_table:", (g.get("group") or g).get("source_config", {}).get("lakeflow_table"))


def run(table):
    c = cfg()
    g = next((g for g in groups() if g.get("full_table") == f"servicenow.{table}"), None)
    if not g:
        sys.exit(f"no pipeline group for servicenow.{table}")
    r = requests.post(f"{BASE}/workflow/pipelines/{g['group_id']}/run-databricks", timeout=300, json={
        "host": c.get("databricks_host", ""), "catalog": c.get("metadata_catalog", ""),
        "schema": c.get("metadata_schema", ""), "workspace_path": "/Shared/DBX/MetadataPipeline",
        "landing_path": c.get("volume_path", "/mnt/landing")}).json()
    print("run:", r.get("success"), "| run_id:", r.get("run_id"), "|", r.get("run_url") or r.get("error"))


def status():
    for g in groups():
        if str(g.get("full_table", "")).startswith("servicenow."):
            print(f"{g['full_table']:28s} {g.get('status')}  group={g['group_id']}")


if __name__ == "__main__":
    wait_app()
    {"create": create, "status": status}.get(sys.argv[1], lambda: run(sys.argv[2]))()
