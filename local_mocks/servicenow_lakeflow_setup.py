"""Wire the real ServiceNow PDI into Databricks Lakeflow Connect.

    python servicenow_lakeflow_setup.py redirect      # step 1: set OAuth redirect URL in ServiceNow
    python servicenow_lakeflow_setup.py connection    # step 2: create UC ServiceNow connection
    python servicenow_lakeflow_setup.py pipeline      # step 3: create + start ingestion pipeline
    python servicenow_lakeflow_setup.py status        # pipeline update status
"""
import base64
import json
import sys

import requests
from databricks.sdk import WorkspaceClient
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SCOPE = "migration-studio"
CONN_NAME = "servicenow_dev405660"
PIPELINE_NAME = "lakeflow_servicenow_dev405660"
DEST_CATALOG, DEST_SCHEMA = "dbx_bronze", "servicenow"
TABLES = ("incident", "change_request", "cmdb_ci", "problem", "sc_request", "sc_req_item", "task",
          "sys_user", "sys_user_group", "cmdb_ci_server", "cmdb_ci_computer")

w = WorkspaceClient()
HOST = w.config.host.rstrip("/")


def secret(key):
    return base64.b64decode(w.secrets.get_secret(SCOPE, key).value).decode()


INSTANCE = secret("source-servicenow-instance-url").rstrip("/")
INSTANCE_ID = INSTANCE.split("//", 1)[-1].split(".", 1)[0]
CID, CSEC = secret("source-servicenow-client-id"), secret("source-servicenow-client-secret")

sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(total=3, connect=3, read=3, backoff_factor=2)))


def sn_tokens():
    r = sess.post(f"{INSTANCE}/oauth_token.do", timeout=(20, 60), data={
        "grant_type": "password", "client_id": CID, "client_secret": CSEC, "scope": "useraccount",
        "username": secret("source-servicenow-username"), "password": secret("source-servicenow-password")})
    r.raise_for_status()
    return r.json()


def step_redirect():
    tok = sn_tokens()["access_token"]
    h = {"Authorization": f"Bearer {tok}", "Accept": "application/json", "Content-Type": "application/json"}
    r = sess.get(f"{INSTANCE}/api/now/table/oauth_entity", headers=h, timeout=(20, 60),
                 params={"sysparm_query": f"client_id={CID}", "sysparm_fields": "sys_id,name,redirect_url"})
    r.raise_for_status()
    rec = r.json()["result"][0]
    redirect = f"{HOST}/login/oauth/servicenow.html"
    p = sess.patch(f"{INSTANCE}/api/now/table/oauth_entity/{rec['sys_id']}", headers=h, timeout=(20, 60),
                   json={"redirect_url": redirect})
    p.raise_for_status()
    print(f"OAuth app '{rec['name']}': redirect_url {rec.get('redirect_url') or '(empty)'} -> {p.json()['result']['redirect_url']}")


def step_connection():
    from databricks.sdk.service.catalog import ConnectionType
    # API-created ServiceNow connections are OAuth M2M (client_credentials grant)
    opts = {"instance_url": INSTANCE, "client_id": CID, "client_secret": CSEC,
            "oauth_scope": "useraccount"}
    existing = {c.name for c in w.connections.list()}
    if CONN_NAME in existing:
        w.connections.update(CONN_NAME, options=opts)
        print(f"Updated connection {CONN_NAME}")
    else:
        w.connections.create(name=CONN_NAME, connection_type=ConnectionType.SERVICENOW, options=opts,
                             comment="Real ServiceNow PDI for Migration Studio / Lakeflow Connect")
        print(f"Created connection {CONN_NAME}")
    info = w.connections.get(CONN_NAME)
    print("type:", info.connection_type, "| options keys:", sorted((info.options or {}).keys()))


def step_m2m():
    r = sess.post(f"{INSTANCE}/oauth_token.do", timeout=(20, 60), data={
        "grant_type": "client_credentials", "client_id": CID, "client_secret": CSEC, "scope": "useraccount"})
    ok = r.ok and "access_token" in r.text
    print(f"client_credentials grant -> HTTP {r.status_code} {'OK' if ok else r.text[:200]}")
    if ok:
        t = r.json()["access_token"]
        q = sess.get(f"{INSTANCE}/api/now/table/incident", params={"sysparm_limit": 1}, timeout=(20, 60),
                     headers={"Authorization": f"Bearer {t}", "Accept": "application/json"})
        print(f"incident via M2M token -> HTTP {q.status_code} {'' if q.ok else q.text[:200]}")


def step_enable_m2m():
    tok = sn_tokens()["access_token"]
    h = {"Authorization": f"Bearer {tok}", "Accept": "application/json", "Content-Type": "application/json"}
    prop = "glide.oauth.inbound.client.credential.grant_type.enabled"
    r = sess.get(f"{INSTANCE}/api/now/table/sys_properties", headers=h, timeout=(20, 60),
                 params={"sysparm_query": f"name={prop}", "sysparm_fields": "sys_id,value"})
    r.raise_for_status()
    rows = r.json()["result"]
    if rows:
        p = sess.patch(f"{INSTANCE}/api/now/table/sys_properties/{rows[0]['sys_id']}", headers=h,
                       timeout=(20, 60), json={"value": "true"})
    else:
        p = sess.post(f"{INSTANCE}/api/now/table/sys_properties", headers=h, timeout=(20, 60),
                      json={"name": prop, "value": "true", "type": "boolean"})
    print(f"{prop} -> HTTP {p.status_code} value={p.json().get('result', {}).get('value')}")

    u = sess.get(f"{INSTANCE}/api/now/table/sys_user", headers=h, timeout=(20, 60),
                 params={"sysparm_query": f"user_name={secret('source-servicenow-username')}", "sysparm_fields": "sys_id"})
    user_id = u.json()["result"][0]["sys_id"]
    e = sess.get(f"{INSTANCE}/api/now/table/oauth_entity", headers=h, timeout=(20, 60),
                 params={"sysparm_query": f"client_id={CID}"})
    rec = e.json()["result"][0]
    user_field = next((f for f in ("user", "oauth_application_user") if f in rec), None)
    if not user_field:
        sys.exit(f"No OAuth application user field on oauth_entity; fields: {sorted(rec)}")
    p = sess.patch(f"{INSTANCE}/api/now/table/oauth_entity/{rec['sys_id']}", headers=h, timeout=(20, 60),
                   json={user_field: user_id})
    print(f"oauth_entity.{user_field} -> HTTP {p.status_code}")


def step_pipeline():
    w.api_client.do("POST", "/api/2.1/unity-catalog/schemas",
                    body={"name": DEST_SCHEMA, "catalog_name": DEST_CATALOG}) if DEST_SCHEMA not in \
        {s.name for s in w.schemas.list(catalog_name=DEST_CATALOG)} else None
    objects = [{"table": {"source_schema": "default", "source_table": t,
                           "destination_catalog": DEST_CATALOG,
                           "destination_schema": DEST_SCHEMA}} for t in TABLES]
    existing = [p for p in w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME}'")]
    if existing:
        pid = existing[0].pipeline_id
        spec = w.api_client.do("GET", f"/api/2.0/pipelines/{pid}")["spec"]
        spec["ingestion_definition"]["objects"] = objects
        w.api_client.do("PUT", f"/api/2.0/pipelines/{pid}", body=spec)
        print(f"Updated pipeline {pid}: {len(objects)} tables")
    else:
        body = {
            "name": PIPELINE_NAME,
            "catalog": DEST_CATALOG,
            "schema": DEST_SCHEMA,
            "serverless": True,
            "continuous": False,
            "ingestion_definition": {"connection_name": CONN_NAME, "objects": objects},
        }
        resp = w.api_client.do("POST", "/api/2.0/pipelines", body=body)
        pid = resp["pipeline_id"]
        print(f"Created pipeline {PIPELINE_NAME}: {pid}")
    upd = w.api_client.do("POST", f"/api/2.0/pipelines/{pid}/updates", body={"full_refresh": False})
    print(f"Started update {upd.get('update_id')} -> {HOST}/pipelines/{pid}")


def step_status():
    ps = list(w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME}'"))
    if not ps:
        sys.exit("pipeline not found")
    pid = ps[0].pipeline_id
    p = w.pipelines.get(pid)
    print("pipeline state:", p.state, "| health:", p.health)
    for u in (p.latest_updates or [])[:3]:
        print("  update", u.update_id, u.state, u.creation_time)
    ev = w.api_client.do("GET", f"/api/2.0/pipelines/{pid}/events", query={"max_results": 15})
    for e in ev.get("events", []):
        if e.get("level") in ("ERROR", "WARN"):
            print("  ", e.get("level"), e.get("message", "")[:300])


if __name__ == "__main__":
    {"redirect": step_redirect, "connection": step_connection, "m2m": step_m2m,
     "enable-m2m": step_enable_m2m, "pipeline": step_pipeline, "status": step_status}[sys.argv[1]]()
