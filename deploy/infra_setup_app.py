"""Standalone Infrastructure Setup UI — runs locally on port 5656.

Usage:
    python deploy/infra_setup_app.py

Opens http://localhost:5656 with a web UI to provision Azure Storage,
Access Connector, and Unity Catalog objects per environment (dev/staging/prod).
Temporary / test-purpose tool — NOT part of the main Migration Studio app.
"""

import json, os, sys, threading, time, uuid
from datetime import datetime
from flask import Flask, render_template_string, request, jsonify, Response

app = Flask(__name__)

# ---------------------------------------------------------------------------
# HTML served inline — single self-contained file
# ---------------------------------------------------------------------------
_HTML_PATH = os.path.join(os.path.dirname(__file__), "infra_setup.html")


@app.route("/")
def index():
    with open(_HTML_PATH, encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_log_lines: list[str] = []
_running = False


def _ts():
    return datetime.now().strftime("%H:%M:%S")


def _log(msg, level="INFO"):
    line = f"[{_ts()}] [{level}] {msg}"
    _log_lines.append(line)
    print(line)


def _get_credential(cfg):
    tenant = cfg.get("azure_tenant_id", "")
    client = cfg.get("azure_client_id", "")
    secret = cfg.get("azure_client_secret", "")
    if tenant and client and secret:
        from azure.identity import ClientSecretCredential
        cred = ClientSecretCredential(tenant_id=tenant, client_id=client, client_secret=secret)
        cred.get_token("https://management.azure.com/.default")
        _log("Authenticated via Service Principal.")
        return cred
    from azure.identity import DefaultAzureCredential
    cred = DefaultAzureCredential()
    cred.get_token("https://management.azure.com/.default")
    _log("Authenticated via DefaultAzureCredential.")
    return cred


def _databricks_api(method, path, cfg, payload=None):
    import requests
    host = cfg["databricks_host"].rstrip("/")
    token = cfg["databricks_token"]
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.request(method, f"{host}{path}", headers=headers, json=payload, timeout=30)
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text}
    return (200 <= r.status_code < 300), body


# ---------------------------------------------------------------------------
# Steps — ported from AutoInfraCreation.py, self-contained
# ---------------------------------------------------------------------------

def step1_storage(cfg):
    _log("═══ Step 1: Storage Account + Container + Folders ═══")
    from azure.mgmt.storage import StorageManagementClient
    from azure.mgmt.storage.models import StorageAccountCreateParameters, Sku, Kind
    from azure.storage.filedatalake import DataLakeServiceClient

    cred = _get_credential(cfg)
    sub, rg, sa, loc, ctr = cfg["subscription_id"], cfg["resource_group"], cfg["storage_account"], cfg["region"], cfg["container"]

    sc = StorageManagementClient(cred, sub)
    try:
        sc.storage_accounts.get_properties(rg, sa)
        _log(f"Storage account '{sa}' already exists — OK.")
    except Exception:
        _log(f"Creating storage account '{sa}' in '{loc}'…")
        p = StorageAccountCreateParameters(sku=Sku(name="Standard_LRS"), kind=Kind.STORAGE_V2, location=loc, is_hns_enabled=True)
        sc.storage_accounts.begin_create(rg, sa, p).result()
        _log(f"✅ Storage account '{sa}' created.")

    dl = DataLakeServiceClient(account_url=f"https://{sa}.dfs.core.windows.net", credential=cred)
    try:
        fs = dl.create_file_system(ctr)
        _log(f"✅ Container '{ctr}' created.")
    except Exception as e:
        if "already exists" in str(e).lower():
            _log(f"Container '{ctr}' already exists — OK.")
            fs = dl.get_file_system_client(ctr)
        else:
            raise

    for folder in cfg.get("folders", []):
        try:
            fs.create_directory(folder)
            _log(f"  ✅ Folder '{folder}' created.")
        except Exception as e:
            if "already exists" in str(e).lower():
                _log(f"  Folder '{folder}' already exists — OK.")
            else:
                _log(f"  ❌ {folder}: {e}", "ERROR")


def step2_access_connector(cfg):
    _log("═══ Step 2: Access Connector + Role Assignment ═══")
    from azure.mgmt.databricks import AzureDatabricksManagementClient
    from azure.mgmt.authorization import AuthorizationManagementClient

    cred = _get_credential(cfg)
    sub, rg, loc = cfg["subscription_id"], cfg["resource_group"], cfg["region"]
    ac_name = cfg["access_connector"]
    sa = cfg["storage_account"]
    role = cfg.get("role_assignment", "Storage Blob Data Owner")

    dbr = AzureDatabricksManagementClient(cred, sub)
    try:
        p = dbr.access_connectors.begin_create_or_update(rg, ac_name, {"location": loc, "identity": {"type": "SystemAssigned"}})
        ac = p.result()
        _log(f"✅ Access Connector '{ac_name}' created/updated.")
    except Exception:
        ac = dbr.access_connectors.get(rg, ac_name)
        _log(f"Access Connector '{ac_name}' already exists — using it.")

    connector_id = ac.id
    principal_id = ac.identity.principal_id if ac.identity else None
    cfg["_connector_id"] = connector_id

    if not principal_id:
        _log("No principalId — role assignment skipped.", "WARN")
        return

    storage_scope = f"/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Storage/storageAccounts/{sa}"
    auth = AuthorizationManagementClient(cred, sub)

    roles_to_assign = [
        "Storage Blob Data Owner",
        "Storage Account Contributor",
        "EventGrid EventSubscription Contributor",
        "Storage Queue Data Contributor",
    ]
    for role_name in roles_to_assign:
        defs = list(auth.role_definitions.list(storage_scope, filter=f"roleName eq '{role_name}'"))
        if not defs:
            _log(f"Role '{role_name}' not found — skipping.", "WARN")
            continue
        assignment_name = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{principal_id}:{defs[0].id}:{storage_scope}"))
        try:
            auth.role_assignments.create(storage_scope, assignment_name, {
                "role_definition_id": defs[0].id, "principal_id": principal_id, "principal_type": "ServicePrincipal"})
            _log(f"✅ Role '{role_name}' assigned.")
        except Exception as e:
            if "already exists" in str(e).lower():
                _log(f"Role '{role_name}' already exists — OK.")
            else:
                _log(f"Role '{role_name}' warning: {e}", "WARN")


def step3_storage_credential(cfg):
    _log("═══ Step 3: Storage Credential ═══")
    cred_name = cfg.get("storage_credential_name") or cfg["access_connector"]
    cid = cfg.get("_connector_id", "")
    if not cid:
        _log("No connector_id — skipping.", "ERROR")
        return
    payload = {"name": cred_name, "azure_managed_identity": {"access_connector_id": cid}, "skip_validation": True}
    ok, body = _databricks_api("POST", "/api/2.1/unity-catalog/storage-credentials", cfg, payload)
    if ok:
        _log(f"✅ Storage credential '{cred_name}' registered.")
    elif "already exists" in json.dumps(body).lower():
        _log(f"Storage credential '{cred_name}' already exists — updating.")
        _databricks_api("PATCH", f"/api/2.1/unity-catalog/storage-credentials/{cred_name}", cfg,
                         {"azure_managed_identity": {"access_connector_id": cid}, "skip_validation": True, "force": True})
    else:
        _log(f"❌ {body}", "ERROR")
        return
    cfg["_cred_name"] = cred_name

    for attempt in range(1, 4):
        vok, _ = _databricks_api("PATCH", f"/api/2.1/unity-catalog/storage-credentials/{cred_name}", cfg,
                                  {"azure_managed_identity": {"access_connector_id": cid}, "skip_validation": False, "force": True})
        if vok:
            _log(f"✅ Validated (attempt {attempt}).")
            return
        if attempt < 3:
            _log(f"Validation attempt {attempt}/3 — RBAC propagating, waiting 15s…")
            time.sleep(15)
    _log("Validation deferred — RBAC still propagating.", "WARN")


def step4_external_locations(cfg):
    _log("═══ Step 4: External Location (single root) ═══")
    cred_name = cfg.get("_cred_name", cfg.get("storage_credential_name", cfg["access_connector"]))
    base = f"abfss://{cfg['container']}@{cfg['storage_account']}.dfs.core.windows.net"
    env = cfg.get("_env", "dev")
    loc_name = f"{env}-root-loc"
    _log(f"Creating single external location '{loc_name}' → {base}")
    ok, body = _databricks_api("POST", "/api/2.1/unity-catalog/external-locations", cfg,
                                {"name": loc_name, "url": base, "credential_name": cred_name, "skip_validation": True})
    if ok:
        _log(f"  ✅ '{loc_name}' created — covers all subfolders.")
    elif "already exists" in json.dumps(body).lower():
        _log(f"  '{loc_name}' already exists — updating credential.")
        _databricks_api("PATCH", f"/api/2.1/unity-catalog/external-locations/{loc_name}", cfg,
                         {"credential_name": cred_name, "skip_validation": True})
    elif "overlap" in json.dumps(body).lower():
        _log(f"  '{loc_name}' overlaps an existing location — all paths are already covered.")
    else:
        _log(f"  ❌ {json.dumps(body)[:200]}", "ERROR")


def step5_catalogs(cfg):
    _log("═══ Step 5: Catalogs + Schemas ═══")
    base = f"abfss://{cfg['container']}@{cfg['storage_account']}.dfs.core.windows.net"
    for cat_name, cat_cfg in cfg.get("catalogs", {}).items():
        loc = cat_cfg.get("location", "")
        schemas = cat_cfg.get("schemas", ["default"]) or ["default"]
        payload = {"name": cat_name, "comment": f"Auto-created ({cat_name})"}
        if loc:
            payload["storage_root"] = loc
        ok, body = _databricks_api("POST", "/api/2.1/unity-catalog/catalogs", cfg, payload)
        if ok:
            _log(f"✅ Catalog '{cat_name}' created.")
        elif "already exists" in json.dumps(body).lower():
            _log(f"Catalog '{cat_name}' already exists — OK.")
        else:
            _log(f"❌ Catalog '{cat_name}': {json.dumps(body)[:200]}", "ERROR")
            continue
        for sch in schemas:
            ok2, body2 = _databricks_api("POST", "/api/2.1/unity-catalog/schemas", cfg,
                                          {"name": sch, "catalog_name": cat_name})
            if ok2:
                _log(f"  ✅ Schema '{cat_name}.{sch}' created.")
            elif "already exists" in json.dumps(body2).lower():
                _log(f"  Schema '{cat_name}.{sch}' already exists — OK.")
            else:
                _log(f"  ❌ Schema: {json.dumps(body2)[:200]}", "ERROR")


def step6_volume(cfg):
    _log("═══ Step 6: Volume ═══")
    vol = cfg.get("volume_name", "landing")
    cat = cfg.get("volume_catalog", "")
    sch = cfg.get("volume_schema", "default")
    base = f"abfss://{cfg['container']}@{cfg['storage_account']}.dfs.core.windows.net"
    path = cfg.get("volume_path", "") or f"{base}/{cfg.get('folders', ['landing'])[0]}"
    if not cat:
        _log("No volume_catalog — skipping.", "WARN")
        return
    payload = {"name": vol, "catalog_name": cat, "schema_name": sch, "volume_type": "EXTERNAL", "storage_location": path}
    for attempt in range(1, 4):
        ok, body = _databricks_api("POST", "/api/2.1/unity-catalog/volumes", cfg, payload)
        if ok:
            _log(f"✅ Volume '{cat}.{sch}.{vol}' created → {path}")
            return
        if "already exists" in json.dumps(body).lower():
            _log(f"Volume '{vol}' already exists — OK.")
            return
        if any(k in json.dumps(body).lower() for k in ("cloud_storage", "access", "abfs")):
            if attempt < 3:
                _log(f"Attempt {attempt}/3 — RBAC propagating, waiting 20s…", "WARN")
                time.sleep(20)
                continue
        _log(f"❌ Volume: {json.dumps(body)[:200]}", "ERROR")
        return


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.route("/api/run", methods=["POST"])
def api_run():
    global _running, _log_lines
    if _running:
        return jsonify({"ok": False, "error": "Already running"}), 409
    data = request.json or {}
    env = data.get("environment", "dev")
    suffix_cat = {"dev": "", "staging": "_staging", "prod": "_prod"}[env]
    suffix_sa = {"dev": "", "staging": "stg", "prod": "prod"}[env]

    sa_base = data.get("storage_account_base", "").strip()
    ac_base = data.get("access_connector_base", "").strip()
    sc_base = data.get("storage_credential_base", "").strip() or ac_base
    bronze_base = data.get("catalog_bronze_base", "bronze").strip()
    silver_base = data.get("catalog_silver_base", "silver").strip()
    volumes_base = data.get("catalog_volumes_base", "dev_volumes").strip()
    default_schema = data.get("default_schema", "hr").strip() or "hr"
    container = data.get("container", "datalake").strip()

    sa = (sa_base + suffix_sa).lower()
    ac = ac_base + suffix_cat
    sc = sc_base + suffix_cat
    bronze = bronze_base + suffix_cat
    silver = silver_base + suffix_cat
    volumes = volumes_base + suffix_cat

    landing_f = f"{env}/landing"
    bronze_f = f"{env}/uc-managed/bronze"
    silver_f = f"{env}/uc-managed/silver"
    base_path = f"abfss://{container}@{sa}.dfs.core.windows.net"

    catalogs = {
        bronze: {"location": f"{base_path}/{bronze_f}", "schemas": [default_schema]},
        silver: {"location": f"{base_path}/{silver_f}", "schemas": [default_schema]},
        volumes: {"location": f"{base_path}/{env}/uc-managed/volumes", "schemas": [default_schema]},
    }
    if data.get("include_metadata"):
        catalogs[data.get("catalog_admin", "admin_source")] = {"location": f"{base_path}/{env}/uc-managed/admin", "schemas": ["configtables", "migration_app"]}
        catalogs[data.get("catalog_recon", "reconciliation")] = {"location": f"{base_path}/{env}/uc-managed/reconciliation", "schemas": [default_schema]}
        catalogs[data.get("catalog_logging", "loggingdetails")] = {"location": f"{base_path}/{env}/uc-managed/logging", "schemas": [default_schema]}

    cfg = {
        "_env": env,
        "subscription_id": data.get("subscription_id", ""),
        "resource_group": data.get("resource_group", ""),
        "region": data.get("region", "centralindia"),
        "storage_account": sa,
        "container": container,
        "access_connector": ac,
        "storage_credential_name": sc,
        "role_assignment": data.get("role_assignment", "Storage Blob Data Owner"),
        "folders": [landing_f, bronze_f, silver_f,
                   f"{env}/uc-managed/volumes", f"{env}/uc-managed/admin",
                   f"{env}/uc-managed/reconciliation", f"{env}/uc-managed/logging"],
        "catalogs": catalogs,
        "volume_name": "landing",
        "volume_catalog": volumes,
        "volume_schema": default_schema,
        "volume_path": f"{base_path}/{landing_f}",
        "databricks_host": data.get("databricks_host", ""),
        "databricks_token": data.get("databricks_token", ""),
        "azure_tenant_id": data.get("azure_tenant_id", ""),
        "azure_client_id": data.get("azure_client_id", ""),
        "azure_client_secret": data.get("azure_client_secret", ""),
    }

    _log_lines = []
    _running = True

    def _run():
        global _running
        try:
            steps_to_run = data.get("steps", [1, 2, 3, 4, 5, 6])
            if 1 in steps_to_run:
                step1_storage(cfg)
            if 2 in steps_to_run:
                step2_access_connector(cfg)
            if 3 in steps_to_run:
                step3_storage_credential(cfg)
            if 4 in steps_to_run:
                step4_external_locations(cfg)
            if 5 in steps_to_run:
                step5_catalogs(cfg)
            if 6 in steps_to_run:
                step6_volume(cfg)
            _log("═══ All steps complete ═══")
        except Exception as e:
            _log(f"Fatal error: {e}", "ERROR")
        finally:
            _running = False

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "message": "Started"})


@app.route("/api/logs")
def api_logs():
    return jsonify({"running": _running, "lines": _log_lines})


@app.route("/api/status")
def api_status():
    return jsonify({"running": _running, "line_count": len(_log_lines)})


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 60)
    print("  Infrastructure Setup UI — http://localhost:5656")
    print("=" * 60)
    app.run(host="0.0.0.0", port=5656, debug=False)
