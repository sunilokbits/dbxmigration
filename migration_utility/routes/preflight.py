"""Pre-flight readiness blueprint — first-run environment validation.

Runs high-level checks across the deployment environment and returns a
structured pass/warn/fail report:
    • Databricks workspace auth (app service principal / PAT)
    • SQL Warehouse connectivity
    • Unity Catalog catalogs
    • Secret scope
    • Storage account (UC credentials / external locations)
    • Azure subscription access (optional — SP credentials from Settings)
    • Python dependency availability

Every check is failure-isolated: the endpoint ALWAYS returns 200 with
per-check status so the UI can render a complete readiness report.
"""
import importlib
import importlib.metadata as importlib_metadata
import os
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout

from flask import Blueprint, jsonify

from .auth import login_required
from log_config import get_logger
from config_cache import get_config

logger = get_logger(__name__)
preflight_bp = Blueprint("preflight", __name__, url_prefix="/api/v1")

_CHECK_TIMEOUT = 20  # seconds per check


def _mk(id_, name, category, fn, required=True):
    return {"id": id_, "name": name, "category": category, "fn": fn, "required": required}


def _run_one(chk):
    t0 = time.time()
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(chk["fn"])
            try:
                detail, status, hint = fut.result(timeout=_CHECK_TIMEOUT)
            except FutTimeout:
                detail, status, hint = (f"Check timed out after {_CHECK_TIMEOUT}s.", "fail",
                                        "Verify network connectivity and that the workspace is running.")
    except Exception as e:  # defensive — never let one check kill the report
        detail, status, hint = (f"{type(e).__name__}: {str(e)[:250]}", "fail", "")
    return {"id": chk["id"], "name": chk["name"], "category": chk["category"],
            "status": status, "detail": detail, "hint": hint,
            "ms": int((time.time() - t0) * 1000), "required": chk["required"]}


# ─────────────────────────────────────────────────────────────────────────────
#  Individual checks
# ─────────────────────────────────────────────────────────────────────────────
def _chk_databricks_auth():
    from databricks.sdk import WorkspaceClient
    ws = WorkspaceClient()
    me = ws.current_user.me()
    email = getattr(me, "user_name", "") or ""
    display = getattr(getattr(me, "display_name", None), "__str__", lambda: "")() or email
    return f"Authenticated as {display} ({email})", "pass", ""


def _chk_sql_warehouse():
    from dbsql_client import execute_query, get_catalog_schema
    try:
        rows = execute_query("SELECT 1 AS ok")
    except RuntimeError:
        return ("Databricks SQL env vars not set (DATABRICKS_SERVER_HOSTNAME / DATABRICKS_HTTP_PATH)",
                "fail", "Provided automatically via app.yml when deployed to Databricks Apps — for local runs, export them from Settings → Databricks.")
    cat, sch = get_catalog_schema()
    ok = bool(rows) and rows[0].get("ok") == 1
    if not ok:
        return "Warehouse query returned no result", "fail", "Check DATABRICKS_HTTP_PATH / warehouse ID configuration."
    return f"SQL Warehouse reachable — app catalog '{cat}.{sch}' in use", "pass", ""


def _chk_unity_catalog():
    from dbsql_client import execute_query, get_catalog_schema
    cfg = get_config()
    needed = {str(c).lower() for c in (cfg.get("catalogs") or {}).keys()}
    needed.add(os.environ.get("DATABRICKS_CATALOG", "admin_source").lower())
    try:
        rows = execute_query("SHOW CATALOGS")
    except RuntimeError:
        return ("Skipped — SQL Warehouse env vars not set locally",
                "warn", "Provided automatically via app.yml in Databricks Apps deployment.")
    present = {str(r.get("catalog", "")).lower() for r in rows}
    missing = sorted(needed - present - {"", "none"})
    if not needed & present:
        return ("No required catalogs found in the workspace",
                "fail", "Create the catalogs (or run Infra auto-creation) before migrating.")
    if missing:
        return (f"Core catalogs present; missing configured: {', '.join(missing)}",
                "warn", "Missing catalogs are only needed if those layers will be used — create via Infra or SQL.")
    return f"All {len(needed)} required catalogs present", "pass", ""


def _chk_secret_scope():
    from databricks.sdk import WorkspaceClient
    scope = os.environ.get("DATABRICKS_SECRET_SCOPE", "migration-studio")
    ws = WorkspaceClient()
    try:
        names = [s.name for s in ws.secrets.list_scopes()]
    except PermissionError:
        return (f"Scope '{scope}' could not be listed (permission) — app may still read it",
                "warn", "Grant the app SP READ on the secret scope, or pre-create secrets via CLI.")
    if scope in names:
        return f"Secret scope '{scope}' exists and is listable", "pass", ""
    return (f"Secret scope '{scope}' not found",
            "warn", "Create it (databricks secrets create-scope) or update DATABRICKS_SECRET_SCOPE — source passwords are stored there.")


def _chk_storage_access():
    cfg = get_config()
    account = (cfg.get("storage_account") or "").strip()
    container = (cfg.get("container") or "").strip()
    if not account:
        return ("Storage account not configured in Settings",
                "warn", "Set Storage Account + Container in Settings → Storage & Infrastructure.")
    from databricks.sdk import WorkspaceClient
    ws = WorkspaceClient()
    hits = []
    try:
        for sc in ws.storage_credentials.list():
            hits.append(f"credential:{sc.name}")
    except Exception:
        pass
    try:
        for el in ws.external_locations.list():
            url = getattr(el, "url", "") or ""
            if account.lower() in url.lower():
                return (f"External location '{getattr(el, 'name', '')}' covers {account} "
                        f"({len(hits)} storage credential(s) in workspace)", "pass", "")
    except Exception:
        pass
    if hits:
        return (f"{account} not covered by an external location yet "
                f"({len(hits)} storage credential(s) exist)",
                "warn", "Create the external location + volume (Infra auto-creation or Settings) so landing writes succeed.")
    return (f"No UC storage credentials / external locations visible for {account}",
            "warn", "Run Infra auto-creation or create an access connector + storage credential + external location.")


def _chk_azure_subscription():
    cfg = get_config()
    tenant = cfg.get("azure_tenant_id") or ""
    client = cfg.get("azure_client_id") or ""
    secret = cfg.get("azure_client_secret") or ""
    sub_id = cfg.get("subscription_id") or ""
    if not (tenant and client and secret):
        return ("Azure service principal not configured in Settings",
                "warn", "Optional — only needed for one-click Infra auto-creation (storage, access connector, catalogs).")
    from azure.identity import ClientSecretCredential
    cred = ClientSecretCredential(tenant_id=tenant, client_id=client, client_secret=secret)
    if sub_id:
        from azure.mgmt.resource import SubscriptionClient
        subs = SubscriptionClient(cred)
        for s in subs.subscriptions.list():
            if (s.subscription_id or "").lower() == sub_id.lower():
                return f"Azure subscription '{s.display_name}' accessible via service principal", "pass", ""
        return (f"Configured subscription {sub_id} not visible to this service principal",
                "fail", "Grant the SP 'Reader' on the subscription and retry.")
    count = sum(1 for _ in subs.subscriptions.list())
    return f"Azure credentials valid — {count} subscription(s) visible", "pass", ""


_REQUIRED_DEPS = [
    ("flask", True), ("databricks-sdk", True), ("databricks-sql-connector", True),
    ("requests", True), ("pyarrow", True), ("flask-compress", True),
]
_OPTIONAL_DEPS = [
    ("pyodbc", False), ("pymssql", False), ("snowflake-connector-python", False),
    ("redshift-connector", False), ("azure-identity", False),
    ("azure-storage-file-datalake", False), ("azure-mgmt-resource", False),
]


def _chk_dependencies():
    missing_req, missing_opt, ok = [], [], []
    for dist, req in _REQUIRED_DEPS + _OPTIONAL_DEPS:
        try:
            ver = importlib_metadata.version(dist)
            ok.append(f"{dist} {ver}")
        except importlib_metadata.PackageNotFoundError:
            # fall back to import-name probe for packages whose dist name differs
            mod = dist.replace("-", "_")
            try:
                importlib.import_module(mod)
                ok.append(dist)
            except Exception:
                (missing_req if req else missing_opt).append(dist)
    if missing_req:
        return (f"Missing required packages: {', '.join(missing_req)}",
                "fail", "pip install -r requirements.txt and restart the app.")
    if missing_opt:
        return (f"All required packages OK ({len(ok)}); optional not installed: {', '.join(missing_opt)}",
                "warn", "Optional packages enable extra source types (Snowflake/Redshift/SharePoint/Azure infra).")
    return f"All {len(ok)} application dependencies installed", "pass", ""


def _collect_checks():
    return [
        _mk("databricks_auth", "Databricks Workspace Access", "Access", _chk_databricks_auth),
        _mk("sql_warehouse", "SQL Warehouse Connectivity", "Access", _chk_sql_warehouse),
        _mk("unity_catalog", "Unity Catalog Catalogs", "Access", _chk_unity_catalog, required=False),
        _mk("secret_scope", "Secret Scope", "Access", _chk_secret_scope, required=False),
        _mk("storage", "Storage / Landing Zone", "Components", _chk_storage_access, required=False),
        _mk("azure_sub", "Azure Subscription Access", "Components", _chk_azure_subscription, required=False),
        _mk("dependencies", "Dependency Installation", "Dependencies", _chk_dependencies),
    ]


@preflight_bp.route("/preflight/run", methods=["GET"])
@login_required
def preflight_run():
    """Run all readiness checks. Always returns 200 with per-check results."""
    t0 = time.time()
    results = [_run_one(c) for c in _collect_checks()]
    summary = {
        "pass": sum(1 for r in results if r["status"] == "pass"),
        "warn": sum(1 for r in results if r["status"] == "warn"),
        "fail": sum(1 for r in results if r["status"] == "fail"),
        "total": len(results),
        "duration_ms": int((time.time() - t0) * 1000),
        "ready": all(r["status"] != "fail" for r in results if r["required"]),
    }
    return jsonify({"success": True, "checks": results, "summary": summary})
