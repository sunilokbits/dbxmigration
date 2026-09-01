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
    from dbsql_client import execute_query, get_catalog_schema, _auto_discover_warehouse_id
    wh_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
    discovered = False
    if not wh_id:
        wh_id = _auto_discover_warehouse_id()
        discovered = True
    if not wh_id and not os.environ.get("DATABRICKS_HTTP_PATH"):
        return ("No SQL warehouse found — set DATABRICKS_SQL_WAREHOUSE_ID or create a SQL warehouse in the workspace",
                "fail", "Go to SQL Warehouses in Databricks, create/start one, then re-run.")
    try:
        rows = execute_query("SELECT 1 AS ok")
    except Exception as e:
        msg = str(e)[:200]
        if "STOPPED" in msg.upper() or "START" in msg.upper():
            return (f"SQL warehouse '{wh_id}' is stopped", "fail", "Start the warehouse in Databricks SQL Warehouses, then re-run.")
        return (f"SQL warehouse connection failed: {msg}", "fail", "Check warehouse status and permissions.")
    cat, sch = get_catalog_schema()
    ok = bool(rows) and rows[0].get("ok") == 1
    if not ok:
        return "Warehouse query returned no result", "fail", "Check warehouse configuration."
    src = "auto-discovered" if discovered else "configured"
    return f"SQL Warehouse reachable ({src}: {wh_id}) — catalog '{cat}.{sch}' in use", "pass", ""


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


def _chk_genie_space():
    """Check whether a Genie Space is configured and reachable."""
    space_id = os.environ.get("GENIE_SPACE_ID", "").strip()
    try:
        from config_cache import get_config as _gc
        if not space_id:
            space_id = (_gc().get("genie_space_id") or "").strip()
    except Exception:
        pass
    # Try live discovery from workspace
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    if host and not host.startswith("http"):
        host = "https://" + host
    spaces_found = []
    if host:
        try:
            from secrets_helper import get_databricks_token
            token = get_databricks_token()
            import requests as _req
            r = _req.get(f"{host}/api/2.0/genie/spaces",
                         headers={"Authorization": f"Bearer {token}"}, timeout=10)
            if r.status_code == 200:
                spaces_found = r.json().get("spaces", [])
        except Exception:
            pass
    if spaces_found:
        titles = ", ".join(s.get("title", s.get("space_id", "?"))[:40] for s in spaces_found[:3])
        count = len(spaces_found)
        if space_id:
            match = next((s for s in spaces_found if s.get("space_id") == space_id), None)
            if match:
                return (f"Genie Space configured: '{match.get('title', space_id)}' ({space_id[:12]}…)",
                        "pass", "")
            return (f"Configured Genie Space ID '{space_id[:12]}…' not found among {count} space(s): {titles}",
                    "warn", "Verify the GENIE_SPACE_ID env var or pick a valid space from the Genie AI panel.")
        return (f"{count} Genie Space(s) available in workspace: {titles}",
                "pass", "")
    if space_id:
        return (f"Genie Space ID configured ({space_id[:12]}…) but could not verify via API",
                "warn", "The space may still work — open the Genie AI panel to test.")
    return ("No Genie Space found — Genie AI chat will not be available",
            "warn", "Create a Genie Space in Databricks (Genie → New Space), or re-deploy via the CI/CD pipeline which auto-creates one.")


def _collect_checks():
    return [
        _mk("databricks_auth", "Databricks Workspace Access", "Access", _chk_databricks_auth),
        _mk("sql_warehouse", "SQL Warehouse Connectivity", "Access", _chk_sql_warehouse),
        _mk("unity_catalog", "Unity Catalog Catalogs", "Access", _chk_unity_catalog, required=False),
        _mk("secret_scope", "Secret Scope", "Access", _chk_secret_scope, required=False),
        _mk("genie_space", "Genie AI Space", "Components", _chk_genie_space, required=False),
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
