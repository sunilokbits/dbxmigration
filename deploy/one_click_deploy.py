#!/usr/bin/env python3
"""
One-click deploy — SQL to Databricks Migration Studio
=======================================================
Deploys the full stack (App + Jobs + Unity Catalog infra + secrets + Genie
Space + app tables) into ANY client's Databricks workspace from a single
command, with pre-flight and post-deploy validation.

Design goals (see deploy/README.md for full usage):
  - Runs from anywhere: only needs Python 3.9+ and this repo checked out.
    All Python dependencies are the ones already in requirements.txt.
  - No secrets are ever read from or written to a config file on disk.
    Secrets come from environment variables (or an interactive masked
    prompt as a fallback) and go straight into the Databricks secret scope.
  - Idempotent: safe to re-run after a partial failure: every step either
    creates-if-missing or updates in place.
  - Every step is validated both before (pre-flight) and after (post-deploy)
    it runs; a machine-readable JSON report + human summary is produced,
    and the process exits non-zero if any REQUIRED step failed.

Usage:
    python deploy/one_click_deploy.py --client-config deploy/clients/acme.json
    python deploy/one_click_deploy.py --client-config deploy/clients/acme.json --yes --skip-infra
"""
from __future__ import annotations

import argparse
import getpass
import json
import os
import secrets as _secrets
import shutil
import subprocess
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MIGRATION_UTILITY_DIR = REPO_ROOT / "migration_utility"
sys.path.insert(0, str(MIGRATION_UTILITY_DIR))

MIN_CLI_VERSION = (0, 205, 0)

# Secret values are sourced ONLY from environment variables / interactive
# prompt — never persisted in the client config JSON or written to disk.
SECRET_ENV_VARS = {
    "databricks_token": {"env": "DATABRICKS_TOKEN", "required": True,
                          "label": "Databricks personal access token for the TARGET workspace"},
    "source_password": {"env": "DBX_SOURCE_PASSWORD", "required": False,
                         "label": "Source database password"},
    "devops_pat": {"env": "DBX_DEVOPS_PAT", "required": False,
                   "label": "Azure DevOps PAT (optional)"},
    "azure_client_secret": {"env": "AZURE_CLIENT_SECRET", "required": False,
                             "label": "Azure Service Principal secret (optional — omit to use browser/CLI login)"},
}


class Report:
    """Collects per-step results for the final JSON + console summary."""

    def __init__(self, client_name: str, on_step=None):
        self.client_name = client_name
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.steps: list[dict] = []
        self.on_step = on_step  # optional callback(step_dict) — used by deploy_app.py for live progress

    def add(self, phase: str, name: str, status: str, message: str = "", required: bool = True):
        step = {
            "phase": phase, "name": name, "status": status,
            "message": message, "required": required,
        }
        self.steps.append(step)
        icon = {"pass": "[OK]", "fail": "[FAIL]", "warn": "[WARN]", "skip": "[SKIP]"}.get(status, "[?]")
        print(f"  {icon} {name}{': ' + message if message else ''}")
        if self.on_step:
            try:
                self.on_step(step)
            except Exception:
                pass

    @property
    def hard_failures(self) -> list[dict]:
        return [s for s in self.steps if s["status"] == "fail" and s["required"]]

    def finish_and_write(self, out_dir: Path) -> Path:
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = out_dir / f"{self.client_name}-{ts}.json"
        payload = {
            "client_name": self.client_name,
            "started_at": self.started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "success": not self.hard_failures,
            "steps": self.steps,
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path


def _phase_header(title: str):
    print(f"\n=== {title} ===")


def _run(cmd: list[str], env: dict | None = None, timeout: int = 900) -> tuple[int, str, str]:
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=timeout)
    return proc.returncode, proc.stdout, proc.stderr


def _mask(value: str) -> str:
    if not value:
        return ""
    return value[:3] + "…" + value[-2:] if len(value) > 6 else "••••"


# ═══════════════════════════════════════════════════════════════════════════
# 1. Client config + secrets loading
# ═══════════════════════════════════════════════════════════════════════════

def load_client_config(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(
            f"Client config not found: {path}\n"
            f"Copy deploy/client.template.json to {path} and fill in your client's values."
        )
    cfg = json.loads(path.read_text(encoding="utf-8"))
    required_top = ["client_name", "databricks_host", "catalog_admin", "app_schema"]
    missing = [k for k in required_top if not cfg.get(k)]
    if missing:
        raise SystemExit(f"Client config {path} is missing required field(s): {', '.join(missing)}")
    cfg.setdefault("bundle_target", "client")
    cfg.setdefault("cloud_provider", "azure")
    cfg.setdefault("with_infra", True)
    return cfg


def collect_secrets(cfg: dict, non_interactive: bool) -> dict:
    """Resolve secrets strictly from env vars, falling back to a masked
    interactive prompt. Never logged, never written to disk."""
    secrets_out = {}
    needs_source_pw = bool((cfg.get("source") or {}).get("server"))
    needs_azure_sp = bool((cfg.get("azure") or {}).get("subscription_id")) and os.environ.get("AZURE_CLIENT_ID")

    for key, spec in SECRET_ENV_VARS.items():
        required = spec["required"] or (key == "source_password" and needs_source_pw) \
            or (key == "azure_client_secret" and needs_azure_sp)
        val = os.environ.get(spec["env"], "")
        if not val and required and not non_interactive and sys.stdin.isatty():
            val = getpass.getpass(f"Enter {spec['label']} ({spec['env']} not set): ")
        if not val and required:
            raise SystemExit(
                f"Missing required secret: set env var {spec['env']} ({spec['label']}) "
                f"before running (or run interactively without --yes)."
            )
        secrets_out[key] = val
    return secrets_out


# ═══════════════════════════════════════════════════════════════════════════
# 2. Pre-flight checks
# ═══════════════════════════════════════════════════════════════════════════

def preflight(cfg: dict, secrets_in: dict, report: Report, auto_yes: bool):
    _phase_header("Pre-flight checks")

    if sys.version_info < (3, 9):
        report.add("preflight", "Python version", "fail", f"Python {sys.version.split()[0]} — need 3.9+")
    else:
        report.add("preflight", "Python version", "pass", sys.version.split()[0])

    for mod in ("databricks.sdk", "databricks.sql"):
        try:
            __import__(mod)
            report.add("preflight", f"Package '{mod}'", "pass")
        except ImportError:
            report.add("preflight", f"Package '{mod}'", "fail",
                        "run: pip install -r requirements.txt")

    if cfg.get("with_infra"):
        try:
            __import__("azure.identity")
            __import__("azure.mgmt.storage")
            report.add("preflight", "Azure SDK packages", "pass")
        except ImportError:
            report.add("preflight", "Azure SDK packages", "fail",
                        "run: pip install -r requirements.txt")

    ensure_databricks_cli(report, auto_yes)

    # Databricks workspace reachability + token validity
    host = cfg["databricks_host"].rstrip("/")
    token = secrets_in["databricks_token"]
    try:
        req = urllib.request.Request(
            f"{host}/api/2.0/preview/scim/v2/Me",
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read())
        report.add("preflight", "Databricks workspace auth", "pass",
                    f"authenticated as {body.get('userName', body.get('emails', ['?'])[0] if body.get('emails') else '?')}")
    except urllib.error.HTTPError as e:
        report.add("preflight", "Databricks workspace auth", "fail",
                    f"HTTP {e.code} — check DATABRICKS_TOKEN and databricks_host")
    except Exception as e:
        report.add("preflight", "Databricks workspace auth", "fail", str(e)[:200])

    if cfg.get("with_infra"):
        try:
            import AutoInfraCreation as infra
            azure_cfg = cfg.get("azure", {})
            cred = infra._get_azure_credential(azure_cfg)
            cred.get_token("https://management.azure.com/.default")
            report.add("preflight", "Azure credential", "pass")
        except Exception as e:
            report.add("preflight", "Azure credential", "fail", str(e)[:200])

    # NOTE: callers decide what to do with hard failures (CLI exits, the web
    # UI just stops the pipeline and reports it) — see run_pipeline() below.


def ensure_databricks_cli(report: Report, auto_yes: bool):
    path = shutil.which("databricks")
    if path:
        try:
            rc, out, _ = _run([path, "-v"])
            report.add("preflight", "Databricks CLI", "pass", out.strip() or "installed")
            return
        except Exception:
            pass

    report.add("preflight", "Databricks CLI", "warn", "not found on PATH")
    if os.name == "nt":
        install_cmd = ["winget", "install", "Databricks.DatabricksCLI"]
        install_str = "winget install Databricks.DatabricksCLI"
    elif shutil.which("brew"):
        install_cmd = None  # two-step, run via shell below
        install_str = "brew tap databricks/tap && brew install databricks"
    else:
        install_cmd = None
        install_str = "curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"

    if not auto_yes:
        answer = input(f"Install the Databricks CLI now via:\n    {install_str}\nProceed? [y/N]: ").strip().lower()
        if answer != "y":
            report.add("preflight", "Databricks CLI", "fail",
                        f"Install manually: {install_str}, then re-run.")
            return
    print(f"Installing Databricks CLI: {install_str}")
    try:
        if install_cmd:
            subprocess.run(install_cmd, check=True)
        else:
            subprocess.run(install_str, shell=True, check=True)
        path = shutil.which("databricks")
        if path:
            report.add("preflight", "Databricks CLI", "pass", "installed successfully")
        else:
            report.add("preflight", "Databricks CLI", "fail",
                        "install command finished but 'databricks' still not on PATH — open a new terminal and re-run")
    except subprocess.CalledProcessError as e:
        report.add("preflight", "Databricks CLI", "fail", f"install failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# 3. Unity Catalog / Azure infrastructure
# ═══════════════════════════════════════════════════════════════════════════

def run_infra(cfg: dict, secrets_in: dict, report: Report):
    _phase_header("Unity Catalog & Azure infrastructure")
    if not cfg.get("with_infra"):
        report.add("infra", "Infrastructure provisioning", "skip", "with_infra=false in client config")
        return

    import AutoInfraCreation as infra

    azure_cfg = dict(cfg.get("azure", {}))
    azure_cfg["databricks_host"] = cfg["databricks_host"]
    azure_cfg["databricks_token"] = secrets_in["databricks_token"]
    if secrets_in.get("azure_client_secret"):
        azure_cfg["azure_client_secret"] = secrets_in["azure_client_secret"]
        azure_cfg["azure_client_id"] = os.environ.get("AZURE_CLIENT_ID", "")
        azure_cfg["azure_tenant_id"] = os.environ.get("AZURE_TENANT_ID", "")

    required_infra_keys = ["subscription_id", "resource_group", "region", "storage_account", "access_connector"]
    missing = [k for k in required_infra_keys if not azure_cfg.get(k)]
    if missing:
        report.add("infra", "Infrastructure provisioning", "skip",
                    f"azure.{{{', '.join(missing)}}} not set in client config — skipping infra step")
        return

    result = infra.run_all_api(azure_cfg)
    for step in result.get("steps", []):
        status = {"success": "pass", "error": "fail", "skipped": "skip"}.get(step["status"], "warn")
        # Azure RBAC propagation delays are expected/non-fatal — treat as warn, not hard fail
        required = status == "fail" and step["name"] not in (
            "Register Storage Credential in Unity Catalog", "Create External Locations", "Create Volume",
        )
        report.add("infra", step["name"], status, step["message"][:300], required=required)


# ═══════════════════════════════════════════════════════════════════════════
# 4. Secret scope + secrets
# ═══════════════════════════════════════════════════════════════════════════

def push_secrets(cfg: dict, secrets_in: dict, flask_secret: str, report: Report):
    _phase_header("Secret scope & secrets")
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(host=cfg["databricks_host"], token=secrets_in["databricks_token"])
    scope = "migration-studio"

    try:
        existing = [s.name for s in w.secrets.list_scopes()]
        if scope not in existing:
            w.secrets.create_scope(scope=scope)
            report.add("secrets", f"Secret scope '{scope}'", "pass", "created")
        else:
            report.add("secrets", f"Secret scope '{scope}'", "pass", "already exists")
    except Exception as e:
        report.add("secrets", f"Secret scope '{scope}'", "fail", str(e)[:200])
        return

    # Grant the App SP READ on the scope so it can fetch secrets at runtime
    try:
        app_name = cfg.get("app_name", "dbxmigration")
        apps = list(w.apps.list())
        app = next((a for a in apps if a.name == app_name), None)
        if app and app.service_principal_client_id:
            sp_id = app.service_principal_client_id
            w.secrets.put_acl(scope=scope, principal=sp_id, permission="READ")
            report.add("secrets", "App SP secret-scope ACL", "pass", f"READ granted to {sp_id}")
    except Exception as e:
        report.add("secrets", "App SP secret-scope ACL", "warn", str(e)[:200], required=False)

    to_store = {"flask-secret-key": flask_secret}
    src_type = (cfg.get("source") or {}).get("source_type", "sqlserver")
    key_by_source = {
        "sqlserver": "source-sql-password", "azuresql": "source-azuresql-password",
        "snowflake": "source-snowflake-password", "bigquery": "source-bigquery-password",
        "redshift": "source-redshift-password", "synapse": "source-synapse-password",
    }
    if secrets_in.get("source_password"):
        to_store[key_by_source.get(src_type, "source-sql-password")] = secrets_in["source_password"]
    if secrets_in.get("devops_pat"):
        to_store["devops-pat"] = secrets_in["devops_pat"]
    to_store["databricks-token"] = secrets_in["databricks_token"]

    for key, value in to_store.items():
        try:
            w.secrets.put_secret(scope=scope, key=key, string_value=value)
            report.add("secrets", f"Secret '{key}'", "pass", "stored")
        except Exception as e:
            report.add("secrets", f"Secret '{key}'", "fail", str(e)[:200])


# ═══════════════════════════════════════════════════════════════════════════
# 4b. SQL Warehouse resolution — needed by the app's env vars, Genie Space
#     creation, and app-tables init; auto-discovered so no manual step is
#     required when a client config omits sql_warehouse_id.
# ═══════════════════════════════════════════════════════════════════════════

def resolve_sql_warehouse_id(cfg: dict, secrets_in: dict, report: Report):
    if cfg.get("sql_warehouse_id"):
        report.add("warehouse", "SQL Warehouse ID", "pass",
                    f"using configured id {cfg['sql_warehouse_id']}", required=False)
        return

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(host=cfg["databricks_host"], token=secrets_in["databricks_token"])
    try:
        warehouses = list(w.warehouses.list())
        running = next((wh for wh in warehouses
                         if "RUNNING" in str(getattr(wh.state, "value", wh.state)).upper()), None)
        chosen = running or (warehouses[0] if warehouses else None)
        if chosen:
            cfg["sql_warehouse_id"] = chosen.id
            report.add("warehouse", "SQL Warehouse auto-discover", "pass",
                        f"using '{chosen.name}' ({chosen.id})", required=False)
        else:
            report.add("warehouse", "SQL Warehouse auto-discover", "warn",
                        "No SQL warehouse found in workspace — create one, then re-run", required=False)
    except Exception as e:
        report.add("warehouse", "SQL Warehouse auto-discover", "warn", str(e)[:200], required=False)


# ═══════════════════════════════════════════════════════════════════════════
# 5. Bundle deploy (App + Jobs)
# ═══════════════════════════════════════════════════════════════════════════

def bundle_deploy(cfg: dict, secrets_in: dict, flask_secret: str, genie_space_id: str, report: Report):
    _phase_header("Deploy App + Jobs (databricks bundle)")

    env = os.environ.copy()
    env["DATABRICKS_HOST"] = cfg["databricks_host"]
    env["DATABRICKS_TOKEN"] = secrets_in["databricks_token"]

    target = cfg.get("bundle_target", "client")
    var_map = {
        "client_name": cfg["client_name"],
        "cloud_provider": cfg.get("cloud_provider", "azure"),
        "catalog_admin": cfg.get("catalog_admin", "admin_source"),
        "app_schema": cfg.get("app_schema", "migration_app"),
        "catalog_bronze": cfg.get("catalog_bronze", "bronze"),
        "catalog_silver": cfg.get("catalog_silver", "silver"),
        "catalog_volumes": cfg.get("catalog_volumes", "dev_volumes"),
        "catalog_recon": cfg.get("catalog_recon", "reconciliation"),
        "catalog_logging": cfg.get("catalog_logging", "loggingdetails"),
        "default_schema": cfg.get("default_schema", "hr"),
        "notebook_root": cfg.get("notebook_root", "/Shared/MetadataPipeline"),
        "landing_path": cfg.get("landing_path", "/Volumes/dev_volumes/hr/landing"),
        "sql_warehouse_id": cfg.get("sql_warehouse_id", ""),
        "storage_account": (cfg.get("azure") or {}).get("storage_account", ""),
        "container": (cfg.get("azure") or {}).get("container", "datalake"),
        "flask_secret_key": flask_secret,
        "genie_space_id": genie_space_id,
    }
    var_args = []
    for k, v in var_map.items():
        var_args += ["--var", f"{k}={v}"]

    rc, out, err = _run(["databricks", "bundle", "validate", "-t", target] + var_args, env=env)
    if rc != 0:
        report.add("bundle", "bundle validate", "fail", (err or out)[:400])
        return
    report.add("bundle", "bundle validate", "pass")

    rc, out, err = _run(["databricks", "bundle", "deploy", "-t", target] + var_args, env=env, timeout=1800)
    if rc != 0:
        report.add("bundle", "bundle deploy", "fail", (err or out)[:400])
        return
    report.add("bundle", "bundle deploy", "pass", "App + Jobs deployed")

    # Trigger the App to (re)start from the freshly deployed source
    rc, out, err = _run(["databricks", "bundle", "run", "-t", target, "migration_studio"], env=env, timeout=600)
    if rc == 0:
        report.add("bundle", "App start/redeploy", "pass")
    else:
        report.add("bundle", "App start/redeploy", "warn",
                    f"Non-fatal — bundle deploy already starts new Apps; check Apps UI if not RUNNING. {(err or out)[:250]}",
                    required=False)


# ═══════════════════════════════════════════════════════════════════════════
# 6. App tables (Delta) init
# ═══════════════════════════════════════════════════════════════════════════

def init_app_tables(cfg: dict, secrets_in: dict, report: Report):
    _phase_header("App persistence tables")
    sql_path = REPO_ROOT / "src" / "sql" / "init_app_tables.sql"
    if not sql_path.exists():
        report.add("tables", "init_app_tables.sql", "fail", "file not found")
        return
    if not cfg.get("sql_warehouse_id"):
        report.add("tables", "App tables init", "skip", "sql_warehouse_id not set in client config")
        return

    catalog = cfg.get("catalog_admin", "admin_source")
    schema = cfg.get("app_schema", "migration_app")
    raw_sql = sql_path.read_text(encoding="utf-8")
    raw_sql = raw_sql.replace("${catalog}", catalog).replace("${schema}", schema)
    statements = [s.strip() for s in raw_sql.split(";") if s.strip() and not s.strip().startswith("--")]

    try:
        from databricks import sql as dbsql
        host = cfg["databricks_host"].replace("https://", "").rstrip("/")
        http_path = f"/sql/1.0/warehouses/{cfg['sql_warehouse_id']}"
        with dbsql.connect(server_hostname=host, http_path=http_path, access_token=secrets_in["databricks_token"]) as conn:
            with conn.cursor() as cur:
                for stmt in statements:
                    cur.execute(stmt)
        report.add("tables", "App tables init", "pass", f"{len(statements)} statement(s) executed")
    except Exception as e:
        report.add("tables", "App tables init", "fail", str(e)[:300])


# ═══════════════════════════════════════════════════════════════════════════
# 7. Genie Space resolution
# ═══════════════════════════════════════════════════════════════════════════

def resolve_genie_space(cfg: dict, secrets_in: dict, report: Report, auto_yes: bool) -> str:
    _phase_header("Genie Space")
    existing = (cfg.get("genie_space_id") or "").strip()
    if existing:
        report.add("genie", "Genie Space ID", "pass", f"using configured id {existing}")
        return existing

    if not cfg.get("sql_warehouse_id"):
        report.add("genie", "Genie Space auto-create", "skip",
                    "sql_warehouse_id not set — auto-create needs a warehouse to attach", required=False)
    else:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient(host=cfg["databricks_host"], token=secrets_in["databricks_token"])
        try:
            # Best-effort — Genie Space programmatic creation availability varies
            # by workspace/region. If unsupported, we fall back to a manual step.
            resp = w.api_client.do("POST", "/api/2.0/genie/spaces", body={
                "title": f"{cfg['client_name']} — Migration Studio",
                "description": "Auto-created by one_click_deploy.py",
                "warehouse_id": cfg.get("sql_warehouse_id", ""),
            })
            space_id = resp.get("space_id", "") if isinstance(resp, dict) else ""
            if space_id:
                report.add("genie", "Genie Space", "pass", f"created {space_id}")
                return space_id
            raise RuntimeError("no space_id in response")
        except Exception as e:
            report.add("genie", "Genie Space auto-create", "warn",
                        f"not supported in this workspace ({str(e)[:120]}) — manual step required", required=False)

    if auto_yes or not sys.stdin.isatty():
        report.add("genie", "Genie Space", "warn",
                    "Not configured — create one manually in the Databricks UI (Genie > New Space), "
                    "then re-run with genie_space_id set in the client config.", required=False)
        return ""

    print("\n  Create a Genie Space manually (2 minutes): Databricks workspace -> Genie -> New Space")
    print("  Point it at the catalogs/schemas from this deployment, then paste its Space ID below.")
    space_id = input("  Genie Space ID (leave blank to skip for now): ").strip()
    if space_id:
        report.add("genie", "Genie Space ID", "pass", f"using manually created id {space_id}")
    else:
        report.add("genie", "Genie Space ID", "warn", "skipped — Genie chat disabled until configured", required=False)
    return space_id


# ═══════════════════════════════════════════════════════════════════════════
# 8. Post-deploy validation
# ═══════════════════════════════════════════════════════════════════════════

def validate_deployment(cfg: dict, secrets_in: dict, genie_space_id: str, report: Report):
    _phase_header("Post-deploy validation")
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(host=cfg["databricks_host"], token=secrets_in["databricks_token"])

    catalog = cfg.get("catalog_admin", "admin_source")
    schema = cfg.get("app_schema", "migration_app")
    try:
        w.catalogs.get(catalog)
        report.add("validate", f"Catalog '{catalog}' reachable", "pass")
    except Exception as e:
        report.add("validate", f"Catalog '{catalog}' reachable", "fail", str(e)[:200])

    try:
        w.schemas.get(full_name=f"{catalog}.{schema}")
        report.add("validate", f"Schema '{catalog}.{schema}' reachable", "pass")
    except Exception as e:
        report.add("validate", f"Schema '{catalog}.{schema}' reachable", "warn", str(e)[:200], required=False)

    try:
        keys = [s.key for s in w.secrets.list_secrets(scope="migration-studio")]
        report.add("validate", "Secret scope populated", "pass" if keys else "warn",
                    f"{len(keys)} secret(s)", required=bool(keys))
    except Exception as e:
        report.add("validate", "Secret scope populated", "fail", str(e)[:200])

    if cfg.get("sql_warehouse_id"):
        try:
            wh = w.warehouses.get(cfg["sql_warehouse_id"])
            report.add("validate", "SQL Warehouse", "pass", f"state={wh.state.value}")
        except Exception as e:
            report.add("validate", "SQL Warehouse", "fail", str(e)[:200])

    try:
        apps = list(w.apps.list())
        app = next((a for a in apps if a.name == "dbxmigration"), None)
        if app:
            state = getattr(getattr(app, "compute_status", None), "state", None) or getattr(app, "app_status", None)
            # Databricks Apps compute state uses ACTIVE (not RUNNING) when healthy;
            # app_status (if present) uses RUNNING — accept either spelling.
            healthy = any(k in str(state).upper() for k in ("ACTIVE", "RUNNING"))
            report.add("validate", "Databricks App status", "pass" if healthy else "warn",
                        f"state={state}", required=False)
        else:
            report.add("validate", "Databricks App status", "warn", "app not found via API yet — check Apps UI",
                        required=False)
    except Exception as e:
        report.add("validate", "Databricks App status", "warn", str(e)[:200], required=False)

    if genie_space_id:
        try:
            resp = w.api_client.do("GET", f"/api/2.0/genie/spaces/{genie_space_id}")
            report.add("validate", "Genie Space reachable", "pass" if resp else "warn", required=False)
        except Exception as e:
            report.add("validate", "Genie Space reachable", "warn", str(e)[:200], required=False)
    else:
        report.add("validate", "Genie Space reachable", "skip", "not configured", required=False)

    # App health endpoint (only meaningful once the App URL is known/public)
    app_url = (cfg.get("app_url") or "").strip()
    if app_url:
        try:
            with urllib.request.urlopen(f"{app_url.rstrip('/')}/health", timeout=15) as resp:
                report.add("validate", "App /health endpoint", "pass" if resp.status == 200 else "warn",
                            f"HTTP {resp.status}", required=False)
        except Exception as e:
            report.add("validate", "App /health endpoint", "warn", str(e)[:200], required=False)


# ═══════════════════════════════════════════════════════════════════════════
# Reusable pipeline — shared by the CLI (main, below) and deploy_app.py
# ═══════════════════════════════════════════════════════════════════════════

def run_pipeline(cfg: dict, secrets_in: dict, *, skip_infra: bool = False, skip_genie: bool = False,
                  non_interactive: bool = False, on_step=None) -> Report:
    """Run every deploy phase in order, stopping early if pre-flight hard-fails.
    Returns the Report — never calls sys.exit, safe to call from a thread."""
    if skip_infra:
        cfg["with_infra"] = False
    report = Report(cfg["client_name"], on_step=on_step)
    report.add("start", "Deployment target", "pass",
               f"{cfg['databricks_host']} (target={cfg.get('bundle_target', 'client')})", required=False)

    preflight(cfg, secrets_in, report, non_interactive)
    if report.hard_failures:
        return report

    resolve_sql_warehouse_id(cfg, secrets_in, report)
    genie_space_id = "" if skip_genie else resolve_genie_space(cfg, secrets_in, report, non_interactive)
    run_infra(cfg, secrets_in, report)

    flask_secret = _secrets.token_urlsafe(48)
    push_secrets(cfg, secrets_in, flask_secret, report)
    bundle_deploy(cfg, secrets_in, flask_secret, genie_space_id, report)
    init_app_tables(cfg, secrets_in, report)
    validate_deployment(cfg, secrets_in, genie_space_id, report)
    return report


# ═══════════════════════════════════════════════════════════════════════════
# main (CLI entry point)
# ═══════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description="One-click deploy DBX Migration Studio to a client workspace.")
    ap.add_argument("--client-config", required=True, help="Path to a per-client JSON config (see client.template.json)")
    ap.add_argument("--skip-infra", action="store_true", help="Skip Azure/Unity Catalog infrastructure provisioning")
    ap.add_argument("--skip-genie", action="store_true", help="Skip Genie Space resolution/creation")
    ap.add_argument("--yes", action="store_true", help="Non-interactive mode: never prompt, fail fast on missing input")
    args = ap.parse_args()

    cfg = load_client_config(Path(args.client_config))
    print(f"Deploying '{cfg['client_name']}' -> {cfg['databricks_host']} (target={cfg.get('bundle_target', 'client')})")

    secrets_in = collect_secrets(cfg, args.yes)
    report = run_pipeline(cfg, secrets_in, skip_infra=args.skip_infra, skip_genie=args.skip_genie,
                           non_interactive=args.yes)

    report_path = report.finish_and_write(REPO_ROOT / "deploy" / "reports")
    n_fail = len(report.hard_failures)
    n_warn = len([s for s in report.steps if s["status"] == "warn"])
    print(f"\n=== Summary ===\n{len(report.steps)} step(s), {n_fail} failed, {n_warn} warning(s)")
    print(f"Report written to {report_path}")

    if n_fail:
        print("\nDEPLOYMENT INCOMPLETE — see failed steps above / in the report.")
        sys.exit(1)
    print("\nDEPLOYMENT SUCCESSFUL.")


if __name__ == "__main__":
    main()
