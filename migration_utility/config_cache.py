"""Centralized deploy-config cache — Delta table + Databricks Secrets.

Replaces the file-based deployconfig.json with a Delta table in Unity Catalog,
and replaces Azure Key Vault with Databricks Secrets.  The public API
(get_config, save_config, reload_config, get_source_password, etc.) is
unchanged so all 15+ importing modules continue to work.
"""
import json
import os
import threading
from flask import g
from log_config import get_logger

logger = get_logger(__name__)

_lock = threading.Lock()
_cache: dict | None = None


def _fqn() -> str:
    catalog = os.environ.get("DATABRICKS_CATALOG", "admin_source")
    schema = os.environ.get("DATABRICKS_SCHEMA", "migration_app")
    return f"{catalog}.{schema}.app_config"


def get_config() -> dict:
    """Return the cached config dict (or load from Delta on first call)."""
    global _cache
    if _cache is not None:
        return _cache
    return reload_config()


def reload_config() -> dict:
    """Force re-read from Delta table and update the cache."""
    global _cache
    with _lock:
        cfg: dict = {}

        # Layer 1: Environment variables (static, from app.yml)
        for key in ("DATABRICKS_HOST", "DATABRICKS_CATALOG", "DATABRICKS_SCHEMA",
                     "DATABRICKS_SQL_WAREHOUSE_ID", "CLOUD_PROVIDER"):
            env_val = os.environ.get(key)
            if env_val:
                cfg[key.lower()] = env_val

        # Layer 2: Delta table (mutable runtime config)
        try:
            from dbsql_client import execute_query
            rows = execute_query(f"SELECT config_key, config_value FROM {_fqn()}")
            for row in rows:
                try:
                    cfg[row["config_key"]] = json.loads(row["config_value"])
                except (json.JSONDecodeError, TypeError):
                    cfg[row["config_key"]] = row["config_value"]
        except Exception as exc:
            logger.warning("Could not load config from Delta: %s", exc)

        # Layer 3: Always fall back to deployconfig.json for fields not yet in Delta.
        # This covers: empty table on first run, migration from file-based config,
        # and partial Delta records. Legacy values are only used where Delta has no entry.
        if not cfg.get("source") or not cfg.get("subscription_id"):
            legacy = _load_legacy_config()
            if legacy:
                merged = dict(legacy)   # start with legacy as base
                merged.update(cfg)      # Delta / env values win on conflict
                cfg = merged
                logger.info("Config hydrated from deployconfig.json (%d keys)", len(legacy))

        _cache = cfg
    return _cache


def _load_legacy_config() -> dict:
    """Load from deployconfig.json if it exists (migration fallback)."""
    base = os.path.dirname(os.path.abspath(__file__))
    for path in [os.path.join(base, "deployconfig.json"),
                 "/home/migration_data/deployconfig.json"]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            continue
    return {}


def save_config(cfg: dict) -> None:
    """Write config to Delta table AND update the in-memory cache."""
    global _cache
    with _lock:
        user = "system"
        try:
            if hasattr(g, "user") and g.user:
                user = g.user.get("email", "system")
        except RuntimeError:
            pass

        try:
            if not cfg:
                raise ValueError("empty config — nothing to save")
            from dbsql_client import execute_write
            fqn = _fqn()
            # Single MERGE for all keys at once — one Databricks SQL Warehouse
            # round-trip instead of one per key (was N sequential statement
            # executions, each with its own poll-until-complete latency, making
            # "Save Config" take many seconds for a config with 15-20+ keys).
            params = {"user": user}
            values_rows = []
            for i, (key, value) in enumerate(cfg.items()):
                val_str = json.dumps(value, default=str) if not isinstance(value, str) else value
                params[f"k{i}"] = key
                params[f"v{i}"] = val_str
                values_rows.append(f"(%(k{i})s, %(v{i})s)")
            execute_write(
                f"""MERGE INTO {fqn} AS t
                    USING (VALUES {", ".join(values_rows)}) AS s(config_key, config_value)
                    ON t.config_key = s.config_key
                    WHEN MATCHED THEN UPDATE SET config_value = s.config_value, updated_by = %(user)s, updated_at = current_timestamp()
                    WHEN NOT MATCHED THEN INSERT (config_key, config_value, updated_by, updated_at) VALUES (s.config_key, s.config_value, %(user)s, current_timestamp())""",
                params,
            )
        except Exception as exc:
            logger.warning("Could not save config to Delta: %s", exc)
            # Fallback: write to deployconfig.json
            _save_legacy_config(cfg)

        _cache = cfg


def _save_legacy_config(cfg: dict) -> None:
    """Fallback: write to deployconfig.json."""
    base = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base, "deployconfig.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
    except Exception:
        pass


# ── Secret accessors (delegate to secrets_helper) ────────────────────────────

def get_source_password(source_type: str = "") -> str:
    """Return the source password, choosing the right secret key based on source_type."""
    # Determine source type from config if not provided
    if not source_type:
        cfg = get_config()
        source_type = cfg.get("source", {}).get("source_type", "sqlserver") if isinstance(cfg.get("source"), dict) else "sqlserver"
    from secrets_helper import get_source_password as _sp, is_masked
    pw = _sp(source_type=source_type)
    if pw and not is_masked(pw):
        logger.info("Source password resolved from secrets for %s (%d chars)", source_type, len(pw))
        return pw
    cfg = get_config()
    val = cfg.get("source", {}).get("password", "") if isinstance(cfg.get("source"), dict) else ""
    if is_masked(val):
        logger.warning("Source password is masked in config and not found in secrets")
        return ""
    return val


def get_databricks_token() -> str:
    from secrets_helper import get_databricks_token as _dt
    tok = _dt()
    if tok:
        return tok
    cfg = get_config()
    val = cfg.get("databricks_token", "")
    from secrets_helper import is_masked
    return "" if is_masked(val) else val


def get_devops_token() -> str:
    from secrets_helper import get_devops_token as _dvt, is_masked
    tok = _dvt()
    if tok and not is_masked(tok):
        logger.info("DevOps PAT resolved from secrets (%d chars)", len(tok))
        return tok
    cfg = get_config()
    val = cfg.get("devops_pat", "")
    if is_masked(val):
        logger.warning("DevOps PAT is masked in config and not found in secrets")
        return ""
    return val


# Keep DEPLOY_CONFIG_PATH for any legacy code that references it
DEPLOY_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "deployconfig.json"
)
