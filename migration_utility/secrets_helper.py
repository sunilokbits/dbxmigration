"""Databricks Secrets helper — replaces Azure Key Vault for secret management.

Reads secrets from a Databricks secret scope. Works across all clouds
(Azure, AWS, GCP) since Databricks Secrets is cloud-agnostic.
"""

import base64
import os
import threading
from log_config import get_logger

logger = get_logger(__name__)

_SECRET_SCOPE = os.environ.get("DATABRICKS_SECRET_SCOPE", "migration-studio")
_cache: dict[str, str] = {}
_cache_lock = threading.Lock()

MASKED_VALUE = "••••••••"


_UNCONFIGURED_PLACEHOLDER = "REPLACE_ME"


def is_masked(val: str | None) -> bool:
    if val is None:
        return False
    v = val.strip()
    if v in (MASKED_VALUE, "********", "***"):
        return True
    # CI/CD scaffolds missing secret-scope keys with this literal placeholder
    # (see azure-pipelines.yml / .github/workflows/cicd.yml) so every expected
    # key is visible/discoverable in the workspace. Treat it as "not really
    # configured" everywhere, the same as a masked display value -- otherwise
    # callers (get_source_password, get_devops_token, etc.) would try to use
    # the literal string "REPLACE_ME" as a real credential and fail.
    if v.upper() == _UNCONFIGURED_PLACEHOLDER:
        return True
    if len(v) > 2 and len(set(v)) == 1:
        return True
    return False


def _get_ws_client():
    try:
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient()
    except Exception:
        return None


def get_secret(key: str, scope: str | None = None) -> str:
    """Fetch a secret from Databricks secret scope.  Cached in memory."""
    scope = scope or _SECRET_SCOPE
    cache_key = f"{scope}/{key}"

    with _cache_lock:
        if cache_key in _cache:
            return _cache[cache_key]

    # Try environment variable override first (for local dev)
    env_key = f"SECRET_{key.upper().replace('-', '_')}"
    env_val = os.environ.get(env_key)
    if env_val:
        with _cache_lock:
            _cache[cache_key] = env_val
        return env_val

    ws = _get_ws_client()
    if ws is None:
        logger.warning("Cannot fetch secret %s/%s — no WorkspaceClient", scope, key)
        return ""

    try:
        resp = ws.secrets.get_secret(scope=scope, key=key)
        raw = resp.value if hasattr(resp, "value") and resp.value else ""
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        # SDK returns base64-encoded secret value — decode it
        value = ""
        if raw:
            try:
                value = base64.b64decode(raw).decode("utf-8")
            except Exception:
                value = raw
        if value:
            logger.info("Secret %s/%s read OK (%d chars)", scope, key, len(value))
        else:
            logger.warning("Secret %s/%s returned empty", scope, key)
        with _cache_lock:
            _cache[cache_key] = value
        return value
    except Exception as exc:
        exc_str = str(exc)
        if "permission" in exc_str.lower() and not getattr(get_secret, "_acl_retry_done", False):
            get_secret._acl_retry_done = True
            if _try_self_grant_acl(scope):
                return get_secret(key, scope)
        logger.warning("Secret fetch failed for %s/%s: %s", scope, key, exc)
        return ""


def _try_self_grant_acl(scope: str) -> bool:
    """One-shot attempt: grant this SP READ on the scope via the scope owner."""
    ws = _get_ws_client()
    if ws is None:
        return False
    sp_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
    if not sp_id:
        return False
    try:
        ws.secrets.put_acl(scope=scope, principal=sp_id, permission="READ")
        logger.info("Self-granted READ ACL on scope %s for SP %s", scope, sp_id)
        return True
    except Exception as e:
        logger.warning("Could not self-grant ACL on %s: %s", scope, e)
        return False


_SOURCE_PASSWORD_SECRET_KEYS = {
    "sqlserver":  "source-sql-password",
    "azuresql":   "source-azuresql-password",
    "snowflake":  "source-snowflake-password",
    "bigquery":   "source-bigquery-password",
    "redshift":   "source-redshift-password",
    "synapse":    "source-synapse-password",
    "sharepoint": "source-sharepoint-password",
    "api":        "source-api-password",
}


def get_source_password(source_type: str = "sqlserver") -> str:
    """Return the source password based on source type, using the secret key
    convention shared with the rest of the workspace (see _SOURCE_PASSWORD_SECRET_KEYS)."""
    key = _SOURCE_PASSWORD_SECRET_KEYS.get(source_type, "source-sql-password")
    return get_secret(key)


def get_databricks_token() -> str:
    """Return a Databricks token for API calls.

    Prefers the stored PAT from the secret scope (full user permissions,
    including clusters, jobs, notebooks) over the app SP OAuth token which
    is limited to the effective_user_api_scopes set at deploy time.
    """
    # 1. Stored PAT — full permissions of the token owner
    stored = get_secret("databricks-token")
    if stored and not is_masked(stored):
        return stored

    # 2. SDK OAuth (SP token) — fallback, limited to app API scopes
    ws = _get_ws_client()
    if ws is None:
        return ""
    try:
        cfg = ws.config
        header_factory = cfg.authenticate()
        if callable(header_factory):
            headers = header_factory(None)
        elif isinstance(header_factory, dict):
            headers = header_factory
        else:
            headers = {}
        auth_header = headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            logger.info("Using SP M2M OAuth token (fallback)")
            return auth_header[7:]
    except Exception as exc:
        logger.warning("SDK OAuth token fallback failed: %s", exc)
    return ""


def get_devops_token() -> str:
    return get_secret("devops-pat")


def set_secret(key: str, value: str, scope: str | None = None) -> bool:
    """Store a secret in Databricks scope (or in-memory + env for local dev)."""
    scope = scope or _SECRET_SCOPE
    cache_key = f"{scope}/{key}"

    with _cache_lock:
        _cache[cache_key] = value

    env_key = f"SECRET_{key.upper().replace('-', '_')}"
    os.environ[env_key] = value

    ws = _get_ws_client()
    if ws is None:
        logger.info("Secret %s cached in-memory (no WorkspaceClient for remote store)", key)
        return True

    try:
        try:
            ws.secrets.create_scope(scope=scope)
            logger.info("Created secret scope: %s", scope)
        except Exception as e:
            if "SCOPE_ALREADY_EXISTS" not in str(e) and "already exists" not in str(e).lower():
                logger.warning("Could not create scope %s: %s", scope, e)
        ws.secrets.put_secret(scope=scope, key=key, string_value=value)
        logger.info("Secret %s/%s stored in Databricks", scope, key)
        return True
    except Exception as exc:
        logger.warning("Failed to store secret %s/%s remotely: %s (kept in-memory)", scope, key, exc)
        return True


def ensure_scope_exists(scope: str | None = None) -> dict:
    """Create the secret scope if it doesn't exist. Returns status dict."""
    scope = scope or _SECRET_SCOPE
    ws = _get_ws_client()
    if ws is None:
        return {"success": False, "error": "No WorkspaceClient available"}
    try:
        scopes = [s.name for s in ws.secrets.list_scopes()]
        if scope in scopes:
            return {"success": True, "message": f"Scope '{scope}' already exists", "created": False}
        ws.secrets.create_scope(scope=scope)
        return {"success": True, "message": f"Scope '{scope}' created", "created": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


def set_source_password(value: str, source_type: str = "sqlserver") -> bool:
    if source_type == "snowflake":
        return set_secret("source-snowflake-password", value)
    key = _SOURCE_PASSWORD_SECRET_KEYS.get(source_type, "source-sql-password")
    return set_secret(key, value)


def set_databricks_token(value: str) -> bool:
    return set_secret("databricks-token", value)


def set_devops_token(value: str) -> bool:
    return set_secret("devops-pat", value)


def clear_cache():
    with _cache_lock:
        _cache.clear()
