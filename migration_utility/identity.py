"""Databricks App user identity — extracts authenticated user from proxy headers / SDK."""

import threading
import time
import os
from flask import request, g
from log_config import get_logger

logger = get_logger(__name__)

_USER_CACHE: dict[str, dict] = {}
_CACHE_TTL = 300  # 5 minutes
_cache_lock = threading.Lock()

# Databricks workspace groups → app roles
_ADMIN_GROUPS = {"admins", "account_admin", "migration-studio-admins", "users"}
_DEVELOPER_GROUPS = {"migration-studio-developers"}
# "users" group includes all workspace members → treat as Admin by default
# Create migration-studio-developers / migration-studio-admins groups for finer control

_workspace_client = None
_ws_lock = threading.Lock()


def _get_workspace_client():
    """Lazy-init a WorkspaceClient (auto-authenticates inside Databricks Apps)."""
    global _workspace_client
    if _workspace_client is not None:
        return _workspace_client
    with _ws_lock:
        if _workspace_client is not None:
            return _workspace_client
        try:
            from databricks.sdk import WorkspaceClient
            _workspace_client = WorkspaceClient()
            return _workspace_client
        except Exception as exc:
            logger.warning("Could not initialise WorkspaceClient: %s", exc)
            return None


def _resolve_role(groups: list[str]) -> str:
    """Map Databricks workspace group membership to app role."""
    group_set = {g.lower() for g in groups}
    if group_set & _ADMIN_GROUPS:
        return "Admin"
    if group_set & _DEVELOPER_GROUPS:
        return "Developer"
    return "Viewer"


def _fetch_user_from_sdk() -> dict | None:
    """Use the Databricks SDK to get the current user's identity.

    Prefers the user's own access token (forwarded by the Apps proxy)
    so we get the real user, not the app service principal.
    """
    user_token = request.headers.get("x-forwarded-access-token")
    if user_token:
        try:
            from databricks.sdk import WorkspaceClient
            user_ws = WorkspaceClient(
                host=os.environ.get("DATABRICKS_HOST", ""),
                token=user_token,
            )
            me = user_ws.current_user.me()
            groups = [g.display for g in (me.groups or []) if g.display]
            return {
                "email": me.user_name or (me.emails[0].value if me.emails else ""),
                "display_name": me.display_name or me.user_name or "",
                "user_id": me.id or "",
                "user_name": me.user_name or "",
                "groups": groups,
                "role": _resolve_role(groups),
            }
        except Exception as exc:
            logger.warning("User-token SDK current_user.me() failed: %s", exc)

    # Fallback to SP client (will return SP identity — used only as last resort)
    ws = _get_workspace_client()
    if ws is None:
        return None
    try:
        me = ws.current_user.me()
        # Detect if this is the service principal (not a real user)
        if me.display_name and "app-" in me.display_name:
            logger.info("SDK returned service principal identity — skipping")
            return None
        groups = [g.display for g in (me.groups or []) if g.display]
        return {
            "email": me.user_name or (me.emails[0].value if me.emails else ""),
            "display_name": me.display_name or me.user_name or "",
            "user_id": me.id or "",
            "user_name": me.user_name or "",
            "groups": groups,
            "role": _resolve_role(groups),
        }
    except Exception as exc:
        logger.warning("SDK current_user.me() failed: %s", exc)
        return None


def _extract_from_headers() -> dict | None:
    """Extract user identity from Databricks Apps proxy headers."""
    email = (
        request.headers.get("X-Forwarded-Email")
        or request.headers.get("X-Forwarded-User")
        or request.headers.get("X-Forwarded-Preferred-Username")
    )
    if not email:
        return None

    display_name = (
        request.headers.get("X-Forwarded-Name")
        or request.headers.get("X-Forwarded-Display-Name")
        or email.split("@")[0].replace(".", " ").title()
    )

    return {
        "email": email,
        "display_name": display_name,
        "user_id": request.headers.get("X-Forwarded-User-Id", ""),
        "user_name": email,
        "groups": [],
        "role": "Viewer",
    }


def _enrich_with_groups(user: dict) -> dict:
    """If groups are empty (header-only path), fetch using the user's own token."""
    if user.get("groups"):
        return user
    # Use the user's own access token (forwarded by the Databricks Apps proxy)
    # to get their real identity and groups. The SP-authenticated client would
    # return the service principal's name/groups instead.
    user_token = request.headers.get("x-forwarded-access-token")
    if user_token:
        try:
            from databricks.sdk import WorkspaceClient
            user_ws = WorkspaceClient(
                host=os.environ.get("DATABRICKS_HOST", ""),
                token=user_token,
            )
            me = user_ws.current_user.me()
            groups = [g.display for g in (me.groups or []) if g.display]
            user["groups"] = groups
            user["role"] = _resolve_role(groups)
            if me.display_name:
                user["display_name"] = me.display_name
        except Exception:
            pass
    else:
        # No user token available - use SP client for groups only, do NOT
        # overwrite display_name (SP client returns "app-XXXX" as name).
        ws = _get_workspace_client()
        if ws is None:
            return user
        try:
            me = ws.current_user.me()
            groups = [g.display for g in (me.groups or []) if g.display]
            user["groups"] = groups
            user["role"] = _resolve_role(groups)
            # Intentionally NOT overwriting display_name here
        except Exception:
            pass
    return user


def get_current_user() -> dict | None:
    """Return the authenticated user dict, or None if not authenticated.

    Checks cache first, then proxy headers, then SDK.
    """
    if hasattr(g, "_dbx_user") and g._dbx_user:
        return g._dbx_user

    # Try headers first (fastest path)
    user = _extract_from_headers()
    if user:
        cache_key = user["email"]
        with _cache_lock:
            cached = _USER_CACHE.get(cache_key)
            if cached and (time.time() - cached["_ts"]) < _CACHE_TTL:
                g._dbx_user = cached
                return cached

        user = _enrich_with_groups(user)
        user["_ts"] = time.time()
        with _cache_lock:
            _USER_CACHE[cache_key] = user
        g._dbx_user = user
        return user

    # Fallback: SDK (works inside Databricks Apps even without headers)
    user = _fetch_user_from_sdk()
    if user:
        user["_ts"] = time.time()
        with _cache_lock:
            _USER_CACHE[user["email"]] = user
        g._dbx_user = user
        return user

    # Local development fallback
    dev_user = os.environ.get("DEV_USER_EMAIL")
    if dev_user:
        user = {
            "email": dev_user,
            "display_name": os.environ.get("DEV_USER_NAME", dev_user.split("@")[0]),
            "user_id": "local-dev",
            "user_name": dev_user,
            "groups": ["admins"],
            "role": "Admin",
            "_ts": time.time(),
        }
        g._dbx_user = user
        return user

    return None


def clear_user_cache(email: str | None = None):
    """Clear cached user info. If email is None, clear all."""
    with _cache_lock:
        if email:
            _USER_CACHE.pop(email, None)
        else:
            _USER_CACHE.clear()
