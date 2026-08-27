"""Audit trail — logs every mutating API call to a Delta table.

Usage:
    from audit import log_action, register_audit_hooks

    # In app.py:
    register_audit_hooks(app)

    # Explicit logging in routes:
    log_action("pipeline_created", "pipeline", pipeline_id, {"tables": 5})
"""

import json
import threading
import time
import uuid
from flask import Flask, request, g
from log_config import get_logger
from dbsql_client import execute_write, execute_many, get_catalog_schema

logger = get_logger(__name__)

_BUFFER: list[dict] = []
_BUFFER_LOCK = threading.Lock()
_FLUSH_SIZE = 20
_FLUSH_INTERVAL = 10  # seconds
_last_flush = time.time()

_MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


def _fqn() -> str:
    catalog, schema = get_catalog_schema()
    return f"{catalog}.{schema}.audit_log"


def log_action(action: str, resource_type: str = "", resource_id: str = "",
               details: dict | None = None, user: dict | None = None):
    """Queue an audit event for batch writing to Delta."""
    if user is None:
        try:
            user = getattr(g, "user", None) or {}
        except RuntimeError:
            user = {}

    event = {
        "event_id": str(uuid.uuid4()),
        "user_email": user.get("email", "system"),
        "user_name": user.get("display_name", ""),
        "action": action,
        "resource_type": resource_type,
        "resource_id": str(resource_id) if resource_id else "",
        "details_json": json.dumps(details, default=str) if details else "{}",
        "ip_address": _get_client_ip(),
        "response_status": 0,
    }

    with _BUFFER_LOCK:
        _BUFFER.append(event)
        if len(_BUFFER) >= _FLUSH_SIZE:
            _flush_buffer()


def _get_client_ip() -> str:
    try:
        return request.headers.get("X-Forwarded-For", request.remote_addr or "")
    except RuntimeError:
        return ""


def _flush_buffer():
    """Write buffered events to Delta (called with lock held)."""
    global _last_flush
    if not _BUFFER:
        return

    events = list(_BUFFER)
    _BUFFER.clear()
    _last_flush = time.time()

    threading.Thread(target=_write_events, args=(events,), daemon=True).start()


def _write_events(events: list[dict]):
    """Background write of audit events to Delta table."""
    try:
        fqn = _fqn()
        for event in events:
            execute_write(
                f"""INSERT INTO {fqn}
                    (event_id, user_email, user_name, action, resource_type, resource_id, details_json, ip_address, response_status, created_at)
                    VALUES (%(event_id)s, %(user_email)s, %(user_name)s, %(action)s, %(resource_type)s, %(resource_id)s, %(details_json)s, %(ip_address)s, %(response_status)s, current_timestamp())""",
                event,
            )
    except Exception as exc:
        logger.warning("Audit write failed (events lost): %s", exc)


def register_audit_hooks(app: Flask):
    """Register Flask hooks for automatic audit logging on mutating requests."""

    @app.after_request
    def _audit_after_request(response):
        if request.method not in _MUTATING_METHODS:
            return response
        if request.path.startswith("/static/") or request.path in ("/health", "/favicon.ico"):
            return response

        user = getattr(g, "user", None) or {}
        event = {
            "event_id": str(uuid.uuid4()),
            "user_email": user.get("email", "anonymous"),
            "user_name": user.get("display_name", ""),
            "action": f"{request.method} {request.path}",
            "resource_type": "api",
            "resource_id": request.path,
            "details_json": json.dumps({
                "method": request.method,
                "path": request.path,
                "status": response.status_code,
            }),
            "ip_address": request.headers.get("X-Forwarded-For", request.remote_addr or ""),
            "response_status": response.status_code,
        }

        with _BUFFER_LOCK:
            _BUFFER.append(event)
            if len(_BUFFER) >= _FLUSH_SIZE or (time.time() - _last_flush) > _FLUSH_INTERVAL:
                _flush_buffer()

        return response

    @app.teardown_appcontext
    def _flush_on_teardown(exc):
        with _BUFFER_LOCK:
            if _BUFFER:
                _flush_buffer()
