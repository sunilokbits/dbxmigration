"""Delta table persistence layer — replaces SQLite with Databricks SQL.

Public API is intentionally identical to the previous SQLite version so that
all existing callers (app.py, blueprints) work without import changes.
An in-memory write-through cache mitigates network latency vs local SQLite.
"""

import json
import threading
from flask import g
from log_config import get_logger
from dbsql_client import execute_query, execute_write, ensure_tables, get_catalog_schema

logger = get_logger(__name__)

_jobs_cache: dict[str, dict] = {}
_models_cache: dict[str, dict] = {}
_cache_lock = threading.Lock()
_cache_loaded = False


def _fqn(table: str) -> str:
    """Return fully-qualified table name."""
    catalog, schema = get_catalog_schema()
    return f"{catalog}.{schema}.{table}"


def _current_user() -> str:
    """Best-effort current user email for updated_by columns."""
    try:
        if hasattr(g, "user") and g.user:
            return g.user.get("email", "system")
    except RuntimeError:
        pass
    return "system"


def init_db():
    """Create Delta tables if they don't exist (called from app.py on startup)."""
    try:
        ensure_tables()
        _warm_cache()
    except Exception as exc:
        logger.warning("Delta table init skipped (no Databricks SQL connection): %s", exc)


def _warm_cache():
    """Pre-load jobs and models into memory for fast reads."""
    global _cache_loaded
    if _cache_loaded:
        return
    with _cache_lock:
        if _cache_loaded:
            return
        try:
            rows = execute_query(f"SELECT job_id, payload FROM {_fqn('migration_jobs')}")
            for r in rows:
                _jobs_cache[r["job_id"]] = json.loads(r["payload"])

            rows = execute_query(f"SELECT model_id, payload FROM {_fqn('dm_models')}")
            for r in rows:
                _models_cache[r["model_id"]] = json.loads(r["payload"])

            _cache_loaded = True
            logger.info("Persistence cache warmed: %d jobs, %d models", len(_jobs_cache), len(_models_cache))
        except Exception as exc:
            logger.warning("Cache warm failed (tables may not exist yet): %s", exc)


# ── Migration Jobs ────────────────────────────────────────────────────────────

def save_job(job_id: str, data: dict):
    payload = json.dumps(data, default=str)
    user = _current_user()
    execute_write(
        f"""MERGE INTO {_fqn('migration_jobs')} AS t
            USING (SELECT %(job_id)s AS job_id) AS s ON t.job_id = s.job_id
            WHEN MATCHED THEN UPDATE SET payload = %(payload)s, updated_by = %(user)s, updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (job_id, payload, updated_by, updated_at) VALUES (%(job_id)s, %(payload)s, %(user)s, current_timestamp())""",
        {"job_id": job_id, "payload": payload, "user": user},
    )
    with _cache_lock:
        _jobs_cache[job_id] = data


def load_job(job_id: str) -> dict | None:
    with _cache_lock:
        if job_id in _jobs_cache:
            return _jobs_cache[job_id]
    rows = execute_query(
        f"SELECT payload FROM {_fqn('migration_jobs')} WHERE job_id = %(job_id)s",
        {"job_id": job_id},
    )
    if not rows:
        return None
    data = json.loads(rows[0]["payload"])
    with _cache_lock:
        _jobs_cache[job_id] = data
    return data


def load_all_jobs() -> dict:
    with _cache_lock:
        if _jobs_cache:
            return dict(_jobs_cache)
    rows = execute_query(f"SELECT job_id, payload FROM {_fqn('migration_jobs')}")
    result = {}
    for r in rows:
        result[r["job_id"]] = json.loads(r["payload"])
    with _cache_lock:
        _jobs_cache.update(result)
    return result


def delete_job(job_id: str):
    execute_write(
        f"DELETE FROM {_fqn('migration_jobs')} WHERE job_id = %(job_id)s",
        {"job_id": job_id},
    )
    with _cache_lock:
        _jobs_cache.pop(job_id, None)


# ── Data Models ───────────────────────────────────────────────────────────────

def save_model(model_id: str, data: dict):
    payload = json.dumps(data, default=str)
    user = _current_user()
    execute_write(
        f"""MERGE INTO {_fqn('dm_models')} AS t
            USING (SELECT %(model_id)s AS model_id) AS s ON t.model_id = s.model_id
            WHEN MATCHED THEN UPDATE SET payload = %(payload)s, updated_by = %(user)s, updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (model_id, payload, updated_by, updated_at) VALUES (%(model_id)s, %(payload)s, %(user)s, current_timestamp())""",
        {"model_id": model_id, "payload": payload, "user": user},
    )
    with _cache_lock:
        _models_cache[model_id] = data


def load_model(model_id: str) -> dict | None:
    with _cache_lock:
        if model_id in _models_cache:
            return _models_cache[model_id]
    rows = execute_query(
        f"SELECT payload FROM {_fqn('dm_models')} WHERE model_id = %(model_id)s",
        {"model_id": model_id},
    )
    if not rows:
        return None
    data = json.loads(rows[0]["payload"])
    with _cache_lock:
        _models_cache[model_id] = data
    return data


def load_all_models() -> dict:
    with _cache_lock:
        if _models_cache:
            return dict(_models_cache)
    rows = execute_query(f"SELECT model_id, payload FROM {_fqn('dm_models')}")
    result = {}
    for r in rows:
        result[r["model_id"]] = json.loads(r["payload"])
    with _cache_lock:
        _models_cache.update(result)
    return result


# ── Job Schedules ─────────────────────────────────────────────────────────────

def save_schedule(schedule_id: str, data: dict, active: bool = True):
    payload = json.dumps(data, default=str)
    user = _current_user()
    execute_write(
        f"""MERGE INTO {_fqn('job_schedules')} AS t
            USING (SELECT %(sid)s AS schedule_id) AS s ON t.schedule_id = s.schedule_id
            WHEN MATCHED THEN UPDATE SET schedule_data = %(payload)s, is_active = %(active)s, updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (schedule_id, schedule_data, is_active, created_by, updated_at) VALUES (%(sid)s, %(payload)s, %(active)s, %(user)s, current_timestamp())""",
        {"sid": schedule_id, "payload": payload, "active": active, "user": user},
    )


def load_schedules() -> dict:
    rows = execute_query(f"SELECT schedule_id, schedule_data, is_active FROM {_fqn('job_schedules')}")
    return {r["schedule_id"]: {**json.loads(r["schedule_data"]), "_active": r["is_active"]} for r in rows}


def delete_schedule(schedule_id: str):
    execute_write(
        f"DELETE FROM {_fqn('job_schedules')} WHERE schedule_id = %(sid)s",
        {"sid": schedule_id},
    )
