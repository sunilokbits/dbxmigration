"""Databricks SQL connection manager for Delta table persistence.

Provides a thread-safe connection pool to a Serverless SQL Warehouse
via the databricks-sql-connector, with automatic OAuth passthrough
when running inside a Databricks App.
"""

import atexit
import os
import threading
from log_config import get_logger

logger = get_logger(__name__)

_local = threading.local()
_init_lock = threading.Lock()
_tables_initialised = False
_discovered_warehouse_id = None
_open_connections = []
_connections_lock = threading.Lock()


def close_all_connections():
    """Close every open connection.

    The connector segfaults during interpreter shutdown if connections are left
    to the garbage collector, so they are closed while the runtime is healthy.
    """
    with _connections_lock:
        connections, _open_connections[:] = list(_open_connections), []
    for conn in connections:
        try:
            conn.close()
        except Exception:
            pass
    _local.conn = None


atexit.register(close_all_connections)


def _auto_discover_warehouse_id():
    """Find a SQL warehouse in the workspace when none is configured via env vars."""
    global _discovered_warehouse_id
    if _discovered_warehouse_id is not None:
        return _discovered_warehouse_id
    try:
        from databricks.sdk import WorkspaceClient
        # Try with the stored PAT first (deploy user has warehouse access),
        # fall back to default SP credentials if no PAT is available.
        host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        if host and not host.startswith("http"):
            host = "https://" + host
        token = None
        try:
            from secrets_helper import get_databricks_token
            token = get_databricks_token()
        except Exception:
            pass
        if host and token:
            w = WorkspaceClient(host=host, token=token)
        else:
            w = WorkspaceClient()
        warehouses = list(w.warehouses.list())
        running = next((wh for wh in warehouses
                        if "RUNNING" in str(getattr(wh.state, "value", wh.state)).upper()), None)
        chosen = running or (warehouses[0] if warehouses else None)
        if chosen:
            _discovered_warehouse_id = chosen.id
            logger.info("Auto-discovered SQL warehouse: %s (%s)", chosen.name, chosen.id)
            return chosen.id
    except Exception as e:
        logger.warning("SQL warehouse auto-discovery failed: %s", e)
    _discovered_warehouse_id = ""
    return ""


def _get_config():
    """Read SQL warehouse config from environment, auto-discovering if needed."""
    warehouse_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
    if not warehouse_id:
        warehouse_id = _auto_discover_warehouse_id()
    return {
        "server_hostname": os.environ.get("DATABRICKS_SERVER_HOSTNAME", os.environ.get("DATABRICKS_HOST", "")),
        "http_path": os.environ.get("DATABRICKS_HTTP_PATH", ""),
        "warehouse_id": warehouse_id,
        "catalog": os.environ.get("DATABRICKS_CATALOG", "admin_source"),
        "schema": os.environ.get("DATABRICKS_SCHEMA", "migration_app"),
    }


def get_catalog_schema() -> tuple[str, str]:
    """Return (catalog, schema) for this app's own tables (user_roles,
    audit_log, job_schedules, migration_jobs, dm_models, doc_qa_chunks*).

    Prefers the "Metadata Catalog"/"Metadata Schema" a user configures at
    runtime in Settings (config_cache's metadata_catalog/metadata_schema --
    the same setting workflow_manager.py's wf_* tables already follow) over
    the static DATABRICKS_CATALOG/DATABRICKS_SCHEMA env vars baked into
    app.yml at deploy time. That way choosing a different catalog to test
    against relocates ALL of this app's tables there, not just the
    workflow ones, and doesn't require a redeploy to change.

    app_config itself is the one exception: it's what makes the dynamic
    value discoverable in the first place, so its own location has to stay
    anchored to the static env vars (see config_cache.py's _fqn()).
    """
    cfg = _get_config()
    try:
        from config_cache import get_config as _get_app_config
        dyn = _get_app_config() or {}
        catalog = dyn.get("metadata_catalog") or cfg["catalog"]
        schema = dyn.get("metadata_schema") or cfg["schema"]
        return catalog, schema
    except Exception:
        return cfg["catalog"], cfg["schema"]


def get_connection():
    """Return a thread-local Databricks SQL connection.

    Auto-reconnects on failure.  Auth is handled by the Databricks SDK
    credential provider (OAuth token passthrough in Databricks Apps).
    """
    if hasattr(_local, "conn") and _local.conn is not None:
        try:
            _local.conn.cursor().execute("SELECT 1")
            return _local.conn
        except Exception:
            try:
                _local.conn.close()
            except Exception:
                pass
            with _connections_lock:
                if _local.conn in _open_connections:
                    _open_connections.remove(_local.conn)
            _local.conn = None

    cfg = _get_config()
    server = cfg["server_hostname"]
    http_path = cfg["http_path"]

    if not server or not http_path:
        if cfg["warehouse_id"]:
            server = server or os.environ.get("DATABRICKS_HOST", "").replace("https://", "").rstrip("/")
            http_path = f"/sql/1.0/warehouses/{cfg['warehouse_id']}"
        else:
            raise RuntimeError(
                "Databricks SQL not configured. Set DATABRICKS_SERVER_HOSTNAME + DATABRICKS_HTTP_PATH "
                "or DATABRICKS_HOST + DATABRICKS_SQL_WAREHOUSE_ID."
            )

    from databricks import sql as dbsql
    from secrets_helper import get_databricks_token as _get_token

    token = _get_token()
    connect_kwargs = {
        "server_hostname": server,
        "http_path": http_path,
        "catalog": cfg["catalog"],
        "schema": cfg["schema"],
    }
    if token:
        connect_kwargs["access_token"] = token
    else:
        # When running on Databricks Apps without a stored PAT,
        # omit access_token so the connector uses the built-in M2M OAuth
        # (DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET from env).
        logger.info("No PAT available — using app SP M2M OAuth for SQL connection")

    try:
        conn = dbsql.connect(**connect_kwargs)
    except Exception as e:
        _local.conn = None
        raise RuntimeError(f"Databricks SQL connection to {server} failed: {e}") from e
    _local.conn = conn
    with _connections_lock:
        _open_connections.append(conn)
    logger.info("Databricks SQL connection established: %s", server)
    return _local.conn


def execute_query(sql: str, params: dict | None = None) -> list[dict]:
    """Execute a SELECT and return rows as list of dicts."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()


def execute_write(sql: str, params: dict | None = None) -> int:
    """Execute an INSERT/UPDATE/DELETE/MERGE and return affected row count."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        return cursor.rowcount if cursor.rowcount is not None else 0
    finally:
        cursor.close()


def execute_many(sql: str, param_list: list[dict]) -> int:
    """Execute a parameterised statement for each param dict."""
    conn = get_connection()
    cursor = conn.cursor()
    total = 0
    try:
        for params in param_list:
            cursor.execute(sql, params)
            total += cursor.rowcount if cursor.rowcount else 0
        return total
    finally:
        cursor.close()


def reset_tables_initialised():
    """Force the next ensure_tables() call to actually run its DDL again.

    Needed after the "Metadata Catalog"/"Metadata Schema" setting changes:
    ensure_tables() only runs once per process by default, so switching to
    a different catalog to test against wouldn't otherwise replicate this
    app's tables there until the process happened to restart.
    """
    global _tables_initialised
    _tables_initialised = False


def ensure_tables():
    """Create app persistence Delta tables if they don't exist (idempotent)."""
    global _tables_initialised
    if _tables_initialised:
        return

    with _init_lock:
        if _tables_initialised:
            return

        catalog, schema = get_catalog_schema()
        # app_config's own table always stays at the static env-var location
        # (see get_catalog_schema()'s docstring) -- it's what makes the
        # dynamic catalog/schema above discoverable, so it can't move with it.
        _static_cfg = _get_config()
        app_cfg_catalog, app_cfg_schema = _static_cfg["catalog"], _static_cfg["schema"]
        ddl_statements = [
            f"CREATE CATALOG IF NOT EXISTS {catalog}",
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}",
            f"CREATE CATALOG IF NOT EXISTS {app_cfg_catalog}",
            f"CREATE SCHEMA IF NOT EXISTS {app_cfg_catalog}.{app_cfg_schema}",
            f"""CREATE TABLE IF NOT EXISTS {catalog}.{schema}.migration_jobs (
                job_id STRING NOT NULL,
                payload STRING NOT NULL,
                updated_by STRING,
                updated_at TIMESTAMP DEFAULT current_timestamp()
            ) USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""",

            f"""CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dm_models (
                model_id STRING NOT NULL,
                payload STRING NOT NULL,
                updated_by STRING,
                updated_at TIMESTAMP DEFAULT current_timestamp()
            ) USING DELTA""",

            f"""CREATE TABLE IF NOT EXISTS {app_cfg_catalog}.{app_cfg_schema}.app_config (
                config_key STRING NOT NULL,
                config_value STRING,
                updated_by STRING,
                updated_at TIMESTAMP DEFAULT current_timestamp()
            ) USING DELTA""",

            f"""CREATE TABLE IF NOT EXISTS {catalog}.{schema}.audit_log (
                event_id STRING NOT NULL,
                user_email STRING,
                user_name STRING,
                action STRING,
                resource_type STRING,
                resource_id STRING,
                details_json STRING,
                ip_address STRING,
                response_status INT,
                created_at TIMESTAMP DEFAULT current_timestamp()
            ) USING DELTA
            TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 90 days')""",

            f"""CREATE TABLE IF NOT EXISTS {catalog}.{schema}.job_schedules (
                schedule_id STRING NOT NULL,
                schedule_data STRING NOT NULL,
                is_active BOOLEAN DEFAULT true,
                created_by STRING,
                updated_at TIMESTAMP DEFAULT current_timestamp()
            ) USING DELTA""",

            f"""CREATE TABLE IF NOT EXISTS {catalog}.{schema}.user_roles (
                user_email STRING NOT NULL,
                role STRING NOT NULL,
                display_name STRING,
                assigned_by STRING,
                updated_at TIMESTAMP DEFAULT current_timestamp()
            ) USING DELTA""",
        ]

        conn = get_connection()
        cursor = conn.cursor()
        try:
            for ddl in ddl_statements:
                try:
                    cursor.execute(ddl)
                except Exception as exc:
                    logger.warning("DDL skipped (may already exist): %s — %s", ddl[:80], exc)
        finally:
            cursor.close()

        _tables_initialised = True
        logger.info("App Delta tables ensured in %s.%s", catalog, schema)
