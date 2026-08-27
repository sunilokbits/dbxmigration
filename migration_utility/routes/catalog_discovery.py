"""
Catalog Discovery Module — Dynamic multi-catalog auto-discovery for Genie AI.

Features:
- Discovers all accessible catalogs, schemas, and tables via information_schema
- Caches results with configurable TTL (auto-refresh)
- Detects new tables/schemas automatically on next refresh
- Provides schema context injection for Genie/MCP queries
- Supports SQL query execution across any discovered catalog
"""

import os
import json
import time
import threading
import logging
from datetime import datetime
from flask import Blueprint, request, jsonify
from routes.auth import login_required

logger = logging.getLogger(__name__)

catalog_discovery_bp = Blueprint("catalog_discovery", __name__)

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════
_HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
_WAREHOUSE_ID = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
_CACHE_TTL_SECONDS = int(os.environ.get("CATALOG_CACHE_TTL", "300"))  # 5 min default
_MAX_TABLES_PER_SCHEMA = int(os.environ.get("MAX_TABLES_PER_SCHEMA", "500"))

# ══════════════════════════════════════════════════════════════════════════════
# IN-MEMORY CACHE
# ══════════════════════════════════════════════════════════════════════════════
_cache = {
    "catalogs": [],          # [{name, comment, owner}]
    "schemas": [],           # [{catalog, schema, comment}]
    "tables": [],            # [{catalog, schema, table, type, columns:[{name, type, comment}]}]
    "last_refreshed": None,  # ISO timestamp
    "refresh_in_progress": False,
    "error": None,
    "stats": {"total_catalogs": 0, "total_schemas": 0, "total_tables": 0}
}
_cache_lock = threading.Lock()


# ══════════════════════════════════════════════════════════════════════════════
# TOKEN HELPER
# ══════════════════════════════════════════════════════════════════════════════
def _get_token():
    """Get auth token from environment or managed identity."""
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not token:
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            token = w.config.authenticate()
        except Exception:
            pass
    return token


def _headers():
    return {
        "Authorization": f"Bearer {_get_token()}",
        "Content-Type": "application/json"
    }


# ══════════════════════════════════════════════════════════════════════════════
# SQL EXECUTION ENGINE (Databricks SQL Statement API)
# ══════════════════════════════════════════════════════════════════════════════
def _execute_sql(sql, warehouse_id=None, max_rows=1000, timeout=120):
    """
    Execute SQL via Databricks Statement Execution API.
    Returns: {"columns": [...], "data": [[...]], "row_count": int, "error": str|None}
    """
    import requests
    wh_id = warehouse_id or _WAREHOUSE_ID
    if not wh_id:
        return {"columns": [], "data": [], "row_count": 0, "error": "No SQL warehouse configured. Set DATABRICKS_SQL_WAREHOUSE_ID."}

    url = f"{_HOST}/api/2.0/sql/statements"
    payload = {
        "warehouse_id": wh_id,
        "statement": sql,
        "wait_timeout": f"{timeout}s",
        "row_limit": max_rows,
        "format": "JSON_ARRAY"
    }

    try:
        r = requests.post(url, json=payload, headers=_headers(), timeout=timeout + 10)
        resp = r.json()

        status = resp.get("status", {}).get("state", "")

        # Poll if still running
        if status in ("PENDING", "RUNNING"):
            stmt_id = resp.get("statement_id", "")
            poll_url = f"{url}/{stmt_id}"
            for _ in range(int(timeout / 2)):
                time.sleep(2)
                pr = requests.get(poll_url, headers=_headers(), timeout=30)
                resp = pr.json()
                status = resp.get("status", {}).get("state", "")
                if status not in ("PENDING", "RUNNING"):
                    break

        if status == "SUCCEEDED":
            manifest = resp.get("manifest", {})
            columns = [c.get("name", "") for c in manifest.get("schema", {}).get("columns", [])]
            col_types = [c.get("type_name", "") for c in manifest.get("schema", {}).get("columns", [])]
            data_array = resp.get("result", {}).get("data_array", [])
            return {
                "columns": columns,
                "column_types": col_types,
                "data": data_array,
                "row_count": len(data_array),
                "truncated": resp.get("result", {}).get("truncated", False),
                "error": None
            }
        elif status == "FAILED":
            error_msg = resp.get("status", {}).get("error", {}).get("message", "SQL execution failed")
            return {"columns": [], "data": [], "row_count": 0, "error": error_msg}
        else:
            return {"columns": [], "data": [], "row_count": 0, "error": f"Unexpected status: {status}"}

    except Exception as exc:
        return {"columns": [], "data": [], "row_count": 0, "error": str(exc)}


# ══════════════════════════════════════════════════════════════════════════════
# CATALOG DISCOVERY (Auto-scan all accessible catalogs)
# ══════════════════════════════════════════════════════════════════════════════
def _discover_catalogs():
    """Discover all accessible catalogs."""
    result = _execute_sql("SHOW CATALOGS", timeout=30)
    if result["error"]:
        return [], result["error"]
    catalogs = []
    for row in result["data"]:
        if row and row[0]:
            catalogs.append({"name": row[0]})
    return catalogs, None


def _discover_schemas(catalog_name):
    """Discover all schemas in a catalog."""
    sql = f"SHOW SCHEMAS IN `{catalog_name}`"
    result = _execute_sql(sql, timeout=30)
    if result["error"]:
        return []
    schemas = []
    for row in result["data"]:
        if row and row[0]:
            # Skip internal schemas
            if row[0] not in ("information_schema", "__databricks_internal"):
                schemas.append({"catalog": catalog_name, "schema": row[0]})
    return schemas


def _discover_tables(catalog_name, schema_name):
    """Discover all tables in a schema with column details."""
    sql = f"""
    SELECT table_name, table_type
    FROM `{catalog_name}`.information_schema.tables
    WHERE table_schema = '{schema_name}'
    AND table_type IN ('MANAGED', 'EXTERNAL', 'VIEW', 'BASE TABLE')
    LIMIT {_MAX_TABLES_PER_SCHEMA}
    """
    result = _execute_sql(sql, timeout=60)
    if result["error"]:
        # Fallback to SHOW TABLES
        result = _execute_sql(f"SHOW TABLES IN `{catalog_name}`.`{schema_name}`", timeout=30)
        if result["error"]:
            return []
        tables = []
        for row in result["data"]:
            if row and len(row) >= 2:
                tables.append({
                    "catalog": catalog_name,
                    "schema": schema_name,
                    "table": row[1] if len(row) > 1 else row[0],
                    "type": "TABLE",
                    "columns": []
                })
        return tables

    tables = []
    for row in result["data"]:
        if row and row[0]:
            tables.append({
                "catalog": catalog_name,
                "schema": schema_name,
                "table": row[0],
                "type": row[1] if len(row) > 1 else "TABLE",
                "columns": []
            })
    return tables


def _discover_columns(catalog_name, schema_name, table_name):
    """Get column details for a table."""
    sql = f"""
    SELECT column_name, data_type, comment
    FROM `{catalog_name}`.information_schema.columns
    WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
    ORDER BY ordinal_position
    """
    result = _execute_sql(sql, timeout=30)
    if result["error"]:
        return []
    columns = []
    for row in result["data"]:
        if row:
            columns.append({
                "name": row[0] if len(row) > 0 else "",
                "type": row[1] if len(row) > 1 else "",
                "comment": row[2] if len(row) > 2 else ""
            })
    return columns


def _full_discovery(include_columns=False, catalogs_filter=None):
    """
    Run full catalog/schema/table discovery.
    Args:
        include_columns: If True, also fetch column metadata (slower)
        catalogs_filter: List of catalog names to scan (None = all accessible)
    """
    global _cache

    with _cache_lock:
        if _cache["refresh_in_progress"]:
            return
        _cache["refresh_in_progress"] = True

    try:
        logger.info("[CatalogDiscovery] Starting full discovery...")

        # Step 1: Discover catalogs
        all_catalogs, err = _discover_catalogs()
        if err:
            with _cache_lock:
                _cache["error"] = f"Failed to list catalogs: {err}"
                _cache["refresh_in_progress"] = False
            return

        # Apply filter if specified
        if catalogs_filter:
            all_catalogs = [c for c in all_catalogs if c["name"] in catalogs_filter]

        # Skip system catalogs that are typically not useful
        skip_catalogs = {"system", "__databricks_internal", "hive_metastore"}
        all_catalogs = [c for c in all_catalogs if c["name"] not in skip_catalogs]

        # Step 2: Discover schemas per catalog
        all_schemas = []
        for cat in all_catalogs:
            schemas = _discover_schemas(cat["name"])
            all_schemas.extend(schemas)

        # Step 3: Discover tables per schema
        all_tables = []
        for schema in all_schemas:
            tables = _discover_tables(schema["catalog"], schema["schema"])
            all_tables.extend(tables)

        # Step 4 (optional): Discover columns
        if include_columns:
            for tbl in all_tables:
                cols = _discover_columns(tbl["catalog"], tbl["schema"], tbl["table"])
                tbl["columns"] = cols

        # Update cache
        with _cache_lock:
            _cache["catalogs"] = all_catalogs
            _cache["schemas"] = all_schemas
            _cache["tables"] = all_tables
            _cache["last_refreshed"] = datetime.utcnow().isoformat() + "Z"
            _cache["error"] = None
            _cache["refresh_in_progress"] = False
            _cache["stats"] = {
                "total_catalogs": len(all_catalogs),
                "total_schemas": len(all_schemas),
                "total_tables": len(all_tables)
            }

        logger.info(f"[CatalogDiscovery] Done: {len(all_catalogs)} catalogs, {len(all_schemas)} schemas, {len(all_tables)} tables")

    except Exception as exc:
        with _cache_lock:
            _cache["error"] = str(exc)
            _cache["refresh_in_progress"] = False
        logger.error(f"[CatalogDiscovery] Error: {exc}")


def _ensure_cache_fresh():
    """Check if cache needs refresh and trigger background refresh if stale."""
    with _cache_lock:
        last = _cache["last_refreshed"]
        in_progress = _cache["refresh_in_progress"]

    if in_progress:
        return

    if last is None:
        # Never refreshed — do it now
        t = threading.Thread(target=_full_discovery, kwargs={"include_columns": True}, daemon=True)
        t.start()
        return

    # Check TTL
    try:
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        age = (datetime.now(last_dt.tzinfo) - last_dt).total_seconds()
        if age > _CACHE_TTL_SECONDS:
            t = threading.Thread(target=_full_discovery, kwargs={"include_columns": True}, daemon=True)
            t.start()
    except Exception:
        pass


def get_schema_context():
    """
    Build a dynamic schema context string for Genie/MCP queries.
    This replaces the hardcoded APP_CONTEXT_PREAMBLE with live data.
    """
    _ensure_cache_fresh()

    with _cache_lock:
        catalogs = _cache["catalogs"]
        schemas = _cache["schemas"]
        tables = _cache["tables"]
        last_refreshed = _cache["last_refreshed"]

    if not catalogs:
        return "(Schema discovery not yet complete — using default context)\n"

    lines = []
    lines.append(f"Available data (auto-discovered, last refreshed: {last_refreshed}):\n")

    # Group tables by catalog.schema
    catalog_map = {}
    for tbl in tables:
        key = f"{tbl['catalog']}.{tbl['schema']}"
        if key not in catalog_map:
            catalog_map[key] = []
        catalog_map[key].append(tbl)

    for key in sorted(catalog_map.keys()):
        tbls = catalog_map[key]
        lines.append(f"\n[{key}] ({len(tbls)} tables)")
        for t in tbls[:20]:  # Show max 20 per schema in context
            col_summary = ""
            if t.get("columns"):
                col_names = [c["name"] for c in t["columns"][:8]]
                col_summary = f" — columns: {', '.join(col_names)}"
                if len(t["columns"]) > 8:
                    col_summary += f" (+{len(t['columns'])-8} more)"
            lines.append(f"  • {t['catalog']}.{t['schema']}.{t['table']} ({t['type']}){col_summary}")
        if len(tbls) > 20:
            lines.append(f"  ... +{len(tbls)-20} more tables")

    lines.append(f"\nTotal: {len(catalogs)} catalogs, {len(schemas)} schemas, {len(tables)} tables")
    lines.append("You can query ANY of these tables using their fully qualified name (catalog.schema.table).\n")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# FLASK ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@catalog_discovery_bp.route("/api/v1/catalog/discover", methods=["POST"])
@login_required
def trigger_discovery():
    """Trigger a fresh catalog discovery scan."""
    data = request.get_json(silent=True) or {}
    catalogs_filter = data.get("catalogs")  # Optional: limit to specific catalogs
    include_columns = data.get("include_columns", True)

    with _cache_lock:
        if _cache["refresh_in_progress"]:
            return jsonify({"status": "already_running", "message": "Discovery scan already in progress"})

    t = threading.Thread(
        target=_full_discovery,
        kwargs={"include_columns": include_columns, "catalogs_filter": catalogs_filter},
        daemon=True
    )
    t.start()
    return jsonify({"status": "started", "message": "Catalog discovery started in background"})


@catalog_discovery_bp.route("/api/v1/catalog/status", methods=["GET"])
@login_required
def discovery_status():
    """Get current discovery cache status."""
    _ensure_cache_fresh()
    with _cache_lock:
        return jsonify({
            "last_refreshed": _cache["last_refreshed"],
            "refresh_in_progress": _cache["refresh_in_progress"],
            "stats": _cache["stats"],
            "error": _cache["error"],
            "cache_ttl_seconds": _CACHE_TTL_SECONDS
        })


@catalog_discovery_bp.route("/api/v1/catalog/list", methods=["GET"])
@login_required
def list_catalogs():
    """List all discovered catalogs with their schemas and table counts."""
    _ensure_cache_fresh()
    with _cache_lock:
        catalogs = _cache["catalogs"]
        schemas = _cache["schemas"]
        tables = _cache["tables"]

    # Build summary per catalog
    result = []
    for cat in catalogs:
        cat_schemas = [s for s in schemas if s["catalog"] == cat["name"]]
        cat_tables = [t for t in tables if t["catalog"] == cat["name"]]
        result.append({
            "catalog": cat["name"],
            "schema_count": len(cat_schemas),
            "table_count": len(cat_tables),
            "schemas": [s["schema"] for s in cat_schemas]
        })

    return jsonify({"catalogs": result, "total": len(result)})


@catalog_discovery_bp.route("/api/v1/catalog/tables", methods=["GET"])
@login_required
def list_tables():
    """List tables — optionally filtered by catalog and/or schema."""
    _ensure_cache_fresh()
    catalog_filter = request.args.get("catalog", "").strip()
    schema_filter = request.args.get("schema", "").strip()
    search = request.args.get("search", "").strip().lower()

    with _cache_lock:
        tables = list(_cache["tables"])

    if catalog_filter:
        tables = [t for t in tables if t["catalog"] == catalog_filter]
    if schema_filter:
        tables = [t for t in tables if t["schema"] == schema_filter]
    if search:
        tables = [t for t in tables if search in t["table"].lower() or search in f"{t['catalog']}.{t['schema']}.{t['table']}".lower()]

    return jsonify({"tables": tables, "total": len(tables)})


@catalog_discovery_bp.route("/api/v1/catalog/table-details", methods=["GET"])
@login_required
def table_details():
    """Get column details for a specific table."""
    full_name = request.args.get("table", "").strip()
    if not full_name or full_name.count(".") < 2:
        return jsonify({"error": "Provide fully qualified table name: catalog.schema.table"}), 400

    parts = full_name.split(".", 2)
    catalog, schema, table = parts[0], parts[1], parts[2]

    # Check cache first
    with _cache_lock:
        for t in _cache["tables"]:
            if t["catalog"] == catalog and t["schema"] == schema and t["table"] == table:
                if t.get("columns"):
                    return jsonify({"table": full_name, "columns": t["columns"]})

    # Fetch live if not in cache
    columns = _discover_columns(catalog, schema, table)
    return jsonify({"table": full_name, "columns": columns})


@catalog_discovery_bp.route("/api/v1/sql/execute", methods=["POST"])
@login_required
def execute_sql_endpoint():
    """
    Execute any SQL query across multiple catalogs.
    Supports SELECT, SHOW, DESCRIBE, and any read query.
    Write queries (INSERT, UPDATE, CREATE) require explicit allow flag.
    """
    data = request.get_json(silent=True) or {}
    sql = (data.get("sql") or data.get("query") or "").strip()
    max_rows = min(int(data.get("max_rows", 200)), 10000)
    allow_writes = data.get("allow_writes", False)
    warehouse_id = data.get("warehouse_id", "") or _WAREHOUSE_ID

    if not sql:
        return jsonify({"error": "sql field is required"}), 400

    # Safety check for write operations
    sql_upper = sql.upper().strip()
    write_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "MERGE", "TRUNCATE"]
    is_write = any(sql_upper.startswith(kw) for kw in write_keywords)

    if is_write and not allow_writes:
        return jsonify({
            "error": "Write operations require allow_writes=true. This is a safety check.",
            "sql": sql
        }), 403

    result = _execute_sql(sql, warehouse_id=warehouse_id, max_rows=max_rows)

    if result["error"]:
        return jsonify({"error": result["error"], "sql": sql}), 400

    return jsonify({
        "sql": sql,
        "columns": result["columns"],
        "column_types": result.get("column_types", []),
        "data": result["data"],
        "row_count": result["row_count"],
        "truncated": result.get("truncated", False)
    })


@catalog_discovery_bp.route("/api/v1/catalog/context", methods=["GET"])
@login_required
def get_context():
    """Return the dynamic schema context string for Genie/MCP injection."""
    context = get_schema_context()
    return jsonify({"context": context, "last_refreshed": _cache.get("last_refreshed")})
