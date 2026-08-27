"""Schema comparison & reconciliation blueprint."""
from flask import Blueprint, request, jsonify
import os, sys, json
from datetime import datetime

from .auth import login_required
from log_config import get_logger
from config_cache import get_config, get_databricks_token, get_source_password
from sql_pool import build_sql_conn_str, get_connection

# Ensure sibling modules are importable
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from unity_catalog_executor import UnityCatalogExecutor

logger = get_logger(__name__)
schema_bp = Blueprint("schema", __name__, url_prefix="/api/v1")

# ── Paths to parent-directory config files ────────────────────────────────────
_PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMA_COMPARE_PATH = os.path.join(_PARENT_DIR, "schema_compare_results.json")

# ── SQL → Databricks type mapping ─────────────────────────────────────────────
_SQL_TO_DBX_TYPE = {
    "int": "INT", "bigint": "BIGINT", "smallint": "SMALLINT", "tinyint": "TINYINT",
    "bit": "BOOLEAN", "float": "DOUBLE", "real": "FLOAT",
    "decimal": "DECIMAL", "numeric": "DECIMAL", "money": "DECIMAL(19,4)", "smallmoney": "DECIMAL(10,4)",
    "char": "STRING", "varchar": "STRING", "nchar": "STRING", "nvarchar": "STRING",
    "text": "STRING", "ntext": "STRING",
    "date": "DATE", "datetime": "TIMESTAMP", "datetime2": "TIMESTAMP",
    "smalldatetime": "TIMESTAMP", "datetimeoffset": "TIMESTAMP",
    "time": "STRING", "uniqueidentifier": "STRING",
    "binary": "BINARY", "varbinary": "BINARY", "image": "BINARY",
    "xml": "STRING", "sql_variant": "STRING",
}


# ── Type helpers ──────────────────────────────────────────────────────────────
def _normalise_sql_type(data_type, char_max_len, precision, scale):
    """Convert a SQL Server INFORMATION_SCHEMA type to a short display string."""
    dt = data_type.lower()
    if dt in ("decimal", "numeric") and precision is not None:
        return f"DECIMAL({precision},{scale or 0})"
    if dt in ("varchar", "nvarchar", "char", "nchar"):
        length = "MAX" if char_max_len in (None, -1) else str(char_max_len)
        return f"{data_type.upper()}({length})"
    return data_type.upper()


def _expected_dbx_type(data_type):
    """What Databricks type we expect a SQL Server type to map to."""
    base = data_type.lower().split("(")[0]
    return _SQL_TO_DBX_TYPE.get(base, data_type.upper())


# ── Column fetchers ───────────────────────────────────────────────────────────
def _fetch_source_columns(source_cfg, schema_name):
    """Return {table_name: [{column, data_type, is_nullable}, ...]} from the source DB."""
    password = source_cfg.get("password", "")
    from keyvault_helper import is_masked
    src_type = source_cfg.get("source_type", "sqlserver")
    if not password or is_masked(password):
        password = get_source_password(source_type=src_type)

    if src_type in ("sharepoint", "api"):
        raise ValueError(
            f"Schema comparison is not supported for {src_type} sources — "
            "use Discovery or Pipeline Studio instead."
        )

    if src_type == "snowflake":
        import snowflake_connector
        database = source_cfg.get("database", "")
        conn = snowflake_connector.get_snowflake_connection(
            account=source_cfg.get("account") or source_cfg.get("server", ""),
            username=source_cfg.get("username", ""), password=password,
            database=database, warehouse=source_cfg.get("warehouse", ""),
            role=source_cfg.get("role", ""),
        )
        cursor = conn.cursor()
        _sch_safe = schema_name.replace("'", "''")
        cursor.execute(f"""
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE,
                   CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
                   IS_NULLABLE
            FROM   {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE  TABLE_SCHEMA = '{_sch_safe}'
            ORDER  BY TABLE_NAME, ORDINAL_POSITION
        """)
        result = {}
        for row in cursor.fetchall():
            tbl = row[0]
            result.setdefault(tbl, []).append({
                "column": row[1],
                "data_type": _normalise_sql_type(row[2], row[3], row[4], row[5]),
                "is_nullable": row[6] == "YES",
            })
        conn.close()
        return result

    if src_type == "redshift":
        from redshift_client import get_redshift_connection
        conn = get_redshift_connection(
            server=source_cfg.get("server", ""), username=source_cfg.get("username", ""),
            password=password, database=source_cfg.get("database", ""),
        )
        cursor = conn.cursor()
        _sch_safe = schema_name.replace("'", "''")
        cursor.execute(f"""
            SELECT table_name, column_name, data_type,
                   character_maximum_length, numeric_precision, numeric_scale,
                   is_nullable
            FROM   information_schema.columns
            WHERE  table_schema = '{_sch_safe}'
            ORDER  BY table_name, ordinal_position
        """)
        result = {}
        for row in cursor.fetchall():
            tbl = row[0]
            result.setdefault(tbl, []).append({
                "column": row[1],
                "data_type": _normalise_sql_type(row[2], row[3], row[4], row[5]),
                "is_nullable": row[6] == "YES",
            })
        conn.close()
        return result

    conn = get_connection(
        src_type, source_cfg["server"], source_cfg["database"],
        source_cfg["username"], password,
    )
    cursor = conn.cursor()
    _sch_safe = schema_name.replace("'", "''")
    cursor.execute(f"""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE,
               CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
               IS_NULLABLE
        FROM   INFORMATION_SCHEMA.COLUMNS
        WHERE  TABLE_SCHEMA = N'{_sch_safe}'
        ORDER  BY TABLE_NAME, ORDINAL_POSITION
    """)
    result = {}
    for row in cursor.fetchall():
        tbl = row[0]
        result.setdefault(tbl, []).append({
            "column": row[1],
            "data_type": _normalise_sql_type(row[2], row[3], row[4], row[5]),
            "is_nullable": row[6] == "YES",
        })
    conn.close()
    return result


def _fetch_target_columns(host, token, catalog, schema_name):
    """Return {table_name: [{column, data_type, is_nullable}, ...]} from Databricks UC."""
    uc = UnityCatalogExecutor(host, token, catalog, schema_name)

    # First, list all tables in catalog.schema via UC REST API
    tables_resp = uc.list_tables()
    table_names = []
    for t in tables_resp.get("tables", []):
        tname = t.get("table_name") or t.get("name", "")
        if tname:
            table_names.append(tname)

    if not table_names:
        # Fallback: try via SQL if REST API didn't return tables
        wh_resp = uc.list_warehouses()
        warehouses = wh_resp.get("warehouses", [])
        wh_id = next((w["id"] for w in warehouses if w.get("state") == "RUNNING"), None)
        if not wh_id and warehouses:
            wh_id = warehouses[0].get("id")
        if wh_id:
            show_result = uc._execute_statement(
                f"SHOW TABLES IN `{catalog}`.`{schema_name}`", wh_id
            )
            for chunk in show_result.get("result", {}).get("data_array", []):
                if len(chunk) >= 2:
                    table_names.append(chunk[1])

    if not table_names:
        return {}

    # Get a warehouse for DESCRIBE queries
    wh_resp = uc.list_warehouses()
    warehouses = wh_resp.get("warehouses", [])
    wh_id = next((w["id"] for w in warehouses if w.get("state") == "RUNNING"), None)
    if not wh_id and warehouses:
        wh_id = warehouses[0].get("id")
    if not wh_id:
        return {}

    result = {}
    for tbl in table_names:
        fqn = f"`{catalog}`.`{schema_name}`.`{tbl}`"
        desc = uc._execute_statement(f"DESCRIBE TABLE {fqn}", wh_id)
        cols = []
        for row in desc.get("result", {}).get("data_array", []):
            col_name = (row[0] or "").strip()
            if not col_name or col_name.startswith("#"):
                break  # stop at partition/metadata section
            cols.append({
                "column": col_name,
                "data_type": (row[1] or "").strip().upper(),
                "is_nullable": True,  # Databricks defaults nullable
            })
        if cols:
            result[tbl] = cols
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  Routes
# ══════════════════════════════════════════════════════════════════════════════

@schema_bp.route("/schema/compare", methods=["POST"])
@login_required
def compare_schemas():
    """Compare source SQL Server schema against target Databricks catalog.schema."""
    data = request.get_json(force=True)
    src = data.get("source", "").strip()
    tgt = data.get("target", "").strip()

    if not src or not tgt:
        return jsonify({"tables": [], "error": "source and target are required"}), 400

    # ── Load config ───────────────────────────────────────────────────────
    cfg = get_config()

    source_cfg = cfg.get("source", {})
    dbx_host = cfg.get("databricks_host", "").rstrip("/")
    dbx_token = get_databricks_token()

    _src_type = source_cfg.get("source_type", "sqlserver")
    _has_src_conn = source_cfg.get("account") if _src_type == "snowflake" else source_cfg.get("server")
    if not _has_src_conn or not source_cfg.get("database"):
        return jsonify({"tables": [], "error": "Source connection not configured. Go to Settings → Source Database and set your connection first."}), 400
    if not dbx_host or not dbx_token:
        return jsonify({"tables": [], "error": "Databricks host/token not configured. Go to Settings → Databricks Workspace and set your host & token first."}), 400

    # ── Parse source / target identifiers ─────────────────────────────────
    src_parts = src.split(".")
    src_schema = src_parts[-1] if src_parts else "dbo"

    tgt_parts = tgt.split(".")
    if len(tgt_parts) < 2:
        return jsonify({"tables": [], "error": "Target must be catalog.schema format"}), 400
    tgt_catalog, tgt_schema = tgt_parts[0], tgt_parts[1]

    # ── Fetch metadata ────────────────────────────────────────────────────
    try:
        src_tables = _fetch_source_columns(source_cfg, src_schema)
    except Exception as e:
        return jsonify({"tables": [], "error": f"Source DB error: {str(e)}"}), 500

    try:
        tgt_tables = _fetch_target_columns(dbx_host, dbx_token, tgt_catalog, tgt_schema)
    except Exception as e:
        return jsonify({"tables": [], "error": f"Databricks error: {str(e)}"}), 500

    logger.info("Source tables (%d): %s", len(src_tables), list(src_tables.keys()))
    logger.info("Target tables (%d): %s", len(tgt_tables), list(tgt_tables.keys()))

    # ── Build comparison ──────────────────────────────────────────────────
    _TIER_PREFIXES = ("bronze_", "silver_", "gold_")

    def _strip_tier_prefix(name):
        lower = name.lower()
        for pfx in _TIER_PREFIXES:
            if lower.startswith(pfx):
                return name[len(pfx):]
        return name

    _seen_lower = {}
    for name in src_tables:
        _seen_lower[name.lower()] = name
    for k in tgt_tables:
        stripped = _strip_tier_prefix(k)
        low = stripped.lower()
        if low not in _seen_lower:
            _seen_lower[low] = stripped
    all_table_names = sorted(_seen_lower.values(), key=str.lower)

    tgt_lookup = {}
    for k in tgt_tables:
        stripped = _strip_tier_prefix(k).lower()
        tgt_lookup[stripped] = k

    src_lookup = {k.lower(): k for k in src_tables}

    tables = []
    for tbl in all_table_names:
        src_key = src_lookup.get(tbl.lower(), tbl)
        src_cols = {c["column"].lower(): c for c in src_tables.get(src_key, [])}
        tgt_key = tgt_lookup.get(tbl.lower(), tbl)
        tgt_cols_raw = tgt_tables.get(tgt_key)
        tgt_cols = {c["column"].lower(): c for c in (tgt_cols_raw or [])}

        diffs = []
        for col_lower, sc in src_cols.items():
            tc = tgt_cols.get(col_lower)
            if tc is None:
                diffs.append({
                    "table": tbl, "column": sc["column"],
                    "src_type": sc["data_type"], "tgt_type": "\u2014",
                    "src_nullable": sc["is_nullable"], "tgt_nullable": False,
                    "diff_type": "missing_col",
                })
            else:
                expected = _expected_dbx_type(sc["data_type"])
                actual = tc["data_type"]
                exp_base = expected.split("(")[0]
                act_base = actual.split("(")[0]
                if exp_base != act_base:
                    diff_type = "type_mismatch"
                elif sc["is_nullable"] != tc["is_nullable"]:
                    diff_type = "nullable_diff"
                else:
                    diff_type = "match"
                diffs.append({
                    "table": tbl, "column": sc["column"],
                    "src_type": sc["data_type"], "tgt_type": actual,
                    "src_nullable": sc["is_nullable"], "tgt_nullable": tc["is_nullable"],
                    "diff_type": diff_type,
                })

        for col_lower, tc in tgt_cols.items():
            if col_lower not in src_cols:
                diffs.append({
                    "table": tbl, "column": tc["column"],
                    "src_type": "\u2014", "tgt_type": tc["data_type"],
                    "src_nullable": False, "tgt_nullable": tc["is_nullable"],
                    "diff_type": "extra_col",
                })

        tables.append({
            "table": tbl,
            "src_cols": len(src_cols),
            "tgt_cols": len(tgt_cols),
            "matched": len([d for d in diffs if d["diff_type"] == "match"]),
            "diffs": diffs,
        })

    compared_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    for t in tables:
        for d in t.get("diffs", []):
            d["compared_at"] = compared_at

    result = {"tables": tables, "source": src, "target": tgt, "compared_at": compared_at}

    try:
        with open(SCHEMA_COMPARE_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, default=str)
    except Exception:
        pass

    return jsonify(result)


@schema_bp.route("/schema/compare/results", methods=["POST"])
@login_required
def save_schema_compare():
    """Save schema comparison results for later retrieval."""
    data = request.get_json(force=True)
    with open(SCHEMA_COMPARE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    return jsonify({"success": True})


@schema_bp.route("/recon/data", methods=["POST"])
@login_required
def get_recon_data():
    """Fetch reconciliation data from Databricks reconciliation table."""
    cfg = get_config()

    dbx_host  = cfg.get("databricks_host", "").rstrip("/")
    dbx_token = get_databricks_token()
    recon_cfg = cfg.get("reconciliation", {})
    recon_cat = recon_cfg.get("catalog", "reconciliation")
    recon_sch = recon_cfg.get("schema", "hr")
    recon_tbl = recon_cfg.get("table", "ReconcilationDetails")

    if not dbx_host or not dbx_token:
        return jsonify({"rows": [], "error": "Databricks host/token not configured."}), 400

    uc = UnityCatalogExecutor(dbx_host, dbx_token, recon_cat, recon_sch)

    wh_resp = uc.list_warehouses()
    warehouses = wh_resp.get("warehouses", [])
    wh_id = next((w["id"] for w in warehouses if w.get("state") == "RUNNING"), None)
    if not wh_id and warehouses:
        wh_id = warehouses[0].get("id")
    if not wh_id:
        return jsonify({"rows": [], "error": "No SQL Warehouse available."}), 400

    data = request.get_json(silent=True) or {}
    limit = min(int(data.get("limit", 500)), 5000)
    offset = max(int(data.get("offset", 0)), 0)

    fqn = f"`{recon_cat}`.`{recon_sch}`.`{recon_tbl}`"
    sql = f"SELECT * FROM {fqn} ORDER BY recon_timestamp DESC LIMIT {limit} OFFSET {offset}"
    result = uc._execute_statement(sql, wh_id, wait_timeout="50s")

    if result.get("error"):
        return jsonify({"rows": [], "error": result["error"]}), 500

    status = result.get("status", {}).get("state", "")
    if status in ("PENDING", "RUNNING"):
        stmt_id = result.get("statement_id", "")
        if stmt_id:
            result = uc._poll_statement(stmt_id)

    columns = [c.get("name", "") for c in result.get("manifest", {}).get("schema", {}).get("columns", [])]
    data_array = result.get("result", {}).get("data_array", [])

    rows = []
    for row in data_array:
        obj = {}
        for i, col in enumerate(columns):
            obj[col] = row[i] if i < len(row) else None
        rows.append(obj)

    return jsonify({"rows": rows, "columns": columns, "table": fqn})
