"""Discovery blueprint — scan, analyse, export SQL object inventory."""
import json, os, tempfile
from flask import Blueprint, request, jsonify, Response

from .auth import login_required
from log_config import get_logger
import discovery_agent as da
import data_profiler as dp

logger = get_logger(__name__)
discovery_bp = Blueprint("discovery", __name__, url_prefix="/api/v1")

# ── In-memory scan cache + file-based persistence (cross-worker safe) ──
_discovery_cache = {}  # { report: {...}, analyses: [...], graph: {...} }
_profile_cache = {}    # { table_name: profile_dict }
_CACHE_FILE = os.path.join(tempfile.gettempdir(), 'discovery_cache.json')

def _save_cache():
    """Persist cache to temp file so all gunicorn workers can access it."""
    try:
        with open(_CACHE_FILE, 'w') as f:
            json.dump(_discovery_cache, f, default=str)
    except Exception as e:
        logger.warning("Failed to persist discovery cache: %s", e)

def _load_cache():
    """Load cache from disk if in-memory is empty."""
    global _discovery_cache
    if _discovery_cache:
        return True
    try:
        if os.path.exists(_CACHE_FILE):
            with open(_CACHE_FILE, 'r') as f:
                _discovery_cache = json.load(f)
            return bool(_discovery_cache)
    except Exception as e:
        logger.warning("Failed to load discovery cache: %s", e)
    return False


@discovery_bp.route("/discovery/scan", methods=["POST"])
@login_required
def discovery_scan():
    """Run a discovery scan against live DB, static objects, or both."""
    global _discovery_cache
    try:
        data = request.get_json(silent=True) or {}
        source = data.get("source", "static")  # "live", "static", "both"
        source_config = data.get("source_config", {})

        analyses = []

        if source in ("static", "both"):
            analyses.extend(da.scan_static_objects())

        if source in ("live", "both"):
            src_type = source_config.get("source_type", "sqlserver")
            if src_type == "snowflake":
                if not source_config.get("account") or not source_config.get("username"):
                    return jsonify({"success": False, "error": "account and username required for Snowflake scan"}), 400
            elif src_type == "sharepoint":
                required = ("server", "tenant_id", "username")
                if not all(source_config.get(k) for k in required):
                    return jsonify({"success": False, "error": "site URL, Tenant ID and Client ID required for SharePoint scan"}), 400
            elif src_type == "api":
                if not source_config.get("server"):
                    return jsonify({"success": False, "error": "API Base URL required for API scan"}), 400
            else:
                required = ("server", "database", "username")
                if not all(source_config.get(k) for k in required):
                    return jsonify({"success": False, "error": "server, database, username required for live scan"}), 400
            schema_filter = data.get("schema_filter", "")
            live = da.scan_live_source(source_config, schema_filter=schema_filter)
            # Merge: avoid duplicates by name
            existing = {a["name"].lower() for a in analyses}
            for a in live:
                if a["name"].lower() not in existing:
                    analyses.append(a)
                    existing.add(a["name"].lower())

        # Sort by complexity score descending
        analyses.sort(key=lambda a: a["complexity_score"], reverse=True)

        graph = da.build_dependency_graph(analyses)
        report = da.generate_discovery_report(analyses, graph)

        _discovery_cache = {
            "report": report,
            "analyses": analyses,
            "graph": graph,
        }
        _save_cache()

        return jsonify({"success": True, "report": report})

    except Exception as e:
        logger.exception("Discovery scan failed")
        return jsonify({"success": False, "error": str(e)}), 500


@discovery_bp.route("/discovery/schemas", methods=["POST"])
@login_required
def discovery_schemas():
    """List schemas from the configured source database."""
    try:
        data = request.get_json(silent=True) or {}
        src_cfg = data.get("source_config", {})
        src_type = src_cfg.get("source_type", "sqlserver")

        from config_cache import get_source_password
        from keyvault_helper import is_masked
        _pw = src_cfg.get("password", "")
        if not _pw or is_masked(_pw):
            _pw = get_source_password(source_type=src_type)

        schemas = []
        if src_type == "sharepoint":
            # SharePoint has a single logical namespace — its lists
            schemas = ["default"]
        elif src_type == "api":
            # REST APIs expose endpoints, not schemas
            schemas = ["default"]
        elif src_type == "snowflake":
            import snowflake_connector
            account = src_cfg.get("account", "") or src_cfg.get("server", "")
            database = src_cfg.get("database", "")
            conn = snowflake_connector.get_snowflake_connection(
                account=account, username=src_cfg.get("username", ""),
                password=_pw, database=database,
                warehouse=src_cfg.get("warehouse", ""),
                role=src_cfg.get("role", ""),
            )
            cur = conn.cursor()
            cur.execute(f"SELECT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME != 'INFORMATION_SCHEMA' ORDER BY SCHEMA_NAME")
            schemas = [r[0] for r in cur.fetchall()]
            cur.close(); conn.close()
        else:
            from sql_pool import get_connection
            conn = get_connection(
                source_type=src_type, server=src_cfg.get("server", ""),
                database=src_cfg.get("database", ""), username=src_cfg.get("username", ""),
                password=_pw,
            )
            cur = conn.cursor()
            cur.execute("SELECT name FROM sys.schemas WHERE schema_id < 16384 AND name NOT IN ('guest','INFORMATION_SCHEMA','sys') ORDER BY name")
            schemas = [r[0] for r in cur.fetchall()]
            cur.close(); conn.close()

        return jsonify({"success": True, "schemas": schemas})
    except Exception as e:
        logger.exception("Schema listing failed")
        return jsonify({"success": False, "error": str(e)}), 500


@discovery_bp.route("/discovery/results", methods=["GET"])
@login_required
def discovery_results():
    """Return cached scan results."""
    _load_cache()
    if not _discovery_cache:
        return jsonify({"success": False, "error": "No scan results. Run a scan first."})
    return jsonify({"success": True, "report": _discovery_cache["report"]})


@discovery_bp.route("/discovery/object/<name>", methods=["GET"])
@login_required
def discovery_object(name):
    """Return detailed analysis for a single object."""
    _load_cache()
    if not _discovery_cache:
        return jsonify({"success": False, "error": "No scan results."})
    for a in _discovery_cache.get("analyses", []):
        if a["name"] == name:
            return jsonify({"success": True, "object": a})
    return jsonify({"success": False, "error": f"Object '{name}' not found"}), 404


@discovery_bp.route("/discovery/dependency-graph", methods=["GET"])
@login_required
def discovery_graph():
    """Return dependency graph JSON for D3.js."""
    _load_cache()
    if not _discovery_cache:
        return jsonify({"success": False, "error": "No scan results."})
    return jsonify({"success": True, "graph": _discovery_cache.get("graph", {})})


@discovery_bp.route("/discovery/export/html", methods=["GET"])
@login_required
def discovery_export_html():
    """Download self-contained HTML report."""
    _load_cache()
    if not _discovery_cache:
        return jsonify({"success": False, "error": "No scan results."})
    html = da.generate_html_report(_discovery_cache["report"])
    return Response(
        html,
        mimetype="text/html",
        headers={"Content-Disposition": "attachment; filename=discovery_report.html"},
    )


@discovery_bp.route("/discovery/export/bom", methods=["GET"])
@login_required
def discovery_export_bom():
    """Download BOM as CSV."""
    _load_cache()
    if not _discovery_cache:
        return jsonify({"success": False, "error": "No scan results."})
    csv_str = da.generate_bom_csv(_discovery_cache.get("analyses", []))
    return Response(
        csv_str,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=discovery_bom.csv"},
    )


# ─────────────────────────────────────────────────────────────────────────────
#  DATA PROFILE endpoints
# ─────────────────────────────────────────────────────────────────────────────
@discovery_bp.route("/discovery/profile/tables", methods=["POST"])
@login_required
def discovery_profile_tables():
    """List tables that can be profiled (demo or live)."""
    try:
        data = request.get_json(silent=True) or {}
        mode = data.get("mode", "demo")
        src_cfg = data.get("source_config", {}) or {}
        schema_filter = data.get("schema_filter", "")
        tables = dp.list_profilable_tables(source_config=src_cfg, mode=mode, schema_filter=schema_filter)
        return jsonify({"success": True, "tables": tables, "mode": mode})
    except Exception as e:
        logger.exception("Profile table list failed")
        return jsonify({"success": False, "error": str(e)}), 500


@discovery_bp.route("/discovery/profile/<table>", methods=["POST"])
@login_required
def discovery_profile_table(table):
    """Return column-level profile for a single table."""
    try:
        data = request.get_json(silent=True) or {}
        mode = data.get("mode", "demo")
        src_cfg = data.get("source_config", {}) or {}

        if mode == "live":
            if src_cfg.get("source_type") == "snowflake":
                if not src_cfg.get("account") or not src_cfg.get("database"):
                    return jsonify({"success": False, "error": "account and database required for Snowflake profile"}), 400
            else:
                if not src_cfg.get("server") or not src_cfg.get("database"):
                    return jsonify({"success": False, "error": "server and database required for live profile"}), 400
            schema = data.get("schema", "dbo")
            prof = dp.profile_table_live(src_cfg, table, schema=schema)
        else:
            prof = dp.profile_table_demo(table)
            if not prof:
                return jsonify({"success": False, "error": f"Table '{table}' not in demo set"}), 404

        _profile_cache[table] = prof
        return jsonify({"success": True, "profile": prof})
    except Exception as e:
        logger.exception("Profile failed")
        return jsonify({"success": False, "error": str(e)}), 500


@discovery_bp.route("/discovery/profile/<table>/rules", methods=["GET"])
@login_required
def discovery_profile_rules(table):
    """Return the flattened list of suggested DQ rules for a profiled table."""
    prof = _profile_cache.get(table)
    if not prof:
        return jsonify({"success": False, "error": "No profile cached — profile the table first"}), 404
    rules = []
    for col in prof.get("columns", []):
        rules.extend(col.get("suggested_rules", []))
    return jsonify({"success": True, "rules": rules, "table": table})
