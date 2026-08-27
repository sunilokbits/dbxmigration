"""Migrate blueprint — data migration endpoints."""
from flask import Blueprint, request, jsonify
import uuid, threading
from datetime import datetime

from .auth import login_required
from log_config import get_logger
from data_migrator import DataMigrator, MIGRATION_JOBS, _build_conn_str
from unity_catalog_executor import UnityCatalogExecutor
from config_cache import get_source_password, get_databricks_token
from keyvault_helper import is_masked
import persistence as db

logger = get_logger(__name__)
migrate_bp = Blueprint("migrate", __name__, url_prefix="/api/v1")


@migrate_bp.route("/migrate/list-tables", methods=["POST"])
@login_required
def migrate_list_tables():
    try:
        d = request.get_json()
        source_type = d.get("source_type", "sqlserver")
        server = d.get("server", "").strip()
        database = d.get("database", "").strip()
        username = d.get("username", "").strip()
        password = d.get("password", "")
        if not password or is_masked(password):
            password = get_source_password()
        if not all([server, database, username]):
            return jsonify({"success": False, "error": "server, database and username required"}), 400
        conn_str = _build_conn_str(source_type, server, database, username, password)
        migrator = DataMigrator(conn_str, "http://placeholder", "placeholder")
        tables = migrator.list_source_tables()
        return jsonify({"success": True, "tables": tables, "total": len(tables)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@migrate_bp.route("/migrate/describe-table", methods=["POST"])
@login_required
def migrate_describe_table():
    try:
        d = request.get_json()
        source_type = d.get("source_type", "sqlserver")
        server = d.get("server", "").strip()
        database = d.get("database", "").strip()
        username = d.get("username", "").strip()
        password = d.get("password", "")
        if not password or is_masked(password):
            password = get_source_password()
        schema = d.get("schema", "dbo").strip()
        table = d.get("table", "").strip()
        conn_str = _build_conn_str(source_type, server, database, username, password)
        migrator = DataMigrator(conn_str, "http://placeholder", "placeholder")
        desc = migrator.describe_source_table(schema, table)
        return jsonify({"success": True, **desc})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@migrate_bp.route("/migrate/warehouses", methods=["POST"])
@login_required
def migrate_list_warehouses():
    try:
        d = request.get_json()
        host = d.get("host", "").strip()
        token = d.get("token", "").strip()
        if not token or is_masked(token):
            token = get_databricks_token()
        if not host or not token:
            return jsonify({"success": False, "error": "host and token required"}), 400
        uc = UnityCatalogExecutor(host, token)
        return jsonify(uc.list_warehouses())
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@migrate_bp.route("/migrate/start", methods=["POST"])
@login_required
def migrate_start():
    try:
        d = request.get_json()
        source_type = d.get("source_type", "sqlserver")
        server = d.get("server", "").strip()
        database = d.get("database", "").strip()
        username = d.get("username", "").strip()
        password = d.get("password", "")
        if not password or is_masked(password):
            password = get_source_password()
        dbx_host = (d.get("host") or d.get("dbx_host") or "").strip()
        dbx_token = (d.get("token") or d.get("dbx_token") or "").strip()
        if not dbx_token or is_masked(dbx_token):
            dbx_token = get_databricks_token()
        catalog = d.get("catalog", "main").strip()
        schema = d.get("schema", "default").strip()
        warehouse_id = d.get("warehouse_id", "").strip()
        tables = d.get("tables", [])
        max_workers = int(d.get("max_workers", 3))
        load_mode = d.get("load_mode", "full").strip()

        if not all([server, database, username, dbx_host, dbx_token, warehouse_id, tables]):
            return jsonify({"success": False, "error": "Missing required fields"}), 400

        job_id = uuid.uuid4().hex
        job_data = {
            "status": "queued", "started_at": datetime.now().isoformat(),
            "total": len(tables), "done": 0, "failed": 0,
            "results": [], "logs": {}, "load_mode": load_mode,
        }
        MIGRATION_JOBS[job_id] = job_data
        db.save_job(job_id, job_data)

        conn_str = _build_conn_str(source_type, server, database, username, password)
        migrator = DataMigrator(conn_str, dbx_host, dbx_token, catalog, schema)

        def _bg():
            try:
                migrator.migrate_tables_parallel(tables, warehouse_id, job_id, max_workers, load_mode)
            except Exception:
                logger.exception("Background migration failed for job %s", job_id)
                job = MIGRATION_JOBS.get(job_id, {})
                job["status"] = "failed"
                job["finished_at"] = datetime.now().isoformat()
            finally:
                db.save_job(job_id, MIGRATION_JOBS.get(job_id, {}))

        threading.Thread(target=_bg, daemon=True).start()
        return jsonify({"success": True, "job_id": job_id,
                        "message": f"Migration started for {len(tables)} tables"})
    except Exception as e:
        logger.exception("Failed to start migration")
        return jsonify({"success": False, "error": str(e)}), 500


@migrate_bp.route("/migrate/status/<job_id>", methods=["GET"])
@login_required
def migrate_status(job_id):
    job = MIGRATION_JOBS.get(job_id)
    if not job:
        # Try loading from SQLite
        job = db.load_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "Job not found"}), 404
    raw_logs = job.get("logs", [])
    if isinstance(raw_logs, dict):
        flat = []
        for tname, lines in raw_logs.items():
            flat.extend(f"[{tname}] {l}" for l in (lines or []))
        raw_logs = flat
    out = {k: v for k, v in job.items() if k != "logs"}
    out["logs"] = raw_logs
    return jsonify({"success": True, **out})
