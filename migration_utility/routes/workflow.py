"""Workflow blueprint — AI workflow manager endpoints."""
from flask import Blueprint, request, jsonify
import json

from .auth import login_required
from log_config import get_logger
from config_cache import get_config, get_databricks_token, get_source_password
from audit import log_action
import workflow_manager as wfm
from data_migrator import DataMigrator, _build_conn_str
from keyvault_helper import is_masked

logger = get_logger(__name__)
workflow_bp = Blueprint("workflow", __name__, url_prefix="/api/v1")


def _sp_item_count(description: str) -> int:
    """Extract '(N items)' from a SharePoint object description string."""
    import re
    m = re.search(r"\((\d+) items?\)", description or "")
    return int(m.group(1)) if m else 0


@workflow_bp.route("/workflow/list-tables", methods=["POST"])
@login_required
def wf_list_tables():
    """List source SQL Server tables for Pipeline Studio.

    Uses sql_pool.get_connection() which transparently falls back to pymssql
    when pyodbc/libodbc is unavailable (non-Docker Databricks Apps runtime).
    This avoids the 'NoneType has no attribute drivers' crash caused by
    data_migrator._build_conn_str() calling pyodbc.drivers() on a None object.
    """
    try:
        d = request.get_json()
        source_type = d.get("source_type", "sqlserver")
        server = d.get("server", "").strip()
        database = d.get("database", "").strip()
        username = d.get("username", "").strip()
        password = d.get("password", "")
        if not password or is_masked(password):
            password = get_source_password(source_type=source_type)
        # ── Snowflake path ──
        if source_type == "snowflake":
            account = (d.get("account") or "").strip()
            warehouse = (d.get("warehouse") or "").strip()
            role = (d.get("role") or "").strip()
            if not account or not username:
                return jsonify({"success": False, "error": "account and username required for Snowflake"}), 400
            try:
                from snowflake_connector import get_snowflake_connection
                conn = get_snowflake_connection(
                    account=account, username=username, password=password,
                    database=database, warehouse=warehouse, role=role
                )
                schema_filter = (d.get("schema_filter") or "").strip()
                cursor = conn.cursor()
                _sf_where = f"AND TABLE_SCHEMA = '{schema_filter}'" if schema_filter else "AND TABLE_SCHEMA != 'INFORMATION_SCHEMA'"
                cursor.execute(f"""
                    SELECT TABLE_SCHEMA, TABLE_NAME,
                           (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS c
                            WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME) AS col_count,
                           ROW_COUNT AS row_estimate
                    FROM INFORMATION_SCHEMA.TABLES t
                    WHERE TABLE_TYPE = 'BASE TABLE'
                      AND TABLE_CATALOG = CURRENT_DATABASE()
                      {_sf_where}
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                """)
                tables = [
                    {
                        "schema":       row[0],
                        "table":        row[1],
                        "full_name":    f"{row[0]}.{row[1]}",
                        "col_count":    row[2],
                        "row_estimate": row[3],
                    }
                    for row in cursor.fetchall()
                ]
                conn.close()
                return jsonify({"success": True, "tables": tables, "total": len(tables)})
            except Exception as e:
                return jsonify({"success": False, "error": str(e)}), 500

        # ── SharePoint path — lists surface as tables ──
        if source_type == "sharepoint":
            site_url = server  # server field holds the site URL
            tenant_id = (d.get("tenant_id") or "").strip()
            client_id = username  # username field holds the Azure AD Client ID
            if not site_url or not tenant_id or not client_id:
                return jsonify({"success": False, "error": "site URL, Tenant ID and Client ID required for SharePoint"}), 400
            try:
                from sharepoint_connector import load_objects as sp_load
                result = sp_load(server=site_url, username=client_id, password=password,
                                 database=database, tenant_id=tenant_id)
                if not result.get("success"):
                    return jsonify({"success": False, "error": result.get("error", "SharePoint load failed")}), 500
                tables = [
                    {
                        "schema":       "sharepoint",
                        "table":        o["name"],
                        "full_name":    f"sharepoint.{o['name']}",
                        "col_count":    0,
                        "row_estimate": _sp_item_count(o.get("description", "")),
                    }
                    for o in result.get("grouped", {}).get("view", [])
                ]
                return jsonify({"success": True, "tables": tables, "total": len(tables)})
            except Exception as e:
                return jsonify({"success": False, "error": str(e)}), 500

        # ── Generic REST API path — GET endpoints surface as tables ──
        if source_type == "api":
            base_url = server  # server field holds the base URL
            if not base_url:
                return jsonify({"success": False, "error": "API Base URL required"}), 400
            try:
                from api_source_client import load_objects as api_load
                result = api_load(server=base_url, username=username, password=password,
                                  database=database,
                                  auth_type=(d.get("api_auth_type") or "none").strip().lower(),
                                  api_key_header=(d.get("api_key_header") or "").strip())
                if not result.get("success"):
                    return jsonify({"success": False, "error": result.get("error", "API load failed")}), 500
                tables = [
                    {
                        "schema":       "api",
                        "table":        o["name"],
                        "full_name":    f"api.{o['name']}",
                        "col_count":    0,
                        "row_estimate": 0,
                    }
                    for o in result.get("grouped", {}).get("view", [])
                ]
                return jsonify({"success": True, "tables": tables, "total": len(tables)})
            except Exception as e:
                return jsonify({"success": False, "error": str(e)}), 500

        if not all([server, database, username]):
            return jsonify({"success": False, "error": "server, database and username required"}), 400

        from sql_pool import get_connection as _get_conn
        conn = _get_conn(source_type, server, database, username, password)
        cursor = conn.cursor()
        sql = """
            SELECT t.TABLE_SCHEMA, t.TABLE_NAME,
                   (SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA
                      AND c.TABLE_NAME   = t.TABLE_NAME) AS col_count,
                   ISNULL(p.rows, 0) AS row_estimate
            FROM   INFORMATION_SCHEMA.TABLES t
            LEFT JOIN sys.partitions p
                   ON p.object_id = OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME)
                  AND p.index_id IN (0, 1)
            WHERE  t.TABLE_TYPE = 'BASE TABLE'
            GROUP  BY t.TABLE_SCHEMA, t.TABLE_NAME, p.rows
            ORDER  BY t.TABLE_SCHEMA, t.TABLE_NAME
        """
        cursor.execute(sql)
        tables = [
            {
                "schema":        row[0],
                "table":         row[1],
                "full_name":     f"{row[0]}.{row[1]}",
                "col_count":     row[2],
                "row_estimate":  row[3],
            }
            for row in cursor.fetchall()
        ]
        try:
            conn.close()
        except Exception:
            pass
        return jsonify({"success": True, "tables": tables, "total": len(tables)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@workflow_bp.route("/workflow/metadata/init", methods=["POST"])
@login_required
def wf_metadata_init():
    d = request.get_json() or {}
    token = d.get("token", "").strip()
    if not token or is_masked(token):
        token = get_databricks_token()
    return jsonify(wfm.init_metadata_flow(
        host=d.get("host", "").strip(), token=token,
        catalog=d.get("catalog", "main").strip(), schema=d.get("schema", "default").strip(),
        warehouse_id=d.get("warehouse_id", "").strip(),
    ))


@workflow_bp.route("/workflow/auto-init", methods=["POST"])
@login_required
def wf_auto_init():
    try:
        cfg = get_config()
        if not cfg:
            return jsonify({"success": False, "reason": "no_config"})
        host = (cfg.get("databricks_host") or "").strip().rstrip("/")
        token = get_databricks_token()
        catalog = (cfg.get("metadata_catalog") or "").strip()
        schema = (cfg.get("metadata_schema") or "").strip()
        if not host or not token:
            return jsonify({"success": False, "reason": "no_credentials"})
        if not catalog or not schema:
            return jsonify({"success": False, "reason": "no_metadata_location"})
        if wfm._metadata_initialized and wfm._dbr_host == host and wfm._dbr_catalog == catalog:
            return jsonify({"success": True, "already_initialized": True,
                            "catalog": catalog, "schema": schema})
        result = wfm.init_metadata_flow(host=host, token=token, catalog=catalog, schema=schema)
        if result.get("success"):
            wfm.load_metadata_from_dbr()
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@workflow_bp.route("/workflow/metadata/status", methods=["GET"])
@login_required
def wf_metadata_status():
    return jsonify(wfm.get_metadata_status())


@workflow_bp.route("/workflow/metadata/load", methods=["POST"])
@login_required
def wf_metadata_load():
    return jsonify(wfm.load_metadata_from_dbr())


@workflow_bp.route("/workflow/metadata/sync", methods=["POST"])
@login_required
def wf_metadata_sync():
    # Fix 3: async by default — dispatch background task, return task_id.
    # Callers wanting blocking behavior can pass ?mode=sync.
    mode = (request.args.get("mode") or "").lower()
    if mode == "sync":
        return jsonify(wfm.full_sync_to_dbr())
    return jsonify(wfm.start_full_sync_to_dbr())


@workflow_bp.route("/workflow/metadata/sync-status/<task_id>", methods=["GET"])
@login_required
def wf_metadata_sync_status(task_id):
    # Fix 3: poll endpoint for background full-sync tasks.
    return jsonify(wfm.get_full_sync_status(task_id))


@workflow_bp.route("/workflow/metadata/save-sources", methods=["POST"])
@login_required
def wf_metadata_save_sources():
    d = request.get_json() or {}
    return jsonify(wfm.sync_source_tables_to_dbr(
        tables=d.get("tables", []), source_config=d.get("source_config", {}),
    ))


@workflow_bp.route("/workflow/notebooks/deploy", methods=["POST"])
@login_required
def wf_deploy_notebooks():
    d = request.get_json() or {}
    token = d.get("token", "").strip()
    if not token or is_masked(token):
        token = get_databricks_token()
    return jsonify(wfm.deploy_metadata_notebooks(
        host=d.get("host", "").strip(), token=token,
        catalog=d.get("catalog", "main").strip(), schema=d.get("schema", "default").strip(),
        landing_path=d.get("landing_path", "/mnt/landing").strip(),
        workspace_path=d.get("workspace_path", "/Shared/MetadataPipeline").strip(),
        pipeline_mode=d.get("pipeline_mode", "standard").strip(),
        cdc_mode=d.get("cdc_mode", "watermark").strip(),
        primary_keys=d.get("primary_keys", []),
        recon_catalog=d.get("recon_catalog", "reconciliation").strip(),
        recon_schema=d.get("recon_schema", "hr").strip(),
        recon_table=d.get("recon_table", "ReconcilationDetails").strip(),
        log_catalog=d.get("log_catalog", "logging").strip(),
        log_schema=d.get("log_schema", "hr").strip(),
        log_table=d.get("log_table", "ExecutionLog").strip(),
        recon_location=d.get("recon_location", "").strip(),
        log_location=d.get("log_location", "").strip(),
    ))


@workflow_bp.route("/workflow/notebooks/status", methods=["GET"])
@login_required
def wf_notebook_status():
    return jsonify(wfm.get_notebook_status())


@workflow_bp.route("/workflow/notebooks/generate", methods=["POST"])
@login_required
def wf_generate_notebooks():
    """Generate metadata notebooks code without deploying — for DevOps push."""
    from metadata_notebooks import generate_metadata_notebooks
    d = request.get_json() or {}
    cfg = get_config()
    catalogs = cfg.get("catalogs", {})
    default_catalog = list(catalogs.keys())[0] if catalogs else "main"
    gen_result = generate_metadata_notebooks(
        catalog=d.get("catalog", default_catalog).strip(),
        schema=d.get("schema", "default").strip(),
        landing_path=d.get("landing_path", "/mnt/landing").strip(),
        workspace_path=d.get("workspace_path", "/Shared/MetadataPipeline").strip(),
        pipeline_mode=d.get("pipeline_mode", "standard").strip(),
        cdc_mode=d.get("cdc_mode", "watermark").strip(),
        primary_keys=d.get("primary_keys", []),
        recon_catalog=d.get("recon_catalog", "reconciliation").strip(),
        recon_schema=d.get("recon_schema", "hr").strip(),
        recon_table=d.get("recon_table", "ReconcilationDetails").strip(),
        log_catalog=d.get("log_catalog", "logging").strip(),
        log_schema=d.get("log_schema", "hr").strip(),
        log_table=d.get("log_table", "ExecutionLog").strip(),
        recon_location=d.get("recon_location", "").strip(),
        log_location=d.get("log_location", "").strip(),
    )
    return jsonify(gen_result)


@workflow_bp.route("/workflow/dq-checks", methods=["GET"])
@login_required
def wf_dq_checks():
    mode = request.args.get("mode", "standard")
    checks = {
        "standard": {
            "bronze": [
                {"id": "DQ-01", "name": "Empty File Detection", "action": "skip", "desc": "Skip Bronze write when landing has 0 rows"},
                {"id": "DQ-02", "name": "Null-Key Detection", "action": "quarantine", "desc": "Flag rows where ALL data columns are null"},
                {"id": "DQ-03", "name": "Duplicate Detection", "action": "warn", "desc": "Count exact-match duplicate rows"},
                {"id": "DQ-04", "name": "Schema Drift Detection", "action": "warn", "desc": "Detect new or missing columns vs existing table"},
                {"id": "DQ-05", "name": "Quarantine Flagging", "action": "flag", "desc": "Mark invalid rows with __is_quarantined=true"},
            ],
            "silver": [
                {"id": "DQ-01", "name": "Quarantine Filter", "action": "drop", "desc": "Exclude rows flagged as quarantined in Bronze"},
                {"id": "DQ-02", "name": "All-Null Removal", "action": "drop", "desc": "Drop records where all data columns are null"},
                {"id": "DQ-03", "name": "Per-Column Null %", "action": "warn", "desc": "Flag columns exceeding 80% null threshold"},
                {"id": "DQ-04", "name": "Deduplication", "action": "drop", "desc": "Remove exact duplicate rows on data columns"},
                {"id": "DQ-05", "name": "String Trimming", "action": "fix", "desc": "Trim whitespace from all string columns"},
                {"id": "DQ-06", "name": "Empty→NULL", "action": "fix", "desc": "Convert empty strings to NULL values"},
                {"id": "DQ-07", "name": "Row Count Anomaly", "action": "warn", "desc": "Alert if row count changes >50% vs last run"},
            ],
            "common": [
                {"id": "RST", "name": "Restore Points", "action": "safety", "desc": "Auto version snapshot before each write"},
                {"id": "RBK", "name": "Auto Rollback", "action": "safety", "desc": "Revert table to previous version on write failure"},
                {"id": "MTR", "name": "DQ Metrics Table", "action": "track", "desc": "__dq_metrics with score, nulls, dupes, drift per run"},
            ],
        },
        "dlt": {
            "bronze": [
                {"id": "dq01", "name": "Valid Landing TS", "action": "expect_or_drop", "desc": "__landing_ts IS NOT NULL"},
                {"id": "dq02", "name": "Source System Present", "action": "expect", "desc": "__source_system IS NOT NULL"},
                {"id": "dq03", "name": "Batch ID Present", "action": "expect", "desc": "__batch_id IS NOT NULL"},
                {"id": "dq04", "name": "Data Freshness", "action": "expect", "desc": "Landing timestamp within 7 days"},
                {"id": "dq05", "name": "Not All Null", "action": "expect", "desc": "Not all audit columns are null simultaneously"},
            ],
            "silver": [
                {"id": "dq01", "name": "Valid Bronze TS", "action": "expect_or_drop", "desc": "__bronze_ts IS NOT NULL"},
                {"id": "dq02", "name": "Not Quarantined", "action": "expect_or_drop", "desc": "__is_quarantined = false"},
                {"id": "dq03", "name": "Source Table Present", "action": "expect", "desc": "__source_table IS NOT NULL"},
                {"id": "dq04", "name": "Bronze Freshness", "action": "expect", "desc": "Bronze timestamp within 7 days"},
                {"id": "dq05", "name": "Source Not Empty", "action": "expect", "desc": "Source table name is non-empty string"},
            ],
            "common": [
                {"id": "AL", "name": "Auto Loader", "action": "built-in", "desc": "Streaming ingestion with schema evolution"},
                {"id": "DD", "name": "Deduplication", "action": "built-in", "desc": "dropDuplicates() on all columns in Silver"},
                {"id": "TR", "name": "String Trimming", "action": "built-in", "desc": "Whitespace normalization on string columns"},
                {"id": "EL", "name": "SDP Event Log", "action": "built-in", "desc": "All expectations auto-tracked in event log"},
            ],
        },
    }
    return jsonify({"success": True, "mode": mode, "checks": checks.get(mode, checks["standard"])})


@workflow_bp.route("/workflow/pipelines/<group_id>/run-databricks", methods=["POST"])
@login_required
def wf_run_on_databricks(group_id):
    d = request.get_json() or {}
    token = d.get("token", "").strip()
    if not token or is_masked(token):
        token = get_databricks_token()
    password = d.get("password", "")
    if not password or is_masked(password):
        password = get_source_password()
    result = wfm.run_pipeline_on_databricks(
        group_id=group_id, host=d.get("host", "").strip(),
        token=token, cluster_id=d.get("cluster_id", "").strip(),
        load_type=d.get("load_type", "").strip(), password=password,
        workspace_path=d.get("workspace_path", "").strip(),
        catalog=d.get("catalog", "").strip(), schema=d.get("schema", "").strip(),
        landing_path=d.get("landing_path", "/mnt/landing").strip(),
        recon_catalog=d.get("recon_catalog", "reconciliation").strip(),
        recon_schema=d.get("recon_schema", "hr").strip(),
        recon_table=d.get("recon_table", "ReconcilationDetails").strip(),
        log_catalog=d.get("log_catalog", "logging").strip(),
        log_schema=d.get("log_schema", "hr").strip(),
        log_table=d.get("log_table", "ExecutionLog").strip(),
    )
    if result.get("success"):
        log_action("pipeline_run_databricks", "pipeline", group_id,
                   {"load_type": d.get("load_type", ""), "dbr_run_id": result.get("run_id")})
    else:
        logger.warning("run-databricks failed for group '%s': %s", group_id, result.get('error') or result.get('message'))
    return jsonify(result)


@workflow_bp.route("/workflow/stats", methods=["GET"])
@login_required
def wf_stats():
    return jsonify(wfm.get_dashboard_stats())


@workflow_bp.route("/workflow/clusters", methods=["GET"])
@login_required
def wf_list_clusters():
    host = request.args.get("host", "").strip()
    token = request.args.get("token", "").strip()
    if not token or is_masked(token):
        token = get_databricks_token()
    if not host or not token:
        return jsonify({"success": False, "error": "host and token required"})
    try:
        from databricks_connector import DatabricksConnector
        connector = DatabricksConnector(host, token)
        result = connector.list_clusters()
        # If token was rejected (403), clear cache and retry with fresh token from KV
        if not result.get("success") and "403" in str(result.get("message", "")):
            from keyvault_helper import clear_cache
            clear_cache()
            token = get_databricks_token()
            if token:
                connector = DatabricksConnector(host, token)
                result = connector.list_clusters()
        # Normalize error key for frontend
        if not result.get("success") and "message" in result and "error" not in result:
            result["error"] = result.pop("message")

        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@workflow_bp.route("/workflow/clusters/start", methods=["POST"])
@login_required
def wf_start_cluster():
    d = request.get_json() or {}
    host = d.get("host", "").strip()
    token = d.get("token", "").strip()
    cluster_id = d.get("cluster_id", "").strip()
    if not token or is_masked(token):
        token = get_databricks_token()
    if not host or not token or not cluster_id:
        return jsonify({"success": False, "error": "host, token, and cluster_id required"})
    try:
        from databricks_connector import DatabricksConnector
        connector = DatabricksConnector(host, token)
        return jsonify(connector.start_cluster(cluster_id))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@workflow_bp.route("/workflow/create-pipeline", methods=["POST"])
@login_required
def wf_create_pipeline():
    d = request.get_json() or {}
    result = wfm.create_pipeline_for_table(
        table_schema=d.get("table_schema", "dbo"), table_name=d.get("table_name", ""),
        load_type=d.get("load_type", "full"), watermark_column=d.get("watermark_column", ""),
        source_config=d.get("source_config"), target_config=d.get("target_config"),
        pipeline_mode=d.get("pipeline_mode", "standard"), cdc_mode=d.get("cdc_mode", "watermark"),
        primary_keys=d.get("primary_keys", []), use_layer_mapping=bool(d.get("use_layer_mapping", False)),
    )
    if result.get("success"):
        log_action("pipeline_created", "pipeline", d.get("table_name", ""),
                   {"load_type": d.get("load_type", "full"), "mode": d.get("pipeline_mode", "standard")})
    return jsonify(result)


@workflow_bp.route("/workflow/create-pipelines-bulk", methods=["POST"])
@login_required
def wf_create_pipelines_bulk():
    d = request.get_json() or {}
    result = wfm.create_pipelines_bulk(
        tables=d.get("tables", []), source_config=d.get("source_config"),
        target_config=d.get("target_config"), pipeline_mode=d.get("pipeline_mode", "standard"),
        cdc_mode=d.get("cdc_mode", "watermark"), primary_keys=d.get("primary_keys", []),
        use_layer_mapping=bool(d.get("use_layer_mapping", False)),
    )
    if result.get("success"):
        log_action("pipelines_bulk_created", "pipeline", "",
                   {"count": len(d.get("tables", [])), "mode": d.get("pipeline_mode", "standard")})
    return jsonify(result)


@workflow_bp.route("/workflow/pipelines", methods=["GET"])
@login_required
def wf_list_pipelines():
    return jsonify(wfm.list_pipeline_groups_live())


@workflow_bp.route("/workflow/jobs", methods=["GET"])
@login_required
def wf_list_jobs():
    return jsonify(wfm.list_jobs(
        group_id=request.args.get("group_id"), stage=request.args.get("stage"),
        status=request.args.get("status"),
    ))


@workflow_bp.route("/workflow/jobs/<job_id>", methods=["GET"])
@login_required
def wf_get_job(job_id):
    return jsonify(wfm.get_job(job_id))


@workflow_bp.route("/workflow/jobs/<job_id>", methods=["PUT"])
@login_required
def wf_update_job(job_id):
    d = request.get_json() or {}
    return jsonify(wfm.update_job(job_id, d))


@workflow_bp.route("/workflow/jobs/<job_id>", methods=["DELETE"])
@login_required
def wf_delete_job(job_id):
    result = wfm.delete_job(job_id)
    if result.get("success"):
        log_action("job_deleted", "job", job_id)
    return jsonify(result)


@workflow_bp.route("/workflow/jobs/history", methods=["GET"])
@login_required
def wf_job_history():
    table_name = request.args.get("table_name", "").strip()
    try:
        if not wfm._metadata_initialized:
            return jsonify({"success": False, "error": "MetadataFlow not initialized"})
        where = ""
        if table_name:
            where = f" WHERE table_name = {wfm._esc(table_name)}"
        sql = f"SELECT * FROM {wfm._fqn(wfm.TBL_JOBS_HISTORY)}{where} ORDER BY archived_at DESC"
        r = wfm._exec_sql(sql)
        state = r.get("status", {}).get("state", "")
        if state != "SUCCEEDED":
            return jsonify({"success": False, "error": "Query failed", "detail": r})
        cols = [c.get("name", "") for c in r.get("result", {}).get("schema", {}).get("columns", [])]
        rows = r.get("result", {}).get("data_array", [])
        history = [dict(zip(cols, row)) for row in rows]
        return jsonify({"success": True, "history": history, "total": len(history)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@workflow_bp.route("/workflow/pipelines/<group_id>", methods=["DELETE"])
@login_required
def wf_delete_pipeline(group_id):
    result = wfm.delete_pipeline_group(group_id)
    if result.get("success"):
        log_action("pipeline_deleted", "pipeline", group_id)
    return jsonify(result)


@workflow_bp.route("/workflow/jobs/<job_id>/run", methods=["POST"])
@login_required
def wf_run_job(job_id):
    d = request.get_json() or {}
    return jsonify(wfm.run_job(job_id, force_full=d.get("force_full", False)))


@workflow_bp.route("/workflow/pipelines/<group_id>/run", methods=["POST"])
@login_required
def wf_run_pipeline(group_id):
    d = request.get_json() or {}
    return jsonify(wfm.run_pipeline_group(group_id, force_full=d.get("force_full", False)))


@workflow_bp.route("/workflow/pipelines/<group_id>/rerun", methods=["POST"])
@login_required
def wf_rerun_pipeline(group_id):
    return jsonify(wfm.rerun_from_failure(group_id))


@workflow_bp.route("/workflow/runs/<run_id>", methods=["GET"])
@login_required
def wf_get_run(run_id):
    return jsonify(wfm.get_run_status(run_id))


@workflow_bp.route("/workflow/runs/<run_id>/databricks-output", methods=["POST"])
@login_required
def wf_get_dbr_output(run_id):
    body = request.get_json(force=True)
    host = (body.get("host") or "").strip()
    token = (body.get("token") or "").strip()
    if not token or is_masked(token):
        token = get_databricks_token()
    if not host or not token:
        return jsonify({"success": False, "message": "Databricks host and token required"}), 400
    run_info = wfm.get_run_status(run_id)
    if not run_info.get("success"):
        return jsonify({"success": False, "message": "Run not found"}), 404
    dbr_run_id = run_info.get("run", {}).get("dbr_run_id")
    if not dbr_run_id:
        return jsonify({"success": False, "message": "No Databricks run ID associated with this run"}), 404
    from databricks_connector import DatabricksConnector
    conn = DatabricksConnector(host, token)
    return jsonify(conn.get_run_output(int(dbr_run_id)))


@workflow_bp.route("/workflow/runs", methods=["GET"])
@login_required
def wf_list_runs():
    return jsonify(wfm.list_runs(
        job_id=request.args.get("job_id"), group_id=request.args.get("group_id"),
        status=request.args.get("status"), limit=request.args.get("limit", 50, type=int),
    ))


@workflow_bp.route("/workflow/jobs/add", methods=["POST"])
@login_required
def wf_add_custom_job():
    d = request.get_json() or {}
    return jsonify(wfm.add_custom_job(
        job_name=d.get("job_name", ""), stage=d.get("stage", "extract"),
        table_schema=d.get("table_schema", "dbo"), table_name=d.get("table_name", ""),
        load_type=d.get("load_type", "full"), watermark_column=d.get("watermark_column", ""),
        group_id=d.get("group_id"),
    ))


@workflow_bp.route("/workflow/watermarks", methods=["GET"])
@login_required
def wf_watermarks():
    return jsonify(wfm.get_watermarks())


@workflow_bp.route("/workflow/watermarks/update", methods=["POST"])
@login_required
def wf_update_watermark():
    d = request.get_json() or {}
    return jsonify(wfm.update_watermark(d.get("table"), d.get("column"), d.get("value")))


@workflow_bp.route("/workflow/watermarks/reset", methods=["POST"])
@login_required
def wf_reset_watermark():
    d = request.get_json() or {}
    return jsonify(wfm.reset_watermark(d.get("table")))


# ─────────────────────────────────────────────────────────────────────────────
#  SCHEDULER ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@workflow_bp.route("/workflow/scheduler", methods=["GET"])
@login_required
def wf_scheduler_load():
    """Load all scheduler configs + history."""
    try:
        data = wfm.scheduler_load_all()
        return jsonify({"success": True, **data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@workflow_bp.route("/workflow/scheduler/tables", methods=["GET"])
@login_required
def wf_scheduler_tables():
    """Get tables that have jobs in the registry (for scheduler dropdown)."""
    try:
        tables = []
        seen = set()
        for gid, grp in wfm.PIPELINE_GROUPS.items():
            tbl = grp.get("table") or grp.get("table_name") or ""
            if tbl and tbl not in seen:
                seen.add(tbl)
                job_ids = grp.get("job_ids", [])
                job_names = []
                for jid in job_ids:
                    job = wfm.JOB_REGISTRY.get(jid)
                    if job:
                        job_names.append(job.get("job_name", jid))
                tables.append({
                    "table_name": tbl,
                    "group_id": gid,
                    "job_names": job_names,
                    "schema": grp.get("table_schema", ""),
                })
        return jsonify({"success": True, "tables": tables})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@workflow_bp.route("/workflow/scheduler/config", methods=["POST"])
@login_required
def wf_scheduler_upsert():
    """Create or update a schedule."""
    try:
        d = request.get_json() or {}
        import uuid
        if not d.get("schedule_id"):
            d["schedule_id"] = uuid.uuid4().hex[:12]
        if not d.get("created_at"):
            from datetime import datetime, timezone
            d["created_at"] = datetime.now(timezone.utc).isoformat()
        if not d.get("status"):
            d["status"] = "active"
        wfm.scheduler_upsert_config(d)
        log_action("schedule_created", "scheduler", d["schedule_id"])
        return jsonify({"success": True, "schedule_id": d["schedule_id"]})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@workflow_bp.route("/workflow/scheduler/config/<schedule_id>", methods=["DELETE"])
@login_required
def wf_scheduler_delete(schedule_id):
    """Delete a schedule."""
    try:
        wfm.scheduler_delete_config(schedule_id)
        log_action("schedule_deleted", "scheduler", schedule_id)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@workflow_bp.route("/workflow/scheduler/config/<schedule_id>/toggle", methods=["PUT"])
@login_required
def wf_scheduler_toggle(schedule_id):
    """Toggle pause/active."""
    try:
        d = request.get_json() or {}
        new_status = d.get("status", "paused")
        data = wfm.scheduler_load_all()
        entry = None
        for s in data.get("schedules", []):
            if s.get("schedule_id") == schedule_id:
                entry = s
                break
        if not entry:
            return jsonify({"success": False, "error": "Schedule not found"}), 404
        entry["status"] = new_status
        wfm.scheduler_upsert_config(entry)
        return jsonify({"success": True, "status": new_status})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@workflow_bp.route("/workflow/scheduler/config/<schedule_id>/run", methods=["POST"])
@login_required
def wf_scheduler_run_now(schedule_id):
    """Manually trigger a scheduled run."""
    try:
        data = wfm.scheduler_load_all()
        entry = None
        for s in data.get("schedules", []):
            if s.get("schedule_id") == schedule_id:
                entry = s
                break
        if not entry:
            return jsonify({"success": False, "error": "Schedule not found"}), 404

        group_id = entry.get("group_id", "")
        result = wfm.run_pipeline_group(group_id)
        # Record in history
        from datetime import datetime, timezone
        jobs_str = " → ".join(entry.get("job_names", []))
        run_id = result.get("run_id", "")
        detail = f"{run_id}" if run_id else ""
        wfm.scheduler_insert_history({
            "schedule_id": schedule_id,
            "table_name": entry.get("table_name", ""),
            "jobs": jobs_str,
            "trigger": "manual",
            "result": "Running" if result.get("success") else "Failed",
            "details": detail,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        # Update last_run on config
        entry["last_run"] = datetime.now(timezone.utc).isoformat()
        wfm.scheduler_upsert_config(entry)
        return jsonify({"success": True, "result": result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@workflow_bp.route("/workflow/scheduler/history/refresh-status", methods=["POST"])
@login_required
def wf_scheduler_refresh_status():
    """Check actual Databricks run status for entries still marked Running."""
    try:
        data = wfm.scheduler_load_all()
        updated = 0
        for h in data.get("history", []):
            if h.get("result", "").lower() != "running":
                continue
            details = h.get("details", "") or ""
            # Try to extract run_id from details (format: "run_id | url" or just "run_id")
            run_id = details.split("|")[0].strip() if details else ""
            if not run_id:
                continue
            # Check actual status via workflow manager
            run_info = wfm.get_run_status(run_id) if hasattr(wfm, 'get_run_status') else None
            if run_info:
                actual_status = run_info.get("status", "")
                if actual_status and actual_status.lower() != "running":
                    # Update history entry
                    hid = h.get("history_id", "")
                    if hid:
                        sql = f"UPDATE {wfm._fqn(wfm.TBL_SCH_HISTORY)} SET result = {wfm._esc(actual_status)} WHERE history_id = {wfm._esc(hid)}"
                        wfm._exec_sql(sql)
                        updated += 1
        return jsonify({"success": True, "updated": updated})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Reconciliation Data — fetch from Delta table via Statement API             ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@workflow_bp.route("/recon/data", methods=["POST"])
@login_required
def wf_recon_data():
    """Fetch reconciliation data from the configured reconciliation table."""
    try:
        from unity_catalog_executor import UnityCatalogExecutor
        cfg = get_config()
        dbx_host = cfg.get("databricks_host", "").rstrip("/")
        dbx_token = get_databricks_token()
        if not dbx_host or not dbx_token:
            return jsonify({"success": False, "rows": [], "error": "Databricks not configured."})

        recon_cfg = cfg.get("reconciliation", {})
        recon_cat = recon_cfg.get("catalog", "reconciliation")
        recon_sch = recon_cfg.get("schema", "hr")
        recon_tbl = recon_cfg.get("table", "ReconcilationDetails")

        uc = UnityCatalogExecutor(dbx_host, dbx_token, recon_cat, recon_sch)
        wh_resp = uc.list_warehouses()
        warehouses = wh_resp.get("warehouses", [])
        wh_id = next((w["id"] for w in warehouses if w.get("state") == "RUNNING"), None)
        if not wh_id and warehouses:
            wh_id = warehouses[0].get("id")
        if not wh_id:
            return jsonify({"success": False, "rows": [], "error": "No SQL Warehouse available."})

        fqn = f"`{recon_cat}`.`{recon_sch}`.`{recon_tbl}`"
        sql = f"SELECT * FROM {fqn} ORDER BY recon_timestamp DESC LIMIT 2000"
        result = uc._execute_statement(sql, wh_id, wait_timeout="30s")
        if result.get("error"):
            return jsonify({"success": False, "rows": [], "error": result["error"]})

        status = result.get("status", {}).get("state", "")
        if status in ("PENDING", "RUNNING"):
            stmt_id = result.get("statement_id", "")
            if stmt_id:
                result = uc._poll_statement(stmt_id)
                status = result.get("status", {}).get("state", "")

        if status in ("FAILED", "CLOSED", "CANCELED"):
            err_msg = result.get("status", {}).get("error", {}).get("message", "Query failed")
            return jsonify({"success": False, "rows": [], "error": err_msg})

        columns = [c.get("name", "") for c in result.get("manifest", {}).get("schema", {}).get("columns", [])]
        data_array = result.get("result", {}).get("data_array", [])

        rows = []
        for row in data_array:
            obj = {}
            for i, col in enumerate(columns):
                obj[col] = row[i] if i < len(row) else None
            rows.append(obj)

        return jsonify({"success": True, "rows": rows})
    except Exception as e:
        logger.exception("recon/data failed")
        return jsonify({"success": False, "rows": [], "error": str(e)}), 500


@workflow_bp.route("/workflow/update-layer-mapping", methods=["POST"])
@login_required
def update_layer_mapping():
    """Re-point specific existing pipelines' target_config at a new layer mapping.

    Scoped to an explicit ``table_names`` list — this used to run an
    unconditional UPDATE across every row of wf_job_metadata (every pipeline
    for every table ever created), so saving the mapping once silently
    rewrote the target catalog/schema for jobs the user never selected.
    """
    import json
    try:
        body = request.get_json(force=True)
        layer_mapping = body.get("layer_mapping", {})
        table_names = [t for t in (body.get("table_names") or []) if t]
        if not layer_mapping:
            return jsonify({"success": False, "error": "layer_mapping required"})
        if not table_names:
            return jsonify({"success": False, "error": "table_names required — select the tables to re-map"}), 400

        # Build the new catalog/schema values from layer mapping
        landing = layer_mapping.get("landing", {})
        bronze = layer_mapping.get("bronze", {})
        silver = layer_mapping.get("silver", {})

        new_volumes_catalog = landing.get("catalog", "")
        new_bronze_catalog = bronze.get("catalog", "")
        new_silver_catalog = silver.get("catalog", "")
        new_target_schema = landing.get("schema", "") or bronze.get("schema", "")

        if not new_bronze_catalog or not new_silver_catalog:
            return jsonify({"success": False, "error": "Bronze and Silver catalogs are required"})

        cfg = get_config()
        meta_cat = cfg.get("metadata_catalog", "admin_source")
        meta_sch = cfg.get("metadata_schema", "configtables")
        fqn = f"`{meta_cat}`.`{meta_sch}`.wf_job_metadata"

        from dbsql_client import execute_query, execute_write

        placeholders = ", ".join(f"%(t{i})s" for i in range(len(table_names)))
        params = {f"t{i}": name for i, name in enumerate(table_names)}
        rows = execute_query(
            f"SELECT table_name, target_config FROM {fqn} WHERE table_name IN ({placeholders})",
            params,
        )

        updated = 0
        for row in rows:
            table_name = row["table_name"]
            try:
                tc = json.loads(row.get("target_config") or "{}")
            except (json.JSONDecodeError, TypeError):
                tc = {}

            tc["volumes_catalog"] = new_volumes_catalog
            tc["bronze_catalog"] = new_bronze_catalog
            tc["silver_catalog"] = new_silver_catalog
            if new_target_schema:
                tc["target_schema"] = new_target_schema

            execute_write(
                f"UPDATE {fqn} SET target_config = %(tc)s WHERE table_name = %(tn)s",
                {"tc": json.dumps(tc), "tn": table_name},
            )
            updated += 1

        # Also refresh in-memory PIPELINE_GROUPS (selected tables only) so next Run uses correct values
        try:
            from workflow_manager import PIPELINE_GROUPS
            selected = set(table_names)
            refreshed = 0
            for gid, grp in PIPELINE_GROUPS.items():
                if grp.get("table_name") not in selected:
                    continue
                tc = grp.get("target_config") or {}
                tc["volumes_catalog"] = new_volumes_catalog
                tc["bronze_catalog"] = new_bronze_catalog
                tc["silver_catalog"] = new_silver_catalog
                if new_target_schema:
                    tc["target_schema"] = new_target_schema
                grp["target_config"] = tc
                refreshed += 1
            logger.info("Refreshed PIPELINE_GROUPS in-memory (%d of %d selected)", refreshed, len(table_names))
        except Exception as _mem_err:
            logger.warning("Could not refresh PIPELINE_GROUPS in-memory: %s", _mem_err)

        logger.info("Updated layer mapping for %d of %d selected table(s): volumes=%s bronze=%s silver=%s schema=%s",
                   updated, len(table_names), new_volumes_catalog, new_bronze_catalog, new_silver_catalog, new_target_schema)

        return jsonify({"success": True, "updated": updated,
                       "mapping": {"volumes_catalog": new_volumes_catalog, "bronze_catalog": new_bronze_catalog,
                                  "silver_catalog": new_silver_catalog, "target_schema": new_target_schema}})
    except Exception as e:
        logger.error("Failed to update layer mapping: %s", e, exc_info=True)
        return jsonify({"success": False, "error": str(e)[:200]})
