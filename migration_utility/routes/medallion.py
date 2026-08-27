"""Medallion blueprint — generate, deploy, and run medallion architecture notebooks."""
from flask import Blueprint, request, jsonify
import base64

from .auth import login_required
from log_config import get_logger
from databricks_connector import DatabricksConnector
from medallion_notebooks import generate_all_medallion_notebooks
from config_cache import get_databricks_token
from keyvault_helper import is_masked

logger = get_logger(__name__)
medallion_bp = Blueprint("medallion", __name__, url_prefix="/api/v1")


@medallion_bp.route("/medallion/generate", methods=["POST"])
@login_required
def medallion_generate():
    try:
        d = request.get_json()
        source_type = d.get("source_type", "sqlserver")
        server = d.get("server", "").strip()
        database = d.get("database", "").strip()
        username = d.get("username", "").strip()
        tables = d.get("tables", [])
        catalog = d.get("catalog", "main").strip()
        schema = d.get("schema", "default").strip()
        landing_path = d.get("landing_path", "/mnt/landing").strip()
        workspace_path = d.get("workspace_path", "/Shared/Medallion").strip()
        volumes_catalog = d.get("volumes_catalog", "").strip()
        bronze_catalog = d.get("bronze_catalog", "").strip()
        silver_catalog = d.get("silver_catalog", "").strip()
        target_schema = d.get("target_schema", "").strip()
        # Snowflake-specific params
        account = d.get("account", "").strip()
        warehouse = d.get("warehouse", "").strip()
        role = d.get("role", "").strip()
        if not tables:
            return jsonify({"success": False, "error": "tables list is required"}), 400
        result = generate_all_medallion_notebooks(
            source_type=source_type, server=server, database=database, username=username,
            tables=tables, catalog=catalog, schema=schema, landing_path=landing_path,
            workspace_path=workspace_path, volumes_catalog=volumes_catalog,
            bronze_catalog=bronze_catalog, silver_catalog=silver_catalog,
            target_schema=target_schema,
            account=account, warehouse=warehouse, role=role,
        )
        return jsonify(result)
    except Exception as e:
        logger.exception("Medallion notebook generation failed")
        return jsonify({"success": False, "error": str(e)}), 500


@medallion_bp.route("/medallion/deploy", methods=["POST"])
@login_required
def medallion_deploy():
    try:
        d = request.get_json()
        host = d.get("host", "").strip()
        token = d.get("token", "").strip()
        if not token or is_masked(token):
            token = get_databricks_token()
        workspace_path = d.get("workspace_path", "/Shared/Medallion").strip()
        notebooks = d.get("notebooks", [])
        if not all([host, token, notebooks]):
            return jsonify({"success": False, "error": "host, token, and notebooks are required"}), 400
        connector = DatabricksConnector(host, token)
        results = []
        for nb in notebooks:
            r = connector.upload_notebook(
                notebook_name=nb.get("name", "Notebook"),
                python_code=nb.get("code", ""), path=workspace_path,
            )
            results.append({
                "name": nb.get("name"), "success": r.get("success"),
                "path": r.get("notebook_path") or r.get("path"),
                "url": r.get("workspace_url"),
                "error": r.get("error") or r.get("message") if not r.get("success") else None,
            })
        ok = sum(1 for r in results if r["success"])
        return jsonify({"success": ok > 0, "results": results,
                        "uploaded": ok, "total": len(results),
                        "workspace_path": workspace_path})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@medallion_bp.route("/medallion/run-pipeline", methods=["POST"])
@login_required
def medallion_run_pipeline():
    try:
        d = request.get_json()
        host = d.get("host", "").strip()
        token = d.get("token", "").strip()
        if not token or is_masked(token):
            token = get_databricks_token()
        workspace_path = d.get("workspace_path", "/Shared/Medallion").strip()
        cluster_id = d.get("cluster_id", "").strip()
        load_type = d.get("load_type", "full").strip()
        password = d.get("password", "")
        if not host or not token:
            return jsonify({"success": False, "error": "host and token are required"}), 400
        pwd_b64 = base64.b64encode((password or "").encode("utf-8")).decode("ascii")
        connector = DatabricksConnector(host, token)
        result = connector.run_notebook(
            notebook_path=f"{workspace_path}/00_Orchestrator",
            cluster_id=cluster_id or None,
            params={"load_type": load_type, "password_b64": pwd_b64},
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
