"""Settings API — Catalog/Schema listing and access validation for ExistingSetting."""
from flask import Blueprint, jsonify, request
from .auth import login_required
from config_cache import get_config, get_databricks_token
from log_config import get_logger

logger = get_logger(__name__)
settings_bp = Blueprint("settings", __name__, url_prefix="/api/v1")


@settings_bp.route("/deploy-config", methods=["GET"])
@login_required
def get_deploy_config():
    """Return the full config for Settings page pre-fill."""
    try:
        cfg = get_config()
        return jsonify({"success": True, "config": cfg})
    except Exception as e:
        logger.error("Failed to load config: %s", e)
        return jsonify({"success": False, "error": str(e)})


@settings_bp.route("/deploy-config", methods=["POST"])
@login_required
def save_deploy_config():
    """Save config from Settings page."""
    try:
        from config_cache import save_config
        data = request.get_json(force=True)
        save_config(data)
        return jsonify({"success": True})
    except Exception as e:
        logger.error("Failed to save config: %s", e)
        return jsonify({"success": False, "error": str(e)})





@settings_bp.route("/test-databricks", methods=["POST"])
@login_required
def test_databricks_conn():
    """Test Databricks workspace connectivity."""
    try:
        import requests as req
        body = request.get_json(force=True)
        host = (body.get("databricks_host") or "").rstrip("/")
        token = body.get("databricks_token") or ""
        # If token is masked, use the real one from secrets
        if not token or token.startswith("•"):
            token = get_databricks_token()
        if not host or not token:
            return jsonify({"success": False, "error": "Host and token required"})
        # Test by calling clusters/list (lightweight)
        resp = req.get(
            f"{host}/api/2.0/clusters/list",
            headers={"Authorization": f"Bearer {token}"},
            params={"page_size": 1},
            timeout=15
        )
        if resp.status_code == 200:
            data = resp.json()
            cluster_count = len(data.get("clusters", []))
            return jsonify({"success": True, "message": f"Connected. {cluster_count} cluster(s) found.",
                           "host": host, "clusters": cluster_count})
        elif resp.status_code == 401:
            return jsonify({"success": False, "error": "Authentication failed (401). Check PAT token."})
        elif resp.status_code == 403:
            return jsonify({"success": False, "error": "Forbidden (403). Token may lack permissions."})
        else:
            return jsonify({"success": False, "error": f"HTTP {resp.status_code}: {resp.text[:200]}"})
    except req.exceptions.Timeout:
        return jsonify({"success": False, "error": "Connection timed out. Check host URL."})
    except req.exceptions.ConnectionError as e:
        return jsonify({"success": False, "error": f"Connection error: {str(e)[:100]}"})
    except Exception as e:
        logger.error("Test Databricks connection failed: %s", e)
        return jsonify({"success": False, "error": str(e)[:200]})

@settings_bp.route("/settings/catalogs", methods=["GET"])
@login_required
def list_catalogs():
    """List catalogs accessible to the current user/SP via SQL."""
    cfg = get_config()
    dbx_host = cfg.get("databricks_host", "").rstrip("/")
    dbx_token = get_databricks_token()
    if not dbx_host or not dbx_token:
        return jsonify({"success": False, "catalogs": [], "error": "Databricks not configured."})
    try:
        from unity_catalog_executor import UnityCatalogExecutor
        meta_cat = cfg.get("metadata_catalog", "admin_source") or "admin_source"
        meta_sch = cfg.get("metadata_schema", "configtables") or "configtables"
        uc = UnityCatalogExecutor(dbx_host, dbx_token, meta_cat, meta_sch)
        wh_resp = uc.list_warehouses()
        warehouses = wh_resp.get("warehouses", [])
        wh_id = next((w["id"] for w in warehouses if w.get("state") == "RUNNING"), None)
        if not wh_id and warehouses:
            wh_id = warehouses[0].get("id")
        if not wh_id:
            logger.error("No SQL Warehouse found for catalog listing")
            return jsonify({"success": False, "catalogs": [], "error": "No SQL Warehouse available."})
        logger.info("Listing catalogs via warehouse %s", wh_id)
        result = uc._execute_statement("SHOW CATALOGS", wh_id, wait_timeout="30s")
        if result.get("error"):
            logger.error("SHOW CATALOGS failed: %s", result["error"])
            return jsonify({"success": False, "catalogs": [], "error": result["error"]})
        status = result.get("status", {}).get("state", "")
        if status not in ("SUCCEEDED", "CLOSED", ""):
            logger.error("SHOW CATALOGS state: %s", status)
            return jsonify({"success": False, "catalogs": [], "error": f"Query state: {status}"})
        data_array = result.get("result", {}).get("data_array", [])
        catalogs = [row[0] for row in data_array if row and row[0]]
        catalogs = [c for c in catalogs if c not in ("system", "__databricks_internal")]
        logger.info("Catalogs listed: %d found", len(catalogs))
        return jsonify({"success": True, "catalogs": sorted(catalogs)})
    except Exception as e:
        logger.error("Failed to list catalogs: %s", e, exc_info=True)
        return jsonify({"success": False, "catalogs": [], "error": str(e)[:200]})


@settings_bp.route("/settings/schemas", methods=["GET"])
@login_required
def list_schemas():
    """List schemas in a catalog via SQL."""
    catalog = request.args.get("catalog", "")
    if not catalog:
        return jsonify({"success": False, "schemas": [], "error": "catalog parameter required."})
    cfg = get_config()
    dbx_host = cfg.get("databricks_host", "").rstrip("/")
    dbx_token = get_databricks_token()
    if not dbx_host or not dbx_token:
        return jsonify({"success": False, "schemas": [], "error": "Databricks not configured."})
    try:
        from unity_catalog_executor import UnityCatalogExecutor
        uc = UnityCatalogExecutor(dbx_host, dbx_token, catalog, "default")
        wh_resp = uc.list_warehouses()
        warehouses = wh_resp.get("warehouses", [])
        wh_id = next((w["id"] for w in warehouses if w.get("state") == "RUNNING"), None)
        if not wh_id and warehouses:
            wh_id = warehouses[0].get("id")
        if not wh_id:
            return jsonify({"success": False, "schemas": [], "error": "No SQL Warehouse available."})
        result = uc._execute_statement(f"SHOW SCHEMAS IN `{catalog}`", wh_id, wait_timeout="30s")
        if result.get("error"):
            return jsonify({"success": False, "schemas": [], "error": result["error"]})
        data_array = result.get("result", {}).get("data_array", [])
        schemas = [row[0] for row in data_array if row and row[0]]
        schemas = [s for s in schemas if s not in ("information_schema",)]
        return jsonify({"success": True, "schemas": sorted(schemas)})
    except Exception as e:
        logger.error("Failed to list schemas for %s: %s", catalog, e)
        return jsonify({"success": False, "schemas": [], "error": str(e)})


@settings_bp.route("/settings/validate-access", methods=["POST"])
@login_required
def validate_access():
    """Validate access for a medallion layer configuration."""
    body = request.get_json(force=True)
    layer = body.get("layer", "")
    catalog = body.get("catalog", "")
    schema = body.get("schema", "")
    storage_account = body.get("storage_account", "")
    container = body.get("container", "")
    base_path = body.get("base_path", "")

    if not catalog or not schema or not storage_account or not container:
        return jsonify({"success": True, "valid": False, "error": "Missing required fields"})

    cfg = get_config()
    dbx_host = cfg.get("databricks_host", "").rstrip("/")
    dbx_token = get_databricks_token()
    if not dbx_host or not dbx_token:
        return jsonify({"success": True, "valid": False, "error": "Databricks not configured."})

    errors = []
    import requests as req

    # 1. Validate catalog access
    try:
        resp = req.get(
            f"{dbx_host}/api/2.1/unity-catalog/catalogs/{catalog}",
            headers={"Authorization": f"Bearer {dbx_token}"},
            timeout=15
        )
        if resp.status_code != 200:
            errors.append(f"Catalog '{catalog}' not accessible (HTTP {resp.status_code})")
    except Exception as e:
        errors.append(f"Catalog check failed: {e}")

    # 2. Validate schema access
    try:
        resp = req.get(
            f"{dbx_host}/api/2.1/unity-catalog/schemas/{catalog}.{schema}",
            headers={"Authorization": f"Bearer {dbx_token}"},
            timeout=15
        )
        if resp.status_code != 200:
            errors.append(f"Schema '{catalog}.{schema}' not accessible (HTTP {resp.status_code})")
    except Exception as e:
        errors.append(f"Schema check failed: {e}")

    # 3. Validate storage path (construct ABFSS path and check via DBFS/Files API)
    abfss_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{base_path}" if base_path else f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    try:
        # Use SQL Statement API to test path access
        from unity_catalog_executor import UnityCatalogExecutor
        uc = UnityCatalogExecutor(dbx_host, dbx_token, catalog, schema)
        wh_resp = uc.list_warehouses()
        warehouses = wh_resp.get("warehouses", [])
        wh_id = next((w["id"] for w in warehouses if w.get("state") == "RUNNING"), None)
        if not wh_id and warehouses:
            wh_id = warehouses[0].get("id")
        if wh_id:
            test_sql = f"LIST \'{abfss_path}\'"
            result = uc._execute_statement(test_sql, wh_id, wait_timeout="15s")
            status_state = result.get("status", {}).get("state", "")
            if status_state in ("FAILED",):
                err_msg = result.get("status", {}).get("error", {}).get("message", "")
                if "ACCESS_DENIED" in err_msg.upper() or "FORBIDDEN" in err_msg.upper():
                    errors.append(f"Storage path access denied: {abfss_path}")
                # If it's just "path not found" that's OK — it will be created
        else:
            # No warehouse available — skip storage validation, just validate catalog/schema
            pass
    except Exception as e:
        # Storage validation is best-effort; catalog/schema validation is critical
        logger.warning("Storage path validation skipped: %s", e)

    if errors:
        return jsonify({"success": True, "valid": False, "error": "; ".join(errors)})

    return jsonify({"success": True, "valid": True, "layer": layer, "message": f"All checks passed for {layer}"})
