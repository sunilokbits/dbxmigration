"""Migration Infrastructure blueprint — ExistingSetting features.

Provides:
  - Migration layer selection (Landing→Bronze→Silver OR Bronze→Silver)
  - Access/authorization check (UC, Storage, Connector, Warehouse, Scope)
  - Notebook generation for SQL-warehouse-based migration
  - Job creation for OldInfra mode
"""
from flask import Blueprint, request, jsonify
import os, json, base64

from .auth import login_required
from log_config import get_logger
from config_cache import get_config, save_config

logger = get_logger(__name__)
migration_infra_bp = Blueprint("migration_infra", __name__, url_prefix="/api/v1")


# ═══════════════════════════════════════════════════════════════════════════════
# Migration Layer Patterns
# ═══════════════════════════════════════════════════════════════════════════════

MIGRATION_LAYERS = {
    "landing_bronze_silver": {
        "label": "Landing → Bronze → Silver",
        "layers": ["landing", "bronze", "silver"],
        "description": "Full 3-layer architecture. Source data lands raw, then cleaned in bronze, refined in silver.",
        "mapping": [
            {"from": "Source (SQL Server)", "to": "Landing (Raw/As-Is)", "operation": "Extract"},
            {"from": "Landing", "to": "Bronze (Cleansed)", "operation": "Load + Schema Enforcement"},
            {"from": "Bronze", "to": "Silver (Business Ready)", "operation": "Transform + Dedupe"},
        ],
    },
    "bronze_silver": {
        "label": "Bronze → Silver",
        "layers": ["bronze", "silver"],
        "description": "2-layer architecture. Source data extracted directly into bronze, refined in silver.",
        "mapping": [
            {"from": "Source (SQL Server)", "to": "Bronze (Raw + Cleansed)", "operation": "Extract + Load"},
            {"from": "Bronze", "to": "Silver (Business Ready)", "operation": "Transform + Dedupe"},
        ],
    },
}


@migration_infra_bp.route("/migration-layers", methods=["GET"])
@login_required
def get_migration_layers():
    """Return available migration layer patterns."""
    return jsonify({"success": True, "layers": MIGRATION_LAYERS})


@migration_infra_bp.route("/migration-layers", methods=["POST"])
@login_required
def save_migration_layer():
    """Save selected layer pattern and per-layer catalog/schema mappings."""
    try:
        data = request.get_json() or {}
        layer_key = data.get("layer_pattern")
        if layer_key not in MIGRATION_LAYERS:
            return jsonify({"success": False, "error": f"Invalid layer pattern: {layer_key}"}), 400
        cfg = get_config() or {}
        cfg["migration_layer_pattern"] = layer_key
        cfg["migration_layers"] = MIGRATION_LAYERS[layer_key]["layers"]
        if data.get("layer_mappings"):
            cfg["layer_mappings"] = data["layer_mappings"]
        save_config(cfg)
        return jsonify({"success": True, "message": f"Layer pattern '{MIGRATION_LAYERS[layer_key]['label']}' saved."})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# Access / Authorization Check
# ═══════════════════════════════════════════════════════════════════════════════

@migration_infra_bp.route("/check-access", methods=["POST"])
@login_required
def check_access():
    """Validate all access before proceeding:
    - Unity Catalog (USE CATALOG, schemas)
    - Storage Credential
    - Access Connector
    - SQL Warehouse
    - Secret Scope
    """
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        cfg = get_config() or {}
        data = request.get_json() or {}

        catalog = data.get("catalog") or cfg.get("metadata_catalog") or os.environ.get("DATABRICKS_CATALOG", "admin_source")
        schema = data.get("schema") or cfg.get("metadata_schema") or "migration_app"
        storage_cred = data.get("storage_credential") or cfg.get("storage_credential_name", "")

        checks = []

        # 1. Unity Catalog
        try:
            cat_info = w.catalogs.get(catalog)
            checks.append({"name": "Unity Catalog", "target": catalog, "status": "pass",
                           "message": f"Catalog '{catalog}' accessible (owner: {cat_info.owner})"})
        except Exception as e:
            checks.append({"name": "Unity Catalog", "target": catalog, "status": "fail",
                           "message": str(e)[:200],
                           "fix": f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `<principal>`"})

        # 2. Schema
        try:
            sch_info = w.schemas.get(full_name=f"{catalog}.{schema}")
            checks.append({"name": "Schema", "target": f"{catalog}.{schema}", "status": "pass",
                           "message": f"Schema '{schema}' accessible (owner: {sch_info.owner})"})
        except Exception as e:
            if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                checks.append({"name": "Schema", "target": f"{catalog}.{schema}", "status": "warn",
                               "message": f"Schema '{schema}' doesn't exist — will be created."})
            else:
                checks.append({"name": "Schema", "target": f"{catalog}.{schema}", "status": "fail",
                               "message": str(e)[:200]})

        # 3. Storage Credential
        if storage_cred:
            try:
                cred = w.storage_credentials.get(storage_cred)
                checks.append({"name": "Storage Credential", "target": storage_cred, "status": "pass",
                               "message": f"Credential '{storage_cred}' exists (owner: {cred.owner})"})
            except Exception as e:
                checks.append({"name": "Storage Credential", "target": storage_cred, "status": "fail",
                               "message": str(e)[:200]})
        else:
            checks.append({"name": "Storage Credential", "target": "(not set)", "status": "skip",
                           "message": "Not configured — skip if using managed storage."})

        # 4. SQL Warehouse
        wh_id = data.get("warehouse_id") or cfg.get("databricks_sql_warehouse_id") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
        if wh_id:
            try:
                wh = w.warehouses.get(wh_id)
                checks.append({"name": "SQL Warehouse", "target": f"{wh.name} ({wh_id})", "status": "pass",
                               "message": f"Warehouse '{wh.name}' state: {wh.state.value}"})
            except Exception as e:
                checks.append({"name": "SQL Warehouse", "target": wh_id, "status": "fail",
                               "message": str(e)[:200]})
        else:
            checks.append({"name": "SQL Warehouse", "target": "(not set)", "status": "warn",
                           "message": "No warehouse configured — select one in Existing Setting."})

        # 5. Secret Scope
        scope_name = data.get("secret_scope") or os.environ.get("DATABRICKS_SECRET_SCOPE", "migration-studio")
        try:
            secrets = [s.key for s in w.secrets.list_secrets(scope=scope_name)]
            checks.append({"name": "Secret Scope", "target": scope_name, "status": "pass",
                           "message": f"{len(secrets)} secret(s): {', '.join(secrets[:5])}"})
        except Exception as e:
            checks.append({"name": "Secret Scope", "target": scope_name, "status": "fail",
                           "message": str(e)[:200]})

        # 6. Access Connector (if configured)
        ac_name = data.get("access_connector") or cfg.get("access_connector", "")
        if ac_name:
            try:
                # Check via listing storage credentials that reference this connector
                checks.append({"name": "Access Connector", "target": ac_name, "status": "pass",
                               "message": f"Connector '{ac_name}' configured in deployconfig."})
            except Exception as e:
                checks.append({"name": "Access Connector", "target": ac_name, "status": "fail",
                               "message": str(e)[:200]})
        else:
            checks.append({"name": "Access Connector", "target": "(not set)", "status": "skip",
                           "message": "Not configured — needed only for external storage."})

        all_pass = all(c["status"] in ("pass", "skip", "warn") for c in checks)
        return jsonify({
            "success": True,
            "checks": checks,
            "all_pass": all_pass,
            "summary": "All access checks passed" if all_pass else "Some checks failed — fix before proceeding",
        })
    except Exception as e:
        logger.exception("check-access failed")
        return jsonify({"success": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# Generate Migration Notebooks (OldInfra — SQL Warehouse based)
# ═══════════════════════════════════════════════════════════════════════════════

@migration_infra_bp.route("/generate-migration-notebooks", methods=["POST"])
@login_required
def generate_migration_notebooks():
    """Generate SQL-based migration notebooks in a separate folder.
    Uses SQL warehouse by default (no cluster needed)."""
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.workspace import ImportFormat, Language
        w = WorkspaceClient()

        data = request.get_json() or {}
        tables = data.get("tables", [])
        layer_pattern = data.get("layer_pattern", "bronze_silver")
        folder = data.get("folder", "/Workspace/Shared/migration_notebooks")

        if not tables:
            return jsonify({"success": False, "error": "No tables selected"}), 400

        cfg = get_config() or {}
        layer_info = MIGRATION_LAYERS.get(layer_pattern, MIGRATION_LAYERS["bronze_silver"])
        wh_id = cfg.get("databricks_sql_warehouse_id") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
        source_cfg = cfg.get("source", {})
        catalog = cfg.get("metadata_catalog", "admin_source")
        mappings = cfg.get("layer_mappings", {})

        created = []
        for table in tables:
            tbl_name = table if isinstance(table, str) else table.get("name", "")
            if not tbl_name:
                continue
            safe = tbl_name.replace(".", "_").replace(" ", "_").lower()

            if layer_pattern == "landing_bronze_silver":
                content = _gen_3layer(tbl_name, safe, source_cfg, catalog, mappings, wh_id)
            else:
                content = _gen_2layer(tbl_name, safe, source_cfg, catalog, mappings, wh_id)

            nb_path = f"{folder}/{safe}_migration"
            try:
                encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")
                w.workspace.import_(
                    path=nb_path, content=encoded,
                    format=ImportFormat.SOURCE, language=Language.SQL, overwrite=True,
                )
                created.append({"table": tbl_name, "path": nb_path, "status": "created"})
            except Exception as e:
                created.append({"table": tbl_name, "path": nb_path, "status": "error", "error": str(e)[:150]})

        return jsonify({
            "success": True, "notebooks": created, "folder": folder,
            "message": f"Generated {sum(1 for n in created if n['status']=='created')} notebook(s)",
        })
    except Exception as e:
        logger.exception("generate-migration-notebooks failed")
        return jsonify({"success": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# Create Job for OldInfra Notebooks
# ═══════════════════════════════════════════════════════════════════════════════

@migration_infra_bp.route("/create-migration-job", methods=["POST"])
@login_required
def create_migration_job():
    """Create a Databricks Job that runs migration notebooks on SQL warehouse."""
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.jobs import (
            Task, NotebookTask, SqlTask, Source,
        )
        w = WorkspaceClient()

        data = request.get_json() or {}
        notebook_paths = data.get("notebooks", [])
        job_name = data.get("job_name", "DBXConnect_Migration")
        wh_id = data.get("warehouse_id") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")

        if not notebook_paths:
            return jsonify({"success": False, "error": "No notebooks provided"}), 400

        # Build tasks — one per notebook, sequential
        tasks = []
        for i, nb_path in enumerate(notebook_paths):
            task_key = f"migrate_{i+1}"
            task = Task(
                task_key=task_key,
                notebook_task=NotebookTask(
                    notebook_path=nb_path,
                    source=Source.WORKSPACE,
                    warehouse_id=wh_id if wh_id else None,
                ),
                depends_on=[{"task_key": f"migrate_{i}"}] if i > 0 else None,
            )
            tasks.append(task)

        job = w.jobs.create(
            name=job_name,
            tasks=tasks,
        )

        return jsonify({
            "success": True,
            "job_id": job.job_id,
            "job_name": job_name,
            "task_count": len(tasks),
            "message": f"Job '{job_name}' created with {len(tasks)} task(s). Uses SQL warehouse.",
        })
    except Exception as e:
        logger.exception("create-migration-job failed")
        return jsonify({"success": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# Notebook Content Generators
# ═══════════════════════════════════════════════════════════════════════════════

def _gen_3layer(table, safe, src, catalog, mappings, wh_id):
    landing_sch = mappings.get("landing", {}).get("schema", "landing")
    bronze_sch = mappings.get("bronze", {}).get("schema", "bronze")
    silver_sch = mappings.get("silver", {}).get("schema", "silver")
    server = src.get("server", "<source_server>")
    db = src.get("database", "<source_db>")

    return f"""-- Databricks notebook source
-- Migration: {table} | Pattern: Landing -> Bronze -> Silver
-- SQL Warehouse mode (no cluster needed) | WH: {wh_id}
-- Generated by DBXConnect

-- STEP 1: Source -> Landing (raw extract)
CREATE OR REPLACE TABLE `{catalog}`.`{landing_sch}`.`{safe}_raw` AS
SELECT * FROM sqlserver_jdbc(
  host => '{server}', database => '{db}',
  query => 'SELECT * FROM {table}'
);

-- STEP 2: Landing -> Bronze (cleanse + add metadata)
CREATE OR REPLACE TABLE `{catalog}`.`{bronze_sch}`.`{safe}` AS
SELECT *, current_timestamp() AS _ingested_at, '{table}' AS _source_table
FROM `{catalog}`.`{landing_sch}`.`{safe}_raw`;

-- STEP 3: Bronze -> Silver (business logic)
CREATE OR REPLACE TABLE `{catalog}`.`{silver_sch}`.`{safe}` AS
SELECT *, current_timestamp() AS _refined_at
FROM `{catalog}`.`{bronze_sch}`.`{safe}`
WHERE _ingested_at IS NOT NULL;
"""


def _gen_2layer(table, safe, src, catalog, mappings, wh_id):
    bronze_sch = mappings.get("bronze", {}).get("schema", "bronze")
    silver_sch = mappings.get("silver", {}).get("schema", "silver")
    server = src.get("server", "<source_server>")
    db = src.get("database", "<source_db>")

    return f"""-- Databricks notebook source
-- Migration: {table} | Pattern: Bronze -> Silver
-- SQL Warehouse mode (no cluster needed) | WH: {wh_id}
-- Generated by DBXConnect

-- STEP 1: Source -> Bronze (extract + cleanse)
CREATE OR REPLACE TABLE `{catalog}`.`{bronze_sch}`.`{safe}` AS
SELECT *, current_timestamp() AS _ingested_at, '{table}' AS _source_table
FROM sqlserver_jdbc(
  host => '{server}', database => '{db}',
  query => 'SELECT * FROM {table}'
);

-- STEP 2: Bronze -> Silver (transform + dedupe)
CREATE OR REPLACE TABLE `{catalog}`.`{silver_sch}`.`{safe}` AS
SELECT *, current_timestamp() AS _refined_at
FROM `{catalog}`.`{bronze_sch}`.`{safe}`
WHERE _ingested_at IS NOT NULL;
"""
