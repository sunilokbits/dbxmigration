"""Data Modeling blueprint — AI-driven star/snowflake schema builder."""
from flask import Blueprint, request, jsonify
import os, json, hashlib, requests as req

from .auth import login_required
from log_config import get_logger
from config_cache import get_config, get_databricks_token
from audit import log_action
from unity_catalog_executor import UnityCatalogExecutor
import data_modeling as dm
import persistence as db

logger = get_logger(__name__)
datamodel_bp = Blueprint("datamodel", __name__, url_prefix="/api/v1")

# In-memory model cache (also persisted to SQLite via persistence.py)
_DM_MODELS = {}


def _dm_get_warehouse(host, token):
    try:
        s = req.Session()
        s.headers.update({"Authorization": f"Bearer {token}"})
        resp = s.get(f"{host}/api/2.0/sql/warehouses", timeout=15)
        if resp.status_code == 200:
            whs = resp.json().get("warehouses", [])
            running = [w for w in whs if w.get("state") == "RUNNING"]
            if running:
                return running[0]["id"]
            if whs:
                return whs[0]["id"]
    except Exception:
        logger.warning("Could not fetch warehouse list from Databricks")
        pass
    return None


# ── Sample / Demo data ────────────────────────────────────────────────────────
_SAMPLE_TABLES_META = [
    {"table_name": "fact_sales", "columns": [
        {"name": "sale_id", "data_type": "BIGINT", "is_nullable": False, "is_pk": True},
        {"name": "customer_id", "data_type": "INT", "is_nullable": False},
        {"name": "product_id", "data_type": "INT", "is_nullable": False},
        {"name": "store_id", "data_type": "INT", "is_nullable": False},
        {"name": "order_date", "data_type": "DATE", "is_nullable": False},
        {"name": "quantity", "data_type": "INT", "is_nullable": True},
        {"name": "unit_price", "data_type": "DECIMAL(18,2)", "is_nullable": True},
        {"name": "total_amount", "data_type": "DECIMAL(18,2)", "is_nullable": True},
        {"name": "discount", "data_type": "FLOAT", "is_nullable": True},
    ]},
    {"table_name": "fact_orders", "columns": [
        {"name": "order_id", "data_type": "BIGINT", "is_nullable": False, "is_pk": True},
        {"name": "customer_id", "data_type": "INT", "is_nullable": False},
        {"name": "employee_id", "data_type": "INT", "is_nullable": False},
        {"name": "order_date", "data_type": "DATE", "is_nullable": False},
        {"name": "ship_date", "data_type": "DATE", "is_nullable": True},
        {"name": "freight", "data_type": "DECIMAL(10,2)", "is_nullable": True},
        {"name": "total_amount", "data_type": "DECIMAL(18,2)", "is_nullable": True},
    ]},
    {"table_name": "dim_customer", "columns": [
        {"name": "customer_id", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "first_name", "data_type": "STRING", "is_nullable": True},
        {"name": "last_name", "data_type": "STRING", "is_nullable": True},
        {"name": "email", "data_type": "STRING", "is_nullable": True},
        {"name": "phone", "data_type": "STRING", "is_nullable": True},
        {"name": "city", "data_type": "STRING", "is_nullable": True},
        {"name": "region_id", "data_type": "INT", "is_nullable": True},
    ]},
    {"table_name": "dim_product", "columns": [
        {"name": "product_id", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "product_name", "data_type": "STRING", "is_nullable": True},
        {"name": "category_id", "data_type": "INT", "is_nullable": True},
        {"name": "brand", "data_type": "STRING", "is_nullable": True},
        {"name": "unit_cost", "data_type": "DECIMAL(10,2)", "is_nullable": True},
    ]},
    {"table_name": "dim_store", "columns": [
        {"name": "store_id", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "store_name", "data_type": "STRING", "is_nullable": True},
        {"name": "city", "data_type": "STRING", "is_nullable": True},
        {"name": "state", "data_type": "STRING", "is_nullable": True},
        {"name": "region_id", "data_type": "INT", "is_nullable": True},
    ]},
    {"table_name": "dim_employee", "columns": [
        {"name": "employee_id", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "first_name", "data_type": "STRING", "is_nullable": True},
        {"name": "last_name", "data_type": "STRING", "is_nullable": True},
        {"name": "department_id", "data_type": "INT", "is_nullable": True},
        {"name": "hire_date", "data_type": "DATE", "is_nullable": True},
    ]},
    {"table_name": "dim_category", "columns": [
        {"name": "category_id", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "category_name", "data_type": "STRING", "is_nullable": True},
        {"name": "description", "data_type": "STRING", "is_nullable": True},
    ]},
    {"table_name": "dim_region", "columns": [
        {"name": "region_id", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "region_name", "data_type": "STRING", "is_nullable": True},
        {"name": "country", "data_type": "STRING", "is_nullable": True},
    ]},
    {"table_name": "dim_department", "columns": [
        {"name": "department_id", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "department_name", "data_type": "STRING", "is_nullable": True},
        {"name": "location", "data_type": "STRING", "is_nullable": True},
    ]},
    {"table_name": "dim_date", "columns": [
        {"name": "date_key", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "full_date", "data_type": "DATE", "is_nullable": False},
        {"name": "year", "data_type": "INT", "is_nullable": True},
        {"name": "quarter", "data_type": "INT", "is_nullable": True},
        {"name": "month", "data_type": "INT", "is_nullable": True},
        {"name": "month_name", "data_type": "STRING", "is_nullable": True},
        {"name": "day_of_week", "data_type": "STRING", "is_nullable": True},
    ]},
]


def _save_model(key, model):
    """Cache model in-memory; best-effort persist to Delta (never blocks on failure)."""
    _DM_MODELS[key] = model
    try:
        db.save_model(key, model)
    except Exception as exc:
        logger.warning("Model persistence skipped (no Databricks SQL connection): %s", exc)


def _get_model(key):
    """Retrieve model from in-memory cache; fallback to SQLite."""
    model = _DM_MODELS.get(key)
    if model is None:
        model = db.load_model(key)
        if model:
            _DM_MODELS[key] = model
    return model


@datamodel_bp.route("/datamodel/catalogs-schemas", methods=["GET"])
@login_required
def dm_list_catalogs_schemas():
    """List catalogs and schemas — live from Databricks if connected, else from config."""
    cfg = get_config()
    host = cfg.get("databricks_host", "").rstrip("/")
    token = get_databricks_token()

    # Try live Databricks Unity Catalog API
    if host and token:
        try:
            s = req.Session()
            s.headers.update({"Authorization": f"Bearer {token}"})
            # List catalogs
            cat_resp = s.get(f"{host}/api/2.1/unity-catalog/catalogs", timeout=15)
            if cat_resp.status_code == 200:
                catalogs = cat_resp.json().get("catalogs", [])
                result = []
                for cat in catalogs:
                    cat_name = cat.get("name", "")
                    if not cat_name or cat_name.startswith("__"):
                        continue
                    # List schemas for each catalog
                    sch_resp = s.get(f"{host}/api/2.1/unity-catalog/schemas?catalog_name={cat_name}", timeout=15)
                    if sch_resp.status_code == 200:
                        schemas = sch_resp.json().get("schemas", [])
                        for sch in schemas:
                            sch_name = sch.get("name", "")
                            if sch_name and not sch_name.startswith("__"):
                                result.append({"catalog": cat_name, "schema": sch_name})
                if result:
                    return jsonify({"success": True, "catalog_schemas": result, "source": "live"})
        except Exception as e:
            logger.warning("Live catalog listing failed, falling back to config: %s", e)

    # Fallback: read from static config
    catalogs_cfg = cfg.get("catalogs", {})
    result = []
    for cat_name, cat_cfg in catalogs_cfg.items():
        for sch in cat_cfg.get("schemas", []):
            result.append({"catalog": cat_name, "schema": sch})
    return jsonify({"success": True, "catalog_schemas": result, "source": "config"})


@datamodel_bp.route("/datamodel/sample-generate", methods=["POST"])
@login_required
def dm_sample_generate():
    d = request.get_json(force=True)
    table_names = d.get("tables", [])
    if not table_names:
        tables_meta = list(_SAMPLE_TABLES_META)
    else:
        tables_meta = [t for t in _SAMPLE_TABLES_META if t["table_name"] in table_names]
    if not tables_meta:
        return jsonify({"success": False, "error": "No matching sample tables found"})
    schema_choice = d.get("schema_choice", "auto")
    model = dm.classify_tables(tables_meta, schema_choice)
    er_json = dm.generate_er_json(model)
    ddl = dm.generate_ddl(model, "sample_catalog", "sample_schema")
    key = hashlib.md5(json.dumps([t["table_name"] for t in tables_meta], sort_keys=True).encode()).hexdigest()[:12]
    _save_model(key, model)
    return jsonify({
        "success": True, "model_id": key, "schema_type": model["schema_type"],
        "facts": model["facts"], "dimensions": model["dimensions"],
        "relationships": model["relationships"], "er_json": er_json, "ddl": ddl,
    })


@datamodel_bp.route("/datamodel/sample-tables", methods=["GET"])
@login_required
def dm_sample_tables():
    return jsonify({"success": True, "tables": [t["table_name"] for t in _SAMPLE_TABLES_META]})


@datamodel_bp.route("/datamodel/tables", methods=["POST"])
@login_required
def dm_list_tables():
    d = request.get_json(force=True)
    cfg = get_config()
    host = cfg.get("databricks_host", "").rstrip("/")
    token = get_databricks_token()
    catalog = d.get("catalog", "").strip()
    schema = d.get("schema", "").strip()
    if not host or not token:
        return jsonify({"success": False, "error": "Databricks not configured"})
    if not catalog or not schema:
        return jsonify({"success": False, "error": "Catalog and schema required"})
    executor = UnityCatalogExecutor(host, token, catalog, schema)
    wh_id = _dm_get_warehouse(host, token)
    tables = dm.list_available_tables(executor, catalog, schema, wh_id)
    return jsonify({"success": True, "tables": tables})


@datamodel_bp.route("/datamodel/tables-multi", methods=["POST"])
@login_required
def dm_list_tables_multi():
    """Load tables from multiple catalog.schema pairs at once."""
    d = request.get_json(force=True)
    cfg = get_config()
    host = cfg.get("databricks_host", "").rstrip("/")
    token = get_databricks_token()
    selections = d.get("selections", [])  # [{catalog, schema}, ...]
    if not host or not token:
        return jsonify({"success": False, "error": "Databricks not configured"})
    if not selections:
        return jsonify({"success": False, "error": "At least one catalog.schema pair required"})
    wh_id = _dm_get_warehouse(host, token)
    result = []
    for sel in selections:
        cat = sel.get("catalog", "").strip()
        sch = sel.get("schema", "").strip()
        if not cat or not sch:
            continue
        executor = UnityCatalogExecutor(host, token, cat, sch)
        tables = dm.list_available_tables(executor, cat, sch, wh_id)
        for t in tables:
            result.append({"table": t, "catalog": cat, "schema": sch, "fqn": f"{cat}.{sch}.{t}"})
    return jsonify({"success": True, "tables": result})


@datamodel_bp.route("/datamodel/generate-multi", methods=["POST"])
@login_required
def dm_generate_model_multi():
    """Generate model from tables across multiple catalog.schema pairs."""
    d = request.get_json(force=True)
    cfg = get_config()
    host = cfg.get("databricks_host", "").rstrip("/")
    token = get_databricks_token()
    # tables_selections: [{table, catalog, schema}, ...]
    tables_selections = d.get("tables_selections", [])
    if not host or not token:
        return jsonify({"success": False, "error": "Databricks not configured"})
    if not tables_selections:
        return jsonify({"success": False, "error": "Select at least one table"})
    wh_id = _dm_get_warehouse(host, token)
    tables_meta = []
    for sel in tables_selections:
        cat = sel.get("catalog", "").strip()
        sch = sel.get("schema", "").strip()
        tname = sel.get("table", "").strip()
        if not cat or not sch or not tname:
            continue
        executor = UnityCatalogExecutor(host, token, cat, sch)
        meta = dm.fetch_table_metadata(executor, cat, sch, [tname], wh_id)
        for m in meta:
            # Prefix table_name with catalog.schema for cross-schema clarity
            m["source_catalog"] = cat
            m["source_schema"] = sch
            m["fqn"] = f"{cat}.{sch}.{m['table_name']}"
            tables_meta.append(m)
    schema_choice = d.get("schema_choice", "auto")
    model = dm.classify_tables(tables_meta, schema_choice)
    er_json = dm.generate_er_json(model)
    # Use first catalog/schema for DDL or "multi" placeholder
    cats = list({s["catalog"] for s in tables_selections})
    schs = list({s["schema"] for s in tables_selections})
    ddl_cat = cats[0] if len(cats) == 1 else "multi_catalog"
    ddl_sch = schs[0] if len(schs) == 1 else "multi_schema"
    ddl = dm.generate_ddl(model, ddl_cat, ddl_sch)
    key = hashlib.md5(json.dumps([s.get("table","") for s in tables_selections], sort_keys=True).encode()).hexdigest()[:12]
    _save_model(key, model)
    return jsonify({
        "success": True, "model_id": key, "schema_type": model["schema_type"],
        "facts": model["facts"], "dimensions": model["dimensions"],
        "relationships": model["relationships"], "er_json": er_json, "ddl": ddl,
    })


@datamodel_bp.route("/datamodel/detect-changes", methods=["POST"])
@login_required
def dm_detect_changes():
    """Compare current model DDL against live Databricks schema to detect drift."""
    d = request.get_json(force=True)
    model_id = d.get("model_id", "")
    model = _get_model(model_id)
    if model is None:
        return jsonify({"success": False, "error": "Model not found"})
    cfg = get_config()
    host = cfg.get("databricks_host", "").rstrip("/")
    token = get_databricks_token()
    if not host or not token:
        return jsonify({"success": False, "error": "Databricks not configured"})
    wh_id = _dm_get_warehouse(host, token)
    changes = []
    all_tables = model.get("facts", []) + model.get("dimensions", [])
    for tbl in all_tables:
        tname = tbl["table_name"]
        cat = tbl.get("source_catalog") or d.get("catalog", "main")
        sch = tbl.get("source_schema") or d.get("schema", "default")
        executor = UnityCatalogExecutor(host, token, cat, sch)
        live_meta = dm.fetch_table_metadata(executor, cat, sch, [tname], wh_id)
        if not live_meta:
            changes.append({"table": tname, "type": "deleted", "detail": "Table no longer exists in source"})
            continue
        live_cols = {c["name"]: c for c in live_meta[0].get("columns", [])}
        model_cols = {c["name"]: c for c in tbl.get("columns", [])}
        # Detect added columns
        for cname in live_cols:
            if cname not in model_cols:
                changes.append({"table": tname, "type": "column_added", "detail": f"New column: {cname} ({live_cols[cname].get('data_type','?')})"})
        # Detect removed columns
        for cname in model_cols:
            if cname not in live_cols:
                changes.append({"table": tname, "type": "column_removed", "detail": f"Column removed: {cname}"})
        # Detect type changes
        for cname in live_cols:
            if cname in model_cols:
                live_type = live_cols[cname].get("data_type", "").upper()
                model_type = model_cols[cname].get("data_type", "").upper()
                if live_type != model_type:
                    changes.append({"table": tname, "type": "type_changed", "detail": f"{cname}: {model_type} → {live_type}"})
    return jsonify({"success": True, "changes": changes, "has_changes": len(changes) > 0})


@datamodel_bp.route("/datamodel/suggest-relationships", methods=["POST"])
@login_required
def dm_suggest_relationships():
    """AI-powered relationship suggestions based on column name/type analysis."""
    d = request.get_json(force=True)
    model_id = d.get("model_id", "")
    model = _get_model(model_id)
    if model is None:
        return jsonify({"success": False, "error": "Model not found"})
    suggestions = dm.suggest_relationships(model)
    return jsonify({"success": True, "suggestions": suggestions})


@datamodel_bp.route("/datamodel/generate", methods=["POST"])
@login_required
def dm_generate_model():
    d = request.get_json(force=True)
    cfg = get_config()
    host = cfg.get("databricks_host", "").rstrip("/")
    token = get_databricks_token()
    catalog = d.get("catalog", "").strip()
    schema = d.get("schema", "").strip()
    table_names = d.get("tables", [])
    if not host or not token:
        return jsonify({"success": False, "error": "Databricks not configured"})
    if not table_names:
        return jsonify({"success": False, "error": "Select at least one table"})
    executor = UnityCatalogExecutor(host, token, catalog, schema)
    wh_id = _dm_get_warehouse(host, token)
    tables_meta = dm.fetch_table_metadata(executor, catalog, schema, table_names, wh_id)
    schema_choice = d.get("schema_choice", "auto")
    model = dm.classify_tables(tables_meta, schema_choice)
    er_json = dm.generate_er_json(model)
    ddl = dm.generate_ddl(model, catalog, schema)
    key = hashlib.md5(json.dumps(table_names, sort_keys=True).encode()).hexdigest()[:12]
    _save_model(key, model)
    log_action("model_generated", "datamodel", key,
               {"schema_type": model["schema_type"], "tables": len(table_names)})
    return jsonify({
        "success": True, "model_id": key, "schema_type": model["schema_type"],
        "facts": model["facts"], "dimensions": model["dimensions"],
        "relationships": model["relationships"], "er_json": er_json, "ddl": ddl,
    })


@datamodel_bp.route("/datamodel/edit", methods=["POST"])
@login_required
def dm_edit_model():
    d = request.get_json(force=True)
    model_id = d.get("model_id", "")
    edits = d.get("edits", {})
    model = _get_model(model_id)
    if model is None:
        return jsonify({"success": False, "error": "Model not found. Please regenerate."})
    model = dm.apply_manual_edits(model, edits)
    _save_model(model_id, model)
    er_json = dm.generate_er_json(model)
    cfg = get_config()
    catalog = d.get("catalog", next(iter(cfg.get("catalogs", {})), "main"))
    schema = d.get("schema", "default")
    ddl = dm.generate_ddl(model, catalog, schema)
    return jsonify({
        "success": True, "model_id": model_id, "schema_type": model["schema_type"],
        "facts": model["facts"], "dimensions": model["dimensions"],
        "relationships": model["relationships"], "er_json": er_json, "ddl": ddl,
        "views": model.get("views", []),
    })


@datamodel_bp.route("/datamodel/ddl", methods=["POST"])
@login_required
def dm_get_ddl():
    d = request.get_json(force=True)
    model_id = d.get("model_id", "")
    catalog = d.get("catalog", "main")
    schema = d.get("schema", "default")
    model = _get_model(model_id)
    if model is None:
        return jsonify({"success": False, "error": "Model not found"})
    ddl = dm.generate_ddl(model, catalog, schema)
    return jsonify({"success": True, "ddl": ddl})


@datamodel_bp.route("/datamodel/views", methods=["POST"])
@login_required
def dm_list_views():
    """List views from a Databricks catalog.schema."""
    d = request.get_json(force=True)
    cfg = get_config()
    host = cfg.get("databricks_host", "").rstrip("/")
    token = get_databricks_token()
    catalog = d.get("catalog", "").strip()
    schema = d.get("schema", "").strip()
    if not host or not token:
        return jsonify({"success": False, "error": "Databricks not configured"})
    if not catalog or not schema:
        return jsonify({"success": False, "error": "Catalog and schema required"})
    executor = UnityCatalogExecutor(host, token, catalog, schema)
    wh_id = _dm_get_warehouse(host, token)
    views = dm.list_available_views(executor, catalog, schema, wh_id)
    return jsonify({"success": True, "views": views})


@datamodel_bp.route("/datamodel/metadata", methods=["POST"])
@login_required
def dm_save_metadata():
    """Save diagram metadata (author, scope, design version, etc.)."""
    d = request.get_json(force=True)
    metadata = d.get("metadata", {})
    # Store metadata in session-level model cache
    from migration_utility.config_cache import get_config
    import json, os
    cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(cache_dir, exist_ok=True)
    meta_path = os.path.join(cache_dir, "dm_metadata.json")
    try:
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


# ── Push to Azure DevOps (with PR-based approval) ────────────────────────────
@datamodel_bp.route("/datamodel/push-devops", methods=["POST"])
@login_required
def dm_push_devops():
    """Push DDL, ER diagram PNG, and model JSON to a feature branch, then create a PR for approval."""
    from flask import session as flask_session
    import time
    d = request.get_json(force=True)
    cfg = get_config()

    # Get DevOps config from request or deployconfig
    org = d.get("org") or cfg.get("devops_org", "")
    project = d.get("project") or cfg.get("devops_project", "")
    repo = d.get("repo") or cfg.get("devops_repo", "")
    target_branch = d.get("branch") or cfg.get("devops_branch", "main")

    # Reviewers
    reviewers = d.get("reviewers", [])
    if isinstance(reviewers, str):
        reviewers = [r.strip() for r in reviewers.split(",") if r.strip()]
    if not reviewers:
        configured_reviewers = cfg.get("devops_reviewers", "")
        if configured_reviewers:
            reviewers = [r.strip() for r in configured_reviewers.split(",") if r.strip()]

    # Push mode: "pr" (default) or "direct"
    push_mode = d.get("push_mode", "pr")

    # Resolve PAT: request body → config_cache (Key Vault → config fallback)
    from config_cache import get_devops_token
    pat = d.get("pat", "")
    from keyvault_helper import is_masked
    if not pat or is_masked(pat):
        pat = get_devops_token()

    if not org or not project or not repo:
        return jsonify({"success": False, "error": "Azure DevOps org, project, and repo are required. Configure in Settings."})
    if not pat:
        return jsonify({"success": False, "error": "Azure DevOps PAT is required. Store as 'devops-pat' in the Databricks secret scope or configure in Settings."})

    ddl = d.get("ddl", "")
    er_image_base64 = d.get("er_image_base64", "")
    model_json = d.get("model_json")
    commit_message = d.get("commit_message", "Update data model — DDL & ER diagram")
    folder_path = d.get("folder_path", "data_modeling").strip("/")
    model_name = d.get("model_name", "model").strip()

    # Sanitize model name for path
    safe_name = "".join(c if c.isalnum() or c in "_-" else "_" for c in model_name)
    target_folder = f"{folder_path}/{safe_name}"

    files = []
    if ddl:
        files.append({"path": f"{target_folder}/ddl.sql", "content": ddl, "encoding": "utf-8"})
    if er_image_base64:
        # Strip data URL prefix if present
        img_data = er_image_base64
        if "," in img_data:
            img_data = img_data.split(",", 1)[1]
        files.append({"path": f"{target_folder}/er_diagram.png", "content": img_data, "encoding": "base64"})
    if model_json:
        files.append({"path": f"{target_folder}/model.json", "content": json.dumps(model_json, indent=2), "encoding": "utf-8"})

    if not files:
        return jsonify({"success": False, "error": "No files to push (DDL or ER image required)"})

    try:
        from devops_connector import push_files_to_repo, create_pull_request

        if push_mode == "direct":
            # Direct push (no PR) — only for Admin role
            if flask_session.get("role") != "Admin":
                return jsonify({"success": False, "error": "Direct push requires Admin role. Use PR mode instead."})
            result = push_files_to_repo(org, project, repo, target_branch, files, commit_message, pat)
            result["mode"] = "direct"
            return jsonify(result)

        # PR mode: push to a timestamped feature branch, then create PR
        timestamp = int(time.time())
        feature_branch = f"data-modeling/{safe_name}-{timestamp}"

        # Push files to the feature branch
        push_result = push_files_to_repo(org, project, repo, feature_branch, files, commit_message, pat)

        # Create a Pull Request
        pr_title = f"[Data Model] {commit_message}"
        pr_description = (
            f"## Data Model Update: {model_name}\n\n"
            f"**Pushed by:** {flask_session.get('user', 'Migration Studio')}\n"
            f"**Files:**\n"
        )
        for f in files:
            pr_description += f"- `{f['path']}`\n"
        pr_description += f"\n---\n*Auto-generated by Migration Studio*"

        pr_result = create_pull_request(
            org=org,
            project=project,
            repo=repo,
            source_branch=feature_branch,
            target_branch=target_branch,
            title=pr_title,
            description=pr_description,
            reviewers=reviewers,
            pat=pat,
            auto_complete=d.get("auto_complete", True),
        )

        return jsonify({
            "success": True,
            "mode": "pr",
            "commit_id": push_result.get("commit_id", ""),
            "pr_id": pr_result.get("pr_id"),
            "pr_url": pr_result.get("web_url", ""),
            "feature_branch": feature_branch,
            "target_branch": target_branch,
            "reviewers": reviewers,
            "files_pushed": push_result.get("files_pushed", 0),
        })

    except Exception as e:
        logger.error("DevOps push failed: %s", e)
        return jsonify({"success": False, "error": str(e)})


@datamodel_bp.route("/datamodel/pr-status", methods=["POST"])
@login_required
def dm_pr_status():
    """Check the approval status of a Pull Request."""
    d = request.get_json(force=True)
    cfg = get_config()

    org = d.get("org") or cfg.get("devops_org", "")
    project = d.get("project") or cfg.get("devops_project", "")
    repo = d.get("repo") or cfg.get("devops_repo", "")
    pr_id = d.get("pr_id")

    from config_cache import get_devops_token
    pat = d.get("pat", "")
    from keyvault_helper import is_masked
    if not pat or is_masked(pat):
        pat = get_devops_token()

    if not pr_id:
        return jsonify({"success": False, "error": "pr_id is required"})

    try:
        from devops_connector import get_pull_request_status
        result = get_pull_request_status(org, project, repo, int(pr_id), pat)
        return jsonify(result)
    except Exception as e:
        logger.error("PR status check failed: %s", e)
        return jsonify({"success": False, "error": str(e)})


@datamodel_bp.route("/datamodel/test-devops", methods=["POST"])
@login_required
def dm_test_devops():
    """Test connection to Azure DevOps repo (read-only)."""
    d = request.get_json(force=True)
    cfg = get_config()

    org = d.get("org") or cfg.get("devops_org", "")
    project = d.get("project") or cfg.get("devops_project", "")
    repo = d.get("repo") or cfg.get("devops_repo", "")

    # Resolve PAT: request body → config_cache (Key Vault → config fallback)
    from config_cache import get_devops_token
    pat = d.get("pat", "")
    from keyvault_helper import is_masked
    if not pat or is_masked(pat):
        pat = get_devops_token()

    if not org or not project or not repo:
        return jsonify({"success": False, "error": "Organization, project, and repo are required."})
    if not pat:
        return jsonify({"success": False, "error": "PAT is required. Store as 'devops-pat' in the Databricks secret scope."})

    try:
        from devops_connector import test_connection
        result = test_connection(org, project, repo, pat)
        return jsonify(result)
    except Exception as e:
        logger.error("DevOps test connection failed: %s", e)
        return jsonify({"success": False, "error": str(e)})
