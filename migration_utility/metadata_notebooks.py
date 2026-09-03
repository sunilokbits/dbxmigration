"""
Metadata-Driven Medallion Notebooks
=====================================
Generates Databricks notebooks that read pipeline/job metadata from Delta tables
and dynamically execute the appropriate ETL stage.

Unlike the static `medallion_notebooks.py` (which embeds table lists in code),
these notebooks query the `wf_job_metadata` / `wf_pipeline_metadata` Delta tables
at runtime to determine WHAT to extract, WHERE to land, and HOW to transform.

The generated notebooks are idempotent — you deploy them ONCE, then trigger them
with parameters (job_id, run_id, load_type) from the Workflow Manager.

Notebooks produced:
  1. 00_Meta_Orchestrator.py  — reads metadata, chains Extract → Bronze → Silver
  2. 01_Meta_Extract.py       — JDBC extraction driven by metadata
  3. 02_Meta_Bronze.py        — Landing → Bronze (Delta) driven by metadata
  4. 03_Meta_Silver.py        — Bronze → Silver (Delta) driven by metadata
"""

import json
import os
from datetime import datetime


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DEPLOY CONFIG LOADER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _load_deploy_config() -> dict:
    """Load deployconfig.json from the same directory as this module."""
    cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "deployconfig.json")
    if os.path.isfile(cfg_path):
        with open(cfg_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PUBLIC API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_metadata_notebooks(
    catalog: str = "",
    schema: str = "",
    landing_path: str = "",
    workspace_path: str = "/Shared/MetadataPipeline",
    pipeline_mode: str = "standard",
    recon_catalog: str = "",
    recon_schema: str = "",
    recon_table: str = "",
    log_catalog: str = "",
    log_schema: str = "",
    log_table: str = "",
    recon_location: str = "",
    log_location: str = "",
    cdc_mode: str = "watermark",
    primary_keys: list = None,
) -> dict:
    """
    Generate metadata-driven notebooks.
    pipeline_mode: "standard" (4+2 notebooks) or "dlt" (3+2 notebooks with DLT).
    All parameters fall back to values from deployconfig.json when not provided.
    Returns: {success, notebooks: [{name, code, description, layer, lines}], summary}
    """
    # ── Load defaults from deployconfig.json ──────────────────────────
    cfg = _load_deploy_config()
    recon_cfg = cfg.get("reconciliation", {})
    log_cfg   = cfg.get("logging", {})

    catalog        = catalog        or cfg.get("catalogs", {}).get("bronze", {}).get("schemas", [""])[0] and "main"
    schema         = schema         or "default"
    landing_path   = landing_path   or cfg.get("volume_path", "/mnt/landing")
    recon_catalog  = recon_catalog  or recon_cfg.get("catalog", "reconciliation")
    recon_schema   = recon_schema   or recon_cfg.get("schema", "hr")
    recon_table    = recon_table    or recon_cfg.get("table", "ReconcilationDetails")
    recon_location = recon_location or recon_cfg.get("location", "")
    log_catalog    = log_catalog    or log_cfg.get("catalog", "logging")
    log_schema     = log_schema     or log_cfg.get("schema", "hr")
    log_table      = log_table      or log_cfg.get("table", "ExecutionLog")
    log_location   = log_location   or log_cfg.get("location", "")

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    if pipeline_mode == "dlt":
        notebooks = [
            {
                "name":        "01_Meta_Extract",
                "code":        _gen_extract(catalog, schema, landing_path, ts),
                "description": "Metadata-driven JDBC extraction → Landing Zone",
                "layer":       "extract",
            },
            {
                "name":        "02_Meta_SDP_Pipeline",
                "code":        _gen_dlt_pipeline(catalog, schema, landing_path, ts),
                "description": "Spark Declarative Pipeline — Bronze + Silver with Auto Loader & expectations",
                "layer":       "dlt",
            },
            {
                "name":        "00_Meta_Orchestrator",
                "code":        _gen_orchestrator_dlt(catalog, schema, landing_path, workspace_path, ts),
                "description": "SDP Orchestrator — Extract → Spark Declarative Pipeline trigger",
                "layer":       "orchestrator",
            },
            {
                "name":        "04_Meta_Reconciliation",
                "code":        _gen_reconciliation(catalog, schema, landing_path, recon_catalog, recon_schema, recon_table, ts, recon_location=recon_location),
                "description": "Aggregate reconciliation — Source vs Bronze numeric column validation",
                "layer":       "reconciliation",
            },
            {
                "name":        "05_Meta_ExecutionLog",
                "code":        _gen_execution_log(catalog, schema, log_catalog, log_schema, log_table, ts, log_location=log_location),
                "description": "Execution logging — saves per-job run details to logging catalog",
                "layer":       "logging",
            },
        ]
    else:
        notebooks = [
            {
                "name":        "01_Meta_Extract",
                "code":        _gen_extract(catalog, schema, landing_path, ts),
                "description": "Metadata-driven JDBC extraction → Landing Zone",
                "layer":       "extract",
            },
            {
                "name":        "02_Meta_Bronze",
                "code":        _gen_bronze(catalog, schema, landing_path, ts),
                "description": "Metadata-driven Landing → Bronze Delta layer",
                "layer":       "bronze",
            },
            {
                "name":        "03_Meta_Silver",
                "code":        _gen_silver(catalog, schema, ts),
                "description": "Metadata-driven Bronze → Silver Delta (cleansed)",
                "layer":       "silver",
            },
            {
                "name":        "00_Meta_Orchestrator",
                "code":        _gen_orchestrator(catalog, schema, landing_path, workspace_path, ts, recon_catalog, recon_schema, recon_table, log_catalog, log_schema, log_table),
                "description": "Orchestrator — reads metadata, chains all stages",
                "layer":       "orchestrator",
            },
            {
                "name":        "04_Meta_Reconciliation",
                "code":        _gen_reconciliation(catalog, schema, landing_path, recon_catalog, recon_schema, recon_table, ts, recon_location=recon_location),
                "description": "Aggregate reconciliation — Source vs Bronze numeric column validation",
                "layer":       "reconciliation",
            },
            {
                "name":        "05_Meta_ExecutionLog",
                "code":        _gen_execution_log(catalog, schema, log_catalog, log_schema, log_table, ts, log_location=log_location),
                "description": "Execution logging — saves per-job run details to logging catalog",
                "layer":       "logging",
            },
        ]

    for nb in notebooks:
        nb["lines"] = nb["code"].count("\n") + 1

    return {
        "success":   True,
        "notebooks": notebooks,
        "summary": {
            "total_notebooks":  len(notebooks),
            "catalog":          catalog,
            "schema":           schema,
            "landing_path":     landing_path,
            "workspace_path":   workspace_path,
            "pipeline_mode":    pipeline_mode,
            "multi_catalog":    "Supported — reads volumes_catalog, bronze_catalog, silver_catalog, target_schema from target_config at runtime",
            "generated_at":     ts,
        },
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  1. METADATA-DRIVEN EXTRACT NOTEBOOK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gen_extract(catalog, schema, landing_path, ts):
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 📥 Metadata-Driven Extract — Landing Zone
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC This notebook reads job metadata from Delta tables and extracts
# MAGIC the specified source table via JDBC into the Landing Zone.
# MAGIC
# MAGIC **Parameters (widgets):**
# MAGIC - `job_id` — The job to execute (looked up from `wf_job_metadata`)
# MAGIC - `run_id` — Run tracking ID (written to `wf_run_history`)
# MAGIC - `load_type` — `full` or `incremental` (override)
# MAGIC - `password_b64` — Base64-encoded source DB password (use Databricks Secrets in prod)
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Widget Configuration

# COMMAND ----------

dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("load_type", "", "Load Type Override (full/incremental)")
dbutils.widgets.text("password_b64", "", "Source DB Password (base64)")
dbutils.widgets.text("catalog", "{catalog}", "Metadata Catalog")
dbutils.widgets.text("schema", "{schema}", "Metadata Schema")
dbutils.widgets.text("landing_path", "{landing_path}", "Landing Base Path")

import base64

JOB_ID       = dbutils.widgets.get("job_id").strip()
RUN_ID       = dbutils.widgets.get("run_id").strip()
LOAD_OVERRIDE= dbutils.widgets.get("load_type").strip()
_PWD_B64     = dbutils.widgets.get("password_b64").strip()
# Decode base64 password — special chars like # ; {{ }} are safe this way
PASSWORD     = base64.b64decode(_PWD_B64.encode("ascii")).decode("utf-8") if _PWD_B64 else ""
CATALOG      = dbutils.widgets.get("catalog").strip()
SCHEMA       = dbutils.widgets.get("schema").strip()
LANDING_PATH = dbutils.widgets.get("landing_path").strip()

print(f"🔧 Job ID  : {{JOB_ID}}")
print(f"🔧 Run ID  : {{RUN_ID}}")
print(f"🔧 Catalog : {{CATALOG}}.{{SCHEMA}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Read Job Metadata from Delta

# COMMAND ----------

import json, re as _re
from pyspark.sql import functions as F
from datetime import datetime

def _sql_esc(val):
    """Escape a value for safe SQL string interpolation."""
    if val is None:
        return "NULL"
    s = str(val).replace("\\x00", "").replace("'", "''").replace("\\n", " ").replace("\\r", " ")
    return s[:500]

job_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata"
wm_tbl  = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_watermark_metadata"
run_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_run_history"

job_df = spark.sql(f"SELECT * FROM {{job_tbl}} WHERE job_id = '{{_sql_esc(JOB_ID)}}'")
if job_df.count() == 0:
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": f"Job {{JOB_ID}} not found in metadata"}}))

job = job_df.collect()[0].asDict()
print(f"📋 Job Name       : {{job['job_name']}}")
print(f"📋 Table           : {{job['full_table']}}")
print(f"📋 Stage           : {{job['stage']}}")
print(f"📋 Load Type (meta): {{job['load_type']}}")

# Parse source config (JSON string)
source_config = json.loads(job.get("source_config", "{{}}") or "{{}}")
SRC_TYPE = source_config.get("source_type", "sqlserver")
# Snowflake identifies itself by account, not "server" -- fall back to it
# so older metadata rows saved before that field was wired through don't
# end up with an empty connection target.
SERVER    = source_config.get("server", "") or (source_config.get("account", "") if SRC_TYPE == "snowflake" else "")
DATABASE = source_config.get("database", "")
USERNAME = source_config.get("username", "")
SF_ACCOUNT   = source_config.get("account", "") or SERVER
SF_WAREHOUSE = source_config.get("warehouse", "")
SF_ROLE      = source_config.get("role", "")
TABLE_SCHEMA = job["table_schema"]
TABLE_NAME   = job["table_name"]
FULL_TABLE   = job["full_table"]
LOAD_TYPE    = LOAD_OVERRIDE if LOAD_OVERRIDE else (job["load_type"] or "full")
WM_COL       = job.get("watermark_column", "")

# Multi-catalog: override landing path with UC Volumes
target_config = json.loads(job.get("target_config", "{{}}") or "{{}}")
VOLUMES_CATALOG = target_config.get("volumes_catalog", "")
TGT_SCHEMA      = target_config.get("target_schema", "")
if VOLUMES_CATALOG and TGT_SCHEMA:
    LANDING_PATH = f"/Volumes/{{VOLUMES_CATALOG}}/{{TGT_SCHEMA}}/landing"
    print(f"📦 Multi-catalog: Landing → UC Volumes: {{LANDING_PATH}}")
    # Auto-create schema and volume if they don't exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{{VOLUMES_CATALOG}}`.`{{TGT_SCHEMA}}`")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{{VOLUMES_CATALOG}}`.`{{TGT_SCHEMA}}`.`landing`")
    print(f"✅ Ensured volume exists: {{VOLUMES_CATALOG}}.{{TGT_SCHEMA}}.landing")

print(f"🔧 Source: {{SRC_TYPE}} → {{SERVER}}/{{DATABASE}}")
print(f"🔧 Load Type: {{LOAD_TYPE}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔌 JDBC Connection

# COMMAND ----------

IS_SNOWFLAKE = (SRC_TYPE == "snowflake")

if IS_SNOWFLAKE:
    # Snowflake identifies itself by account (e.g. xy12345.us-east-1), not
    # host:port -- no comma/colon port-splitting needed.
    print(f"🔧 JDBC target: Snowflake account {{SF_ACCOUNT}}")
    _sf_url_params = [f"db={{DATABASE}}"] if DATABASE else []
    if SF_WAREHOUSE:
        _sf_url_params.append(f"warehouse={{SF_WAREHOUSE}}")
    if SF_ROLE:
        _sf_url_params.append(f"role={{SF_ROLE}}")
    jdbc_url = f"jdbc:snowflake://{{SF_ACCOUNT}}.snowflakecomputing.com/?" + "&".join(_sf_url_params)
    jdbc_props = {{
        "user":     USERNAME,
        "password": PASSWORD,
        "driver":   "net.snowflake.client.jdbc.SnowflakeDriver",
        "loginTimeout": "60",
    }}

    def _qtbl(sch, tbl):
        return f'"{{sch}}"."{{tbl}}"'

    def _qcol(col):
        return f'"{{col}}"'
else:
    encrypt = "true" if SRC_TYPE in ("azuresql", "synapse") else "false"
    trust   = "false" if SRC_TYPE in ("azuresql", "synapse") else "true"

    # Normalize server address to hostname:port for JDBC
    # Azure SQL often uses comma notation (server.database.windows.net,1433) but
    # the JDBC driver only accepts colon notation (server:1433) in the URL.
    if "," in SERVER:
        _host, _port = SERVER.rsplit(",", 1)
    elif ":" in SERVER:
        _host, _port = SERVER.rsplit(":", 1)
    else:
        _host, _port = SERVER, "1433"
    print(f"🔧 JDBC target: {{_host}}:{{_port}}")

    jdbc_url = (
        f"jdbc:sqlserver://{{_host}}:{{_port}};databaseName={{DATABASE}};"
        f"encrypt={{encrypt}};trustServerCertificate={{trust}};"
        f"loginTimeout=60;socketTimeout=0;selectMethod=cursor"
    )

    jdbc_props = {{
        "user":     USERNAME,
        "password": PASSWORD,
        "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "fetchsize": "10000",
        "queryTimeout": "0",
        "loginTimeout": "60",
        "socketTimeout": "0",
    }}

    def _qtbl(sch, tbl):
        return f"[{{sch}}].[{{tbl}}]"

    def _qcol(col):
        return f"[{{col}}]"

# Verify JDBC connectivity
try:
    test_df = spark.read.jdbc(jdbc_url, "(SELECT 1 AS ok) AS t", properties=jdbc_props)
    test_df.collect()
    print("✅ JDBC connection verified")
except Exception as e:
    msg = f"❌ JDBC connection failed: {{e}}"
    print(msg)
    try:
        spark.sql(f"""
            MERGE INTO {{run_tbl}} AS t
            USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'failed',
                t.error_message = '{{_sql_esc(e)}}',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "stage": "connection", "error": (lambda _m: _m[_m.rfind("Caused by:"):] if "Caused by:" in _m else _m)(str(e))[:2000]}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Read Watermark (Incremental)

# COMMAND ----------

watermark = None
use_incremental = (LOAD_TYPE == "incremental" and WM_COL)

if use_incremental:
    try:
        wm_df = spark.sql(f"SELECT last_value FROM {{wm_tbl}} WHERE table_name = '{{FULL_TABLE}}'")
        rows = wm_df.collect()
        if rows and rows[0]["last_value"]:
            watermark = rows[0]["last_value"]
            print(f"🔄 Watermark found: {{WM_COL}} > '{{watermark}}'")
        else:
            print("🔄 No watermark — will do initial full load")
    except Exception:
        print("🔄 Watermark table not found — will do full load")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Extract Data

# COMMAND ----------

run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
landing_dest = f"{{LANDING_PATH}}/{{TABLE_NAME}}"

# Build query
_qualified_table = _qtbl(TABLE_SCHEMA, TABLE_NAME)
if use_incremental and watermark:
    _esc_wm = _sql_esc(watermark)
    query = f"(SELECT * FROM {{_qualified_table}} WHERE {{_qcol(WM_COL)}} > '{{_esc_wm}}') AS q"
    print(f"📥 Incremental extract: {{WM_COL}} > '{{_esc_wm}}'")
else:
    query = _qualified_table
    print(f"📥 Full extract from {{_qualified_table}}")

# Read from source
try:
    # For large tables (>100K rows), use partitioned JDBC read to avoid
    # driver memory pressure.  We estimate row count first with a fast
    # COUNT query, then use numPartitions if it's a big table.
    _est_count = 0
    try:
        _cnt_q = f"(SELECT COUNT(1) AS cnt FROM {{_qualified_table}}) AS cq"
        _est_count = spark.read.jdbc(jdbc_url, _cnt_q, properties=jdbc_props).collect()[0][0]
        print(f"📊 Estimated row count: {{_est_count:,}}")
    except Exception:
        pass

    _num_partitions = 1
    if _est_count > 500000:
        _num_partitions = 8
    elif _est_count > 100000:
        _num_partitions = 4

    if _num_partitions > 1:
        jdbc_props["numPartitions"] = str(_num_partitions)
        print(f"📊 Using {{_num_partitions}} JDBC partitions for large table")

    df = spark.read.jdbc(jdbc_url, query, properties=jdbc_props)

    # Add audit columns
    df = (df
          .withColumn("__landing_ts", F.current_timestamp())
          .withColumn("__source_system", F.lit(f"{{SERVER}}/{{DATABASE}}"))
          .withColumn("__load_type", F.lit("incremental" if use_incremental and watermark else "full"))
          .withColumn("__batch_id", F.lit(run_ts))
          .withColumn("__job_id", F.lit(JOB_ID))
          .withColumn("__run_id", F.lit(RUN_ID)))

    row_count = df.count()
    print(f"📊 Rows extracted: {{row_count:,}}")

    # Pre-compute watermark BEFORE writing so we capture the exact max
    # from this batch.  Saved AFTER write succeeds to avoid data gaps.
    new_wm = None
    if use_incremental and WM_COL and row_count > 0:
        try:
            new_wm = df.agg(F.max(F.col(WM_COL)).cast("string")).collect()[0][0]
            if new_wm:
                print(f"📏 Pre-computed watermark: {{WM_COL}} → {{new_wm}}")
        except Exception as wm_err:
            print(f"⚠️ Watermark pre-compute failed (will skip update): {{wm_err}}")

    # Write to landing zone
    if LOAD_TYPE == "full" or not (use_incremental and watermark):
        df.write.mode("overwrite").option("overwriteSchema", "true").parquet(landing_dest)
        print(f"✅ Written to {{landing_dest}} (overwrite)")
    else:
        # Overwrite landing even for incremental — watermark guarantees we
        # only pull new rows, so landing is a transient staging area.
        # Appending would cause duplication when Bronze reads all files.
        df.write.mode("overwrite").option("overwriteSchema", "true").parquet(landing_dest)
        print(f"✅ Written to {{landing_dest}} (overwrite — watermark controls incrementality)")

except Exception as e:
    msg = f"❌ Extract failed: {{e}}"
    print(msg)
    try:
        spark.sql(f"""
            MERGE INTO {{run_tbl}} AS t
            USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'failed',
                t.error_message = '{{_sql_esc(e)}}',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "stage": "extract", "error": (lambda _m: _m[_m.rfind("Caused by:"):] if "Caused by:" in _m else _m)(str(e))[:2000]}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Update Watermark & Run History

# COMMAND ----------

# Update watermark if incremental (using pre-computed value)
if use_incremental and WM_COL and row_count > 0 and new_wm:
    try:
        spark.sql(f"""
            MERGE INTO {{wm_tbl}} AS t
            USING (SELECT '{{FULL_TABLE}}' AS table_name, '{{new_wm}}' AS last_value, '{{WM_COL}}' AS watermark_column, current_timestamp() AS updated_at) AS s
            ON t.table_name = s.table_name
            WHEN MATCHED THEN UPDATE SET t.last_value = s.last_value, t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"💾 Watermark updated: {{WM_COL}} → {{new_wm}}")
    except Exception as e:
        print(f"⚠️ Watermark update failed: {{e}}")

# Update run history
try:
    spark.sql(f"""
        MERGE INTO {{run_tbl}} AS t
        USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET
            t.status = 'success',
            t.rows_processed = {{row_count}},
            t.completed_at = current_timestamp(),
            t.duration_sec = unix_timestamp(current_timestamp()) - unix_timestamp(t.started_at)
    """)
except Exception as e:
    print(f"⚠️ Run history update failed: {{e}}")

# Update job metadata
try:
    spark.sql(f"""
        UPDATE {{job_tbl}}
        SET last_run_id = '{{RUN_ID}}',
            last_run_at = current_timestamp(),
            last_status = 'success',
            status = 'success',
            run_count = run_count + 1,
            updated_at = current_timestamp()
        WHERE job_id = '{{JOB_ID}}'
    """)
except Exception as e:
    print(f"⚠️ Job metadata update failed: {{e}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Summary

# COMMAND ----------

exit_payload = json.dumps({{
    "status":       "COMPLETED",
    "job_id":       JOB_ID,
    "run_id":       RUN_ID,
    "table":        FULL_TABLE,
    "rows":         row_count,
    "load_type":    LOAD_TYPE,
    "landing_path": landing_dest,
    "batch_id":     run_ts,
}})

print(f"\\n✅ EXTRACT COMPLETE — {{FULL_TABLE}} — {{row_count:,}} rows")
dbutils.notebook.exit(exit_payload)
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  2. METADATA-DRIVEN BRONZE NOTEBOOK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gen_bronze(catalog, schema, landing_path, ts):
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Metadata-Driven Bronze Layer
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC Reads raw Parquet from Landing Zone, applies schema enforcement,
# MAGIC adds audit columns, and writes to Bronze Delta table.
# MAGIC Driven by `wf_job_metadata` — no hardcoded table names.
# MAGIC ---

# COMMAND ----------

dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("catalog", "{catalog}", "Metadata Catalog")
dbutils.widgets.text("schema", "{schema}", "Metadata Schema")
dbutils.widgets.text("landing_path", "{landing_path}", "Landing Base Path")

JOB_ID       = dbutils.widgets.get("job_id").strip()
RUN_ID       = dbutils.widgets.get("run_id").strip()
CATALOG      = dbutils.widgets.get("catalog").strip()
SCHEMA       = dbutils.widgets.get("schema").strip()
LANDING_PATH = dbutils.widgets.get("landing_path").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Read Job Metadata

# COMMAND ----------

import json, re as _re
from pyspark.sql import functions as F
from datetime import datetime

def _sql_esc(val):
    """Escape a value for safe SQL string interpolation."""
    if val is None:
        return "NULL"
    s = str(val).replace("\\x00", "").replace("'", "''").replace("\\n", " ").replace("\\r", " ")
    return s[:500]

job_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata"
run_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_run_history"

job_df = spark.sql(f"SELECT * FROM {{job_tbl}} WHERE job_id = '{{JOB_ID}}'")
if job_df.count() == 0:
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": f"Job {{JOB_ID}} not found"}}))

job = job_df.collect()[0].asDict()
TABLE_NAME   = job["table_name"]
TABLE_SCHEMA = job["table_schema"]
FULL_TABLE   = job["full_table"]
LOAD_TYPE    = job.get("load_type", "full") or "full"

target_config = json.loads(job.get("target_config", "{{}}") or "{{}}")
VOLUMES_CATALOG = target_config.get("volumes_catalog", "")
BRONZE_CATALOG  = target_config.get("bronze_catalog", "")
TGT_SCHEMA      = target_config.get("target_schema", "")

# ─── Auto-derive bronze_catalog if missing ────────────────────────────────────
if not BRONZE_CATALOG:
    BRONZE_CATALOG = "bronze"
    print(f"⚠️  bronze_catalog not set — defaulting to '{{BRONZE_CATALOG}}'")
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{{BRONZE_CATALOG}}`")
    if TGT_SCHEMA:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{{BRONZE_CATALOG}}`.`{{TGT_SCHEMA}}`")
except Exception as _ce:
    _ce_msg = f"Could not auto-create bronze catalog/schema '{{BRONZE_CATALOG}}': {{_ce}}"
    print(f"❌ {{_ce_msg}}")
    # Verify the catalog exists — if not, fail fast instead of hiding the error
    try:
        _cat_check = spark.sql(f"SHOW CATALOGS LIKE '{{BRONZE_CATALOG}}'").count()
        if _cat_check == 0:
            dbutils.notebook.exit(json.dumps({{
                "status": "FAILED",
                "stage": "catalog_validation",
                "error": _ce_msg[:500]
            }}))
    except Exception:
        pass  # If we can't even check, continue and let downstream fail with clearer error

MULTI_CATALOG   = bool(VOLUMES_CATALOG and BRONZE_CATALOG and TGT_SCHEMA)

if MULTI_CATALOG:
    TARGET_CATALOG = BRONZE_CATALOG
    TARGET_SCHEMA  = TGT_SCHEMA
    TABLE_PREFIX   = ""
    LANDING_PATH   = f"/Volumes/{{VOLUMES_CATALOG}}/{{TGT_SCHEMA}}/landing"
    print(f"✅ Multi-catalog medallion: {{VOLUMES_CATALOG}} → {{BRONZE_CATALOG}}.{{TGT_SCHEMA}} (no prefix)")
else:
    _fallback_cat = target_config.get("catalog", "")
    _meta_cat = target_config.get("metadata_catalog", CATALOG)
    if _fallback_cat and _fallback_cat != _meta_cat and _fallback_cat != CATALOG:
        TARGET_CATALOG = _fallback_cat
    elif BRONZE_CATALOG:
        TARGET_CATALOG = BRONZE_CATALOG
    else:
        TARGET_CATALOG = CATALOG
        print(f"⚠️ WARNING: No bronze_catalog — falling back to metadata catalog {{CATALOG}}")
    _fallback_sch = target_config.get("schema", "")
    _meta_sch = target_config.get("metadata_schema", SCHEMA)
    if _fallback_sch and _fallback_sch != _meta_sch and _fallback_sch != SCHEMA:
        TARGET_SCHEMA = _fallback_sch
    elif TGT_SCHEMA:
        TARGET_SCHEMA = TGT_SCHEMA
    else:
        TARGET_SCHEMA = SCHEMA
    TABLE_PREFIX   = "bronze_"

print(f"📋 Job: {{job['job_name']}}")
print(f"📋 Table: {{FULL_TABLE}}")
print(f"📋 Target: {{TARGET_CATALOG}}.{{TARGET_SCHEMA}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📂 Read from Landing Zone

# COMMAND ----------

landing_src = f"{{LANDING_PATH}}/{{TABLE_NAME}}"
print(f"📂 Reading from: {{landing_src}}")

try:
    df = spark.read.parquet(landing_src)
    row_count = df.count()
    print(f"📊 Rows in landing: {{row_count:,}}")
except Exception as e:
    msg = f"❌ Failed to read landing zone: {{e}}"
    print(msg)
    try:
        spark.sql(f"""
            MERGE INTO {{run_tbl}} AS t
            USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'failed',
                t.error_message = '{{_sql_esc(e)}}',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": (lambda _m: _m[_m.rfind("Caused by:"):] if "Caused by:" in _m else _m)(str(e))[:2000]}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Schema Enforcement & Data Quality Checks

# COMMAND ----------

# Create restore point if table exists
restore_version = None
bronze_table = f"`{{TARGET_CATALOG}}`.`{{TARGET_SCHEMA}}`.`{{TABLE_PREFIX}}{{TABLE_NAME}}`"
try:
    history = spark.sql(f"DESCRIBE HISTORY {{bronze_table}} LIMIT 1").collect()
    if history:
        restore_version = history[0]["version"]
        print(f"📌 Restore point: v{{restore_version}}")
except Exception:
    print("📌 No existing table — first load")

# ── DQ-01: Empty file check ─────────────────────────────────────────
if row_count == 0:
    print("⚠️ DQ-01: Landing file has 0 rows — skipping Bronze write")
    try:
        spark.sql(f"""
            MERGE INTO {{run_tbl}} AS t
            USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'skipped',
                t.error_message = 'Empty landing file — 0 rows',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({{"status": "SKIPPED", "reason": "empty_landing", "rows": 0}}))

# ── DQ-02: Null-key row detection (all data cols null) ──────────────
audit_cols = [c for c in df.columns if c.startswith("__")]
data_cols  = [c for c in df.columns if c not in audit_cols]
null_key_count = 0
if data_cols:
    null_expr = data_cols[0]
    all_null = F.col(data_cols[0]).isNull()
    for dc in data_cols[1:]:
        all_null = all_null & F.col(dc).isNull()
    null_key_count = df.filter(all_null).count()
    if null_key_count > 0:
        print(f"⚠️ DQ-02: {{null_key_count}} all-null rows detected")

# ── DQ-03: Duplicate detection ──────────────────────────────────────
dup_count = row_count - df.dropDuplicates(data_cols).count() if data_cols else 0
if dup_count > 0:
    print(f"⚠️ DQ-03: {{dup_count}} duplicate rows detected")

# ── DQ-04: Schema drift detection ───────────────────────────────────
schema_drift = False
try:
    existing = spark.sql(f"DESCRIBE {{bronze_table}}").select("col_name").rdd.flatMap(lambda x: x).collect()
    existing_data_cols = [c for c in existing if not c.startswith("__") and not c.startswith("#")]
    incoming_data_cols = [c for c in df.columns if not c.startswith("__")]
    new_cols     = set(incoming_data_cols) - set(existing_data_cols)
    dropped_cols = set(existing_data_cols) - set(incoming_data_cols)
    if new_cols:
        schema_drift = True
        print(f"⚠️ DQ-04 Schema drift — new columns: {{new_cols}}")
    if dropped_cols:
        schema_drift = True
        print(f"⚠️ DQ-04 Schema drift — missing columns: {{dropped_cols}}")
except Exception:
    pass  # Table doesn't exist yet

# ── DQ-05: Quarantine flagging ──────────────────────────────────────
# Flag rows with all-null data columns as quarantined instead of dropping
if data_cols:
    all_null_expr = F.col(data_cols[0]).isNull()
    for dc in data_cols[1:]:
        all_null_expr = all_null_expr & F.col(dc).isNull()
    is_quarantined = all_null_expr
else:
    is_quarantined = F.lit(False)

# Add audit columns
df_bronze = (df
    .withColumn("__bronze_ts", F.current_timestamp())
    .withColumn("__bronze_version", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    .withColumn("__source_table", F.lit(FULL_TABLE))
    .withColumn("__job_id", F.lit(JOB_ID))
    .withColumn("__run_id", F.lit(RUN_ID))
    .withColumn("__is_quarantined", is_quarantined)
)

quarantined_count = df_bronze.filter(F.col("__is_quarantined") == True).count()
clean_count = row_count - quarantined_count

print(f"\\n📊 Bronze DQ Summary:")
print(f"   Total rows      : {{row_count:,}}")
print(f"   Clean rows      : {{clean_count:,}}")
print(f"   Quarantined     : {{quarantined_count:,}}")
print(f"   Null-key rows   : {{null_key_count}}")
print(f"   Duplicates      : {{dup_count}}")
print(f"   Schema drift    : {{'Yes' if schema_drift else 'No'}}")

# ── Bronze DQ metrics are saved AFTER the Delta write succeeds ─────
# (moved below the write so a failed load can never record a passing score)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Write to Bronze Delta

# COMMAND ----------

try:
    if LOAD_TYPE == "full":
        (df_bronze.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(bronze_table))
        print(f"✅ Full load → {{bronze_table}} ({{row_count:,}} rows)")
    else:
        (df_bronze.write
            .format("delta")
            .mode("append")
            .saveAsTable(bronze_table))
        print(f"✅ Append → {{bronze_table}} ({{row_count:,}} rows)")

    # ── Save Bronze DQ metrics (only after a successful write) ─────────
    try:
        dq_tbl = f"`{{TARGET_CATALOG}}`.`{{TARGET_SCHEMA}}`.__dq_metrics"
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {{dq_tbl}} (
                run_id STRING, job_id STRING, table_name STRING, layer STRING,
                input_rows BIGINT, output_rows BIGINT, rejected_rows BIGINT,
                null_rows BIGINT, dupe_rows BIGINT, quarantined_rows BIGINT,
                schema_drift BOOLEAN, dq_checks_passed INT, dq_checks_total INT,
                dq_score DOUBLE, checked_at TIMESTAMP
            ) USING DELTA
        """)
        checks_passed = sum([1 for c in [row_count > 0, null_key_count == 0, dup_count < row_count * 0.5, not schema_drift] if c])
        checks_total = 4
        dq_score = round(checks_passed / checks_total * 100, 1)
        spark.sql(f"""
            INSERT INTO {{dq_tbl}} VALUES (
                '{{RUN_ID}}', '{{JOB_ID}}', '{{FULL_TABLE}}', 'bronze',
                {{row_count}}, {{clean_count}}, {{quarantined_count}},
                {{null_key_count}}, {{dup_count}}, {{quarantined_count}},
                {{'true' if schema_drift else 'false'}}, {{checks_passed}}, {{checks_total}},
                {{dq_score}}, current_timestamp()
            )
        """)
    except Exception as dq_e:
        print(f"⚠️ DQ metrics save failed: {{dq_e}}")

except Exception as e:
    # Attempt restore on failure
    if restore_version is not None:
        try:
            spark.sql(f"RESTORE TABLE {{bronze_table}} TO VERSION AS OF {{restore_version}}")
            print(f"🔄 Restored to v{{restore_version}} after failure")
        except Exception:
            pass
    # Record the FAILED run in __dq_metrics so the dashboard shows it honestly
    try:
        dq_tbl = f"`{{TARGET_CATALOG}}`.`{{TARGET_SCHEMA}}`.__dq_metrics"
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {{dq_tbl}} (
                run_id STRING, job_id STRING, table_name STRING, layer STRING,
                input_rows BIGINT, output_rows BIGINT, rejected_rows BIGINT,
                null_rows BIGINT, dupe_rows BIGINT, quarantined_rows BIGINT,
                schema_drift BOOLEAN, dq_checks_passed INT, dq_checks_total INT,
                dq_score DOUBLE, checked_at TIMESTAMP
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {{dq_tbl}} VALUES (
                '{{RUN_ID}}', '{{JOB_ID}}', '{{FULL_TABLE}}', 'bronze',
                {{row_count}}, 0, 0, 0, 0, 0, false, 0, 4, 0.0, current_timestamp()
            )
        """)
    except Exception:
        pass
    try:
        spark.sql(f"""
            MERGE INTO {{run_tbl}} AS t
            USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'failed',
                t.error_message = '{{_sql_esc(e)}}',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": (lambda _m: _m[_m.rfind("Caused by:"):] if "Caused by:" in _m else _m)(str(e))[:2000]}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Update Metadata

# COMMAND ----------

try:
    spark.sql(f"""
        MERGE INTO {{run_tbl}} AS t
        USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET
            t.status = 'success',
            t.rows_processed = {{row_count}},
            t.completed_at = current_timestamp(),
            t.duration_sec = unix_timestamp(current_timestamp()) - unix_timestamp(t.started_at)
    """)
except Exception as e:
    print(f"⚠️ Run history update failed: {{e}}")

try:
    spark.sql(f"""
        UPDATE {{job_tbl}}
        SET last_run_id = '{{RUN_ID}}', last_run_at = current_timestamp(),
            last_status = 'success', status = 'success',
            run_count = run_count + 1, updated_at = current_timestamp()
        WHERE job_id = '{{JOB_ID}}'
    """)
except Exception as e:
    print(f"⚠️ Job update failed: {{e}}")

# COMMAND ----------

exit_payload = json.dumps({{
    "status": "COMPLETED", "job_id": JOB_ID, "run_id": RUN_ID,
    "table": FULL_TABLE, "rows": row_count, "bronze_table": bronze_table,
}})
print(f"\\n✅ BRONZE COMPLETE — {{FULL_TABLE}} — {{row_count:,}} rows")
dbutils.notebook.exit(exit_payload)
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  3. METADATA-DRIVEN SILVER NOTEBOOK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gen_silver(catalog, schema, ts):
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Metadata-Driven Silver Layer
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC Reads Bronze Delta, applies data quality checks, deduplication,
# MAGIC cleansing, and writes to Silver Delta table.
# MAGIC ---

# COMMAND ----------

dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("catalog", "{catalog}", "Metadata Catalog")
dbutils.widgets.text("schema", "{schema}", "Metadata Schema")

JOB_ID  = dbutils.widgets.get("job_id").strip()
RUN_ID  = dbutils.widgets.get("run_id").strip()
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA  = dbutils.widgets.get("schema").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Read Job Metadata

# COMMAND ----------

import json, re as _re
from pyspark.sql import functions as F
from datetime import datetime

def _sql_esc(val):
    \"\"\"Escape a value for safe SQL string interpolation.\"\"\"
    if val is None:
        return "NULL"
    s = str(val).replace("\\x00", "").replace("'", "''").replace("\\n", " ").replace("\\r", " ")
    return s[:500]

job_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata"
run_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_run_history"

job_df = spark.sql(f"SELECT * FROM {{job_tbl}} WHERE job_id = '{{_sql_esc(JOB_ID)}}'")
if job_df.count() == 0:
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": f"Job {{JOB_ID}} not found"}}))

job = job_df.collect()[0].asDict()
TABLE_NAME   = job["table_name"]
FULL_TABLE   = job["full_table"]
LOAD_TYPE    = job.get("load_type", "full") or "full"

target_config = json.loads(job.get("target_config", "{{}}") or "{{}}")
BRONZE_CATALOG = target_config.get("bronze_catalog", "")
SILVER_CATALOG = target_config.get("silver_catalog", "")
TGT_SCHEMA     = target_config.get("target_schema", "")

# ─── Auto-derive silver_catalog if missing/duplicate (medallion separation) ───
def _derive_silver_catalog(bronze_cat: str) -> str:
    if not bronze_cat:
        return "silver"
    low = bronze_cat.lower()
    if "bronze" in low:
        return bronze_cat.replace("bronze", "silver").replace("BRONZE", "SILVER").replace("Bronze", "Silver")
    return "silver"

if not SILVER_CATALOG or SILVER_CATALOG == BRONZE_CATALOG:
    SILVER_CATALOG = _derive_silver_catalog(BRONZE_CATALOG)
    print(f"⚠️  silver_catalog auto-derived as '{{SILVER_CATALOG}}' to keep silver SEPARATE from bronze.")
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{{SILVER_CATALOG}}`")
    if TGT_SCHEMA:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{{SILVER_CATALOG}}`.`{{TGT_SCHEMA}}`")
except Exception as _ce:
    print(f"⚠️  Could not auto-create silver catalog/schema: {{_ce}}")

MULTI_CATALOG  = bool(BRONZE_CATALOG and SILVER_CATALOG and TGT_SCHEMA and BRONZE_CATALOG != SILVER_CATALOG)

if MULTI_CATALOG:
    bronze_table = f"`{{BRONZE_CATALOG}}`.`{{TGT_SCHEMA}}`.`{{TABLE_NAME}}`"
    silver_table = f"`{{SILVER_CATALOG}}`.`{{TGT_SCHEMA}}`.`{{TABLE_NAME}}`"
    DQ_CATALOG   = SILVER_CATALOG
    DQ_SCHEMA    = TGT_SCHEMA
    print(f"✅ Multi-catalog medallion: {{BRONZE_CATALOG}}.{{TGT_SCHEMA}} → {{SILVER_CATALOG}}.{{TGT_SCHEMA}} (no prefix)")
else:
    _fallback_cat = target_config.get("catalog", "")
    _meta_cat = target_config.get("metadata_catalog", CATALOG)
    if _fallback_cat and _fallback_cat != _meta_cat and _fallback_cat != CATALOG:
        TARGET_CATALOG = _fallback_cat
    elif SILVER_CATALOG:
        TARGET_CATALOG = SILVER_CATALOG
    elif BRONZE_CATALOG:
        TARGET_CATALOG = BRONZE_CATALOG
    else:
        TARGET_CATALOG = CATALOG
        print(f"⚠️ WARNING: No silver/bronze_catalog — falling back to metadata catalog {{CATALOG}}")
    _fallback_sch = target_config.get("schema", "")
    _meta_sch = target_config.get("metadata_schema", SCHEMA)
    if _fallback_sch and _fallback_sch != _meta_sch and _fallback_sch != SCHEMA:
        TARGET_SCHEMA = _fallback_sch
    elif TGT_SCHEMA:
        TARGET_SCHEMA = TGT_SCHEMA
    else:
        TARGET_SCHEMA = SCHEMA
    bronze_table = f"`{{TARGET_CATALOG}}`.`{{TARGET_SCHEMA}}`.`bronze_{{TABLE_NAME}}`"
    silver_table = f"`{{TARGET_CATALOG}}`.`{{TARGET_SCHEMA}}`.`silver_{{TABLE_NAME}}`"
    DQ_CATALOG   = TARGET_CATALOG
    DQ_SCHEMA    = TARGET_SCHEMA

print(f"📋 Job: {{job['job_name']}}")
print(f"📋 Bronze: {{bronze_table}}")
print(f"📋 Silver: {{silver_table}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📂 Read from Bronze

# COMMAND ----------

try:
    df = spark.read.table(bronze_table)
    initial_count = df.count()
    print(f"📊 Bronze rows: {{initial_count:,}}")
except Exception as e:
    try:
        spark.sql(f"""
            MERGE INTO {{run_tbl}} AS t
            USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'failed',
                t.error_message = '{{_sql_esc(e)}}',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": (lambda _m: _m[_m.rfind("Caused by:"):] if "Caused by:" in _m else _m)(str(e))[:2000]}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Data Quality Checks

# COMMAND ----------

# Create restore point
restore_version = None
try:
    history = spark.sql(f"DESCRIBE HISTORY {{silver_table}} LIMIT 1").collect()
    if history:
        restore_version = history[0]["version"]
        print(f"📌 Restore point: v{{restore_version}}")
except Exception:
    print("📌 No existing silver table — first load")

# ── DQ-01: Filter quarantined rows ──────────────────────────────────
quarantined_count = df.filter(F.col("__is_quarantined") == True).count()
df_clean = df.filter(F.col("__is_quarantined") == False)
print(f"🚮 DQ-01 Quarantine filter: {{quarantined_count}} quarantined rows excluded")

# ── DQ-02: Remove all-null rows ─────────────────────────────────────
audit_cols = [c for c in df_clean.columns if c.startswith("__")]
data_cols  = [c for c in df_clean.columns if c not in audit_cols]

if data_cols:
    null_check = [F.col(c).isNull() for c in data_cols]
    all_null   = null_check[0]
    for nc in null_check[1:]:
        all_null = all_null & nc
    rejected_nulls = df_clean.filter(all_null).count()
    df_clean = df_clean.filter(~all_null)
    print(f"🚮 DQ-02 Null-key removal: {{rejected_nulls}} all-null rows dropped")
else:
    rejected_nulls = 0

# ── DQ-03: Per-column null percentage check ─────────────────────────
high_null_cols = []
total_for_null = df_clean.count()
if total_for_null > 0 and data_cols:
    null_counts = df_clean.select(
        *[F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in data_cols]
    ).collect()[0].asDict()
    for col_name, cnt in null_counts.items():
        pct = (cnt / total_for_null) * 100 if cnt else 0
        if pct > 80:
            high_null_cols.append(f"{{col_name}}({{pct:.0f}}%)")
    if high_null_cols:
        print(f"⚠️ DQ-03 High null columns (>80%): {{', '.join(high_null_cols)}}")
    else:
        print(f"✅ DQ-03 No columns exceed 80% null threshold")

# ── DQ-04: Deduplication ────────────────────────────────────────────
before_dedup = df_clean.count()
df_clean = df_clean.dropDuplicates(data_cols) if data_cols else df_clean
after_dedup = df_clean.count()
dupes_removed = before_dedup - after_dedup
print(f"🔄 DQ-04 Deduplication: {{dupes_removed}} duplicates removed")

# ── DQ-05: Trim string columns (whitespace normalization) ──────────
string_cols = [f.name for f in df_clean.schema.fields if str(f.dataType) == "StringType"]
for sc in string_cols:
    df_clean = df_clean.withColumn(sc, F.trim(F.col(sc)))
print(f"✅ DQ-05 String trimming: {{len(string_cols)}} columns normalized")

# ── DQ-06: Empty string → NULL normalization ────────────────────────
for sc in string_cols:
    df_clean = df_clean.withColumn(sc, F.when(F.col(sc) == "", None).otherwise(F.col(sc)))
print(f"✅ DQ-06 Empty-to-NULL: {{len(string_cols)}} string columns normalized")

# ── DQ-05/06 verification: confirm no un-normalized strings remain ──
trim_dirty_after = 0
empty_after = 0
if string_cols:
    _agg_exprs = []
    for _i, _sc in enumerate(string_cols):
        _agg_exprs.append(F.sum(F.when(F.trim(F.col(_sc)) != F.col(_sc), 1).otherwise(0)).alias(f"_t{{_i}}"))
        _agg_exprs.append(F.sum(F.when(F.col(_sc) == "", 1).otherwise(0)).alias(f"_e{{_i}}"))
    _row = df_clean.agg(*_agg_exprs).collect()[0].asDict()
    trim_dirty_after = sum(int(v or 0) for k, v in _row.items() if k.startswith("_t"))
    empty_after = sum(int(v or 0) for k, v in _row.items() if k.startswith("_e"))
if trim_dirty_after == 0 and empty_after == 0:
    print(f"✅ DQ-05/06 verified: no un-normalized strings remain")
else:
    print(f"⚠️ DQ-05/06 verification: {{trim_dirty_after}} untrimmed / {{empty_after}} empty cells remain")

# ── DQ-07: Row count anomaly detection ──────────────────────────────
row_anomaly = False
try:
    prev = spark.sql(f"SELECT MAX(output_rows) AS prev_rows FROM `{{DQ_CATALOG}}`.`{{DQ_SCHEMA}}`.__dq_metrics WHERE table_name = '{{FULL_TABLE}}' AND layer = 'silver'").collect()[0]["prev_rows"]
    if prev and prev > 0:
        pct_change = abs(after_dedup - prev) / prev * 100
        if pct_change > 50:
            row_anomaly = True
            print(f"⚠️ DQ-07 Row count anomaly: {{pct_change:.0f}}% change vs previous ({{prev:,}} → {{after_dedup:,}})")
        else:
            print(f"✅ DQ-07 Row count change: {{pct_change:.1f}}% (within threshold)")
except Exception:
    print("ℹ️ DQ-07 No previous run — skipping anomaly detection")

# ── Compute DQ status per row ───────────────────────────────────────
if data_cols:
    _any_null = F.col(data_cols[0]).isNull()
    for dc in data_cols[1:]:
        _any_null = _any_null | F.col(dc).isNull()
    dq_status_expr = F.when(_any_null, F.lit("warn")).otherwise(F.lit("passed"))
else:
    dq_status_expr = F.lit("passed")

# Add silver audit columns
df_silver = (df_clean
    .withColumn("__silver_ts", F.current_timestamp())
    .withColumn("__silver_version", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    .withColumn("__dq_status", dq_status_expr)
    .withColumn("__job_id", F.lit(JOB_ID))
    .withColumn("__run_id", F.lit(RUN_ID))
)

final_count = df_silver.count()
warn_count  = df_silver.filter(F.col("__dq_status") == "warn").count()
total_rejected = initial_count - final_count

# DQ score calculation
checks_passed = sum([1 for c in [
    quarantined_count == 0,
    rejected_nulls == 0,
    len(high_null_cols) == 0,
    dupes_removed == 0,
    trim_dirty_after == 0,   # verified — no untrimmed strings remain
    empty_after == 0,        # verified — no empty strings remain
    not row_anomaly,
] if c])
checks_total = 7
dq_score = round(checks_passed / checks_total * 100, 1)

print(f"\\n📊 Silver DQ Summary:")
print(f"   Input:          {{initial_count:,}}")
print(f"   Output:         {{final_count:,}}")
print(f"   Rejected:       {{total_rejected:,}} (quarantined={{quarantined_count}}, nulls={{rejected_nulls}}, dupes={{dupes_removed}})")
print(f"   Warn rows:      {{warn_count:,}} (partial nulls)")
print(f"   High-null cols: {{len(high_null_cols)}}")
print(f"   Row anomaly:    {{'Yes' if row_anomaly else 'No'}}")
print(f"   DQ Score:       {{dq_score}}% ({{checks_passed}}/{{checks_total}} checks passed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Write to Silver Delta

# COMMAND ----------

try:
    if LOAD_TYPE == "full":
        (df_silver.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(silver_table))
        print(f"✅ Full load → {{silver_table}} ({{final_count:,}} rows)")
    else:
        (df_silver.write
            .format("delta")
            .mode("append")
            .saveAsTable(silver_table))
        print(f"✅ Append → {{silver_table}} ({{final_count:,}} rows)")
except Exception as e:
    if restore_version is not None:
        try:
            spark.sql(f"RESTORE TABLE {{silver_table}} TO VERSION AS OF {{restore_version}}")
            print(f"🔄 Restored to v{{restore_version}}")
        except Exception:
            pass
    try:
        spark.sql(f"""
            MERGE INTO {{run_tbl}} AS t
            USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'failed',
                t.error_message = '{{_sql_esc(e)}}',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": (lambda _m: _m[_m.rfind("Caused by:"):] if "Caused by:" in _m else _m)(str(e))[:2000]}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Update Metadata

# COMMAND ----------

# Save DQ metrics
try:
    dq_table = f"`{{DQ_CATALOG}}`.`{{DQ_SCHEMA}}`.__dq_metrics"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {{dq_table}} (
            run_id STRING, job_id STRING, table_name STRING, layer STRING,
            input_rows BIGINT, output_rows BIGINT, rejected_rows BIGINT,
            null_rows BIGINT, dupe_rows BIGINT, quarantined_rows BIGINT,
            schema_drift BOOLEAN, dq_checks_passed INT, dq_checks_total INT,
            dq_score DOUBLE, checked_at TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        INSERT INTO {{dq_table}} VALUES (
            '{{RUN_ID}}', '{{JOB_ID}}', '{{FULL_TABLE}}', 'silver',
            {{initial_count}}, {{final_count}}, {{total_rejected}},
            {{rejected_nulls}}, {{dupes_removed}}, {{quarantined_count}},
            false, {{checks_passed}}, {{checks_total}},
            {{dq_score}}, current_timestamp()
        )
    """)
except Exception as e:
    print(f"⚠️ DQ metrics save failed: {{e}}")

# Update run history
try:
    spark.sql(f"""
        MERGE INTO {{run_tbl}} AS t
        USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET
            t.status = 'success',
            t.rows_processed = {{final_count}},
            t.completed_at = current_timestamp(),
            t.duration_sec = unix_timestamp(current_timestamp()) - unix_timestamp(t.started_at)
    """)
except Exception as e:
    print(f"⚠️ Run history update failed: {{e}}")

# Update job metadata
try:
    spark.sql(f"""
        UPDATE {{job_tbl}}
        SET last_run_id = '{{RUN_ID}}', last_run_at = current_timestamp(),
            last_status = 'success', status = 'success',
            run_count = run_count + 1, updated_at = current_timestamp()
        WHERE job_id = '{{JOB_ID}}'
    """)
except Exception as e:
    print(f"⚠️ Job update failed: {{e}}")

# COMMAND ----------

exit_payload = json.dumps({{
    "status": "COMPLETED", "job_id": JOB_ID, "run_id": RUN_ID,
    "table": FULL_TABLE, "rows": final_count,
    "rejected": total_rejected, "silver_table": silver_table,
}})
print(f"\\n✅ SILVER COMPLETE — {{FULL_TABLE}} — {{final_count:,}} rows ({{total_rejected}} rejected)")
dbutils.notebook.exit(exit_payload)
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  4. METADATA-DRIVEN ORCHESTRATOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gen_orchestrator(catalog, schema, landing_path, workspace_path, ts, recon_catalog="reconciliation", recon_schema="hr", recon_table="ReconcilationDetails", log_catalog="logging", log_schema="hr", log_table="ExecutionLog"):
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 Metadata-Driven Orchestrator
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC Reads pipeline metadata from Delta tables and chains:
# MAGIC   Extract → Bronze → Reconciliation → Silver for each pipeline group.
# MAGIC   Then logs all execution details to the Logging catalog.
# MAGIC
# MAGIC Can run a **single pipeline group** or **all groups**.
# MAGIC ---

# COMMAND ----------

dbutils.widgets.text("group_id", "", "Pipeline Group ID (blank = run all)")
dbutils.widgets.text("load_type", "", "Load Type Override (full/incremental, blank = use metadata)")
dbutils.widgets.text("password_b64", "", "Source DB Password (base64)")
dbutils.widgets.text("catalog", "{catalog}", "Metadata Catalog")
dbutils.widgets.text("schema", "{schema}", "Metadata Schema")
dbutils.widgets.text("landing_path", "{landing_path}", "Landing Base Path")
dbutils.widgets.text("workspace_path", "{workspace_path}", "Notebook Workspace Path")
dbutils.widgets.text("recon_catalog", "{recon_catalog}", "Reconciliation Catalog")
dbutils.widgets.text("recon_schema", "{recon_schema}", "Reconciliation Schema")
dbutils.widgets.text("recon_table", "{recon_table}", "Reconciliation Table")
dbutils.widgets.text("log_catalog", "{log_catalog}", "Logging Catalog")
dbutils.widgets.text("log_schema", "{log_schema}", "Logging Schema")
dbutils.widgets.text("log_table", "{log_table}", "Logging Table")

GROUP_ID       = dbutils.widgets.get("group_id").strip()
LOAD_OVERRIDE  = dbutils.widgets.get("load_type").strip()
PASSWORD_B64   = dbutils.widgets.get("password_b64").strip()
CATALOG        = dbutils.widgets.get("catalog").strip()
SCHEMA         = dbutils.widgets.get("schema").strip()
LANDING_PATH   = dbutils.widgets.get("landing_path").strip()
WORKSPACE_PATH = dbutils.widgets.get("workspace_path").strip()
RECON_CATALOG  = dbutils.widgets.get("recon_catalog").strip()
RECON_SCHEMA   = dbutils.widgets.get("recon_schema").strip()
RECON_TABLE    = dbutils.widgets.get("recon_table").strip()
LOG_CATALOG    = dbutils.widgets.get("log_catalog").strip()
LOG_SCHEMA     = dbutils.widgets.get("log_schema").strip()
LOG_TABLE      = dbutils.widgets.get("log_table").strip()

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

import json, uuid
from datetime import datetime

job_tbl  = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata"
run_tbl  = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_run_history"
pipe_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_pipeline_metadata"

# Get pipeline groups
if GROUP_ID:
    groups_df = spark.sql(f"SELECT * FROM {{pipe_tbl}} WHERE group_id = '{{GROUP_ID}}'")
else:
    groups_df = spark.sql(f"SELECT * FROM {{pipe_tbl}}")

groups = [r.asDict() for r in groups_df.collect()]
print(f"📋 Pipeline groups to run: {{len(groups)}}")
for g in groups:
    print(f"   • {{g['full_table']}} ({{g.get('load_type','full')}})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Execute Pipelines

# COMMAND ----------

stage_notebook = {{
    "extract":           f"{{WORKSPACE_PATH}}/01_Meta_Extract",
    "landing_to_bronze": f"{{WORKSPACE_PATH}}/02_Meta_Bronze",
    "bronze_to_silver":  f"{{WORKSPACE_PATH}}/03_Meta_Silver",
    "dlt_bronze_silver": "__STANDARD_CONVERT__",  # Will run bronze then silver
}}

results = []

for group in groups:
    gid = group["group_id"]
    print(f"\\n{{'='*60}}")
    print(f"🔗 Pipeline: {{group['full_table']}}")
    print(f"{{'='*60}}")

    # Get jobs for this group, ordered by job_order
    jobs_df = spark.sql(f"""
        SELECT * FROM {{job_tbl}}
        WHERE group_id = '{{gid}}' AND (enabled = true OR enabled IS NULL)
        ORDER BY job_order ASC
    """)
    jobs = [r.asDict() for r in jobs_df.collect()]

    group_ok = True
    for job in jobs:
        job_id   = job["job_id"]
        stage    = job["stage"]
        nb_path  = stage_notebook.get(stage)
        if not nb_path:
            print(f"   ⚠️ Unknown stage '{{stage}}' — skipping")
            continue
        
        # Convert DLT stage to standard (Bronze → Silver sequentially)
        if nb_path == "__STANDARD_CONVERT__":
            print(f"   🔄 Converting DLT stage to Standard (Bronze → Silver)")
            for _sub_stage, _sub_nb in [("landing_to_bronze", f"{{WORKSPACE_PATH}}/02_Meta_Bronze"), ("bronze_to_silver", f"{{WORKSPACE_PATH}}/03_Meta_Silver")]:
                _sub_run_id = uuid.uuid4().hex[:12]
                print(f"      ▶ Running: {{_sub_stage}} ({{_sub_nb}})")
                try:
                    _sub_result = dbutils.notebook.run(_sub_nb, 3600, {{
                        "job_id": job_id, "run_id": _sub_run_id,
                        "load_type": load_type, "password_b64": PASSWORD_B64,
                        "catalog": CATALOG, "schema": SCHEMA, "landing_path": LANDING_PATH,
                    }})
                    _sub_parsed = json.loads(_sub_result) if _sub_result else {{}}
                    if _sub_parsed.get("status") in ("FAILED", "ERROR"):
                        print(f"      ❌ {{_sub_stage}} failed: {{_sub_parsed.get('error','')}}")
                        group_ok = False
                        break
                    else:
                        print(f"      ✅ {{_sub_stage}} completed: {{_sub_parsed.get('rows', 0)}} rows")
                except Exception as _sub_e:
                    print(f"      ❌ {{_sub_stage}} exception: {{_sub_e}}")
                    group_ok = False
                    break
            results.append({{"group": gid, "table": group["full_table"], "status": "OK" if group_ok else "FAILED"}})
            continue

        run_id = uuid.uuid4().hex[:12]
        load_type = LOAD_OVERRIDE if LOAD_OVERRIDE else (job.get("load_type") or "full")

        # Create run record in metadata
        try:
            spark.sql(f"""
                INSERT INTO {{run_tbl}} (run_id, job_id, job_name, stage, full_table,
                    load_type, watermark_column, status, started_at)
                VALUES ('{{run_id}}', '{{job_id}}', '{{job["job_name"]}}', '{{stage}}',
                    '{{job["full_table"]}}', '{{load_type}}', '{{job.get("watermark_column","")}}',
                    'running', current_timestamp())
            """)
        except Exception as e:
            print(f"   ⚠️ Could not create run record: {{e}}")

        print(f"\\n   ▶ Running: {{job['job_name']}} ({{stage}})")

        try:
            result_json = dbutils.notebook.run(
                nb_path,
                timeout_seconds=3600,
                arguments={{
                    "job_id":       job_id,
                    "run_id":       run_id,
                    "load_type":    load_type,
                    "password_b64": PASSWORD_B64,
                    "catalog":      CATALOG,
                    "schema":       SCHEMA,
                    "landing_path": LANDING_PATH,
                }}
            )
            result = json.loads(result_json) if result_json else {{}}
            status = result.get("status", "UNKNOWN")
            rows   = result.get("rows", 0)
            error  = result.get("error", "")

            if status in ("FAILED", "ERROR"):
                print(f"   ❌ {{job['job_name']}}: {{status}} — {{error}}")
                results.append({{"job": job["job_name"], "status": "FAILED", "rows": rows, "error": error}})
                group_ok = False
                print(f"   ⛔ Stopping pipeline for {{group['full_table']}} due to failure")
                break
            else:
                print(f"   ✅ {{job['job_name']}}: {{status}} ({{rows:,}} rows)")
                results.append({{"job": job["job_name"], "status": status, "rows": rows}})

                # ── Reconciliation after Bronze ──────────────────────────
                if stage == "landing_to_bronze":
                    print(f"\\n   🔍 Running Reconciliation for {{job['job_name']}}…")
                    try:
                        recon_json = dbutils.notebook.run(
                            f"{{WORKSPACE_PATH}}/04_Meta_Reconciliation",
                            timeout_seconds=1800,
                            arguments={{
                                "job_id":        job_id,
                                "run_id":        run_id,
                                "password_b64":  PASSWORD_B64,
                                "catalog":       CATALOG,
                                "schema":        SCHEMA,
                                "landing_path":  LANDING_PATH,
                                "recon_catalog": RECON_CATALOG,
                                "recon_schema":  RECON_SCHEMA,
                                "recon_table":   RECON_TABLE,
                            }}
                        )
                        recon_result = json.loads(recon_json) if recon_json else {{}}
                        r_status = recon_result.get("status", "UNKNOWN")
                        r_checks = recon_result.get("checks", 0)
                        r_passed = recon_result.get("passed", 0)
                        r_failed = recon_result.get("failed", 0)
                        print(f"   🔍 Reconciliation: {{r_status}} — {{r_checks}} checks ({{r_passed}} pass, {{r_failed}} fail)")
                        results.append({{"job": f"Recon_{{job['job_name']}}", "status": r_status, "rows": r_checks}})
                    except Exception as re:
                        print(f"   ⚠️ Reconciliation failed (non-blocking): {{re}}")
                        results.append({{"job": f"Recon_{{job['job_name']}}", "status": "WARN", "rows": 0, "error": str(re)[:200]}})

        except Exception as e:
            print(f"   ❌ {{job['job_name']}} FAILED: {{e}}")
            # Mark failure in run history
            try:
                spark.sql(f"""
                    MERGE INTO {{run_tbl}} AS t
                    USING (SELECT '{{run_id}}' AS run_id) AS s ON t.run_id = s.run_id
                    WHEN MATCHED THEN UPDATE SET
                        t.status = 'failed',
                        t.error_message = '{{_sql_esc(e)}}'
                        , t.completed_at = current_timestamp()
                """)
                spark.sql(f"""
                    UPDATE {{job_tbl}}
                    SET last_status = 'failed', status = 'failed',
                        fail_count = fail_count + 1, updated_at = current_timestamp()
                    WHERE job_id = '{{job_id}}'
                """)
            except Exception:
                pass
            results.append({{"job": job["job_name"], "status": "FAILED", "error": str(e)}})
            group_ok = False
            print(f"   ⛔ Stopping pipeline for {{group['full_table']}} due to failure")
            break

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Orchestration Summary

# COMMAND ----------

succeeded = [r for r in results if r.get("status") in ("COMPLETED", "SUCCESS") and not r.get("job","").startswith("Recon_")]
failed    = [r for r in results if r.get("status") == "FAILED" and not r.get("job","").startswith("Recon_")]
recon_results = [r for r in results if r.get("job","").startswith("Recon_")]
total_rows = sum(r.get("rows", 0) for r in results if not r.get("job","").startswith("Recon_"))

print(f"\\n{{'='*60}}")
print(f"📊 ORCHESTRATION COMPLETE")
print(f"{{'='*60}}")
print(f"  ✅ Succeeded : {{len(succeeded)}} / {{len(succeeded) + len(failed)}}")
print(f"  ❌ Failed    : {{len(failed)}} / {{len(succeeded) + len(failed)}}")
print(f"  📊 Total Rows: {{total_rows:,}}")
if recon_results:
    r_ok = sum(1 for r in recon_results if r.get("status") in ("COMPLETED",))
    r_warn = sum(1 for r in recon_results if r.get("status") not in ("COMPLETED",))
    print(f"  🔍 Recon     : {{r_ok}} pass, {{r_warn}} warn/fail")

if failed:
    print(f"\\n⚠️ Failed jobs:")
    for f_item in failed:
        print(f"   • {{f_item['job']}}: {{f_item.get('error','unknown')}}")

# Build error detail list for visibility
error_details = []
for f_item in failed:
    error_details.append(f"{{f_item['job']}}: {{f_item.get('error','unknown')[:200]}}")

exit_payload = json.dumps({{
    "status":     "COMPLETED" if not failed else "PARTIAL",
    "succeeded":  len(succeeded),
    "failed":     len(failed),
    "total_rows": total_rows,
    "groups":     len(groups),
    "errors":     error_details,
}})

# ── Execution Logging ──────────────────────────────────────────────
print(f"\\n📝 Saving execution log to {{LOG_CATALOG}}.{{LOG_SCHEMA}}.{{LOG_TABLE}}…")
try:
    log_json = dbutils.notebook.run(
        f"{{WORKSPACE_PATH}}/05_Meta_ExecutionLog",
        timeout_seconds=600,
        arguments={{
            "catalog":      CATALOG,
            "schema":       SCHEMA,
            "log_catalog":  LOG_CATALOG,
            "log_schema":   LOG_SCHEMA,
            "log_table":    LOG_TABLE,
            "results_json": json.dumps(results),
            "groups_json":  json.dumps([{{"group_id": g["group_id"], "full_table": g["full_table"], "load_type": g.get("load_type","full")}} for g in groups]),
            "orchestrator_status": "COMPLETED" if not failed else "PARTIAL",
        }}
    )
    print(f"   ✅ Execution log saved")
except Exception as log_err:
    print(f"   ⚠️ Execution logging failed (non-blocking): {{log_err}}")

dbutils.notebook.exit(exit_payload)
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  4b. AGGREGATE RECONCILIATION NOTEBOOK  (Source vs Bronze)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gen_reconciliation(catalog, schema, landing_path, recon_catalog, recon_schema, recon_table, ts, recon_location=""):
    _loc_clause = f" MANAGED LOCATION '{recon_location}'" if recon_location else ""
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Aggregate Reconciliation — Source vs Bronze
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC This notebook performs aggregate reconciliation between the **source database**
# MAGIC and the **Bronze Delta table** for the current pipeline execution.
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Identifies all numeric columns (int, bigint, float, decimal, numeric, smallint, tinyint, real, money)
# MAGIC 2. Computes SUM for each numeric column from **Source** (via JDBC) and **Bronze** (Delta)
# MAGIC 3. Compares row counts
# MAGIC 4. Saves per-column results to `{recon_catalog}.{recon_schema}.{recon_table}`
# MAGIC 5. Each execution creates a unique `recon_run_id` — no duplicates, full audit trail
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Widget Configuration

# COMMAND ----------

dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("password_b64", "", "Source DB Password (base64)")
dbutils.widgets.text("catalog", "{catalog}", "Metadata Catalog")
dbutils.widgets.text("schema", "{schema}", "Metadata Schema")
dbutils.widgets.text("landing_path", "{landing_path}", "Landing Base Path")
dbutils.widgets.text("recon_catalog", "{recon_catalog}", "Reconciliation Catalog")
dbutils.widgets.text("recon_schema", "{recon_schema}", "Reconciliation Schema")
dbutils.widgets.text("recon_table", "{recon_table}", "Reconciliation Table")

import base64, json, uuid
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

JOB_ID       = dbutils.widgets.get("job_id").strip()
RUN_ID       = dbutils.widgets.get("run_id").strip()
_PWD_B64     = dbutils.widgets.get("password_b64").strip()
PASSWORD     = base64.b64decode(_PWD_B64.encode("ascii")).decode("utf-8") if _PWD_B64 else ""
CATALOG      = dbutils.widgets.get("catalog").strip()
SCHEMA       = dbutils.widgets.get("schema").strip()
LANDING_PATH = dbutils.widgets.get("landing_path").strip()
RECON_CATALOG= dbutils.widgets.get("recon_catalog").strip()
RECON_SCHEMA = dbutils.widgets.get("recon_schema").strip()
RECON_TABLE  = dbutils.widgets.get("recon_table").strip()

RECON_RUN_ID = uuid.uuid4().hex[:12]

print(f"🔍 Reconciliation for Job: {{JOB_ID}}, Run: {{RUN_ID}}")
print(f"📦 Results → {{RECON_CATALOG}}.{{RECON_SCHEMA}}.{{RECON_TABLE}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## � Ensure Reconciliation Table Exists

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{{RECON_CATALOG}}`{_loc_clause}")
except Exception as cat_err:
    print(f"⚠️ Could not create catalog {{RECON_CATALOG}}: {{cat_err}} — assuming it exists")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{{RECON_CATALOG}}`.`{{RECON_SCHEMA}}`")

recon_full_table = f"`{{RECON_CATALOG}}`.`{{RECON_SCHEMA}}`.`{{RECON_TABLE}}`"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {{recon_full_table}} (
        recon_run_id    STRING NOT NULL,
        pipeline_run_id STRING NOT NULL,
        job_id          STRING NOT NULL,
        source_table    STRING NOT NULL,
        bronze_table    STRING NOT NULL,
        column_name     STRING NOT NULL,
        data_type       STRING,
        source_value    DOUBLE,
        bronze_value    DOUBLE,
        variance        DOUBLE,
        variance_pct    DOUBLE,
        status          STRING,
        recon_timestamp TIMESTAMP
    ) USING DELTA
""")
print(f"📦 Table {{recon_full_table}} ready")

recon_schema_def = StructType([
    StructField("recon_run_id",    StringType(),    False),
    StructField("pipeline_run_id", StringType(),    False),
    StructField("job_id",          StringType(),    False),
    StructField("source_table",    StringType(),    False),
    StructField("bronze_table",    StringType(),    False),
    StructField("column_name",     StringType(),    False),
    StructField("data_type",       StringType(),    True),
    StructField("source_value",    DoubleType(),    True),
    StructField("bronze_value",    DoubleType(),    True),
    StructField("variance",        DoubleType(),    True),
    StructField("variance_pct",    DoubleType(),    True),
    StructField("status",          StringType(),    True),
    StructField("recon_timestamp", TimestampType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## �🔍 Read Job Metadata

# COMMAND ----------

job_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata"
job_df  = spark.sql(f"SELECT * FROM {{job_tbl}} WHERE job_id = '{{JOB_ID}}'")

if job_df.count() == 0:
    dbutils.notebook.exit(json.dumps({{"status": "SKIPPED", "reason": f"Job {{JOB_ID}} not found"}}))

job = job_df.collect()[0].asDict()
TABLE_NAME   = job["table_name"]
TABLE_SCHEMA = job["table_schema"]
FULL_TABLE   = job["full_table"]

source_config = json.loads(job.get("source_config", "{{}}") or "{{}}")
SERVER   = source_config.get("server", "")
DATABASE = source_config.get("database", "")
USERNAME = source_config.get("username", "")

target_config = json.loads(job.get("target_config", "{{}}") or "{{}}")
BRONZE_CATALOG = target_config.get("bronze_catalog", "")
TGT_SCHEMA     = target_config.get("target_schema", "")
VOLUMES_CATALOG= target_config.get("volumes_catalog", "")

# Mirror Bronze notebook's MULTI_CATALOG logic to resolve the actual bronze table name.
# In multi-catalog medallion mode (volumes + bronze + schema all set), the bronze
# notebook writes the table WITHOUT the legacy "bronze_" prefix.
MULTI_CATALOG = bool(VOLUMES_CATALOG and BRONZE_CATALOG and TGT_SCHEMA)

if MULTI_CATALOG:
    BRONZE_TABLE = f"`{{BRONZE_CATALOG}}`.`{{TGT_SCHEMA}}`.`{{TABLE_NAME}}`"
elif BRONZE_CATALOG and TGT_SCHEMA:
    # bronze_catalog + schema set, but volumes missing → bronze still uses prefix
    BRONZE_TABLE = f"`{{BRONZE_CATALOG}}`.`{{TGT_SCHEMA}}`.`bronze_{{TABLE_NAME}}`"
else:
    _fallback_cat = target_config.get('catalog', '')
    _meta_cat = target_config.get('metadata_catalog', CATALOG)
    if _fallback_cat and _fallback_cat != _meta_cat and _fallback_cat != CATALOG:
        BRONZE_TABLE = f"`{{_fallback_cat}}`.`{{target_config.get('schema', SCHEMA)}}`.`bronze_{{TABLE_NAME}}`"
    elif BRONZE_CATALOG:
        BRONZE_TABLE = f"`{{BRONZE_CATALOG}}`.`{{TGT_SCHEMA or SCHEMA}}`.`bronze_{{TABLE_NAME}}`"
    else:
        BRONZE_TABLE = f"`{{CATALOG}}`.`{{SCHEMA}}`.`bronze_{{TABLE_NAME}}`"
        print(f"⚠️ WARNING: No bronze_catalog — falling back to metadata catalog {{CATALOG}}")

# Defensive fallback: if the resolved table doesn't exist but the prefixed
# (or unprefixed) variant does, use whichever one is actually present.
def _table_exists(fq: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {{fq}}").limit(1).collect()
        return True
    except Exception:
        return False

if not _table_exists(BRONZE_TABLE):
    _alt_candidates = []
    if MULTI_CATALOG:
        _alt_candidates.append(f"`{{BRONZE_CATALOG}}`.`{{TGT_SCHEMA}}`.`bronze_{{TABLE_NAME}}`")
    else:
        _alt_candidates.append(f"`{{BRONZE_CATALOG or CATALOG}}`.`{{TGT_SCHEMA or SCHEMA}}`.`{{TABLE_NAME}}`")
    for _alt in _alt_candidates:
        if _table_exists(_alt):
            print(f"⚠️ Bronze table {{BRONZE_TABLE}} not found — using {{_alt}} instead")
            BRONZE_TABLE = _alt
            break

print(f"📋 Source: [{{TABLE_SCHEMA}}].[{{TABLE_NAME}}] on {{SERVER}}/{{DATABASE}}")
print(f"📋 Bronze: {{BRONZE_TABLE}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔌 JDBC Connection to Source

# COMMAND ----------

encrypt = "true" if source_config.get("source_type") in ("azuresql", "synapse") else "false"
trust   = "false" if source_config.get("source_type") in ("azuresql", "synapse") else "true"

if "," in SERVER:
    _host, _port = SERVER.rsplit(",", 1)
elif ":" in SERVER:
    _host, _port = SERVER.rsplit(":", 1)
else:
    _host, _port = SERVER, "1433"

jdbc_url = f"jdbc:sqlserver://{{_host}}:{{_port}};databaseName={{DATABASE}};encrypt={{encrypt}};trustServerCertificate={{trust}}"
jdbc_props = {{
    "user":     USERNAME,
    "password": PASSWORD,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "fetchsize": "10000",
}}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Identify Numeric Columns from Source

# COMMAND ----------

# Query SQL Server INFORMATION_SCHEMA to find numeric columns
numeric_types_sql = "('int','bigint','smallint','tinyint','float','real','decimal','numeric','money','smallmoney')"
col_query = f"""(
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{{TABLE_SCHEMA}}'
      AND TABLE_NAME   = '{{TABLE_NAME}}'
      AND DATA_TYPE IN {{numeric_types_sql}}
) AS col_info"""

try:
    cols_df = spark.read.jdbc(jdbc_url, col_query, properties=jdbc_props)
    numeric_cols = [(r["COLUMN_NAME"], r["DATA_TYPE"]) for r in cols_df.collect()]
    print(f"🔢 Found {{len(numeric_cols)}} numeric columns:")
    for cn, ct in numeric_cols:
        print(f"   • {{cn}} ({{ct}})")
except Exception as e:
    print(f"❌ Failed to read column metadata: {{e}}")
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": (lambda _m: _m[_m.rfind("Caused by:"):] if "Caused by:" in _m else _m)(str(e))[:2000]}}))

if not numeric_cols:
    print("⚠️ No numeric columns found — reconciliation skipped")
    dbutils.notebook.exit(json.dumps({{"status": "SKIPPED", "reason": "No numeric columns"}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Compute Source Aggregates (JDBC)

# COMMAND ----------

# Build a single SQL query that computes COUNT(*) plus SUM of each numeric column
agg_exprs = ["COUNT(*) AS __row_count"]
for cn, _ in numeric_cols:
    safe_col = cn.replace("'", "''")
    agg_exprs.append(f"SUM(CAST([{{cn}}] AS FLOAT)) AS [sum_{{cn}}]")

agg_sql = ", ".join(agg_exprs)
src_query = f"(SELECT {{agg_sql}} FROM [{{TABLE_SCHEMA}}].[{{TABLE_NAME}}]) AS src_agg"

try:
    src_agg_df = spark.read.jdbc(jdbc_url, src_query, properties=jdbc_props)
    src_row = src_agg_df.collect()[0]
    src_count = int(src_row["__row_count"])
    print(f"📊 Source row count: {{src_count:,}}")
except Exception as e:
    print(f"❌ Failed to compute source aggregates: {{e}}")
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": (lambda _m: _m[_m.rfind("Caused by:"):] if "Caused by:" in _m else _m)(str(e))[:2000]}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Compute Bronze Aggregates (Delta)

# COMMAND ----------

try:
    brz_df = spark.table(BRONZE_TABLE)
    brz_count = brz_df.count()
    print(f"📊 Bronze row count: {{brz_count:,}}")

    # Compute SUM of each numeric column in Bronze
    brz_agg_exprs = [F.count("*").alias("__row_count")]
    for cn, _ in numeric_cols:
        brz_agg_exprs.append(F.sum(F.col(f"`{{cn}}`").cast("double")).alias(f"sum_{{cn}}"))

    brz_agg_df = brz_df.agg(*brz_agg_exprs)
    brz_row = brz_agg_df.collect()[0]
except Exception as e:
    print(f"❌ Failed to compute Bronze aggregates: {{e}}")
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": (lambda _m: _m[_m.rfind("Caused by:"):] if "Caused by:" in _m else _m)(str(e))[:2000]}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Compare & Build Reconciliation Results

# COMMAND ----------

recon_ts = datetime.now()
results = []

# Row count reconciliation
count_match = "PASS" if src_count == brz_count else "FAIL"
count_variance = abs(src_count - brz_count)
results.append({{
    "recon_run_id":    RECON_RUN_ID,
    "pipeline_run_id": RUN_ID,
    "job_id":          JOB_ID,
    "source_table":    FULL_TABLE,
    "bronze_table":    BRONZE_TABLE,
    "column_name":     "__ROW_COUNT__",
    "data_type":       "count",
    "source_value":    float(src_count),
    "bronze_value":    float(brz_count),
    "variance":        float(count_variance),
    "variance_pct":    round((count_variance / src_count * 100), 4) if src_count > 0 else 0.0,
    "status":          count_match,
    "recon_timestamp": recon_ts,
}})

# Per-column SUM reconciliation
for cn, ct in numeric_cols:
    src_val = src_row[f"sum_{{cn}}"]
    brz_val = brz_row[f"sum_{{cn}}"]
    s = float(src_val) if src_val is not None else 0.0
    b = float(brz_val) if brz_val is not None else 0.0
    var = abs(s - b)
    pct = round((var / abs(s) * 100), 4) if s != 0.0 else 0.0
    status = "PASS" if var < 0.01 else ("WARN" if pct < 0.01 else "FAIL")

    results.append({{
        "recon_run_id":    RECON_RUN_ID,
        "pipeline_run_id": RUN_ID,
        "job_id":          JOB_ID,
        "source_table":    FULL_TABLE,
        "bronze_table":    BRONZE_TABLE,
        "column_name":     cn,
        "data_type":       ct,
        "source_value":    s,
        "bronze_value":    b,
        "variance":        var,
        "variance_pct":    pct,
        "status":          status,
        "recon_timestamp": recon_ts,
    }})

print(f"\\n📊 Reconciliation results: {{len(results)}} checks")
for r in results:
    icon = "✅" if r["status"] == "PASS" else ("⚠️" if r["status"] == "WARN" else "❌")
    print(f"   {{icon}} {{r['column_name']:<30}} src={{r['source_value']:>15,.2f}}  brz={{r['bronze_value']:>15,.2f}}  var={{r['variance_pct']:.4f}}%  {{r['status']}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save to Reconciliation Table

# COMMAND ----------

# Ensure reconciliation catalog, schema, and table exist (already done early — safe to repeat)
recon_df = spark.createDataFrame(results, schema=recon_schema_def)

# Append — each execution creates new rows with unique recon_run_id
recon_df.write.mode("append").option("mergeSchema", "true").saveAsTable(recon_full_table)

total_checks = len(results)
passed  = sum(1 for r in results if r["status"] == "PASS")
warned  = sum(1 for r in results if r["status"] == "WARN")
failed_ = sum(1 for r in results if r["status"] == "FAIL")

print(f"\\n💾 Saved {{total_checks}} reconciliation records to {{recon_full_table}}")
print(f"   ✅ PASS: {{passed}}  ⚠️ WARN: {{warned}}  ❌ FAIL: {{failed_}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Summary

# COMMAND ----------

exit_payload = json.dumps({{
    "status":       "COMPLETED",
    "recon_run_id": RECON_RUN_ID,
    "job_id":       JOB_ID,
    "run_id":       RUN_ID,
    "table":        FULL_TABLE,
    "checks":       total_checks,
    "passed":       passed,
    "warned":       warned,
    "failed":       failed_,
    "recon_table":  recon_full_table,
}})

print(f"\\n✅ RECONCILIATION COMPLETE — {{FULL_TABLE}} — {{total_checks}} checks ({{passed}} pass, {{warned}} warn, {{failed_}} fail)")
dbutils.notebook.exit(exit_payload)
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  EXECUTION LOG NOTEBOOK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _gen_execution_log(catalog, schema, log_catalog, log_schema, log_table, ts, log_location=""):
    """Generate the 05_Meta_ExecutionLog notebook.

    This notebook is called by the Orchestrator AFTER all jobs complete.
    It receives the per-job results JSON and the groups JSON, then writes
    a full audit-trail row per job into the logging Delta table.
    """
    _log_loc_clause = f" MANAGED LOCATION '{log_location}'" if log_location else ""
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 📝 Execution Log — Pipeline Run Audit Trail
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC This notebook saves per-job execution details to
# MAGIC `{{log_catalog}}.{{log_schema}}.{{log_table}}` as an append-only audit trail.
# MAGIC
# MAGIC **Logged per job:** job_id, job_name, stage, full_table, load_type,
# MAGIC status, rows_processed, started_at, completed_at, duration_sec, error_message
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Widget Configuration

# COMMAND ----------

dbutils.widgets.text("catalog",              "{catalog}",      "Metadata Catalog")
dbutils.widgets.text("schema",               "{schema}",       "Metadata Schema")
dbutils.widgets.text("log_catalog",          "{log_catalog}",  "Log Catalog")
dbutils.widgets.text("log_schema",           "{log_schema}",   "Log Schema")
dbutils.widgets.text("log_table",            "{log_table}",    "Log Table")
dbutils.widgets.text("results_json",         "{{}}", "Results JSON")
dbutils.widgets.text("groups_json",          "[]", "Groups JSON")
dbutils.widgets.text("orchestrator_status",  "",  "Orchestrator Status")

import json, uuid
from datetime import datetime
from pyspark.sql.types import (StructType, StructField, StringType,
                                LongType, DoubleType, TimestampType)

CATALOG      = dbutils.widgets.get("catalog").strip()
SCHEMA       = dbutils.widgets.get("schema").strip()
LOG_CATALOG  = dbutils.widgets.get("log_catalog").strip()
LOG_SCHEMA   = dbutils.widgets.get("log_schema").strip()
LOG_TABLE    = dbutils.widgets.get("log_table").strip()
RESULTS_JSON = dbutils.widgets.get("results_json").strip()
GROUPS_JSON  = dbutils.widgets.get("groups_json").strip()
ORCH_STATUS  = dbutils.widgets.get("orchestrator_status").strip()

LOG_RUN_ID   = uuid.uuid4().hex[:12]
LOG_TS       = datetime.now()

print(f"📝 Execution Log Run: {{LOG_RUN_ID}}")
print(f"📦 Target: {{LOG_CATALOG}}.{{LOG_SCHEMA}}.{{LOG_TABLE}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Parse Execution Results

# COMMAND ----------

try:
    results_raw = json.loads(RESULTS_JSON)
except Exception:
    results_raw = []

try:
    groups = json.loads(GROUPS_JSON)
except Exception:
    groups = []

# Build group lookup for load_type
# groups can be a list of strings (group IDs) or a list of dicts
group_lookup = {{}}
for g in groups:
    if isinstance(g, dict):
        gid = g.get("group_id", "")
        group_lookup[gid] = {{
            "full_table": g.get("full_table", ""),
            "load_type":  g.get("load_type", "full"),
        }}
    else:
        # g is a plain group_id string
        group_lookup[str(g)] = {{"full_table": "", "load_type": "full"}}

# Normalise results — orchestrator sends a flat list of dicts
if isinstance(results_raw, dict):
    results_list = [results_raw]
elif isinstance(results_raw, list):
    results_list = results_raw
else:
    results_list = []

print(f"📊 Received {{len(results_list)}} job results, {{len(groups)}} groups")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Ensure Logging Table Exists

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField, StringType,
                                LongType, DoubleType, TimestampType)

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{{LOG_CATALOG}}`{_log_loc_clause}")
except Exception as cat_err:
    print(f"⚠️ Could not create catalog {{LOG_CATALOG}}: {{cat_err}} — assuming it exists")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{{LOG_CATALOG}}`.`{{LOG_SCHEMA}}`")

log_full_table = f"`{{LOG_CATALOG}}`.`{{LOG_SCHEMA}}`.`{{LOG_TABLE}}`"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {{log_full_table}} (
        log_run_id          STRING NOT NULL,
        group_id            STRING NOT NULL,
        full_table          STRING NOT NULL,
        stage               STRING NOT NULL,
        load_type           STRING,
        status              STRING,
        rows_processed      BIGINT,
        started_at          STRING,
        completed_at        STRING,
        duration_sec        DOUBLE,
        error_message       STRING,
        orchestrator_status STRING,
        log_timestamp       TIMESTAMP
    ) USING DELTA
""")
print(f"📦 Table {{log_full_table}} ready")

log_schema = StructType([
    StructField("log_run_id",          StringType(),    False),
    StructField("group_id",            StringType(),    False),
    StructField("full_table",          StringType(),    False),
    StructField("stage",               StringType(),    False),
    StructField("load_type",           StringType(),    True),
    StructField("status",              StringType(),    True),
    StructField("rows_processed",      LongType(),      True),
    StructField("started_at",          StringType(),    True),
    StructField("completed_at",        StringType(),    True),
    StructField("duration_sec",        DoubleType(),    True),
    StructField("error_message",       StringType(),    True),
    StructField("orchestrator_status", StringType(),    True),
    StructField("log_timestamp",       TimestampType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔨 Build Log Rows

# COMMAND ----------

log_rows = []

# results_list is a flat list of job result dicts from the orchestrator
for entry in results_list:
    job_name   = entry.get("job", "unknown")
    status     = entry.get("status", "UNKNOWN")
    rows       = entry.get("rows", 0)
    error      = entry.get("error", "")

    # Infer stage from job name pattern
    if "Recon_" in job_name:
        stage = "reconciliation"
    elif job_name.startswith("ExtractTo_"):
        stage = "extract"
    elif "_To_bronze_" in job_name or "_To_Bronze_" in job_name:
        stage = "landing_to_bronze"
    elif "_To_silver_" in job_name or "_To_Silver_" in job_name:
        stage = "bronze_to_silver"
    else:
        stage = "unknown"

    # Try to match a group for full_table/load_type
    full_table = job_name
    load_type  = "full"
    for gid, ginfo in group_lookup.items():
        if ginfo.get("full_table", "") and ginfo["full_table"] in job_name:
            full_table = ginfo["full_table"]
            load_type  = ginfo.get("load_type", "full")
            break

    log_rows.append({{
        "log_run_id":          LOG_RUN_ID,
        "group_id":            job_name,
        "full_table":          str(full_table),
        "stage":               str(stage),
        "load_type":           str(load_type),
        "status":              str(status),
        "rows_processed":      int(rows) if rows else 0,
        "started_at":          "",
        "completed_at":        "",
        "duration_sec":        0.0,
        "error_message":       str(error)[:2000] if error else "",
        "orchestrator_status": str(ORCH_STATUS),
        "log_timestamp":       LOG_TS,
    }})

print(f"📝 Built {{len(log_rows)}} log entries")

if not log_rows:
    print("⚠️ No execution data to log")
    dbutils.notebook.exit(json.dumps({{"status": "SKIPPED", "reason": "No execution data"}}))

for lr in log_rows[:5]:
    icon = "✅" if lr["status"] == "SUCCESS" else ("⚠️" if lr["status"] == "SKIPPED" else "❌")
    print(f"   {{icon}} {{lr['full_table']}} / {{lr['stage']}} → {{lr['status']}} ({{lr['rows_processed']:,}} rows, {{lr['duration_sec']:.1f}}s)")
if len(log_rows) > 5:
    print(f"   … and {{len(log_rows) - 5}} more")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save to Logging Table

# COMMAND ----------

log_df = spark.createDataFrame(log_rows, schema=log_schema)
log_df.write.mode("append").option("mergeSchema", "true").saveAsTable(log_full_table)

total_logged = len(log_rows)
success_count = sum(1 for r in log_rows if r["status"] == "SUCCESS")
failed_count  = sum(1 for r in log_rows if r["status"] == "FAILED")

print(f"\\n💾 Saved {{total_logged}} execution log records to {{log_full_table}}")
print(f"   ✅ SUCCESS: {{success_count}}  ❌ FAILED: {{failed_count}}  📊 OTHER: {{total_logged - success_count - failed_count}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Summary

# COMMAND ----------

exit_payload = json.dumps({{
    "status":       "COMPLETED",
    "log_run_id":   LOG_RUN_ID,
    "total_logged": total_logged,
    "success":      success_count,
    "failed":       failed_count,
    "log_table":    log_full_table,
}})

print(f"\\n✅ EXECUTION LOG COMPLETE — {{total_logged}} entries saved to {{log_full_table}}")
dbutils.notebook.exit(exit_payload)
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  5. DLT PIPELINE NOTEBOOK  (Bronze + Silver combined)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gen_dlt_pipeline(catalog, schema, landing_path, ts):
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ Metadata-Driven Spark Declarative Pipeline — Bronze & Silver
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC Lakeflow Spark Declarative Pipelines pipeline that dynamically discovers tables from
# MAGIC `wf_job_metadata` and creates Bronze + Silver layers with:
# MAGIC - **Auto Loader** (`cloudFiles`) for streaming Bronze ingestion
# MAGIC - **Expectations** for data quality enforcement
# MAGIC - Automatic dependency resolution (Silver reads from Bronze)
# MAGIC - Schema evolution & auto-optimize
# MAGIC
# MAGIC **Configuration** (set in Spark Declarative Pipeline settings):
# MAGIC | Key | Description |
# MAGIC |-----|-------------|
# MAGIC | `pipeline.catalog` | Unity Catalog for metadata tables |
# MAGIC | `pipeline.schema` | Schema for metadata tables |
# MAGIC | `pipeline.landing_path` | Base landing zone path |
# MAGIC | `pipeline.group_id` | Pipeline group filter (blank = all) |
# MAGIC ---

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
import json

# ─── Pipeline configuration (injected via Spark Declarative Pipeline settings) ──────
# Note: The Spark Declarative Pipeline spec's catalog/schema controls where tables are created.
# meta_catalog/meta_schema point to where wf_job_metadata lives (may differ).
META_CATALOG = spark.conf.get("pipeline.meta_catalog", "{catalog}")
META_SCHEMA  = spark.conf.get("pipeline.meta_schema", "{schema}")
LANDING_PATH = spark.conf.get("pipeline.landing_path", "{landing_path}")
GROUP_ID     = spark.conf.get("pipeline.group_id", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Discover Tables from Job Metadata

# COMMAND ----------

job_tbl = f"`{{META_CATALOG}}`.`{{META_SCHEMA}}`.wf_job_metadata"

_gf = f"AND group_id = '{{GROUP_ID}}'" if GROUP_ID else ""

# In DLT mode, jobs are stored with stage='dlt_bronze_silver' (single stage).
# In standard mode, they use 'landing_to_bronze' / 'bronze_to_silver'.
# Query for ALL matching stages so both modes work.
all_dlt_jobs = [r.asDict() for r in spark.sql(f"""
    SELECT DISTINCT table_name, full_table, target_config, load_type
    FROM {{job_tbl}}
    WHERE stage IN ('landing_to_bronze', 'bronze_to_silver', 'dlt_bronze_silver')
      AND (enabled = true OR enabled IS NULL)
      {{_gf}}
""").collect()]

# Both bronze and silver use the same job list
bronze_jobs = all_dlt_jobs
silver_jobs = all_dlt_jobs

print(f"⚡ DLT — Bronze tables: {{len(bronze_jobs)}}, Silver tables: {{len(silver_jobs)}}")

if not bronze_jobs:
    print("⚠️ WARNING: No tables found in wf_job_metadata for SDP processing!")
    print(f"   Checked stages: landing_to_bronze, bronze_to_silver, dlt_bronze_silver")
    print(f"   Metadata table: {{job_tbl}}")
    # Show what stages DO exist
    _existing = [r[0] for r in spark.sql(f"SELECT DISTINCT stage FROM {{job_tbl}}").collect()]
    print(f"   Existing stages in metadata: {{_existing}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥉 Bronze Layer — Auto Loader + Expectations
# MAGIC
# MAGIC Each source table gets a **streaming table** that ingests new Parquet
# MAGIC files via Auto Loader with schema evolution.

# COMMAND ----------

def _make_bronze(job):
    """Factory: register a DLT streaming table for one Bronze source."""
    tbl   = job["table_name"]
    full  = job["full_table"]
    # Always use LANDING_PATH (from pipeline config) — it matches the path
    # used by the extract notebook.  Never construct a per-table Volumes path
    # here because the extract may write to an ABFSS path or a different
    # volume schema (e.g. hr vs dbo).
    src   = f"{{LANDING_PATH}}/{{tbl}}"
    # Use simple names — Spark Declarative Pipeline's catalog/schema controls where tables
    # are published.  3-part names cause "Failed to analyze flow" errors.
    bronze_full = f"bronze_{{tbl}}"

    @dlt.table(
        name=bronze_full,
        comment=f"Bronze — raw ingestion of {{full}} via Auto Loader",
        table_properties={{
            "quality": "bronze",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact":   "true",
            "pipelines.autoOptimize.managed":   "true",
        }},
    )
    @dlt.expect_or_drop("dq01_valid_landing_ts",   "__landing_ts IS NOT NULL")
    @dlt.expect("dq02_has_source_system",            "__source_system IS NOT NULL")
    @dlt.expect("dq03_has_batch_id",                 "__batch_id IS NOT NULL")
    @dlt.expect("dq04_fresh_data",                   "__landing_ts >= current_timestamp() - INTERVAL 7 DAYS")
    @dlt.expect("dq05_not_all_null",                 "NOT(__landing_ts IS NULL AND __source_system IS NULL AND __batch_id IS NULL)")
    def _inner():
        return (
            spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .load(src)
                .withColumn("__bronze_ts",      F.current_timestamp())
                .withColumn("__source_table",   F.lit(full))
                .withColumn("__is_quarantined", F.lit(False))
        )

if not bronze_jobs:
    print("⚠️ No tables found in wf_job_metadata for Spark Declarative Pipeline.")
    print(f"   Checked stages: landing_to_bronze, bronze_to_silver, dlt_bronze_silver")
    print(f"   Metadata table: {{job_tbl}}")

# Check landing paths before registering tables — skip tables whose
# landing directory does not exist or is empty.  This prevents the
# CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE error that kills the whole pipeline.
# NOTE: dbutils.fs.ls() is BLOCKED inside Spark Declarative Pipelines (PY4J_BLOCKED_API).
# Use spark.read.format("parquet") instead — it is DLT-compatible.
_bronze_registered = []
for _j in bronze_jobs:
    _landing = f"{{LANDING_PATH}}/{{_j['table_name']}}"
    try:
        _check = spark.read.format("parquet").load(_landing).limit(1).count()
        if _check > 0:
            _make_bronze(_j)
            _bronze_registered.append(_j["table_name"])
            print(f"  ✅ Registered bronze: {{_j['table_name']}}")
        else:
            print(f"  ⏭️ Skipping {{_j['table_name']}}: landing path is empty")
    except Exception as _ls_err:
        _err_msg = str(_ls_err).lower()
        if "path does not exist" in _err_msg or "filenotfoundexception" in _err_msg or "is not a delta table" in _err_msg or "unable to infer schema" in _err_msg:
            print(f"  ⏭️ Skipping {{_j['table_name']}}: landing path not found ({{_landing}})")
        else:
            raise   # surface credential / permission errors

if not _bronze_registered:
    raise ValueError(
        "No tables have data in the landing zone. "
        "Run extracts first to populate landing data before triggering DLT."
    )

print(f"\\n⚡ Registered {{len(_bronze_registered)}}/{{len(bronze_jobs)}} bronze tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥈 Silver Layer — Quality Enforcement & Cleansing
# MAGIC
# MAGIC Each Silver table reads from its Bronze counterpart via `dlt.read()`,
# MAGIC applies deduplication, string trimming, and quality expectations.

# COMMAND ----------

def _make_silver(job):
    """Factory: register a DLT materialized view for one Silver table."""
    tbl         = job["table_name"]
    full        = job["full_table"]
    tcfg        = json.loads(job.get("target_config") or "{{}}")
    b_cat       = tcfg.get("bronze_catalog", "")
    s_cat       = tcfg.get("silver_catalog", "")
    t_sch       = tcfg.get("target_schema", "")
    # Use simple names — must match the bronze name used in _make_bronze.
    # Spark Declarative Pipeline's catalog/schema controls where tables are published.
    bronze_name = f"bronze_{{tbl}}"
    silver_full = f"silver_{{tbl}}"

    @dlt.table(
        name=silver_full,
        comment=f"Silver — cleansed & validated {{full}}",
        table_properties={{
            "quality": "silver",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact":   "true",
        }},
    )
    @dlt.expect_or_drop("dq01_valid_bronze_ts",    "__bronze_ts IS NOT NULL")
    @dlt.expect("dq02_has_source_table",            "__source_table IS NOT NULL")
    @dlt.expect("dq03_bronze_freshness",            "__bronze_ts >= current_timestamp() - INTERVAL 7 DAYS")
    @dlt.expect("dq04_no_empty_source",             "length(trim(coalesce(__source_table, ''))) > 0")
    def _inner():
        # Always use dlt.read() for within-pipeline dependency resolution.
        # spark.read.table() fails because bronze isn't committed yet
        # during the same pipeline update.
        df = dlt.read(bronze_name)

        # Filter quarantined rows before dropping the flag column
        df = df.filter(F.col("__is_quarantined") == False)

        # Trim all string columns (skip audit cols)
        trimmed = df
        for c in df.schema:
            if c.dataType.simpleString() == "string" and not c.name.startswith("__"):
                trimmed = trimmed.withColumn(c.name, F.trim(F.col(c.name)))

        # Real per-row DQ status — warn when any data column is null
        _data_cols = [c.name for c in trimmed.schema if not c.name.startswith("__")]
        if _data_cols:
            _any_null = F.col(_data_cols[0]).isNull()
            for _dc in _data_cols[1:]:
                _any_null = _any_null | F.col(_dc).isNull()
            _dq_status = F.when(_any_null, F.lit("warn")).otherwise(F.lit("passed"))
        else:
            _dq_status = F.lit("passed")

        return (
            trimmed
                .drop("__is_quarantined")
                .dropDuplicates()
                .withColumn("__silver_ts",  F.current_timestamp())
                .withColumn("__dq_status",  _dq_status)
        )

# Only create silver tables for tables that have bronze registered
# (i.e. tables that had landing data)
for _j in silver_jobs:
    if _j["table_name"] in _bronze_registered:
        _make_silver(_j)
    else:
        print(f"  ⏭️ Skipping silver for {{_j['table_name']}}: no bronze table registered")

print(f"⚡ Registered {{len(_bronze_registered)}} silver tables")
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  6. DLT ORCHESTRATOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gen_orchestrator_dlt(catalog, schema, landing_path, workspace_path, ts):
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 Metadata-Driven SDP Orchestrator
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC Two-phase execution:
# MAGIC 1. **Extract** — JDBC extraction via `dbutils.notebook.run()` (standard)
# MAGIC 2. **Spark Declarative Pipeline** — Bronze + Silver via Lakeflow Spark Declarative Pipelines REST API
# MAGIC
# MAGIC The orchestrator auto-creates the Spark Declarative Pipeline on first run,
# MAGIC then triggers pipeline updates for subsequent runs.
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Widget Configuration

# COMMAND ----------

dbutils.widgets.text("group_id", "", "Pipeline Group ID (blank = run all)")
dbutils.widgets.text("load_type", "", "Load Type Override (full/incremental)")
dbutils.widgets.text("password_b64", "", "Source DB Password (base64)")
dbutils.widgets.text("catalog", "{catalog}", "Metadata Catalog")
dbutils.widgets.text("schema", "{schema}", "Metadata Schema")
dbutils.widgets.text("landing_path", "{landing_path}", "Landing Base Path")
dbutils.widgets.text("workspace_path", "{workspace_path}", "Notebook Workspace Path")
dbutils.widgets.text("volumes_catalog", "", "Volumes Catalog (dev_volumes)")
dbutils.widgets.text("bronze_catalog", "", "Bronze Catalog")
dbutils.widgets.text("silver_catalog", "", "Silver Catalog")
dbutils.widgets.text("target_schema", "", "Target Schema (hr)")

GROUP_ID       = dbutils.widgets.get("group_id").strip()
LOAD_OVERRIDE  = dbutils.widgets.get("load_type").strip()
PASSWORD_B64   = dbutils.widgets.get("password_b64").strip()
CATALOG        = dbutils.widgets.get("catalog").strip()
SCHEMA         = dbutils.widgets.get("schema").strip()
LANDING_PATH   = dbutils.widgets.get("landing_path").strip()
WORKSPACE_PATH = dbutils.widgets.get("workspace_path").strip()
_VOLUMES_CAT   = dbutils.widgets.get("volumes_catalog").strip()
_BRONZE_CAT    = dbutils.widgets.get("bronze_catalog").strip()
_SILVER_CAT    = dbutils.widgets.get("silver_catalog").strip()
_TARGET_SCHEMA = dbutils.widgets.get("target_schema").strip()

# Multi-catalog: override landing path with UC Volumes (must match extract notebook)
if _VOLUMES_CAT and _TARGET_SCHEMA:
    LANDING_PATH = f"/Volumes/{{_VOLUMES_CAT}}/{{_TARGET_SCHEMA}}/landing"
    print(f"📦 Multi-catalog: Landing → UC Volumes: {{LANDING_PATH}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔐 Workspace Context for REST API

# COMMAND ----------

import json, uuid, time, requests
from datetime import datetime

# Obtain host & token from the running notebook context
# Use safe fallbacks that work on both classic clusters and serverless
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

# Host — try spark conf first (works everywhere), then context API
try:
    HOST = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
except Exception:
    try:
        HOST = "https://" + ctx.browserHostName().get()
    except Exception:
        HOST = "https://" + ctx.tags().apply("browserHostName")

# Token — context API with safe .getOrElse fallback
try:
    TOKEN = ctx.apiToken().getOrElse(None)
    if not TOKEN:
        TOKEN = ctx.apiToken().get()
except Exception:
    TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

assert TOKEN, "❌ Could not obtain API token from notebook context — check cluster permissions"
_hdrs = {{"Authorization": f"Bearer {{TOKEN}}", "Content-Type": "application/json"}}

print(f"🔗 Workspace: {{HOST}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Discover Extract Jobs

# COMMAND ----------

job_tbl  = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata"
run_tbl  = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_run_history"
pipe_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_pipeline_metadata"

if GROUP_ID:
    groups_df = spark.sql(f"SELECT * FROM {{pipe_tbl}} WHERE group_id = '{{GROUP_ID}}'")
else:
    groups_df = spark.sql(f"SELECT * FROM {{pipe_tbl}}")

groups = [r.asDict() for r in groups_df.collect()]
print(f"📋 Pipeline groups to run: {{len(groups)}}")

# Collect extract jobs across all selected groups
extract_jobs = []
for g in groups:
    gid = g["group_id"]
    jobs = spark.sql(f"""
        SELECT * FROM {{job_tbl}}
        WHERE group_id = '{{gid}}' AND stage = 'extract'
          AND (enabled = true OR enabled IS NULL)
        ORDER BY job_order ASC
    """).collect()
    extract_jobs.extend([r.asDict() for r in jobs])

print(f"📋 Extract jobs: {{len(extract_jobs)}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Phase 1 — Run Extract Notebooks

# COMMAND ----------

extract_nb = f"{{WORKSPACE_PATH}}/01_Meta_Extract"
extract_results = [None] * len(extract_jobs)

from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Skip extracts that already succeeded recently ──────────────────
# When run_pipeline_group() triggers Extract (job 1) first and then
# this orchestrator (job 2), the extract is already done.  Re-running
# it wastes time and fails on large tables.  Check wf_run_history for
# a successful extract for each table within the last 4 hours.
_already_extracted = set()
try:
    _recent = spark.sql(f"""
        SELECT DISTINCT full_table
        FROM {{run_tbl}}
        WHERE stage = 'extract'
          AND status = 'success'
          AND started_at >= current_timestamp() - INTERVAL 4 HOURS
    """).collect()
    _already_extracted = set(r[0] for r in _recent if r[0])
    if _already_extracted:
        print(f"⏭️ {{len(_already_extracted)}} table(s) already extracted in last 4h — will skip")
except Exception:
    pass

def _run_extract(idx, job):
    """Run a single extract notebook — designed for parallel execution."""
    job_id    = job["job_id"]
    run_id    = uuid.uuid4().hex[:12]
    load_type = LOAD_OVERRIDE or job.get("load_type") or "full"
    full_table = job.get("full_table", "")

    # Skip if extract already succeeded recently
    if full_table in _already_extracted:
        print(f"  ⏭️ {{job['job_name']}}: extract already succeeded recently — skipping")
        return idx, {{"job": job["job_name"], "status": "OK", "rows": 0, "run_id": run_id, "skipped": True}}

    # Create run record
    try:
        spark.sql(f"""
            INSERT INTO {{run_tbl}} (run_id, job_id, job_name, stage, full_table,
                load_type, watermark_column, status, started_at)
            VALUES ('{{run_id}}', '{{job_id}}', '{{job["job_name"]}}', 'extract',
                '{{job["full_table"]}}', '{{load_type}}',
                '{{job.get("watermark_column","")}}', 'running', current_timestamp())
        """)
    except Exception:
        pass

    try:
        result_json = dbutils.notebook.run(extract_nb, 3600, {{
            "job_id": job_id, "run_id": run_id,
            "load_type": load_type, "password_b64": PASSWORD_B64,
            "catalog": CATALOG, "schema": SCHEMA, "landing_path": LANDING_PATH,
        }})
        result = json.loads(result_json) if result_json else {{}}
        status = result.get("status", "UNKNOWN")
        rows   = result.get("rows", 0)
        if status in ("FAILED", "ERROR"):
            return idx, {{"job": job["job_name"], "status": "FAILED", "error": result.get("error",""), "run_id": run_id}}
        else:
            return idx, {{"job": job["job_name"], "status": "OK", "rows": rows, "run_id": run_id}}
    except Exception as e:
        return idx, {{"job": job["job_name"], "status": "FAILED", "error": (lambda _m: _m[_m.rfind("Caused by:"):] if "Caused by:" in _m else _m)(str(e))[:2000], "run_id": run_id}}

# Run all extracts in parallel (up to 4 concurrent)
_max_workers = min(4, len(extract_jobs)) if extract_jobs else 1
print(f"🚀 Running {{len(extract_jobs)}} extracts in parallel (max {{_max_workers}} workers)")
with ThreadPoolExecutor(max_workers=_max_workers) as pool:
    futures = {{pool.submit(_run_extract, i, j): i for i, j in enumerate(extract_jobs)}}
    for future in as_completed(futures):
        idx, result_entry = future.result()
        extract_results[idx] = result_entry
        emoji = "✅" if result_entry["status"] == "OK" else "❌"
        rows_info = f": {{result_entry.get('rows',0):,}} rows" if result_entry["status"] == "OK" else f": {{result_entry.get('error','')}}"
        print(f"  {{emoji}} {{result_entry['job']}}{{rows_info}}")

extract_ok   = len([r for r in extract_results if r["status"] == "OK"])
extract_fail = len([r for r in extract_results if r["status"] == "FAILED"])
if extract_fail:
    print(f"\\n⚠️ {{extract_fail}} extract(s) failed — Spark Declarative Pipeline will process remaining tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚡ Phase 2 — Create / Update Spark Declarative Pipeline

# COMMAND ----------

DLT_NAME = f"MetadataPipeline_{{GROUP_ID}}" if GROUP_ID else "MetadataPipeline_All"
DLT_NB   = f"{{WORKSPACE_PATH}}/02_Meta_SDP_Pipeline"

# ── Determine DLT output catalog/schema ──────────────────────────
# Use explicit widget parameters first (most reliable), then fall back
# to wf_job_metadata target_config, then to metadata catalog.
if _BRONZE_CAT:
    DLT_CATALOG = _BRONZE_CAT
    DLT_SCHEMA  = _TARGET_SCHEMA or SCHEMA
    print(f"📦 Using explicit bronze_catalog widget: {{DLT_CATALOG}}.{{DLT_SCHEMA}}")
else:
    # Fallback: read from wf_job_metadata target_config
    try:
        _first_target = spark.sql(f"""
            SELECT target_config FROM `{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata
            WHERE target_config IS NOT NULL AND LENGTH(TRIM(target_config)) > 2
            LIMIT 1
        """).first()
        if _first_target:
            _tcfg = json.loads(_first_target[0] or "{{}}")
            DLT_CATALOG = _tcfg.get("bronze_catalog", "")
            DLT_SCHEMA  = _tcfg.get("target_schema", "")
        else:
            DLT_CATALOG = ""
            DLT_SCHEMA  = ""
    except Exception:
        DLT_CATALOG = ""
        DLT_SCHEMA  = ""

# Safety: never use the metadata catalog for DLT data tables
if DLT_CATALOG == CATALOG and _BRONZE_CAT:
    DLT_CATALOG = _BRONZE_CAT
if DLT_SCHEMA == SCHEMA and _TARGET_SCHEMA:
    DLT_SCHEMA = _TARGET_SCHEMA

# ── FAIL-FAST: DLT MUST have a valid catalog \u2500\u2500
if not DLT_CATALOG or DLT_CATALOG == CATALOG:
    raise ValueError(
        f"FATAL: bronze_catalog is empty or same as metadata catalog ({{CATALOG}}). "
        "Cannot run Spark Declarative Pipeline without explicit bronze_catalog. "
        "Re-create the pipeline group with bronze/silver catalogs set."
    )
if not DLT_SCHEMA:
    DLT_SCHEMA = "hr"
    print(f"⚠️ DLT_SCHEMA was empty, defaulting to 'hr'")

# Ensure the DLT output schema exists
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{{DLT_CATALOG}}`.`{{DLT_SCHEMA}}`")
    print(f"✅ Ensured schema exists: {{DLT_CATALOG}}.{{DLT_SCHEMA}}")
except Exception as schema_err:
    print(f"⚠️ Could not create schema {{DLT_CATALOG}}.{{DLT_SCHEMA}}: {{schema_err}}")

print(f"📦 DLT output target: {{DLT_CATALOG}}.{{DLT_SCHEMA}}")
print(f"📋 Metadata source:   {{CATALOG}}.{{SCHEMA}}")

pipeline_cfg = {{
    "pipeline.meta_catalog":  CATALOG,
    "pipeline.meta_schema":   SCHEMA,
    "pipeline.landing_path":  LANDING_PATH,
    "pipeline.group_id":      GROUP_ID,
    # Allow this pipeline to (re)claim tables previously written by another DLT
    # pipeline. Required when switching from single-catalog to multi-catalog
    # (bronze.hr.* + silver.hr.*) publishing layout.
    "pipelines.tableManagedByMultiplePipelinesCheck.enabled": "false",
}}

pipeline_spec = {{
    "name":          DLT_NAME,
    "catalog":       DLT_CATALOG,
    "schema":        DLT_SCHEMA,
    "configuration": pipeline_cfg,
    "libraries":     [{{"notebook": {{"path": DLT_NB}}}}],
    "continuous":    False,
    "development":   True,
    "channel":       "CURRENT",
    "serverless":    True,
}}

# ── Collect active group_ids from wf_job_metadata ──
_active_groups = set()
try:
    _ag_rows = spark.sql(f"SELECT DISTINCT group_id FROM {{job_tbl}} WHERE group_id IS NOT NULL").collect()
    _active_groups = set(r[0] for r in _ag_rows if r[0])
except Exception:
    pass
print(f"📋 Active group_ids in metadata: {{len(_active_groups)}}")

# ── Clean up stale Spark Declarative Pipelines for same catalog.schema ──
# A stale pipeline is one whose group_id is no longer in wf_job_metadata.
# This prevents "Table already managed by another pipeline" errors.

# ── Step 1: Find existing pipeline by name (fast, reliable) ──────
existing = None

# Use filter param to search by name directly (avoids pagination issues)
_search_resp = requests.get(
    f"{{HOST}}/api/2.0/pipelines",
    params={{"filter": f"name LIKE '{{DLT_NAME}}'", "max_results": 10}},
    headers=_hdrs,
)
if _search_resp.ok:
    for p in _search_resp.json().get("statuses", []):
        if p.get("name") == DLT_NAME:
            existing = {{"pipeline_id": p["pipeline_id"], "name": p["name"]}}
            break

# Fallback: paginate through all pipelines if filter didn't work
if not existing:
    _next_token = None
    _searched_all = False
    while not _searched_all:
        _params = {{"max_results": 100}}
        if _next_token:
            _params["page_token"] = _next_token
        _lr = requests.get(f"{{HOST}}/api/2.0/pipelines", params=_params, headers=_hdrs)
        if not _lr.ok:
            break
        _lr_json = _lr.json()
        for p in _lr_json.get("statuses", []):
            if p.get("name") == DLT_NAME:
                existing = {{"pipeline_id": p["pipeline_id"], "name": p["name"]}}
                _searched_all = True
                break
        _next_token = _lr_json.get("next_page_token")
        if not _next_token:
            _searched_all = True

# ── Step 2: Clean up stale pipelines for same catalog.schema ─────
# Only do this on the first page of results (best-effort cleanup)
_first_page = requests.get(
    f"{{HOST}}/api/2.0/pipelines",
    params={{"max_results": 100}},
    headers=_hdrs,
)
_all_pipelines = _first_page.json().get("statuses", []) if _first_page.ok else []
_stale_ids = []

for p in _all_pipelines:
    pid = p.get("pipeline_id", "")
    pname = p.get("name", "")
    if pname == DLT_NAME:
        continue  # this is ours, not stale
    try:
        pd = requests.get(f"{{HOST}}/api/2.0/pipelines/{{pid}}", headers=_hdrs)
        if not pd.ok:
            continue
        pspec = pd.json().get("spec", {{}})
        pcfg  = pspec.get("configuration", {{}})
        p_cat = pspec.get("catalog", "")
        p_sch = pspec.get("schema", "")
        p_gid = pcfg.get("pipeline.group_id", "")

        # Only consider pipelines targeting OUR catalog.schema
        if p_cat != DLT_CATALOG or p_sch != DLT_SCHEMA:
            continue

        # Pipeline belongs to a group_id that is no longer active → stale
        if p_gid and p_gid not in _active_groups:
            _stale_ids.append((pid, pname, p_gid))
    except Exception:
        pass

# Delete stale pipelines so their table ownership is released
for _s_pid, _s_name, _s_gid in _stale_ids:
    print(f"🗑️ Deleting stale Spark Declarative Pipeline '{{_s_name}}' ({{_s_pid}}) — group {{_s_gid}} no longer active")
    try:
        requests.delete(f"{{HOST}}/api/2.0/pipelines/{{_s_pid}}", headers=_hdrs)
        print(f"   ✅ Deleted")
    except Exception as del_err:
        print(f"   ⚠️ Delete failed: {{del_err}}")

# ── Step 3: Create or update the pipeline ────────────────────────
if existing:
    pipeline_id = existing["pipeline_id"]
    print(f"📦 Existing Spark Declarative Pipeline: {{DLT_NAME}} ({{pipeline_id}})")
    # Verify it still exists (may be stale)
    verify_r = requests.get(f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}", headers=_hdrs)
    if verify_r.status_code == 404:
        print(f"⚠️ Pipeline {{pipeline_id}} no longer exists — will recreate.")
        existing = None
    else:
        # Update pipeline config
        requests.put(
            f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}",
            json=pipeline_spec,
            headers=_hdrs,
        )

if not existing:
    print(f"📦 Creating Spark Declarative Pipeline: {{DLT_NAME}}")
    cr = requests.post(f"{{HOST}}/api/2.0/pipelines", json=pipeline_spec, headers=_hdrs)
    # Handle 409 (pipeline already exists but wasn't found in listing)
    if cr.status_code == 409:
        print("⚠️ 409 Conflict — pipeline already exists. Searching by name…")
        # Re-search with filter
        _retry_search = requests.get(
            f"{{HOST}}/api/2.0/pipelines",
            params={{"filter": f"name LIKE '{{DLT_NAME}}'", "max_results": 10}},
            headers=_hdrs,
        )
        _found = False
        if _retry_search.ok:
            for p in _retry_search.json().get("statuses", []):
                if p.get("name") == DLT_NAME:
                    pipeline_id = p["pipeline_id"]
                    print(f"✅ Found existing pipeline: {{pipeline_id}} — updating config")
                    requests.put(
                        f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}",
                        json=pipeline_spec,
                        headers=_hdrs,
                    )
                    _found = True
                    break
        if not _found:
            # Last resort: list ALL pipelines with pagination
            _nt = None
            while not _found:
                _pp = {{"max_results": 100}}
                if _nt:
                    _pp["page_token"] = _nt
                _pr = requests.get(f"{{HOST}}/api/2.0/pipelines", params=_pp, headers=_hdrs)
                if not _pr.ok:
                    break
                _prj = _pr.json()
                for p in _prj.get("statuses", []):
                    if p.get("name") == DLT_NAME:
                        pipeline_id = p["pipeline_id"]
                        print(f"✅ Found existing pipeline (page scan): {{pipeline_id}}")
                        requests.put(
                            f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}",
                            json=pipeline_spec,
                            headers=_hdrs,
                        )
                        _found = True
                        break
                _nt = _prj.get("next_page_token")
                if not _nt:
                    break
            if not _found:
                raise Exception(f"409 Conflict creating pipeline '{{DLT_NAME}}' but could not find existing pipeline. Delete manually in Databricks UI.")
    else:
        cr.raise_for_status()
        pipeline_id = cr.json()["pipeline_id"]
        print(f"✅ Created Spark Declarative Pipeline: {{pipeline_id}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Pre-DLT: Drop non-DLT managed tables that would collide

# COMMAND ----------

# DLT creates streaming tables named bronze_<tbl> and silver_<tbl>.
# If regular MANAGED tables with those names already exist (e.g. from
# a CTAS restore or manual creation), DLT will fail with:
# "Could not materialize ... because a MANAGED table already exists".
# We detect and drop those here so DLT can recreate them as streaming tables.
#
# Query the SAME stages the DLT notebook uses to discover tables.

_dlt_job_names = set()
for _j in extract_jobs:
    _tname = _j.get("table_name") or _j.get("job_name", "").split(".")[-1]
    if _tname:
        _dlt_job_names.add(_tname)

# Also query DLT-specific stages to catch tables not in extract_jobs
try:
    _gf2 = f"AND group_id = '{{GROUP_ID}}'" if GROUP_ID else ""
    _dlt_stage_rows = spark.sql(f"""
        SELECT DISTINCT table_name FROM `{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata
        WHERE stage IN ('landing_to_bronze', 'bronze_to_silver', 'dlt_bronze_silver')
          AND (enabled = true OR enabled IS NULL)
          {{_gf2}}
    """).collect()
    for _r in _dlt_stage_rows:
        if _r[0]:
            _dlt_job_names.add(_r[0])
except Exception:
    pass

_dlt_tables = []
for _tname in _dlt_job_names:
    _dlt_tables.append(f"bronze_{{_tname}}")
    _dlt_tables.append(f"silver_{{_tname}}")

print(f"🔍 Collision check: {{len(_dlt_job_names)}} table names → {{len(_dlt_tables)}} DLT targets to verify in `{{DLT_CATALOG}}`.`{{DLT_SCHEMA}}`")

_dropped_pre = 0
for _dt in _dlt_tables:
    _fqn = f"`{{DLT_CATALOG}}`.`{{DLT_SCHEMA}}`.`{{_dt}}`"
    try:
        _info = spark.sql(f"DESCRIBE EXTENDED {{_fqn}}")
        _type_row = [r for r in _info.collect() if r[0].strip().lower() == "type"]
        _tbl_type = _type_row[0][1].strip().upper() if _type_row else ""
        # Only drop if it's a plain MANAGED table, NOT a streaming table (DLT-owned)
        if _tbl_type == "MANAGED" or _tbl_type == "":
            # Check if it's actually a DLT streaming table by looking for pipeline_id
            _prop_rows = {{r[0].strip().lower(): r[1] for r in _info.collect()}}
            if "pipelines.pipelineid" not in _prop_rows:
                spark.sql(f"DROP TABLE IF EXISTS {{_fqn}}")
                print(f"  🧹 Dropped pre-existing non-DLT table: {{_fqn}}")
                _dropped_pre += 1
    except Exception:
        pass  # Table doesn't exist — fine

if _dropped_pre:
    print(f"🧹 Dropped {{_dropped_pre}} pre-existing non-DLT tables to avoid collisions")
else:
    print(f"✅ No pre-existing table collisions found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Trigger Spark Declarative Pipeline Update

# COMMAND ----------

full_refresh = LOAD_OVERRIDE.lower() == "full" if LOAD_OVERRIDE else False
# Only force full_refresh if we dropped pre-existing colliding tables
_force_full = full_refresh or _dropped_pre > 0
if _force_full and not full_refresh:
    print(f"🔄 Forcing full_refresh because {{_dropped_pre}} colliding non-DLT tables were dropped")
print(f"🚀 Triggering DLT update (full_refresh={{_force_full}})…")

# Verify pipeline exists before triggering
verify_before = requests.get(f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}", headers=_hdrs)
if verify_before.status_code == 404:
    raise Exception(f"Spark Declarative Pipeline {{pipeline_id}} not found (404). It may have been deleted. Please re-run to recreate.")
verify_before.raise_for_status()

# Check if pipeline already has an active update — wait or stop it
pipe_info = verify_before.json()
pipe_state = pipe_info.get("state", "")
if pipe_state not in ("IDLE", ""):
    print(f"⏳ Pipeline is currently {{pipe_state}} — waiting for it to finish…")
    wait_count = 0
    while pipe_state not in ("IDLE", "FAILED", ""):
        if wait_count >= 60:  # 60 × 10s = 10 min max wait
            # Stop the running update so we can start a fresh one
            print(f"⏳ Pipeline still busy after 10 min — stopping active update…")
            try:
                requests.post(f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}/stop", headers=_hdrs)
                time.sleep(15)
            except Exception:
                pass
            break
        time.sleep(10)
        wait_count += 1
        try:
            wr = requests.get(f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}", headers=_hdrs)
            if wr.ok:
                pipe_state = wr.json().get("state", "")
                if wait_count % 3 == 0:
                    print(f"  ⏳ Pipeline state: {{pipe_state}} ({{wait_count * 10}}s)")
        except Exception:
            break
    print(f"✅ Pipeline is now {{pipe_state}} — ready to trigger.")

trigger_resp = requests.post(
    f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}/updates",
    json={{"full_refresh": _force_full}},
    headers=_hdrs,
)
# Handle 409 Conflict: pipeline still has an active update — exponential backoff
if trigger_resp.status_code == 409:
    _max_409_retries = 4
    _backoff_secs = [20, 40, 80, 120]
    for _retry_idx in range(_max_409_retries):
        _wait = _backoff_secs[_retry_idx] if _retry_idx < len(_backoff_secs) else 120
        print(f"⚠️ 409 Conflict (attempt {{_retry_idx + 1}}/{{_max_409_retries}}). Stopping pipeline, waiting {{_wait}}s…")
        try:
            requests.post(f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}/stop", headers=_hdrs)
        except Exception:
            pass
        time.sleep(_wait)
        trigger_resp = requests.post(
            f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}/updates",
            json={{"full_refresh": _force_full}},
            headers=_hdrs,
        )
        if trigger_resp.status_code != 409:
            break
    if trigger_resp.status_code == 409:
        raise Exception(f"Spark Declarative Pipeline {{pipeline_id}} still has an active update after {{_max_409_retries}} retries. Check Databricks UI.")
trigger_resp.raise_for_status()
update_id = trigger_resp.json().get("update_id", "")
print(f"📋 Update ID: {{update_id}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⏳ Poll for DLT Completion

# COMMAND ----------

terminal_states = {{"COMPLETED", "FAILED", "CANCELED"}}
dlt_status  = "WAITING"
poll_count  = 0
MAX_POLLS   = 360   # 360 × 10s = 60 min max
POLL_INTERVAL = 10  # seconds between polls
consecutive_404 = 0
MAX_404 = 3         # Give up after 3 consecutive 404s (pipeline does not exist)

print(f"🔗 Spark Declarative Pipeline URL: {{HOST}}/#joblist/pipelines/{{pipeline_id}}")

while dlt_status not in terminal_states and poll_count < MAX_POLLS:
    time.sleep(POLL_INTERVAL)
    poll_count += 1
    try:
        pr = requests.get(f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}", headers=_hdrs)
        if pr.status_code == 404:
            consecutive_404 += 1
            print(f"  ⚠️ Pipeline not found (404) — attempt {{consecutive_404}}/{{MAX_404}}")
            if consecutive_404 >= MAX_404:
                print(f"  ❌ Pipeline {{pipeline_id}} does not exist. Stopping poll.")
                dlt_status = "FAILED"
                break
            continue
        consecutive_404 = 0
        pr.raise_for_status()
        pipe_data = pr.json()
        latest = (pipe_data.get("latest_updates") or [{{}}])[0]
        update_state = latest.get("state", "")
        if update_state in terminal_states:
            dlt_status = update_state
        elif pipe_data.get("state") in terminal_states:
            dlt_status = pipe_data["state"]
        if poll_count % 3 == 0:
            elapsed = poll_count * POLL_INTERVAL
            print(f"  ⏳ DLT status: {{update_state or pipe_data.get('state','UNKNOWN')}} ({{elapsed}}s)")
    except Exception as e:
        print(f"  ⚠️ Poll error: {{e}}")

if poll_count >= MAX_POLLS and dlt_status not in terminal_states:
    dlt_status = "TIMEOUT"
print(f"\\n⚡ Spark Declarative Pipeline finished: {{dlt_status}}")

# Fetch error details if DLT failed
if dlt_status == "FAILED":
    print("\\n❌ Spark Declarative Pipeline FAILED — fetching diagnostics...")

    # 1. Fetch update-level cause (most useful)
    if update_id:
        try:
            upd_resp = requests.get(
                f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}/updates/{{update_id}}",
                headers=_hdrs,
            )
            if upd_resp.ok:
                upd = upd_resp.json().get("update", {{}})
                cause = upd.get("cause", "")
                if cause:
                    print(f"\\n📋 Update Cause: {{cause}}")
                # Check for cluster/compute errors
                cluster_id = upd.get("cluster_id", "")
                if cluster_id:
                    print(f"   Cluster: {{cluster_id}}")
        except Exception:
            pass

    # 2. Fetch pipeline events (errors, flow progress)
    try:
        ev_resp = requests.get(
            f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}/events",
            params={{"max_results": 50, "order_by": "timestamp desc"}},
            headers=_hdrs,
        )
        ev_resp.raise_for_status()
        events = ev_resp.json().get("events", [])

        # Filter for actual error events (not generic update_progress)
        error_events = [
            e for e in events
            if (e.get("level") == "ERROR" and e.get("event_type") != "update_progress")
            or (e.get("event_type") == "flow_progress" and "ERROR" in json.dumps(e.get("details", {{}})))
        ]

        if error_events:
            print("\\n❌ DLT Error Events:")
            for ev in error_events[:10]:
                etype = ev.get("event_type", "")
                msg = ev.get("message", "")
                details = ev.get("details", {{}})
                # Extract nested error messages from details
                if not msg and isinstance(details, dict):
                    msg = details.get("cause", "") or details.get("reason", "") or json.dumps(details)
                print(f"  • [{{etype}}] {{msg[:500]}}")
        else:
            # Fallback: show ALL recent events for debugging
            print("\\n⚠️ No specific error events — showing recent pipeline events:")
            for ev in events[:8]:
                etype = ev.get("event_type", "")
                msg = ev.get("message", "")
                lvl = ev.get("level", "")
                print(f"  • [{{lvl}}/{{etype}}] {{msg[:300]}}")
    except Exception as ev_err:
        print(f"\\n⚠️ Could not fetch DLT events: {{ev_err}}")

    print(f"\\n🔗 Check SDP UI: {{HOST}}/#joblist/pipelines/{{pipeline_id}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Phase 3 — Relocate Silver Tables to Silver Catalog

# COMMAND ----------

silver_relocated = 0
silver_failed    = 0

if dlt_status == "COMPLETED":
    # Determine the silver catalog AND schema — use explicit widget first
    SILVER_CATALOG = _SILVER_CAT
    SILVER_SCHEMA  = _TARGET_SCHEMA or DLT_SCHEMA
    if not SILVER_CATALOG:
        # Fallback: read from wf_job_metadata target_config
        try:
            _tgt_row = spark.sql(f"""
                SELECT target_config FROM `{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata
                WHERE target_config IS NOT NULL AND LENGTH(TRIM(target_config)) > 2
                  AND target_config LIKE '%silver_catalog%'
                LIMIT 1
            """).first()
            if _tgt_row:
                _tgt = json.loads(_tgt_row[0] or "{{}}")
                SILVER_CATALOG = _tgt.get("silver_catalog", "")
                SILVER_SCHEMA  = _tgt.get("target_schema", "") or DLT_SCHEMA
        except Exception:
            pass

    if not SILVER_CATALOG:
        raise ValueError(
            "FATAL: silver_catalog is empty — cannot relocate silver tables. "
            "Re-create the pipeline group with bronze/silver catalogs set."
        )

    if SILVER_CATALOG and SILVER_CATALOG != DLT_CATALOG:
        print(f"🔄 Relocating silver tables: {{DLT_CATALOG}}.{{DLT_SCHEMA}} → {{SILVER_CATALOG}}.{{SILVER_SCHEMA}}")
        print(f"📦 Bronze tables (DLT-managed) stay in: {{DLT_CATALOG}}.{{DLT_SCHEMA}}")
        print(f"📦 Spark Declarative Pipeline '{{DLT_NAME}}' ({{pipeline_id}}) is preserved — NOT deleted")

        # Ensure silver schema exists
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{{SILVER_CATALOG}}`.`{{SILVER_SCHEMA}}`")
        except Exception as se:
            print(f"⚠️ Could not create schema {{SILVER_CATALOG}}.{{SILVER_SCHEMA}}: {{se}}")

        # Discover silver tables in the DLT catalog
        silver_tables = []
        try:
            _all_tables = [r[1] for r in spark.sql(f"""
                SHOW TABLES IN `{{DLT_CATALOG}}`.`{{DLT_SCHEMA}}`
            """).collect()]
            silver_tables = [t for t in _all_tables if t.startswith("silver_")]
        except Exception:
            pass

        print(f"📋 Found {{len(silver_tables)}} silver tables to relocate")

        # ── Copy silver tables to silver catalog via CTAS ──
        # The Spark Declarative Pipeline creates silver tables as materialized views in DLT_CATALOG.
        # We copy them to the silver catalog. Bronze DLT streaming tables are untouched.
        # Strip "silver_" prefix so the destination table has a clean name
        # (e.g. silver.hr.DimDate instead of silver.hr.silver_DimDate).
        import time as _time
        for stbl in silver_tables:
            # Strip prefix for clean destination name
            clean_name = stbl[7:] if stbl.startswith("silver_") else stbl
            src_full = f"`{{DLT_CATALOG}}`.`{{DLT_SCHEMA}}`.`{{stbl}}`"
            dst_full = f"`{{SILVER_CATALOG}}`.`{{SILVER_SCHEMA}}`.`{{clean_name}}`"
            print(f"  📋 {{src_full}} → {{dst_full}}")
            try:
                spark.sql(f"SELECT 1 FROM {{src_full}} LIMIT 1")
            except Exception as src_err:
                if "TABLE_OR_VIEW_NOT_FOUND" in str(src_err):
                    print(f"    ⏭️ Source table not found — skipping {{stbl}}")
                    continue
                print(f"    ⚠️ Source table unreadable: {{src_err}}")

            try:
                # Drop existing destination to allow fresh copy
                for _drop_sql in [
                    f"DROP TABLE IF EXISTS {{dst_full}}",
                ]:
                    try:
                        spark.sql(_drop_sql)
                    except Exception:
                        pass
                _time.sleep(1)
                spark.sql(f"CREATE TABLE {{dst_full}} AS SELECT * FROM {{src_full}}")
                silver_relocated += 1
                print(f"    ✅ Relocated {{stbl}}")
            except Exception as rel_err:
                silver_failed += 1
                print(f"    ❌ Failed to relocate {{stbl}}: {{rel_err}}")

        print(f"\\n📦 Silver relocation: {{silver_relocated}} ok / {{silver_failed}} failed")
        print(f"📦 Bronze tables: untouched (DLT streaming tables preserved)")
        print(f"📦 Spark Declarative Pipeline: preserved ({{pipeline_id}})")
    else:
        if not SILVER_CATALOG:
            print("ℹ️ No silver_catalog in target_config — silver tables remain in DLT catalog")
        else:
            print(f"ℹ️ Silver catalog same as DLT catalog ({{SILVER_CATALOG}}) — no relocation needed")
else:
    print("⏭️ Skipping silver relocation — Spark Declarative Pipeline did not complete successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Phase 4 — Run Reconciliation

# COMMAND ----------

recon_status = "SKIPPED"
if dlt_status == "COMPLETED" and extract_results:
    try:
        recon_nb = f"{{WORKSPACE_PATH}}/04_Meta_Reconciliation"
        recon_ok_count = 0
        recon_fail_count = 0

        # Run reconciliation for each successfully extracted job
        for job_idx, job in enumerate(extract_jobs):
            if extract_results[job_idx]["status"] != "OK":
                continue
            try:
                jid   = job["job_id"]
                rid   = extract_results[job_idx].get("run_id", "")
                print(f"  📊 Reconciling: {{job['job_name']}}")
                dbutils.notebook.run(recon_nb, 1800, {{
                    "job_id": jid, "run_id": rid,
                    "password_b64": PASSWORD_B64,
                    "catalog": CATALOG, "schema": SCHEMA,
                    "landing_path": LANDING_PATH,
                }})
                recon_ok_count += 1
            except Exception as rj_err:
                recon_fail_count += 1
                print(f"    ⚠️ Recon failed for {{job['job_name']}}: {{rj_err}}")

        recon_status = f"{{recon_ok_count}} ok / {{recon_fail_count}} failed"
        print(f"  ✅ Reconciliation: {{recon_status}}")
    except Exception as recon_err:
        recon_status = "FAILED"
        print(f"  ❌ Reconciliation failed: {{recon_err}}")
else:
    print("⏭️ Skipping reconciliation — Spark Declarative Pipeline did not complete successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Phase 5 — Run Execution Logging

# COMMAND ----------

log_status = "SKIPPED"
if dlt_status == "COMPLETED":
    try:
        log_nb = f"{{WORKSPACE_PATH}}/05_Meta_ExecutionLog"
        print(f"📝 Running execution logging: {{log_nb}}")

        # Build results JSON for the execution log
        _log_results = json.dumps(extract_results)
        _log_groups  = json.dumps([g.get("group_id","") for g in groups])
        _orch_status = "COMPLETED" if dlt_status == "COMPLETED" and not extract_fail else "PARTIAL"

        log_result = dbutils.notebook.run(log_nb, 1800, {{
            "catalog": CATALOG, "schema": SCHEMA,
            "results_json": _log_results,
            "groups_json": _log_groups,
            "orchestrator_status": _orch_status,
        }})
        log_status = "COMPLETED"
        print(f"  ✅ Execution logging complete")
    except Exception as log_err:
        log_status = "FAILED"
        print(f"  ❌ Execution logging failed: {{log_err}}")
else:
    print("⏭️ Skipping execution logging — Spark Declarative Pipeline did not complete successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## �📊 Orchestration Summary

# COMMAND ----------

total_rows = sum(r.get("rows", 0) for r in extract_results)

print(f"\\n{{'='*60}}")
print(f"📊 DLT ORCHESTRATION COMPLETE")
print(f"{{'='*60}}")
print(f"  📥 Extracts        : {{extract_ok}} ok / {{extract_fail}} failed")
print(f"  ⚡ Spark Declarative Pipeline    : {{dlt_status}}")
print(f"  🔄 Silver Relocated: {{silver_relocated}} ok / {{silver_failed}} failed")
print(f"  📊 Reconciliation  : {{recon_status}}")
print(f"  📝 Execution Log   : {{log_status}}")
print(f"  📊 Rows (JDBC)     : {{total_rows:,}}")
print(f"  🔗 Pipeline ID     : {{pipeline_id}}")

_overall = "COMPLETED"
if extract_fail or silver_failed:
    _overall = "PARTIAL"
if dlt_status != "COMPLETED":
    _overall = "FAILED"

exit_payload = json.dumps({{
    "status":          _overall,
    "extract_ok":      extract_ok,
    "extract_failed":  extract_fail,
    "dlt_status":      dlt_status,
    "silver_relocated": silver_relocated,
    "silver_failed":   silver_failed,
    "recon_status":    recon_status,
    "log_status":      log_status,
    "pipeline_id":     pipeline_id,
    "total_rows":      total_rows,
}})
dbutils.notebook.exit(exit_payload)
'''
