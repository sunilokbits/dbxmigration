# Databricks notebook source
# pyright: reportUndefinedVariable=false
# MAGIC %md
# MAGIC # Metadata-Driven Extract — Landing Zone
# MAGIC
# MAGIC Reads job metadata from Delta tables and extracts the specified
# MAGIC source table via JDBC into the Landing Zone.
# MAGIC
# MAGIC **Parameters (widgets):**
# MAGIC - `job_id` — Job to execute (from `wf_job_metadata`)
# MAGIC - `run_id` — Run tracking ID (written to `wf_run_history`)
# MAGIC - `load_type` — `full` or `incremental` (override)
# MAGIC - `password_b64` — Base64-encoded source DB password
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("load_type", "", "Load Type Override (full/incremental)")
dbutils.widgets.text("password_b64", "", "Source DB Password (base64)")
dbutils.widgets.text("catalog", "main", "Metadata Catalog")
dbutils.widgets.text("schema", "default", "Metadata Schema")
dbutils.widgets.text("landing_path", "/mnt/landing", "Landing Base Path")
dbutils.widgets.text("cdc_mode", "", "CDC Mode (watermark/change_tracking)")

import base64

JOB_ID       = dbutils.widgets.get("job_id").strip()
RUN_ID       = dbutils.widgets.get("run_id").strip()
LOAD_OVERRIDE= dbutils.widgets.get("load_type").strip()
_PWD_B64     = dbutils.widgets.get("password_b64").strip()
PASSWORD     = base64.b64decode(_PWD_B64.encode("ascii")).decode("utf-8") if _PWD_B64 else ""
CATALOG      = dbutils.widgets.get("catalog").strip()
SCHEMA       = dbutils.widgets.get("schema").strip()
LANDING_PATH = dbutils.widgets.get("landing_path").strip()
CDC_MODE     = dbutils.widgets.get("cdc_mode").strip() or ""

print(f"Job ID  : {JOB_ID}")
print(f"Run ID  : {RUN_ID}")
print(f"Catalog : {CATALOG}.{SCHEMA}")
print(f"CDC Mode: {CDC_MODE or 'watermark (default)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Job Metadata from Delta

# COMMAND ----------

import json
from pyspark.sql import functions as F
from datetime import datetime

job_tbl = f"`{CATALOG}`.`{SCHEMA}`.wf_job_metadata"
wm_tbl  = f"`{CATALOG}`.`{SCHEMA}`.wf_watermark_metadata"
run_tbl = f"`{CATALOG}`.`{SCHEMA}`.wf_run_history"

job_df = spark.sql(f"SELECT * FROM {job_tbl} WHERE job_id = '{JOB_ID}'")
if job_df.count() == 0:
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "error": f"Job {JOB_ID} not found in metadata"}))

job = job_df.collect()[0].asDict()
print(f"Job Name       : {job['job_name']}")
print(f"Table          : {job['full_table']}")
print(f"Stage          : {job['stage']}")

source_config = json.loads(job.get("source_config", "{}") or "{}")
SERVER   = source_config.get("server", "")
DATABASE = source_config.get("database", "")
USERNAME = source_config.get("username", "")
SRC_TYPE = source_config.get("source_type", "sqlserver")
TABLE_SCHEMA = job["table_schema"]
TABLE_NAME   = job["table_name"]
FULL_TABLE   = job["full_table"]
LOAD_TYPE    = LOAD_OVERRIDE if LOAD_OVERRIDE else (job["load_type"] or "full")
WM_COL       = job.get("watermark_column", "")

# CDC configuration — prefer widget param, fallback to source_config
if not CDC_MODE:
    CDC_MODE = source_config.get("cdc_mode", "watermark")
# primary_keys are required for Change Tracking MERGE operations
PRIMARY_KEYS = source_config.get("primary_keys", [])

# Multi-catalog support
target_config = json.loads(job.get("target_config", "{}") or "{}")
VOLUMES_CATALOG = target_config.get("volumes_catalog", "")
TGT_SCHEMA      = target_config.get("target_schema", "")
if VOLUMES_CATALOG and TGT_SCHEMA:
    LANDING_PATH = f"/Volumes/{VOLUMES_CATALOG}/{TGT_SCHEMA}/landing"
    print(f"Multi-catalog: Landing -> UC Volumes: {LANDING_PATH}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{VOLUMES_CATALOG}`.`{TGT_SCHEMA}`")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{VOLUMES_CATALOG}`.`{TGT_SCHEMA}`.`landing`")

print(f"Source: {SRC_TYPE} -> {SERVER}/{DATABASE}")
print(f"Load Type: {LOAD_TYPE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## JDBC Connection

# COMMAND ----------

encrypt = "true" if SRC_TYPE in ("azuresql", "synapse") else "false"
trust   = "false" if SRC_TYPE in ("azuresql", "synapse") else "true"

if "," in SERVER:
    _host, _port = SERVER.rsplit(",", 1)
elif ":" in SERVER:
    _host, _port = SERVER.rsplit(":", 1)
else:
    _host, _port = SERVER, "1433"

jdbc_url = (
    f"jdbc:sqlserver://{_host}:{_port};databaseName={DATABASE};"
    f"encrypt={encrypt};trustServerCertificate={trust};"
    f"loginTimeout=60;socketTimeout=0;selectMethod=cursor"
)

jdbc_props = {
    "user":     USERNAME,
    "password": PASSWORD,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "fetchsize": "10000",
    "queryTimeout": "0",
    "loginTimeout": "60",
    "socketTimeout": "0",
}

try:
    test_df = spark.read.jdbc(jdbc_url, "(SELECT 1 AS ok) AS t", properties=jdbc_props)
    test_df.collect()
    print("JDBC connection verified")
except Exception as e:
    msg = f"JDBC connection failed: {e}"
    print(msg)
    try:
        spark.sql(f"""
            MERGE INTO {run_tbl} AS t
            USING (SELECT '{RUN_ID}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'failed',
                t.error_message = '{str(e).replace("'","''")[:500]}',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "stage": "connection", "error": str(e)[:500]}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Watermark (Incremental)

# COMMAND ----------

watermark = None
ct_version = None
use_incremental = (LOAD_TYPE == "incremental" and (WM_COL or CDC_MODE == "change_tracking"))

if use_incremental:
    if CDC_MODE == "change_tracking":
        # SQL Server Change Tracking — read last synced CT version
        try:
            wm_df = spark.sql(f"SELECT last_value FROM {wm_tbl} WHERE table_name = '{FULL_TABLE}'")
            rows = wm_df.collect()
            if rows and rows[0]["last_value"]:
                ct_version = int(rows[0]["last_value"])
                print(f"Change Tracking version found: {ct_version}")
            else:
                print("No CT version — will do initial full load with CHANGE_TRACKING_CURRENT_VERSION()")
        except Exception:
            print("Watermark table not found — will do full load")
    else:
        # Classic watermark-based incremental
        try:
            wm_df = spark.sql(f"SELECT last_value FROM {wm_tbl} WHERE table_name = '{FULL_TABLE}'")
            rows = wm_df.collect()
            if rows and rows[0]["last_value"]:
                watermark = rows[0]["last_value"]
                print(f"Watermark found: {WM_COL} > '{watermark}'")
            else:
                print("No watermark — will do initial full load")
        except Exception:
            print("Watermark table not found — will do full load")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Data

# COMMAND ----------

run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
landing_dest = f"{LANDING_PATH}/{TABLE_NAME}"

# Determine the CDC operation column name
_cdc_operation_col = "__cdc_operation"

if CDC_MODE == "change_tracking" and use_incremental:
    if ct_version is not None:
        # Incremental Change Tracking — CHANGETABLE(CHANGES …) with INNER JOIN
        pk_join = " AND ".join([f"CT.[{pk}] = T.[{pk}]" for pk in PRIMARY_KEYS]) if PRIMARY_KEYS else "CT.[id] = T.[id]"
        pk_select = ", ".join([f"CT.[{pk}]" for pk in PRIMARY_KEYS]) if PRIMARY_KEYS else "CT.[id]"
        query = (
            f"(SELECT T.*, CT.SYS_CHANGE_OPERATION AS {_cdc_operation_col}, "
            f"CT.SYS_CHANGE_VERSION AS __cdc_version "
            f"FROM CHANGETABLE(CHANGES [{TABLE_SCHEMA}].[{TABLE_NAME}], {ct_version}) AS CT "
            f"INNER JOIN [{TABLE_SCHEMA}].[{TABLE_NAME}] AS T ON {pk_join} "
            f"WHERE CT.SYS_CHANGE_OPERATION IN ('I','U')) AS q"
        )
        print(f"Change Tracking incremental: CHANGETABLE(CHANGES, {ct_version})")
    else:
        # First load — full snapshot + get current CT version
        query = f"[{TABLE_SCHEMA}].[{TABLE_NAME}]"
        print(f"Change Tracking initial full load from [{TABLE_SCHEMA}].[{TABLE_NAME}]")
elif use_incremental and watermark:
    query = f"(SELECT * FROM [{TABLE_SCHEMA}].[{TABLE_NAME}] WHERE [{WM_COL}] > '{watermark}') AS q"
    print(f"Incremental extract: {WM_COL} > '{watermark}'")
else:
    query = f"[{TABLE_SCHEMA}].[{TABLE_NAME}]"
    print(f"Full extract from [{TABLE_SCHEMA}].[{TABLE_NAME}]")

try:
    df = spark.read.jdbc(jdbc_url, query, properties=jdbc_props)

    df = (df
          .withColumn("__landing_ts", F.current_timestamp())
          .withColumn("__source_system", F.lit(f"{SERVER}/{DATABASE}"))
          .withColumn("__load_type", F.lit("incremental" if use_incremental and (watermark or ct_version is not None) else "full"))
          .withColumn("__batch_id", F.lit(run_ts))
          .withColumn("__job_id", F.lit(JOB_ID))
          .withColumn("__run_id", F.lit(RUN_ID))
          .withColumn("__cdc_mode", F.lit(CDC_MODE or "watermark")))

    # For CT initial full load, add synthetic CDC operation
    if CDC_MODE == "change_tracking" and ct_version is None:
        df = df.withColumn("__cdc_operation", F.lit("I"))

    row_count = df.count()
    print(f"Rows extracted: {row_count:,}")

    if LOAD_TYPE == "full" or not use_incremental or (CDC_MODE == "change_tracking" and ct_version is None):
        df.write.mode("overwrite").option("overwriteSchema", "true").parquet(landing_dest)
        print(f"Written to {landing_dest} (overwrite)")
    else:
        df.write.mode("append").parquet(landing_dest)
        print(f"Appended to {landing_dest}")

except Exception as e:
    msg = f"Extract failed: {e}"
    print(msg)
    try:
        spark.sql(f"""
            MERGE INTO {run_tbl} AS t
            USING (SELECT '{RUN_ID}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'failed',
                t.error_message = '{str(e).replace("'","''")[:500]}',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "stage": "extract", "error": str(e)[:500]}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Watermark & Run History

# COMMAND ----------

if CDC_MODE == "change_tracking" and row_count >= 0:
    # Fetch CHANGE_TRACKING_CURRENT_VERSION() from source
    try:
        ct_ver_query = "(SELECT CHANGE_TRACKING_CURRENT_VERSION() AS ct_ver) AS ctv"
        ct_ver_df = spark.read.jdbc(jdbc_url, ct_ver_query, properties=jdbc_props)
        new_ct_ver = str(ct_ver_df.collect()[0]["ct_ver"])
        spark.sql(f"""
            MERGE INTO {wm_tbl} AS t
            USING (SELECT '{FULL_TABLE}' AS table_name, '{new_ct_ver}' AS last_value, 'SYS_CHANGE_VERSION' AS watermark_column, current_timestamp() AS updated_at) AS s
            ON t.table_name = s.table_name
            WHEN MATCHED THEN UPDATE SET t.last_value = s.last_value, t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"CT version updated: {new_ct_ver}")
    except Exception as e:
        print(f"CT version update failed: {e}")

elif use_incremental and WM_COL and row_count > 0:
    try:
        new_wm = df.agg(F.max(F.col(WM_COL)).cast("string")).collect()[0][0]
        if new_wm:
            spark.sql(f"""
                MERGE INTO {wm_tbl} AS t
                USING (SELECT '{FULL_TABLE}' AS table_name, '{new_wm}' AS last_value, '{WM_COL}' AS watermark_column, current_timestamp() AS updated_at) AS s
                ON t.table_name = s.table_name
                WHEN MATCHED THEN UPDATE SET t.last_value = s.last_value, t.updated_at = s.updated_at
                WHEN NOT MATCHED THEN INSERT *
            """)
            print(f"Watermark updated: {WM_COL} -> {new_wm}")
    except Exception as e:
        print(f"Watermark update failed: {e}")

try:
    spark.sql(f"""
        MERGE INTO {run_tbl} AS t
        USING (SELECT '{RUN_ID}' AS run_id) AS s ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET
            t.status = 'success',
            t.rows_processed = {row_count},
            t.completed_at = current_timestamp(),
            t.duration_sec = unix_timestamp(current_timestamp()) - unix_timestamp(t.started_at)
    """)
except Exception as e:
    print(f"Run history update failed: {e}")

try:
    spark.sql(f"""
        UPDATE {job_tbl}
        SET last_run_id = '{RUN_ID}',
            last_run_at = current_timestamp(),
            last_status = 'success',
            status = 'success',
            run_count = run_count + 1,
            updated_at = current_timestamp()
        WHERE job_id = '{JOB_ID}'
    """)
except Exception as e:
    print(f"Job metadata update failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

exit_payload = json.dumps({
    "status":       "COMPLETED",
    "job_id":       JOB_ID,
    "run_id":       RUN_ID,
    "table":        FULL_TABLE,
    "rows":         row_count,
    "load_type":    LOAD_TYPE,
    "cdc_mode":     CDC_MODE or "watermark",
    "landing_path": landing_dest,
    "batch_id":     run_ts,
})

print(f"\nEXTRACT COMPLETE — {FULL_TABLE} — {row_count:,} rows")
dbutils.notebook.exit(exit_payload)
