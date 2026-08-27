# Databricks notebook source
# pyright: reportUndefinedVariable=false
# pyright: reportMissingImports=false
# MAGIC %md
# MAGIC # Metadata-Driven Bronze Layer
# MAGIC
# MAGIC Reads raw Parquet from Landing Zone, applies schema enforcement,
# MAGIC adds audit columns, and writes to Bronze Delta table.
# MAGIC Driven by `wf_job_metadata` — no hardcoded table names.
# MAGIC ---

# COMMAND ----------

dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("catalog", "main", "Metadata Catalog")
dbutils.widgets.text("schema", "default", "Metadata Schema")
dbutils.widgets.text("landing_path", "/mnt/landing", "Landing Base Path")
dbutils.widgets.text("exec_mode", "standard", "Execution Mode (standard/dlt)")
dbutils.widgets.text("cdc_mode", "", "CDC Mode (watermark/change_tracking)")

JOB_ID       = dbutils.widgets.get("job_id").strip()
RUN_ID       = dbutils.widgets.get("run_id").strip()
CATALOG      = dbutils.widgets.get("catalog").strip()
SCHEMA       = dbutils.widgets.get("schema").strip()
LANDING_PATH = dbutils.widgets.get("landing_path").strip()
EXEC_MODE    = dbutils.widgets.get("exec_mode").strip().lower()
CDC_MODE     = dbutils.widgets.get("cdc_mode").strip() or "watermark"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Job Metadata

# COMMAND ----------

import json
from pyspark.sql import functions as F
from datetime import datetime

job_tbl = f"`{CATALOG}`.`{SCHEMA}`.wf_job_metadata"
run_tbl = f"`{CATALOG}`.`{SCHEMA}`.wf_run_history"

job_df = spark.sql(f"SELECT * FROM {job_tbl} WHERE job_id = '{JOB_ID}'")
if job_df.count() == 0:
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "error": f"Job {JOB_ID} not found"}))

job = job_df.collect()[0].asDict()
TABLE_NAME   = job["table_name"]
TABLE_SCHEMA = job["table_schema"]
FULL_TABLE   = job["full_table"]
LOAD_TYPE    = job.get("load_type", "full") or "full"

# Read CDC mode from source_config if not provided via widget
source_config = json.loads(job.get("source_config", "{}") or "{}")
if not CDC_MODE or CDC_MODE == "watermark":
    CDC_MODE = source_config.get("cdc_mode", CDC_MODE or "watermark")
PRIMARY_KEYS = source_config.get("primary_keys", [])

target_config = json.loads(job.get("target_config", "{}") or "{}")
VOLUMES_CATALOG = target_config.get("volumes_catalog", "")
BRONZE_CATALOG  = target_config.get("bronze_catalog", "")
TGT_SCHEMA      = target_config.get("target_schema", "")

# ─── Auto-derive bronze_catalog if missing ────────────────────────────────────
# Bronze layer should always live in its own catalog OR at minimum its own schema,
# never co-mingled with the metadata or silver layer.
if not BRONZE_CATALOG:
    BRONZE_CATALOG = "bronze"
    print(f"⚠️  bronze_catalog not set — defaulting to '{BRONZE_CATALOG}'")
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{BRONZE_CATALOG}`")
    if TGT_SCHEMA:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{BRONZE_CATALOG}`.`{TGT_SCHEMA}`")
except Exception as _ce:
    print(f"⚠️  Could not auto-create bronze catalog/schema (may need admin): {_ce}")

MULTI_CATALOG   = bool(VOLUMES_CATALOG and BRONZE_CATALOG and TGT_SCHEMA)

if MULTI_CATALOG:
    TARGET_CATALOG = BRONZE_CATALOG
    TARGET_SCHEMA  = TGT_SCHEMA
    TABLE_PREFIX   = ""
    LANDING_PATH   = f"/Volumes/{VOLUMES_CATALOG}/{TGT_SCHEMA}/landing"
    print(f"✅ Multi-catalog medallion: {VOLUMES_CATALOG} -> {BRONZE_CATALOG}.{TGT_SCHEMA} (no prefix)")
else:
    # Fallback: use target_config catalog/schema but NEVER the metadata catalog
    _fallback_cat = target_config.get("catalog", "")
    _meta_cat = target_config.get("metadata_catalog", CATALOG)
    if _fallback_cat and _fallback_cat != _meta_cat and _fallback_cat != CATALOG:
        TARGET_CATALOG = _fallback_cat
    elif BRONZE_CATALOG:
        TARGET_CATALOG = BRONZE_CATALOG
    else:
        TARGET_CATALOG = CATALOG
        print(f"⚠️ WARNING: No bronze_catalog in target_config — falling back to metadata catalog {CATALOG}")
    _fallback_sch = target_config.get("schema", "")
    _meta_sch = target_config.get("metadata_schema", SCHEMA)
    if _fallback_sch and _fallback_sch != _meta_sch and _fallback_sch != SCHEMA:
        TARGET_SCHEMA = _fallback_sch
    elif TGT_SCHEMA:
        TARGET_SCHEMA = TGT_SCHEMA
    else:
        TARGET_SCHEMA = SCHEMA
    TABLE_PREFIX   = "bronze_"

print(f"Job: {job['job_name']}")
print(f"Table: {FULL_TABLE}")
print(f"Target: {TARGET_CATALOG}.{TARGET_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from Landing Zone

# COMMAND ----------

landing_src = f"{LANDING_PATH}/{TABLE_NAME}"
print(f"Reading from: {landing_src}")

try:
    df = spark.read.parquet(landing_src)
    row_count = df.count()
    print(f"Rows in landing: {row_count:,}")
except Exception as e:
    msg = f"Failed to read landing zone: {e}"
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
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "error": str(e)[:500]}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Enforcement & Data Quality Checks

# COMMAND ----------

# Create restore point if table exists
restore_version = None
bronze_table = f"`{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.`{TABLE_PREFIX}{TABLE_NAME}`"
try:
    history = spark.sql(f"DESCRIBE HISTORY {bronze_table} LIMIT 1").collect()
    if history:
        restore_version = history[0]["version"]
        print(f"Restore point: v{restore_version}")
except Exception:
    print("No existing table — first load")

# DQ-01: Empty file check
if row_count == 0:
    print("DQ-01: Landing file has 0 rows — skipping Bronze write")
    try:
        spark.sql(f"""
            MERGE INTO {run_tbl} AS t
            USING (SELECT '{RUN_ID}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'skipped',
                t.error_message = 'Empty landing file - 0 rows',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED", "reason": "empty_landing", "rows": 0}))

# DQ-02: Null-key row detection
audit_cols = [c for c in df.columns if c.startswith("__")]
data_cols  = [c for c in df.columns if c not in audit_cols]
null_key_count = 0
if data_cols:
    all_null = F.col(data_cols[0]).isNull()
    for dc in data_cols[1:]:
        all_null = all_null & F.col(dc).isNull()
    null_key_count = df.filter(all_null).count()
    if null_key_count > 0:
        print(f"DQ-02: {null_key_count} all-null rows detected")

# DQ-03: Duplicate detection
dup_count = row_count - df.dropDuplicates(data_cols).count() if data_cols else 0
if dup_count > 0:
    print(f"DQ-03: {dup_count} duplicate rows detected")

# DQ-04: Schema drift detection
schema_drift = False
try:
    existing = spark.sql(f"DESCRIBE {bronze_table}").select("col_name").rdd.flatMap(lambda x: x).collect()
    existing_data_cols = [c for c in existing if not c.startswith("__") and not c.startswith("#")]
    incoming_data_cols = [c for c in df.columns if not c.startswith("__")]
    new_cols     = set(incoming_data_cols) - set(existing_data_cols)
    dropped_cols = set(existing_data_cols) - set(incoming_data_cols)
    if new_cols:
        schema_drift = True
        print(f"DQ-04 Schema drift — new columns: {new_cols}")
    if dropped_cols:
        schema_drift = True
        print(f"DQ-04 Schema drift — missing columns: {dropped_cols}")
except Exception:
    pass

# DQ-05: Quarantine flagging
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

print(f"\nBronze DQ Summary:")
print(f"   Total rows      : {row_count:,}")
print(f"   Clean rows      : {clean_count:,}")
print(f"   Quarantined     : {quarantined_count:,}")
print(f"   Null-key rows   : {null_key_count}")
print(f"   Duplicates      : {dup_count}")
print(f"   Schema drift    : {'Yes' if schema_drift else 'No'}")

# Save Bronze DQ metrics
try:
    dq_tbl = f"`{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.__dq_metrics"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {dq_tbl} (
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
        INSERT INTO {dq_tbl} VALUES (
            '{RUN_ID}', '{JOB_ID}', '{FULL_TABLE}', 'bronze',
            {row_count}, {clean_count}, {quarantined_count},
            {null_key_count}, {dup_count}, {quarantined_count},
            {'true' if schema_drift else 'false'}, {checks_passed}, {checks_total},
            {dq_score}, current_timestamp()
        )
    """)
except Exception as e:
    print(f"DQ metrics save failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Pipeline Definition (when running as DLT)

# COMMAND ----------

# When running as a DLT pipeline, define the table using DLT decorators
if EXEC_MODE == "dlt":
    import dlt

    @dlt.table(
        name=f"{TABLE_PREFIX}{TABLE_NAME}",
        comment=f"Bronze layer — raw ingestion of {FULL_TABLE} with DLT quality expectations",
        table_properties={
            "quality": "bronze",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
            "delta.enableChangeDataFeed": "true",
            "pipelines.autoOptimize.managed": "true",
        },
    )
    @dlt.expect_or_drop("__valid_landing_ts", "__landing_ts IS NOT NULL")
    @dlt.expect("__has_batch_id", "__batch_id IS NOT NULL")
    @dlt.expect("__has_source", "__source_table IS NOT NULL")
    def bronze_dlt_table():
        landing_src = f"{LANDING_PATH}/{TABLE_NAME}"
        df = spark.read.parquet(landing_src)
        return (df
            .withColumn("__bronze_ts", F.current_timestamp())
            .withColumn("__bronze_version", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
            .withColumn("__source_table", F.lit(FULL_TABLE))
            .withColumn("__job_id", F.lit(JOB_ID))
            .withColumn("__run_id", F.lit(RUN_ID))
            .withColumn("__is_quarantined", F.lit(False)))

    print(f"DLT table defined: {TABLE_PREFIX}{TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta (Standard Mode)

# COMMAND ----------

if EXEC_MODE != "dlt":
    try:
        # Enable CDF on the bronze table for downstream CDC consumers
        if LOAD_TYPE == "full":
            (df_bronze.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option("delta.enableChangeDataFeed", "true")
                .saveAsTable(bronze_table))
            print(f"Full load -> {bronze_table} ({row_count:,} rows)")
        else:
            (df_bronze.write
                .format("delta")
                .mode("append")
                .saveAsTable(bronze_table))
            print(f"Append -> {bronze_table} ({row_count:,} rows)")

        # Enable CDF on the table after first creation
        try:
            spark.sql(f"ALTER TABLE {bronze_table} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
        except Exception:
            pass

    except Exception as e:
        if restore_version is not None:
            try:
                spark.sql(f"RESTORE TABLE {bronze_table} TO VERSION AS OF {restore_version}")
                print(f"Restored to v{restore_version} after failure")
            except Exception:
                pass
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
        dbutils.notebook.exit(json.dumps({"status": "FAILED", "error": str(e)[:500]}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Metadata

# COMMAND ----------

if EXEC_MODE != "dlt":
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
            SET last_run_id = '{RUN_ID}', last_run_at = current_timestamp(),
                last_status = 'success', status = 'success',
                run_count = run_count + 1, updated_at = current_timestamp()
            WHERE job_id = '{JOB_ID}'
        """)
    except Exception as e:
        print(f"Job update failed: {e}")

# COMMAND ----------

if EXEC_MODE != "dlt":
    exit_payload = json.dumps({
        "status": "COMPLETED", "job_id": JOB_ID, "run_id": RUN_ID,
        "table": FULL_TABLE, "rows": row_count, "bronze_table": bronze_table,
        "cdc_mode": CDC_MODE, "exec_mode": EXEC_MODE,
    })
    print(f"\nBRONZE COMPLETE — {FULL_TABLE} — {row_count:,} rows")
    dbutils.notebook.exit(exit_payload)
else:
    print(f"DLT mode — Bronze table '{TABLE_PREFIX}{TABLE_NAME}' managed by Delta Live Tables engine.")
