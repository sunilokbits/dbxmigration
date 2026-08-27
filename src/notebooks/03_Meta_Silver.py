# Databricks notebook source
# pyright: reportUndefinedVariable=false
# pyright: reportMissingImports=false
# MAGIC %md
# MAGIC # Metadata-Driven Silver Layer
# MAGIC
# MAGIC Reads Bronze Delta, applies data quality checks, deduplication,
# MAGIC cleansing, and writes to Silver Delta table.
# MAGIC ---

# COMMAND ----------

dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("catalog", "main", "Metadata Catalog")
dbutils.widgets.text("schema", "default", "Metadata Schema")
dbutils.widgets.text("exec_mode", "standard", "Execution Mode (standard/dlt)")
dbutils.widgets.text("cdc_mode", "", "CDC Mode (watermark/change_tracking)")

JOB_ID    = dbutils.widgets.get("job_id").strip()
RUN_ID    = dbutils.widgets.get("run_id").strip()
CATALOG   = dbutils.widgets.get("catalog").strip()
SCHEMA    = dbutils.widgets.get("schema").strip()
EXEC_MODE = dbutils.widgets.get("exec_mode").strip().lower()
CDC_MODE  = dbutils.widgets.get("cdc_mode").strip() or "watermark"

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
FULL_TABLE   = job["full_table"]
LOAD_TYPE    = job.get("load_type", "full") or "full"
WM_COL       = job.get("watermark_column", "")

# CDC & primary key config from source_config
source_config = json.loads(job.get("source_config", "{}") or "{}")
if not CDC_MODE or CDC_MODE == "watermark":
    CDC_MODE = source_config.get("cdc_mode", CDC_MODE or "watermark")
PRIMARY_KEYS = source_config.get("primary_keys", [])

target_config = json.loads(job.get("target_config", "{}") or "{}")
BRONZE_CATALOG = target_config.get("bronze_catalog", "")
SILVER_CATALOG = target_config.get("silver_catalog", "")
TGT_SCHEMA     = target_config.get("target_schema", "")

# ─── Auto-derive silver_catalog if missing/duplicate ──────────────────────────
# Medallion architecture REQUIRES bronze and silver to be in DIFFERENT catalogs
# (or at minimum different schemas) to avoid mixing layers and storage duplication.
def _derive_silver_catalog(bronze_cat: str) -> str:
    """Derive a silver catalog name from the bronze catalog when none is configured."""
    if not bronze_cat:
        return "silver"
    low = bronze_cat.lower()
    if "bronze" in low:
        return bronze_cat.replace("bronze", "silver").replace("BRONZE", "SILVER").replace("Bronze", "Silver")
    if low.endswith("_brz") or low.endswith("-brz"):
        return bronze_cat[:-3] + ("_slv" if bronze_cat[-4] == "_" else "-slv")
    return "silver"

if not SILVER_CATALOG or SILVER_CATALOG == BRONZE_CATALOG:
    derived = _derive_silver_catalog(BRONZE_CATALOG)
    print(f"⚠️  silver_catalog not set or equals bronze_catalog — auto-deriving: '{derived}'")
    SILVER_CATALOG = derived

# Ensure silver catalog + schema exist (no-op if user lacks permission → caught below)
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{SILVER_CATALOG}`")
    if TGT_SCHEMA:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{SILVER_CATALOG}`.`{TGT_SCHEMA}`")
except Exception as _ce:
    print(f"⚠️  Could not auto-create silver catalog/schema (may need admin): {_ce}")

MULTI_CATALOG  = bool(BRONZE_CATALOG and SILVER_CATALOG and TGT_SCHEMA and BRONZE_CATALOG != SILVER_CATALOG)

if MULTI_CATALOG:
    bronze_table = f"`{BRONZE_CATALOG}`.`{TGT_SCHEMA}`.`{TABLE_NAME}`"
    silver_table = f"`{SILVER_CATALOG}`.`{TGT_SCHEMA}`.`{TABLE_NAME}`"
    DQ_CATALOG   = SILVER_CATALOG
    DQ_SCHEMA    = TGT_SCHEMA
    print(f"✅ Multi-catalog medallion: {BRONZE_CATALOG}.{TGT_SCHEMA} -> {SILVER_CATALOG}.{TGT_SCHEMA} (no prefix)")
else:
    # Fallback: use target_config catalog/schema but NEVER the metadata catalog
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
        print(f"⚠️ WARNING: No silver/bronze_catalog in target_config — falling back to metadata catalog {CATALOG}")
    _fallback_sch = target_config.get("schema", "")
    _meta_sch = target_config.get("metadata_schema", SCHEMA)
    if _fallback_sch and _fallback_sch != _meta_sch and _fallback_sch != SCHEMA:
        TARGET_SCHEMA = _fallback_sch
    elif TGT_SCHEMA:
        TARGET_SCHEMA = TGT_SCHEMA
    else:
        TARGET_SCHEMA = SCHEMA
    bronze_table = f"`{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.`bronze_{TABLE_NAME}`"
    silver_table = f"`{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.`silver_{TABLE_NAME}`"
    DQ_CATALOG   = TARGET_CATALOG
    DQ_SCHEMA    = TARGET_SCHEMA

print(f"Job: {job['job_name']}")
print(f"Bronze: {bronze_table}")
print(f"Silver: {silver_table}")
print(f"CDC Mode: {CDC_MODE}")
print(f"Exec Mode: {EXEC_MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Pipeline Definition (when running as DLT)

# COMMAND ----------

if EXEC_MODE == "dlt":
    import dlt

    # Bronze source table name for DLT reads
    _bronze_dlt_name = f"{TABLE_NAME}" if MULTI_CATALOG else f"bronze_{TABLE_NAME}"

    if CDC_MODE == "change_tracking" and PRIMARY_KEYS:
        # CDC mode: use dlt.apply_changes() for SCD Type 1 merge
        dlt.create_streaming_table(
            name=f"silver_{TABLE_NAME}" if not MULTI_CATALOG else TABLE_NAME,
            comment=f"Silver — CDC merge of {FULL_TABLE} via Change Tracking",
            table_properties={
                "quality": "silver",
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.enableChangeDataFeed": "true",
            },
        )

        dlt.apply_changes(
            target=f"silver_{TABLE_NAME}" if not MULTI_CATALOG else TABLE_NAME,
            source=_bronze_dlt_name,
            keys=PRIMARY_KEYS,
            sequence_by=F.col("__bronze_ts"),
            apply_as_deletes=F.expr("__cdc_operation = 'D'"),
            except_column_list=["__landing_ts", "__load_type", "__is_quarantined",
                                "__cdc_operation", "__cdc_version", "__cdc_mode",
                                "__batch_id", "__bronze_version"],
        )
        print(f"DLT apply_changes defined: {_bronze_dlt_name} -> silver_{TABLE_NAME}")
    else:
        # Standard DLT: read from Bronze with expectations
        @dlt.table(
            name=f"silver_{TABLE_NAME}" if not MULTI_CATALOG else TABLE_NAME,
            comment=f"Silver layer — cleansed & validated {FULL_TABLE}",
            table_properties={
                "quality": "silver",
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true",
                "delta.enableChangeDataFeed": "true",
            },
        )
        @dlt.expect_or_drop("__valid_bronze_ts", "__bronze_ts IS NOT NULL")
        @dlt.expect_or_drop("__not_quarantined", "__is_quarantined = false")
        @dlt.expect("__has_run_id", "__run_id IS NOT NULL")
        def silver_dlt_table():
            bronze_df = dlt.read(_bronze_dlt_name)
            # Drop internal audit columns
            drop_cols = ["__landing_ts", "__load_type", "__is_quarantined",
                         "__cdc_operation", "__cdc_version", "__cdc_mode"]
            for dc in drop_cols:
                if dc in bronze_df.columns:
                    bronze_df = bronze_df.drop(dc)
            # Trim string columns
            for field in bronze_df.schema:
                if field.dataType.simpleString() == "string" and not field.name.startswith("__"):
                    bronze_df = bronze_df.withColumn(field.name, F.trim(F.col(field.name)))
            return (bronze_df
                .dropDuplicates()
                .withColumn("__silver_ts", F.current_timestamp())
                .withColumn("__silver_version", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
                .withColumn("__dq_status", F.lit("passed")))

        print(f"DLT table defined: silver_{TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from Bronze (Standard Mode)

# COMMAND ----------

if EXEC_MODE != "dlt":
    try:
        df = spark.read.table(bronze_table)
        initial_count = df.count()
        print(f"Bronze rows: {initial_count:,}")
    except Exception as e:
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
# MAGIC ## Data Quality Checks

# COMMAND ----------

if EXEC_MODE == "dlt":
    print("DLT mode — Silver quality handled by DLT expectations & apply_changes.")
    dbutils.notebook.exit(json.dumps({"status": "DLT_MODE", "job_id": JOB_ID, "table": FULL_TABLE, "message": "Silver managed by DLT pipeline"}))

# Create restore point
restore_version = None
try:
    history = spark.sql(f"DESCRIBE HISTORY {silver_table} LIMIT 1").collect()
    if history:
        restore_version = history[0]["version"]
        print(f"Restore point: v{restore_version}")
except Exception:
    print("No existing silver table — first load")

# DQ-01: Filter quarantined rows
quarantined_count = df.filter(F.col("__is_quarantined") == True).count()
df_clean = df.filter(F.col("__is_quarantined") == False)
print(f"DQ-01 Quarantine filter: {quarantined_count} quarantined rows excluded")

# DQ-02: Remove all-null rows
audit_cols = [c for c in df_clean.columns if c.startswith("__")]
data_cols  = [c for c in df_clean.columns if c not in audit_cols]

if data_cols:
    null_check = [F.col(c).isNull() for c in data_cols]
    all_null   = null_check[0]
    for nc in null_check[1:]:
        all_null = all_null & nc
    rejected_nulls = df_clean.filter(all_null).count()
    df_clean = df_clean.filter(~all_null)
    print(f"DQ-02 Null-key removal: {rejected_nulls} all-null rows dropped")
else:
    rejected_nulls = 0

# DQ-03: Per-column null percentage check
high_null_cols = []
total_for_null = df_clean.count()
if total_for_null > 0 and data_cols:
    null_counts = df_clean.select(
        *[F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in data_cols]
    ).collect()[0].asDict()
    for col_name, cnt in null_counts.items():
        pct = (cnt / total_for_null) * 100 if cnt else 0
        if pct > 80:
            high_null_cols.append(f"{col_name}({pct:.0f}%)")
    if high_null_cols:
        print(f"DQ-03 High null columns (>80%): {', '.join(high_null_cols)}")

# DQ-04: Deduplication
before_dedup = df_clean.count()
df_clean = df_clean.dropDuplicates(data_cols) if data_cols else df_clean
after_dedup = df_clean.count()
dupes_removed = before_dedup - after_dedup
print(f"DQ-04 Deduplication: {dupes_removed} duplicates removed")

# DQ-05: Trim string columns
string_cols = [f.name for f in df_clean.schema.fields if str(f.dataType) == "StringType"]
for sc in string_cols:
    df_clean = df_clean.withColumn(sc, F.trim(F.col(sc)))
print(f"DQ-05 String trimming: {len(string_cols)} columns normalized")

# DQ-06: Empty string -> NULL normalization
for sc in string_cols:
    df_clean = df_clean.withColumn(sc, F.when(F.col(sc) == "", None).otherwise(F.col(sc)))
print(f"DQ-06 Empty-to-NULL: {len(string_cols)} string columns normalized")

# DQ-07: Row count anomaly detection
row_anomaly = False
try:
    prev = spark.sql(f"SELECT MAX(output_rows) AS prev_rows FROM `{DQ_CATALOG}`.`{DQ_SCHEMA}`.__dq_metrics WHERE table_name = '{FULL_TABLE}' AND layer = 'silver'").collect()[0]["prev_rows"]
    if prev and prev > 0:
        pct_change = abs(after_dedup - prev) / prev * 100
        if pct_change > 50:
            row_anomaly = True
            print(f"DQ-07 Row count anomaly: {pct_change:.0f}% change vs previous ({prev:,} -> {after_dedup:,})")
except Exception:
    print("DQ-07 No previous run — skipping anomaly detection")

# Compute DQ status per row
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

checks_passed = sum([1 for c in [
    quarantined_count == 0,
    rejected_nulls == 0,
    len(high_null_cols) == 0,
    dupes_removed == 0,
    True,
    True,
    not row_anomaly,
] if c])
checks_total = 7
dq_score = round(checks_passed / checks_total * 100, 1)

print(f"\nSilver DQ Summary:")
print(f"   Input:          {initial_count:,}")
print(f"   Output:         {final_count:,}")
print(f"   Rejected:       {total_rejected:,}")
print(f"   DQ Score:       {dq_score}% ({checks_passed}/{checks_total} checks passed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Delta

# COMMAND ----------

try:
    if LOAD_TYPE == "full":
        (df_silver.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(silver_table))
        print(f"Full load -> {silver_table} ({final_count:,} rows)")
    else:
        (df_silver.write
            .format("delta")
            .mode("append")
            .saveAsTable(silver_table))
        print(f"Append -> {silver_table} ({final_count:,} rows)")
except Exception as e:
    if restore_version is not None:
        try:
            spark.sql(f"RESTORE TABLE {silver_table} TO VERSION AS OF {restore_version}")
            print(f"Restored to v{restore_version}")
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

# Save DQ metrics
try:
    dq_table = f"`{DQ_CATALOG}`.`{DQ_SCHEMA}`.__dq_metrics"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {dq_table} (
            run_id STRING, job_id STRING, table_name STRING, layer STRING,
            input_rows BIGINT, output_rows BIGINT, rejected_rows BIGINT,
            null_rows BIGINT, dupe_rows BIGINT, quarantined_rows BIGINT,
            schema_drift BOOLEAN, dq_checks_passed INT, dq_checks_total INT,
            dq_score DOUBLE, checked_at TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        INSERT INTO {dq_table} VALUES (
            '{RUN_ID}', '{JOB_ID}', '{FULL_TABLE}', 'silver',
            {initial_count}, {final_count}, {total_rejected},
            {rejected_nulls}, {dupes_removed}, {quarantined_count},
            false, {checks_passed}, {checks_total},
            {dq_score}, current_timestamp()
        )
    """)
except Exception as e:
    print(f"DQ metrics save failed: {e}")

# Update run history
try:
    spark.sql(f"""
        MERGE INTO {run_tbl} AS t
        USING (SELECT '{RUN_ID}' AS run_id) AS s ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET
            t.status = 'success',
            t.rows_processed = {final_count},
            t.completed_at = current_timestamp(),
            t.duration_sec = unix_timestamp(current_timestamp()) - unix_timestamp(t.started_at)
    """)
except Exception as e:
    print(f"Run history update failed: {e}")

# Update job metadata
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

exit_payload = json.dumps({
    "status": "COMPLETED", "job_id": JOB_ID, "run_id": RUN_ID,
    "table": FULL_TABLE, "rows": final_count,
    "rejected": total_rejected, "silver_table": silver_table,
    "cdc_mode": CDC_MODE, "exec_mode": EXEC_MODE,
})
print(f"\nSILVER COMPLETE — {FULL_TABLE} — {final_count:,} rows ({total_rejected} rejected)")
dbutils.notebook.exit(exit_payload)
