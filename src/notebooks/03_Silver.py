# Databricks notebook source
# pyright: reportUndefinedVariable=false
# MAGIC %md
# MAGIC # Silver Layer — Bronze to Cleansed Delta
# MAGIC
# MAGIC **Source:** Bronze Delta Tables
# MAGIC **Target:** Silver Delta Tables in Unity Catalog
# MAGIC
# MAGIC ### Data Quality Checks
# MAGIC | # | Check | Action |
# MAGIC |---|-------|--------|
# MAGIC | 1 | Null bronze timestamps | Drop rows |
# MAGIC | 2 | Quarantined rows | Drop rows |
# MAGIC | 3 | Duplicate detection | Deduplicate |
# MAGIC | 4 | Entirely-null rows | Drop rows |
# MAGIC | 5 | String trimming | Trim whitespace |
# MAGIC | 6 | Completeness audit | Record metrics |
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Silver Catalog")
dbutils.widgets.text("schema", "default", "Silver Schema")
dbutils.widgets.text("bronze_catalog", "", "Bronze Catalog")
dbutils.widgets.text("bronze_schema", "", "Bronze Schema")
dbutils.widgets.text("mode", "standard", "Execution Mode (dlt / standard)")
dbutils.widgets.text("tables_json", "[]", "Tables JSON array (table names)")

import json

CATALOG        = dbutils.widgets.get("catalog").strip()
SCHEMA         = dbutils.widgets.get("schema").strip()
BRONZE_CATALOG = dbutils.widgets.get("bronze_catalog").strip() or CATALOG
BRONZE_SCHEMA  = dbutils.widgets.get("bronze_schema").strip() or SCHEMA
EXEC_MODE      = dbutils.widgets.get("mode").strip().lower()

try:
    TABLE_NAMES = json.loads(dbutils.widgets.get("tables_json").strip())
except Exception:
    TABLE_NAMES = []

TABLE_PREFIX = "silver_"
BRONZE_TABLE_PREFIX = "bronze_"

# Auto-discover tables from Bronze catalog if TABLE_NAMES is empty
if not TABLE_NAMES:
    try:
        bronze_tables_df = spark.sql(f"SHOW TABLES IN `{BRONZE_CATALOG}`.`{BRONZE_SCHEMA}`")
        bronze_tables = [row.tableName for row in bronze_tables_df.collect()]
        TABLE_NAMES = [
            t.replace(BRONZE_TABLE_PREFIX, "", 1) if t.startswith(BRONZE_TABLE_PREFIX) else t
            for t in bronze_tables
            if not t.startswith("__")
        ]
        if TABLE_NAMES:
            print(f"Auto-discovered {len(TABLE_NAMES)} tables from Bronze catalog: {TABLE_NAMES}")
    except Exception as e:
        print(f"Auto-discovery from Bronze catalog failed: {e}")

print(f"Silver Catalog  : {CATALOG}.{SCHEMA}")
print(f"Bronze Catalog  : {BRONZE_CATALOG}.{BRONZE_SCHEMA}")
print(f"Mode            : {EXEC_MODE}")
print(f"Tables          : {len(TABLE_NAMES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

def create_restore_point(catalog, schema, table_name, stage):
    try:
        delta_tbl = f"`{catalog}`.`{schema}`.`{table_name}`"
        history = spark.sql(f"DESCRIBE HISTORY {delta_tbl} LIMIT 1").collect()
        if history:
            version = history[0]["version"]
            print(f"   Restore point: {table_name} v{version} ({stage})")
            return version
    except Exception:
        pass
    return None

def restore_table(catalog, schema, table_name, version):
    try:
        delta_tbl = f"`{catalog}`.`{schema}`.`{table_name}`"
        spark.sql(f"RESTORE TABLE {delta_tbl} TO VERSION AS OF {version}")
        print(f"   Restored {table_name} to version {version}")
        return True
    except Exception as e:
        print(f"   Restore failed: {e}")
        return False

def save_dq_metrics(catalog, schema, table_name, dq_results, before, after):
    metrics_tbl = f"`{catalog}`.`{schema}`.__dq_metrics"
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {metrics_tbl} (
                table_name STRING, check_time TIMESTAMP,
                rows_before BIGINT, rows_after BIGINT, rows_dropped BIGINT,
                null_ts BIGINT, quarantined BIGINT, duplicates BIGINT,
                empty_rows BIGINT, dq_pass BOOLEAN
            ) USING DELTA
        """)
        rows_dropped = before - after
        spark.sql(f"""
            INSERT INTO {metrics_tbl} VALUES (
                '{table_name}', current_timestamp(),
                {before}, {after}, {rows_dropped},
                {dq_results.get("null_bronze_ts", 0)},
                {dq_results.get("quarantined", 0)},
                {dq_results.get("duplicates_removed", 0)},
                {dq_results.get("empty_rows", 0)},
                {str(rows_dropped < before * 0.5).lower()}
            )
        """)
        print(f"   DQ metrics saved for {table_name}")
    except Exception as e:
        print(f"   DQ metrics save failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Processing Functions

# COMMAND ----------

def process_silver_table(tname):
    """Standard mode: Bronze -> Silver with quality checks & restore."""
    safe = tname.replace(" ", "_").replace("-", "_").lower()
    delta_name = f"{TABLE_PREFIX}{safe}"
    bronze_delta_name = f"{BRONZE_TABLE_PREFIX}{safe}"
    bronze_tbl = f"`{BRONZE_CATALOG}`.`{BRONZE_SCHEMA}`.`{bronze_delta_name}`"
    silver_tbl = f"`{CATALOG}`.`{SCHEMA}`.`{delta_name}`"

    print(f"  Processing: {bronze_delta_name} -> {delta_name}")
    restore_ver = create_restore_point(CATALOG, SCHEMA, delta_name, "pre-silver")

    try:
        df = spark.sql(f"SELECT * FROM {bronze_tbl}")
        initial_count = df.count()
        dq_results = {}

        # DQ-1: Null bronze timestamps
        null_ts = df.filter(F.col("__bronze_ts").isNull()).count()
        dq_results["null_bronze_ts"] = null_ts
        if null_ts > 0:
            df = df.filter(F.col("__bronze_ts").isNotNull())

        # DQ-2: Quarantined rows
        quarantined = 0
        if "__is_quarantined" in df.columns:
            quarantined = df.filter(F.col("__is_quarantined") == True).count()
            dq_results["quarantined"] = quarantined
            if quarantined > 0:
                df = df.filter(F.col("__is_quarantined") == False)

        # DQ-3: Deduplication
        before_dedup = df.count()
        df = df.dropDuplicates()
        dup_count = before_dedup - df.count()
        dq_results["duplicates_removed"] = dup_count

        # DQ-4: Entirely-null rows
        all_cols = [c for c in df.columns if not c.startswith("__")]
        if all_cols:
            null_expr = F.lit(True)
            for c in all_cols:
                null_expr = null_expr & F.col(c).isNull()
            empty_rows = df.filter(null_expr).count()
            dq_results["empty_rows"] = empty_rows
            if empty_rows > 0:
                df = df.filter(~null_expr)

        # Trim string columns
        for c in df.dtypes:
            if c[1] == "string" and not c[0].startswith("__"):
                df = df.withColumn(c[0], F.trim(F.col(c[0])))

        # Drop downstream-irrelevant audit columns
        for dc in ["__landing_ts", "__load_type", "__is_quarantined"]:
            if dc in df.columns:
                df = df.drop(dc)

        # Add silver audit columns
        df = (df
              .withColumn("__silver_ts", F.current_timestamp())
              .withColumn("__silver_version", F.lit(1))
              .withColumn("__dq_status", F.lit("passed")))

        final_count = df.count()

        (df.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .option("delta.autoOptimize.optimizeWrite", "true")
         .saveAsTable(silver_tbl))

        print(f"    Silver {silver_tbl}: {final_count:,} rows written")
        save_dq_metrics(CATALOG, SCHEMA, delta_name, dq_results, initial_count, final_count)

        return {
            "table": tname, "status": "success",
            "bronze_rows": initial_count, "silver_rows": final_count,
            "dq": dq_results
        }

    except Exception as e:
        print(f"    FAILED: {e}")
        if restore_ver is not None:
            restore_table(CATALOG, SCHEMA, delta_name, restore_ver)
        return {"table": tname, "status": "failed", "error": str(e)}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Silver Pipeline

# COMMAND ----------

if EXEC_MODE != "dlt":
    print("\n" + "="*60)
    print("SILVER LAYER PIPELINE — Standard Mode")
    print("="*60)

    silver_results = []
    for tname in TABLE_NAMES:
        res = process_silver_table(tname)
        silver_results.append(res)

    success = [r for r in silver_results if r.get("status") == "success"]
    failed  = [r for r in silver_results if r.get("status") == "failed"]
    total_bronze = sum(r.get("bronze_rows", 0) for r in silver_results)
    total_silver = sum(r.get("silver_rows", 0) for r in silver_results)

    print(f"\n{'='*60}")
    print(f"SILVER LAYER COMPLETE")
    print(f"{'='*60}")
    print(f"  Succeeded : {len(success)} / {len(silver_results)}")
    print(f"  Failed    : {len(failed)} / {len(silver_results)}")
    print(f"  Bronze In : {total_bronze:,}")
    print(f"  Silver Out: {total_silver:,}")

    exit_payload = json.dumps({
        "status": "COMPLETED" if not failed else "PARTIAL",
        "succeeded": len(success),
        "failed": len(failed),
        "layer": "silver",
        "total_bronze": total_bronze,
        "total_silver": total_silver,
    })
    dbutils.notebook.exit(exit_payload)
else:
    print("Running in DLT mode — tables are managed by Delta Live Tables engine.")
