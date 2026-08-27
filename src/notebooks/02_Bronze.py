# Databricks notebook source
# pyright: reportUndefinedVariable=false
# MAGIC %md
# MAGIC # Bronze Layer — Landing to Delta
# MAGIC
# MAGIC **Source:** Landing Zone (Parquet)
# MAGIC **Target:** Bronze Delta Tables in Unity Catalog
# MAGIC
# MAGIC ### Responsibilities
# MAGIC - Raw data ingestion from Landing Zone (Parquet to Delta)
# MAGIC - Schema enforcement & type preservation
# MAGIC - Data quality checks (null landing_ts, duplicates)
# MAGIC - Audit columns: `__bronze_ts`, `__bronze_version`, `__is_quarantined`
# MAGIC - Restore points for rollback on failure
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Target Catalog")
dbutils.widgets.text("schema", "default", "Target Schema")
dbutils.widgets.text("landing_path", "/mnt/landing", "Landing Base Path")
dbutils.widgets.text("mode", "standard", "Execution Mode (dlt / standard)")
dbutils.widgets.text("tables_json", "[]", "Tables JSON array (table names)")

import json

CATALOG      = dbutils.widgets.get("catalog").strip()
SCHEMA       = dbutils.widgets.get("schema").strip()
LANDING_PATH = dbutils.widgets.get("landing_path").strip()
EXEC_MODE    = dbutils.widgets.get("mode").strip().lower()

try:
    TABLE_NAMES = json.loads(dbutils.widgets.get("tables_json").strip())
except Exception:
    TABLE_NAMES = []

TABLE_PREFIX = "bronze_"

# Auto-discover tables from landing zone if TABLE_NAMES is empty
if not TABLE_NAMES:
    try:
        landing_entries = dbutils.fs.ls(LANDING_PATH)
        TABLE_NAMES = [
            entry.name.rstrip("/")
            for entry in landing_entries
            if entry.isDir() and not entry.name.startswith("_") and not entry.name.startswith(".")
        ]
        if TABLE_NAMES:
            print(f"Auto-discovered {len(TABLE_NAMES)} tables from landing zone: {TABLE_NAMES}")
    except Exception as e:
        print(f"Auto-discovery from landing zone failed: {e}")

print(f"Catalog      : {CATALOG}")
print(f"Schema       : {SCHEMA}")
print(f"Landing Path : {LANDING_PATH}")
print(f"Mode         : {EXEC_MODE}")
print(f"Tables       : {len(TABLE_NAMES)}")

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

def validate_bronze_table(catalog, schema, table_name):
    try:
        tbl = f"`{catalog}`.`{schema}`.`{table_name}`"
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {tbl}").collect()[0]["cnt"]
        nulls = spark.sql(f"SELECT COUNT(*) AS cnt FROM {tbl} WHERE __bronze_ts IS NULL").collect()[0]["cnt"]
        print(f"   Validation {table_name}: {count:,} rows, {nulls} null bronze_ts")
        return {"rows": count, "null_audit": nulls, "valid": nulls == 0}
    except Exception as e:
        print(f"   Validation failed: {e}")
        return {"rows": 0, "null_audit": -1, "valid": False}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Load Functions

# COMMAND ----------

def load_bronze_table(tname):
    """Load a single table from landing into Bronze Delta with quality checks."""
    safe = tname.replace(" ", "_").replace("-", "_").lower()
    delta_name = f"{TABLE_PREFIX}{safe}"
    src_path = f"{LANDING_PATH}/{tname}"
    target   = f"`{CATALOG}`.`{SCHEMA}`.`{delta_name}`"

    print(f"  Loading {src_path} -> {target}")

    restore_ver = create_restore_point(CATALOG, SCHEMA, delta_name, "pre-bronze-load")

    try:
        df = spark.read.parquet(src_path)
        total = df.count()
        nulls = df.filter(F.col("__landing_ts").isNull()).count()
        dup_count = total - df.dropDuplicates().count()

        print(f"    Rows: {total:,} | Null landing_ts: {nulls} | Duplicates: {dup_count}")

        if nulls > 0:
            df = df.filter(F.col("__landing_ts").isNotNull())
            print(f"    Dropped {nulls} rows with null __landing_ts")

        df = (df
              .withColumn("__bronze_ts", F.current_timestamp())
              .withColumn("__bronze_version", F.lit(1))
              .withColumn("__is_quarantined", F.lit(False)))

        (df.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .option("delta.autoOptimize.optimizeWrite", "true")
         .saveAsTable(target))

        print(f"    Bronze {target}: {df.count():,} rows written")
        return {"table": tname, "status": "success", "rows": df.count()}

    except Exception as e:
        print(f"    FAILED: {e}")
        if restore_ver is not None:
            restore_table(CATALOG, SCHEMA, delta_name, restore_ver)
        return {"table": tname, "status": "failed", "error": str(e)}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Bronze Pipeline

# COMMAND ----------

if EXEC_MODE != "dlt":
    print("\n" + "="*60)
    print("BRONZE LAYER PIPELINE — Standard Mode")
    print("="*60)

    bronze_results = []
    for tname in TABLE_NAMES:
        result = load_bronze_table(tname)
        bronze_results.append(result)

    # Post-load validation
    print("\n" + "-"*40)
    print("Post-Load Validation")
    print("-"*40)
    for tname in TABLE_NAMES:
        safe = tname.replace(" ", "_").replace("-", "_").lower()
        validate_bronze_table(CATALOG, SCHEMA, f"{TABLE_PREFIX}{safe}")

    success = [r for r in bronze_results if r.get("status") == "success"]
    failed  = [r for r in bronze_results if r.get("status") == "failed"]
    print(f"\n{'='*60}")
    print(f"BRONZE LAYER COMPLETE")
    print(f"{'='*60}")
    print(f"  Succeeded: {len(success)} / {len(bronze_results)}")
    print(f"  Failed   : {len(failed)} / {len(bronze_results)}")

    exit_payload = json.dumps({
        "status": "COMPLETED" if not failed else "PARTIAL",
        "succeeded": len(success),
        "failed": len(failed),
        "layer": "bronze",
    })
    dbutils.notebook.exit(exit_payload)
else:
    print("Running in DLT mode — tables are managed by Delta Live Tables engine.")
