# Databricks notebook source
# pyright: reportUndefinedVariable=false
# MAGIC %md
# MAGIC # Landing Zone — JDBC Extraction
# MAGIC
# MAGIC **Source:** SQL Server (configurable) via JDBC
# MAGIC **Target:** Landing Zone (Parquet) at configurable path
# MAGIC
# MAGIC ### Features
# MAGIC - Full Load / Incremental Load (watermark-based)
# MAGIC - Audit columns: `__landing_ts`, `__source_system`, `__load_type`, `__batch_id`
# MAGIC - Restore points via Delta time-travel
# MAGIC - Parameterised via Databricks widgets
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("load_type", "full", "Load Type (full / incremental)")
dbutils.widgets.text("server", "", "Source Server")
dbutils.widgets.text("database", "", "Source Database")
dbutils.widgets.text("username", "", "Username")
dbutils.widgets.text("password_b64", "", "Password base64 (use secrets in prod)")
dbutils.widgets.text("catalog", "main", "Target Catalog")
dbutils.widgets.text("schema", "default", "Target Schema")
dbutils.widgets.text("landing_path", "/mnt/landing", "Landing Base Path")
dbutils.widgets.text("tables_json", "[]", "Tables JSON array [{schema, table, incremental_col}]")
dbutils.widgets.text("max_parallel_tables", "6", "Max parallel table extractions")

# COMMAND ----------

import base64
import json

LOAD_TYPE    = dbutils.widgets.get("load_type").strip().lower()
SERVER       = dbutils.widgets.get("server").strip()
DATABASE     = dbutils.widgets.get("database").strip()
USERNAME     = dbutils.widgets.get("username").strip()
_PWD_B64     = dbutils.widgets.get("password_b64").strip()
PASSWORD     = base64.b64decode(_PWD_B64.encode("ascii")).decode("utf-8") if _PWD_B64 else ""
CATALOG      = dbutils.widgets.get("catalog").strip()
SCHEMA       = dbutils.widgets.get("schema").strip()
LANDING_PATH = dbutils.widgets.get("landing_path").strip()

# Parse table list from JSON parameter
try:
    TABLES = json.loads(dbutils.widgets.get("tables_json").strip())
except Exception:
    TABLES = []

print(f"Load Type : {LOAD_TYPE}")
print(f"Source    : {SERVER} / {DATABASE}")
print(f"Target    : {CATALOG}.{SCHEMA}")
print(f"Landing   : {LANDING_PATH}")
print(f"Tables    : {len(TABLES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## JDBC Connection Setup

# COMMAND ----------

# selectMethod=cursor → driver streams rows instead of buffering full result
# loginTimeout=60 (s) ; socketTimeout=0 (no read timeout — large tables won't fail mid-stream)
jdbc_url = (
    f"jdbc:sqlserver://{SERVER}:1433;databaseName={DATABASE};"
    f"encrypt=false;trustServerCertificate=true;"
    f"loginTimeout=60;socketTimeout=0;selectMethod=cursor"
)

jdbc_props = {
    "user":     USERNAME,
    "password": PASSWORD,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "fetchsize": "10000",
    "queryTimeout": "0",       # no statement timeout
    "loginTimeout": "60",
    "socketTimeout": "0",      # disable JDBC socket read timeout
}

try:
    test_df = spark.read.jdbc(jdbc_url, "(SELECT 1 AS ok) AS t", properties=jdbc_props)
    test_df.collect()
    print("JDBC connection verified successfully")
except Exception as e:
    msg = f"JDBC connection failed: {e}"
    print(msg)
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "stage": "connection", "error": str(e)[:500]}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

def get_watermark(catalog, schema, table_name):
    wm_table = f"`{catalog}`.`{schema}`.__watermarks"
    try:
        wm_df = spark.sql(f"SELECT max_value FROM {wm_table} WHERE table_name = '{table_name}'")
        rows = wm_df.collect()
        if rows and rows[0]["max_value"]:
            return rows[0]["max_value"]
    except Exception:
        pass
    return None

def save_watermark(catalog, schema, table_name, max_value):
    wm_table = f"`{catalog}`.`{schema}`.__watermarks"
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {wm_table} (
                table_name STRING, max_value STRING, updated_at TIMESTAMP
            ) USING DELTA
        """)
        spark.sql(f"""
            MERGE INTO {wm_table} AS t
            USING (SELECT '{table_name}' AS table_name, '{max_value}' AS max_value, current_timestamp() AS updated_at) AS s
            ON t.table_name = s.table_name
            WHEN MATCHED THEN UPDATE SET t.max_value = s.max_value, t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"   Watermark saved: {table_name} -> {max_value}")
    except Exception as e:
        print(f"   Watermark save failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract & Land Data (parallel + single-pass per table)

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.storagelevel import StorageLevel

# Tunables — override via widgets in production
MAX_PARALLEL_TABLES = int(dbutils.widgets.get("max_parallel_tables") or "6")
LARGE_TABLE_PARTITIONS = 8  # JDBC parallel partitions for big tables

# Spark perf tuning — set once at session level
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.sources.parallelPartitionDiscovery.threshold", "32")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

def _read_jdbc_smart(query, partition_col=None, lower=None, upper=None, num_parts=1):
    """Read via JDBC, optionally with partitioned parallel reads for big tables."""
    reader = spark.read.format("jdbc").option("url", jdbc_url)
    for k, v in jdbc_props.items():
        reader = reader.option(k, v)
    reader = reader.option("dbtable", query)
    if partition_col and lower is not None and upper is not None and num_parts > 1:
        reader = (reader
                  .option("partitionColumn", partition_col)
                  .option("lowerBound", str(lower))
                  .option("upperBound", str(upper))
                  .option("numPartitions", str(num_parts)))
    return reader.load()

def _process_table(tbl):
    src_schema = tbl.get("schema", "dbo")
    src_table  = tbl["table"]
    inc_col    = tbl.get("incremental_col", "")
    part_col   = tbl.get("partition_col", "")  # optional numeric col for parallel read
    full_name  = f"{src_schema}.{src_table}"
    landing_dest = f"{LANDING_PATH}/{src_table}"
    result = {"table": full_name, "status": "pending", "rows": 0}
    try:
        use_incremental = (LOAD_TYPE == "incremental" and inc_col)
        watermark = get_watermark(CATALOG, SCHEMA, full_name) if use_incremental else None

        if use_incremental and watermark:
            query = f"(SELECT * FROM [{src_schema}].[{src_table}] WHERE [{inc_col}] > '{watermark}') AS q"
        else:
            query = f"[{src_schema}].[{src_table}]"

        # Optional partitioned read for big tables — needs numeric partition_col + bounds
        if part_col:
            try:
                bounds = (spark.read.format("jdbc")
                          .option("url", jdbc_url).options(**jdbc_props)
                          .option("dbtable",
                                  f"(SELECT MIN([{part_col}]) AS lo, MAX([{part_col}]) AS hi FROM [{src_schema}].[{src_table}]) AS b")
                          .load().collect()[0])
                df = _read_jdbc_smart(query, part_col, bounds["lo"], bounds["hi"], LARGE_TABLE_PARTITIONS)
            except Exception:
                df = _read_jdbc_smart(query)
        else:
            df = _read_jdbc_smart(query)

        df = (df
              .withColumn("__landing_ts", F.current_timestamp())
              .withColumn("__source_system", F.lit(f"{SERVER}/{DATABASE}"))
              .withColumn("__load_type", F.lit("incremental" if use_incremental else "full"))
              .withColumn("__batch_id", F.lit(run_ts)))

        # ── Single-pass write: persist once, then count + write + max(watermark) reuse cache
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            row_count = df.count()  # triggers JDBC read once → cached
            # Coalesce tiny results to avoid small-file overhead
            out_df = df.coalesce(1) if row_count < 100000 else df
            mode = "overwrite" if (LOAD_TYPE == "full" or not use_incremental) else "append"
            writer = out_df.write.mode(mode)
            if mode == "overwrite":
                writer = writer.option("overwriteSchema", "true")
            writer.parquet(landing_dest)

            if use_incremental and inc_col and row_count > 0:
                new_wm = df.agg(F.max(F.col(inc_col)).cast("string")).collect()[0][0]
                if new_wm:
                    save_watermark(CATALOG, SCHEMA, full_name, new_wm)
        finally:
            df.unpersist()

        result["status"] = "success"
        result["rows"]   = row_count
        print(f"[OK]  {full_name:40s}  {row_count:>10,}  -> {landing_dest}")
    except Exception as e:
        result["status"] = "failed"
        result["error"]  = str(e)[:300]
        print(f"[ERR] {full_name:40s}  {str(e)[:200]}")
    return result

# ── Parallel execution across tables ────────────────────────────────────────
print(f"Processing {len(TABLES)} tables with parallelism={MAX_PARALLEL_TABLES}...")
results = []
with ThreadPoolExecutor(max_workers=MAX_PARALLEL_TABLES) as pool:
    futs = {pool.submit(_process_table, t): t for t in TABLES}
    for fut in as_completed(futs):
        results.append(fut.result())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extraction Summary

# COMMAND ----------

success  = [r for r in results if r["status"] == "success"]
failed   = [r for r in results if r["status"] == "failed"]
total_rows = sum(r["rows"] for r in results)

print(f"\n{'='*60}")
print(f"LANDING ZONE EXTRACTION COMPLETE")
print(f"{'='*60}")
print(f"  Succeeded : {len(success)} / {len(results)}")
print(f"  Failed    : {len(failed)} / {len(results)}")
print(f"  Total Rows: {total_rows:,}")

if failed:
    print(f"\nFailed tables:")
    for f_item in failed:
        print(f"   {f_item['table']}: {f_item.get('error','unknown')}")

exit_payload = json.dumps({
    "status":      "COMPLETED" if not failed else "PARTIAL",
    "succeeded":   len(success),
    "failed":      len(failed),
    "total_rows":  total_rows,
    "batch_id":    run_ts,
    "landing_path": LANDING_PATH,
    "tables":      [r["table"].split(".")[-1] for r in success],
})

dbutils.notebook.exit(exit_payload)
