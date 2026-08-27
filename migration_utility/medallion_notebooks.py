"""
Medallion Architecture Notebook Generator
==========================================
Generates 3 Databricks notebooks for a professional Medallion pipeline:
  1. 01_Landing_Zone.py  — Extract from source DB → land raw data (Parquet/Delta)
  2. 02_Bronze.py        — Raw ingestion with DLT, schema enforcement, audit columns
  3. 03_Silver.py        — Cleansed/validated layer with DLT, data quality, restore points

Supports:
  • Full Load  — truncate + reload
  • Incremental Load — watermark-based CDC via configurable timestamp column
  • DLT (Lakeflow Spark Declarative Pipelines) pipelines
  • Data quality checks & expectations
  • Restore points / time-travel rollback on failure
  • Dynamic source selection (SQL Server, Azure SQL, Synapse, SQL MI)
"""

from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
#  Helper: JDBC URL builder for Databricks
# ─────────────────────────────────────────────────────────────────────────────
def _jdbc_snippet(source_type: str) -> str:
    """Return a JDBC URL template string for the chosen source type."""
    if source_type == "snowflake":
        # Snowflake JDBC: account is passed as 'server' placeholder
        return '"jdbc:snowflake://{server}.snowflakecomputing.com/?db={database}"'
    encrypt = "true" if source_type in ("azuresql", "synapse") else "false"
    trust   = "false" if source_type in ("azuresql", "synapse") else "true"
    port    = "1433"
    return (
        f'"jdbc:sqlserver://{{server}}:{port};'
        f'databaseName={{database}};'
        f'encrypt={encrypt};trustServerCertificate={trust}"'
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  1. LANDING ZONE NOTEBOOK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def generate_landing_zone(
    source_type: str,
    server: str,
    database: str,
    username: str,
    tables: list,       # [{schema, table, incremental_col?}]
    catalog: str = "main",
    schema: str = "default",
    landing_path: str = "/mnt/landing",
    account: str = "",
    warehouse: str = "",
    role: str = "",
) -> str:
    """Generate the Landing Zone extraction notebook."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    jdbc_url = _jdbc_snippet(source_type)
    is_snowflake = (source_type == "snowflake")
    # For Snowflake, 'server' is the account identifier
    _effective_server = account or server
    _effective_wh = warehouse
    _effective_role = role
    table_list_str = ",\n    ".join(
        '{"schema": "' + t.get("schema", "dbo") + '", '
        '"table": "' + t["table"] + '", '
        '"incremental_col": "' + t.get("incremental_col", "") + '"}'
        for t in tables
    )

    # Build JDBC configuration block depending on source type
    # NOTE: These use plain strings (not f-strings) since they'll be injected
    # into the outer f-string template via {_jdbc_config_block}
    if is_snowflake:
        _jdbc_config_block = (
            '# \u2500\u2500 Snowflake JDBC configuration \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n'
            '_WAREHOUSE = dbutils.widgets.get("warehouse").strip() if "warehouse" in [w.name for w in dbutils.widgets.getAll()] else ""\n'
            '_ROLE = dbutils.widgets.get("role").strip() if "role" in [w.name for w in dbutils.widgets.getAll()] else ""\n'
            'jdbc_url = f"jdbc:snowflake://{SERVER}.snowflakecomputing.com/?db={DATABASE}"\n'
            'if _WAREHOUSE:\n'
            '    jdbc_url += f"&warehouse={_WAREHOUSE}"\n'
            'if _ROLE:\n'
            '    jdbc_url += f"&role={_ROLE}"\n'
            '\n'
            'jdbc_props = {\n'
            '    "user":     USERNAME,\n'
            '    "password": PASSWORD,\n'
            '    "driver":   "net.snowflake.client.jdbc.SnowflakeDriver",\n'
            '    "fetchsize": "10000",\n'
            '}\n'
            '\n'
            '# \u2500\u2500 Verify Snowflake JDBC connectivity \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n'
            'try:\n'
            '    test_df = spark.read.jdbc(jdbc_url, "(SELECT 1 AS ok) AS t", properties=jdbc_props)\n'
            '    test_df.collect()\n'
            '    print("\u2705 Snowflake JDBC connection verified successfully")\n'
            'except Exception as e:\n'
            '    msg = f"\u274c Snowflake JDBC connection failed: {e}"\n'
            '    print(msg)\n'
            '    dbutils.notebook.exit(\'{"status": "FAILED", "stage": "connection", "error": "\' + str(e).replace(\'"\', "\'") + \'"}\')'
        )
    else:
        _jdbc_config_block = (
            '# \u2500\u2500 JDBC configuration \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n'
            '# selectMethod=cursor streams rows ; socketTimeout=0 disables read timeout for big tables\n'
            f'base_jdbc = {jdbc_url}.format(server=SERVER, database=DATABASE)\n'
            'if "loginTimeout=" not in base_jdbc:\n'
            '    base_jdbc = base_jdbc.rstrip(";") + ";loginTimeout=60;socketTimeout=0;selectMethod=cursor"\n'
            'jdbc_url = base_jdbc\n'
            '\n'
            'jdbc_props = {\n'
            '    "user":     USERNAME,\n'
            '    "password": PASSWORD,\n'
            '    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",\n'
            '    "fetchsize": "10000",\n'
            '    "queryTimeout": "0",\n'
            '    "loginTimeout": "60",\n'
            '    "socketTimeout": "0",\n'
            '}\n'
            '\n'
            '# \u2500\u2500 Verify JDBC connectivity \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n'
            'try:\n'
            '    test_df = spark.read.jdbc(jdbc_url, "(SELECT 1 AS ok) AS t", properties=jdbc_props)\n'
            '    test_df.collect()\n'
            '    print("\u2705 JDBC connection verified successfully")\n'
            'except Exception as e:\n'
            '    msg = f"\u274c JDBC connection failed: {e}"\n'
            '    print(msg)\n'
            '    dbutils.notebook.exit(\'{"status": "FAILED", "stage": "connection", "error": "\' + str(e).replace(\'"\', "\'") + \'"}\')'
        )


    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 🏭 Medallion Architecture — Landing Zone
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC **Source:** {source_type.upper()} → `{server}` / `{database}`
# MAGIC **Target:** `{catalog}.{schema}` Landing Zone at `{landing_path}`
# MAGIC
# MAGIC ### Pipeline Flow
# MAGIC ```
# MAGIC ┌─────────────┐     ┌──────────────────┐     ┌───────────────┐
# MAGIC │  SQL Source  │ ──► │  Landing Zone     │ ──► │  Bronze Layer │
# MAGIC │  (JDBC)     │     │  (Raw Parquet)    │     │  (DLT Delta)  │
# MAGIC └─────────────┘     └──────────────────────┘     └───────────────┘
# MAGIC ```
# MAGIC ---
# MAGIC **Features:** Full Load · Incremental Load · Restore Points · Parallel Extraction

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Configuration

# COMMAND ----------

# ── Widgets for parameterized runs ────────────────────────────────────────────
dbutils.widgets.text("load_type", "full", "Load Type (full / incremental)")
dbutils.widgets.text("server", "{_effective_server}", "Source {'Account' if is_snowflake else 'Server'}")
dbutils.widgets.text("database", "{database}", "Source Database")
dbutils.widgets.text("username", "{username}", "Username")
dbutils.widgets.text("password_b64", "", "Password base64 (use secrets in prod)")
dbutils.widgets.text("catalog", "{catalog}", "Target Catalog")
dbutils.widgets.text("schema", "{schema}", "Target Schema")
dbutils.widgets.text("landing_path", "{landing_path}", "Landing Base Path")
dbutils.widgets.text("max_parallel_tables", "6", "Max parallel table extractions")
{'dbutils.widgets.text("warehouse", "' + _effective_wh + '", "Snowflake Warehouse")' if is_snowflake else ''}
{'dbutils.widgets.text("role", "' + _effective_role + '", "Snowflake Role")' if is_snowflake else ''}

# COMMAND ----------

import base64

# ── Read widget values ────────────────────────────────────────────────────────
LOAD_TYPE    = dbutils.widgets.get("load_type").strip().lower()
SERVER       = dbutils.widgets.get("server").strip()
DATABASE     = dbutils.widgets.get("database").strip()
USERNAME     = dbutils.widgets.get("username").strip()
_PWD_B64     = dbutils.widgets.get("password_b64").strip()
# Decode base64 password — special chars like # ; {{ }} are safe this way
PASSWORD     = base64.b64decode(_PWD_B64.encode("ascii")).decode("utf-8") if _PWD_B64 else ""
CATALOG      = dbutils.widgets.get("catalog").strip()
SCHEMA       = dbutils.widgets.get("schema").strip()
LANDING_PATH = dbutils.widgets.get("landing_path").strip()
SOURCE_TYPE  = "{'snowflake' if is_snowflake else source_type}"

print(f"🔧 Load Type : {{LOAD_TYPE}}")
print(f"🔧 Source    : {{SERVER}} / {{DATABASE}} ({{SOURCE_TYPE}})")
print(f"🔧 Target    : {{CATALOG}}.{{SCHEMA}}")
print(f"🔧 Landing   : {{LANDING_PATH}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔌 JDBC Connection Setup

# COMMAND ----------

{_jdbc_config_block}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📑 Table Configuration

# COMMAND ----------

# ── Tables to extract ─────────────────────────────────────────────────────────
TABLES = [
    {table_list_str}
]

print(f"📋 Tables to extract: {{len(TABLES)}}")
for t in TABLES:
    mode = "INCREMENTAL" if t["incremental_col"] else "FULL"
    print(f"   • {{t['schema']}}.{{t['table']}}  ({{mode}})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ Utility Functions

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import datetime
import json

def get_watermark(catalog, schema, table_name):
    """Retrieve the last successful watermark for incremental loads."""
    wm_table = f"`{{catalog}}`.`{{schema}}`.__watermarks"
    try:
        wm_df = spark.sql(f"SELECT max_value FROM {{wm_table}} WHERE table_name = '{{table_name}}'")
        rows = wm_df.collect()
        if rows and rows[0]["max_value"]:
            return rows[0]["max_value"]
    except Exception:
        # Watermark table doesn't exist yet — will be created after first run
        pass
    return None

def save_watermark(catalog, schema, table_name, max_value):
    """Persist watermark after successful extraction."""
    wm_table = f"`{{catalog}}`.`{{schema}}`.__watermarks"
    try:
        spark.sql(f\"\"\"
            CREATE TABLE IF NOT EXISTS {{wm_table}} (
                table_name STRING,
                max_value  STRING,
                updated_at TIMESTAMP
            ) USING DELTA
        \"\"\")
        spark.sql(f\"\"\"
            MERGE INTO {{wm_table}} AS t
            USING (SELECT '{{table_name}}' AS table_name, '{{max_value}}' AS max_value, current_timestamp() AS updated_at) AS s
            ON t.table_name = s.table_name
            WHEN MATCHED THEN UPDATE SET t.max_value = s.max_value, t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT *
        \"\"\")
        print(f"   💾 Watermark saved: {{table_name}} → {{max_value}}")
    except Exception as e:
        print(f"   ⚠️ Watermark save failed: {{e}}")

def create_restore_point(catalog, schema, table_name, stage):
    """Create a Delta time-travel restore point by recording the version."""
    try:
        delta_tbl = f"`{{catalog}}`.`{{schema}}`.`{{table_name}}`"
        history = spark.sql(f"DESCRIBE HISTORY {{delta_tbl}} LIMIT 1").collect()
        if history:
            version = history[0]["version"]
            print(f"   📌 Restore point: {{table_name}} v{{version}} ({{stage}})")
            return version
    except Exception:
        pass
    return None

def restore_table(catalog, schema, table_name, version):
    """Restore a Delta table to a previous version on failure."""
    try:
        delta_tbl = f"`{{catalog}}`.`{{schema}}`.`{{table_name}}`"
        spark.sql(f"RESTORE TABLE {{delta_tbl}} TO VERSION AS OF {{version}}")
        print(f"   🔄 Restored {{table_name}} to version {{version}}")
        return True
    except Exception as e:
        print(f"   ❌ Restore failed: {{e}}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Extract & Land Data

# COMMAND ----------

# ── Optimized parallel extraction ─────────────────────────────────────────────
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.storagelevel import StorageLevel

MAX_PARALLEL_TABLES = int(dbutils.widgets.get("max_parallel_tables") if "max_parallel_tables" in [w.name for w in dbutils.widgets.getAll()] else "6")
LARGE_TABLE_PARTITIONS = 8

# Spark perf tuning
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

results = []
run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

def _read_jdbc_smart(query, partition_col=None, lower=None, upper=None, num_parts=1):
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
    src_schema = tbl["schema"]
    src_table  = tbl["table"]
    inc_col    = tbl.get("incremental_col", "")
    part_col   = tbl.get("partition_col", "")
    full_name  = f"{{src_schema}}.{{src_table}}"
    landing_dest = f"{{LANDING_PATH}}/{{src_table}}"
    result = {{"table": full_name, "status": "pending", "rows": 0}}
    try:
        use_incremental = (LOAD_TYPE == "incremental" and inc_col)
        watermark = get_watermark(CATALOG, SCHEMA, full_name) if use_incremental else None
        if SOURCE_TYPE == "snowflake":
            _q = lambda s, t: f'"{{s}}"."{{t}}"'
        else:
            _q = lambda s, t: f'[{{s}}].[{{t}}]'
        if use_incremental and watermark:
            tbl_ref = _q(src_schema, src_table)
            if SOURCE_TYPE == "snowflake":
                query = f"(SELECT * FROM {{tbl_ref}} WHERE \"{{inc_col}}\" > '{{watermark}}') AS q"
            else:
                query = f"(SELECT * FROM {{tbl_ref}} WHERE [{{inc_col}}] > '{{watermark}}') AS q"
        else:
            query = _q(src_schema, src_table)

        if part_col:
            try:
                _col_q = f'"{{part_col}}"' if SOURCE_TYPE == "snowflake" else f'[{{part_col}}]'
                _tbl_q = _q(src_schema, src_table)
                bounds = (spark.read.format("jdbc").option("url", jdbc_url).options(**jdbc_props)
                          .option("dbtable",
                                  f"(SELECT MIN({{_col_q}}) AS lo, MAX({{_col_q}}) AS hi FROM {{_tbl_q}}) AS b")
                          .load().collect()[0])
                df = _read_jdbc_smart(query, part_col, bounds["lo"], bounds["hi"], LARGE_TABLE_PARTITIONS)
            except Exception:
                df = _read_jdbc_smart(query)
        else:
            df = _read_jdbc_smart(query)

        df = (df
              .withColumn("__landing_ts", F.current_timestamp())
              .withColumn("__source_system", F.lit(f"{{SERVER}}/{{DATABASE}}"))
              .withColumn("__load_type", F.lit("incremental" if use_incremental else "full"))
              .withColumn("__batch_id", F.lit(run_ts)))

        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            row_count = df.count()
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
        print(f"[OK]  {{full_name:40s}}  {{row_count:>10,}}  -> {{landing_dest}}")
    except Exception as e:
        result["status"] = "failed"
        result["error"]  = str(e)[:300]
        print(f"[ERR] {{full_name:40s}}  {{str(e)[:200]}}")
    return result

print(f"🚀 Processing {{len(TABLES)}} tables with parallelism={{MAX_PARALLEL_TABLES}}...")
with ThreadPoolExecutor(max_workers=MAX_PARALLEL_TABLES) as pool:
    futs = {{pool.submit(_process_table, t): t for t in TABLES}}
    for fut in as_completed(futs):
        results.append(fut.result())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Extraction Summary

# COMMAND ----------

# ── Print summary ─────────────────────────────────────────────────────────────
success  = [r for r in results if r["status"] == "success"]
failed   = [r for r in results if r["status"] == "failed"]
total_rows = sum(r["rows"] for r in results)

print(f"\\n{'='*60}")
print(f"📊 LANDING ZONE EXTRACTION COMPLETE")
print(f"{'='*60}")
print(f"  ✅ Succeeded : {{len(success)}} / {{len(results)}}")
print(f"  ❌ Failed    : {{len(failed)}} / {{len(results)}}")
print(f"  📊 Total Rows: {{total_rows:,}}")

if failed:
    print(f"\\n⚠️ Failed tables:")
    for f_item in failed:
        print(f"   • {{f_item['table']}}: {{f_item.get('error','unknown')}}")

# ── Exit with structured result ───────────────────────────────────────────────
exit_payload = json.dumps({{
    "status":      "COMPLETED" if not failed else "PARTIAL",
    "succeeded":   len(success),
    "failed":      len(failed),
    "total_rows":  total_rows,
    "batch_id":    run_ts,
    "landing_path": LANDING_PATH,
}})

dbutils.notebook.exit(exit_payload)
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  2. BRONZE NOTEBOOK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def generate_bronze(
    tables: list,
    catalog: str = "main",
    schema: str = "default",
    landing_path: str = "/mnt/landing",
    table_prefix: str = "bronze_",
    cdc_mode: str = "watermark",
) -> str:
    """Generate the Bronze layer DLT notebook."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Build DLT table definitions for each table
    dlt_defs = []
    raw_defs = []
    for t in tables:
        tname = t["table"]
        safe  = tname.replace(" ", "_").replace("-", "_").lower()
        delta_name = f"{table_prefix}{safe}"
        dlt_defs.append(f'''
@dlt.table(
    name="{delta_name}",
    comment="Bronze layer — raw ingestion of {tname} with schema enforcement",
    table_properties={{
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true",
    }},
)
@dlt.expect_or_drop("__valid_landing_ts", "__landing_ts IS NOT NULL")
@dlt.expect("__has_batch_id", "__batch_id IS NOT NULL")
def bronze_{safe}():
    """Ingest raw {tname} from landing zone into Bronze Delta table."""
    return (
        spark.read.parquet(f"{{LANDING_PATH}}/{tname}")
        .withColumn("__bronze_ts", F.current_timestamp())
        .withColumn("__bronze_version", F.lit(1))
    )''')
        # delta_name already computed above for DLT

        raw_defs.append(f'''
def load_bronze_{safe}_raw():
    """Non-DLT fallback: load {tname} into Bronze Delta table with quality checks."""
    src_path = f"{{LANDING_PATH}}/{tname}"
    target   = f"`{{CATALOG}}`.`{{SCHEMA}}`.`{delta_name}`"

    print(f"  📥 Loading {{src_path}} → {{target}}")

    # Create restore point before write
    restore_ver = create_restore_point(CATALOG, SCHEMA, f"{delta_name}", "pre-bronze-load")

    try:
        df = spark.read.parquet(src_path)

        # ── Data quality checks ───────────────────────────────────────
        total = df.count()
        nulls = df.filter(F.col("__landing_ts").isNull()).count()
        dup_count = total - df.dropDuplicates().count()

        print(f"    📊 Rows: {{total:,}} | Null landing_ts: {{nulls}} | Duplicates: {{dup_count}}")

        if nulls > 0:
            df = df.filter(F.col("__landing_ts").isNotNull())
            print(f"    🧹 Dropped {{nulls}} rows with null __landing_ts")

        # ── Add bronze audit columns ──────────────────────────────────
        df = (df
              .withColumn("__bronze_ts", F.current_timestamp())
              .withColumn("__bronze_version", F.lit(1))
              .withColumn("__is_quarantined", F.lit(False)))

        # ── Write to Bronze Delta table ───────────────────────────────
        (df.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .option("delta.autoOptimize.optimizeWrite", "true")
         .saveAsTable(target))

        print(f"    ✅ Bronze {{target}}: {{df.count():,}} rows written")
        return {{"table": "{tname}", "status": "success", "rows": df.count()}}

    except Exception as e:
        print(f"    ❌ FAILED: {{e}}")
        if restore_ver is not None:
            restore_table(CATALOG, SCHEMA, f"{delta_name}", restore_ver)
        return {{"table": "{tname}", "status": "failed", "error": str(e)}}''')

    dlt_block = "\n".join(dlt_defs)
    raw_block = "\n".join(raw_defs)

    # Table list builder
    table_names_str = ", ".join(f'"{t["table"]}"' for t in tables)

    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Medallion Architecture — Bronze Layer
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC **Target:** `{catalog}.{schema}` (Bronze Delta Tables)
# MAGIC **Source:** Landing Zone at `{landing_path}`
# MAGIC
# MAGIC ### Pipeline Flow
# MAGIC ```
# MAGIC ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
# MAGIC │  Landing Zone   │ ──► │  Bronze Layer    │ ──► │  Silver Layer    │
# MAGIC │  ({landing_path})    │     │  ({catalog}.{schema})  │     │                 │
# MAGIC └─────────────────┘     └─────────────────┘     └─────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Bronze Layer Responsibilities
# MAGIC - Raw data ingestion from Landing Zone (Parquet → Delta)
# MAGIC - Schema enforcement & type preservation
# MAGIC - Data quality expectations (DLT) / checks (standard)
# MAGIC - Audit columns: `__bronze_ts`, `__bronze_version`, `__is_quarantined`
# MAGIC - Restore points for rollback on failure
# MAGIC
# MAGIC ### Supports Two Execution Modes
# MAGIC 1. **DLT Pipeline** — use `import dlt` with expectations (recommended)
# MAGIC 2. **Standard Spark** — fallback with manual quality checks
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "{catalog}", "Target Catalog")
dbutils.widgets.text("schema", "{schema}", "Target Schema")
dbutils.widgets.text("landing_path", "{landing_path}", "Landing Base Path")

CATALOG      = dbutils.widgets.get("catalog").strip()
SCHEMA       = dbutils.widgets.get("schema").strip()
LANDING_PATH = dbutils.widgets.get("landing_path").strip()

# Auto-detect DLT runtime — the dlt module is only importable inside a Spark Declarative Pipeline
try:
    import dlt
    _IS_DLT = True
except ImportError:
    _IS_DLT = False

EXEC_MODE = "dlt" if _IS_DLT else "standard"
print(f"🔧 Catalog      : {{CATALOG}}")
print(f"🔧 Schema       : {{SCHEMA}}")
print(f"🔧 Landing Path : {{LANDING_PATH}}")
print(f"🔧 Mode         : {{EXEC_MODE}}")

TABLE_NAMES = [{table_names_str}]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Utility Functions

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import json

def create_restore_point(catalog, schema, table_name, stage):
    """Record Delta table version for rollback."""
    try:
        delta_tbl = f"`{{catalog}}`.`{{schema}}`.`{{table_name}}`"
        history = spark.sql(f"DESCRIBE HISTORY {{delta_tbl}} LIMIT 1").collect()
        if history:
            version = history[0]["version"]
            print(f"   📌 Restore point: {{table_name}} v{{version}} ({{stage}})")
            return version
    except Exception:
        pass
    return None

def restore_table(catalog, schema, table_name, version):
    """Restore Delta table to a previous version on failure."""
    try:
        delta_tbl = f"`{{catalog}}`.`{{schema}}`.`{{table_name}}`"
        spark.sql(f"RESTORE TABLE {{delta_tbl}} TO VERSION AS OF {{version}}")
        print(f"   🔄 Restored {{table_name}} to version {{version}}")
        return True
    except Exception as e:
        print(f"   ❌ Restore failed: {{e}}")
        return False

def validate_bronze_table(catalog, schema, table_name):
    """Post-load validation for a Bronze table."""
    try:
        tbl = f"`{{catalog}}`.`{{schema}}`.`{{table_name}}`"
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {{tbl}}").collect()[0]["cnt"]
        nulls = spark.sql(f"SELECT COUNT(*) AS cnt FROM {{tbl}} WHERE __bronze_ts IS NULL").collect()[0]["cnt"]
        print(f"   🔍 Validation {{table_name}}: {{count:,}} rows, {{nulls}} null bronze_ts")
        return {{"rows": count, "null_audit": nulls, "valid": nulls == 0}}
    except Exception as e:
        print(f"   ⚠️ Validation failed: {{e}}")
        return {{"rows": 0, "null_audit": -1, "valid": False}}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏭 Spark Declarative Pipeline Definitions
# MAGIC > These are only active when running as a **Spark Declarative Pipeline**.
# MAGIC > In standard mode, they are skipped.

# COMMAND ----------

# ── SDP Mode: Lakeflow Spark Declarative Pipelines definitions ──────────────────────────────────
if _IS_DLT:
{dlt_block}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ Standard Mode: Manual Bronze Load

# COMMAND ----------

# ── Standard Mode: manual load with quality checks & restore points ──────────
{raw_block}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Execute Bronze Pipeline

# COMMAND ----------

if not _IS_DLT:
    print("\\n" + "="*60)
    print("🥉 BRONZE LAYER PIPELINE — Standard Mode")
    print("="*60)

    bronze_results = []
    for tname in TABLE_NAMES:
        safe = tname.replace(" ", "_").replace("-", "_").lower()
        fn_name = f"load_bronze_{{safe}}_raw"
        fn = globals().get(fn_name)
        if fn:
            result = fn()
            bronze_results.append(result)
        else:
            print(f"  ⚠️ No loader found for {{tname}}")
            bronze_results.append({{"table": tname, "status": "skipped"}})

    # ── Post-load validation ──────────────────────────────────────────────
    print("\\n" + "─"*40)
    print("🔍 Post-Load Validation")
    print("─"*40)
    for tname in TABLE_NAMES:
        safe = tname.replace(" ", "_").replace("-", "_").lower()
        validate_bronze_table(CATALOG, SCHEMA, f"{{TABLE_PREFIX}}{{safe}}")

    # ── Summary ───────────────────────────────────────────────────────────
    success = [r for r in bronze_results if r.get("status") == "success"]
    failed  = [r for r in bronze_results if r.get("status") == "failed"]
    print(f"\\n{'='*60}")
    print(f"📊 BRONZE LAYER COMPLETE")
    print(f"{'='*60}")
    print(f"  ✅ Succeeded: {{len(success)}} / {{len(bronze_results)}}")
    print(f"  ❌ Failed   : {{len(failed)}} / {{len(bronze_results)}}")

    exit_payload = json.dumps({{
        "status": "COMPLETED" if not failed else "PARTIAL",
        "succeeded": len(success),
        "failed": len(failed),
        "layer": "bronze",
    }})
    dbutils.notebook.exit(exit_payload)
else:
    print("ℹ️ Running in SDP mode — tables are managed by Lakeflow Spark Declarative Pipelines engine.")
    print(f"   📋 {{len(TABLE_NAMES)}} Bronze streaming table(s) registered in pipeline graph.")
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  3. SILVER NOTEBOOK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def generate_silver(
    tables: list,
    catalog: str = "main",
    schema: str = "default",
    bronze_catalog: str = "",
    bronze_schema: str = "",
    table_prefix: str = "silver_",
    bronze_table_prefix: str = "bronze_",
    cdc_mode: str = "watermark",
    primary_keys: list = None,
) -> str:
    """Generate the Silver layer DLT notebook with quality checks and CDC apply_changes."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    # Resolve bronze catalog/schema — defaults to same catalog/schema if not specified
    br_catalog = bronze_catalog or catalog
    br_schema = bronze_schema or schema
    pk_list = primary_keys or []

    # Build DLT definitions
    dlt_defs = []
    cdc_defs = []
    std_defs = []
    for t in tables:
        tname = t["table"]
        safe  = tname.replace(" ", "_").replace("-", "_").lower()
        delta_name = f"{table_prefix}{safe}"
        bronze_delta_name = f"{bronze_table_prefix}{safe}"

        # Per-table primary keys: table-level override or global
        t_pks = t.get("primary_keys", pk_list)
        pk_str = ", ".join(f'"{pk}"' for pk in t_pks) if t_pks else ""

        if cdc_mode == "change_tracking" and t_pks:
            # CDC path: use dlt.apply_changes() for SCD Type 1 MERGE
            cdc_defs.append(f'''
    dlt.create_streaming_table(
        name="{delta_name}",
        comment="Silver — CDC merge of {tname} via Change Tracking (SCD Type 1)",
        table_properties={{
            "quality": "silver",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.enableChangeDataFeed": "true",
        }},
    )

    dlt.apply_changes(
        target="{delta_name}",
        source="{bronze_delta_name}",
        keys=[{pk_str}],
        sequence_by=F.col("__bronze_ts"),
        apply_as_deletes=F.expr("__cdc_operation = 'D'"),
        except_column_list=["__landing_ts", "__load_type", "__is_quarantined",
                            "__cdc_operation", "__cdc_version", "__cdc_mode",
                            "__batch_id", "__bronze_version"],
    )
    print(f"  CDC apply_changes: {bronze_delta_name} -> {delta_name}")''')
        else:
            dlt_defs.append(f'''
@dlt.table(
    name="{delta_name}",
    comment="Silver layer — cleansed & validated {tname}",
    table_properties={{
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true",
    }},
)
@dlt.expect_or_drop("__valid_bronze_ts", "__bronze_ts IS NOT NULL")
@dlt.expect_or_drop("__not_quarantined", "__is_quarantined = false")
@dlt.expect("__has_batch_id", "__batch_id IS NOT NULL")
def silver_{safe}():
    """Read from Bronze, apply cleansing & quality rules for {tname}."""
    bronze_df = dlt.read("{bronze_delta_name}")

    return (
        bronze_df
        # ── Drop internal audit columns from landing ──────────────────
        .drop("__landing_ts", "__load_type", "__is_quarantined")
        # ── Trim all string columns ───────────────────────────────────
        .select([
            F.trim(F.col(c.name)).alias(c.name) if c.dataType.simpleString() == "string"
            else F.col(c.name)
            for c in bronze_df.schema
            if c.name not in ("__landing_ts", "__load_type", "__is_quarantined")
        ])
        # ── Deduplicate ───────────────────────────────────────────────
        .dropDuplicates()
        # ── Silver audit columns ──────────────────────────────────────
        .withColumn("__silver_ts", F.current_timestamp())
        .withColumn("__silver_version", F.lit(1))
        .withColumn("__dq_status", F.lit("passed"))
    )''')

        std_defs.append(f'''
def process_silver_{safe}():
    """Standard mode: Bronze → Silver for {tname} with quality checks & restore."""
    bronze_tbl = f"`{{BRONZE_CATALOG}}`.`{{BRONZE_SCHEMA}}`.`{bronze_delta_name}`"
    silver_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.`{delta_name}`"

    print(f"  🔄 Processing: {bronze_delta_name} ({{BRONZE_CATALOG}}.{{BRONZE_SCHEMA}}) → {delta_name} ({{CATALOG}}.{{SCHEMA}})")

    # ── Create restore point ──────────────────────────────────────────
    restore_ver = create_restore_point(CATALOG, SCHEMA, f"{delta_name}", "pre-silver")

    try:
        df = spark.sql(f"SELECT * FROM {{bronze_tbl}}")
        initial_count = df.count()
        print(f"    📊 Bronze rows: {{initial_count:,}}")

        # ── Data Quality Checks ───────────────────────────────────────
        dq_results = {{}}

        # Check 1: Null bronze timestamps
        null_ts = df.filter(F.col("__bronze_ts").isNull()).count()
        dq_results["null_bronze_ts"] = null_ts
        if null_ts > 0:
            df = df.filter(F.col("__bronze_ts").isNotNull())
            print(f"    🧹 Removed {{null_ts}} rows with null __bronze_ts")

        # Check 2: Quarantined rows
        quarantined = 0
        if "__is_quarantined" in df.columns:
            quarantined = df.filter(F.col("__is_quarantined") == True).count()
            dq_results["quarantined"] = quarantined
            if quarantined > 0:
                df = df.filter(F.col("__is_quarantined") == False)
                print(f"    🧹 Removed {{quarantined}} quarantined rows")

        # Check 3: Duplicate detection
        before_dedup = df.count()
        df = df.dropDuplicates()
        dup_count = before_dedup - df.count()
        dq_results["duplicates_removed"] = dup_count
        if dup_count > 0:
            print(f"    🧹 Removed {{dup_count}} duplicate rows")

        # Check 4: Completeness — ensure no entirely-null rows
        all_cols = [c for c in df.columns if not c.startswith("__")]
        if all_cols:
            null_expr = F.lit(True)
            for c in all_cols:
                null_expr = null_expr & F.col(c).isNull()
            empty_rows = df.filter(null_expr).count()
            dq_results["empty_rows"] = empty_rows
            if empty_rows > 0:
                df = df.filter(~null_expr)
                print(f"    🧹 Removed {{empty_rows}} entirely-null rows")

        # ── Apply transformations ─────────────────────────────────────
        # Trim all string columns
        for c in df.dtypes:
            if c[1] == "string" and not c[0].startswith("__"):
                df = df.withColumn(c[0], F.trim(F.col(c[0])))

        # Drop downstream-irrelevant audit columns
        drop_cols = ["__landing_ts", "__load_type", "__is_quarantined"]
        for dc in drop_cols:
            if dc in df.columns:
                df = df.drop(dc)

        # Add silver audit columns
        df = (df
              .withColumn("__silver_ts", F.current_timestamp())
              .withColumn("__silver_version", F.lit(1))
              .withColumn("__dq_status", F.lit("passed")))

        final_count = df.count()
        print(f"    📊 Silver rows: {{final_count:,}} (removed {{initial_count - final_count}})")

        # ── Write to Silver table ─────────────────────────────────────
        (df.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .option("delta.autoOptimize.optimizeWrite", "true")
         .saveAsTable(silver_tbl))

        print(f"    ✅ Silver {{silver_tbl}}: {{final_count:,}} rows written")

        # ── Record DQ metrics ─────────────────────────────────────────
        save_dq_metrics(CATALOG, SCHEMA, f"{delta_name}", dq_results, initial_count, final_count)

        return {{
            "table": "{tname}", "status": "success",
            "bronze_rows": initial_count, "silver_rows": final_count,
            "dq": dq_results
        }}

    except Exception as e:
        print(f"    ❌ FAILED: {{e}}")
        if restore_ver is not None:
            restore_table(CATALOG, SCHEMA, f"{delta_name}", restore_ver)
        return {{"table": "{tname}", "status": "failed", "error": str(e)}}''')

    dlt_block = "\n".join(dlt_defs)
    cdc_block = "\n".join(cdc_defs)
    std_block = "\n".join(std_defs)
    table_names_str = ", ".join(f'"{t["table"]}"' for t in tables)

    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Medallion Architecture — Silver Layer
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC **Target:** `{catalog}.{schema}` (Silver Delta Tables)
# MAGIC **Source:** Bronze Delta Tables in `{br_catalog}.{br_schema}`
# MAGIC
# MAGIC ### Pipeline Flow
# MAGIC ```
# MAGIC ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
# MAGIC │  Bronze Layer    │ ──► │  Silver Layer    │ ──► │  Gold / Reports  │
# MAGIC │  ({br_catalog}.{br_schema})  │     │  ({catalog}.{schema})  │     │                 │
# MAGIC └─────────────────┘     └─────────────────┘     └─────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Silver Layer Responsibilities
# MAGIC - Data cleansing: trim strings, remove nulls, deduplicate
# MAGIC - Data quality validation with DLT expectations / manual checks
# MAGIC - Quality metrics recording in `__dq_metrics` table
# MAGIC - Restore points via Delta time-travel for rollback
# MAGIC - Audit columns: `__silver_ts`, `__silver_version`, `__dq_status`
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
# MAGIC ## 📋 Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "{catalog}", "Silver Catalog")
dbutils.widgets.text("schema", "{schema}", "Silver Schema")
dbutils.widgets.text("bronze_catalog", "{br_catalog}", "Bronze Catalog")
dbutils.widgets.text("bronze_schema", "{br_schema}", "Bronze Schema")

CATALOG        = dbutils.widgets.get("catalog").strip()
SCHEMA         = dbutils.widgets.get("schema").strip()
BRONZE_CATALOG = dbutils.widgets.get("bronze_catalog").strip()
BRONZE_SCHEMA  = dbutils.widgets.get("bronze_schema").strip()

# Auto-detect DLT runtime — the dlt module is only importable inside a Spark Declarative Pipeline
try:
    import dlt
    _IS_DLT = True
except ImportError:
    _IS_DLT = False

EXEC_MODE = "dlt" if _IS_DLT else "standard"
print(f"🔧 Silver Catalog  : {{CATALOG}}.{{SCHEMA}}")
print(f"🔧 Bronze Catalog  : {{BRONZE_CATALOG}}.{{BRONZE_SCHEMA}}")
print(f"🔧 Mode           : {{EXEC_MODE}}")

TABLE_NAMES = [{table_names_str}]
TABLE_PREFIX = "{table_prefix}"
BRONZE_TABLE_PREFIX = "{bronze_table_prefix}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Utility Functions

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import json

def create_restore_point(catalog, schema, table_name, stage):
    """Record Delta version for rollback."""
    try:
        delta_tbl = f"`{{catalog}}`.`{{schema}}`.`{{table_name}}`"
        history = spark.sql(f"DESCRIBE HISTORY {{delta_tbl}} LIMIT 1").collect()
        if history:
            version = history[0]["version"]
            print(f"   📌 Restore point: {{table_name}} v{{version}} ({{stage}})")
            return version
    except Exception:
        pass
    return None

def restore_table(catalog, schema, table_name, version):
    """Restore Delta table to a specific version."""
    try:
        delta_tbl = f"`{{catalog}}`.`{{schema}}`.`{{table_name}}`"
        spark.sql(f"RESTORE TABLE {{delta_tbl}} TO VERSION AS OF {{version}}")
        print(f"   🔄 Restored {{table_name}} to version {{version}}")
        return True
    except Exception as e:
        print(f"   ❌ Restore failed: {{e}}")
        return False

def save_dq_metrics(catalog, schema, table_name, dq_results, before, after):
    """Persist data quality metrics for audit trail."""
    metrics_tbl = f"`{{catalog}}`.`{{schema}}`.__dq_metrics"
    try:
        spark.sql(f\"\"\"
            CREATE TABLE IF NOT EXISTS {{metrics_tbl}} (
                table_name     STRING,
                check_time     TIMESTAMP,
                rows_before    BIGINT,
                rows_after     BIGINT,
                rows_dropped   BIGINT,
                null_ts        BIGINT,
                quarantined    BIGINT,
                duplicates     BIGINT,
                empty_rows     BIGINT,
                dq_pass        BOOLEAN
            ) USING DELTA
        \"\"\")

        rows_dropped = before - after
        spark.sql(f\"\"\"
            INSERT INTO {{metrics_tbl}} VALUES (
                '{{table_name}}',
                current_timestamp(),
                {{before}},
                {{after}},
                {{rows_dropped}},
                {{dq_results.get("null_bronze_ts", 0)}},
                {{dq_results.get("quarantined", 0)}},
                {{dq_results.get("duplicates_removed", 0)}},
                {{dq_results.get("empty_rows", 0)}},
                {{str(rows_dropped < before * 0.5).lower()}}
            )
        \"\"\")
        print(f"   📈 DQ metrics saved for {{table_name}}")
    except Exception as e:
        print(f"   ⚠️ DQ metrics save failed: {{e}}")

def validate_silver_table(catalog, schema, table_name):
    """Post-silver validation."""
    try:
        tbl = f"`{{catalog}}`.`{{schema}}`.`{{table_name}}`"
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {{tbl}}").collect()[0]["cnt"]
        null_silver = spark.sql(f"SELECT COUNT(*) AS cnt FROM {{tbl}} WHERE __silver_ts IS NULL").collect()[0]["cnt"]
        null_dq = spark.sql(f"SELECT COUNT(*) AS cnt FROM {{tbl}} WHERE __dq_status IS NULL OR __dq_status != 'passed'").collect()[0]["cnt"]
        print(f"   🔍 {{table_name}}: {{count:,}} rows | null silver_ts: {{null_silver}} | dq_status!=passed: {{null_dq}}")
        return {{"rows": count, "null_ts": null_silver, "dq_issues": null_dq, "valid": null_silver == 0 and null_dq == 0}}
    except Exception as e:
        print(f"   ⚠️ Validation error: {{e}}")
        return {{"valid": False}}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏭 Spark Declarative Pipeline Definitions
# MAGIC > Active only when running as a **Spark Declarative Pipeline**.
# MAGIC > CDC tables use `dlt.apply_changes()` for SCD Type 1 merge.

# COMMAND ----------

if _IS_DLT:
{dlt_block}
{cdc_block}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ Standard Mode: Silver Processing

# COMMAND ----------

{std_block}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Execute Silver Pipeline

# COMMAND ----------

if not _IS_DLT:
    print("\\n" + "="*60)
    print("🥈 SILVER LAYER PIPELINE — Standard Mode")
    print("="*60)

    silver_results = []
    for tname in TABLE_NAMES:
        safe = tname.replace(" ", "_").replace("-", "_").lower()
        fn_name = f"process_silver_{{safe}}"
        fn = globals().get(fn_name)
        if fn:
            res = fn()
            silver_results.append(res)
        else:
            print(f"  ⚠️ No processor for {{tname}}")
            silver_results.append({{"table": tname, "status": "skipped"}})

    # ── Post-processing validation ────────────────────────────────────────
    print("\\n" + "─"*40)
    print("🔍 Post-Silver Validation")
    print("─"*40)
    for tname in TABLE_NAMES:
        safe = tname.replace(" ", "_").replace("-", "_").lower()
        validate_silver_table(CATALOG, SCHEMA, f"{{TABLE_PREFIX}}{{safe}}")

    # ── Summary ───────────────────────────────────────────────────────────
    success = [r for r in silver_results if r.get("status") == "success"]
    failed  = [r for r in silver_results if r.get("status") == "failed"]
    total_bronze = sum(r.get("bronze_rows", 0) for r in silver_results)
    total_silver = sum(r.get("silver_rows", 0) for r in silver_results)

    print(f"\\n{'='*60}")
    print(f"📊 SILVER LAYER COMPLETE")
    print(f"{'='*60}")
    print(f"  ✅ Succeeded    : {{len(success)}} / {{len(silver_results)}}")
    print(f"  ❌ Failed       : {{len(failed)}} / {{len(silver_results)}}")
    print(f"  📊 Bronze In    : {{total_bronze:,}}")
    print(f"  📊 Silver Out   : {{total_silver:,}}")
    print(f"  🧹 Rows Cleaned : {{total_bronze - total_silver:,}}")

    if failed:
        print(f"\\n⚠️ Failed tables:")
        for f_item in failed:
            print(f"   • {{f_item['table']}}: {{f_item.get('error','unknown')}}")

    exit_payload = json.dumps({{
        "status": "COMPLETED" if not failed else "PARTIAL",
        "succeeded": len(success),
        "failed": len(failed),
        "total_bronze": total_bronze,
        "total_silver": total_silver,
        "layer": "silver",
    }})
    dbutils.notebook.exit(exit_payload)
else:
    print("ℹ️ Running in SDP mode — tables are managed by Lakeflow Spark Declarative Pipelines engine.")
    print(f"   📋 {{len(TABLE_NAMES)}} Silver table(s) registered in pipeline graph.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 DQ Metrics Dashboard Query
# MAGIC > Run this cell manually to view accumulated quality metrics.

# COMMAND ----------

# ── Optional: view DQ metrics ─────────────────────────────────────────────────
try:
    metrics_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.__dq_metrics"
    display(spark.sql(f\"\"\"
        SELECT table_name, check_time, rows_before, rows_after, rows_dropped,
               null_ts, quarantined, duplicates, empty_rows, dq_pass
        FROM {{metrics_tbl}}
        ORDER BY check_time DESC
        LIMIT 50
    \"\"\"))
except Exception:
    print("ℹ️ No DQ metrics recorded yet. Run the Silver pipeline first.")
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  4. ORCHESTRATOR NOTEBOOK (Bonus: chains all 3)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def generate_orchestrator(
    source_type: str,
    server: str,
    database: str,
    username: str,
    tables: list,
    catalog: str = "main",
    schema: str = "default",
    landing_path: str = "/mnt/landing",
    workspace_path: str = "/Shared/Medallion",
    volumes_catalog: str = "",
    bronze_catalog: str = "",
    silver_catalog: str = "",
    target_schema: str = "",
) -> str:
    """Generate an orchestrator notebook that chains Landing → Bronze → Silver."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    # Resolve multi-catalog labels for display
    vol_label = volumes_catalog or "Landing Zone"
    brz_label = f"{bronze_catalog}.{target_schema}" if bronze_catalog else f"{catalog}.{schema}"
    slv_label = f"{silver_catalog}.{target_schema}" if silver_catalog else f"{catalog}.{schema}"
    brz_cat = bronze_catalog or catalog
    brz_sch = target_schema or schema
    slv_cat = silver_catalog or catalog
    slv_sch = target_schema or schema
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 Medallion Pipeline Orchestrator
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC ### Pipeline Flow
# MAGIC ```
# MAGIC ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
# MAGIC │  Source Extract  │ ──► │  Bronze Layer    │ ──► │  Silver Layer   │
# MAGIC │  → {vol_label:<13} │     │  ({brz_label:<13}) │     │  ({slv_label:<13}) │
# MAGIC └─────────────────┘     └─────────────────┘     └─────────────────┘
# MAGIC ```
# MAGIC
# MAGIC Chains: **Extract → {vol_label}** → **{brz_label} (Bronze)** → **{slv_label} (Silver)**
# MAGIC
# MAGIC Includes automatic rollback if any stage fails.
# MAGIC ---

# COMMAND ----------

dbutils.widgets.text("load_type", "full", "Load Type (full / incremental)")
dbutils.widgets.text("password_b64", "", "Source DB Password (base64)")

LOAD_TYPE    = dbutils.widgets.get("load_type").strip()
PASSWORD_B64 = dbutils.widgets.get("password_b64").strip()

import json
from datetime import datetime

run_log = []

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{{ts}}] {{msg}}"
    run_log.append(entry)
    print(entry)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 1: Landing Zone

# COMMAND ----------

log("🚀 Stage 1: Extract → {vol_label} — starting extraction…")
try:
    landing_result = dbutils.notebook.run(
        "{workspace_path}/01_Landing_Zone",
        timeout_seconds=3600,
        arguments={{
            "load_type":     LOAD_TYPE,
            "password_b64":  PASSWORD_B64,
            "landing_path":  "{landing_path}",
        }}
    )
    landing_data = json.loads(landing_result)
    if landing_data.get("status") == "FAILED":
        raise Exception(f"Landing failed: {{landing_data.get('error', 'unknown')}}")
    log(f"✅ Extract → {vol_label} complete: {{landing_data.get('succeeded', 0)}} tables, {{landing_data.get('total_rows', 0):,}} rows")
    # Collect table names from landing for downstream stages
    _landed_tables = landing_data.get("tables", [])
    _tables_json = json.dumps(_landed_tables)
    log(f"   Tables landed: {{_landed_tables}}")
except Exception as e:
    log(f"❌ Extract → {vol_label} FAILED: {{e}}")
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "stage": "extract", "error": str(e), "log": run_log}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 2: Bronze Layer

# COMMAND ----------

log("🚀 Stage 2: {vol_label} → {brz_label} (Bronze) — ingesting raw data…")
try:
    bronze_result = dbutils.notebook.run(
        "{workspace_path}/02_Bronze",
        timeout_seconds=3600,
        arguments={{"mode": "standard", "catalog": "{brz_cat}", "schema": "{brz_sch}", "landing_path": "{landing_path}", "tables_json": _tables_json}}
    )
    bronze_data = json.loads(bronze_result)
    if bronze_data.get("failed", 0) > 0:
        log(f"⚠️ Bronze had {{bronze_data['failed']}} failures — continuing to Silver for successful tables")
    log(f"✅ Bronze complete: {{bronze_data.get('succeeded', 0)}} tables processed")
except Exception as e:
    log(f"❌ Bronze Layer FAILED: {{e}}")
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "stage": "bronze", "error": str(e), "log": run_log}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 3: Silver Layer

# COMMAND ----------

log("🚀 Stage 3: {brz_label} (Bronze) → {slv_label} (Silver) — cleansing & validation…")
try:
    silver_result = dbutils.notebook.run(
        "{workspace_path}/03_Silver",
        timeout_seconds=3600,
        arguments={{"mode": "standard", "catalog": "{slv_cat}", "schema": "{slv_sch}", "bronze_catalog": "{brz_cat}", "bronze_schema": "{brz_sch}", "tables_json": _tables_json}}
    )
    silver_data = json.loads(silver_result)
    log(f"✅ Silver complete: {{silver_data.get('succeeded', 0)}} tables, Bronze→Silver: {{silver_data.get('total_bronze', 0):,}} → {{silver_data.get('total_silver', 0):,}}")
except Exception as e:
    log(f"❌ Silver Layer FAILED: {{e}}")
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "stage": "silver", "error": str(e), "log": run_log}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Pipeline Summary

# COMMAND ----------

log("🎯 Medallion Pipeline COMPLETE")
print("\\n" + "="*60)
print("PIPELINE RUN LOG")
print("="*60)
for entry in run_log:
    print(entry)

dbutils.notebook.exit(json.dumps({{
    "status": "COMPLETED",
    "stages": ["landing", "bronze", "silver"],
    "log": run_log,
}}))
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PUBLIC API: Generate all notebooks at once
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def generate_all_medallion_notebooks(
    source_type: str,
    server: str,
    database: str,
    username: str,
    tables: list,
    catalog: str = "main",
    schema: str = "default",
    landing_path: str = "/mnt/landing",
    workspace_path: str = "/Shared/Medallion",
    volumes_catalog: str = "",
    bronze_catalog: str = "",
    silver_catalog: str = "",
    target_schema: str = "",
    cdc_mode: str = "watermark",
    primary_keys: list = None,
    account: str = "",
    warehouse: str = "",
    role: str = "",
) -> dict:
    """
    Generate all Medallion notebooks and return them as a dict.

    Multi-Catalog Medallion Architecture:
      When volumes_catalog, bronze_catalog, silver_catalog are provided:
        1. Source Extract → dev_volumes (UC Volumes: /Volumes/{volumes_catalog}/{target_schema}/landing/)
        2. dev_volumes → bronze.{target_schema} (Bronze catalog, hr schema)
        3. bronze.{target_schema} → silver.{target_schema} (Silver catalog, hr schema)

    Returns: {notebooks: [{name, code, description}], summary: {...}}
    """
    # ── Resolve multi-catalog vs legacy mode ──
    multi_catalog = bool(volumes_catalog and bronze_catalog and silver_catalog)
    tgt_schema = target_schema or schema

    if multi_catalog:
        effective_landing_path = f"/Volumes/{volumes_catalog}/{tgt_schema}/landing"
        bronze_table_prefix = ""       # No prefix — catalog IS the layer
        silver_table_prefix = ""
    else:
        effective_landing_path = landing_path
        bronze_table_prefix = "bronze_"
        silver_table_prefix = "silver_"

    landing = generate_landing_zone(
        source_type, server, database, username, tables,
        catalog=volumes_catalog or catalog,
        schema=tgt_schema,
        landing_path=effective_landing_path,
        account=account,
        warehouse=warehouse,
        role=role,
    )
    bronze = generate_bronze(
        tables,
        catalog=bronze_catalog or catalog,
        schema=tgt_schema,
        landing_path=effective_landing_path,
        table_prefix=bronze_table_prefix,
        cdc_mode=cdc_mode,
    )
    silver = generate_silver(
        tables,
        catalog=silver_catalog or catalog,
        schema=tgt_schema,
        bronze_catalog=bronze_catalog or catalog,
        bronze_schema=tgt_schema,
        table_prefix=silver_table_prefix,
        bronze_table_prefix=bronze_table_prefix,
        cdc_mode=cdc_mode,
        primary_keys=primary_keys or [],
    )
    orchestrator = generate_orchestrator(
        source_type, server, database, username, tables,
        catalog=catalog,
        schema=schema,
        landing_path=effective_landing_path,
        workspace_path=workspace_path,
        volumes_catalog=volumes_catalog,
        bronze_catalog=bronze_catalog,
        silver_catalog=silver_catalog,
        target_schema=target_schema,
    )

    # Build descriptive labels for multi-catalog or legacy mode
    vol_lbl = volumes_catalog or "Landing Zone"
    brz_lbl = f"{bronze_catalog}.{tgt_schema}" if bronze_catalog else f"{catalog}.{schema}"
    slv_lbl = f"{silver_catalog}.{tgt_schema}" if silver_catalog else f"{catalog}.{schema}"

    notebooks = [
        {
            "name": "01_Landing_Zone",
            "code": landing,
            "description": f"Extract from source DB → {vol_lbl} ({effective_landing_path})",
            "layer": "landing",
            "lines": len(landing.splitlines()),
        },
        {
            "name": "02_Bronze",
            "code": bronze,
            "description": f"{vol_lbl} → {brz_lbl} (Bronze Delta tables with SDP & quality checks)",
            "layer": "bronze",
            "lines": len(bronze.splitlines()),
        },
        {
            "name": "03_Silver",
            "code": silver,
            "description": f"{brz_lbl} → {slv_lbl} (Silver with cleansing, DQ validation & restore)",
            "layer": "silver",
            "lines": len(silver.splitlines()),
        },
        {
            "name": "00_Orchestrator",
            "code": orchestrator,
            "description": f"Chains {vol_lbl} → {brz_lbl} → {slv_lbl} with auto-rollback",
            "layer": "orchestrator",
            "lines": len(orchestrator.splitlines()),
        },
    ]

    return {
        "success": True,
        "notebooks": notebooks,
        "summary": {
            "total_notebooks": len(notebooks),
            "total_tables": len(tables),
            "total_lines": sum(n["lines"] for n in notebooks),
            "layers": ["landing", "bronze", "silver", "orchestrator"],
            "multi_catalog": multi_catalog,
            "volumes_catalog": volumes_catalog,
            "bronze_catalog": bronze_catalog or catalog,
            "silver_catalog": silver_catalog or catalog,
            "target_schema": tgt_schema,
            "landing_path": effective_landing_path,
            "features": [
                "Full Load & Incremental Load",
                "Lakeflow Spark Declarative Pipelines (SDP) pipelines",
                "Data quality checks & expectations",
                "Restore points (Delta time-travel)",
                "Watermark-based CDC",
                "Audit columns at every layer",
                "DQ metrics recording",
                "Parallel extraction",
                "Auto-rollback on failure",
                "Multi-Catalog Medallion Architecture" if multi_catalog else "Single-Catalog Mode",
                "SQL Server Change Tracking (CDC)" if cdc_mode == "change_tracking" else "Watermark CDC",
                "SDP apply_changes() SCD Type 1" if cdc_mode == "change_tracking" else "Standard DLT tables",
                "Change Data Feed (CDF) enabled on Bronze & Silver",
            ],
        },
    }
