# Databricks notebook source
# pyright: reportUndefinedVariable=false
# MAGIC %md
# MAGIC # Aggregate Reconciliation — Source vs Bronze
# MAGIC
# MAGIC Performs aggregate reconciliation between the **source database**
# MAGIC and the **Bronze Delta table** for the current pipeline execution.
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Identifies all numeric columns (int, bigint, float, decimal, etc.)
# MAGIC 2. Computes SUM for each numeric column from Source (JDBC) and Bronze (Delta)
# MAGIC 3. Compares row counts
# MAGIC 4. Saves per-column results to reconciliation table
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("password_b64", "", "Source DB Password (base64)")
dbutils.widgets.text("catalog", "main", "Metadata Catalog")
dbutils.widgets.text("schema", "default", "Metadata Schema")
dbutils.widgets.text("landing_path", "/mnt/landing", "Landing Base Path")
dbutils.widgets.text("recon_catalog", "reconciliation", "Reconciliation Catalog")
dbutils.widgets.text("recon_schema", "hr", "Reconciliation Schema")
dbutils.widgets.text("recon_table", "ReconcilationDetails", "Reconciliation Table")

import base64, json, uuid
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

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

print(f"Reconciliation for Job: {JOB_ID}, Run: {RUN_ID}")
print(f"Results -> {RECON_CATALOG}.{RECON_SCHEMA}.{RECON_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure Reconciliation Table Exists

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{RECON_CATALOG}`.`{RECON_SCHEMA}`")

recon_full_table = f"`{RECON_CATALOG}`.`{RECON_SCHEMA}`.`{RECON_TABLE}`"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {recon_full_table} (
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
print(f"Table {recon_full_table} ready")

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
# MAGIC ## Read Job Metadata

# COMMAND ----------

job_tbl = f"`{CATALOG}`.`{SCHEMA}`.wf_job_metadata"
job_df  = spark.sql(f"SELECT * FROM {job_tbl} WHERE job_id = '{JOB_ID}'")

if job_df.count() == 0:
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED", "reason": f"Job {JOB_ID} not found"}))

job = job_df.collect()[0].asDict()
TABLE_NAME   = job["table_name"]
TABLE_SCHEMA = job["table_schema"]
FULL_TABLE   = job["full_table"]

source_config = json.loads(job.get("source_config", "{}") or "{}")
SERVER   = source_config.get("server", "")
DATABASE = source_config.get("database", "")
USERNAME = source_config.get("username", "")

target_config = json.loads(job.get("target_config", "{}") or "{}")
BRONZE_CATALOG = target_config.get("bronze_catalog", "")
TGT_SCHEMA     = target_config.get("target_schema", "")

if BRONZE_CATALOG and TGT_SCHEMA:
    BRONZE_TABLE = f"`{BRONZE_CATALOG}`.`{TGT_SCHEMA}`.`{TABLE_NAME}`"
else:
    BRONZE_TABLE = f"`{target_config.get('catalog', CATALOG)}`.`{target_config.get('schema', SCHEMA)}`.`bronze_{TABLE_NAME}`"

print(f"Source: [{TABLE_SCHEMA}].[{TABLE_NAME}] on {SERVER}/{DATABASE}")
print(f"Bronze: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## JDBC Connection to Source

# COMMAND ----------

encrypt = "true" if source_config.get("source_type") in ("azuresql", "synapse") else "false"
trust   = "false" if source_config.get("source_type") in ("azuresql", "synapse") else "true"

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Numeric Columns from Source

# COMMAND ----------

numeric_types_sql = "('int','bigint','smallint','tinyint','float','real','decimal','numeric','money','smallmoney')"
col_query = f"""(
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{TABLE_SCHEMA}'
      AND TABLE_NAME   = '{TABLE_NAME}'
      AND DATA_TYPE IN {numeric_types_sql}
) AS col_info"""

try:
    cols_df = spark.read.jdbc(jdbc_url, col_query, properties=jdbc_props)
    numeric_cols = [(r["COLUMN_NAME"], r["DATA_TYPE"]) for r in cols_df.collect()]
    print(f"Found {len(numeric_cols)} numeric columns")
except Exception as e:
    print(f"Failed to read column metadata: {e}")
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "error": str(e)[:500]}))

if not numeric_cols:
    print("No numeric columns found — reconciliation skipped")
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED", "reason": "No numeric columns"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Source & Bronze Aggregates

# COMMAND ----------

# Source aggregates
agg_exprs = ["COUNT(*) AS __row_count"]
for cn, _ in numeric_cols:
    agg_exprs.append(f"SUM(CAST([{cn}] AS FLOAT)) AS [sum_{cn}]")

agg_sql = ", ".join(agg_exprs)
src_query = f"(SELECT {agg_sql} FROM [{TABLE_SCHEMA}].[{TABLE_NAME}]) AS src_agg"

try:
    src_agg_df = spark.read.jdbc(jdbc_url, src_query, properties=jdbc_props)
    src_row = src_agg_df.collect()[0]
    src_count = int(src_row["__row_count"])
    print(f"Source row count: {src_count:,}")
except Exception as e:
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "error": str(e)[:500]}))

# Bronze aggregates
try:
    brz_df = spark.table(BRONZE_TABLE)
    brz_count = brz_df.count()
    print(f"Bronze row count: {brz_count:,}")

    brz_agg_exprs = [F.count("*").alias("__row_count")]
    for cn, _ in numeric_cols:
        brz_agg_exprs.append(F.sum(F.col(f"`{cn}`").cast("double")).alias(f"sum_{cn}"))

    brz_agg_df = brz_df.agg(*brz_agg_exprs)
    brz_row = brz_agg_df.collect()[0]
except Exception as e:
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "error": str(e)[:500]}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare & Build Reconciliation Results

# COMMAND ----------

recon_ts = datetime.now()
results = []

# Row count reconciliation
count_match = "PASS" if src_count == brz_count else "FAIL"
count_variance = abs(src_count - brz_count)
results.append({
    "recon_run_id": RECON_RUN_ID, "pipeline_run_id": RUN_ID, "job_id": JOB_ID,
    "source_table": FULL_TABLE, "bronze_table": BRONZE_TABLE,
    "column_name": "__ROW_COUNT__", "data_type": "count",
    "source_value": float(src_count), "bronze_value": float(brz_count),
    "variance": float(count_variance),
    "variance_pct": round((count_variance / src_count * 100), 4) if src_count > 0 else 0.0,
    "status": count_match, "recon_timestamp": recon_ts,
})

# Per-column SUM reconciliation
for cn, ct in numeric_cols:
    src_val = src_row[f"sum_{cn}"]
    brz_val = brz_row[f"sum_{cn}"]
    s = float(src_val) if src_val is not None else 0.0
    b = float(brz_val) if brz_val is not None else 0.0
    var = abs(s - b)
    pct = round((var / abs(s) * 100), 4) if s != 0.0 else 0.0
    status = "PASS" if var < 0.01 else ("WARN" if pct < 0.01 else "FAIL")

    results.append({
        "recon_run_id": RECON_RUN_ID, "pipeline_run_id": RUN_ID, "job_id": JOB_ID,
        "source_table": FULL_TABLE, "bronze_table": BRONZE_TABLE,
        "column_name": cn, "data_type": ct,
        "source_value": s, "bronze_value": b,
        "variance": var, "variance_pct": pct,
        "status": status, "recon_timestamp": recon_ts,
    })

print(f"\nReconciliation results: {len(results)} checks")
for r in results:
    icon = "PASS" if r["status"] == "PASS" else ("WARN" if r["status"] == "WARN" else "FAIL")
    print(f"   [{icon}] {r['column_name']:<30} src={r['source_value']:>15,.2f}  brz={r['bronze_value']:>15,.2f}  var={r['variance_pct']:.4f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Reconciliation Table

# COMMAND ----------

recon_df = spark.createDataFrame(results, schema=recon_schema_def)
recon_df.write.mode("append").option("mergeSchema", "true").saveAsTable(recon_full_table)

total_checks = len(results)
passed  = sum(1 for r in results if r["status"] == "PASS")
warned  = sum(1 for r in results if r["status"] == "WARN")
failed_ = sum(1 for r in results if r["status"] == "FAIL")

print(f"\nSaved {total_checks} reconciliation records to {recon_full_table}")
print(f"   PASS: {passed}  WARN: {warned}  FAIL: {failed_}")

# COMMAND ----------

exit_payload = json.dumps({
    "status": "COMPLETED", "recon_run_id": RECON_RUN_ID,
    "job_id": JOB_ID, "run_id": RUN_ID, "table": FULL_TABLE,
    "checks": total_checks, "passed": passed, "warned": warned, "failed": failed_,
    "recon_table": recon_full_table,
})

print(f"\nRECONCILIATION COMPLETE — {FULL_TABLE} — {total_checks} checks ({passed} pass, {warned} warn, {failed_} fail)")
dbutils.notebook.exit(exit_payload)
