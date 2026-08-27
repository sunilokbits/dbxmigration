# Databricks notebook source
# pyright: reportUndefinedVariable=false
# MAGIC %md
# MAGIC # Execution Log — Pipeline Run Audit Trail
# MAGIC
# MAGIC Saves per-job execution details to the logging catalog
# MAGIC as an append-only audit trail.
# MAGIC
# MAGIC Called by the Orchestrator AFTER all jobs complete.
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Metadata Catalog")
dbutils.widgets.text("schema", "default", "Metadata Schema")
dbutils.widgets.text("log_catalog", "loggingdetails", "Log Catalog")
dbutils.widgets.text("log_schema", "hr", "Log Schema")
dbutils.widgets.text("log_table", "ExecutionLog", "Log Table")
dbutils.widgets.text("results_json", "{}", "Results JSON")
dbutils.widgets.text("groups_json", "[]", "Groups JSON")
dbutils.widgets.text("orchestrator_status", "", "Orchestrator Status")

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

print(f"Execution Log Run: {LOG_RUN_ID}")
print(f"Target: {LOG_CATALOG}.{LOG_SCHEMA}.{LOG_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Execution Results

# COMMAND ----------

try:
    results_raw = json.loads(RESULTS_JSON)
except Exception:
    results_raw = []

try:
    groups = json.loads(GROUPS_JSON)
except Exception:
    groups = []

group_lookup = {}
for g in groups:
    gid = g.get("group_id", "")
    group_lookup[gid] = {
        "full_table": g.get("full_table", ""),
        "load_type":  g.get("load_type", "full"),
    }

if isinstance(results_raw, dict):
    results_list = [results_raw]
elif isinstance(results_raw, list):
    results_list = results_raw
else:
    results_list = []

print(f"Received {len(results_list)} job results, {len(groups)} groups")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure Logging Table Exists

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{LOG_CATALOG}`.`{LOG_SCHEMA}`")

log_full_table = f"`{LOG_CATALOG}`.`{LOG_SCHEMA}`.`{LOG_TABLE}`"

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

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {log_full_table} (
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
print(f"Table {log_full_table} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Log Rows

# COMMAND ----------

log_rows = []

for entry in results_list:
    job_name = entry.get("job", "unknown")
    status   = entry.get("status", "UNKNOWN")
    rows     = entry.get("rows", 0)
    error    = entry.get("error", "")

    # Infer stage from job name
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

    full_table = job_name
    load_type  = "full"
    for gid, ginfo in group_lookup.items():
        if ginfo.get("full_table", "") and ginfo["full_table"] in job_name:
            full_table = ginfo["full_table"]
            load_type  = ginfo.get("load_type", "full")
            break

    log_rows.append({
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
    })

print(f"Built {len(log_rows)} log entries")

if not log_rows:
    print("No execution data to log")
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED", "reason": "No execution data"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Logging Table

# COMMAND ----------

log_df = spark.createDataFrame(log_rows, schema=log_schema)
log_df.write.mode("append").option("mergeSchema", "true").saveAsTable(log_full_table)

total_logged = len(log_rows)
success_count = sum(1 for r in log_rows if r["status"] == "SUCCESS")
failed_count  = sum(1 for r in log_rows if r["status"] == "FAILED")

print(f"\nSaved {total_logged} execution log records to {log_full_table}")
print(f"   SUCCESS: {success_count}  FAILED: {failed_count}  OTHER: {total_logged - success_count - failed_count}")

# COMMAND ----------

exit_payload = json.dumps({
    "status":       "COMPLETED",
    "log_run_id":   LOG_RUN_ID,
    "total_logged": total_logged,
    "success":      success_count,
    "failed":       failed_count,
    "log_table":    log_full_table,
})

print(f"\nEXECUTION LOG COMPLETE — {total_logged} entries saved to {log_full_table}")
dbutils.notebook.exit(exit_payload)
