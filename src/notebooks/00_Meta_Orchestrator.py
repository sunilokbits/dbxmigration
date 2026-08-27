# Databricks notebook source
# pyright: reportUndefinedVariable=false
# MAGIC %md
# MAGIC # Metadata-Driven Orchestrator
# MAGIC
# MAGIC Reads pipeline metadata from Delta tables and chains:
# MAGIC   Extract -> Bronze -> Reconciliation -> Silver -> Logging
# MAGIC
# MAGIC Can run a **single pipeline group** or **all groups**.
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("group_id", "", "Pipeline Group ID (blank = run all)")
dbutils.widgets.text("load_type", "", "Load Type Override (full/incremental, blank = use metadata)")
dbutils.widgets.text("password_b64", "", "Source DB Password (base64)")
dbutils.widgets.text("catalog", "main", "Metadata Catalog")
dbutils.widgets.text("schema", "default", "Metadata Schema")
dbutils.widgets.text("landing_path", "/mnt/landing", "Landing Base Path")
dbutils.widgets.text("workspace_path", "/Shared/MetadataPipeline", "Notebook Workspace Path")
dbutils.widgets.text("recon_catalog", "reconciliation", "Reconciliation Catalog")
dbutils.widgets.text("recon_schema", "hr", "Reconciliation Schema")
dbutils.widgets.text("recon_table", "ReconcilationDetails", "Reconciliation Table")
dbutils.widgets.text("log_catalog", "loggingdetails", "Logging Catalog")
dbutils.widgets.text("log_schema", "hr", "Logging Schema")
dbutils.widgets.text("log_table", "ExecutionLog", "Logging Table")

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
# MAGIC ## Load Pipeline Groups

# COMMAND ----------

import json, uuid
from datetime import datetime

job_tbl  = f"`{CATALOG}`.`{SCHEMA}`.wf_job_metadata"
run_tbl  = f"`{CATALOG}`.`{SCHEMA}`.wf_run_history"
pipe_tbl = f"`{CATALOG}`.`{SCHEMA}`.wf_pipeline_metadata"

if GROUP_ID:
    groups_df = spark.sql(f"SELECT * FROM {pipe_tbl} WHERE group_id = '{GROUP_ID}'")
else:
    groups_df = spark.sql(f"SELECT * FROM {pipe_tbl}")

groups = [r.asDict() for r in groups_df.collect()]
print(f"Pipeline groups to run: {len(groups)}")
for g in groups:
    print(f"   {g['full_table']} ({g.get('load_type','full')})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Pipelines

# COMMAND ----------

stage_notebook = {
    "extract":           f"{WORKSPACE_PATH}/01_Meta_Extract",
    "landing_to_bronze": f"{WORKSPACE_PATH}/02_Meta_Bronze",
    "bronze_to_silver":  f"{WORKSPACE_PATH}/03_Meta_Silver",
}

results = []

for group in groups:
    gid = group["group_id"]
    print(f"\n{'='*60}")
    print(f"Pipeline: {group['full_table']}")
    print(f"{'='*60}")

    jobs_df = spark.sql(f"""
        SELECT * FROM {job_tbl}
        WHERE group_id = '{gid}' AND (enabled = true OR enabled IS NULL)
        ORDER BY job_order ASC
    """)
    jobs = [r.asDict() for r in jobs_df.collect()]

    group_ok = True
    for job in jobs:
        job_id   = job["job_id"]
        stage    = job["stage"]
        nb_path  = stage_notebook.get(stage)
        if not nb_path:
            print(f"   Unknown stage '{stage}' — skipping")
            continue

        run_id = uuid.uuid4().hex[:12]
        load_type = LOAD_OVERRIDE if LOAD_OVERRIDE else (job.get("load_type") or "full")

        try:
            spark.sql(f"""
                INSERT INTO {run_tbl} (run_id, job_id, job_name, stage, full_table,
                    load_type, watermark_column, status, started_at)
                VALUES ('{run_id}', '{job_id}', '{job["job_name"]}', '{stage}',
                    '{job["full_table"]}', '{load_type}', '{job.get("watermark_column","")}',
                    'running', current_timestamp())
            """)
        except Exception as e:
            print(f"   Could not create run record: {e}")

        print(f"\n   Running: {job['job_name']} ({stage})")

        try:
            result_json = dbutils.notebook.run(
                nb_path,
                timeout_seconds=3600,
                arguments={
                    "job_id":       job_id,
                    "run_id":       run_id,
                    "load_type":    load_type,
                    "password_b64": PASSWORD_B64,
                    "catalog":      CATALOG,
                    "schema":       SCHEMA,
                    "landing_path": LANDING_PATH,
                }
            )
            result = json.loads(result_json) if result_json else {}
            status = result.get("status", "UNKNOWN")
            rows   = result.get("rows", 0)
            error  = result.get("error", "")

            if status in ("FAILED", "ERROR"):
                print(f"   {job['job_name']}: {status} — {error}")
                results.append({"job": job["job_name"], "status": "FAILED", "rows": rows, "error": error})
                group_ok = False
                print(f"   Stopping pipeline for {group['full_table']} due to failure")
                break
            else:
                print(f"   {job['job_name']}: {status} ({rows:,} rows)")
                results.append({"job": job["job_name"], "status": status, "rows": rows})

                # Reconciliation after Bronze
                if stage == "landing_to_bronze":
                    print(f"\n   Running Reconciliation for {job['job_name']}...")
                    try:
                        recon_json = dbutils.notebook.run(
                            f"{WORKSPACE_PATH}/04_Meta_Reconciliation",
                            timeout_seconds=1800,
                            arguments={
                                "job_id":        job_id,
                                "run_id":        run_id,
                                "password_b64":  PASSWORD_B64,
                                "catalog":       CATALOG,
                                "schema":        SCHEMA,
                                "landing_path":  LANDING_PATH,
                                "recon_catalog": RECON_CATALOG,
                                "recon_schema":  RECON_SCHEMA,
                                "recon_table":   RECON_TABLE,
                            }
                        )
                        recon_result = json.loads(recon_json) if recon_json else {}
                        r_status = recon_result.get("status", "UNKNOWN")
                        r_checks = recon_result.get("checks", 0)
                        r_passed = recon_result.get("passed", 0)
                        r_failed = recon_result.get("failed", 0)
                        print(f"   Reconciliation: {r_status} — {r_checks} checks ({r_passed} pass, {r_failed} fail)")
                        results.append({"job": f"Recon_{job['job_name']}", "status": r_status, "rows": r_checks})
                    except Exception as re:
                        print(f"   Reconciliation failed (non-blocking): {re}")
                        results.append({"job": f"Recon_{job['job_name']}", "status": "WARN", "rows": 0, "error": str(re)[:200]})

        except Exception as e:
            print(f"   {job['job_name']} FAILED: {e}")
            try:
                spark.sql(f"""
                    MERGE INTO {run_tbl} AS t
                    USING (SELECT '{run_id}' AS run_id) AS s ON t.run_id = s.run_id
                    WHEN MATCHED THEN UPDATE SET
                        t.status = 'failed',
                        t.error_message = '{str(e).replace("'","''")[:500]}',
                        t.completed_at = current_timestamp()
                """)
                spark.sql(f"""
                    UPDATE {job_tbl}
                    SET last_status = 'failed', status = 'failed',
                        fail_count = fail_count + 1, updated_at = current_timestamp()
                    WHERE job_id = '{job_id}'
                """)
            except Exception:
                pass
            results.append({"job": job["job_name"], "status": "FAILED", "error": str(e)})
            group_ok = False
            print(f"   Stopping pipeline for {group['full_table']} due to failure")
            break

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestration Summary

# COMMAND ----------

succeeded = [r for r in results if r.get("status") in ("COMPLETED", "SUCCESS") and not r.get("job","").startswith("Recon_")]
failed    = [r for r in results if r.get("status") == "FAILED" and not r.get("job","").startswith("Recon_")]
recon_results = [r for r in results if r.get("job","").startswith("Recon_")]
total_rows = sum(r.get("rows", 0) for r in results if not r.get("job","").startswith("Recon_"))

print(f"\n{'='*60}")
print(f"ORCHESTRATION COMPLETE")
print(f"{'='*60}")
print(f"  Succeeded : {len(succeeded)} / {len(succeeded) + len(failed)}")
print(f"  Failed    : {len(failed)} / {len(succeeded) + len(failed)}")
print(f"  Total Rows: {total_rows:,}")

if failed:
    print(f"\nFailed jobs:")
    for f_item in failed:
        print(f"   {f_item['job']}: {f_item.get('error','unknown')}")

error_details = [f"{f_item['job']}: {f_item.get('error','unknown')[:200]}" for f_item in failed]

exit_payload = json.dumps({
    "status":     "COMPLETED" if not failed else "PARTIAL",
    "succeeded":  len(succeeded),
    "failed":     len(failed),
    "total_rows": total_rows,
    "groups":     len(groups),
    "errors":     error_details,
})

# Execution Logging
print(f"\nSaving execution log to {LOG_CATALOG}.{LOG_SCHEMA}.{LOG_TABLE}...")
try:
    log_json = dbutils.notebook.run(
        f"{WORKSPACE_PATH}/05_Meta_ExecutionLog",
        timeout_seconds=600,
        arguments={
            "catalog":      CATALOG,
            "schema":       SCHEMA,
            "log_catalog":  LOG_CATALOG,
            "log_schema":   LOG_SCHEMA,
            "log_table":    LOG_TABLE,
            "results_json": json.dumps(results),
            "groups_json":  json.dumps([{"group_id": g["group_id"], "full_table": g["full_table"], "load_type": g.get("load_type","full")} for g in groups]),
            "orchestrator_status": "COMPLETED" if not failed else "PARTIAL",
        }
    )
    print(f"   Execution log saved")
except Exception as log_err:
    print(f"   Execution logging failed (non-blocking): {log_err}")

dbutils.notebook.exit(exit_payload)
