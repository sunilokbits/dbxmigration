-- ============================================================================
-- Metadata Tables DDL for Workflow Manager
-- ============================================================================
-- Run this SQL on a Databricks SQL Warehouse to initialise the 5 metadata
-- tables used by the metadata-driven medallion pipeline.
--
-- Replace ${catalog} and ${schema} with your actual values, e.g.:
--   admin_source.Configtables
-- ============================================================================

-- 1. Pipeline metadata (one row per source table)
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.wf_pipeline_metadata (
    group_id         STRING NOT NULL,
    table_schema     STRING,
    table_name       STRING,
    full_table       STRING,
    load_type        STRING DEFAULT 'full',
    source_type      STRING DEFAULT 'sqlserver',
    enabled          BOOLEAN DEFAULT true,
    created_at       TIMESTAMP DEFAULT current_timestamp(),
    updated_at       TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;

-- 2. Job metadata (multiple rows per pipeline group: extract, bronze, silver)
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.wf_job_metadata (
    job_id           STRING NOT NULL,
    group_id         STRING NOT NULL,
    job_name         STRING,
    stage            STRING,
    job_order        INT,
    table_schema     STRING,
    table_name       STRING,
    full_table       STRING,
    load_type        STRING DEFAULT 'full',
    watermark_column STRING,
    source_config    STRING,
    target_config    STRING,
    enabled          BOOLEAN DEFAULT true,
    status           STRING DEFAULT 'pending',
    last_status      STRING,
    last_run_id      STRING,
    last_run_at      TIMESTAMP,
    run_count        INT DEFAULT 0,
    fail_count       INT DEFAULT 0,
    created_at       TIMESTAMP DEFAULT current_timestamp(),
    updated_at       TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;

-- 3. Run history (one row per job execution)
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.wf_run_history (
    run_id           STRING NOT NULL,
    job_id           STRING NOT NULL,
    job_name         STRING,
    stage            STRING,
    full_table       STRING,
    load_type        STRING,
    watermark_column STRING,
    status           STRING DEFAULT 'pending',
    rows_processed   BIGINT DEFAULT 0,
    error_message    STRING,
    started_at       TIMESTAMP DEFAULT current_timestamp(),
    completed_at     TIMESTAMP,
    duration_sec     DOUBLE DEFAULT 0
) USING DELTA;

-- 4. Watermark metadata (one row per source table for incremental loads)
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.wf_watermark_metadata (
    table_name       STRING NOT NULL,
    watermark_column STRING,
    last_value       STRING,
    updated_at       TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;

-- 5. Source tables (discovered tables from source database)
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.wf_source_tables (
    table_schema     STRING,
    table_name       STRING,
    full_table       STRING,
    row_count        BIGINT DEFAULT 0,
    discovered_at    TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;
