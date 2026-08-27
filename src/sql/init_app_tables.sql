-- ============================================================================
-- Delta tables for Migration Studio app persistence
-- Run once per environment to bootstrap the app schema.
--
-- Usage:
--   Replace ${catalog} and ${schema} with your target values, e.g.:
--     catalog = admin_source
--     schema  = migration_app
-- ============================================================================

CREATE CATALOG IF NOT EXISTS ${catalog};
CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema};

-- Migration job state (pipeline configurations and status)
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.migration_jobs (
    job_id      STRING NOT NULL COMMENT 'Unique migration job identifier',
    payload     STRING NOT NULL COMMENT 'JSON blob with full job configuration and state',
    updated_by  STRING          COMMENT 'Email of user who last modified this job',
    updated_at  TIMESTAMP DEFAULT current_timestamp() COMMENT 'Last modification timestamp'
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'Migration pipeline job definitions and state';

-- Data model cache (star/snowflake schema definitions)
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.dm_models (
    model_id    STRING NOT NULL COMMENT 'Unique data model identifier',
    payload     STRING NOT NULL COMMENT 'JSON blob with model schema, ER diagram, DDL',
    updated_by  STRING          COMMENT 'Email of user who last modified this model',
    updated_at  TIMESTAMP DEFAULT current_timestamp()
) USING DELTA
COMMENT 'Data modeling cache — star/snowflake schema definitions';

-- Application configuration (replaces deployconfig.json)
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.app_config (
    config_key   STRING NOT NULL COMMENT 'Configuration key (e.g., source.server, databricks_host)',
    config_value STRING          COMMENT 'JSON-encoded configuration value',
    updated_by   STRING          COMMENT 'Email of user who last changed this setting',
    updated_at   TIMESTAMP DEFAULT current_timestamp()
) USING DELTA
COMMENT 'Runtime application configuration — key/value store';

-- Audit trail (tracks all user actions)
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.audit_log (
    event_id        STRING NOT NULL COMMENT 'Unique event UUID',
    user_email      STRING          COMMENT 'Email of the user who performed the action',
    user_name       STRING          COMMENT 'Display name of the user',
    action          STRING          COMMENT 'Action performed (e.g., POST /api/v1/workflow/run)',
    resource_type   STRING          COMMENT 'Type of resource affected (api, pipeline, model, etc.)',
    resource_id     STRING          COMMENT 'Identifier of the affected resource',
    details_json    STRING          COMMENT 'JSON blob with additional action details',
    ip_address      STRING          COMMENT 'Client IP address',
    response_status INT             COMMENT 'HTTP response status code',
    created_at      TIMESTAMP DEFAULT current_timestamp()
) USING DELTA
TBLPROPERTIES (
    'delta.logRetentionDuration' = 'interval 90 days',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'User action audit trail — 90-day retention';

-- Job schedules (replaces job_schedules.json)
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.job_schedules (
    schedule_id   STRING NOT NULL COMMENT 'Unique schedule identifier',
    schedule_data STRING NOT NULL COMMENT 'JSON blob with schedule configuration',
    is_active     BOOLEAN DEFAULT true COMMENT 'Whether the schedule is currently active',
    created_by    STRING          COMMENT 'Email of user who created this schedule',
    updated_at    TIMESTAMP DEFAULT current_timestamp()
) USING DELTA
COMMENT 'Pipeline job schedule definitions';

-- User role assignments (replaces users.json)
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.user_roles (
    user_email   STRING NOT NULL COMMENT 'User email address (Entra ID / workspace identity)',
    role         STRING NOT NULL COMMENT 'App role: Admin, Developer, or Viewer',
    display_name STRING          COMMENT 'User display name',
    assigned_by  STRING          COMMENT 'Admin who assigned this role',
    updated_at   TIMESTAMP DEFAULT current_timestamp()
) USING DELTA
COMMENT 'Application-level role assignments for RBAC';

-- ── Grants ──────────────────────────────────────────────────────────────────
-- Uncomment and adjust group names for your workspace:
-- GRANT USAGE ON CATALOG ${catalog} TO `migration-studio-users`;
-- GRANT USAGE ON SCHEMA ${catalog}.${schema} TO `migration-studio-users`;
-- GRANT SELECT ON SCHEMA ${catalog}.${schema} TO `migration-studio-viewers`;
-- GRANT SELECT, MODIFY ON SCHEMA ${catalog}.${schema} TO `migration-studio-developers`;
-- GRANT ALL PRIVILEGES ON SCHEMA ${catalog}.${schema} TO `migration-studio-admins`;
