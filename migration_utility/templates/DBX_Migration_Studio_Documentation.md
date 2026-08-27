# DBX Migration Studio — Complete Documentation

## Overview

**DBX Migration Studio** is an end-to-end SQL-to-Databricks migration accelerator built as a Databricks Native App. It automates the full journey of migrating a SQL Server data estate into the Databricks Lakehouse — from discovery and conversion through to production pipelines and data validation.

**App Name:** dbxmigrator  
**Runtime:** Databricks Apps (Flask + Gunicorn)  
**Cloud:** Azure  
**Source:** Azure SQL Server  
**Target:** Databricks Lakehouse (Unity Catalog + Delta Lake)  

---

## Architecture

```
SQL Server (source)
      │
      ▼
Azure Data Lake Storage Gen2 (adlssqltodatabrickspoc)
      │
      ▼  (Auto Loader / Change Data Feed)
Bronze Layer  ──→  Silver Layer  ──→  Gold Layer
  (raw ingest)    (cleaned data)    (business models)
      │
      ▼
Unity Catalog (admin_source.configtables)
      │
      ▼
Migration Studio App  ←→  Genie AI Assistant
```

### Tech Stack

| Component | Technology |
|---|---|
| Frontend | Flask + Vanilla JS (SPA on Databricks Apps) |
| Backend | Python / Flask blueprints (17 route modules) |
| Pipelines | Lakeflow Spark Declarative Pipelines with Auto Loader |
| Orchestration | Lakeflow Jobs |
| Catalog | Unity Catalog with Delta Lake tables |
| Storage | Azure Data Lake Storage Gen2 |
| Secrets | Azure Key Vault + Databricks Secret Scopes |
| AI Assistant | Databricks Genie Conversations API |
| Containerization | Docker (Gunicorn, 2 workers, 8 threads) |

---

## App Configuration

### Environment Variables (app.yml)

| Variable | Purpose |
|---|---|
| DATABRICKS_CATALOG | Target catalog (admin_source) |
| DATABRICKS_SCHEMA | Target schema (migration_app) |
| DATABRICKS_SECRET_SCOPE | Secret scope name (migration-studio) |
| CLOUD_PROVIDER | Cloud platform (azure) |
| DATABRICKS_HOST | Workspace URL |
| DATABRICKS_HTTP_PATH | SQL Warehouse path |
| DATABRICKS_SQL_WAREHOUSE_ID | SQL Warehouse ID |
| RUNNING_ON_DBX_APPS | Flag for Databricks Apps runtime |

### Deployment Configuration (deployconfig.json)

| Setting | Value |
|---|---|
| Key Vault | kv-dbxmigrator-west3 |
| Region | westus3 |
| Storage Account | adlssqltodatabrickspoc |
| Container | datalake |
| Access Connector | acsqltodatabrickspoc |
| Storage Credential | credsqltodbxpoc |
| Source DB Server | poc-az-sqlserver-rksandbox-db.database.windows.net |
| Source Database | pocaiacceldb |
| DevOps Org | EMEA-SalesOps |
| DevOps Project | AI Accelerator |

---

## Modules & Features

### 1. Discovery Module

**Purpose:** Scan and analyze SQL Server databases to inventory all objects.

**Capabilities:**
- Scans tables, stored procedures, views, and UDFs
- Assigns complexity scores (1-5) to each object
- Builds dependency graphs (D3.js visualization)
- Generates HTML reports and Bill of Materials (BOM) CSV
- Data profiling: column-level statistics, null rates, suggested DQ rules
- Supports both live database scanning and static object analysis

**API Endpoints:**
- `POST /api/v1/discovery/scan` — Run discovery scan
- `GET /api/v1/discovery/results` — Get cached results
- `GET /api/v1/discovery/object/<name>` — Object detail
- `GET /api/v1/discovery/dependency-graph` — D3 graph JSON
- `GET /api/v1/discovery/export/html` — Download HTML report
- `GET /api/v1/discovery/export/bom` — Download BOM CSV
- `POST /api/v1/discovery/profile/tables` — List profilable tables
- `POST /api/v1/discovery/profile/<table>` — Profile a table
- `GET /api/v1/discovery/profile/<table>/rules` — Get suggested DQ rules

---

### 2. Convert to PySpark Module

**Purpose:** AI-powered T-SQL to PySpark/Databricks notebook conversion.

**Capabilities:**
- Converts stored procedures → .py notebooks
- Converts views → DataFrame transformations
- Converts UDFs → shared HelperFunction.py
- Single object, multi-object, and batch conversion modes
- Generates optimized PySpark code using AI

**API Endpoints:**
- `GET /api/v1/stored-procedures` — List all procedures
- `GET /api/v1/sp-code/<sp_name>` — Get SP source code
- `POST /api/v1/convert` — Convert single SP to PySpark
- `GET /api/v1/all-objects` — List all objects (SPs, views, UDFs)
- `GET /api/v1/object-code/<obj_name>` — Get object source
- `POST /api/v1/convert-multi` — Combined conversion
- `POST /api/v1/convert-separate` — Separate notebook per object

---

### 3. MetadataFlow Module

**Purpose:** Provision Unity Catalog schemas and Delta metadata tables.

**Creates:**
- `admin_source.configtables.wf_job_metadata` — Registered migration jobs
- `admin_source.configtables.wf_pipeline_metadata` — Pipeline definitions
- `admin_source.configtables.wf_run_history` — Pipeline run history
- `admin_source.configtables.wf_scheduler_config` — Cron schedules
- `admin_source.configtables.wf_source_tables` — Discovered source tables
- `admin_source.configtables.wf_watermark_metadata` — Incremental watermarks

**API Endpoints:**
- `POST /api/v1/workflow/metadata/init` — Initialize metadata flow
- `POST /api/v1/workflow/auto-init` — Auto-initialize from config
- `GET /api/v1/workflow/metadata/status` — Check status
- `POST /api/v1/workflow/metadata/load` — Load from Databricks
- `POST /api/v1/workflow/metadata/sync` — Sync to Databricks (async)
- `GET /api/v1/workflow/metadata/sync-status/<task_id>` — Poll sync status
- `POST /api/v1/workflow/metadata/save-sources` — Save source tables

---

### 4. Pipeline Studio Module

**Purpose:** Create and manage Bronze/Silver/Gold medallion pipelines.

**Pipeline Types:**
- **Bronze** — Raw ingestion using Auto Loader and Change Data Feed
- **Silver** — Cleaned, deduplicated, enriched data
- **Gold** — Business aggregations and models

**Capabilities:**
- Table selection from source SQL Server
- Automatic pipeline notebook generation
- Lakeflow Spark Declarative Pipeline creation
- CDC support (change tracking, watermark)
- Deploy notebooks to Databricks workspace

**API Endpoints:**
- `POST /api/v1/workflow/list-tables` — List source tables
- `POST /api/v1/workflow/notebooks/deploy` — Deploy notebooks
- `GET /api/v1/workflow/notebooks/status` — Deployment status
- `POST /api/v1/workflow/notebooks/generate` — Generate without deploying

---

### 5. Job Manager & Scheduler Module

**Purpose:** Create, monitor, and schedule Lakeflow Jobs.

**Scheduling Types:**
- Cron expressions (e.g., `0 2 * * *`)
- Interval-based (every N hours)
- One-time execution (specific datetime)

**Tracked in:** `admin_source.configtables.wf_scheduler_config`

---

### 6. Source Connection Module

**Purpose:** Test and manage connections to source SQL Server databases.

**Connection Methods:**
1. Direct pymssql/pyodbc connection
2. Databricks SQL Warehouse JDBC fallback
3. TCP connectivity test for network diagnostics

**Supported Sources:** Azure SQL, Synapse, SQL Server

**API Endpoints:**
- `POST /api/v1/source/test-connection` — Test connectivity
- `POST /api/v1/source/load-objects` — Load database objects

---

### 7. Self-Healing Bot Module

**Purpose:** Intelligent failure detection, diagnosis, and auto-recovery.

**Capabilities:**
- Health checks: SQL Server, Databricks API, Azure Storage, Unity Catalog, pipelines, secrets
- Error diagnosis with AI-powered root cause analysis
- Automatic remediation actions
- Job run monitoring with auto-heal
- Restore point management
- Configurable healing rules (toggle on/off)

**API Endpoints:**
- `POST /api/v1/healer/health-check` — Run system health check
- `POST /api/v1/healer/diagnose` — Diagnose an error
- `POST /api/v1/healer/heal` — Execute healing action
- `POST /api/v1/healer/monitor/start` — Start run monitoring
- `POST /api/v1/healer/monitor/check/<id>` — Check monitor status
- `GET /api/v1/healer/monitors` — List active monitors
- `GET /api/v1/healer/recent-runs` — Recent job runs
- `POST /api/v1/healer/restore-point` — Create restore point
- `GET /api/v1/healer/restore-points` — List restore points
- `GET /api/v1/healer/rules` — Get healing rules
- `POST /api/v1/healer/rules/toggle` — Toggle a rule

---

### 8. Reports & Analytics Module

**Purpose:** Migration dashboards, progress charts, and exportable reports.

---

### 9. Reconciliation Module

**Purpose:** Compare source vs target data after migration.

**Checks:**
- Row count comparison
- Numeric aggregate sums
- NULL value differences
- Variance percentage calculation

**Results stored in:** `reconciliation.hr.reconcilationdetails`

---

### 10. Data Quality Module

**Purpose:** Validate data completeness, accuracy, consistency, and freshness.

**Features:**
- Per-column quality scorecard
- Overall quality score
- Failed checks logged to audit_log
- Suggested DQ rules from profiling

---

### 11. Schema Comparison Module

**Purpose:** Detect column-level type and nullability drift between source and target.

**Features:**
- Side-by-side diff of SQL Server schema vs Databricks schema
- Exportable as CSV

---

### 12. Audit & Compliance Module

**Purpose:** Full action history tracking.

**Tracks:** Login, settings change, pipeline create/run, deployment, user management actions.

**Stored in:** `admin_source.migration_app.audit_log`

**Fields:** user_email, action, entity, timestamp, details

---

### 13. User Management (RBAC)

**Roles:**

| Role | Permissions |
|---|---|
| Admin | Full access: manage users, settings, run migrations |
| Operator | Run pipelines and jobs; cannot change settings |
| Viewer | Read-only: view dashboards and reports |

**Stored in:** `admin_source.migration_app.user_roles`

---

### 14. Genie AI Assistant Module

**Purpose:** Natural language Q&A over all connected data.

**Capabilities:**
- FAQ knowledge base for app-level questions
- Proxies data questions to Databricks Genie Conversations API
- Context preamble injection for accurate answers
- Conversation management (create, follow-up, polling)
- Custom FAQ store management

**Connected Genie Space:** DBX Migration — Full Workspace (ID: 01f1871469af1a4c858f4c7ac661634c)

---

### 15. Data Modeling Module

**Purpose:** Manage data models and relationships.

---

### 16. DevOps Integration

**Purpose:** Azure DevOps integration for CI/CD.

**Configuration:**
- Organization: EMEA-SalesOps
- Project: AI Accelerator
- Repository: AI Accelerator
- Branch: main
- Reviewers: Sunil.Kumar@insight.com

---

## Connected Catalogs & Tables

### Migration Control — admin_source

| Schema | Table | Purpose |
|---|---|---|
| configtables | wf_job_metadata | Registered migration jobs (source_table, target_table, status, row_count, error_msg) |
| configtables | wf_pipeline_metadata | Pipeline definitions (catalog, schema, layer, pipeline_id, pipeline_type) |
| configtables | wf_run_history | Every pipeline run (job_name, start_time, end_time, rows_read, rows_written, status, duration_mins) |
| configtables | wf_scheduler_config | Cron schedules (job_name, schedule, enabled, next_run) |
| configtables | wf_source_tables | Source tables discovered in SQL Server (schema, table_name, row_count, complexity_score) |
| configtables | wf_watermark_metadata | Incremental watermarks (table_name, last_loaded_value, column_name) |
| migration_app | migration_jobs | Live job tracker (job_id, source, target, state, started_at, finished_at, rows_migrated, error) |
| migration_app | audit_log | Every user action (user_email, action, entity, timestamp, details) |
| migration_app | user_roles | RBAC (user_email, role, granted_by, granted_at) |

### Bronze Layer — bronze.hr

Raw ingested HR/business data:
- bronze_customers
- bronze_products
- bronze_categories
- bronze_stores
- bronze_fact_sales_orders
- bronze_invoices
- bronze_payments
- bronze_dimemployee
- bronze_dimdepartment
- bronze_dimjobrole

### Silver Layer — silver.hr

Cleaned and enriched data:
- customers
- products
- stores
- fact_sales_orders
- invoices
- payments
- dimemployee
- dimdepartment
- dimjobrole
- dimlocation

### Operations

| Catalog | Schema | Table | Purpose |
|---|---|---|---|
| loggingdetails | hr | executionlog | Pipeline execution logs |
| reconciliation | hr | reconcilationdetails | Source vs target row-count reconciliation |

### Samples

| Catalog | Schema | Table | Purpose |
|---|---|---|---|
| samples | nyctaxi | trips | NYC taxi data (fare, tip, distance, pickup/dropoff) |
| samples | tpch | orders | TPC-H benchmark orders |

---

## ETL Pipeline Notebooks

Located at: `/apps/dbxmigrator/src/notebooks/`

| Notebook | Purpose |
|---|---|
| 00_Setup_Secrets | Configure Databricks secret scopes and Azure Key Vault |
| 00_Meta_Orchestrator | Master orchestrator for metadata-driven pipeline |
| 01_Landing_Zone | Ingest raw files to landing zone |
| 01_Meta_Extract | Metadata-driven extraction from source |
| 02_Bronze | Raw data ingestion to Bronze layer |
| 02_Meta_Bronze | Metadata-driven Bronze ingestion |
| 03_Silver | Data transformation to Silver layer |
| 03_Meta_Silver | Metadata-driven Silver processing |
| 04_Meta_Reconciliation | Source vs target reconciliation |
| 05_Meta_ExecutionLog | Pipeline execution logging |

---

## Data Flow (Medallion Architecture)

```
1. SQL Server → Landing Zone (ADLS Gen2)
   - Full load or incremental (watermark/CDC)
   - Stored in /dev/landing/

2. Landing Zone → Bronze (raw)
   - Auto Loader ingestion
   - Schema: bronze.hr
   - Location: /dev/uc-managed/bronze/

3. Bronze → Silver (cleaned)
   - Deduplication, type casting, null handling
   - Schema: silver.hr
   - Location: /dev/uc-managed/silver/

4. Cross-cutting:
   - Reconciliation: row count comparison
   - Execution logging: timing, rows processed
   - Watermark tracking: incremental state
```

---

## Storage Layout (ADLS Gen2)

**Account:** adlssqltodatabrickspoc  
**Container:** datalake

| Path | Purpose |
|---|---|
| dev/landing | Raw files from SQL Server |
| dev/uc-managed/bronze | Bronze Delta tables |
| dev/uc-managed/silver | Silver Delta tables |
| dev/uc-managed/admin_source | Metadata/config tables |
| dev/uc-managed/reconciliation | Recon results |
| dev/uc-managed/logging | Execution logs |

---

## Security & Authentication

- **App Auth:** Databricks proxy-based authentication (DATABRICKS_CLIENT_ID/SECRET)
- **Identity:** User email extracted from proxy headers on every request
- **Secrets:** Azure Key Vault (kv-dbxmigrator-west3) + Databricks Secret Scope (migration-studio)
- **RBAC:** Admin/Operator/Viewer roles enforced at route level
- **Session:** HTTP-only cookies with SameSite=Lax

---

## Getting Started (5-Step Workflow)

**Step 1 — Configure** (Settings tab)  
Set Azure SQL Server connection, Databricks host, storage account, and Unity Catalog targets.

**Step 2 — Discover** (Discovery tab)  
Scan SQL Server to inventory all tables, stored procedures, views, and UDFs. Review complexity scores and dependency graph.

**Step 3 — Convert** (Convert to PySpark tab)  
Select discovered objects and click Convert. The app auto-generates PySpark notebooks for each stored procedure/view.

**Step 4 — Deploy Pipelines** (MetadataFlow → Pipeline Studio → Job Manager)  
Provision metadata schema, create Bronze/Silver pipelines, and schedule jobs.

**Step 5 — Validate** (Reconciliation + Data Quality)  
Compare source vs target row counts, check data quality metrics, and export compliance reports.

---

## API Route Summary

| Blueprint | URL Prefix | Purpose |
|---|---|---|
| auth | /api/v1 | Authentication & login |
| pages | / | HTML page serving (index, help, BOM) |
| convert | /api/v1 | SP/View/UDF conversion |
| databricks | /api/v1 | Databricks workspace operations |
| source | /api/v1 | Source DB connection & objects |
| healer | /api/v1 | Self-healing bot |
| workflow | /api/v1 | Pipeline workflows & metadata |
| scheduler | /api/v1 | Job scheduling |
| reports | /api/v1 | Reports & analytics |
| schema | /api/v1 | Schema comparison |
| settings | /api/v1 | App settings management |
| datamodel | /api/v1 | Data modeling |
| admin | /api/v1 | Admin operations |
| discovery | /api/v1 | Discovery scanning |
| genie | / | Genie AI chat proxy |

---

## Dependencies (requirements.txt)

| Package | Purpose |
|---|---|
| flask >= 3.0.0 | Web framework |
| gunicorn >= 21.2.0 | Production WSGI server |
| requests >= 2.31.0 | HTTP client |
| pyodbc >= 4.0.39 | SQL Server ODBC driver |
| pymssql >= 2.2.11 | SQL Server native driver |
| pyarrow >= 14.0.0 | Columnar data format |
| databricks-sdk >= 0.30.0 | Databricks SDK |
| databricks-sql-connector >= 3.1.0 | SQL Warehouse connector |
| flask-compress >= 1.15 | Response compression |
| azure-identity >= 1.15.0 | Azure AD authentication |
| azure-mgmt-storage >= 21.0.0 | Storage management |
| azure-mgmt-resource >= 23.0.0 | Resource management |
| azure-mgmt-databricks >= 2.0.0 | Databricks management |
| azure-mgmt-authorization >= 4.0.0 | RBAC management |
| azure-storage-file-datalake >= 12.14.0 | ADLS Gen2 operations |

---

## Key Source Files

| File | Purpose |
|---|---|
| app.py | Flask application factory, blueprint registration |
| app.yml | Databricks Apps deployment config |
| Dockerfile | Container build definition |
| databricks.yml | Declarative Automation Bundle config |
| deployconfig.json | Infrastructure & catalog configuration |
| requirements.txt | Python dependencies |
| databricks_connector.py | Databricks REST API client |
| workflow_manager.py | Pipeline & job orchestration logic |
| discovery_agent.py | Source database scanning engine |
| data_migrator.py | Data transfer orchestration |
| data_profiler.py | Column-level data profiling |
| sp_converter.py | T-SQL to PySpark AI conversion |
| self_healing_bot.py | Auto-recovery engine |
| unity_catalog_executor.py | UC DDL operations |
| persistence.py | Delta table state management |
| identity.py | User identity from proxy headers |
| audit.py | Action logging hooks |
| secrets_helper.py | Secret scope operations |
| keyvault_helper.py | Azure Key Vault integration |
| cloud_provider.py | Azure resource provisioning |
| devops_connector.py | Azure DevOps integration |
| metadata_notebooks.py | Notebook code generation |
| medallion_notebooks.py | Medallion pipeline generation |

---

## Example Genie AI Questions

**Migration & Pipelines:**
- "How many jobs completed vs failed?"
- "Show failed pipeline runs with errors"
- "Which pipelines ran more than 2 hours?"
- "Which source tables are still pending?"

**Business Data:**
- "Total sales by product category"
- "Top 10 customers by order value"
- "Employee headcount by department"

**Operations:**
- "Show audit log entries from today"
- "Which users have admin roles?"
- "Compare bronze vs silver row counts"
- "Show latest watermark values"

**Sample Data:**
- "Average NYC taxi fare by hour"
- "TPC-H revenue by nation"

---

## Troubleshooting

| Issue | Resolution |
|---|---|
| Connection timeout to SQL Server | Check firewall rules, ensure IP is allow-listed in Azure SQL |
| ODBC driver not available | App falls back to SQL Warehouse JDBC automatically |
| Database paused (serverless) | Auto-pause detected — retry in 30 seconds |
| TLS/SSL handshake failed | Set Encrypt=yes;TrustServerCertificate=yes |
| Login failed (18456) | Check username/password in settings |
| No SQL Warehouse available | Ensure at least one warehouse is running |
| Pipeline fails | Use Self-Healing Bot → Health Check → Auto-Recover |
| Missing metadata tables | Run MetadataFlow initialization first |

---

*Document generated from source code analysis of the dbxmigrator Databricks App.*  
*Last updated: July 2026*
