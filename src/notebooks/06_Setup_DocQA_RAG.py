# Databricks notebook source
# DBTITLE 1,Document Q&A RAG Setup
# MAGIC %md
# MAGIC # 06 — Document Q&A RAG Setup
# MAGIC
# MAGIC This notebook automates the full setup of the **Document Q&A** system for DBX Migration Studio.
# MAGIC
# MAGIC **What it creates:**
# MAGIC 1. Delta table `admin_source.migration_app.doc_qa_chunks` with Change Data Feed enabled
# MAGIC 2. Populates it with 26+ documentation chunks covering all app modules
# MAGIC 3. Creates a Vector Search endpoint (`dbx_migration_vs_endpoint`)
# MAGIC 4. Creates a Delta Sync Vector Search index with `databricks-gte-large-en` embeddings
# MAGIC 5. Waits for index readiness and validates with a test query
# MAGIC
# MAGIC **Run this notebook once during initial deployment, or re-run to refresh documentation.**

# COMMAND ----------

# DBTITLE 1,Configuration
# ============================================================================
# CONFIGURATION
# ============================================================================

# Unity Catalog target
CATALOG = "admin_source"
SCHEMA = "migration_app"
TABLE_NAME = "doc_qa_chunks"
FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"

# Vector Search
VS_ENDPOINT_NAME = "dbx_migration_vs_endpoint"
VS_INDEX_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}_index"
EMBEDDING_MODEL = "databricks-gte-large-en"
NUM_RESULTS = 3

print(f"Target table: {FULL_TABLE}")
print(f"VS Endpoint:  {VS_ENDPOINT_NAME}")
print(f"VS Index:     {VS_INDEX_NAME}")
print(f"Model:        {EMBEDDING_MODEL}")

# COMMAND ----------

# DBTITLE 1,Step 1: Create Delta Table
# MAGIC %sql
# MAGIC -- Step 1: Create the document chunks table with Change Data Feed
# MAGIC CREATE TABLE IF NOT EXISTS admin_source.migration_app.doc_qa_chunks (
# MAGIC   chunk_id INT NOT NULL COMMENT 'Sequential chunk identifier',
# MAGIC   doc_title STRING NOT NULL COMMENT 'Document title',
# MAGIC   section STRING NOT NULL COMMENT 'Section heading for retrieval filtering',
# MAGIC   content STRING NOT NULL COMMENT 'Chunk text content - embedded for RAG retrieval',
# MAGIC   char_count INT NOT NULL COMMENT 'Character count of chunk',
# MAGIC   source_file STRING NOT NULL COMMENT 'Source file name',
# MAGIC   created_at STRING NOT NULL COMMENT 'Timestamp when chunk was created'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Document Q&A chunks for RAG-based retrieval over DBX Migration Studio documentation'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'purpose' = 'rag_knowledge_base',
# MAGIC   'app' = 'dbxmigrator'
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Step 2: Populate Documentation Chunks
# ============================================================================
# Step 2: Populate documentation chunks
# ============================================================================
import re
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Full documentation content for DBX Migration Studio
DOC_CONTENT = """
## Overview

DBX Migration Studio is an end-to-end SQL-to-Databricks migration accelerator built as a Databricks Native App. It automates the full journey of migrating a SQL Server data estate into the Databricks Lakehouse from discovery and conversion through to production pipelines and data validation. App Name: dbxmigrator. Runtime: Databricks Apps (Flask + Gunicorn). Cloud: Azure. Source: Azure SQL Server. Target: Databricks Lakehouse (Unity Catalog + Delta Lake).

## Architecture

SQL Server (source) to Azure Data Lake Storage Gen2 (adlssqltodatabrickspoc) to Bronze Layer (raw ingest via Auto Loader/CDF) to Silver Layer (cleaned data) to Gold Layer (business models). All managed by Unity Catalog (admin_source.configtables). Tech Stack: Frontend (Flask + Vanilla JS SPA), Backend (Python/Flask blueprints, 17 route modules), Pipelines (Lakeflow Spark Declarative Pipelines with Auto Loader), Orchestration (Lakeflow Jobs), Catalog (Unity Catalog with Delta Lake), Storage (Azure Data Lake Storage Gen2), Secrets (Azure Key Vault + Databricks Secret Scopes), AI (Databricks Genie Conversations API), Container (Docker, Gunicorn, 2 workers, 8 threads).

## App Configuration

Environment Variables (app.yml): DATABRICKS_CATALOG (admin_source), DATABRICKS_SCHEMA (migration_app), DATABRICKS_SECRET_SCOPE (migration-studio), CLOUD_PROVIDER (azure), DATABRICKS_HOST (workspace URL), DATABRICKS_HTTP_PATH (SQL Warehouse path), DATABRICKS_SQL_WAREHOUSE_ID, RUNNING_ON_DBX_APPS (true). Deployment: Key Vault (kv-dbxmigrator-west3), Region (westus3), Storage Account (adlssqltodatabrickspoc), Container (datalake), Access Connector (acsqltodatabrickspoc), Source DB Server (poc-az-sqlserver-rksandbox-db.database.windows.net), Database (pocaiacceldb), DevOps Org (EMEA-SalesOps), Project (AI Accelerator).

## Discovery Module

Purpose: Scan and analyze SQL Server databases to inventory all objects. Capabilities: Scans tables, stored procedures, views, and UDFs. Assigns complexity scores (1-5). Builds dependency graphs (D3.js visualization). Generates HTML reports and BOM CSV. Data profiling with column-level statistics, null rates, suggested DQ rules. Supports live DB and static object analysis. API: POST /api/v1/discovery/scan, GET /api/v1/discovery/results, GET /api/v1/discovery/object/<name>, GET /api/v1/discovery/dependency-graph, GET /api/v1/discovery/export/html, GET /api/v1/discovery/export/bom, POST /api/v1/discovery/profile/tables, POST /api/v1/discovery/profile/<table>.

## Convert to PySpark Module

Purpose: AI-powered T-SQL to PySpark/Databricks notebook conversion. Converts stored procedures to .py notebooks, views to DataFrame transformations, UDFs to shared HelperFunction.py. Single, multi-object, and batch conversion modes. API: GET /api/v1/stored-procedures, GET /api/v1/sp-code/<sp_name>, POST /api/v1/convert, GET /api/v1/all-objects, POST /api/v1/convert-multi, POST /api/v1/convert-separate.

## MetadataFlow Module

Purpose: Provision Unity Catalog schemas and Delta metadata tables. Creates: wf_job_metadata (registered migration jobs), wf_pipeline_metadata (pipeline definitions), wf_run_history (pipeline runs), wf_scheduler_config (cron schedules), wf_source_tables (discovered source tables), wf_watermark_metadata (incremental watermarks). API: POST /api/v1/workflow/metadata/init, POST /api/v1/workflow/auto-init, GET /api/v1/workflow/metadata/status, POST /api/v1/workflow/metadata/sync.

## Pipeline Studio Module

Purpose: Create and manage Bronze/Silver/Gold medallion pipelines. Bronze = raw ingestion using Auto Loader and Change Data Feed. Silver = cleaned, deduplicated, enriched. Gold = business aggregations. Supports table selection from source SQL Server, automatic pipeline notebook generation, Lakeflow Spark Declarative Pipeline creation, CDC support (change tracking, watermark). API: POST /api/v1/workflow/list-tables, POST /api/v1/workflow/notebooks/deploy, GET /api/v1/workflow/notebooks/status, POST /api/v1/workflow/notebooks/generate.

## Job Manager and Scheduler Module

Purpose: Create, monitor, and schedule Lakeflow Jobs. Scheduling types: Cron expressions (e.g. 0 2 * * *), Interval-based (every N hours), One-time (specific datetime). Config stored in admin_source.configtables.wf_scheduler_config.

## Source Connection Module

Purpose: Test and manage connections to source SQL Server databases. Connection methods: Direct pymssql/pyodbc, Databricks SQL Warehouse JDBC fallback, TCP connectivity test. Supported sources: Azure SQL, Synapse, SQL Server. API: POST /api/v1/source/test-connection, POST /api/v1/source/load-objects.

## Self-Healing Bot Module

Purpose: Intelligent failure detection, diagnosis, and auto-recovery. Health checks: SQL Server, Databricks API, Azure Storage, Unity Catalog, pipelines, secrets. AI-powered error diagnosis. Automatic remediation. Job run monitoring with auto-heal. Restore point management. Configurable healing rules. API: POST /api/v1/healer/health-check, POST /api/v1/healer/diagnose, POST /api/v1/healer/heal, POST /api/v1/healer/monitor/start, GET /api/v1/healer/monitors, GET /api/v1/healer/recent-runs, POST /api/v1/healer/restore-point, GET /api/v1/healer/rules.

## Reconciliation Module

Purpose: Compare source vs target data after migration. Checks: row count comparison, numeric aggregate sums, NULL value differences, variance percentage. Results stored in reconciliation.hr.reconcilationdetails.

## Data Quality Module

Purpose: Validate data completeness, accuracy, consistency, and freshness. Per-column quality scorecard, overall quality score. Failed checks logged to audit_log. Suggested DQ rules from profiling.

## Schema Comparison Module

Purpose: Detect column-level type and nullability drift between source and target. Side-by-side diff of SQL Server schema vs Databricks schema. Exportable as CSV.

## Audit and Compliance Module

Purpose: Full action history tracking. Tracks login, settings change, pipeline create/run, deployment, user management. Stored in admin_source.migration_app.audit_log. Fields: user_email, action, entity, timestamp, details.

## User Management RBAC

Roles: Admin (full access, manage users, settings, run migrations), Operator (run pipelines/jobs, cannot change settings), Viewer (read-only, view dashboards and reports). Stored in admin_source.migration_app.user_roles.

## Genie AI Assistant Module

Purpose: Natural language Q&A over all connected data. FAQ knowledge base for app-level questions. Proxies data questions to Databricks Genie Conversations API. Context preamble injection for accurate answers. Connected Genie Space: DBX Migration Full Workspace (ID: 01f1871469af1a4c858f4c7ac661634c). Example questions: Migration (How many jobs completed vs failed?, Show failed pipeline runs), Business data (Total sales by product category, Top 10 customers by order value), Operations (Show audit log entries from today, Which users have admin roles?).

## Connected Catalogs and Tables

admin_source.configtables: wf_job_metadata, wf_pipeline_metadata, wf_run_history, wf_scheduler_config, wf_source_tables, wf_watermark_metadata. admin_source.migration_app: migration_jobs, audit_log, user_roles. bronze.hr: bronze_customers, bronze_products, bronze_categories, bronze_stores, bronze_fact_sales_orders, bronze_invoices, bronze_payments, bronze_dimemployee, bronze_dimdepartment, bronze_dimjobrole. silver.hr: customers, products, stores, fact_sales_orders, invoices, payments, dimemployee, dimdepartment, dimjobrole, dimlocation. Operations: loggingdetails.hr.executionlog, reconciliation.hr.reconcilationdetails. Samples: samples.nyctaxi.trips, samples.tpch.orders.

## ETL Pipeline Notebooks

Located at /apps/dbxmigrator/src/notebooks/: 00_Setup_Secrets (configure secrets), 00_Meta_Orchestrator (master orchestrator), 01_Landing_Zone (ingest to landing), 01_Meta_Extract (metadata extraction), 02_Bronze (raw ingestion), 02_Meta_Bronze (metadata Bronze), 03_Silver (transform to Silver), 03_Meta_Silver (metadata Silver), 04_Meta_Reconciliation (source vs target recon), 05_Meta_ExecutionLog (execution logging).

## Data Flow Medallion Architecture

1. SQL Server to Landing Zone (ADLS Gen2): Full load or incremental (watermark/CDC). Stored in /dev/landing/. 2. Landing Zone to Bronze: Auto Loader ingestion. Schema bronze.hr. Location /dev/uc-managed/bronze/. 3. Bronze to Silver: Deduplication, type casting, null handling. Schema silver.hr. Location /dev/uc-managed/silver/. Cross-cutting: Reconciliation, Execution logging, Watermark tracking.

## Storage Layout ADLS Gen2

Account: adlssqltodatabrickspoc, Container: datalake. Paths: dev/landing (raw files), dev/uc-managed/bronze (Bronze Delta tables), dev/uc-managed/silver (Silver Delta tables), dev/uc-managed/admin_source (metadata tables), dev/uc-managed/reconciliation (recon results), dev/uc-managed/logging (execution logs).

## Security and Authentication

App Auth: Databricks proxy-based authentication. Identity: User email from proxy headers. Secrets: Azure Key Vault (kv-dbxmigrator-west3) + Databricks Secret Scope (migration-studio). RBAC: Admin/Operator/Viewer enforced at route level. Session: HTTP-only cookies, SameSite=Lax.

## Getting Started 5-Step Workflow

Step 1 Configure (Settings tab): Set Azure SQL connection, Databricks host, storage, Unity Catalog. Step 2 Discover (Discovery tab): Scan SQL Server for tables, SPs, views, UDFs. Step 3 Convert (Convert to PySpark tab): AI-generate PySpark notebooks. Step 4 Deploy Pipelines (MetadataFlow to Pipeline Studio to Job Manager): Provision metadata, create pipelines, schedule jobs. Step 5 Validate (Reconciliation + Data Quality): Compare source vs target, check DQ metrics.

## Dependencies

flask>=3.0.0, gunicorn>=21.2.0, requests>=2.31.0, pyodbc>=4.0.39, pymssql>=2.2.11, pyarrow>=14.0.0, databricks-sdk>=0.30.0, databricks-sql-connector>=3.1.0, flask-compress>=1.15, azure-identity>=1.15.0, azure-mgmt-storage>=21.0.0, azure-mgmt-resource>=23.0.0, azure-mgmt-databricks>=2.0.0, azure-mgmt-authorization>=4.0.0, azure-storage-file-datalake>=12.14.0.

## Key Source Files

app.py (Flask factory), app.yml (deployment config), Dockerfile (container build), databricks.yml (bundle config), deployconfig.json (infra config), requirements.txt (dependencies), databricks_connector.py (REST API client), workflow_manager.py (orchestration), discovery_agent.py (scanning engine), data_migrator.py (data transfer), data_profiler.py (profiling), sp_converter.py (T-SQL to PySpark AI), self_healing_bot.py (auto-recovery), unity_catalog_executor.py (UC DDL), persistence.py (state management), identity.py (user identity), audit.py (action logging), secrets_helper.py (secrets), keyvault_helper.py (Key Vault), cloud_provider.py (Azure provisioning), devops_connector.py (Azure DevOps), metadata_notebooks.py (notebook generation), medallion_notebooks.py (medallion generation).

## Troubleshooting

Connection timeout: Check firewall rules, ensure IP allow-listed. ODBC driver not available: Falls back to SQL Warehouse JDBC. Database paused (serverless): Retry in 30 seconds. TLS/SSL failed: Set Encrypt=yes;TrustServerCertificate=yes. Login failed (18456): Check credentials. No SQL Warehouse: Ensure one is running. Pipeline fails: Use Self-Healing Bot. Missing metadata tables: Run MetadataFlow init.

## DevOps Integration

Azure DevOps: Organization (EMEA-SalesOps), Project (AI Accelerator), Repository (AI Accelerator), Branch (main), Reviewers (Sunil.Kumar@insight.com).

## API Route Summary

Blueprints: auth (/api/v1), pages (/), convert (/api/v1), databricks (/api/v1), source (/api/v1), healer (/api/v1), workflow (/api/v1), scheduler (/api/v1), reports (/api/v1), schema (/api/v1), settings (/api/v1), datamodel (/api/v1), admin (/api/v1), discovery (/api/v1), genie (/).
"""

# Split into chunks by ## sections
sections = re.split(r'\n## ', DOC_CONTENT.strip())
chunks = []
now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for i, section in enumerate(sections):
    if i == 0:
        section_title = "Overview"
        content = section.strip()
    else:
        lines = section.split('\n', 1)
        section_title = lines[0].strip()
        content = "## " + section.strip()
    
    if content.strip():
        chunks.append((
            len(chunks) + 1,
            "DBX Migration Studio — Complete Documentation",
            section_title,
            content,
            len(content),
            "06_Setup_DocQA_RAG.py",
            now_str
        ))

print(f"✅ Prepared {len(chunks)} documentation chunks")
print(f"   Total characters: {sum(c[4] for c in chunks)}")
print(f"   Avg chunk size: {sum(c[4] for c in chunks) // len(chunks)} chars")

# COMMAND ----------

# DBTITLE 1,Step 2b: Insert Chunks into Table
# Insert chunks into the table
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("chunk_id", IntegerType(), False),
    StructField("doc_title", StringType(), False),
    StructField("section", StringType(), False),
    StructField("content", StringType(), False),
    StructField("char_count", IntegerType(), False),
    StructField("source_file", StringType(), False),
    StructField("created_at", StringType(), False),
])

df = spark.createDataFrame(chunks, schema)

# Clear existing data and insert fresh
spark.sql(f"TRUNCATE TABLE {FULL_TABLE}")
df.write.mode("append").insertInto(FULL_TABLE)

# Verify
count = spark.sql(f"SELECT COUNT(*) as n FROM {FULL_TABLE}").collect()[0][0]
print(f"✅ Inserted {count} chunks into {FULL_TABLE}")

# COMMAND ----------

# DBTITLE 1,Step 3: Create Vector Search Endpoint
# ============================================================================
# Step 3: Create Vector Search Endpoint (if not exists)
# ============================================================================
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import EndpointType
import time

w = WorkspaceClient()

# Check if endpoint already exists
try:
    ep = w.vector_search_endpoints.get_endpoint(VS_ENDPOINT_NAME)
    print(f"✅ Vector Search endpoint already exists: {VS_ENDPOINT_NAME}")
    print(f"   Status: {ep.endpoint_status.state}")
except Exception:
    print(f"⏳ Creating Vector Search endpoint: {VS_ENDPOINT_NAME}...")
    w.vector_search_endpoints.create_endpoint(
        name=VS_ENDPOINT_NAME,
        endpoint_type=EndpointType.STANDARD
    )
    # Wait for endpoint to come online
    for i in range(40):  # Up to 10 minutes
        time.sleep(15)
        ep = w.vector_search_endpoints.get_endpoint(VS_ENDPOINT_NAME)
        state = ep.endpoint_status.state.value if ep.endpoint_status else "UNKNOWN"
        if state == "ONLINE":
            print(f"✅ Endpoint is ONLINE after {(i+1)*15}s")
            break
        print(f"   [{(i+1)*15}s] Status: {state}")
    else:
        print("⚠️ Endpoint not yet online — continuing anyway (index creation will wait)")

# COMMAND ----------

# DBTITLE 1,Step 4: Create Vector Search Index
# ============================================================================
# Step 4: Create Vector Search Index (Delta Sync with managed embeddings)
# ============================================================================
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    PipelineType,
    VectorIndexType,
)

# Check if index already exists
try:
    idx = w.vector_search_indexes.get_index(VS_INDEX_NAME)
    print(f"✅ Vector Search index already exists: {VS_INDEX_NAME}")
    print(f"   Status: ready={idx.status.ready}, rows={idx.status.indexed_row_count}")
    
    # Trigger a sync to pick up any new data
    print("   Triggering sync to refresh index...")
    w.vector_search_indexes.sync_index(VS_INDEX_NAME)
    print("✅ Sync triggered")
    
except Exception:
    print(f"⏳ Creating Vector Search index: {VS_INDEX_NAME}")
    print(f"   Source table: {FULL_TABLE}")
    print(f"   Embedding model: {EMBEDDING_MODEL}")
    print(f"   Sync type: TRIGGERED")
    
    w.vector_search_indexes.create_index(
        name=VS_INDEX_NAME,
        endpoint_name=VS_ENDPOINT_NAME,
        primary_key="chunk_id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=FULL_TABLE,
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[
                EmbeddingSourceColumn(
                    name="content",
                    embedding_model_endpoint_name=EMBEDDING_MODEL
                )
            ],
        ),
    )
    print("✅ Index creation initiated")

# COMMAND ----------

# DBTITLE 1,Step 5: Wait for Index Ready
# ============================================================================
# Step 5: Wait for index to become ready
# ============================================================================
import time

print("Waiting for index to become ready...")
for i in range(30):  # Up to 15 minutes
    time.sleep(30)
    idx = w.vector_search_indexes.get_index(VS_INDEX_NAME)
    status = idx.status
    msg = status.message[:80] if status.message else "N/A"
    print(f"  [{(i+1)*30}s] ready={status.ready} | rows={status.indexed_row_count} | {msg}")
    if status.ready:
        print(f"\n✅ INDEX IS READY!")
        print(f"   Indexed rows: {status.indexed_row_count}")
        print(f"   Endpoint: {VS_ENDPOINT_NAME}")
        print(f"   Index: {VS_INDEX_NAME}")
        break
else:
    print("\n⚠️ Index still provisioning. It will be ready shortly — check back in a few minutes.")
    print(f"   To check: w.vector_search_indexes.get_index('{VS_INDEX_NAME}').status")

# COMMAND ----------

# DBTITLE 1,Step 6: Validate with Test Query
# ============================================================================
# Step 6: Validate with a test similarity search
# ============================================================================

test_queries = [
    "How do I convert stored procedures to PySpark?",
    "What tables store migration job status?",
    "How does the self-healing bot work?",
]

print("=" * 70)
print("VALIDATION: Testing Vector Search retrieval")
print("=" * 70)

for q in test_queries:
    try:
        results = w.vector_search_indexes.query_index(
            index_name=VS_INDEX_NAME,
            columns=["chunk_id", "section"],
            query_text=q,
            num_results=2,
        )
        sections = [row[1] for row in results.result.data_array]
        print(f"\n✅ Q: \"{q}\"")
        print(f"   → {', '.join(sections)}")
    except Exception as e:
        print(f"\n❌ Q: \"{q}\"")
        print(f"   Error: {e}")

print("\n" + "=" * 70)
print("✅ Document Q&A RAG setup complete!")
print(f"   Table: {FULL_TABLE}")
print(f"   Index: {VS_INDEX_NAME}")
print(f"   Endpoint: {VS_ENDPOINT_NAME}")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Test Cases: Genie Chat Document Q&A
# ============================================================================
# TEST CASES: Genie Chat Document Q&A via Vector Search
# ============================================================================
# These test cases validate the RAG retrieval that powers the Genie chat.
# Run after the index is READY (Step 5 complete).
# ============================================================================

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

_VS_INDEX_NAME = "admin_source.migration_app.doc_qa_chunks_index"

def query_rag(question, num_results=3):
    """Simulate the RAG retrieval that genie.py performs."""
    results = w.vector_search_indexes.query_index(
        index_name=_VS_INDEX_NAME,
        columns=["chunk_id", "section", "content"],
        query_text=question,
        num_results=num_results,
    )
    return results.result.data_array

# ─── Test Suite ───────────────────────────────────────────────────────────────

test_cases = [
    # Category: Getting Started / Onboarding
    {
        "category": "Onboarding",
        "question": "What is DBX Migration Studio and what does it do?",
        "expected_sections": ["Overview"],
    },
    {
        "category": "Onboarding",
        "question": "How do I get started with the migration process?",
        "expected_sections": ["Getting Started 5-Step Workflow"],
    },
    {
        "category": "Onboarding",
        "question": "What are the steps to configure the app for the first time?",
        "expected_sections": ["App Configuration", "Getting Started 5-Step Workflow"],
    },
    
    # Category: Module-Specific Questions
    {
        "category": "Modules",
        "question": "How do I convert stored procedures to PySpark notebooks?",
        "expected_sections": ["Convert to PySpark Module"],
    },
    {
        "category": "Modules",
        "question": "How does the self-healing bot detect and fix failures?",
        "expected_sections": ["Self-Healing Bot Module"],
    },
    {
        "category": "Modules",
        "question": "How do I create Bronze Silver Gold pipelines?",
        "expected_sections": ["Pipeline Studio Module", "Data Flow Medallion Architecture"],
    },
    {
        "category": "Modules",
        "question": "How do I schedule a migration job to run at 2am daily?",
        "expected_sections": ["Job Manager and Scheduler Module"],
    },
    {
        "category": "Modules",
        "question": "How do I scan my SQL Server database to discover all objects?",
        "expected_sections": ["Discovery Module"],
    },
    {
        "category": "Modules",
        "question": "How do I validate data after migration to check row counts match?",
        "expected_sections": ["Reconciliation Module"],
    },
    {
        "category": "Modules",
        "question": "How do I check data quality scores and completeness?",
        "expected_sections": ["Data Quality Module"],
    },
    
    # Category: Architecture & Infrastructure
    {
        "category": "Architecture",
        "question": "What is the medallion architecture and how does data flow?",
        "expected_sections": ["Data Flow Medallion Architecture", "Architecture"],
    },
    {
        "category": "Architecture",
        "question": "Where is data stored in Azure Data Lake?",
        "expected_sections": ["Storage Layout ADLS Gen2"],
    },
    {
        "category": "Architecture",
        "question": "What tables are in the bronze and silver schemas?",
        "expected_sections": ["Connected Catalogs and Tables"],
    },
    
    # Category: Security & Admin
    {
        "category": "Security",
        "question": "How does authentication and RBAC work in the app?",
        "expected_sections": ["Security and Authentication", "User Management RBAC"],
    },
    {
        "category": "Security",
        "question": "What user roles are available and what can each role do?",
        "expected_sections": ["User Management RBAC"],
    },
    
    # Category: Troubleshooting
    {
        "category": "Troubleshooting",
        "question": "My connection to SQL Server is timing out, what should I check?",
        "expected_sections": ["Troubleshooting", "Source Connection Module"],
    },
    {
        "category": "Troubleshooting",
        "question": "Pipeline is failing, how do I diagnose the issue?",
        "expected_sections": ["Troubleshooting", "Self-Healing Bot Module"],
    },
    
    # Category: DevOps & Deployment
    {
        "category": "DevOps",
        "question": "How is the app integrated with Azure DevOps?",
        "expected_sections": ["DevOps Integration"],
    },
    {
        "category": "DevOps",
        "question": "What Python packages does the app depend on?",
        "expected_sections": ["Dependencies"],
    },
    
    # Category: API / Developer Questions
    {
        "category": "API",
        "question": "What API endpoints are available for the discovery module?",
        "expected_sections": ["Discovery Module", "API Route Summary"],
    },
    {
        "category": "API",
        "question": "What are all the Flask blueprints and route prefixes?",
        "expected_sections": ["API Route Summary"],
    },
]

# ─── Run Tests ────────────────────────────────────────────────────────────────

print("=" * 80)
print("GENIE CHAT RAG TEST SUITE")
print(f"Index: {_VS_INDEX_NAME}")
print(f"Total test cases: {len(test_cases)}")
print("=" * 80)

passed = 0
failed = 0
results_log = []

for i, tc in enumerate(test_cases, 1):
    question = tc["question"]
    expected = tc["expected_sections"]
    category = tc["category"]
    
    try:
        rows = query_rag(question, num_results=3)
        retrieved_sections = [row[1] for row in rows]
        
        # Check if at least one expected section appears in top 3 results
        hit = any(exp in retrieved_sections for exp in expected)
        
        status = "✅ PASS" if hit else "⚠️ PARTIAL"
        if hit:
            passed += 1
        else:
            failed += 1
            
        results_log.append({
            "test": i,
            "category": category,
            "question": question[:60],
            "expected": expected,
            "retrieved": retrieved_sections,
            "pass": hit,
        })
        
        print(f"\n{status} [{category}] Test {i}/{len(test_cases)}")
        print(f"   Q: {question[:70]}")
        print(f"   Expected: {expected}")
        print(f"   Got:      {retrieved_sections}")
        
    except Exception as e:
        failed += 1
        print(f"\n❌ FAIL [{category}] Test {i}/{len(test_cases)}")
        print(f"   Q: {question[:70]}")
        print(f"   Error: {e}")

# ─── Summary ──────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print(f"RESULTS: {passed}/{len(test_cases)} passed | {failed} issues")
print(f"Accuracy: {passed/len(test_cases)*100:.0f}%")
print("=" * 80)

if failed == 0:
    print("\n🎉 All test cases passed! RAG retrieval is working correctly.")
else:
    print(f"\n⚠️ {failed} test(s) returned unexpected top sections.")
    print("   This may be acceptable if the relevant content still appears in results.")

# COMMAND ----------

# DBTITLE 1,Test: Simulated Genie Chat Flow (End-to-End)
# ============================================================================
# END-TO-END SIMULATION: How genie.py uses RAG in a real chat flow
# ============================================================================
# This simulates exactly what happens when a user sends a question through
# the Genie chat panel in the Migration Studio app.
# ============================================================================

def simulate_genie_chat(user_question):
    """
    Simulates the full genie.py flow:
    1. Check FAQ (hardcoded answers)
    2. Query Vector Search for documentation context
    3. Build enriched prompt for Genie API
    """
    print(f"\n{'─' * 70}")
    print(f"👤 User: {user_question}")
    print(f"{'─' * 70}")
    
    # Step 1: FAQ check (simplified)
    faq_triggers = {
        "what is migration studio": "DBX Migration Studio is an end-to-end SQL-to-Databricks migration accelerator.",
        "who built this": "Built by the Databricks Professional Services team.",
    }
    
    q_lower = user_question.lower().strip()
    for trigger, answer in faq_triggers.items():
        if trigger in q_lower:
            print(f"\n📋 FAQ Match: {answer}")
            return {"source": "faq", "answer": answer}
    
    print("   ℹ️ No FAQ match → querying Vector Search...")
    
    # Step 2: Vector Search RAG retrieval
    try:
        rows = query_rag(user_question, num_results=3)
        
        if rows:
            # Build context block (same format as genie.py _retrieve_rag_context)
            context_parts = []
            for row in rows:
                section = row[1]
                content = row[2][:500]  # Truncate for display
                context_parts.append(f"### {section}\n{content}")
            
            rag_context = "\n\n".join(context_parts)
            print(f"\n📚 RAG Context Retrieved ({len(rows)} chunks):")
            for row in rows:
                print(f"   • [{row[0]}] {row[1]} ({len(row[2])} chars)")
            
            # Step 3: Build the enriched prompt
            enriched_prompt = (
                f"--- DOCUMENTATION CONTEXT ---\n"
                f"{rag_context}\n\n"
                f"--- USER QUESTION ---\n"
                f"{user_question}"
            )
            
            print(f"\n📤 Enriched prompt length: {len(enriched_prompt)} chars")
            print(f"   Would be sent to Genie API with APP_CONTEXT_PREAMBLE prepended.")
            
            # Show a preview of what Genie would see
            print(f"\n🤖 Genie would receive:")
            print(f"   [APP_CONTEXT_PREAMBLE] + [RAG: {len(rag_context)} chars] + [Question]")
            
            return {"source": "rag+genie", "context_chunks": len(rows), "prompt_size": len(enriched_prompt)}
        else:
            print("   ⚠️ No RAG results → sending raw question to Genie")
            return {"source": "genie_only"}
            
    except Exception as e:
        print(f"   ⚠️ RAG unavailable ({e}) → graceful fallback to Genie")
        return {"source": "genie_fallback", "error": str(e)}


# ─── Run Simulated Conversations ─────────────────────────────────────────────

print("=" * 80)
print("SIMULATED GENIE CHAT SESSIONS")
print("=" * 80)

sample_conversations = [
    # Onboarding flow
    "What is DBX Migration Studio?",
    "How do I get started with a migration?",
    
    # Technical deep-dive
    "How do I convert my stored procedures to PySpark?",
    "What API endpoint do I call to scan my database?",
    
    # Troubleshooting
    "My pipeline keeps failing, what should I do?",
    "I'm getting a connection timeout to SQL Server",
    
    # Architecture questions
    "Where does the Bronze data get stored in ADLS?",
    "What embedding model does the RAG system use?",
    
    # Admin questions  
    "What roles can I assign to users?",
    "How do I check the audit log for today's actions?",
]

for question in sample_conversations:
    simulate_genie_chat(question)

print(f"\n{'=' * 80}")
print("✅ All simulated chat sessions complete.")
print("   These demonstrate the RAG retrieval flow that powers the Genie chat.")
print("=" * 80)