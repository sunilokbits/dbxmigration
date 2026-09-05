"""
Genie Space proxy blueprint — forwards chat requests to Databricks Genie Conversations API.
Includes a comprehensive FAQ / knowledge-base intercept layer for app-level questions.
"""

import os, json, uuid, re, time, requests, threading
from flask import Blueprint, request, jsonify
from routes.auth import login_required
from routes.catalog_discovery import get_relevant_schema_context, _ensure_cache_fresh

genie_bp = Blueprint("genie", __name__)

_HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
if not _HOST:
    try:
        from config_cache import get_config as _get_cfg
        _HOST = (_get_cfg().get("databricks_host") or "").rstrip("/")
    except Exception:
        pass
if _HOST and not _HOST.startswith("http"):
    _HOST = "https://" + _HOST
_faq_store = {}
_mcp_sessions = {}
_mcp_results = {}  # Async MCP query results: {message_id: {status, result, error}}

# ══════════════════════════════════════════════════════════════════════════════
# APP CONTEXT PREAMBLE — injected into every real Genie conversation
# ══════════════════════════════════════════════════════════════════════════════
APP_CONTEXT_PREAMBLE = (
    "You are the AI assistant embedded inside DBX Migration Studio — "
    "a SQL-to-Databricks migration accelerator. Answer questions using the data in the connected tables.\n\n"
    # Deliberately no hardcoded catalog/schema names here (e.g. a literal
    # "admin_source" or "bronze.hr") -- this app's catalogs are configured
    # per-deployment in Settings (Metadata Catalog, and each medallion
    # layer's target catalog), so a fixed list here would silently go
    # stale/wrong the moment a different catalog is configured, exactly
    # like the workflow_manager._fqn() staleness fixed earlier. The actual
    # available catalogs/schemas/tables are appended live below via
    # get_relevant_schema_context(), which discovers them directly from
    # the workspace (scoped to Settings' configured catalogs and ranked
    # by relevance to the question) instead of assuming any particular
    # name or dumping every table in the workspace.
    "IMPORTANT FORMATTING RULE: Whenever you show a SQL query in your response, "
    "you MUST wrap it in a fenced code block using triple backticks with the sql language tag, like:\n"
    "```sql\nSHOW CATALOGS;\n```\n"
    "This ensures the query renders with a Run button in the UI. Never put SQL inline.\n\n"
    "Now answer the following question:\n"
)


def resolve_configured_catalogs() -> dict:
    """Resolve this deployment's actually-configured catalog.schema pairs,
    read live from Settings -- Metadata Catalog, and each medallion/
    reconciliation/logging catalog. Mirrors the same dynamic resolution
    used for _fqn()/get_catalog_schema() and the deploy pipeline's Genie
    Space instructions rendering, so every place that needs to tell an
    LLM "here's where things live" resolves it the same way instead of
    each hardcoding its own guess.

    Returns a dict of name -> (catalog, schema), entries omitted when not
    configured (both empty strings).
    """
    try:
        from config_cache import get_config
        cfg = get_config() or {}
    except Exception:
        cfg = {}

    meta_cat = cfg.get("metadata_catalog") or ""
    meta_sch = cfg.get("metadata_schema") or ""
    mapping = (cfg.get("existing_setting") or {}).get("medallion_layer_mapping") or {}

    def _layer(name):
        lm = mapping.get(name) or {}
        return lm.get("catalog") or "", lm.get("schema") or ""

    bronze_cat, bronze_sch = _layer("bronze")
    silver_cat, silver_sch = _layer("silver")

    # reports.py/workflow.py's Reconciliation Report and execution-log routes
    # read these from top-level "reconciliation"/"logging" keys, not the
    # medallion mapping -- prefer those, fall back to the mapping's entries.
    recon_top = cfg.get("reconciliation") or {}
    recon_cat = recon_top.get("catalog") or _layer("reconciliation")[0]
    recon_sch = recon_top.get("schema") or _layer("reconciliation")[1]
    log_top = cfg.get("logging") or {}
    log_cat = log_top.get("catalog") or _layer("loggingdetails")[0]
    log_sch = log_top.get("schema") or _layer("loggingdetails")[1]

    return {
        "metadata": (meta_cat, meta_sch),
        "bronze": (bronze_cat, bronze_sch),
        "silver": (silver_cat, silver_sch),
        "reconciliation": (recon_cat, recon_sch),
        "logging": (log_cat, log_sch),
    }


def _build_configured_catalog_context() -> str:
    """List this deployment's actually-configured catalogs, read live from
    Settings, so Genie knows which of everything get_schema_context()
    discovers is THIS app's own data -- not a hardcoded admin_source/
    bronze.hr/... guess, and not just "search the whole workspace" either.
    """
    resolved = resolve_configured_catalogs()
    rows = [
        ("Metadata Catalog (migration jobs/pipelines/runs/schedules/audit/roles)", *resolved["metadata"]),
        ("Bronze layer (raw ingested data)", *resolved["bronze"]),
        ("Silver layer (cleaned/enriched data)", *resolved["silver"]),
        ("Reconciliation (source vs target row-count checks)", *resolved["reconciliation"]),
        ("Logging (pipeline execution logs)", *resolved["logging"]),
    ]
    lines = [r for r in rows if r[1] and r[2]]
    if not lines:
        return ""

    out = ["This deployment's currently configured catalogs (from Settings):\n"]
    for label, cat, sch in lines:
        out.append(f"• {label} → `{cat}`.`{sch}`")
    out.append("\nTreat these as the primary source for questions about this app's own migration/pipeline/reconciliation/audit data. "
                "Other catalogs discovered below may also be relevant depending on the question.\n")
    return "\n".join(out) + "\n"

# ══════════════════════════════════════════════════════════════════════════════
# FAQ KNOWLEDGE BASE — Rich detailed answers for app-level questions
# ══════════════════════════════════════════════════════════════════════════════
APP_FAQ = [
    # ── What is the app ──────────────────────────────────────────────────────
    (
        ["what is migration studio", "what is this app", "what does this app do",
         "about this application", "what is dbxmigrator", "tell me about this",
         "purpose of this app", "describe this tool"],
        """**DBX Migration Studio** is an end-to-end SQL-to-Databricks migration accelerator built on Databricks Apps.

**What it does:**
It automates the full journey of migrating a SQL Server data estate into the Databricks Lakehouse — from discovery and conversion through to production pipelines and data validation.

**Key Modules:**

| Module | What it does |
|---|---|
| Discovery | Scans SQL Server, scores object complexity, builds dependency graph |
| Convert to PySpark | AI-powered T-SQL → PySpark notebook conversion |
| Deploy Notebooks | Pushes converted notebooks to Databricks workspace |
| MetadataFlow | Provisions Unity Catalog + Delta metadata tables |
| Pipeline Studio | Creates Bronze → Silver → Gold medallion pipelines |
| Job Manager | Creates and monitors Lakeflow Jobs |
| Job Scheduler | Cron / interval / one-time scheduling |
| Reports & Analytics | Migration dashboards, progress charts |
| Reconciliation | Source vs target row-count comparison |
| Data Quality | Completeness, accuracy, consistency, freshness checks |
| Schema Comparison | Column-level type drift detection |
| System Health Check | Intelligent failure detection and auto-recovery |
| User Management | Role-based access (Admin, Operator, Viewer) |
| **Genie AI** | This assistant — natural language queries across all data |

**Architecture:** Azure SQL Server → Azure Data Lake Storage → Databricks Lakehouse (Bronze / Silver / Gold) managed by Unity Catalog."""
    ),

    # ── Getting started ──────────────────────────────────────────────────────
    (
        ["how do i use", "how to use", "getting started", "where do i start",
         "how to begin", "first step", "how does it work", "workflow"],
        """**Getting Started with DBX Migration Studio**

The recommended workflow follows 5 steps:

**Step 1 — Configure** *(Settings tab)*
Set your Azure SQL Server connection, Databricks host, storage account, and Unity Catalog targets.

**Step 2 — Discover** *(Discovery tab)*
Scan your SQL Server to inventory all tables, stored procedures, views, and UDFs. Review the complexity score and dependency graph.

**Step 3 — Convert** *(Convert to PySpark tab)*
Select discovered objects and click Convert. The app auto-generates PySpark notebooks for each stored procedure/view.

**Step 4 — Deploy Pipelines** *(MetadataFlow → Pipeline Studio → Job Manager)*
Provision the metadata schema, create Bronze/Silver pipelines, and schedule jobs.

**Step 5 — Validate** *(Reconciliation + Data Quality)*
Compare source vs target row counts, check data quality metrics, and export compliance reports.

**Tip:** The **Dashboard** tab gives you a real-time overview of all 5 steps."""
    ),

    # ── Architecture ─────────────────────────────────────────────────────────
    (
        ["architecture", "tech stack", "technology", "how is it built", "infrastructure"],
        """**DBX Migration Studio — Architecture**

```
SQL Server (source)
      │
      ▼
Azure Data Lake Storage Gen2
      │
      ▼  (Auto Loader / CDF)
Bronze Layer  ──→  Silver Layer  ──→  Gold Layer
  (raw ingest)    (cleaned data)    (business models)
      │
      ▼
Unity Catalog (admin_source.configtables)
      │
      ▼
Migration Studio App  ←→  Genie AI (this panel)
```

**Tech stack:**
- **Frontend:** Flask + vanilla JS (single-page app on Databricks Apps)
- **Backend:** Python / Flask blueprints
- **Pipelines:** Lakeflow Spark Declarative Pipelines with Auto Loader
- **Orchestration:** Lakeflow Jobs
- **Catalog:** Unity Catalog with Delta Lake tables
- **Storage:** Azure Data Lake Storage Gen2
- **Secrets:** Azure Key Vault + Databricks Secret Scopes
- **AI:** Databricks Genie Conversations API"""
    ),

    # ── Catalogs / data ──────────────────────────────────────────────────────
    (
        ["what catalogs", "which catalog", "what data", "what tables",
         "list catalog", "available data", "connected tables", "what schemas"],
        """**Connected Catalogs & Data (26 tables)**

| Catalog | Schema | Purpose |
|---|---|---|
| `admin_source` | `configtables` | Migration pipeline config: job metadata, run history, schedules, watermarks |
| `admin_source` | `migration_app` | App runtime: migration jobs, audit log, user roles, schedules |
| `bronze` | `hr` | Raw SQL Server ingestion: customers, products, sales orders, employees, invoices |
| `silver` | `hr` | Cleaned/enriched HR data after medallion processing |
| `loggingdetails` | `hr` | Pipeline execution logs (rows processed, duration, errors) |
| `reconciliation` | `hr` | Source vs target row-count reconciliation results |
| `samples` | various | NYC taxi trips, TPC-H benchmark orders |

You can ask data questions about any of these — for example: *"Show total sales by product"* or *"Which migration jobs failed?"*"""
    ),

    # ── Migration status ─────────────────────────────────────────────────────
    (
        ["migration status", "progress", "how many tables migrated",
         "migration summary", "overall status"],
        """To get **live migration status**, ask a data question like:

- *"Show migration progress: total, migrated, pending, failed"*
- *"How many migration jobs completed successfully vs failed?"*
- *"Which source tables are still pending?"*
- *"Show latest run history with status"*

These query `admin_source.configtables.wf_job_metadata` and `admin_source.migration_app.migration_jobs` in real-time."""
    ),

    # ── Genie / AI ───────────────────────────────────────────────────────────
    (
        ["what is genie", "what can genie do", "genie ai", "ai assistant",
         "what can i ask", "what questions", "examples"],
        """**Genie AI Assistant** (this panel)

Ask natural language questions and get SQL-backed answers instantly.

**Example questions:**

*Migration & pipelines:*
- "How many jobs completed vs failed?"
- "Show failed pipeline runs with errors"
- "Which pipelines ran > 2 hours?"

*Business data:*
- "Total sales by product category"
- "Top 10 customers by order value"
- "Employee headcount by department"

*Operations:*
- "Show audit log entries from today"
- "Which users have admin roles?"
- "Compare bronze vs silver row counts"

*Sample data:*
- "Average NYC taxi fare by hour"
- "TPC-H revenue by nation"

**How it works:** Type → Genie generates SQL → runs it → returns answer + underlying query."""
    ),

    # ── Help ─────────────────────────────────────────────────────────────────
    (
        ["help", "support", "contact", "documentation", "user guide"],
        """**Help & Support**

- **In-app Help:** Click the **Help** button (topbar) for documentation
- **Solution BOM:** Click **Solution BOM** for the full bill of materials
- **Accelerator Video:** Click **Accelerator Video** for a walkthrough demo
- **System Health Check:** Runs automated diagnostics on all connections
- **Audit & Compliance:** Full history of all actions taken

**For data questions**, just type naturally here — no SQL needed."""
    ),
]

# ══════════════════════════════════════════════════════════════════════════════
# PER-FEATURE FAQ — triggered by "how X work" / "explain X" patterns
# ══════════════════════════════════════════════════════════════════════════════
APP_FEATURE_FAQ = {
    "pipeline_studio": """**Pipeline Studio** — Create & manage Bronze/Silver/Gold medallion pipelines.

Pipeline Studio creates Lakeflow Spark Declarative Pipelines that automatically ingest data from Azure Data Lake:
- **Bronze** — raw ingestion (Auto Loader, CDF)
- **Silver** — cleaned, deduplicated, enriched
- **Gold** — business aggregations

**How to use:** Pipeline Studio tab → Select tables → Choose layer → Create Pipeline → Run Pipeline""",

    "discovery": """**Discovery** — Scan and analyse your SQL Server database.

Discovers tables, stored procedures, views, and UDFs with complexity scoring (1-5).
Results stored in `admin_source.configtables.wf_source_tables`.

**How to use:** Discovery tab → Start Discovery → Review objects → Select for migration""",

    "convert_pyspark": """**Convert to PySpark** — AI-powered T-SQL → PySpark conversion.

Converts stored procedures → `.py` notebooks, views → DataFrames, UDFs → shared `HelperFunction.py`.

**How to use:** Convert tab → Select objects → Convert → Review → Deploy""",

    "deploy": """**Deploy Notebooks** — Push converted notebooks to Databricks workspace.

Uses the Databricks Workspace API to upload each notebook to the configured target folder.

**How to use:** Deploy tab → Select notebooks → Configure path → Deploy""",

    "metadataflow": """**MetadataFlow** — Provision Unity Catalog and Delta metadata tables.

Creates `admin_source` catalog, schemas, and all 6 config tables (wf_job_metadata, wf_pipeline_metadata, wf_run_history, wf_scheduler_config, wf_source_tables, wf_watermark_metadata).

**Must complete before Pipeline Studio or Job Manager.**""",

    "job_manager": """**Job Manager** — Create and monitor Lakeflow Jobs.

Creates Databricks Jobs that run migration pipelines/notebooks. Shows status, duration, rows processed.
Runs tracked in `admin_source.configtables.wf_run_history`.""",

    "scheduler": """**Job Scheduler** — Cron, interval, or one-time job scheduling.

**Types:** Cron expression (e.g. `0 2 * * *`), Interval (every N hours), One-time (specific datetime).
Config stored in `admin_source.configtables.wf_scheduler_config`.""",

    "reconciliation": """**Reconciliation** — Compare source vs target after migration.

Checks row counts, numeric aggregate sums, NULL differences, and variance %.
Results in `reconciliation.hr.reconcilationdetails`.""",

    "data_quality": """**Data Quality** — Validate completeness, accuracy, consistency, freshness.

Per-column scorecard with overall quality score. Failed checks logged to audit_log.""",

    "schema_comparison": """**Schema Comparison** — Detect column-level type and nullability drift.

Side-by-side diff of source SQL Server schema vs target Databricks schema. Exportable as CSV.""",

    "audit": """**Audit & Compliance** — Full action history.

Tracks every action: login, settings change, pipeline create/run, deployment. Filter by date/user/action.
Stored in `admin_source.migration_app.audit_log`.""",

    "user_management": """**User Management** — RBAC (Admin only).

| Role | Permissions |
|---|---|
| Admin | Full access: manage users, settings, run migrations |
| Operator | Run pipelines and jobs; cannot change settings |
| Viewer | Read-only: view dashboards and reports |

Stored in `admin_source.migration_app.user_roles`.""",

    "health_check": """**System Health Check** — Failure detection + auto-recovery.

Monitors: SQL Server connectivity, Databricks API, Azure Storage, Unity Catalog, pipeline status, secret scopes.
Click **Auto-Recover** to attempt automatic fixes.""",
}

FEATURE_MAP = {
    "pipeline studio": "pipeline_studio", "discovery": "discovery",
    "convert": "convert_pyspark", "pyspark": "convert_pyspark",
    "deploy notebook": "deploy", "metadataflow": "metadataflow",
    "metadata flow": "metadataflow", "job manager": "job_manager",
    "job scheduler": "scheduler", "scheduler": "scheduler",
    "reconciliation": "reconciliation", "recon": "reconciliation",
    "data quality": "data_quality", "schema comparison": "schema_comparison",
    "audit": "audit", "user management": "user_management",
    "health check": "health_check", "system health": "health_check",
}

QUESTION_VERBS = re.compile(
    r"\b(how|what|explain|describe|tell me|show me|give me|"
    r"what does|how does|how do|can you explain|can you describe)\b"
)


def _check_faq(question):
    """Return FAQ answer if question matches, else None."""
    q = question.lower().strip()
    # Phase 1: explicit trigger match
    for triggers, answer in APP_FAQ:
        for trigger in triggers:
            if trigger in q:
                return answer
    # Phase 2: feature-keyword + question verb
    if QUESTION_VERBS.search(q):
        for kw, key in FEATURE_MAP.items():
            if kw in q:
                return APP_FEATURE_FAQ.get(key)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# SUGGESTIONS — shown in Genie panel welcome screen
# ══════════════════════════════════════════════════════════════════════════════
DEFAULT_SUGGESTIONS = [
    "What is Migration Studio?",
    "What catalogs are connected?",
    "How do I get started?",
    "What can Genie AI answer?",
    "How many migration jobs completed successfully vs failed?",
    "Show all failed pipeline runs in the last 7 days",
    "Which source tables are still pending migration?",
    "Show total sales revenue by product category",
    "Show average NYC taxi fare by hour of day",
    "Show all tables where source and target row counts differ",
    "How does Pipeline Studio work?",
    "Explain Discovery",
    "What is the architecture?",
    "Which users have admin roles?",
    "Show latest watermark values for all tables",
    "Compare bronze vs silver row counts",
    "Top 10 customers by total order value",
    "Show execution log errors from the last pipeline run",
    "Show audit log entries from today",
    "What is the average pipeline run duration?",
    "How does Reconciliation work?",
    "Describe Data Quality checks",
]


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def _get_token():
    try:
        from secrets_helper import get_databricks_token
        return get_databricks_token()
    except Exception:
        return os.environ.get("DATABRICKS_TOKEN", "")


def _serving_headers():
    """Auth headers for calling a serving-endpoint's /invocations.

    Prefers the app's own service-principal OAuth token — Databricks Apps
    grants CAN_QUERY to that SP for every serving_endpoint resource declared
    in app.yml, whereas the PAT _headers() uses may have no grant on the
    endpoint at all (the cause of 403s on Foundation Model calls).
    """
    from secrets_helper import get_serving_endpoint_token
    token = get_serving_endpoint_token()
    if not token:
        raise ValueError("No Databricks token available")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _headers():
    token = _get_token()
    if not token:
        raise ValueError("No Databricks token available")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _spaces_config_path():
    import tempfile
    tmp_path = os.path.join(tempfile.gettempdir(), "genie_spaces.json")
    if os.path.isfile(tmp_path):
        return tmp_path
    # Copy from source dir to /tmp on first access (source is read-only in SNAPSHOT deploy)
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src_path = os.path.join(base, "genie_spaces.json")
    if os.path.isfile(src_path):
        try:
            import shutil
            shutil.copy2(src_path, tmp_path)
        except Exception:
            pass
    return tmp_path


def _load_spaces():
    path = _spaces_config_path()
    if os.path.isfile(path):
        try:
            with open(path) as f:
                spaces = json.load(f)
            if spaces:
                return _refresh_stale_names(spaces)
        except Exception:
            pass
    try:
        deploy_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "deployconfig.json")
        if os.path.isfile(deploy_path):
            with open(deploy_path) as f:
                cfg = json.load(f)
            if cfg.get("genie_spaces"):
                return _refresh_stale_names(cfg["genie_spaces"])
    except Exception:
        pass
    # Auto-discover from Databricks Genie API if no local config
    return _discover_workspace_spaces()


def _refresh_stale_names(spaces):
    """Re-fetch titles for cached entries whose name is missing or equals their space_id (stale/legacy cache)."""
    stale = [s for s in spaces if not s.get("name") or s.get("name") == s.get("space_id")]
    if not stale:
        return spaces
    try:
        r = requests.get(f"{_HOST}/api/2.0/genie/spaces", headers=_headers(), timeout=10)
        if r.status_code != 200:
            return spaces
        api_spaces = {s["space_id"]: s for s in r.json().get("spaces", []) if s.get("space_id")}
    except Exception:
        return spaces
    changed = False
    for s in spaces:
        api_s = api_spaces.get(s.get("space_id"))
        if api_s and api_s.get("title") and s.get("name") != api_s["title"]:
            s["name"] = api_s["title"]
            if api_s.get("description"):
                s["description"] = api_s["description"]
            changed = True
    if changed:
        _save_spaces(spaces)
    return spaces


def _discover_workspace_spaces():
    """Fetch Genie Spaces from the Databricks API and persist locally."""
    try:
        r = requests.get(f"{_HOST}/api/2.0/genie/spaces", headers=_headers(), timeout=10)
        if r.status_code != 200:
            return []
        api_spaces = r.json().get("spaces", [])
        spaces = [{"space_id": s["space_id"], "name": s.get("title", s["space_id"]),
                    "description": s.get("description", "")} for s in api_spaces if s.get("space_id")]
        if spaces:
            _save_spaces(spaces)
        return spaces
    except Exception:
        return []


def _save_spaces(spaces):
    with open(_spaces_config_path(), "w") as f:
        json.dump(spaces, f, indent=2)


def _mcp_config_path():
    """Return a writable path for MCP endpoint config."""
    # Primary: OS temp dir (always writable in container; cross-platform for local dev)
    import tempfile
    tmp_path = os.path.join(tempfile.gettempdir(), "genie_mcp_endpoints.json")
    if os.path.isfile(tmp_path):
        return tmp_path
    # Fallback: source dir (may be read-only in SNAPSHOT deploy)
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src_path = os.path.join(base, "genie_mcp_endpoints.json")
    # If source file exists but /tmp doesn't, copy to /tmp for writability
    if os.path.isfile(src_path):
        try:
            import shutil
            shutil.copy2(src_path, tmp_path)
        except Exception:
            pass
        return tmp_path
    return tmp_path


def _load_mcp_endpoints():
    """Load MCP Genie endpoints from config file."""
    path = _mcp_config_path()
    if os.path.isfile(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return []


def _save_mcp_endpoints(endpoints):
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(base, "genie_mcp_endpoints.json")
    with open(path, "w") as f:
        json.dump(endpoints, f, indent=2)


# ══════════════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@genie_bp.route("/api/v1/genie/spaces", methods=["GET"])
@login_required
def list_spaces():
    return jsonify({"spaces": _load_spaces(), "suggestions": DEFAULT_SUGGESTIONS, "mcp_endpoints": _load_mcp_endpoints()})


@genie_bp.route("/api/v1/genie/spaces/save", methods=["POST"])
@login_required
def save_space():
    data = request.get_json() or {}
    space_id = (data.get("space_id") or "").strip()
    name = (data.get("name") or "").strip() or space_id
    description = (data.get("description") or "").strip()
    if not space_id:
        return jsonify({"error": "space_id is required"}), 400
    spaces = _load_spaces()
    updated = False
    for s in spaces:
        if s.get("space_id") == space_id:
            s["name"] = name; s["description"] = description; updated = True; break
    if not updated:
        spaces.append({"space_id": space_id, "name": name, "description": description})
    _save_spaces(spaces)
    return jsonify({"ok": True, "space": {"space_id": space_id, "name": name, "description": description}})


@genie_bp.route("/api/v1/genie/spaces/<space_id>", methods=["DELETE"])
@login_required
def delete_space(space_id):
    spaces = [s for s in _load_spaces() if s.get("space_id") != space_id]
    _save_spaces(spaces)
    return jsonify({"ok": True})


@genie_bp.route("/api/v1/genie/start", methods=["POST"])
@login_required
def start_conversation():
    data = request.get_json() or {}
    space_id = (data.get("space_id") or "").strip()
    content = (data.get("content") or "").strip()
    if not space_id or not content:
        return jsonify({"error": "space_id and content are required"}), 400

    # MCP endpoint detection — run async, return fake IDs for poll flow
    if space_id.startswith("mcp-"):
        mcp_eps = _load_mcp_endpoints()
        ep_url = ""
        for ep in mcp_eps:
            if ep.get("id") == space_id:
                ep_url = ep.get("endpoint_url", "")
                break
        if not ep_url:
            ep_url = f"{_HOST}/api/2.0/mcp/genie"
        fake_cid = "mcp-conv-" + str(uuid.uuid4())[:8]
        fake_mid = "mcp-msg-" + str(uuid.uuid4())[:8]
        _mcp_results[fake_mid] = {"status": "IN_PROGRESS", "result": None}
        # Capture token NOW (Flask request context won't exist in thread)
        token = _get_token()
        t = threading.Thread(target=_run_mcp_background, args=(fake_mid, content, ep_url, token), daemon=True)
        t.start()
        return jsonify({"conversation_id": fake_cid, "message_id": fake_mid})

    # FAQ intercept
    faq_answer = _check_faq(content)
    if faq_answer:
        fake_cid = "faq-" + str(uuid.uuid4())
        fake_mid = "faq-" + str(uuid.uuid4())
        _faq_store[fake_mid] = {"text": faq_answer, "conv_id": fake_cid}
        return jsonify({"conversation_id": fake_cid, "message_id": fake_mid})

    # Real Genie call — send raw content (Genie space has its own table config)
    try:
        r = requests.post(f"{_HOST}/api/2.0/genie/spaces/{space_id}/start-conversation",
                          json={"content": content}, headers=_headers(), timeout=30)
        return jsonify(r.json()), r.status_code
    except requests.exceptions.Timeout:
        return jsonify({"error": "Genie API timeout"}), 504
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


def _run_mcp_background(message_id, question, endpoint_url, token):
    """Background thread: run MCP query and store result in _mcp_results."""
    try:
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json"}
        req_counter = 10

        # Resolve Genie space_id (required by genie_ask tool)
        default_space_id = ""
        try:
            spaces = _load_spaces()
            if spaces:
                default_space_id = spaces[0].get("space_id", "")
        except Exception:
            pass

        # Initialize MCP session
        init_payload = {
            "jsonrpc": "2.0", "method": "initialize",
            "params": {"protocolVersion": "2025-03-26", "capabilities": {"tools": {}},
                       "clientInfo": {"name": "DBXConnect-MigrationStudio", "version": "1.0"}},
            "id": req_counter
        }
        r = requests.post(endpoint_url, json=init_payload, headers=headers, timeout=30)
        mcp_session_hdr = r.headers.get("mcp-session-id", "")
        if mcp_session_hdr:
            headers["mcp-session-id"] = mcp_session_hdr
        req_counter += 1

        # Initialized notification
        requests.post(endpoint_url, json={"jsonrpc": "2.0", "method": "notifications/initialized"}, headers=headers, timeout=10)
        req_counter += 1

        # List tools to discover tool name
        tool_name = "genie_ask"
        tr = requests.post(endpoint_url, json={"jsonrpc": "2.0", "method": "tools/list", "id": req_counter}, headers=headers, timeout=20)
        req_counter += 1
        try:
            tools_list = tr.json().get("result", {}).get("tools", [])
            if tools_list:
                tool_name = tools_list[0].get("name", "genie_ask")
        except Exception:
            pass

        # Build arguments with space_id
        tool_args = {"question": question}
        if default_space_id:
            tool_args["space_id"] = default_space_id

        # Call the tool
        call_payload = {"jsonrpc": "2.0", "method": "tools/call",
                        "params": {"name": tool_name, "arguments": tool_args}, "id": req_counter}
        r = requests.post(endpoint_url, json=call_payload, headers=headers, timeout=180)
        content_type = r.headers.get("content-type", "")
        result_text, result_sql, result_data = _parse_mcp_response(r, content_type)
        result_text, result_sql, result_data = _poll_if_in_progress(
            result_text, result_sql, result_data, endpoint_url, headers, tool_name, req_counter
        )

        _mcp_results[message_id] = {"status": "COMPLETED", "result": {"text": result_text, "sql": result_sql, "data": result_data}}
    except Exception as exc:
        _mcp_results[message_id] = {"status": "FAILED", "error": str(exc)}


def _run_mcp_background(message_id, question, endpoint_url, token):
    """Background thread: run MCP query and store result in _mcp_results."""
    try:
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json"}
        req_counter = 10

        # Resolve Genie space_id (required by genie_ask tool)
        default_space_id = ""
        try:
            spaces = _load_spaces()
            if spaces:
                default_space_id = spaces[0].get("space_id", "")
        except Exception:
            pass

        # Initialize MCP session
        init_payload = {
            "jsonrpc": "2.0", "method": "initialize",
            "params": {"protocolVersion": "2025-03-26", "capabilities": {"tools": {}},
                       "clientInfo": {"name": "DBXConnect-MigrationStudio", "version": "1.0"}},
            "id": req_counter
        }
        r = requests.post(endpoint_url, json=init_payload, headers=headers, timeout=30)
        mcp_session_hdr = r.headers.get("mcp-session-id", "")
        if mcp_session_hdr:
            headers["mcp-session-id"] = mcp_session_hdr
        req_counter += 1

        # Initialized notification
        requests.post(endpoint_url, json={"jsonrpc": "2.0", "method": "notifications/initialized"}, headers=headers, timeout=10)
        req_counter += 1

        # List tools to discover tool name
        tool_name = "genie_ask"
        tr = requests.post(endpoint_url, json={"jsonrpc": "2.0", "method": "tools/list", "id": req_counter}, headers=headers, timeout=20)
        req_counter += 1
        try:
            tools_list = tr.json().get("result", {}).get("tools", [])
            if tools_list:
                tool_name = tools_list[0].get("name", "genie_ask")
        except Exception:
            pass

        # Build arguments with space_id
        tool_args = {"question": question}
        if default_space_id:
            tool_args["space_id"] = default_space_id

        # Call the tool
        call_payload = {"jsonrpc": "2.0", "method": "tools/call",
                        "params": {"name": tool_name, "arguments": tool_args}, "id": req_counter}
        r = requests.post(endpoint_url, json=call_payload, headers=headers, timeout=180)
        content_type = r.headers.get("content-type", "")
        result_text, result_sql, result_data = _parse_mcp_response(r, content_type)
        result_text, result_sql, result_data = _poll_if_in_progress(
            result_text, result_sql, result_data, endpoint_url, headers, tool_name, req_counter
        )

        _mcp_results[message_id] = {"status": "COMPLETED", "result": {"text": result_text, "sql": result_sql, "data": result_data}}
    except Exception as exc:
        _mcp_results[message_id] = {"status": "FAILED", "error": str(exc)}


def _handle_mcp_query(question, endpoint_url):
    """Internal helper: route a question through MCP Genie One."""
    try:
        headers = _headers()
        headers["Accept"] = "application/json"
        req_counter = 10

        # Initialize MCP session
        init_payload = {
            "jsonrpc": "2.0", "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {"tools": {}},
                "clientInfo": {"name": "DBXConnect-MigrationStudio", "version": "1.0"}
            },
            "id": req_counter
        }
        r = requests.post(endpoint_url, json=init_payload, headers=headers, timeout=30)
        mcp_session_hdr = r.headers.get("mcp-session-id", "")
        if mcp_session_hdr:
            headers["mcp-session-id"] = mcp_session_hdr
        req_counter += 1

        # Initialized notification
        notif_payload = {"jsonrpc": "2.0", "method": "notifications/initialized"}
        requests.post(endpoint_url, json=notif_payload, headers=headers, timeout=10)
        req_counter += 1

        # List tools to get correct tool name
        tool_name = "genie_ask"
        tools_payload = {"jsonrpc": "2.0", "method": "tools/list", "id": req_counter}
        tr = requests.post(endpoint_url, json=tools_payload, headers=headers, timeout=20)
        req_counter += 1
        try:
            tools_data = tr.json()
            tools_list = tools_data.get("result", {}).get("tools", [])
            if tools_list:
                tool_name = tools_list[0].get("name", "genie_ask")
        except Exception:
            pass

        # Call the tool with the question
        call_payload = {
            "jsonrpc": "2.0", "method": "tools/call",
            "params": {"name": tool_name, "arguments": {"question": question}},
            "id": req_counter
        }
        r = requests.post(endpoint_url, json=call_payload, headers=headers, timeout=180)
        content_type = r.headers.get("content-type", "")
        result_text, result_sql, result_data = _parse_mcp_response(r, content_type)
        result_text, result_sql, result_data = _poll_if_in_progress(
            result_text, result_sql, result_data,
            endpoint_url, headers, tool_name, req_counter
        )

        req_id = "mcp-" + str(uuid.uuid4())[:8]
        return jsonify({
            "request_id": req_id,
            "status": "COMPLETED",
            "result": {"text": result_text, "sql": result_sql, "data": result_data}
        })
    except requests.exceptions.Timeout:
        return jsonify({"error": "MCP Genie timeout"}), 504
    except Exception as exc:
        return jsonify({"error": f"MCP query failed: {str(exc)}"}), 500


@genie_bp.route("/api/v1/genie/message", methods=["POST"])
@login_required
def send_message():
    data = request.get_json() or {}
    space_id = (data.get("space_id") or "").strip()
    conversation_id = (data.get("conversation_id") or "").strip()
    content = (data.get("content") or "").strip()
    if not all([space_id, conversation_id, content]):
        return jsonify({"error": "space_id, conversation_id and content are required"}), 400

    # MCP endpoint detection — route MCP spaces to MCP query handler
    if space_id.startswith("mcp-"):
        mcp_eps = _load_mcp_endpoints()
        ep_url = ""
        for ep in mcp_eps:
            if ep.get("id") == space_id:
                ep_url = ep.get("endpoint_url", "")
                break
        if not ep_url:
            ep_url = f"{_HOST}/api/2.0/mcp/genie"
        return _handle_mcp_query(content, ep_url)

    # FAQ intercept (follow-ups can also be FAQ)
    faq_answer = _check_faq(content)
    if faq_answer:
        fake_cid = "faq-" + str(uuid.uuid4())
        fake_mid = "faq-" + str(uuid.uuid4())
        _faq_store[fake_mid] = {"text": faq_answer, "conv_id": fake_cid}
        return jsonify({"conversation_id": fake_cid, "message_id": fake_mid})

    # If prior turn was FAQ, start fresh real conversation
    if conversation_id.startswith("faq-"):
        enriched = APP_CONTEXT_PREAMBLE + _build_configured_catalog_context() + get_relevant_schema_context(question=content) + content
        try:
            r = requests.post(f"{_HOST}/api/2.0/genie/spaces/{space_id}/start-conversation",
                              json={"content": enriched}, headers=_headers(), timeout=30)
            return jsonify(r.json()), r.status_code
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    # Normal follow-up
    try:
        r = requests.post(f"{_HOST}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages",
                          json={"content": content}, headers=_headers(), timeout=30)
        return jsonify(r.json()), r.status_code
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@genie_bp.route("/api/v1/genie/poll", methods=["GET"])
@login_required
def poll_message():
    space_id = (request.args.get("space_id") or "").strip()
    conversation_id = (request.args.get("conversation_id") or "").strip()
    message_id = (request.args.get("message_id") or "").strip()
    if not all([space_id, conversation_id, message_id]):
        return jsonify({"error": "space_id, conversation_id and message_id are required"}), 400

    # FAQ instant resolution
    if message_id in _faq_store:
        entry = _faq_store.pop(message_id)
        return jsonify({"status": "COMPLETED", "attachments": [{"text": {"content": entry["text"]}}]})

    # MCP async result check
    if message_id in _mcp_results:
        entry = _mcp_results[message_id]
        if entry["status"] == "IN_PROGRESS":
            return jsonify({"status": "EXECUTING_QUERY"})
        # Done (COMPLETED or FAILED)
        result = _mcp_results.pop(message_id)
        if result["status"] == "FAILED":
            return jsonify({"status": "FAILED", "error": {"message": result.get("error", "MCP query failed")}})
        res = result.get("result", {})
        resp = {"status": "COMPLETED", "attachments": [{"text": {"content": res.get("text", "")}}]}
        if res.get("sql"):
            resp["attachments"].append({"query": {"query": res["sql"]}})
        return jsonify(resp)

    # Normal Genie poll
    try:
        r = requests.get(f"{_HOST}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
                         headers=_headers(), timeout=20)
        return jsonify(r.json()), r.status_code
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@genie_bp.route("/api/v1/genie/result", methods=["GET"])
@login_required
def get_result():
    space_id = (request.args.get("space_id") or "").strip()
    conversation_id = (request.args.get("conversation_id") or "").strip()
    message_id = (request.args.get("message_id") or "").strip()
    if not all([space_id, conversation_id, message_id]):
        return jsonify({"error": "space_id, conversation_id and message_id are required"}), 400
    try:
        r = requests.get(f"{_HOST}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result/0",
                         headers=_headers(), timeout=60)
        return jsonify(r.json()), r.status_code
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# MCP GENIE ONE ROUTES
# JSON-RPC over Streamable HTTP — /api/2.0/mcp/genie
# ══════════════════════════════════════════════════════════════════════════════

@genie_bp.route("/api/v1/genie/mcp/endpoints", methods=["GET"])
@login_required
def list_mcp_endpoints():
    """List saved MCP Genie endpoints."""
    return jsonify({"endpoints": _load_mcp_endpoints()})


@genie_bp.route("/api/v1/genie/mcp/endpoints/save", methods=["POST"])
@login_required
def save_mcp_endpoint():
    """Save a new MCP Genie endpoint."""
    data = request.get_json() or {}
    endpoint_url = (data.get("endpoint_url") or "").strip()
    name = (data.get("name") or "").strip() or "Genie One (MCP)"
    if not endpoint_url:
        return jsonify({"error": "endpoint_url is required"}), 400
    endpoints = _load_mcp_endpoints()
    # Check for duplicate
    for ep in endpoints:
        if ep.get("endpoint_url") == endpoint_url:
            return jsonify({"error": "This MCP endpoint is already added"}), 400
    ep_id = "mcp-" + str(uuid.uuid4())[:8]
    new_ep = {"id": ep_id, "name": name, "endpoint_url": endpoint_url, "type": "mcp"}
    endpoints.append(new_ep)
    _save_mcp_endpoints(endpoints)
    return jsonify({"ok": True, "endpoint": new_ep})


@genie_bp.route("/api/v1/genie/mcp/endpoints/<ep_id>", methods=["DELETE"])
@login_required
def delete_mcp_endpoint(ep_id):
    endpoints = [ep for ep in _load_mcp_endpoints() if ep.get("id") != ep_id]
    _save_mcp_endpoints(endpoints)
    return jsonify({"ok": True})


_DEFAULT_FM_ENDPOINTS = [
    {"name": "databricks-claude-opus-4-6", "display_name": "Claude Opus 4.6", "type": "pay-per-token", "state": "Ready"},
    {"name": "databricks-claude-opus-4-7", "display_name": "Claude Opus 4.7", "type": "pay-per-token", "state": "Ready"},
    {"name": "databricks-claude-opus-4-8", "display_name": "Claude Opus 4.8", "type": "pay-per-token", "state": "Ready"},
    {"name": "databricks-claude-opus-5", "display_name": "Claude Opus 5", "type": "pay-per-token", "state": "Ready"},
    {"name": "databricks-claude-sonnet-5", "display_name": "Claude Sonnet 5", "type": "pay-per-token", "state": "Ready"},
    {"name": "databricks-claude-haiku-4-5", "display_name": "Claude Haiku 4.5", "type": "pay-per-token", "state": "Ready"},
]


@genie_bp.route("/api/v1/genie/fm/endpoints", methods=["GET"])
@login_required
def list_fm_endpoints():
    """List available Foundation Model serving endpoints."""
    try:
        r = requests.get(f"{_HOST}/api/2.0/serving-endpoints", headers=_headers(), timeout=10)
        if r.status_code == 200:
            data = r.json()
            endpoints = []
            for ep in data.get("endpoints", []):
                name = ep.get("name", "")
                state = ep.get("state", {}).get("ready", "NOT_READY")
                if state != "READY":
                    continue
                ep_type = "pay-per-token" if "databricks-" in name else "provisioned"
                served = ep.get("config", {}).get("served_entities", [])
                model = served[0].get("foundation_model", {}).get("name", name) if served else name
                endpoints.append({"name": name, "display_name": model.replace("databricks-", "").replace("-", " ").title(), "type": ep_type, "state": "Ready"})
            if endpoints:
                endpoints.sort(key=lambda x: (0 if x["type"] == "pay-per-token" else 1, x["name"]))
                return jsonify({"endpoints": endpoints})
    except Exception:
        pass
    return jsonify({"endpoints": _DEFAULT_FM_ENDPOINTS})


_RCA_KEYWORDS = (
    "fail", "error", "broke", "broken", "issue", "wrong", "rca",
    "root cause", "why did", "why is", "not working", "didn't work",
    "crash", "exception", "stuck",
)


def _build_fm_messages(content: str, messages: list, top_n: int, history_n: int) -> list:
    system_context = _build_configured_catalog_context() + get_relevant_schema_context(question=content, top_n=top_n)
    # Only pull in recent-failures RCA context when the question actually
    # looks failure-related -- it's cheap to build (one cached-friendly SQL
    # query + a deterministic classifier, no extra LLM/embedding call), but
    # there's no reason to spend the tokens on it for an unrelated question
    # like "how many tables are in bronze".
    lowered = content.lower()
    if any(kw in lowered for kw in _RCA_KEYWORDS):
        try:
            import workflow_manager as _wfm
            system_context += "\n" + _wfm.get_recent_failed_runs_context()
        except Exception:
            pass
    chat_messages = [{"role": "system", "content": APP_CONTEXT_PREAMBLE + system_context}]
    for msg in messages[-history_n:]:
        chat_messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})
    chat_messages.append({"role": "user", "content": content})
    return chat_messages


def _estimate_tokens(chat_messages: list) -> int:
    """~4 chars/token heuristic (no tokenizer dependency in requirements.txt).

    Only used to size the "standard" (never sent) prompt for the Token
    Optimiser's before/after comparison -- the tokens actually billed always
    come from the API's own usage.prompt_tokens on the request that's really
    sent, never from this estimate.
    """
    total_chars = sum(len(m.get("content", "") or "") for m in chat_messages)
    return max(1, total_chars // 4)


@genie_bp.route("/api/v1/genie/fm/chat", methods=["POST"])
@login_required
def fm_chat():
    """Chat with a Foundation Model endpoint. Returns response + token usage.

    optimize_tokens (from the UI's "Token Optimiser" toggle) trims the two
    parts of this prompt that actually grow the request -- the number of
    ranked tables included in the schema context, and how much raw
    conversation history gets resent -- instead of doing nothing, which is
    what this flag previously did (accepted by this endpoint and never read).
    """
    data = request.get_json() or {}
    endpoint_name = (data.get("endpoint") or "").strip()
    content = (data.get("content") or "").strip()
    messages = data.get("messages", [])
    optimize_tokens = bool(data.get("optimize_tokens"))
    if not endpoint_name or not content:
        return jsonify({"error": "endpoint and content are required"}), 400

    top_n = 6 if optimize_tokens else 15
    history_n = 4 if optimize_tokens else 10
    chat_messages = _build_fm_messages(content, messages, top_n, history_n)

    # Estimate-only: what the un-optimised (top_n=15, history=10) prompt would
    # have cost, purely for the savings comparison the UI shows -- table/
    # question embeddings are already cached, so this doesn't add a real
    # embedding or LLM call, and it's never actually sent to the model.
    standard_tokens_estimate = _estimate_tokens(_build_fm_messages(content, messages, 15, 10)) if optimize_tokens else None

    try:
        payload = {"messages": chat_messages, "max_tokens": 2048, "temperature": 0.1}
        r = requests.post(f"{_HOST}/serving-endpoints/{endpoint_name}/invocations", json=payload, headers=_serving_headers(), timeout=60)
        if r.status_code != 200:
            return jsonify({"error": f"Endpoint returned {r.status_code}: {r.text[:300]}"}), r.status_code
        resp = r.json()
        choices = resp.get("choices", [])
        raw_content = choices[0].get("message", {}).get("content", "") if choices else "No response"
        # Some serving endpoints (observed on newer models like Sonnet 5 --
        # Opus 4.6/4.7 happened to always return a plain string) return
        # `content` as a list of Anthropic-style content blocks
        # ([{"type": "text", "text": "..."}, ...]) instead of a plain string,
        # even through this OpenAI-compatible /invocations endpoint. The
        # frontend always expected a string (it calls .replace() on it to
        # render markdown) and had no guard, so picking one of those models
        # crashed the chat with "text.replace is not a function" -- normalize
        # here so every model returns the same shape regardless of which
        # response format its serving endpoint actually uses.
        if isinstance(raw_content, list):
            response_text = "".join(
                (block.get("text", "") if isinstance(block, dict) else str(block))
                for block in raw_content
            )
        elif raw_content is None:
            response_text = ""
        else:
            response_text = str(raw_content)
        usage = resp.get("usage", {})
        prompt_tokens = usage.get("prompt_tokens", 0)
        result = {"text": response_text, "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": usage.get("completion_tokens", 0), "total_tokens": usage.get("total_tokens", 0)}, "model": resp.get("model", endpoint_name), "endpoint": endpoint_name}
        if optimize_tokens:
            standard = max(standard_tokens_estimate or 0, prompt_tokens)
            savings_pct = max(0, round((1 - prompt_tokens / standard) * 100)) if standard else 0
            result["token_comparison"] = {"standard_tokens": standard, "optimised_tokens": prompt_tokens, "savings_pct": savings_pct}
        return jsonify(result)
    except requests.exceptions.Timeout:
        return jsonify({"error": "Endpoint request timed out"}), 504
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@genie_bp.route("/api/v1/genie/mcp/initialize", methods=["POST"])
@login_required
def mcp_initialize():
    """
    Initialize an MCP session with the Databricks MCP Genie endpoint.
    Returns session info including available tools.
    """
    data = request.get_json() or {}
    endpoint_url = (data.get("endpoint_url") or "").strip()
    if not endpoint_url:
        endpoint_url = f"{_HOST}/api/2.0/mcp/genie"

    session_id = "mcp-sess-" + str(uuid.uuid4())[:12]

    try:
        headers = _headers()
        headers["Accept"] = "application/json, text/event-stream"

        # Step 1: Initialize
        init_payload = {
            "jsonrpc": "2.0",
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {"tools": {}},
                "clientInfo": {"name": "DBXConnect-MigrationStudio", "version": "1.0"}
            },
            "id": 1
        }
        r = requests.post(endpoint_url, json=init_payload, headers=headers, timeout=30)
        init_resp = r.json()

        # Extract mcp-session-id from response headers
        mcp_session_hdr = r.headers.get("mcp-session-id", "")

        # Step 2: Send initialized notification
        notif_headers = dict(headers)
        if mcp_session_hdr:
            notif_headers["mcp-session-id"] = mcp_session_hdr
        notif_payload = {"jsonrpc": "2.0", "method": "notifications/initialized"}
        requests.post(endpoint_url, json=notif_payload, headers=notif_headers, timeout=10)

        # Step 3: List tools
        tools_headers = dict(headers)
        if mcp_session_hdr:
            tools_headers["mcp-session-id"] = mcp_session_hdr
        tools_payload = {"jsonrpc": "2.0", "method": "tools/list", "id": 2}
        tr = requests.post(endpoint_url, json=tools_payload, headers=tools_headers, timeout=20)
        tools_resp = tr.json()

        # Cache session
        _mcp_sessions[session_id] = {
            "endpoint": endpoint_url,
            "mcp_session_id": mcp_session_hdr,
            "tools": tools_resp.get("result", {}).get("tools", []),
            "request_counter": 10
        }

        return jsonify({
            "session_id": session_id,
            "server_info": init_resp.get("result", {}).get("serverInfo", {}),
            "tools": tools_resp.get("result", {}).get("tools", []),
            "mcp_session_id": mcp_session_hdr
        })

    except requests.exceptions.Timeout:
        return jsonify({"error": "MCP endpoint timeout during initialization"}), 504
    except Exception as exc:
        return jsonify({"error": f"MCP initialization failed: {str(exc)}"}), 500


def _extract_text_sql_data_from_result(result):
    result_text = ""
    result_sql = ""
    result_data = None
    if isinstance(result, dict):
        content_items = result.get("content", [])
        for item in content_items:
            if isinstance(item, dict):
                if item.get("type") == "text":
                    result_text = item.get("text", result_text or "")
                elif item.get("type") == "resource":
                    res = item.get("resource", {})
                    if "text" in res:
                        try:
                            parsed = json.loads(res["text"])
                            if "sql" in parsed:
                                result_sql = parsed["sql"]
                            if "data" in parsed:
                                result_data = parsed["data"]
                        except Exception:
                            result_text += ("\n" if result_text else "") + res["text"]
            elif isinstance(item, str):
                result_text += item
        if not content_items and "text" in result:
            result_text = result.get("text", result_text)
    elif isinstance(result, str):
        result_text = result
    return result_text, result_sql, result_data


def _parse_mcp_response(resp, content_type):
    result_text = ""
    result_sql = ""
    result_data = None
    if "text/event-stream" in content_type:
        last_event_data = None
        for line in resp.iter_lines(decode_unicode=True):
            if not line:
                continue
            if line.startswith("data:"):
                event_data = line[5:].strip()
                if event_data:
                    try:
                        last_event_data = json.loads(event_data)
                    except json.JSONDecodeError:
                        result_text += event_data
        if last_event_data:
            result = last_event_data.get("result", {})
            result_text, result_sql, result_data = _extract_text_sql_data_from_result(result)
        return result_text, result_sql, result_data

    resp_json = resp.json()
    if resp_json.get("error"):
        err = resp_json["error"]
        err_msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
        raise ValueError(f"MCP error: {err_msg}")
    result = resp_json.get("result", {})
    return _extract_text_sql_data_from_result(result)


def _poll_if_in_progress(result_text, result_sql, result_data, endpoint_url, headers, tool_name, req_counter):
    """If genie_ask returned async in-progress, poll using genie_poll_response tool."""
    try:
        payload = json.loads(result_text) if result_text and result_text.strip().startswith("{") else None
    except Exception:
        payload = None

    if not isinstance(payload, dict):
        return result_text, result_sql, result_data

    conversation_id = payload.get("conversation_id")
    response_id = payload.get("response_id")
    status = str(payload.get("status", "")).lower()
    if not conversation_id or not response_id or status not in {"in_progress", "queued", "running"}:
        return result_text, result_sql, result_data

    # Use genie_poll_response tool (NOT genie_ask) to check status
    for attempt in range(40):  # ~80s max
        time.sleep(2)
        req_counter += 1
        poll_payload = {
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {
                "name": "genie_poll_response",
                "arguments": {
                    "conversation_id": conversation_id,
                    "response_id": response_id
                }
            },
            "id": req_counter
        }
        try:
            pr = requests.post(endpoint_url, json=poll_payload, headers=headers, timeout=90)
            p_text, p_sql, p_data = _parse_mcp_response(pr, pr.headers.get("content-type", ""))
        except Exception:
            time.sleep(3)
            continue
        try:
            p_obj = json.loads(p_text) if p_text and p_text.strip().startswith("{") else None
        except Exception:
            p_obj = None
        if isinstance(p_obj, dict) and str(p_obj.get("status", "")).lower() in {"in_progress", "queued", "running"}:
            continue
        # Got a real result
        return p_text, p_sql, p_data

    return "Genie One is still processing. Please try again shortly.", result_sql, result_data


@genie_bp.route("/api/v1/genie/mcp/query", methods=["POST"])
@login_required
def mcp_query():
    """
    Send a natural language question to Genie One via MCP tools/call.
    Supports both session-based (initialized) and direct (one-shot) modes.
    """
    data = request.get_json() or {}
    question = (data.get("content") or data.get("question") or "").strip()
    session_id = (data.get("session_id") or "").strip()
    endpoint_url = (data.get("endpoint_url") or "").strip()

    if not question:
        return jsonify({"error": "content/question is required"}), 400

    # Determine endpoint and session info
    mcp_session_hdr = ""
    req_counter = 10
    tool_name = "genie"  # Default tool name for Genie One

    if session_id and session_id in _mcp_sessions:
        sess = _mcp_sessions[session_id]
        endpoint_url = sess["endpoint"]
        mcp_session_hdr = sess.get("mcp_session_id", "")
        req_counter = sess.get("request_counter", 10) + 1
        sess["request_counter"] = req_counter
        # Use the first available tool name
        if sess.get("tools"):
            tool_name = sess["tools"][0].get("name", "genie")
    elif not endpoint_url:
        endpoint_url = f"{_HOST}/api/2.0/mcp/genie"

    # FAQ intercept for MCP too
    faq_answer = _check_faq(question)
    if faq_answer:
        req_id = "mcp-faq-" + str(uuid.uuid4())[:8]
        return jsonify({
            "request_id": req_id,
            "status": "COMPLETED",
            "result": {"text": faq_answer, "sql": None, "data": None}
        })

    try:
        headers = _headers()
        headers["Accept"] = "application/json"
        if mcp_session_hdr:
            headers["mcp-session-id"] = mcp_session_hdr

        # If no session, initialize first
        if not mcp_session_hdr:
            init_payload = {
                "jsonrpc": "2.0",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {"tools": {}},
                    "clientInfo": {"name": "DBXConnect-MigrationStudio", "version": "1.0"}
                },
                "id": req_counter
            }
            r = requests.post(endpoint_url, json=init_payload, headers=headers, timeout=30)
            mcp_session_hdr = r.headers.get("mcp-session-id", "")
            if mcp_session_hdr:
                headers["mcp-session-id"] = mcp_session_hdr
            req_counter += 1

            # Send initialized notification
            notif_payload = {"jsonrpc": "2.0", "method": "notifications/initialized"}
            requests.post(endpoint_url, json=notif_payload, headers=headers, timeout=10)
            req_counter += 1

            # List tools to discover the tool name
            tools_payload = {"jsonrpc": "2.0", "method": "tools/list", "id": req_counter}
            tr = requests.post(endpoint_url, json=tools_payload, headers=headers, timeout=20)
            req_counter += 1
            try:
                tools_data = tr.json()
                tools_list = tools_data.get("result", {}).get("tools", [])
                if tools_list:
                    tool_name = tools_list[0].get("name", "genie")
            except Exception:
                pass

        # Resolve space_id for genie_ask (required parameter)
        default_space_id = ""
        try:
            spaces = _load_spaces()
            if spaces:
                default_space_id = spaces[0].get("space_id", "")
        except Exception:
            pass

        # Build tool arguments — genie_ask requires both question AND space_id
        tool_args = {"question": question}
        if default_space_id:
            tool_args["space_id"] = default_space_id

        # Call the Genie tool
        call_payload = {
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": tool_args
            },
            "id": req_counter
        }

        # Use synchronous request (no stream) — MCP endpoint should return
        # the final result directly when the tool finishes. Longer timeout
        # because Genie queries can take 30-60s.
        r = requests.post(endpoint_url, json=call_payload, headers=headers, timeout=180)

        # Handle response — use helper functions for parsing + async poll
        content_type = r.headers.get("content-type", "")
        result_text, result_sql, result_data = _parse_mcp_response(r, content_type)
        result_text, result_sql, result_data = _poll_if_in_progress(
            result_text, result_sql, result_data,
            endpoint_url, headers, tool_name, req_counter
        )

        req_id = "mcp-" + str(uuid.uuid4())[:8]
        if session_id and session_id in _mcp_sessions:
            _mcp_sessions[session_id]["mcp_session_id"] = mcp_session_hdr
            _mcp_sessions[session_id]["request_counter"] = req_counter
        elif mcp_session_hdr:
            new_sess_id = "mcp-sess-" + str(uuid.uuid4())[:12]
            _mcp_sessions[new_sess_id] = {
                "endpoint": endpoint_url,
                "mcp_session_id": mcp_session_hdr,
                "tools": [],
                "request_counter": req_counter
            }
            session_id = new_sess_id

        return jsonify({
            "request_id": req_id,
            "session_id": session_id or "",
            "status": "COMPLETED",
            "result": {"text": result_text, "sql": result_sql, "data": result_data}
        })

        if False:  # Dead code marker — old SSE parsing replaced by _parse_mcp_response
            pass
        if "text/event-stream_DEAD" in content_type:
            # Parse SSE events to extract the final result
            result_text = ""
            result_sql = ""
            result_data = None
            last_event_data = None

            for line in r.iter_lines(decode_unicode=True):
                if not line:
                    continue
                if line.startswith("data:"):
                    event_data = line[5:].strip()
                    if event_data:
                        try:
                            last_event_data = json.loads(event_data)
                        except json.JSONDecodeError:
                            result_text += event_data

            # Process the final event data
            if last_event_data:
                result = last_event_data.get("result", {})
                if isinstance(result, dict):
                    content_items = result.get("content", [])
                    for item in content_items:
                        if item.get("type") == "text":
                            result_text = item.get("text", result_text)
                        elif item.get("type") == "resource":
                            # May contain SQL or data
                            res = item.get("resource", {})
                            if "text" in res:
                                try:
                                    parsed = json.loads(res["text"])
                                    if "sql" in parsed:
                                        result_sql = parsed["sql"]
                                    if "data" in parsed:
                                        result_data = parsed["data"]
                                except Exception:
                                    result_text += "\n" + res["text"]
                elif isinstance(result, str):
                    result_text = result

            req_id = "mcp-" + str(uuid.uuid4())[:8]
            # Cache session for reuse
            if session_id and session_id in _mcp_sessions:
                _mcp_sessions[session_id]["mcp_session_id"] = mcp_session_hdr
                _mcp_sessions[session_id]["request_counter"] = req_counter
            elif mcp_session_hdr:
                new_sess_id = "mcp-sess-" + str(uuid.uuid4())[:12]
                _mcp_sessions[new_sess_id] = {
                    "endpoint": endpoint_url,
                    "mcp_session_id": mcp_session_hdr,
                    "tools": [],
                    "request_counter": req_counter
                }
                session_id = new_sess_id

            return jsonify({
                "request_id": req_id,
                "session_id": session_id,
                "status": "COMPLETED",
                "result": {"text": result_text, "sql": result_sql, "data": result_data}
            })

        else:
            # Direct JSON response
            resp_json = r.json()
            result = resp_json.get("result", {})
            result_text = ""
            result_sql = ""
            result_data = None

            if isinstance(result, dict):
                content_items = result.get("content", [])
                for item in content_items:
                    if isinstance(item, dict):
                        if item.get("type") == "text":
                            result_text = item.get("text", "")
                        elif item.get("type") == "resource":
                            res = item.get("resource", {})
                            if "text" in res:
                                try:
                                    parsed = json.loads(res["text"])
                                    if "sql" in parsed:
                                        result_sql = parsed["sql"]
                                    if "data" in parsed:
                                        result_data = parsed["data"]
                                except Exception:
                                    result_text += "\n" + res["text"]
                    elif isinstance(item, str):
                        result_text += item
                # If no content array, check for direct text
                if not content_items and "text" in result:
                    result_text = result["text"]
            elif isinstance(result, str):
                result_text = result

            # Handle error responses
            if resp_json.get("error"):
                err = resp_json["error"]
                err_msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                return jsonify({"error": f"MCP error: {err_msg}"}), 400

            req_id = "mcp-" + str(uuid.uuid4())[:8]
            if session_id and session_id in _mcp_sessions:
                _mcp_sessions[session_id]["request_counter"] = req_counter

            return jsonify({
                "request_id": req_id,
                "session_id": session_id or "",
                "status": "COMPLETED",
                "result": {"text": result_text, "sql": result_sql, "data": result_data}
            })

    except requests.exceptions.Timeout:
        return jsonify({"error": "MCP Genie request timed out. Try a simpler question."}), 504
    except Exception as exc:
        return jsonify({"error": f"MCP query failed: {str(exc)}"}), 500
