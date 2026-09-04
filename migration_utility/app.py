"""
Flask Backend — SQL to Databricks Migration Studio (Databricks Native App)

Runs as a Databricks App with proxy-based authentication.
All route logic lives in routes/*.py blueprints.
"""

import os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Windows' default console codepage (cp1252) can't encode the emoji/box-
# drawing characters this codebase's print()/log statements use (e.g.
# workflow_manager.py's background-thread status messages), which raises
# UnicodeEncodeError and can silently kill whatever was printing -- this
# was reproduced locally (a background hydration thread died on it).
# Databricks Apps runs on Linux/UTF-8 so this never triggers there; this
# only matters for `python app.py` on a Windows dev machine, but costs
# nothing either way.
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

from flask import Flask, redirect, request, jsonify, g
from flask_compress import Compress
from log_config import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

# ── Blueprints ────────────────────────────────────────────────────────────────
from routes.auth       import auth_bp
from routes.pages      import pages_bp
from routes.convert    import convert_bp
from routes.databricks import databricks_bp
from routes.source     import source_bp
from routes.healer     import healer_bp
from routes.workflow   import workflow_bp
from routes.scheduler  import scheduler_bp, start_scheduler
from routes.reports    import reports_bp
from routes.schema     import schema_bp
from routes.settings   import settings_bp
from routes.datamodel  import datamodel_bp
from routes.admin      import admin_bp
from routes.discovery  import discovery_bp
from routes.genie      import genie_bp
from routes.preflight  import preflight_bp
from routes.catalog_discovery import catalog_discovery_bp
from routes.migration_infra import migration_infra_bp
from persistence       import init_db
from identity          import get_current_user
from audit             import register_audit_hooks

# ── App factory ───────────────────────────────────────────────────────────────
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
app.config["COMPRESS_MIMETYPES"] = [
    "text/html", "text/css", "text/javascript",
    "application/javascript", "application/json",
]
app.config["COMPRESS_MIN_SIZE"] = 512
Compress(app)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "migration-studio-secret-change-me")
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_HTTPONLY"] = True

# Register all blueprints
app.register_blueprint(auth_bp)
app.register_blueprint(pages_bp)
app.register_blueprint(convert_bp)
app.register_blueprint(databricks_bp)
app.register_blueprint(source_bp)
app.register_blueprint(healer_bp)
app.register_blueprint(workflow_bp)
app.register_blueprint(scheduler_bp)
app.register_blueprint(reports_bp)
app.register_blueprint(schema_bp)
app.register_blueprint(settings_bp)
app.register_blueprint(datamodel_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(discovery_bp)
app.register_blueprint(genie_bp)
app.register_blueprint(preflight_bp)

# ── Override fm_chat: Hybrid approach (Genie Space API + Claude fallback) ──
# Per-workspace value — NEVER hardcode a Genie Space ID, it does not exist in
# other clients' workspaces. Resolved at runtime from (in order): env var,
# saved app config, or empty (falls back to Claude-only mode until configured).
def _resolve_genie_space_id() -> str:
    env_val = os.environ.get("GENIE_SPACE_ID", "").strip()
    if env_val:
        return env_val
    try:
        from config_cache import get_config
        return (get_config() or {}).get("genie_space_id", "").strip()
    except Exception:
        return ""

_GENIE_SPACE_ID = _resolve_genie_space_id()
if not _GENIE_SPACE_ID:
    logger.warning("GENIE_SPACE_ID not configured — Genie Space queries disabled until set (Settings or one-click deploy).")

def _query_genie_space(question):
    """Route question through Genie Space API using SDK auth (same as Playground MCP)."""
    import time
    from databricks.sdk import WorkspaceClient
    if not _GENIE_SPACE_ID:
        return None
    try:
        w = WorkspaceClient()
        # Start conversation using SDK api_client (handles M2M OAuth correctly)
        start_resp = w.api_client.do(
            "POST", f"/api/2.0/genie/spaces/{_GENIE_SPACE_ID}/start-conversation",
            body={"content": question}
        )
        conv_id = start_resp.get("conversation_id", "")
        msg_id = start_resp.get("message_id", "")
        if not conv_id or not msg_id:
            return None
        # Poll for results (up to 60s)
        for _ in range(20):
            time.sleep(3)
            poll_resp = w.api_client.do(
                "GET", f"/api/2.0/genie/spaces/{_GENIE_SPACE_ID}/conversations/{conv_id}/messages/{msg_id}"
            )
            status = poll_resp.get("status", "")
            if status == "COMPLETED":
                attachments = poll_resp.get("attachments", [])
                result_text = ""
                result_sql = ""
                for att in attachments:
                    if "text" in att:
                        result_text = att["text"].get("content", "")
                    if "query" in att:
                        result_sql = att["query"].get("query", "")
                return {"text": result_text, "sql": result_sql, "status": "COMPLETED"}
            elif status in ("FAILED", "CANCELLED"):
                return None
        return None  # Timeout
    except Exception:
        return None

# ── Token Optimiser: Intent Classifier + Response Cache ──────────────────────
import re as _re, hashlib as _hashlib, time as _time
from collections import OrderedDict as _OrderedDict

_DATA_PATTERNS = [r'\b(show|list|count|how many|get|find|select|query|fetch|total|number)\b',
                  r'\b(table|column|row|record|data|job|pipeline|run|migration|status)\b',
                  r'\b(last|recent|today|yesterday|this week|failed|success|running)\b',
                  r'\b(average|sum|total|max|min|group by|order by|where|between)\b']
_HOWTO_PATTERNS = [r'\b(how to|how do i|what is|explain|help|guide|steps|tutorial|why)\b',
                   r'\b(configure|setup|install|create|build|deploy|connect|difference)\b']

def _classify_intent(question):
    q = question.lower().strip()
    data_score = sum(1 for p in _DATA_PATTERNS if _re.search(p, q))
    howto_score = sum(1 for p in _HOWTO_PATTERNS if _re.search(p, q))
    if data_score >= 2: return 'data_query'
    if howto_score >= 2: return 'how_to'
    return 'general'

# Tiered system prompts
_PROMPT_MINIMAL = ("You are the AI assistant for DBX Migration Studio (SQL-to-Databricks migration tool). "
                   "Answer concisely about migration workflows, Databricks concepts, and SQL conversion.")

def _prompt_data_slim() -> str:
    """Same intent as _PROMPT_MINIMAL's tiering (a short, token-cheap
    prompt for data_query intent) but the table prefix is resolved live
    from Settings instead of hardcoded to admin_source.configtables --
    a fixed string here would tell the model the wrong catalog the moment
    a deployment is configured differently.
    """
    from routes.genie import resolve_configured_catalogs
    meta = resolve_configured_catalogs()["metadata"]
    prefix = ".".join(meta) if all(meta) else "admin_source.configtables"
    return ("You are the AI assistant for DBX Migration Studio.\n"
        f"Key tables: {prefix}.wf_run_history (run_id,job_name,status[SUCCESS/FAILED/RUNNING],started_at,duration_sec,rows_processed,error_message), "
        f"{prefix}.wf_job_metadata (job_name,last_status,enabled,run_count,fail_count).\n"
        "Always use 3-part names (catalog.schema.table). Wrap SQL in ```sql blocks.")

# Simple LRU response cache
class _FMCache:
    def __init__(s, max_size=100, ttl=1800):
        s._c = _OrderedDict()
        s._max = max_size
        s._ttl = ttl
    def _key(s, q):
        n = _re.sub(r'\s+', ' ', q.lower().strip())
        for w in ['please','can you','show me','i want to','give me']: n = n.replace(w, '')
        return _hashlib.md5(n.strip().encode()).hexdigest()
    def get(s, q):
        k = s._key(q)
        if k in s._c:
            e = s._c[k]
            if _time.time() - e['t'] < s._ttl:
                s._c.move_to_end(k)
                return e['r']
            del s._c[k]
        return None
    def put(s, q, r):
        k = s._key(q)
        s._c[k] = {'r': r, 't': _time.time()}
        if len(s._c) > s._max: s._c.popitem(last=False)

_fm_cache = _FMCache()

def _compress_history(messages, max_msgs=3):
    """Keep only last N messages + topic summary of older ones."""
    if len(messages) <= max_msgs:
        return messages
    recent = messages[-max_msgs:]
    older = messages[:-max_msgs]
    topics = set()
    for m in older:
        c = m.get('content', '').lower()
        if 'sql' in c or 'query' in c: topics.add('SQL')
        if 'pipeline' in c: topics.add('pipelines')
        if 'job' in c or 'run' in c: topics.add('jobs')
        if 'migration' in c: topics.add('migration')
    summary = f"[Prior context: {', '.join(topics) if topics else 'general discussion'}]"
    return [{'role': 'system', 'content': summary}] + recent

def _fm_chat_sdk_override():
    """Chat with FM endpoint — with optional Token Optimiser."""
    from flask import request as req, jsonify as jfy
    from routes.catalog_discovery import get_relevant_schema_context
    data = req.get_json() or {}
    endpoint_name = (data.get("endpoint") or "").strip()
    content_text = (data.get("content") or "").strip()
    messages = data.get("messages", [])
    optimize_tokens = data.get("optimize_tokens", False)
    if not endpoint_name or not content_text:
        return jfy({"error": "endpoint and content are required"}), 400
    # === FULL system prompt (standard mode) ===
    # Table locations resolved live from Settings (Metadata Catalog + each
    # medallion/reconciliation/logging catalog) instead of being hardcoded
    # to admin_source/bronze.hr/silver.hr/... -- those go stale/wrong the
    # moment a deployment is configured with different catalogs, exactly
    # the class of bug already fixed for _fqn()/get_catalog_schema().
    from routes.genie import resolve_configured_catalogs
    _cats = resolve_configured_catalogs()
    _meta = ".".join(_cats["metadata"]) if all(_cats["metadata"]) else "admin_source.configtables"
    _bronze = ".".join(_cats["bronze"]) if all(_cats["bronze"]) else "bronze.hr"
    _silver = ".".join(_cats["silver"]) if all(_cats["silver"]) else "silver.hr"
    _log = ".".join(_cats["logging"]) if all(_cats["logging"]) else "loggingdetails.hr"
    _recon = ".".join(_cats["reconciliation"]) if all(_cats["reconciliation"]) else "reconciliation.hr"
    _sys_full = ("You are the AI assistant inside DBX Migration Studio, a SQL-to-Databricks migration accelerator.\n"
            "CRITICAL SQL RULES:\n"
            "1. ALWAYS use fully-qualified 3-part table names (catalog.schema.table) in ALL SQL.\n"
            "2. ONLY use tables from the schema below. NEVER invent table names.\n"
            "3. Wrap SQL in ```sql code blocks.\n\n"
            "=== AVAILABLE TABLES ===\n\n"
            f"[{_meta}] — Migration control tables:\n"
            f"  {_meta}.wf_run_history — Every pipeline/job run\n"
            "    Columns: run_id(str), job_id(str), job_name(str), stage(str), full_table(str), "
            "load_type(str), watermark_column(str), watermark_value(str), status(str), "
            "started_at(timestamp), completed_at(timestamp), duration_sec(double), rows_processed(bigint), error_message(str), logs(str)\n"
            "    status values: SUCCESS, FAILED, RUNNING, SKIPPED\n\n"
            f"  {_meta}.wf_job_metadata — Registered migration jobs\n"
            "    Columns: job_id(str), job_name(str), stage(str), group_id(str), table_schema(str), "
            "table_name(str), full_table(str), load_type(str), watermark_column(str), status(str), "
            "last_run_id(str), last_run_at(timestamp), last_status(str), run_count(int), fail_count(int), "
            "enabled(boolean), job_order(int), source_config(str), target_config(str), created_at(timestamp), updated_at(timestamp)\n\n"
            f"  {_meta}.wf_pipeline_metadata — Pipeline definitions\n"
            f"  {_meta}.wf_scheduler_config — Cron schedules\n"
            f"  {_meta}.wf_scheduler_history — Scheduler run history\n"
            f"  {_meta}.wf_source_tables — Discovered source tables\n"
            f"  {_meta}.wf_watermark_metadata — Incremental watermarks\n\n"
            f"[{_bronze}] — Raw ingested data: bronze_customers, bronze_products, bronze_stores, bronze_fact_sales_orders\n"
            f"[{_silver}] — Cleaned: customers, products, stores, fact_sales_orders, dimemployee\n"
            f"[{_log}] — executionlog\n"
            f"[{_recon}] — reconcilationdetails\n\n"
            "=== END TABLES ===\n\n"
            "Now answer the question using ONLY these tables:\n")

    # === TOKEN OPTIMISER LOGIC ===
    optimizations_applied = []
    token_comparison = None

    if optimize_tokens:
        # Phase 1: Check response cache
        cached = _fm_cache.get(content_text)
        if cached:
            optimizations_applied.append('cache_hit')
            return jfy({"text": cached['text'], "usage": cached.get('usage', {}),
                        "model": cached.get('model', ''), "endpoint": endpoint_name,
                        "optimization_applied": "Cache hit (0 tokens used)",
                        "token_comparison": {"standard_tokens": cached.get('standard_est', 1500),
                                            "optimised_tokens": 0, "tokens_saved": cached.get('standard_est', 1500),
                                            "savings_pct": 100}})

        # Phase 2: Intent classification
        intent = _classify_intent(content_text)
        optimizations_applied.append(f'intent:{intent}')

        # Phase 3: Select tiered prompt
        if intent == 'how_to':
            system_prompt = _PROMPT_MINIMAL
            optimizations_applied.append('prompt:minimal(~200tkns)')
        elif intent == 'data_query':
            system_prompt = _prompt_data_slim()
            optimizations_applied.append('prompt:data_slim(~400tkns)')
        else:
            system_prompt = _sys_full
            optimizations_applied.append('prompt:full')

        # Phase 4: History compression
        compressed_msgs = _compress_history(messages, max_msgs=3)
        optimizations_applied.append(f'history:{len(messages)}→{len(compressed_msgs)}')

        # Build optimised chat messages
        chat_messages = [{"role": "system", "content": system_prompt}]
        for msg in compressed_msgs:
            chat_messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})
        chat_messages.append({"role": "user", "content": content_text})

        # Phase 6: Lower max_tokens for focused queries
        _max_out = 1024 if intent == 'data_query' else 2048 if intent == 'how_to' else 4096
        optimizations_applied.append(f'max_out:{_max_out}')

        # Calculate token savings from optimization (chars/4 approximation for DELTA)
        _prompt_chars_saved = max(0, len(_sys_full) - len(system_prompt))
        _history_chars_saved = max(0, sum(len(m.get('content','')) for m in messages[-10:]) - sum(len(m.get('content','')) for m in compressed_msgs))
        _total_chars_saved = _prompt_chars_saved + _history_chars_saved
        _tokens_saved_estimate = _total_chars_saved // 4  # delta only
    else:
        # Standard mode (no optimization)
        system_context = get_relevant_schema_context(question=content_text)
        if "not yet complete" in system_context:
            system_context = ""
        chat_messages = [{"role": "system", "content": _sys_full + system_context}]
        for msg in messages[-10:]:
            chat_messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})
        chat_messages.append({"role": "user", "content": content_text})
        standard_est = 0
        optimised_est = 0

    # === CALL CLAUDE ===
    try:
        from databricks.sdk import WorkspaceClient
        import json as _json
        w = WorkspaceClient()
        _out_limit = _max_out if optimize_tokens else 4096
        payload = {"messages": chat_messages, "max_tokens": _out_limit}
        payload.pop("temperature", None)  # Claude rejects temperature
        raw = w.api_client.do("POST", f"/serving-endpoints/{endpoint_name}/invocations", body=payload)
        resp = _json.loads(raw.content) if hasattr(raw, "content") else raw
        choices = resp.get("choices", [])
        response_text = choices[0].get("message", {}).get("content", "") if choices else "No response"
        usage = resp.get("usage", {})
        actual_total = usage.get("total_tokens", 0) or (usage.get("prompt_tokens", 0) + usage.get("completion_tokens", 0))

        # Build response
        result = {"text": response_text,
                  "usage": {"prompt_tokens": usage.get("prompt_tokens", 0),
                            "completion_tokens": usage.get("completion_tokens", 0),
                            "total_tokens": actual_total},
                  "model": resp.get("model", endpoint_name), "endpoint": endpoint_name}

        if optimize_tokens:
            # Standard = what it WOULD cost without optimization
            # = actual tokens used + tokens we saved through prompt/history compression
            standard_total = actual_total + _tokens_saved_estimate
            tokens_saved = _tokens_saved_estimate
            savings_pct = int((tokens_saved / max(standard_total, 1)) * 100) if standard_total > 0 else 0
            # Ensure non-negative display (when no optimization kicks in, show 0%)
            savings_pct = max(0, min(99, savings_pct))
            result["token_comparison"] = {
                "standard_tokens": standard_total,
                "optimised_tokens": actual_total,
                "tokens_saved": tokens_saved,
                "savings_pct": savings_pct
            }
            result["optimization_applied"] = " | ".join(optimizations_applied)
            # Cache the response
            _fm_cache.put(content_text, {'text': response_text, 'usage': result['usage'],
                                          'model': result['model'], 'standard_est': standard_total})

        return jfy(result)
    except Exception as exc:
        err_str = str(exc)
        if "model-serving" in err_str or "403" in err_str or "PERMISSION" in err_str:
            return jfy({"error": "Permission denied: app lacks model-serving scope."}), 403
        return jfy({"error": err_str}), 500

# Replace the genie blueprint's fm_chat view with SDK-based version
from routes.auth import login_required as _login_req
app.view_functions["genie.fm_chat"] = _login_req(_fm_chat_sdk_override)

# ── FM SQL Execution: run SQL from Claude responses against the warehouse ─────
@app.route("/api/v1/genie/fm/execute-sql", methods=["POST"])
@_login_req
def _fm_execute_sql():
    """Execute SQL via Databricks SQL Statement Execution API."""
    data = request.get_json() or {}
    sql_text = (data.get("sql") or "").strip()
    if not sql_text:
        return jsonify({"error": "sql is required"}), 400
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        warehouse_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "d01073f7104f07ff")
        stmt = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql_text,
            wait_timeout="50s"
        )
        state_val = "UNKNOWN"
        if stmt.status and stmt.status.state:
            state_val = stmt.status.state.value if hasattr(stmt.status.state, "value") else str(stmt.status.state)
        if state_val == "FAILED":
            err_msg = stmt.status.error.message if stmt.status.error else "Query failed"
            return jsonify({"error": err_msg, "state": "FAILED"}), 400
        if state_val in ("CANCELED", "CLOSED"):
            return jsonify({"error": "Query was canceled", "state": state_val}), 400
        columns = []
        rows = []
        if stmt.manifest and stmt.manifest.schema and stmt.manifest.schema.columns:
            columns = [col.name for col in stmt.manifest.schema.columns]
        if stmt.result and stmt.result.data_array:
            rows = stmt.result.data_array
        return jsonify({"state": state_val, "columns": columns, "rows": rows[:200], "total_rows": len(rows), "truncated": len(rows) > 200})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

# ── Backward-compatible redirect: /api/* → /api/v1/* ─────────────────────────
@app.route("/api/<path:subpath>", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
def api_compat_redirect(subpath):
    dest = f"/api/v1/{subpath}"
    if request.query_string:
        dest += f"?{request.query_string.decode()}"
    return redirect(dest, code=307)

# ── Health endpoint (required by Databricks Apps) ─────────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "ok", "app": "migration-studio"}), 200

# ── Identity middleware ───────────────────────────────────────────────────────
@app.before_request
def _inject_user_identity():
    """Populate g.user from Databricks proxy on every request."""
    if request.path.startswith("/static/") or request.path in ("/health", "/favicon.ico"):
        return
    user = get_current_user()
    if user:
        g.user = user

# ── Audit trail ───────────────────────────────────────────────────────────────
register_audit_hooks(app)

# Initialise Delta table persistence on startup
init_db()

# ── Static asset caching ──────────────────────────────────────────────────────
@app.after_request
def add_cache_headers(response):
    if request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response

# ── Global error handlers ─────────────────────────────────────────────────────
@app.errorhandler(404)
def not_found(e):
    return jsonify({"success": False, "error": "Resource not found"}), 404

@app.errorhandler(405)
def method_not_allowed(e):
    return jsonify({"success": False, "error": "Method not allowed"}), 405

@app.errorhandler(500)
def internal_error(e):
    logger.exception("Unhandled 500 error")
    return jsonify({"success": False, "error": "Internal server error"}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    logger.exception("Unhandled exception: %s", e)
    return jsonify({"success": False, "error": "Internal server error"}), 500


# ============================================================================
#  Start background scheduler (runs in both dev and production)
# ============================================================================
start_scheduler()


# ============================================================================
#  Run Server (local development only — production uses gunicorn)
# ============================================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    logger.info("=" * 65)
    logger.info("  SQL -> Databricks Migration Studio (Databricks Native App)")
    logger.info("  URL : http://localhost:%d", port)
    logger.info("=" * 65)
    app.debug = True
    # use_reloader=False: the reloader's forked child process breaks in
    # sandboxed/CI terminals where Ctrl+C is broadcast to the whole console
    # process group (kills the child). Debugger/auto-reload on save is lost,
    # but manual restarts still work fine.
    app.run(host="0.0.0.0", port=port, debug=True, threaded=True, use_reloader=False)
