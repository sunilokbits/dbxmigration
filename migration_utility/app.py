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

def _query_genie_space(question):
    """Route question through Genie Space API using SDK auth (same as Playground MCP)."""
    import time
    from databricks.sdk import WorkspaceClient
    space_id = _resolve_genie_space_id()
    if not space_id:
        return None
    try:
        w = WorkspaceClient()
        # Start conversation using SDK api_client (handles M2M OAuth correctly)
        start_resp = w.api_client.do(
            "POST", f"/api/2.0/genie/spaces/{space_id}/start-conversation",
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
                "GET", f"/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}"
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
from threading import Lock as _Lock
import json as _json

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
    """Short instructions only; actual schema is appended from discovery."""
    return ("You are the AI assistant for DBX Migration Studio.\n"
        "Always use 3-part names (catalog.schema.table). Wrap SQL in ```sql blocks.")

# Process-local, bounded LRU. Exact text is part of the key, never normalized.
class _FMCache:
    def __init__(s, max_size=100, ttl=120):
        s._c = _OrderedDict()
        s._max = max_size
        s._ttl = ttl
        s._lock = _Lock()
    def _key(s, q, scope=''):
        return _hashlib.sha256(_json.dumps([scope, q], sort_keys=True).encode()).hexdigest()
    def get(s, q, scope=''):
        k = s._key(q, scope)
        with s._lock:
            if k in s._c:
                e = s._c[k]
                if _time.monotonic() - e['t'] < s._ttl:
                    s._c.move_to_end(k)
                    return dict(e['r'])
                del s._c[k]
        return None
    def put(s, q, r, scope=''):
        k = s._key(q, scope)
        with s._lock:
            s._c[k] = {'r': dict(r), 't': _time.monotonic()}
            s._c.move_to_end(k)
            while len(s._c) > s._max:
                s._c.popitem(last=False)

_fm_cache = _FMCache()

def _safe_fm_history(messages):
    """Never promote client text to system/developer/tool instructions."""
    if not isinstance(messages, list):
        return []
    return [{"role": m["role"], "content": m["content"][:8000]}
            for m in messages[-10:]
            if isinstance(m, dict) and m.get("role") in ("user", "assistant")
            and isinstance(m.get("content"), str) and m["content"].strip()]

def _compress_history(messages, max_msgs=3):
    """Keep recent safe turns without turning client history into a system role."""
    return _safe_fm_history(messages)[-max_msgs:] if max_msgs > 0 else []

def _fm_cacheable_question(content, data):
    """Fail closed: only explicitly standalone, static concept explanations.

    Intent heuristics are not safe enough to decide whether data is live.
    History (even rejected history) and conversation IDs always bypass caching.
    """
    if data.get("messages") or any(data.get(k) for k in (
            "conversation_id", "conversationId", "conversation", "history",
            "parent_message_id", "thread_id", "session_id", "follow_up", "is_followup")):
        return False
    return bool(_re.fullmatch(
        r"(?:what is|explain) (?:delta lake|unity catalog|a lakehouse|"
        r"medallion architecture|change data capture|a slowly changing dimension)[?.!]?",
        content, flags=_re.IGNORECASE))

def _fm_cache_scope(host, endpoint, cfg, catalogs, context):
    """Bind answers to authenticated identity, session, Settings and discovery."""
    from flask import session
    from uuid import uuid4
    from routes import catalog_discovery
    user = getattr(g, "user", None) or {}
    if not (user.get("user_id") or user.get("email")):
        return None
    # last_refreshed is the current discovery generation. Also honor an explicit
    # generation field if the discovery implementation provides one.
    with catalog_discovery._cache_lock:
        state = catalog_discovery._cache
        generation = (state.get("generation"), state.get("last_refreshed"))
        if not any(generation) or state.get("refresh_in_progress") or state.get("error"):
            return None
    if "_fm_cache_session" not in session:
        session["_fm_cache_session"] = uuid4().hex
    return _json.dumps({
        "user": [user.get(k) for k in ("user_id", "email", "role", "groups")],
        "session": session["_fm_cache_session"], "host": host, "endpoint": endpoint,
        "catalogs": catalogs,
        # Include ALL mappings, even layers not yet returned by the resolver.
        "configured_catalogs": {k: cfg.get(k) for k in (
            "metadata_catalog", "metadata_schema", "databricks_catalog", "databricks_schema")},
        "layers": (cfg.get("existing_setting") or {}).get("medallion_layer_mapping"),
        "discovery_generation": generation,
        "context": _hashlib.sha256(context.encode()).hexdigest(),
    }, sort_keys=True)

def _fm_chat_sdk_override():
    """Chat with FM endpoint — with optional Token Optimiser."""
    from flask import request as req, jsonify as jfy
    from routes.catalog_discovery import get_relevant_schema_context
    from routes.genie import (_build_configured_catalog_context, resolve_configured_catalogs,
                              _serving_headers, _known_metadata_schema_context)
    from config_cache import get_config, normalize_host
    from urllib.parse import quote, urlsplit
    import requests

    data = req.get_json(silent=True)
    if not isinstance(data, dict):
        return jfy({"error": "JSON object required"}), 400
    endpoint_name = data.get("endpoint")
    content_text = data.get("content")
    if (not isinstance(endpoint_name, str) or not endpoint_name.strip()
            or not isinstance(content_text, str) or not content_text.strip()):
        return jfy({"error": "endpoint and content are required"}), 400
    if len(endpoint_name) > 256 or len(content_text) > 16000:
        return jfy({"error": "endpoint or content exceeds the supported length"}), 400
    endpoint_name = endpoint_name.strip()
    # Keep content EXACT, including case, spaces and words, for payload and cache.
    messages = _safe_fm_history(data.get("messages", []))
    optimize_tokens = data.get("optimize_tokens") is True
    try:
        cfg = get_config() or {}
        host = normalize_host(cfg.get("databricks_host") or "")
        parsed_host = urlsplit(host)
        if (parsed_host.scheme != "https" or not parsed_host.hostname
                or parsed_host.username or parsed_host.password
                or parsed_host.path or parsed_host.query or parsed_host.fragment):
            return jfy({"error": "Configure a valid HTTPS Databricks workspace host in Settings."}), 400
        _cats = resolve_configured_catalogs()
        system_context = (_build_configured_catalog_context()
                          + _known_metadata_schema_context()
                          + get_relevant_schema_context(
                              question=content_text, top_n=6 if optimize_tokens else 15))
    except Exception:
        logger.warning("FM catalog/configuration context unavailable")
        return jfy({"error": "Configured catalog context is unavailable; retry after discovery is ready."}), 503

    _schema_rules = (
        "\nGround every SQL query strictly in the tables and columns listed above — this app's own "
        "metadata tables plus any live-discovered tables. These are real, authoritative tables; write "
        "clean, runnable SQL for migration, pipeline, run-history, reconciliation and audit questions. "
        "Use fully-qualified catalog.schema.table names exactly as shown and wrap every query in a ```sql "
        "code block so it renders with a Run button. "
        "Do NOT invent catalogs, schemas, tables, columns, or status values that are not listed above. "
        "If no tables are listed above at all, ask the user to finish configuration/discovery rather than "
        "guessing SQL. Schema metadata is not live query results: never claim to have executed SQL or "
        "fabricate row values.\n")
    _sys_full = (
        "You are the AI assistant inside DBX Migration Studio, a SQL-to-Databricks migration accelerator.\n"
        "Answer in a clear, concise, professional style. Explain migration workflows plainly, lead with a "
        "short direct answer, and always include a runnable ```sql query when the question is about data. "
        "Distinguish suggested SQL from executed results.\n")

    # === TOKEN OPTIMISER LOGIC ===
    optimizations_applied = []
    cache_scope = None
    if optimize_tokens and _fm_cacheable_question(content_text, data):
        try:
            cache_scope = _fm_cache_scope(host, endpoint_name, cfg, _cats, system_context)
        except Exception:
            # Unknown identity or discovery state must never share cached answers.
            cache_scope = None

    if optimize_tokens:
        # Phase 1: Check response cache
        cached = _fm_cache.get(content_text, scope=cache_scope) if cache_scope is not None else None
        if cached:
            optimizations_applied.append('cache_hit')
            return jfy({"text": cached['text'], "usage": {
                            "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
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
            optimizations_applied.append('prompt:minimal')
        elif intent == 'data_query':
            system_prompt = _prompt_data_slim()
            optimizations_applied.append('prompt:data_slim')
        else:
            system_prompt = _sys_full
            optimizations_applied.append('prompt:full')

        # Phase 4: History compression
        compressed_msgs = _compress_history(messages, max_msgs=3)
        optimizations_applied.append(f'history:{len(messages)}→{len(compressed_msgs)}')

        # Build optimised chat messages
        chat_messages = [{"role": "system", "content": system_prompt + _schema_rules + system_context}]
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
        # Do not fetch top_n=15 a second time just to estimate retrieval savings.
        optimizations_applied.append('schema:top6; savings estimate excludes schema reduction')
    else:
        # Standard mode (no optimization)
        chat_messages = [{"role": "system", "content": _sys_full + _schema_rules + system_context}]
        for msg in messages[-10:]:
            chat_messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})
        chat_messages.append({"role": "user", "content": content_text})

    # One bounded invocation; no SDK retries, auth retries, or redirects.
    try:
        _out_limit = _max_out if optimize_tokens else 4096
        payload = {"messages": chat_messages, "max_tokens": _out_limit}
        raw = requests.post(
            f"{host}/serving-endpoints/{quote(endpoint_name, safe='')}/invocations",
            headers=_serving_headers(), json=payload, timeout=(5, 60), allow_redirects=False)
        if raw.status_code in (401, 403):
            return jfy({"error": "Authentication failed or permission denied for model serving."}), raw.status_code
        if raw.status_code == 429:
            return jfy({"error": "Model serving rate limit reached; try again later."}), 429
        if not 200 <= raw.status_code < 300:
            return jfy({"error": f"Model serving request failed (HTTP {raw.status_code})."}), 502
        resp = raw.json()
        choices = resp.get("choices", [])
        raw_content = choices[0].get("message", {}).get("content", "") if choices else "No response"
        # Some serving endpoints (observed on newer models like Sonnet 5 --
        # Opus 4.6/4.7 happened to always return a plain string) return
        # `content` as a list of Anthropic-style content blocks
        # ([{"type": "text", "text": "..."}, ...]) instead of a plain
        # string. The frontend always expected a string (it calls
        # .replace() on it to render markdown) with no guard, so picking
        # one of those models crashed the chat with
        # "text.replace is not a function". This is the handler actually
        # invoked for /api/v1/genie/fm/chat (see the view_functions
        # override below), not routes/genie.py's fm_chat.
        if isinstance(raw_content, list):
            response_text = "".join(
                (str(block.get("text") or "") if isinstance(block, dict) else str(block))
                for block in raw_content
            )
        elif raw_content is None:
            response_text = ""
        else:
            response_text = str(raw_content)
        usage = resp.get("usage") or {}
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
            if cache_scope is not None and response_text:
                _fm_cache.put(content_text, {'text': response_text,
                    'model': result['model'], 'standard_est': standard_total}, scope=cache_scope)

        return jfy(result)
    except requests.exceptions.Timeout:
        return jfy({"error": "Model serving request timed out."}), 504
    except requests.exceptions.RequestException:
        return jfy({"error": "Unable to reach the model serving endpoint."}), 502
    except Exception:
        # Do not expose upstream response bodies, tokens, or credential errors.
        logger.warning("FM invocation failed")
        return jfy({"error": "Model serving authentication or response processing failed."}), 502

# Replace the genie blueprint's fm_chat view with the bounded runtime handler.
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
