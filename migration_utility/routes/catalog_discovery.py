"""
Catalog Discovery Module — Dynamic multi-catalog auto-discovery for Genie AI.

Features:
- Discovers configured catalog/schema pairs via information_schema (metadata only)
- Caches results with configurable TTL (auto-refresh)
- Detects new tables/schemas automatically on next refresh
- Provides schema context injection for Genie/MCP queries
- Supports SQL query execution across any discovered catalog
"""

import os
import re
import time
import threading
import logging
import math
from collections import OrderedDict
from datetime import datetime, timezone
from urllib.parse import quote
from flask import Blueprint, request, jsonify
from routes.auth import login_required

logger = logging.getLogger(__name__)

catalog_discovery_bp = Blueprint("catalog_discovery", __name__)

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════
_CACHE_TTL_SECONDS = int(os.environ.get("CATALOG_CACHE_TTL", "300"))  # 5 min default
_MAX_TABLES_PER_SCHEMA = int(os.environ.get("MAX_TABLES_PER_SCHEMA", "500"))
_SQL_HTTP_TIMEOUT = 10.0
_SQL_MAX_DEADLINE = 120.0
_MAX_COLUMN_ROWS = 20000
_runtime_local = threading.local()

# ══════════════════════════════════════════════════════════════════════════════
# IN-MEMORY CACHE
# ══════════════════════════════════════════════════════════════════════════════
_cache = {
    "catalogs": [],          # [{name, comment, owner}]
    "schemas": [],           # [{catalog, schema, comment}]
    "tables": [],            # [{catalog, schema, table, type, columns:[{name, type, comment}]}]
    "last_refreshed": None,  # ISO timestamp
    "refresh_in_progress": False,
    "error": None,
    "stats": {"total_catalogs": 0, "total_schemas": 0, "total_tables": 0},
    "scope": None,
    "generation": 0,
    "scan_id": None,
    "retrieval": {},
    "columns_truncated": False,
}
_cache_lock = threading.Lock()


# ══════════════════════════════════════════════════════════════════════════════
# TOKEN HELPER
# ══════════════════════════════════════════════════════════════════════════════
def _runtime_config():
    """Read Settings on use, never via this module's SQL transport.

    The guard also handles config hydration calling back into this module.
    Upstream get_config retains its own cross-worker freshness policy.
    """
    cfg = {}
    if not getattr(_runtime_local, "reading_config", False):
        _runtime_local.reading_config = True
        try:
            from config_cache import get_config
            cfg = dict(get_config() or {})
        except Exception:
            logger.warning("[CatalogDiscovery] Runtime Settings unavailable")
        finally:
            _runtime_local.reading_config = False
    host = str(cfg.get("databricks_host") or os.environ.get("DATABRICKS_HOST", "")).strip().rstrip("/")
    if host and not host.startswith(("https://", "http://")):
        host = "https://" + host
    return {
        "host": host,
        "warehouse": cfg.get("databricks_sql_warehouse_id") or cfg.get("sql_warehouse_id")
        or cfg.get("warehouse_id") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", ""),
        "token": cfg.get("databricks_token") or os.environ.get("DATABRICKS_TOKEN", ""),
    }


def _get_token(config=None):
    """Return a token string, not the SDK's authentication header mapping."""
    config = config if config is not None else _runtime_config()
    token = config.get("token")
    if isinstance(token, str) and token.strip() and token.strip() not in (
            "********", "***", "••••••••", "REPLACE_ME"):
        return token.strip()
    from databricks.sdk import WorkspaceClient
    auth = WorkspaceClient(host=config["host"], http_timeout_seconds=5,
                           retry_timeout_seconds=5).config.authenticate()
    if callable(auth):
        try:
            auth = auth()
        except TypeError:
            auth = auth(None)
    if isinstance(auth, dict):
        header = next((v for k, v in auth.items() if k.lower() == "authorization"), "")
        if isinstance(header, str) and header.lower().startswith("bearer ") and header[7:].strip():
            return header[7:].strip()
    raise RuntimeError("Databricks authentication did not provide a bearer token")


def _headers(config=None):
    return {"Authorization": f"Bearer {_get_token(config)}", "Content-Type": "application/json"}


def _configured_pairs():
    """Fail closed; include every complete resolver pair, including `app`."""
    try:
        from routes.genie import resolve_configured_catalogs
        pairs = set()
        for value in resolve_configured_catalogs().values():
            if not isinstance(value, (tuple, list)) or len(value) != 2:
                continue
            cat, schema = value
            if isinstance(cat, str) and isinstance(schema, str) and cat.strip() and schema.strip():
                pairs.add((cat.strip(), schema.strip()))
        return tuple(sorted(pairs))
    except Exception:
        return ()


def _sync_scope():
    """Invalidate before any read; an old generation may never publish a scan."""
    config = _runtime_config()
    scope = (config["host"], config["warehouse"], _configured_pairs())
    with _cache_lock:
        if _cache["scope"] != scope:
            _cache.update(scope=scope, generation=_cache["generation"] + 1,
                          catalogs=[], schemas=[], tables=[], last_refreshed=None,
                          refresh_in_progress=False, scan_id=None, error=None,
                          retrieval={}, columns_truncated=False,
                          stats={"total_catalogs": 0, "total_schemas": 0, "total_tables": 0})
            # Keep one lock order: discovery then vectors. Network I/O holds neither.
            with _embedding_cache_lock:
                _embedding_cache.clear()
        return config, scope, _cache["generation"]


# ══════════════════════════════════════════════════════════════════════════════
# SQL EXECUTION ENGINE (Databricks SQL Statement API)
# ══════════════════════════════════════════════════════════════════════════════
def _execute_sql(sql, warehouse_id=None, max_rows=1000, timeout=120):
    """Statement API with async submission and one monotonic polling deadline.

    Only inline result chunks are followed (never external result URLs). The
    row limit and deadline cover pagination too; truncation is reported.
    """
    import requests
    empty = {"columns": [], "data": [], "row_count": 0}
    try:
        budget = float(timeout)
        if not math.isfinite(budget) or budget <= 0:
            raise ValueError("SQL deadline must be positive and finite")
        budget = min(budget, _SQL_MAX_DEADLINE)
        deadline = time.monotonic() + budget
        config = getattr(_runtime_local, "scan_config", None) or _runtime_config()
        wh_id = warehouse_id or config["warehouse"]
        if not wh_id or not config["host"]:
            raise ValueError("No Databricks host or SQL warehouse configured in Settings")
        limit = max(1, min(int(max_rows), 100000))
        headers = _headers(config)
        url = f"{config['host']}/api/2.0/sql/statements"

        def send(method, target, **kwargs):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("SQL execution deadline exceeded")
            response = method(target, headers=headers, timeout=min(_SQL_HTTP_TIMEOUT, remaining),
                              allow_redirects=False, **kwargs)
            response.raise_for_status()
            if not 200 <= response.status_code < 300:
                raise RuntimeError(f"SQL HTTP status {response.status_code}")
            if time.monotonic() >= deadline:
                raise TimeoutError("SQL execution deadline exceeded")
            return response.json()

        resp = send(requests.post, url, json={
            "warehouse_id": wh_id, "statement": sql, "wait_timeout": "0s",
            "on_wait_timeout": "CONTINUE", "row_limit": limit, "format": "JSON_ARRAY",
        })
        stmt_id = resp.get("statement_id")
        status = resp.get("status", {}).get("state", "")
        while status in ("PENDING", "RUNNING"):
            if not stmt_id:
                raise RuntimeError("SQL response missing statement_id")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("SQL execution deadline exceeded")
            time.sleep(min(0.5, remaining))
            resp = send(requests.get, f"{url}/{quote(str(stmt_id), safe='')}")
            status = resp.get("status", {}).get("state", "")
        if status != "SUCCEEDED":
            message = resp.get("status", {}).get("error", {}).get("message")
            raise RuntimeError(message or f"SQL execution ended with status: {status or 'UNKNOWN'}")

        manifest = resp.get("manifest", {})
        columns = manifest.get("schema", {}).get("columns", [])
        result = resp.get("result") or {}
        rows = list(result.get("data_array") or [])
        truncated = bool(manifest.get("truncated") or result.get("truncated"))
        seen = set()
        while result.get("next_chunk_index") is not None and len(rows) < limit:
            index = int(result["next_chunk_index"])
            if not stmt_id or index in seen or index < 0:
                raise RuntimeError("Invalid SQL result chunk sequence")
            seen.add(index)
            result = send(requests.get, f"{url}/{quote(str(stmt_id), safe='')}/result/chunks/{index}")
            rows.extend(result.get("data_array") or [])
            truncated = truncated or bool(result.get("truncated"))
        truncated = truncated or len(rows) > limit or result.get("next_chunk_index") is not None
        truncated = truncated or int(manifest.get("total_row_count") or 0) > len(rows)
        rows = rows[:limit]
        return {"columns": [c.get("name", "") for c in columns],
                "column_types": [c.get("type_name", "") for c in columns],
                "data": rows, "row_count": len(rows), "truncated": truncated, "error": None}
    except Exception as exc:
        return {**empty, "error": str(exc)}


# ══════════════════════════════════════════════════════════════════════════════
# CATALOG DISCOVERY (Configured catalog/schema pairs)
# ══════════════════════════════════════════════════════════════════════════════
def _discover_catalogs():
    """Discover all accessible catalogs."""
    result = _execute_sql("SHOW CATALOGS", timeout=30)
    if result["error"]:
        return [], result["error"]
    catalogs = []
    for row in result["data"]:
        if row and row[0]:
            catalogs.append({"name": row[0]})
    return catalogs, None


def _discover_schemas(catalog_name):
    """Discover all schemas in a catalog."""
    sql = f"SHOW SCHEMAS IN {_identifier(catalog_name)}"
    result = _execute_sql(sql, timeout=30)
    if result["error"]:
        return []
    schemas = []
    for row in result["data"]:
        if row and row[0]:
            # Skip internal schemas
            if row[0] not in ("information_schema", "__databricks_internal"):
                schemas.append({"catalog": catalog_name, "schema": row[0]})
    return schemas


def _identifier(value):
    return "`" + value.replace("`", "``") + "`"


def _literal(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _discover_tables(catalog_name, schema_name):
    """Discover all tables in a schema with column details."""
    sql = f"""
    SELECT table_name, table_type
    FROM {_identifier(catalog_name)}.information_schema.tables
    WHERE table_schema = {_literal(schema_name)}
    AND table_type IN ('MANAGED', 'EXTERNAL', 'VIEW', 'BASE TABLE')
    ORDER BY table_name
    LIMIT {_MAX_TABLES_PER_SCHEMA}
    """
    result = _execute_sql(sql, max_rows=_MAX_TABLES_PER_SCHEMA, timeout=60)
    if result["error"]:
        raise RuntimeError(result["error"])

    tables = []
    for row in result["data"]:
        if row and row[0]:
            tables.append({
                "catalog": catalog_name,
                "schema": schema_name,
                "table": row[0],
                "type": row[1] if len(row) > 1 else "TABLE",
                "columns": []
            })
    return tables


def _discover_columns(catalog_name, schema_name, table_name):
    """Get column details for a table."""
    sql = f"""
    SELECT column_name, data_type, comment
    FROM {_identifier(catalog_name)}.information_schema.columns
    WHERE table_schema = {_literal(schema_name)} AND table_name = {_literal(table_name)}
    ORDER BY ordinal_position
    """
    result = _execute_sql(sql, timeout=30)
    if result["error"]:
        return []
    columns = []
    for row in result["data"]:
        if row:
            columns.append({
                "name": row[0] if len(row) > 0 else "",
                "type": row[1] if len(row) > 1 else "",
                "comment": row[2] if len(row) > 2 else ""
            })
    return columns


def _discover_schema_columns(catalog_name, schema_name):
    """One bounded metadata query per schema, not one request per table."""
    sql = f"""
    SELECT table_name, column_name, data_type, comment
    FROM {_identifier(catalog_name)}.information_schema.columns
    WHERE table_schema = {_literal(schema_name)}
    ORDER BY table_name, ordinal_position
    """
    result = _execute_sql(sql, max_rows=_MAX_COLUMN_ROWS, timeout=60)
    if result["error"]:
        raise RuntimeError(result["error"])
    columns = {}
    for row in result["data"]:
        if len(row) >= 3:
            columns.setdefault(row[0], []).append({
                "name": row[1], "type": row[2], "comment": row[3] if len(row) > 3 else ""})
    return columns, bool(result.get("truncated"))


def _full_discovery(include_columns=False, catalogs_filter=None):
    """Scan configured pairs only; explicit catalog filters can only narrow.

    Capture connection + generation once. A Settings change cancels publication,
    including A -> B -> A changes observed while the scan is running.
    """
    config, scope, generation = _sync_scope()
    scan_id = object()
    with _cache_lock:
        if _cache["generation"] != generation or _cache["refresh_in_progress"]:
            return
        _cache.update(refresh_in_progress=True, scan_id=scan_id)
    previous_config = getattr(_runtime_local, "scan_config", None)
    _runtime_local.scan_config = config
    try:
        pairs = [p for p in scope[2] if catalogs_filter is None or p[0] in catalogs_filter]
        all_schemas, all_tables, failed_pairs = [], [], []
        columns_truncated = False
        for catalog, schema in pairs:
            if _sync_scope()[2] != generation:
                return
            # Tolerate a single inaccessible/stale pair (e.g. a bronze/silver
            # catalog the app's own principal has no grant on -> 403): skip it
            # and keep the pairs that DID resolve, instead of letting one
            # failure abort the whole scan and leave Genie with no schema at
            # all -- previously a permission gap on one configured catalog
            # wiped even the accessible metadata catalog's tables.
            try:
                tables = _discover_tables(catalog, schema)
                if include_columns and tables:
                    columns, truncated = _discover_schema_columns(catalog, schema)
                    columns_truncated = columns_truncated or truncated
                    for table in tables:
                        table["columns"] = columns.get(table["table"], [])
            except Exception as exc:
                failed_pairs.append(f"{catalog}.{schema}: {exc}")
                logger.warning("[CatalogDiscovery] Skipped inaccessible %s.%s: %s", catalog, schema, exc)
                continue
            all_schemas.append({"catalog": catalog, "schema": schema})
            all_tables.extend(tables)
        # Total failure — every configured pair errored (e.g. an unreachable
        # warehouse or all catalogs ungranted). Do NOT publish an empty
        # "successful" scan: surface it as an error and leave last_refreshed
        # untouched so it retries instead of masking a permission/transport
        # problem as an authoritative "no tables exist".
        if pairs and failed_pairs and not all_schemas:
            raise RuntimeError("; ".join(failed_pairs))
        all_catalogs = [{"name": name} for name in sorted({s["catalog"] for s in all_schemas})]
        if _sync_scope()[2] != generation:
            return
        with _cache_lock:
            if _cache["generation"] == generation and _cache["scan_id"] is scan_id:
                _cache.update(catalogs=all_catalogs, schemas=all_schemas, tables=all_tables,
                              last_refreshed=datetime.now(timezone.utc).isoformat(), error=None,
                              columns_truncated=columns_truncated,
                              stats={"total_catalogs": len(all_catalogs),
                                     "total_schemas": len(all_schemas), "total_tables": len(all_tables)})
    except Exception as exc:
        _sync_scope()
        with _cache_lock:
            if _cache["generation"] == generation and _cache["scan_id"] is scan_id:
                _cache["error"] = str(exc)
        logger.warning("[CatalogDiscovery] Scan failed: %s", exc)
    finally:
        _runtime_local.scan_config = previous_config
        with _cache_lock:
            if _cache["scan_id"] is scan_id:
                _cache.update(refresh_in_progress=False, scan_id=None)


def _ensure_cache_fresh():
    """Check if cache needs refresh and trigger background refresh if stale."""
    _sync_scope()
    with _cache_lock:
        last = _cache["last_refreshed"]
        in_progress = _cache["refresh_in_progress"]

    if in_progress:
        return

    if last is None:
        # Never refreshed — do it now
        t = threading.Thread(target=_full_discovery, kwargs={"include_columns": True}, daemon=True)
        t.start()
        return

    # Check TTL
    try:
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        age = (datetime.now(last_dt.tzinfo) - last_dt).total_seconds()
        if age > _CACHE_TTL_SECONDS:
            t = threading.Thread(target=_full_discovery, kwargs={"include_columns": True}, daemon=True)
            t.start()
    except Exception:
        pass


def get_schema_context():
    """Legacy context entry point shares the same strict scope and chunk bounds."""
    return get_relevant_schema_context(top_n=_PRESELECT_LIMIT)


# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA RELEVANCE RANKING — chunking + caching + vector similarity
#
# Metadata-only chunks and vectors are process-local; doc_qa and business
# rows are deliberately not read or persisted by this retrieval cache.
# ══════════════════════════════════════════════════════════════════════════════
_EMBED_ENDPOINT = "databricks-gte-large-en"
_EMBED_TIMEOUT = 5.0
_EMBED_COOLDOWN = 60.0
_PRESELECT_LIMIT = 40
_MAX_CHUNK_CHARS = 1600
_MAX_QUESTION_CHARS = 2000
_VECTOR_CACHE_LIMIT = 512
_CIRCUIT_LIMIT = 32
_QUESTION_CACHE_TTL_SECONDS = 600
_embedding_cache = OrderedDict()  # (host, model, scope, kind, exact text) -> (ts, vector)
_embedding_circuits = OrderedDict()  # endpoint/scope -> {busy, until}
_embedding_cache_lock = threading.Lock()


def _embed_texts(texts: list, config=None, scope=None, stats=None) -> "list | None":
    """One bounded batch, fail-fast circuit and no lock held during HTTP.

    Concurrent cold requests fall back lexically instead of queueing behind
    another embedding call. Failed endpoints are not retried during cooldown.
    """
    if not texts:
        return []
    stats = stats if stats is not None else {}
    config = config if config is not None else _runtime_config()
    key = (config["host"], _EMBED_ENDPOINT, scope)
    now = time.monotonic()
    with _embedding_cache_lock:
        state = _embedding_circuits.get(key)
        if state and (state["busy"] or state["until"] > now):
            stats["fallback_reason"] = "embedding_busy" if state["busy"] else "embedding_cooldown"
            return None
        if key not in _embedding_circuits and len(_embedding_circuits) >= _CIRCUIT_LIMIT:
            disposable = next((k for k, v in _embedding_circuits.items() if not v["busy"]), None)
            if disposable is None:
                stats["fallback_reason"] = "embedding_busy"
                return None
            del _embedding_circuits[disposable]
        state = {"busy": True, "until": 0.0}
        _embedding_circuits[key] = state
        _embedding_circuits.move_to_end(key)
    succeeded = False
    try:
        import requests
        if not config["host"]:
            raise ValueError("No embedding host configured")
        headers = _headers(config)
        remaining = _EMBED_TIMEOUT - (time.monotonic() - now)
        if remaining <= 0:
            raise TimeoutError("Embedding authentication exceeded deadline")
        stats["embedding_requests"] = stats.get("embedding_requests", 0) + 1
        response = requests.post(
            f"{config['host']}/serving-endpoints/{quote(_EMBED_ENDPOINT, safe='')}/invocations",
            headers=headers, json={"input": texts}, timeout=remaining, allow_redirects=False)
        response.raise_for_status()
        if response.status_code != 200 or time.monotonic() - now >= _EMBED_TIMEOUT:
            raise RuntimeError("Embedding request failed or exceeded deadline")
        data = response.json().get("data", [])
        if len(data) != len(texts):
            raise ValueError("Embedding response length mismatch")
        if any("index" in item for item in data):
            if sorted(item.get("index", -1) for item in data) != list(range(len(texts))):
                raise ValueError("Invalid embedding indices")
            data = sorted(data, key=lambda item: item["index"])
        vectors = [item.get("embedding") for item in data]
        dimension = len(vectors[0]) if vectors and isinstance(vectors[0], list) else 0
        if not 0 < dimension <= 8192 or any(
                not isinstance(v, list) or len(v) != dimension
                or any(not isinstance(x, (int, float)) or not math.isfinite(x) for x in v)
                or not any(v) for v in vectors):
            raise ValueError("Invalid embedding vectors")
        succeeded = True
        return vectors
    except Exception:
        stats["fallback_reason"] = "embedding_failed"
        logger.info("[SchemaRelevance] Embedding unavailable; using lexical ranking")
        return None
    finally:
        with _embedding_cache_lock:
            state.update(busy=False, until=0.0 if succeeded else time.monotonic() + _EMBED_COOLDOWN)


def _cosine_sim(a: list, b: list) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(y * y for y in b) ** 0.5
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _table_blurb(tbl: dict) -> str:
    """A bounded table chunk containing observed column names AND types."""
    columns = tbl.get("columns") or []
    text = f"{tbl['catalog']}.{tbl['schema']}.{tbl['table']} ({tbl.get('type', '')})"
    added = 0
    for column in columns[:24]:
        item = f"{str(column.get('name') or '')[:128]} {str(column.get('type') or 'UNKNOWN')[:128]}"
        suffix = (" columns: " if not added else ", ") + item
        if len(text) + len(suffix) > _MAX_CHUNK_CHARS - 40:
            break
        text += suffix
        added += 1
    if len(columns) > added:
        text += f" (+{len(columns) - added} columns omitted)"
    return text[:_MAX_CHUNK_CHARS]


def _batch_vectors(question, blurbs, config, scope, generation, stats):
    """Retrieve all hits, then batch only missing question/table vectors."""
    items = [("question", question)] + [("table", text) for text in blurbs]
    keys = [(config["host"], _EMBED_ENDPOINT, scope, kind, text) for kind, text in items]
    found, missing = {}, []
    now = time.monotonic()
    with _embedding_cache_lock:
        for key in keys:
            entry = _embedding_cache.get(key)
            ttl = _QUESTION_CACHE_TTL_SECONDS if key[-2] == "question" else _CACHE_TTL_SECONDS
            if entry and now - entry[0] < ttl:
                found[key] = entry[1]
                _embedding_cache.move_to_end(key)
            else:
                _embedding_cache.pop(key, None)
                if key not in missing:
                    missing.append(key)
    stats.update(vector_cache_hits=len(keys) - len(missing), vector_cache_misses=len(missing))
    if missing:
        vectors = _embed_texts([key[-1] for key in missing], config, scope, stats)
        if vectors:
            found.update(zip(missing, vectors))
            with _cache_lock:
                if _cache["generation"] == generation and _cache["scope"] == scope:
                    with _embedding_cache_lock:
                        for key, vector in zip(missing, vectors):
                            _embedding_cache[key] = (time.monotonic(), vector)
                            _embedding_cache.move_to_end(key)
                        while len(_embedding_cache) > _VECTOR_CACHE_LIMIT:
                            _embedding_cache.popitem(last=False)
    return [found.get(key) for key in keys]


def _words(text):
    # Split snake_case too so 'failed jobs' can find failed_jobs.
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def get_relevant_schema_context(question: str = "", top_n: int = 15, configured_only: bool = True) -> str:
    """Lexical preselection -> one batch -> vector ranking, strictly scoped.

    configured_only is retained for call compatibility, not a workspace-wide
    escape hatch. Missing configuration or zero matches always fail closed.
    """
    started = time.monotonic()
    _ensure_cache_fresh()
    config, scope, generation = _sync_scope()
    with _cache_lock:
        tables = list(_cache["tables"])
        last_refreshed = _cache["last_refreshed"]
    pairs = set(scope[2])
    scoped = [t for t in tables if (t.get("catalog"), t.get("schema")) in pairs]
    limit = max(0, min(int(top_n), _PRESELECT_LIMIT))
    question = question.strip()[:_MAX_QUESTION_CHARS]
    words = _words(question)
    candidates = [(_table_blurb(t), t) for t in scoped]
    candidates.sort(key=lambda item: (-len(words & _words(item[0])), item[0]))
    candidates = candidates[:_PRESELECT_LIMIT] if limit else []
    stats = {"total_discovered": len(tables), "scoped_tables": len(scoped),
             "preselected_tables": len(candidates), "returned_tables": 0,
             "embedding_requests": 0, "vector_cache_hits": 0, "vector_cache_misses": 0,
             "ranking": "lexical", "fallback_reason": None}
    if question and len(candidates) > limit and limit:
        vectors = _batch_vectors(question, [b for b, _ in candidates], config, scope, generation, stats)
        q_vec, table_vectors = vectors[0], vectors[1:]
        if q_vec and all(v and len(v) == len(q_vec) for v in table_vectors):
            scored = sorted(zip(candidates, table_vectors),
                            key=lambda item: _cosine_sim(q_vec, item[1]), reverse=True)
            candidates = [item[0] for item in scored]
            stats["ranking"] = "vector"
        elif not stats["fallback_reason"]:
            stats["fallback_reason"] = "incomplete_vectors"
    ranked = candidates[:limit]
    # Re-resolve after network I/O: don't return an old-scope prompt either.
    if _sync_scope()[2] != generation:
        return "(Settings changed during schema retrieval; retry with the current scope.)\n"
    stats.update(returned_tables=len(ranked), elapsed_ms=round((time.monotonic() - started) * 1000, 2))
    with _cache_lock:
        if _cache["generation"] != generation:
            return "(Settings changed during schema retrieval; retry with the current scope.)\n"
        _cache["retrieval"] = stats
    if not scoped:
        return "(No matching tables discovered in the configured catalog/schema pairs; no workspace fallback.)\n"
    lines = [f"Relevant tables (schema last refreshed: {last_refreshed}):\n"]
    lines.extend(f"  • {blurb}" for blurb, _ in ranked)
    lines.append(f"\n({len(scoped)} tables in configured scope; {len(candidates)} preselected; "
                 f"{len(ranked)} shown. Only the listed, observed names and types are supplied.)\n")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# FLASK ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@catalog_discovery_bp.route("/api/v1/catalog/discover", methods=["POST"])
@login_required
def trigger_discovery():
    """Trigger a fresh catalog discovery scan."""
    data = request.get_json(silent=True) or {}
    catalogs_filter = data.get("catalogs")  # Optional: limit to specific catalogs
    include_columns = data.get("include_columns", True)
    if catalogs_filter is not None and (
            not isinstance(catalogs_filter, list) or
            not all(isinstance(name, str) for name in catalogs_filter)):
        return jsonify({"error": "catalogs must be a list of catalog names"}), 400
    _sync_scope()

    with _cache_lock:
        if _cache["refresh_in_progress"]:
            return jsonify({"status": "already_running", "message": "Discovery scan already in progress"})

    t = threading.Thread(
        target=_full_discovery,
        kwargs={"include_columns": include_columns, "catalogs_filter": catalogs_filter},
        daemon=True
    )
    t.start()
    return jsonify({"status": "started", "message": "Catalog discovery started in background"})


@catalog_discovery_bp.route("/api/v1/catalog/status", methods=["GET"])
@login_required
def discovery_status():
    """Get current discovery cache status."""
    _ensure_cache_fresh()
    with _cache_lock:
        with _embedding_cache_lock:
            vector_entries = len(_embedding_cache)
        return jsonify({
            "last_refreshed": _cache["last_refreshed"],
            "refresh_in_progress": _cache["refresh_in_progress"],
            "stats": _cache["stats"],
            "error": _cache["error"],
            "cache_ttl_seconds": _CACHE_TTL_SECONDS,
            "generation": _cache["generation"],
            "configured_pairs": _cache["scope"][2] if _cache["scope"] else [],
            "columns_truncated": _cache["columns_truncated"],
            "max_tables_per_schema": _MAX_TABLES_PER_SCHEMA,
            "retrieval": dict(_cache["retrieval"]),
            "vector_cache_entries": vector_entries,
            "vector_cache_limit": _VECTOR_CACHE_LIMIT,
            "preselection_limit": _PRESELECT_LIMIT,
            "embedding_timeout_seconds": _EMBED_TIMEOUT,
        })


@catalog_discovery_bp.route("/api/v1/catalog/list", methods=["GET"])
@login_required
def list_catalogs():
    """List all discovered catalogs with their schemas and table counts."""
    _ensure_cache_fresh()
    with _cache_lock:
        catalogs = _cache["catalogs"]
        schemas = _cache["schemas"]
        tables = _cache["tables"]

    # Build summary per catalog
    result = []
    for cat in catalogs:
        cat_schemas = [s for s in schemas if s["catalog"] == cat["name"]]
        cat_tables = [t for t in tables if t["catalog"] == cat["name"]]
        result.append({
            "catalog": cat["name"],
            "schema_count": len(cat_schemas),
            "table_count": len(cat_tables),
            "schemas": [s["schema"] for s in cat_schemas]
        })

    return jsonify({"catalogs": result, "total": len(result)})


@catalog_discovery_bp.route("/api/v1/catalog/tables", methods=["GET"])
@login_required
def list_tables():
    """List tables — optionally filtered by catalog and/or schema."""
    _ensure_cache_fresh()
    catalog_filter = request.args.get("catalog", "").strip()
    schema_filter = request.args.get("schema", "").strip()
    search = request.args.get("search", "").strip().lower()

    with _cache_lock:
        tables = list(_cache["tables"])

    if catalog_filter:
        tables = [t for t in tables if t["catalog"] == catalog_filter]
    if schema_filter:
        tables = [t for t in tables if t["schema"] == schema_filter]
    if search:
        tables = [t for t in tables if search in t["table"].lower() or search in f"{t['catalog']}.{t['schema']}.{t['table']}".lower()]

    return jsonify({"tables": tables, "total": len(tables)})


@catalog_discovery_bp.route("/api/v1/catalog/table-details", methods=["GET"])
@login_required
def table_details():
    """Get column details for a specific table."""
    full_name = request.args.get("table", "").strip()
    if not full_name or full_name.count(".") < 2:
        return jsonify({"error": "Provide fully qualified table name: catalog.schema.table"}), 400

    parts = full_name.split(".", 2)
    catalog, schema, table = parts[0], parts[1], parts[2]
    _, scope, generation = _sync_scope()
    if (catalog, schema) not in scope[2]:
        return jsonify({"error": "Table is outside configured catalog/schema pairs"}), 403

    # Check cache first
    with _cache_lock:
        for t in _cache["tables"]:
            if t["catalog"] == catalog and t["schema"] == schema and t["table"] == table:
                if t.get("columns"):
                    return jsonify({"table": full_name, "columns": t["columns"]})

    # Fetch live if not in cache
    columns = _discover_columns(catalog, schema, table)
    if _sync_scope()[2] != generation:
        return jsonify({"error": "Settings changed during schema retrieval; retry"}), 409
    return jsonify({"table": full_name, "columns": columns})


@catalog_discovery_bp.route("/api/v1/sql/execute", methods=["POST"])
@login_required
def execute_sql_endpoint():
    """
    Execute any SQL query across multiple catalogs.
    Supports SELECT, SHOW, DESCRIBE, and any read query.
    Write queries (INSERT, UPDATE, CREATE) require explicit allow flag.
    """
    data = request.get_json(silent=True) or {}
    sql = (data.get("sql") or data.get("query") or "").strip()
    max_rows = min(int(data.get("max_rows", 200)), 10000)
    allow_writes = data.get("allow_writes", False)
    warehouse_id = data.get("warehouse_id")

    if not sql:
        return jsonify({"error": "sql field is required"}), 400

    # Safety check for write operations
    sql_upper = sql.upper().strip()
    write_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "MERGE", "TRUNCATE"]
    is_write = any(sql_upper.startswith(kw) for kw in write_keywords)

    if is_write and not allow_writes:
        return jsonify({
            "error": "Write operations require allow_writes=true. This is a safety check.",
            "sql": sql
        }), 403

    result = _execute_sql(sql, warehouse_id=warehouse_id, max_rows=max_rows)

    if result["error"]:
        return jsonify({"error": result["error"], "sql": sql}), 400

    return jsonify({
        "sql": sql,
        "columns": result["columns"],
        "column_types": result.get("column_types", []),
        "data": result["data"],
        "row_count": result["row_count"],
        "truncated": result.get("truncated", False)
    })


@catalog_discovery_bp.route("/api/v1/catalog/context", methods=["GET"])
@login_required
def get_context():
    """Return the dynamic schema context string for Genie/MCP injection."""
    context = get_schema_context()
    return jsonify({"context": context, "last_refreshed": _cache.get("last_refreshed")})
