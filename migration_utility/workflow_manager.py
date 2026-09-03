"""
Workflow Manager — Metadata-Driven Job Orchestration
=====================================================
Manages medallion pipeline jobs with:
  • Dynamic job creation per source table (Extract → Landing → Bronze → Silver)
  • Full / Incremental load support with watermark column tracking
  • Metadata tables for job registry, run history, and watermarks
  • Job CRUD operations (Add, Update, Delete, Rerun from failure)
  • Proper logging and failure tracking
  • **Databricks Unity Catalog persistence** — metadata stored in Delta tables
"""

import os
import uuid
import json
import logging
import threading
import requests
import time
from datetime import datetime
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

_DEPLOY_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "deployconfig.json")



# ═══════════════════════════════════════════════════════════════════════════════
#  Medallion Layer Resolution — ExistingSetting support
# ═══════════════════════════════════════════════════════════════════════════════

def resolve_layer_config(deploy_cfg, layer_name):
    """
    Resolve catalog, schema, storage, container, path for a medallion layer.
    
    If selected_setting == 'ExistingSetting', uses medallion_layer_mapping.
    Otherwise returns None (NewSetting mode — caller uses existing logic).
    
    Args:
        deploy_cfg: Full deploy config dict
        layer_name: 'landing', 'bronze', 'silver', 'reconciliation', or 'loggingdetails'
    
    Returns:
        dict with keys: catalog, schema, storage_account, container, base_path
        OR None if NewSetting mode (caller uses existing logic)
    """
    selected = deploy_cfg.get("selected_setting", "NewSetting")
    if selected != "ExistingSetting":
        return None  # Use existing/legacy logic
    
    existing = deploy_cfg.get("existing_setting", {})
    mapping = existing.get("medallion_layer_mapping", {})
    layer_cfg = mapping.get(layer_name, {})
    
    if not layer_cfg.get("catalog"):
        return None  # Layer not configured, fall back to legacy
    return layer_cfg


def get_layer_abfss_path(layer_cfg):
    """Build ABFSS path from layer config."""
    if not layer_cfg:
        return None
    storage = layer_cfg.get("storage_account", "")
    container = layer_cfg.get("container", "")
    base_path = layer_cfg.get("base_path", "")
    if not storage or not container:
        return None
    path = f"abfss://{container}@{storage}.dfs.core.windows.net"
    if base_path:
        path += f"/{base_path.strip('/')}"
    return path


def get_layer_catalog_schema(deploy_cfg, layer_name):
    """Get catalog.schema for a layer from ExistingSetting config.
    
    Returns (catalog, schema) tuple or (None, None) if not configured.
    Used by pipeline notebooks to determine target location.
    """
    layer_cfg = resolve_layer_config(deploy_cfg, layer_name)
    if not layer_cfg:
        return None, None
    return layer_cfg.get("catalog"), layer_cfg.get("schema")


def _resolve_databricks_token(dcfg=None):
    """Resolve Databricks token from Key Vault, then fall back to config."""
    try:
        from config_cache import get_databricks_token
        val = get_databricks_token()
        if val:
            return val
    except Exception:
        pass
    if dcfg:
        return dcfg.get("databricks_token", "")
    return ""


def _resolve_source_password(dcfg=None):
    """Resolve source password from Key Vault, then fall back to config."""
    try:
        from config_cache import get_source_password
        val = get_source_password()
        if val:
            return val
    except Exception:
        pass
    if dcfg:
        return dcfg.get("source", {}).get("password", "")
    return ""

def _load_deploy_config() -> dict:
    """Read deployconfig.json for fallback Databricks credentials."""
    try:
        if os.path.isfile(_DEPLOY_CONFIG_PATH):
            with open(_DEPLOY_CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def _save_deploy_config_field(key: str, value):
    """Upsert a top-level field in deployconfig.json."""
    try:
        cfg = _load_deploy_config()
        cfg[key] = value
        with open(_DEPLOY_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────────────────────
#  In-Memory Metadata Store  +  Databricks Delta persistence
# ─────────────────────────────────────────────────────────────────────────────
JOB_REGISTRY = OrderedDict()       # job_id → job metadata
JOB_RUNS = OrderedDict()           # run_id → run details
WATERMARKS = {}                    # table_name → {column, last_value, updated_at}
PIPELINE_GROUPS = OrderedDict()    # group_id → {table, jobs: [job_ids]}
SOURCE_TABLES = []                 # discovered source tables

# Fix 5: Secondary index for O(1) lookup by Databricks run id.
# Maps str(dbr_run_id) -> list of local run_ids. Maintained by
# _index_run_by_dbr()/_unindex_run_by_dbr() helpers — every write site of
# JOB_RUNS that assigns a dbr_run_id must call _index_run_by_dbr(). Safe
# by design: poller falls back to O(n) scan if index entry is missing, so
# stale/missing index rows never cause incorrect behaviour.
DBR_RUN_INDEX = {}                 # str(dbr_run_id) -> [run_id, ...]

def _index_run_by_dbr(run_id: str, dbr_run_id):
    """Register a local run under its Databricks run id (must be called under _lock)."""
    if not dbr_run_id:
        return
    key = str(dbr_run_id)
    bucket = DBR_RUN_INDEX.setdefault(key, [])
    if run_id not in bucket:
        bucket.append(run_id)

def _unindex_run_by_dbr(run_id: str, dbr_run_id=None):
    """Remove a local run from the index (must be called under _lock)."""
    if dbr_run_id is not None:
        keys = [str(dbr_run_id)]
    else:
        keys = list(DBR_RUN_INDEX.keys())
    for k in keys:
        bucket = DBR_RUN_INDEX.get(k)
        if not bucket:
            continue
        try:
            bucket.remove(run_id)
        except ValueError:
            pass
        if not bucket:
            DBR_RUN_INDEX.pop(k, None)

def _runs_for_dbr(dbr_run_str, grp_job_ids):
    """Return (run_id, run) pairs for the given Databricks run id that belong
    to the pipeline group's job ids. Uses DBR_RUN_INDEX for O(1) lookup;
    falls back to O(n) scan of JOB_RUNS if the index is empty/stale.
    Must be called while holding _lock.
    """
    bucket = DBR_RUN_INDEX.get(dbr_run_str)
    if bucket:
        pairs = []
        for rid in bucket:
            run = JOB_RUNS.get(rid)
            if run and run["job_id"] in grp_job_ids:
                pairs.append((rid, run))
        if pairs:
            return pairs
    # Fallback — safety net if index is missing an entry
    return [(rid, run) for rid, run in JOB_RUNS.items()
            if str(run.get("dbr_run_id", "")) == dbr_run_str and run["job_id"] in grp_job_ids]

# ── Lock for thread-safe writes ──
# Fix 9: Custom Read/Write lock. Backward compatible — using `with _lock:`
# acquires the WRITER lock (full mutual exclusion, identical to the prior
# threading.Lock behavior). Readers can opt-in via `with _lock.reader():`
# to allow concurrent reads while still blocking during writes. Writer
# preference prevents reader starvation under heavy read load.
class _RWLock:
    def __init__(self):
        self._cond = threading.Condition(threading.Lock())
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False

    def acquire_write(self):
        with self._cond:
            self._writers_waiting += 1
            while self._writer_active or self._readers > 0:
                self._cond.wait()
            self._writers_waiting -= 1
            self._writer_active = True

    def release_write(self):
        with self._cond:
            self._writer_active = False
            self._cond.notify_all()

    def acquire_read(self):
        with self._cond:
            while self._writer_active or self._writers_waiting > 0:
                self._cond.wait()
            self._readers += 1

    def release_read(self):
        with self._cond:
            self._readers -= 1
            if self._readers == 0:
                self._cond.notify_all()

    # Default context manager = writer (backward compatible with threading.Lock usage)
    def __enter__(self):
        self.acquire_write()
        return self
    def __exit__(self, exc_type, exc, tb):
        self.release_write()

    def reader(self):
        lock = self
        class _ReaderCtx:
            def __enter__(self_inner):
                lock.acquire_read()
                return lock
            def __exit__(self_inner, exc_type, exc, tb):
                lock.release_read()
        return _ReaderCtx()

_lock = _RWLock()

# ── Fix 8: Cap JOB_RUNS memory — OrderedDict keeps insertion order; oldest
#           entries evicted once the cap is exceeded. Before eviction, runs
#           are flushed to Delta so history survives. list_runs() falls back
#           to Delta query for historical lookups older than the in-memory
#           window. This prevents unbounded memory growth under high run
#           volumes (e.g. 10k+ runs over weeks). ──
_JOB_RUNS_MAX = 500

def _evict_old_runs_if_needed():
    """Trim JOB_RUNS to _JOB_RUNS_MAX by evicting oldest entries.
    Caller must hold _lock. Evicted runs are synced to Delta first
    (best-effort) so they remain queryable via list_runs_from_dbr().
    """
    if len(JOB_RUNS) <= _JOB_RUNS_MAX:
        return
    overflow = len(JOB_RUNS) - _JOB_RUNS_MAX
    to_evict = []
    for rid in list(JOB_RUNS.keys())[:overflow]:
        to_evict.append((rid, JOB_RUNS[rid]))
    for rid, run in to_evict:
        # Preserve in Delta before dropping from memory (best-effort)
        try:
            _sync_run_to_dbr(run)
        except Exception:
            pass
        _unindex_run_by_dbr(rid, run.get("dbr_run_id"))
        JOB_RUNS.pop(rid, None)


def list_runs_from_dbr(job_id: str = None, limit: int = 100) -> dict:
    """Fix 8: query Delta directly for historical runs beyond the in-memory window."""
    if not _metadata_initialized:
        return {"success": False, "error": "MetadataFlow not initialized"}
    where = f" WHERE job_id = {_esc(job_id)}" if job_id else ""
    cols = ("run_id, job_id, job_name, stage, full_table, load_type, status, "
            "started_at, completed_at, duration_sec, rows_processed, error_message, dbr_run_id")
    try:
        r = _exec_sql(
            f"SELECT {cols} FROM {_fqn(TBL_RUNS)}{where} "
            f"ORDER BY started_at DESC LIMIT {int(limit)}"
        )
        if r.get("status", {}).get("state") != "SUCCEEDED":
            return {"success": False, "error": r.get("status", {}).get("error", {}).get("message", "query failed")}
        cnames = [c["name"] for c in r.get("manifest", {}).get("schema", {}).get("columns", [])]
        rows = [dict(zip(cnames, row)) for row in r.get("result", {}).get("data_array", []) or []]
        return {"success": True, "runs": rows, "total": len(rows)}
    except Exception as e:
        return {"success": False, "error": str(e)}

# ── Fix 10: Bounded semaphore caps concurrent worker threads to prevent thread-storm
#           during bulk runs (e.g. 100 tables × 3 jobs = 300 threads). Threads acquire
#           on spawn and release in a try/finally wrapper. max_workers=20 keeps
#           interactive single-run latency near zero while capping burst load. ──
_MAX_WORKER_THREADS = 20
_thread_semaphore = threading.BoundedSemaphore(_MAX_WORKER_THREADS)

def _spawn_worker(target, args=(), kwargs=None, name=None):
    """Spawn a daemon worker thread guarded by the bounded semaphore.
    Acquires before start and releases when target() returns or raises.
    """
    kwargs = kwargs or {}
    def _runner():
        try:
            target(*args, **kwargs)
        except Exception:
            logger.exception("Worker thread %s crashed", name or getattr(target, "__name__", "?"))
        finally:
            try:
                _thread_semaphore.release()
            except ValueError:
                pass
    _thread_semaphore.acquire()
    t = threading.Thread(target=_runner, name=name, daemon=True)
    t.start()
    return t

# ── Pipeline completion callbacks (called from poller thread) ──
_pipeline_complete_callbacks = []

def on_pipeline_complete(fn):
    """Register callback fn(group_id, final_status) for when a Databricks run finishes."""
    _pipeline_complete_callbacks.append(fn)

# ── Databricks connection state (set via init_metadata_flow) ──
_dbr_host = None
_dbr_token = None
_dbr_catalog = None
_dbr_schema = None
_dbr_warehouse_id = None
_metadata_initialized = False

# ── Auto-restore connection state from deployconfig.json on module load ──
# Fix 7: Warehouse auto-detection now runs in a background daemon thread so
# module import never blocks on Databricks HTTP (15s timeout). An Event signals
# when restoration completes for callers that need to wait.
_config_ready = threading.Event()

def _discover_warehouse_async():
    """Background worker: discover warehouse via HTTP if not already set."""
    global _dbr_warehouse_id, _metadata_initialized
    try:
        if _dbr_host and _dbr_token and not _dbr_warehouse_id:
            try:
                s = requests.Session()
                s.headers.update({
                    "Authorization": f"Bearer {_dbr_token}",
                    "Content-Type": "application/json",
                })
                resp = s.get(f"{_dbr_host}/api/2.0/sql/warehouses", timeout=15)
                if resp.status_code == 200:
                    whs = resp.json().get("warehouses", [])
                    running = [w for w in whs if w.get("state") == "RUNNING"]
                    if running:
                        _dbr_warehouse_id = running[0]["id"]
                    elif whs:
                        _dbr_warehouse_id = whs[0]["id"]
            except Exception:
                pass
        if _dbr_host and _dbr_token and _dbr_catalog and _dbr_schema and _dbr_warehouse_id:
            _metadata_initialized = True
            # Auto-hydrate in-memory stores from Databricks Delta tables
            _auto_hydrate_from_dbr()
    finally:
        _config_ready.set()


def _auto_hydrate_from_dbr():
    """Silently load pipeline/job/watermark data from Databricks into memory.

    Called once after metadata_initialized is set during startup so that the
    in-memory JOB_REGISTRY, PIPELINE_GROUPS, and WATERMARKS survive app restarts.
    """
    try:
        result = load_metadata_from_dbr()
        if result.get("success"):
            loaded = result.get("loaded", {})
            print(
                f"✅ Auto-hydrated from Databricks: "
                f"{loaded.get('pipelines', 0)} pipelines, "
                f"{loaded.get('jobs', 0)} jobs, "
                f"{loaded.get('watermarks', 0)} watermarks"
            )
        else:
            print(f"⚠️ Auto-hydration skipped: {result.get('error', 'unknown')}")
    except Exception as e:
        print(f"⚠️ Auto-hydration failed (non-blocking): {e}")

def _restore_from_deploy_config():
    """Fast synchronous portion — load config file and populate in-memory globals.
    Warehouse discovery (HTTP) is dispatched to a background thread.
    """
    global _dbr_host, _dbr_token, _dbr_catalog, _dbr_schema, _dbr_warehouse_id, _metadata_initialized
    dcfg = _load_deploy_config()
    if dcfg:
        _dbr_host = _dbr_host or dcfg.get("databricks_host", "").rstrip("/") or None
        _dbr_token = _dbr_token or _resolve_databricks_token(dcfg) or None
        _dbr_catalog = _dbr_catalog or dcfg.get("metadata_catalog") or None
        _dbr_schema = _dbr_schema or dcfg.get("metadata_schema") or None
        # If warehouse was persisted in config, use it immediately (no HTTP needed)
        if not _dbr_warehouse_id:
            _dbr_warehouse_id = dcfg.get("warehouse_id") or dcfg.get("databricks_warehouse_id") or None
        # Fast-path: everything present, mark initialized right away
        if _dbr_host and _dbr_token and _dbr_catalog and _dbr_schema and _dbr_warehouse_id:
            _metadata_initialized = True
            _config_ready.set()
            # Auto-hydrate in background to avoid blocking module import
            t = threading.Thread(target=_auto_hydrate_from_dbr, name="dbr-auto-hydrate", daemon=True)
            t.start()
            return
    # Slow path — discover warehouse in background
    t = threading.Thread(target=_discover_warehouse_async, name="dbr-warehouse-discover", daemon=True)
    t.start()

_restore_from_deploy_config()

# ── Table names ──
TBL_PIPELINES    = "wf_pipeline_metadata"
TBL_JOBS         = "wf_job_metadata"
TBL_JOBS_HISTORY = "wf_job_metadatahis"
TBL_RUNS         = "wf_run_history"
TBL_WATERMARKS   = "wf_watermark_metadata"
TBL_SOURCES      = "wf_source_tables"
TBL_SCH_CONFIG   = "wf_scheduler_config"
TBL_SCH_HISTORY  = "wf_scheduler_history"


# ─────────────────────────────────────────────────────────────────────────────
#  DATABRICKS SQL EXECUTION HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _dbr_session():
    """Return requests session with auth headers.

    Resolves the token fresh on every call instead of trusting the frozen
    _dbr_token global captured once at process start (_restore_from_deploy_
    config runs only at module import). Otherwise rotating the token via
    Settings > Secret Vault leaves every page backed by _exec_sql (Job
    Manager, Pipeline Studio, ...) silently authenticating with the OLD
    token, while routes that call get_databricks_token() directly
    (Reconciliation, Scheduler) pick up the new one immediately -- exactly
    the "some pages work, some show a permission error" split.
    """
    s = requests.Session()
    token = _resolve_databricks_token() or _dbr_token
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    return s

def _exec_sql(sql: str, wait_timeout: str = "30s") -> dict:
    """Execute SQL on Databricks SQL Warehouse and return result."""
    if not _dbr_host or not _dbr_token or not _dbr_warehouse_id:
        return {"error": "Databricks not connected — run Create MetadataFlow first"}
    s = _dbr_session()
    payload = {
        "statement": sql,
        "warehouse_id": _dbr_warehouse_id,
        "catalog": _dbr_catalog or "main",
        "schema": _dbr_schema or "default",
        "wait_timeout": wait_timeout,
        "on_wait_timeout": "CONTINUE",
    }
    try:
        resp = s.post(f"{_dbr_host}/api/2.0/sql/statements", json=payload, timeout=60)
        data = resp.json() if resp.status_code == 200 else {"error": resp.text[:300]}
        sid = data.get("statement_id")
        if not sid:
            return data
        # Poll
        for _ in range(40):
            state = data.get("status", {}).get("state", "")
            if state in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
                break
            time.sleep(2)
            resp2 = s.get(f"{_dbr_host}/api/2.0/sql/statements/{sid}", timeout=15)
            data = resp2.json() if resp2.status_code == 200 else {"error": "poll error"}
        return data
    except Exception as e:
        return {"error": str(e)}

def _fqn(table: str) -> str:
    """Fully qualified table name."""
    c = _dbr_catalog or "main"
    s = _dbr_schema or "default"
    return f"`{c}`.`{s}`.`{table}`"


# ─────────────────────────────────────────────────────────────────────────────
#  METADATA FLOW — INITIALISE DELTA TABLES IN DATABRICKS
# ─────────────────────────────────────────────────────────────────────────────
def _find_warehouse(session) -> str:
    """Auto-detect a running SQL Warehouse."""
    try:
        resp = session.get(f"{_dbr_host}/api/2.0/sql/warehouses", timeout=15)
        if resp.status_code == 200:
            whs = resp.json().get("warehouses", [])
            running = [w for w in whs if w.get("state") == "RUNNING"]
            if running:
                return running[0]["id"]
            if whs:
                return whs[0]["id"]
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
#  STORAGE CREDENTIAL PRE-VALIDATION  (before DDL)
# ─────────────────────────────────────────────────────────────────────────────
def _prevalidate_storage_credentials(session):
    """Quick-check the configured storage credential (not all of them).

    Only validates the single credential from deployconfig.json to avoid
    iterating over dozens of unrelated credentials.  Non-fatal.
    """
    try:
        cfg = _load_deploy_config()
        cred_name = cfg.get("storage_credential_name") or cfg.get("access_connector", "")
        if not cred_name:
            return

        resp = session.get(
            f"{_dbr_host}/api/2.1/unity-catalog/storage-credentials/{cred_name}",
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning("Storage credential '%s' not found (%s) — skipping.", cred_name, resp.status_code)
            return

        cred = resp.json()
        ami = cred.get("azure_managed_identity", {})
        connector_id = ami.get("access_connector_id", "")
        if not connector_id:
            return

        # Single PATCH attempt — no retries, no sleeps
        logger.info("Pre-validating storage credential '%s'…", cred_name)
        patch_resp = session.patch(
            f"{_dbr_host}/api/2.1/unity-catalog/storage-credentials/{cred_name}",
            json={
                "azure_managed_identity": {"access_connector_id": connector_id},
                "skip_validation": False,
                "force": True,
            },
            timeout=15,
        )
        if 200 <= patch_resp.status_code < 300:
            logger.info("Storage credential '%s' validated OK.", cred_name)
        else:
            logger.warning("Credential '%s' validation returned %s — continuing anyway.",
                           cred_name, patch_resp.status_code)
    except Exception as e:
        logger.warning("Storage credential pre-validation skipped: %s", e)


def init_metadata_flow(host: str, token: str, catalog: str = "main",
                       schema: str = "default", warehouse_id: str = "") -> dict:
    """
    Provision the 5 metadataCalog.
    Tables are created IF NOT EXISTS so calling again is safe.
    """
    global _dbr_host, _dbr_token, _dbr_catalog, _dbr_schema, _dbr_warehouse_id, _metadata_initialized

    _dbr_host = host.rstrip("/")
    _dbr_token = token
    _dbr_catalog = catalog or "main"
    _dbr_schema = schema or "default"

    # Find warehouse
    s = _dbr_session()
    if warehouse_id:
        _dbr_warehouse_id = warehouse_id
    else:
        _dbr_warehouse_id = _find_warehouse(s)

    if not _dbr_warehouse_id:
        return {"success": False, "error": "No SQL Warehouse found. Start one in your Databricks workspace."}

    # ── Pre-validate storage credentials to avoid UC_CLOUD_STORAGE_ACCESS_FAILURE ──
    _prevalidate_storage_credentials(s)

    # Ensure schema exists
    _exec_sql(f"CREATE SCHEMA IF NOT EXISTS `{_dbr_catalog}`.`{_dbr_schema}`")

    # ── DDL for 5 metadata tables ──
    ddl_statements = [
        # 1. Pipeline metadata
        f"""CREATE TABLE IF NOT EXISTS {_fqn(TBL_PIPELINES)} (
            group_id         STRING NOT NULL,
            table_schema     STRING,
            table_name       STRING,
            full_table       STRING,
            load_type        STRING,
            watermark_column STRING,
            status           STRING,
            source_config    STRING,
            target_config    STRING,
            created_at       TIMESTAMP,
            updated_at       TIMESTAMP
        ) USING DELTA
        COMMENT 'Workflow pipeline groups — one row per source table'
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""",

        # 2. Job metadata
        f"""CREATE TABLE IF NOT EXISTS {_fqn(TBL_JOBS)} (
            job_id           STRING NOT NULL,
            job_name         STRING,
            stage            STRING,
            group_id         STRING,
            table_schema     STRING,
            table_name       STRING,
            full_table       STRING,
            load_type        STRING,
            watermark_column STRING,
            status           STRING,
            last_run_id      STRING,
            last_run_at      TIMESTAMP,
            last_status      STRING,
            run_count        INT,
            fail_count       INT,
            enabled          BOOLEAN,
            job_order        INT,
            source_config    STRING,
            target_config    STRING,
            created_at       TIMESTAMP,
            updated_at       TIMESTAMP
        ) USING DELTA
        COMMENT 'Individual jobs in the medallion pipeline'
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""",

        # 3. Run history
        f"""CREATE TABLE IF NOT EXISTS {_fqn(TBL_RUNS)} (
            run_id           STRING NOT NULL,
            job_id           STRING,
            job_name         STRING,
            stage            STRING,
            full_table       STRING,
            load_type        STRING,
            watermark_column STRING,
            watermark_value  STRING,
            status           STRING,
            started_at       TIMESTAMP,
            completed_at     TIMESTAMP,
            duration_sec     DOUBLE,
            rows_processed   BIGINT,
            error_message    STRING,
            logs             STRING
        ) USING DELTA
        COMMENT 'Job execution run history'""",

        # 4. Watermarks
        f"""CREATE TABLE IF NOT EXISTS {_fqn(TBL_WATERMARKS)} (
            table_name       STRING NOT NULL,
            watermark_column STRING,
            last_value       STRING,
            updated_at       TIMESTAMP
        ) USING DELTA
        COMMENT 'Watermark tracking for incremental loads'""",

        # 5a. Job metadata history (archive)
        f"""CREATE TABLE IF NOT EXISTS {_fqn(TBL_JOBS_HISTORY)} (
            history_id       STRING NOT NULL,
            job_id           STRING NOT NULL,
            job_name         STRING,
            stage            STRING,
            group_id         STRING,
            table_schema     STRING,
            table_name       STRING,
            full_table       STRING,
            load_type        STRING,
            watermark_column STRING,
            status           STRING,
            last_run_id      STRING,
            last_run_at      TIMESTAMP,
            last_status      STRING,
            run_count        INT,
            fail_count       INT,
            enabled          BOOLEAN,
            job_order        INT,
            source_config    STRING,
            target_config    STRING,
            created_at       TIMESTAMP,
            updated_at       TIMESTAMP,
            archived_at      TIMESTAMP,
            archive_reason   STRING
        ) USING DELTA
        COMMENT 'Archived job metadata — audit history for job changes'
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""",

        # 6. Source tables
        f"""CREATE TABLE IF NOT EXISTS {_fqn(TBL_SOURCES)} (
            source_id        STRING NOT NULL,
            source_type      STRING,
            server           STRING,
            database_name    STRING,
            table_schema     STRING,
            table_name       STRING,
            full_name        STRING,
            col_count        INT,
            row_estimate     BIGINT,
            discovered_at    TIMESTAMP
        ) USING DELTA
        COMMENT 'Discovered source tables from SQL Server'""",

        # 7. Scheduler configuration
        f"""CREATE TABLE IF NOT EXISTS {_fqn(TBL_SCH_CONFIG)} (
            schedule_id      STRING NOT NULL,
            table_name       STRING,
            table_schema     STRING,
            group_id         STRING,
            job_names        STRING,
            type             STRING,
            cron             STRING,
            interval_value   INT,
            interval_unit    STRING,
            once_at          STRING,
            schedule_desc    STRING,
            status           STRING,
            created_at       TIMESTAMP,
            last_run         TIMESTAMP,
            next_run         TIMESTAMP
        ) USING DELTA
        COMMENT 'Job scheduler configuration — one schedule per table'
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""",

        # 8. Scheduler execution history
        f"""CREATE TABLE IF NOT EXISTS {_fqn(TBL_SCH_HISTORY)} (
            history_id       STRING NOT NULL,
            schedule_id      STRING,
            table_name       STRING,
            jobs             STRING,
            trigger_type     STRING,
            result           STRING,
            details          STRING,
            executed_at      TIMESTAMP
        ) USING DELTA
        COMMENT 'Scheduler execution history — audit log for all scheduled runs'""",
    ]

    results = []
    errors = []
    # Fix 1: Parallel DDL — all 8 statements use IF NOT EXISTS, so order is irrelevant
    # and they can run concurrently against the SQL warehouse for ~8x speedup.
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(_exec_sql, ddl) for ddl in ddl_statements]
        for fut in as_completed(futures):
            try:
                r = fut.result()
            except Exception as e:
                errors.append(str(e))
                continue
            state = r.get("status", {}).get("state", "UNKNOWN")
            if "error" in r:
                errors.append(r["error"])
            elif state == "FAILED":
                err_msg = r.get("status", {}).get("error", {}).get("message", "DDL failed")
                errors.append(err_msg)
            else:
                results.append(state)

    if errors:
        # Detect storage access failures and give actionable guidance
        joined = "; ".join(errors)
        if "CLOUD_STORAGE_ACCESS_FAILURE" in joined or "AbfsRestOperationException" in joined:
            return {
                "success": False,
                "error": (
                    "Storage access failed — the Access Connector's RBAC role may still be "
                    "propagating (takes up to 10 minutes after Deploy Infrastructure). "
                    "Please wait a few minutes and click Create MetadataFlow again. "
                    f"Details: {joined[:300]}"
                ),
                "partial_results": results,
                "retry": True,
            }
        return {"success": False, "error": joined, "partial_results": results}

    _metadata_initialized = True

    # Persist metadata catalog/schema so they survive Flask restarts
    _save_deploy_config_field("metadata_catalog", _dbr_catalog)
    _save_deploy_config_field("metadata_schema", _dbr_schema)

    return {
        "success": True,
        "message": f"MetadataFlow created — 8 Delta tables provisioned in {_dbr_catalog}.{_dbr_schema}",
        "catalog": _dbr_catalog,
        "schema": _dbr_schema,
        "warehouse_id": _dbr_warehouse_id,
        "tables": [TBL_PIPELINES, TBL_JOBS, TBL_JOBS_HISTORY, TBL_RUNS, TBL_WATERMARKS, TBL_SOURCES, TBL_SCH_CONFIG, TBL_SCH_HISTORY],
    }


# ─────────────────────────────────────────────────────────────────────────────
#  CHECK METADATA STATUS
# ─────────────────────────────────────────────────────────────────────────────
def get_metadata_status() -> dict:
    """Check if metadata tables exist and return row counts."""
    if not _dbr_host or not _dbr_token:
        return {"success": True, "initialized": False, "message": "Databricks not connected"}

    tables = [TBL_PIPELINES, TBL_JOBS, TBL_JOBS_HISTORY, TBL_RUNS, TBL_WATERMARKS, TBL_SOURCES, TBL_SCH_CONFIG, TBL_SCH_HISTORY]
    tables_status = {}

    # Fix 2: Single UNION ALL query replaces 8 serial COUNTs for ~8x faster dashboard polling.
    # If the combined query fails (e.g. one table missing), gracefully fall back to per-table queries.
    union_sql = " UNION ALL ".join(
        [f"SELECT '{tbl}' AS tbl, COUNT(*) AS cnt FROM {_fqn(tbl)}" for tbl in tables]
    )
    r = _exec_sql(union_sql)
    state = r.get("status", {}).get("state", "")
    if state == "SUCCEEDED":
        rows = r.get("result", {}).get("data_array", []) or []
        row_map = {row[0]: int(row[1]) for row in rows if row and len(row) >= 2}
        for tbl in tables:
            if tbl in row_map:
                tables_status[tbl] = {"exists": True, "rows": row_map[tbl]}
            else:
                tables_status[tbl] = {"exists": False, "rows": 0}
    else:
        # Fallback: one table may not exist — query each independently
        for tbl in tables:
            r = _exec_sql(f"SELECT COUNT(*) AS cnt FROM {_fqn(tbl)}")
            tstate = r.get("status", {}).get("state", "")
            if tstate == "SUCCEEDED":
                trows = r.get("result", {}).get("data_array", [["0"]])
                tables_status[tbl] = {"exists": True, "rows": int(trows[0][0])}
            else:
                tables_status[tbl] = {"exists": False, "rows": 0}

    all_exist = all(v["exists"] for v in tables_status.values())
    return {
        "success": True,
        "initialized": all_exist and _metadata_initialized,
        "host": _dbr_host,
        "catalog": _dbr_catalog,
        "schema": _dbr_schema,
        "warehouse_id": _dbr_warehouse_id,
        "tables": tables_status,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  SYNC IN-MEMORY → DATABRICKS (called after each CRUD operation)
# ─────────────────────────────────────────────────────────────────────────────
def _esc(val):
    """Escape single quotes for SQL string literals."""
    if val is None:
        return "NULL"
    return "'" + str(val).replace("'", "''") + "'"

def _sync_pipeline_to_dbr(group: dict):
    """Upsert a pipeline group to Databricks."""
    if not _metadata_initialized:
        return
    try:
        sql = f"""MERGE INTO {_fqn(TBL_PIPELINES)} AS t
        USING (SELECT {_esc(group['group_id'])} AS group_id) AS s
        ON t.group_id = s.group_id
        WHEN MATCHED THEN UPDATE SET
            status = {_esc(group.get('status'))},
            load_type = {_esc(group.get('load_type'))},
            watermark_column = {_esc(group.get('watermark_column', ''))},
            source_config = {_esc(json.dumps(group.get('source_config') or {}))},
            target_config = {_esc(json.dumps(group.get('target_config') or {}))},
            updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            group_id, table_schema, table_name, full_table, load_type,
            watermark_column, status, source_config, target_config, created_at, updated_at
        ) VALUES (
            {_esc(group['group_id'])}, {_esc(group.get('table_schema'))},
            {_esc(group.get('table_name'))}, {_esc(group.get('full_table'))},
            {_esc(group.get('load_type'))}, {_esc(group.get('watermark_column', ''))},
            {_esc(group.get('status'))},
            {_esc(json.dumps(group.get('source_config') or {}))},
            {_esc(json.dumps(group.get('target_config') or {}))},
            current_timestamp(), current_timestamp()
        )"""
        _exec_sql(sql)
    except Exception:
        pass  # non-blocking

def _sync_job_to_dbr(job: dict):
    """Upsert a job to Databricks."""
    if not _metadata_initialized:
        return
    try:
        sql = f"""MERGE INTO {_fqn(TBL_JOBS)} AS t
        USING (SELECT {_esc(job['job_id'])} AS job_id) AS s
        ON t.job_id = s.job_id
        WHEN MATCHED THEN UPDATE SET
            status = {_esc(job.get('status'))},
            last_run_id = {_esc(job.get('last_run_id'))},
            last_run_at = {_esc(job.get('last_run_at'))},
            last_status = {_esc(job.get('last_status'))},
            run_count = {job.get('run_count', 0)},
            fail_count = {job.get('fail_count', 0)},
            enabled = {str(job.get('enabled', True)).lower()},
            load_type = {_esc(job.get('load_type'))},
            watermark_column = {_esc(job.get('watermark_column', ''))},
            source_config = {_esc(json.dumps(job.get('source_config') or {}))},
            target_config = {_esc(json.dumps(job.get('target_config') or {}))},
            updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            job_id, job_name, stage, group_id, table_schema, table_name, full_table,
            load_type, watermark_column, status, run_count, fail_count, enabled,
            job_order, source_config, target_config, created_at, updated_at
        ) VALUES (
            {_esc(job['job_id'])}, {_esc(job['job_name'])}, {_esc(job['stage'])},
            {_esc(job['group_id'])}, {_esc(job.get('table_schema'))},
            {_esc(job.get('table_name'))}, {_esc(job.get('full_table'))},
            {_esc(job.get('load_type'))}, {_esc(job.get('watermark_column', ''))},
            {_esc(job.get('status'))}, {job.get('run_count', 0)}, {job.get('fail_count', 0)},
            {str(job.get('enabled', True)).lower()}, {job.get('order', 1)},
            {_esc(json.dumps(job.get('source_config') or {}))},
            {_esc(json.dumps(job.get('target_config') or {}))},
            current_timestamp(), current_timestamp()
        )"""
        _exec_sql(sql)
    except Exception:
        pass

def _sync_run_to_dbr(run: dict):
    """Insert/update a run record to Databricks."""
    if not _metadata_initialized:
        return
    try:
        logs_str = json.dumps(run.get("logs", []))
        sql = f"""MERGE INTO {_fqn(TBL_RUNS)} AS t
        USING (SELECT {_esc(run['run_id'])} AS run_id) AS s
        ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET
            status = {_esc(run.get('status'))},
            completed_at = {_esc(run.get('completed_at'))},
            duration_sec = {run.get('duration_sec') or 'NULL'},
            rows_processed = {run.get('rows_processed', 0)},
            error_message = {_esc(run.get('error'))},
            logs = {_esc(logs_str)}
        WHEN NOT MATCHED THEN INSERT (
            run_id, job_id, job_name, stage, full_table, load_type,
            watermark_column, watermark_value, status, started_at, rows_processed, logs
        ) VALUES (
            {_esc(run['run_id'])}, {_esc(run['job_id'])}, {_esc(run.get('job_name'))},
            {_esc(run.get('stage'))}, {_esc(run.get('full_table'))},
            {_esc(run.get('load_type'))}, {_esc(run.get('watermark_column', ''))},
            {_esc(run.get('watermark_value'))}, {_esc(run.get('status'))},
            {_esc(run.get('started_at'))}, {run.get('rows_processed', 0)},
            {_esc(logs_str)}
        )"""
        _exec_sql(sql)
    except Exception:
        pass

def _sync_watermark_to_dbr(table_name: str, wm: dict):
    """Upsert watermark to Databricks."""
    if not _metadata_initialized:
        return
    try:
        sql = f"""MERGE INTO {_fqn(TBL_WATERMARKS)} AS t
        USING (SELECT {_esc(table_name)} AS table_name) AS s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET
            watermark_column = {_esc(wm.get('column'))},
            last_value = {_esc(wm.get('last_value'))},
            updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (table_name, watermark_column, last_value, updated_at)
        VALUES ({_esc(table_name)}, {_esc(wm.get('column'))}, {_esc(wm.get('last_value'))}, current_timestamp())"""
        _exec_sql(sql)
    except Exception:
        pass

def _delete_pipeline_from_dbr(group_id: str):
    """Delete pipeline and associated jobs from Databricks."""
    if not _metadata_initialized:
        return
    try:
        _exec_sql(f"DELETE FROM {_fqn(TBL_JOBS)} WHERE group_id = {_esc(group_id)}")
        _exec_sql(f"DELETE FROM {_fqn(TBL_PIPELINES)} WHERE group_id = {_esc(group_id)}")
    except Exception:
        pass

def _delete_job_from_dbr(job_id: str):
    """Delete a single job from Databricks."""
    if not _metadata_initialized:
        return
    try:
        _exec_sql(f"DELETE FROM {_fqn(TBL_JOBS)} WHERE job_id = {_esc(job_id)}")
        _exec_sql(f"DELETE FROM {_fqn(TBL_RUNS)} WHERE job_id = {_esc(job_id)}")
    except Exception:
        pass

def sync_source_tables_to_dbr(tables: list, source_config: dict) -> dict:
    """Store discovered source tables to Databricks."""
    if not _metadata_initialized:
        return {"success": False, "error": "MetadataFlow not initialized"}
    try:
        # Clear old entries for this source
        server = source_config.get("server", "")
        db = source_config.get("database", "")
        _exec_sql(f"DELETE FROM {_fqn(TBL_SOURCES)} WHERE server = {_esc(server)} AND database_name = {_esc(db)}")

        if not tables:
            return {"success": True, "synced": 0}

        # Fix 4: Single multi-row INSERT replaces N round-trips.
        # Chunk at 200 tables per statement to keep SQL payload under warehouse limits.
        src_type = source_config.get('source_type', 'sqlserver')
        total = 0
        CHUNK = 200
        for i in range(0, len(tables), CHUNK):
            chunk = tables[i:i+CHUNK]
            rows = []
            for t in chunk:
                sid = uuid.uuid4().hex[:12]
                rows.append(
                    f"({_esc(sid)}, {_esc(src_type)}, {_esc(server)}, {_esc(db)}, "
                    f"{_esc(t.get('schema','dbo'))}, {_esc(t.get('table',''))}, "
                    f"{_esc(t.get('full_name',''))}, {int(t.get('col_count',0) or 0)}, "
                    f"{int(t.get('row_estimate',0) or 0)}, current_timestamp())"
                )
            sql = f"INSERT INTO {_fqn(TBL_SOURCES)} VALUES " + ", ".join(rows)
            r = _exec_sql(sql)
            if r.get("status", {}).get("state") == "SUCCEEDED":
                total += len(chunk)
            else:
                # Fall back to per-row INSERT for this chunk if batch failed
                for t in chunk:
                    sid = uuid.uuid4().hex[:12]
                    fallback = f"""INSERT INTO {_fqn(TBL_SOURCES)} VALUES (
                        {_esc(sid)}, {_esc(src_type)},
                        {_esc(server)}, {_esc(db)},
                        {_esc(t.get('schema', 'dbo'))}, {_esc(t.get('table', ''))},
                        {_esc(t.get('full_name', ''))}, {int(t.get('col_count', 0) or 0)},
                        {int(t.get('row_estimate', 0) or 0)}, current_timestamp()
                    )"""
                    _exec_sql(fallback)
                total += len(chunk)
        return {"success": True, "synced": total}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
#  LOAD FROM DATABRICKS → IN-MEMORY (hydrate on connect)
# ─────────────────────────────────────────────────────────────────────────────
def load_metadata_from_dbr() -> dict:
    """Load all metadata from Databricks Delta tables into in-memory stores."""
    global JOB_REGISTRY, JOB_RUNS, WATERMARKS, PIPELINE_GROUPS
    if not _metadata_initialized:
        return {"success": False, "error": "MetadataFlow not initialized"}

    loaded = {"pipelines": 0, "jobs": 0, "runs": 0, "watermarks": 0}

    try:
        # Load pipelines
        r = _exec_sql(f"SELECT * FROM {_fqn(TBL_PIPELINES)} ORDER BY created_at")
        if r.get("status", {}).get("state") == "SUCCEEDED":
            cols = [c["name"] for c in r.get("manifest", {}).get("schema", {}).get("columns", [])]
            for row in r.get("result", {}).get("data_array", []):
                rec = dict(zip(cols, row))
                gid = rec["group_id"]
                src_cfg = {}
                tgt_cfg = {}
                try: src_cfg = json.loads(rec.get("source_config") or "{}")
                except: pass
                try: tgt_cfg = json.loads(rec.get("target_config") or "{}")
                except: pass
                with _lock:
                    PIPELINE_GROUPS[gid] = {
                        "group_id": gid,
                        "table_schema": rec.get("table_schema", ""),
                        "table_name": rec.get("table_name", ""),
                        "full_table": rec.get("full_table", ""),
                        "load_type": rec.get("load_type", "full"),
                        "watermark_column": rec.get("watermark_column", ""),
                        "job_ids": [],
                        "status": rec.get("status", "created"),
                        "source_config": src_cfg,
                        "target_config": tgt_cfg,
                        "created_at": rec.get("created_at", ""),
                    }
                loaded["pipelines"] += 1

        # Load jobs
        r = _exec_sql(f"SELECT * FROM {_fqn(TBL_JOBS)} ORDER BY created_at")
        if r.get("status", {}).get("state") == "SUCCEEDED":
            cols = [c["name"] for c in r.get("manifest", {}).get("schema", {}).get("columns", [])]
            for row in r.get("result", {}).get("data_array", []):
                rec = dict(zip(cols, row))
                jid = rec["job_id"]
                gid = rec.get("group_id", "")
                src_cfg = {}
                tgt_cfg = {}
                try: src_cfg = json.loads(rec.get("source_config") or "{}")
                except: pass
                try: tgt_cfg = json.loads(rec.get("target_config") or "{}")
                except: pass
                job = {
                    "job_id": jid,
                    "job_name": rec.get("job_name", ""),
                    "stage": rec.get("stage", ""),
                    "group_id": gid,
                    "table_schema": rec.get("table_schema", ""),
                    "table_name": rec.get("table_name", ""),
                    "full_table": rec.get("full_table", ""),
                    "load_type": rec.get("load_type", "full"),
                    "watermark_column": rec.get("watermark_column", ""),
                    "status": rec.get("status", "created"),
                    "last_run_id": rec.get("last_run_id"),
                    "last_run_at": rec.get("last_run_at"),
                    "last_status": rec.get("last_status"),
                    "run_count": int(rec.get("run_count", 0) or 0),
                    "fail_count": int(rec.get("fail_count", 0) or 0),
                    "created_at": rec.get("created_at", ""),
                    "updated_at": rec.get("updated_at", ""),
                    "source_config": src_cfg,
                    "target_config": tgt_cfg,
                    "order": int(rec.get("job_order", 1) or 1),
                    "enabled": str(rec.get("enabled", "true")).lower() in ("true", "1", "yes"),
                }
                with _lock:
                    JOB_REGISTRY[jid] = job
                    if gid in PIPELINE_GROUPS:
                        PIPELINE_GROUPS[gid]["job_ids"].append(jid)
                loaded["jobs"] += 1

        # Infer pipeline_mode from job stages (DLT pipelines have dlt_bronze_silver stage)
        with _lock:
            for gid, grp in PIPELINE_GROUPS.items():
                stages = {JOB_REGISTRY[jid].get("stage", "") for jid in grp.get("job_ids", []) if jid in JOB_REGISTRY}
                grp["pipeline_mode"] = "dlt" if "dlt_bronze_silver" in stages else "standard"

        # Load watermarks
        r = _exec_sql(f"SELECT * FROM {_fqn(TBL_WATERMARKS)}")
        if r.get("status", {}).get("state") == "SUCCEEDED":
            cols = [c["name"] for c in r.get("manifest", {}).get("schema", {}).get("columns", [])]
            for row in r.get("result", {}).get("data_array", []):
                rec = dict(zip(cols, row))
                tbl = rec["table_name"]
                with _lock:
                    WATERMARKS[tbl] = {
                        "column": rec.get("watermark_column", ""),
                        "last_value": rec.get("last_value"),
                        "updated_at": rec.get("updated_at", ""),
                    }
                loaded["watermarks"] += 1

        # Load run history — Fix 6: skip logs column (lazy-loaded on demand)
        run_cols_no_logs = (
            "run_id, job_id, job_name, stage, full_table, load_type, "
            "watermark_column, watermark_value, status, started_at, completed_at, "
            "duration_sec, rows_processed, error_message, dbr_run_id"
        )
        r = _exec_sql(f"SELECT {run_cols_no_logs} FROM {_fqn(TBL_RUNS)} ORDER BY started_at DESC LIMIT 200")
        if r.get("status", {}).get("state") == "SUCCEEDED":
            cols = [c["name"] for c in r.get("manifest", {}).get("schema", {}).get("columns", [])]
            for row in r.get("result", {}).get("data_array", []):
                rec = dict(zip(cols, row))
                rid = rec["run_id"]
                run_entry = {
                    "run_id":           rid,
                    "job_id":           rec.get("job_id", ""),
                    "job_name":         rec.get("job_name", ""),
                    "stage":            rec.get("stage", ""),
                    "full_table":       rec.get("full_table", ""),
                    "load_type":        rec.get("load_type", ""),
                    "watermark_column": rec.get("watermark_column", ""),
                    "watermark_value":  rec.get("watermark_value"),
                    "status":           rec.get("status", ""),
                    "started_at":       rec.get("started_at", ""),
                    "completed_at":     rec.get("completed_at"),
                    "duration_sec":     rec.get("duration_sec"),
                    "rows_processed":   int(rec.get("rows_processed", 0) or 0),
                    "error":            rec.get("error_message"),
                    "dbr_run_id":       rec.get("dbr_run_id"),
                    "logs":             [],
                    "logs_loaded":      False,
                }
                with _lock:
                    JOB_RUNS[rid] = run_entry
                    _index_run_by_dbr(rid, run_entry.get("dbr_run_id"))
                loaded["runs"] += 1

        return {"success": True, "loaded": loaded}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
#  FULL SYNC — flush all in-memory data to Databricks
# ─────────────────────────────────────────────────────────────────────────────
# Fix 3: async task registry — callers dispatch sync via start_full_sync_to_dbr()
# and poll get_full_sync_status(task_id). Synchronous full_sync_to_dbr() remains
# for backward compatibility (tests, CLI, scripted callers).
_sync_tasks = {}        # task_id -> {status, started_at, completed_at, synced, error, progress}
_sync_tasks_lock = threading.Lock()
_SYNC_TASKS_MAX = 20    # keep last N tasks only

def _run_full_sync(task_id: str):
    """Worker: perform full sync, updating task state in _sync_tasks."""
    def _set(**kw):
        with _sync_tasks_lock:
            if task_id in _sync_tasks:
                _sync_tasks[task_id].update(kw)

    synced = {"pipelines": 0, "jobs": 0, "runs": 0, "watermarks": 0}
    try:
        if not _metadata_initialized:
            _set(status="failed", error="MetadataFlow not initialized",
                 completed_at=datetime.now().isoformat())
            return
        _set(status="running", progress="pipelines")
        for gid, grp in list(PIPELINE_GROUPS.items()):
            try:
                _sync_pipeline_to_dbr(grp)
                synced["pipelines"] += 1
            except Exception as e:
                logger.warning("full_sync pipeline %s failed: %s", gid, e)
        _set(progress="jobs", synced=dict(synced))
        for jid, job in list(JOB_REGISTRY.items()):
            try:
                _sync_job_to_dbr(job)
                synced["jobs"] += 1
            except Exception as e:
                logger.warning("full_sync job %s failed: %s", jid, e)
        _set(progress="runs", synced=dict(synced))
        for rid, run in list(JOB_RUNS.items()):
            try:
                _sync_run_to_dbr(run)
                synced["runs"] += 1
            except Exception as e:
                logger.warning("full_sync run %s failed: %s", rid, e)
        _set(progress="watermarks", synced=dict(synced))
        for tbl, wm in list(WATERMARKS.items()):
            try:
                _sync_watermark_to_dbr(tbl, wm)
                synced["watermarks"] += 1
            except Exception as e:
                logger.warning("full_sync watermark %s failed: %s", tbl, e)
        _set(status="succeeded", progress="done", synced=synced,
             completed_at=datetime.now().isoformat())
    except Exception as e:
        logger.exception("full_sync task %s crashed", task_id)
        _set(status="failed", error=str(e), synced=synced,
             completed_at=datetime.now().isoformat())


def start_full_sync_to_dbr() -> dict:
    """Fix 3: dispatch a full sync in the background. Returns task_id for polling."""
    if not _metadata_initialized:
        return {"success": False, "error": "MetadataFlow not initialized"}
    task_id = f"sync-{uuid.uuid4().hex[:12]}"
    with _sync_tasks_lock:
        # Trim oldest
        if len(_sync_tasks) >= _SYNC_TASKS_MAX:
            oldest = sorted(_sync_tasks.items(), key=lambda kv: kv[1].get("started_at", ""))[0][0]
            _sync_tasks.pop(oldest, None)
        _sync_tasks[task_id] = {
            "task_id":      task_id,
            "status":       "queued",
            "started_at":   datetime.now().isoformat(),
            "completed_at": None,
            "synced":       {"pipelines": 0, "jobs": 0, "runs": 0, "watermarks": 0},
            "progress":     "queued",
            "error":        None,
        }
    _spawn_worker(_run_full_sync, args=(task_id,), name=f"full-sync-{task_id}")
    return {"success": True, "task_id": task_id, "status": "queued"}


def get_full_sync_status(task_id: str) -> dict:
    """Fix 3: poll status of a background full-sync task."""
    with _sync_tasks_lock:
        t = _sync_tasks.get(task_id)
        if not t:
            return {"success": False, "error": f"Task '{task_id}' not found"}
        return {"success": True, "task": dict(t)}


def full_sync_to_dbr() -> dict:
    """Write all in-memory metadata to Databricks (for bulk sync).
    Synchronous version — blocks caller until complete. Prefer
    start_full_sync_to_dbr() for HTTP callers to avoid request timeout.
    """
    if not _metadata_initialized:
        return {"success": False, "error": "MetadataFlow not initialized"}
    synced = {"pipelines": 0, "jobs": 0, "runs": 0, "watermarks": 0}
    for gid, grp in PIPELINE_GROUPS.items():
        _sync_pipeline_to_dbr(grp)
        synced["pipelines"] += 1
    for jid, job in JOB_REGISTRY.items():
        _sync_job_to_dbr(job)
        synced["jobs"] += 1
    for rid, run in JOB_RUNS.items():
        _sync_run_to_dbr(run)
        synced["runs"] += 1
    for tbl, wm in WATERMARKS.items():
        _sync_watermark_to_dbr(tbl, wm)
        synced["watermarks"] += 1
    return {"success": True, "synced": synced}


# ─────────────────────────────────────────────────────────────────────────────
#  JOB NAMING CONVENTION
# ─────────────────────────────────────────────────────────────────────────────
def _job_name(stage: str, table_name: str, target_config: dict = None) -> str:
    """
    Generate standard job name per convention:
      1. ExtractToVolumes_<Table>   — extract from SQL source → dev_volumes
      2. VolumesToBronze_<Table>    — dev_volumes → bronze.hr
      3. BronzeToSilver_<Table>     — bronze.hr → silver.hr

    Falls back to legacy naming when target_config is not provided:
      1. SqlExtract_<Table>         — extract from SQL source
      2. LandingToBronze_<Table>    — landing to bronze
      3. BronzeToSilver_<Table>     — bronze to silver
    """
    clean = table_name.replace(".", "_").replace("[", "").replace("]", "").strip()
    tc = target_config or {}
    vol_cat = tc.get("volumes_catalog", "")
    brz_cat = tc.get("bronze_catalog", "")
    slv_cat = tc.get("silver_catalog", "")

    if vol_cat and brz_cat and slv_cat:
        # Multi-catalog medallion naming — includes schema for differentiation
        # e.g. ExtractTo_volume_data_it_customers vs ExtractTo_volume_data_hr_customers
        tgt_sch = tc.get("target_schema", "")
        sch_tag = f"_{tgt_sch}" if tgt_sch else ""
        prefix_map = {
            "extract":           f"ExtractTo_{vol_cat}{sch_tag}_{clean}",
            "landing_to_bronze":  f"{vol_cat}_To_{brz_cat}{sch_tag}_{clean}",
            "bronze_to_silver":   f"{brz_cat}_To_{slv_cat}{sch_tag}_{clean}",
            "dlt_bronze_silver":  f"SDP_{vol_cat}_To_{slv_cat}{sch_tag}_{clean}",
        }
    else:
        # Legacy naming
        prefix_map = {
            "extract":           f"SqlExtract_{clean}",
            "landing_to_bronze":  f"LandingToBronze_{clean}",
            "bronze_to_silver":   f"BronzeToSilver_{clean}",
            "dlt_bronze_silver":  f"SDP_BronzeToSilver_{clean}",
        }
    return prefix_map.get(stage, f"{stage}_{clean}")


# ─────────────────────────────────────────────────────────────────────────────
#  SCHEDULER — DATABRICKS CRUD
# ─────────────────────────────────────────────────────────────────────────────
def scheduler_upsert_config(entry: dict):
    """Insert or update a schedule in wf_scheduler_config."""
    if not _metadata_initialized:
        return
    sid = entry.get("schedule_id", "")
    job_names_str = json.dumps(entry.get("job_names", []))
    sql = f"""
    MERGE INTO {_fqn(TBL_SCH_CONFIG)} AS t
    USING (SELECT {_esc(sid)} AS schedule_id) AS s
    ON t.schedule_id = s.schedule_id
    WHEN MATCHED THEN UPDATE SET
        type           = {_esc(entry.get('type'))},
        cron           = {_esc(entry.get('cron'))},
        interval_value = {_esc(entry.get('interval_value'))},
        interval_unit  = {_esc(entry.get('interval_unit'))},
        once_at        = {_esc(entry.get('once_at'))},
        schedule_desc  = {_esc(entry.get('schedule_desc'))},
        status         = {_esc(entry.get('status'))},
        last_run       = {_esc(entry.get('last_run'))},
        next_run       = {_esc(entry.get('next_run'))}
    WHEN NOT MATCHED THEN INSERT (
        schedule_id, table_name, table_schema, group_id, job_names,
        type, cron, interval_value, interval_unit, once_at,
        schedule_desc, status, created_at, last_run, next_run
    ) VALUES (
        {_esc(sid)}, {_esc(entry.get('table_name'))},
        {_esc(entry.get('table_schema'))}, {_esc(entry.get('group_id'))},
        {_esc(job_names_str)},
        {_esc(entry.get('type'))}, {_esc(entry.get('cron'))},
        {_esc(entry.get('interval_value'))}, {_esc(entry.get('interval_unit'))},
        {_esc(entry.get('once_at'))},
        {_esc(entry.get('schedule_desc'))}, {_esc(entry.get('status'))},
        {_esc(entry.get('created_at'))}, {_esc(entry.get('last_run'))},
        {_esc(entry.get('next_run'))}
    )
    """
    _exec_sql(sql)


def scheduler_delete_config(schedule_id: str):
    """Delete a schedule from wf_scheduler_config."""
    if not _metadata_initialized:
        return
    _exec_sql(f"DELETE FROM {_fqn(TBL_SCH_CONFIG)} WHERE schedule_id = {_esc(schedule_id)}")


def scheduler_insert_history(entry: dict):
    """Insert a scheduler execution history record."""
    if not _metadata_initialized:
        return
    import uuid as _uuid
    hid = _uuid.uuid4().hex[:12]
    sql = f"""INSERT INTO {_fqn(TBL_SCH_HISTORY)}
    (history_id, schedule_id, table_name, jobs, trigger_type, result, details, executed_at)
    VALUES (
        {_esc(hid)}, {_esc(entry.get('schedule_id'))},
        {_esc(entry.get('table_name'))}, {_esc(entry.get('jobs'))},
        {_esc(entry.get('trigger'))}, {_esc(entry.get('result'))},
        {_esc(entry.get('details'))}, {_esc(entry.get('timestamp'))}
    )"""
    _exec_sql(sql)


def scheduler_update_history_result(schedule_id: str, timestamp: str, new_result: str):
    """Update the result of an existing scheduler history entry (reconciliation)."""
    if not _metadata_initialized:
        return
    if not schedule_id or not timestamp or not new_result:
        return
    sql = f"""UPDATE {_fqn(TBL_SCH_HISTORY)}
    SET result = {_esc(new_result)}
    WHERE schedule_id = {_esc(schedule_id)}
      AND executed_at = {_esc(timestamp)}
      AND result IN ('running', 'started')"""
    try:
        _exec_sql(sql)
    except Exception:
        pass  # best-effort — don't crash the scheduler tick


def scheduler_load_all() -> dict:
    """Load all schedules and history from Databricks tables. Returns {schedules:[], history:[]}."""
    if not _metadata_initialized:
        return {"schedules": [], "history": []}

    result = {"schedules": [], "history": []}

    # Load schedules
    r = _exec_sql(f"SELECT * FROM {_fqn(TBL_SCH_CONFIG)} ORDER BY created_at DESC")
    state = r.get("status", {}).get("state", "")
    if state == "SUCCEEDED":
        columns = [c.get("name", "") for c in r.get("manifest", {}).get("schema", {}).get("columns", [])]
        for row in r.get("result", {}).get("data_array", []):
            obj = {}
            for i, col in enumerate(columns):
                obj[col] = row[i] if i < len(row) else None
            # Parse job_names back from JSON string
            try:
                obj["job_names"] = json.loads(obj.get("job_names") or "[]")
            except (json.JSONDecodeError, TypeError):
                obj["job_names"] = []
            # Convert interval_value to int if present
            if obj.get("interval_value"):
                try:
                    obj["interval_value"] = int(obj["interval_value"])
                except (ValueError, TypeError):
                    pass
            result["schedules"].append(obj)

    # Load history
    r2 = _exec_sql(f"SELECT * FROM {_fqn(TBL_SCH_HISTORY)} ORDER BY executed_at DESC LIMIT 200")
    state2 = r2.get("status", {}).get("state", "")
    if state2 == "SUCCEEDED":
        columns2 = [c.get("name", "") for c in r2.get("manifest", {}).get("schema", {}).get("columns", [])]
        for row in r2.get("result", {}).get("data_array", []):
            obj = {}
            for i, col in enumerate(columns2):
                obj[col] = row[i] if i < len(row) else None
            # Map column names to what the frontend expects
            obj["timestamp"] = obj.pop("executed_at", obj.get("timestamp"))
            obj["trigger"] = obj.pop("trigger_type", obj.get("trigger"))
            result["history"].append(obj)

    return result


# ─────────────────────────────────────────────────────────────────────────────
#  ARCHIVE EXISTING JOBS FOR A TABLE (before upsert)
# ─────────────────────────────────────────────────────────────────────────────
def _derive_mapping_tag(target_config: dict) -> str:
    """
    Derive a short mapping tag from target_config to differentiate pipelines
    for the same table but different layer mappings.
    E.g. target_config with bronze_catalog='bronze_data', target_schema='it'
         → mapping_tag = 'bronze_data_it'
    If no multi-catalog config, returns '' (legacy/default mapping).
    """
    tc = target_config or {}
    brz = tc.get("bronze_catalog", "")
    sch = tc.get("target_schema", "")
    if brz and sch:
        return f"{brz}_{sch}"
    elif brz:
        return brz
    return ""


def _derive_source_tag(source_config: dict) -> str:
    """Derive a short tag identifying the SOURCE connection.

    Same purpose as _derive_mapping_tag but for the source side: the same
    table_name can legitimately exist in two different source systems
    (e.g. SQL Server vs Snowflake, or two different servers/accounts).
    Without this, creating a pipeline for "customers" from a second source
    would archive/replace the first source's still-active pipeline for a
    table of the same name, since archiving only matched on table_name.
    """
    sc = source_config or {}
    ident = sc.get("server") or sc.get("account") or ""
    db = sc.get("database", "")
    if ident and db:
        return f"{ident}_{db}"
    return ident


def _delete_dlt_pipelines_for_groups(group_ids) -> None:
    """Delete the native Spark Declarative Pipeline (DLT) for each archived group_id.

    metadata_notebooks.py names each DLT pipeline "MetadataPipeline_<group_id>"
    (a fresh group_id per pipeline-creation, so re-creating a pipeline for the
    same table never reuses the old name) and only cleans up orphans of its
    OWN catalog.schema, best-effort, whenever some other table's job happens
    to run. If the new pipeline's target catalog/schema differs even slightly
    from the old one (e.g. a different layer mapping), that cleanup never
    finds it — the old pipeline is orphaned forever. Delete it directly here,
    at archive time, instead of waiting on that indirect cleanup.
    """
    if not group_ids:
        return
    try:
        dcfg = _load_deploy_config()
        host = _dbr_host or dcfg.get("databricks_host", "")
        # Fresh resolve first, not the frozen _dbr_token global -- otherwise
        # rotating the token via Settings (e.g. to add serving-endpoint
        # permissions) leaves this call still authenticating with whatever
        # token was cached at process start.
        token = _resolve_databricks_token(dcfg) or _dbr_token
        if not host or not token:
            return
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        for gid in group_ids:
            if not gid:
                continue
            name = f"MetadataPipeline_{gid}"
            try:
                r = requests.get(
                    f"{host}/api/2.0/pipelines",
                    params={"filter": f"name LIKE '{name}'", "max_results": 10},
                    headers=headers, timeout=15,
                )
                if not r.ok:
                    continue
                for p in r.json().get("statuses", []):
                    if p.get("name") == name:
                        requests.delete(f"{host}/api/2.0/pipelines/{p['pipeline_id']}", headers=headers, timeout=15)
                        logger.info("Deleted orphaned DLT pipeline %s (%s) for archived group %s", name, p["pipeline_id"], gid)
                        break
            except Exception as exc:
                logger.warning("Could not delete DLT pipeline for archived group %s: %s", gid, exc)
    except Exception:
        pass


def _archive_existing_jobs(table_name: str, reason: str = "load_type_change", mapping_tag: str = "", source_tag: str = "") -> list:
    """
    Archive existing jobs for a given table_name into wf_job_metadatahis.
    If mapping_tag is provided, ONLY archive jobs whose target_config has
    the SAME mapping (same bronze_catalog + target_schema). This allows
    the same table to have multiple pipelines with different layer mappings.
    If source_tag is provided, ONLY archive jobs whose source_config has
    the SAME source connection (server/account + database) -- otherwise
    creating a pipeline for "customers" from a second source (e.g.
    Snowflake) would archive/replace an unrelated, still-active pipeline
    for a same-named "customers" table from a different source.
    Also removes from in-memory JOB_REGISTRY and PIPELINE_GROUPS.
    """
    archived = []
    if not _metadata_initialized:
        return archived

    _extra_filter = ""
    if mapping_tag:
        _extra_filter += f" AND target_config LIKE '%{mapping_tag}%'"
    if source_tag:
        _extra_filter += f" AND source_config LIKE '%{source_tag}%'"

    try:
        # 1. Copy matching rows from wf_job_metadata → wf_job_metadatahis
        archive_sql = f"""
        INSERT INTO {_fqn(TBL_JOBS_HISTORY)}
        SELECT
            uuid() AS history_id,
            job_id, job_name, stage, group_id, table_schema, table_name,
            full_table, load_type, watermark_column, status, last_run_id,
            last_run_at, last_status, run_count, fail_count, enabled,
            job_order, source_config, target_config, created_at, updated_at,
            current_timestamp() AS archived_at,
            {_esc(reason)} AS archive_reason
        FROM {_fqn(TBL_JOBS)}
        WHERE table_name = {_esc(table_name)}
        {_extra_filter}
        """
        _exec_sql(archive_sql)

        # 2. Get the job_ids + group_ids being removed (for in-memory cleanup)
        fetch_sql = f"""
        SELECT job_id, group_id FROM {_fqn(TBL_JOBS)}
        WHERE table_name = {_esc(table_name)}
        {_extra_filter}
        """
        r = _exec_sql(fetch_sql)
        rows = r.get("result", {}).get("data_array", [])
        old_job_ids = set()
        old_group_ids = set()
        for row in rows:
            old_job_ids.add(row[0])
            old_group_ids.add(row[1])

        # 3. Delete from wf_job_metadata in Databricks
        # If mapping_tag/source_tag provided, only delete jobs matching them
        if _extra_filter:
            _exec_sql(f"DELETE FROM {_fqn(TBL_JOBS)} WHERE table_name = {_esc(table_name)}{_extra_filter}")
        else:
            _exec_sql(f"DELETE FROM {_fqn(TBL_JOBS)} WHERE table_name = {_esc(table_name)}")

        # 4. Clean in-memory registries
        with _lock:
            for jid in old_job_ids:
                JOB_REGISTRY.pop(jid, None)
                archived.append(jid)
            for gid in old_group_ids:
                PIPELINE_GROUPS.pop(gid, None)
            # Also clean pipeline metadata table
        for gid in old_group_ids:
            try:
                _exec_sql(f"DELETE FROM {_fqn(TBL_PIPELINES)} WHERE group_id = {_esc(gid)}")
            except Exception:
                pass

        # Delete the actual Databricks-side DLT pipeline(s) for these archived
        # groups now, rather than leaving them orphaned until some unrelated
        # job's notebook happens to clean them up (see docstring).
        _delete_dlt_pipelines_for_groups(old_group_ids)

    except Exception:
        pass
    return archived


# ─────────────────────────────────────────────────────────────────────────────
#  CREATE PIPELINE GROUP FOR A TABLE
# ─────────────────────────────────────────────────────────────────────────────
def create_pipeline_for_table(
    table_schema: str,
    table_name: str,
    load_type: str = "full",          # "full" or "incremental"
    watermark_column: str = "",       # e.g. "ModifiedDate"
    source_config: dict = None,       # {source_type, server, database, ...}
    target_config: dict = None,       # {catalog, schema, landing_path, ...}
    pipeline_mode: str = "standard",  # "standard" (PySpark) or "dlt" (Delta Live Tables)
    cdc_mode: str = "watermark",      # "watermark" or "change_tracking"
    primary_keys: list = None,        # e.g. ["CustomerID"] — required for change_tracking
    use_layer_mapping: bool = False,  # apply the saved Layer→Catalog.Schema Mapping to THIS table
) -> dict:
    """
    Create a pipeline group for one source table:
      Standard (3 jobs): extract → landing_to_bronze → bronze_to_silver
      DLT      (2 jobs): extract → dlt_bronze_silver
    CDC Mode:
      watermark        — classic incremental via watermark column
      change_tracking  — SQL Server Change Tracking via CHANGETABLE()

    If jobs already exist for this table_name, the old records are archived
    to wf_job_metadatahis before new jobs are created (one active set per table).
    """
    full_table = f"{table_schema}.{table_name}"

    # ── Validate catalogs exist on Databricks before creating pipeline ──
    tc = target_config or {}
    catalogs_to_check = set()
    for key in ("volumes_catalog", "bronze_catalog", "silver_catalog", "catalog"):
        cat = tc.get(key, "")
        if cat:
            catalogs_to_check.add(cat)
    if catalogs_to_check and _metadata_initialized:
        try:
            from unity_catalog_executor import execute_sql
            existing_cats_df = execute_sql("SHOW CATALOGS", max_wait=30)
            if existing_cats_df and existing_cats_df.get("success"):
                rows = existing_cats_df.get("data", [])
                existing_names = {r[0].lower() for r in rows if r}
                missing = [c for c in catalogs_to_check if c.lower() not in existing_names]
                if missing:
                    return {
                        "success": False,
                        "error": (
                            f"Catalog(s) {missing} not found on Databricks. "
                            "Create them first or check your target_config."
                        ),
                    }
        except Exception as e:
            # Non-blocking: log warning but continue
            print(f"⚠️ Catalog validation skipped: {e}")

    # ── Deduplicate: archive existing jobs for this table ──
    # Derive mapping tag from resolved target_config for multi-mapping support,
    # and a source tag so the same table_name from a DIFFERENT source
    # connection (e.g. Snowflake vs SQL Server) isn't treated as a duplicate.
    _mapping_tag = _derive_mapping_tag(target_config)
    _source_tag = _derive_source_tag(source_config)
    archived_ids = _archive_existing_jobs(table_name, reason="load_type_change", mapping_tag=_mapping_tag, source_tag=_source_tag)

    group_id = uuid.uuid4().hex[:12]
    ts = datetime.now().isoformat()

    source_config = source_config or {}
    target_config = target_config or {}
    primary_keys  = primary_keys or []

    # ── Auto-populate multi-catalog keys if missing ──
    # The frontend may not always send bronze_catalog / silver_catalog /
    # volumes_catalog / target_schema.  First try SHOW CATALOGS for real
    # validation, then fall back to deployconfig.json defaults.
    if not target_config.get("bronze_catalog"):
        _auto_populated = False
        if _metadata_initialized:
            try:
                from unity_catalog_executor import execute_sql
                cats_resp = execute_sql("SHOW CATALOGS", max_wait=20)
                if cats_resp and cats_resp.get("success"):
                    cat_names = {r[0].lower() for r in cats_resp.get("data", []) if r}
                    if "bronze" in cat_names:
                        target_config.setdefault("bronze_catalog", "bronze")
                    if "silver" in cat_names:
                        target_config.setdefault("silver_catalog", "silver")
                    if "dev_volumes" in cat_names:
                        target_config.setdefault("volumes_catalog", "dev_volumes")
                    _auto_populated = True
            except Exception as _ac_err:
                logger.warning("Could not auto-populate from SHOW CATALOGS: %s", _ac_err)

        # Fallback: read catalog names from deployconfig.json
        if not _auto_populated or not target_config.get("bronze_catalog"):
            _dcfg = _load_deploy_config()
            _dcfg_cats = _dcfg.get("catalogs", {})
            for _cat_name in ("bronze", "silver", "dev_volumes"):
                if _cat_name in _dcfg_cats:
                    _key = "volumes_catalog" if _cat_name == "dev_volumes" else f"{_cat_name}_catalog"
                    target_config.setdefault(_key, _cat_name)

        # Only set target_schema from table_schema if not present AND
        # no default schema was provided via the 'schema' key in target_config.
        # This prevents the source schema (e.g. 'dbo') from overriding the
        # user's intended target schema mapping from the UI.
        if not target_config.get("target_schema"):
            # Prefer the 'schema' key (set by _wfTargetConfig from Default Schema dropdown)
            ui_default_schema = target_config.get("schema", "")
            if ui_default_schema and ui_default_schema != target_config.get("metadata_schema", ""):
                target_config["target_schema"] = ui_default_schema
            # Do NOT fall back to table_schema (source schema like 'dbo')
            # — leave it empty so downstream notebooks can handle it explicitly

        logger.info("Auto-populated multi-catalog keys: bronze=%s silver=%s volumes=%s schema=%s",
                 target_config.get("bronze_catalog"), target_config.get("silver_catalog"),
                 target_config.get("volumes_catalog"), target_config.get("target_schema"))

        # ── Layer Mapping override: read from config cache (Delta table) ──
        # Only applied when THIS creation call opted in via use_layer_mapping
        # (the "Use Layer Mapping" checkbox for the currently selected/checked
        # tables in Quick Create). Previously this fired for every pipeline
        # ever created afterward as soon as any catalog was saved in the
        # mapping once — i.e. one Save Mapping click silently rewrote the
        # target catalog/schema for every future table, not just the ones
        # selected at the time. Scoping it to the request keeps it tied to
        # the actual table selection instead of being a sticky global default.
        _has_create_mapping = False
        if use_layer_mapping:
            try:
                from config_cache import get_config as _get_create_cfg
                deploy_cfg = _get_create_cfg()
            except Exception:
                deploy_cfg = _load_deploy_config()
            ex_mapping = deploy_cfg.get("existing_setting", {}).get("medallion_layer_mapping", {})
            _has_create_mapping = any(ex_mapping.get(l, {}).get("catalog") for l in ("landing", "bronze", "silver"))
        if _has_create_mapping:
            landing_cfg = ex_mapping.get("landing", {})
            bronze_cfg = ex_mapping.get("bronze", {})
            silver_cfg = ex_mapping.get("silver", {})
            recon_cfg = ex_mapping.get("reconciliation", {})
            logging_cfg = ex_mapping.get("loggingdetails", {})
            if landing_cfg.get("catalog"):
                target_config["volumes_catalog"] = landing_cfg["catalog"]
                target_config["target_schema"] = landing_cfg.get("schema", target_config.get("target_schema", ""))
            if bronze_cfg.get("catalog"):
                target_config["bronze_catalog"] = bronze_cfg["catalog"]
            if silver_cfg.get("catalog"):
                target_config["silver_catalog"] = silver_cfg["catalog"]
            if recon_cfg.get("catalog"):
                target_config["recon_catalog"] = recon_cfg["catalog"]
                target_config["recon_schema"] = recon_cfg.get("schema", "")
            if logging_cfg.get("catalog"):
                target_config["log_catalog"] = logging_cfg["catalog"]
                target_config["log_schema"] = logging_cfg.get("schema", "")
            logger.info("ExistingSetting override applied: landing=%s bronze=%s silver=%s recon=%s logging=%s",
                       landing_cfg.get("catalog"), bronze_cfg.get("catalog"), silver_cfg.get("catalog"),
                       recon_cfg.get("catalog"), logging_cfg.get("catalog"))

    # Inject CDC config into source_config for downstream notebooks
    if cdc_mode and cdc_mode != "watermark":
        source_config["cdc_mode"] = cdc_mode
    if primary_keys:
        source_config["primary_keys"] = primary_keys

    if pipeline_mode == "dlt":
        stage_list = ["extract", "dlt_bronze_silver"]
    else:
        stage_list = ["extract", "landing_to_bronze", "bronze_to_silver"]

    jobs = []
    for stage in stage_list:
        job_id = uuid.uuid4().hex[:12]
        job = {
            "job_id":           job_id,
            "job_name":         _job_name(stage, table_name, target_config),
            "stage":            stage,
            "group_id":         group_id,
            "table_schema":     table_schema,
            "table_name":       table_name,
            "full_table":       full_table,
            "load_type":        load_type,
            "watermark_column": watermark_column if load_type == "incremental" else "",
            "status":           "created",       # created | running | success | failed | disabled
            "last_run_id":      None,
            "last_run_at":      None,
            "last_status":      None,
            "run_count":        0,
            "fail_count":       0,
            "created_at":       ts,
            "updated_at":       ts,
            "source_config":    source_config,
            "target_config":    target_config,
            "order":            stage_list.index(stage) + 1,
            "enabled":          True,
        }
        with _lock:
            JOB_REGISTRY[job_id] = job
        jobs.append(job)

    # Register watermark if incremental
    if load_type == "incremental" and watermark_column:
        with _lock:
            WATERMARKS[full_table] = {
                "column":      watermark_column,
                "last_value":  None,
                "updated_at":  ts,
            }

    group = {
        "group_id":           group_id,
        "table_schema":       table_schema,
        "table_name":         table_name,
        "full_table":         full_table,
        "load_type":          load_type,
        "watermark_column":   watermark_column,
        "job_ids":            [j["job_id"] for j in jobs],
        "status":             "created",
        "source_config":      source_config,
        "target_config":      target_config,
        "pipeline_mode":      pipeline_mode,
        "cdc_mode":           cdc_mode,
        "primary_keys":       primary_keys,
        "created_at":         ts,
    }
    with _lock:
        PIPELINE_GROUPS[group_id] = group

    # ── Sync to Databricks ──
    _sync_pipeline_to_dbr(group)
    for j in jobs:
        _sync_job_to_dbr(j)
    if load_type == "incremental" and watermark_column and full_table in WATERMARKS:
        _sync_watermark_to_dbr(full_table, WATERMARKS[full_table])

    return {
        "success":       True,
        "group_id":      group_id,
        "group":         group,
        "jobs":          jobs,
        "archived_jobs": archived_ids,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  BULK CREATE PIPELINES
# ─────────────────────────────────────────────────────────────────────────────
def create_pipelines_bulk(
    tables: list,           # [{schema, table, load_type, watermark_column, target_catalog, target_schema}, ...]
    source_config: dict = None,
    target_config: dict = None,
    pipeline_mode: str = "standard",
    cdc_mode: str = "watermark",
    primary_keys: list = None,
    use_layer_mapping: bool = False,   # apply saved Layer→Catalog.Schema Mapping to just this batch
) -> dict:
    """Create pipeline groups for multiple tables at once.

    Each table entry may include ``target_catalog`` and ``target_schema`` for
    per-table Databricks schema mapping.  When present, they override the
    shared target_config values for that specific table's pipeline.
    """
    results = []
    for t in tables:
        # Build per-table target config — only override target_schema
        tc = dict(target_config or {})
        if t.get("target_schema"):
            tc["target_schema"] = t["target_schema"]

        r = create_pipeline_for_table(
            table_schema=t.get("schema", "dbo"),
            table_name=t.get("table", ""),
            load_type=t.get("load_type", "full"),
            watermark_column=t.get("watermark_column", ""),
            source_config=source_config,
            target_config=tc,
            pipeline_mode=pipeline_mode,
            cdc_mode=cdc_mode,
            primary_keys=t.get("primary_keys", primary_keys or []),
            use_layer_mapping=use_layer_mapping,
        )
        results.append(r)

    return {
        "success":    True,
        "created":    len(results),
        "groups":     [{**r["group"], "archived_jobs": r.get("archived_jobs", [])} for r in results],
        "total_jobs": sum(len(r["jobs"]) for r in results),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  LIST ALL JOBS
# ─────────────────────────────────────────────────────────────────────────────
def list_jobs(group_id: str = None, stage: str = None, status: str = None) -> dict:
    """List jobs with optional filters."""
    jobs = list(JOB_REGISTRY.values())
    if group_id:
        jobs = [j for j in jobs if j["group_id"] == group_id]
    if stage:
        jobs = [j for j in jobs if j["stage"] == stage]
    if status:
        jobs = [j for j in jobs if j["status"] == status]
    return {"success": True, "jobs": jobs, "total": len(jobs)}


# ─────────────────────────────────────────────────────────────────────────────
#  LIST ALL PIPELINE GROUPS
# ─────────────────────────────────────────────────────────────────────────────
def list_pipeline_groups() -> dict:
    """List all pipeline groups with enriched job info."""
    groups = []
    for gid, grp in PIPELINE_GROUPS.items():
        job_details = []
        for jid in grp.get("job_ids", []):
            job = JOB_REGISTRY.get(jid)
            if job:
                job_details.append(job)
        # Compute overall status
        statuses = [j["status"] for j in job_details]
        if any(s == "failed" for s in statuses):
            overall = "failed"
        elif all(s == "success" for s in statuses):
            overall = "success"
        elif any(s == "running" for s in statuses):
            overall = "running"
        else:
            overall = "created"

        groups.append({
            **grp,
            "jobs":   job_details,
            "status": overall,
        })
    return {"success": True, "groups": groups, "total": len(groups)}


def list_pipeline_groups_live() -> dict:
    """Query pipeline/job status directly from Databricks metadata tables (real-time)."""
    if not _metadata_initialized:
        # Fallback to in-memory if not connected
        return list_pipeline_groups()

    try:
        # Query pipelines with their jobs in a single JOIN
        sql = f"""
            SELECT
                p.group_id, p.table_schema, p.table_name, p.full_table,
                p.load_type, p.watermark_column, p.status AS pipeline_status,
                p.source_config, p.target_config, p.created_at AS pipeline_created_at,
                p.updated_at AS pipeline_updated_at,
                j.job_id, j.job_name, j.stage, j.status AS job_status,
                j.last_run_id, j.last_run_at, j.last_status,
                j.run_count, j.fail_count, j.enabled, j.job_order, j.updated_at AS job_updated_at
            FROM {_fqn(TBL_PIPELINES)} p
            LEFT JOIN {_fqn(TBL_JOBS)} j ON p.group_id = j.group_id
            ORDER BY p.created_at, j.job_order
        """
        r = _exec_sql(sql)
        state = r.get("status", {}).get("state", "")
        if state != "SUCCEEDED":
            logger.warning("list_pipeline_groups_live SQL failed, falling back to memory")
            return list_pipeline_groups()

        cols = [c["name"] for c in r.get("manifest", {}).get("schema", {}).get("columns", [])]
        rows = r.get("result", {}).get("data_array", [])

        # Build group dict
        groups_map = OrderedDict()
        for row in rows:
            rec = dict(zip(cols, row))
            gid = rec["group_id"]
            if gid not in groups_map:
                src_cfg = {}
                tgt_cfg = {}
                try: src_cfg = json.loads(rec.get("source_config") or "{}")
                except: pass
                try: tgt_cfg = json.loads(rec.get("target_config") or "{}")
                except: pass
                groups_map[gid] = {
                    "group_id": gid,
                    "table_schema": rec.get("table_schema", ""),
                    "table_name": rec.get("table_name", ""),
                    "full_table": rec.get("full_table", ""),
                    "load_type": rec.get("load_type", "full"),
                    "watermark_column": rec.get("watermark_column", ""),
                    "status": rec.get("pipeline_status", "created"),
                    "source_config": src_cfg,
                    "target_config": tgt_cfg,
                    "created_at": rec.get("pipeline_created_at", ""),
                    "updated_at": rec.get("pipeline_updated_at", ""),
                    "job_ids": [],
                    "jobs": [],
                }

            # Add job if present
            jid = rec.get("job_id")
            if jid:
                job = {
                    "job_id": jid,
                    "job_name": rec.get("job_name", ""),
                    "stage": rec.get("stage", ""),
                    "group_id": gid,
                    "status": rec.get("job_status", "created"),
                    "last_run_id": rec.get("last_run_id"),
                    "last_run_at": rec.get("last_run_at"),
                    "last_status": rec.get("last_status"),
                    "run_count": int(rec.get("run_count", 0) or 0),
                    "fail_count": int(rec.get("fail_count", 0) or 0),
                    "enabled": str(rec.get("enabled", "true")).lower() in ("true", "1", "yes"),
                    "order": int(rec.get("job_order", 1) or 1),
                    "updated_at": rec.get("job_updated_at", ""),
                }
                groups_map[gid]["job_ids"].append(jid)
                groups_map[gid]["jobs"].append(job)

        # Compute overall status + pipeline_mode + last_activity
        groups = []
        for gid, grp in groups_map.items():
            statuses = [j["status"] for j in grp["jobs"]]
            if any(s == "failed" for s in statuses):
                overall = "failed"
            elif all(s == "success" for s in statuses):
                overall = "success"
            elif any(s == "running" for s in statuses):
                overall = "running"
            else:
                overall = "created"
            grp["status"] = overall

            # Determine pipeline_mode from stages
            stages = {j.get("stage", "") for j in grp["jobs"]}
            grp["pipeline_mode"] = "dlt" if "dlt_bronze_silver" in stages else "standard"

            # Last activity timestamp (most recent last_run_at across jobs)
            run_times = [j["last_run_at"] for j in grp["jobs"] if j.get("last_run_at")]
            grp["last_activity"] = max(run_times) if run_times else grp.get("updated_at") or grp.get("created_at") or ""

            groups.append(grp)

        return {"success": True, "groups": groups, "total": len(groups)}

    except Exception as e:
        logger.error(f"list_pipeline_groups_live error: {e}")
        return list_pipeline_groups()


# ─────────────────────────────────────────────────────────────────────────────
#  GET SINGLE JOB
# ─────────────────────────────────────────────────────────────────────────────
def get_job(job_id: str) -> dict:
    """Get details of a single job."""
    job = JOB_REGISTRY.get(job_id)
    if not job:
        return {"success": False, "error": f"Job '{job_id}' not found"}
    # Include run history
    runs = [r for r in JOB_RUNS.values() if r["job_id"] == job_id]
    runs.sort(key=lambda r: r["started_at"], reverse=True)
    return {"success": True, "job": job, "runs": runs}


# ─────────────────────────────────────────────────────────────────────────────
#  UPDATE JOB
# ─────────────────────────────────────────────────────────────────────────────
def update_job(job_id: str, updates: dict) -> dict:
    """Update job metadata (load_type, watermark_column, enabled, etc.)."""
    job = JOB_REGISTRY.get(job_id)
    if not job:
        return {"success": False, "error": f"Job '{job_id}' not found"}

    allowed = {"load_type", "watermark_column", "enabled", "source_config", "target_config"}
    with _lock:
        for k, v in updates.items():
            if k in allowed:
                job[k] = v
        job["updated_at"] = datetime.now().isoformat()

    # If load_type changed to incremental and watermark set, update WATERMARKS
    if job["load_type"] == "incremental" and job["watermark_column"]:
        ft = job["full_table"]
        if ft not in WATERMARKS:
            WATERMARKS[ft] = {"column": job["watermark_column"], "last_value": None, "updated_at": job["updated_at"]}
        else:
            WATERMARKS[ft]["column"] = job["watermark_column"]
        _sync_watermark_to_dbr(ft, WATERMARKS[ft])

    _sync_job_to_dbr(job)
    return {"success": True, "job": job}


# ─────────────────────────────────────────────────────────────────────────────
#  DELETE JOB
# ─────────────────────────────────────────────────────────────────────────────
def delete_job(job_id: str) -> dict:
    """Delete a job from registry."""
    job = JOB_REGISTRY.get(job_id)
    if not job:
        return {"success": False, "error": f"Job '{job_id}' not found"}

    group_id = job["group_id"]
    with _lock:
        del JOB_REGISTRY[job_id]
        # Remove from group
        if group_id in PIPELINE_GROUPS:
            grp = PIPELINE_GROUPS[group_id]
            grp["job_ids"] = [jid for jid in grp["job_ids"] if jid != job_id]
            if not grp["job_ids"]:
                del PIPELINE_GROUPS[group_id]
                _delete_pipeline_from_dbr(group_id)
        # Remove related runs
        run_ids_to_remove = [rid for rid, r in JOB_RUNS.items() if r["job_id"] == job_id]
        for rid in run_ids_to_remove:
            _unindex_run_by_dbr(rid, JOB_RUNS[rid].get("dbr_run_id"))
            del JOB_RUNS[rid]

    _delete_job_from_dbr(job_id)
    return {"success": True, "deleted": job_id, "job_name": job["job_name"]}


# ─────────────────────────────────────────────────────────────────────────────
#  DELETE PIPELINE GROUP
# ─────────────────────────────────────────────────────────────────────────────
def delete_pipeline_group(group_id: str) -> dict:
    """Delete an entire pipeline group and all its jobs."""
    grp = PIPELINE_GROUPS.get(group_id)
    if not grp:
        return {"success": False, "error": f"Pipeline group '{group_id}' not found"}

    deleted_jobs = []
    with _lock:
        for jid in grp.get("job_ids", []):
            if jid in JOB_REGISTRY:
                deleted_jobs.append(JOB_REGISTRY[jid]["job_name"])
                del JOB_REGISTRY[jid]
            # Remove runs
            run_ids = [rid for rid, r in JOB_RUNS.items() if r["job_id"] == jid]
            for rid in run_ids:
                _unindex_run_by_dbr(rid, JOB_RUNS[rid].get("dbr_run_id"))
                del JOB_RUNS[rid]
        del PIPELINE_GROUPS[group_id]

    _delete_pipeline_from_dbr(group_id)
    return {"success": True, "deleted_group": group_id, "deleted_jobs": deleted_jobs}


# ─────────────────────────────────────────────────────────────────────────────
#  RUN A JOB (simulated execution with logging)
# ─────────────────────────────────────────────────────────────────────────────
def run_job(job_id: str, force_full: bool = False) -> dict:
    """Start a job run. Returns run details immediately (background execution)."""
    job = JOB_REGISTRY.get(job_id)
    if not job:
        return {"success": False, "error": f"Job '{job_id}' not found"}
    if not job.get("enabled", True):
        return {"success": False, "error": f"Job '{job['job_name']}' is disabled"}

    run_id = uuid.uuid4().hex[:12]
    ts = datetime.now().isoformat()
    load_type = "full" if force_full else job["load_type"]

    # Get watermark if incremental
    watermark_value = None
    if load_type == "incremental" and job["watermark_column"]:
        wm = WATERMARKS.get(job["full_table"])
        if wm:
            watermark_value = wm.get("last_value")

    run = {
        "run_id":           run_id,
        "job_id":           job_id,
        "job_name":         job["job_name"],
        "stage":            job["stage"],
        "full_table":       job["full_table"],
        "load_type":        load_type,
        "watermark_column": job.get("watermark_column", ""),
        "watermark_value":  watermark_value,
        "status":           "running",
        "started_at":       ts,
        "completed_at":     None,
        "duration_sec":     None,
        "rows_processed":   0,
        "error":            None,
        "logs":             [
            f"[{ts}] 🚀 Started {job['job_name']}",
            f"[{ts}] 📋 Load type: {load_type}",
            f"[{ts}] 📊 Table: {job['full_table']}",
        ],
    }

    if watermark_value:
        run["logs"].append(f"[{ts}] 🔄 Watermark: {job['watermark_column']} > '{watermark_value}'")
    elif load_type == "incremental":
        run["logs"].append(f"[{ts}] ⚠️ No watermark found — will do initial full load")

    with _lock:
        JOB_RUNS[run_id] = run
        job["last_run_id"] = run_id
        job["last_run_at"] = ts
        job["status"] = "running"
        job["run_count"] += 1
        _evict_old_runs_if_needed()

    # Sync run start to Databricks
    _sync_run_to_dbr(run)
    _sync_job_to_dbr(job)

    # Start background execution (Fix 10: bounded by _thread_semaphore)
    _spawn_worker(_execute_job_run, args=(run_id, job_id), name=f"job-run-{run_id}")

    return {"success": True, "run_id": run_id, "run": run}


def _execute_job_run(run_id: str, job_id: str):
    """Background execution of a job run via Databricks notebook.

    Submits the appropriate metadata-driven notebook on Databricks and
    polls for completion. Falls back to marking the run as failed with
    a clear error when Databricks credentials are missing.
    """
    import time

    run = JOB_RUNS.get(run_id)
    job = JOB_REGISTRY.get(job_id)
    if not run or not job:
        return

    try:
        ts = datetime.now().isoformat()
        stage = job["stage"]

        # ── Resolve Databricks connection ──
        dcfg = _load_deploy_config()
        host  = _dbr_host or dcfg.get("databricks_host", "")
        # Fresh resolve first, not the frozen _dbr_token global -- otherwise
        # rotating the token via Settings (e.g. to add serving-endpoint
        # permissions) leaves this call still authenticating with whatever
        # token was cached at process start.
        token = _resolve_databricks_token(dcfg) or _dbr_token
        cat   = _dbr_catalog or dcfg.get("metadata_catalog", "") or "main"
        sch   = _dbr_schema or dcfg.get("metadata_schema", "") or "default"
        ws    = _notebooks_workspace_path or "/Shared/MetadataPipeline"
        password = _resolve_source_password(dcfg)

        if not host or not token:
            raise RuntimeError(
                "Databricks not connected — use 'Run on Databricks' from Pipeline Studio "
                "or initialise MetadataFlow first."
            )

        from databricks_connector import DatabricksConnector
        import base64
        connector = DatabricksConnector(host, token)

        # Map stage → notebook path
        # Check current pipeline_mode from config — if "standard" and stage is
        # "dlt_bronze_silver" (legacy from DLT creation), convert to standard
        # notebooks instead of running the DLT orchestrator.
        _current_mode = (dcfg.get("cdc", {}).get("dlt_mode", "standard"))
        
        # If standard mode but job has DLT stage, run as two sequential standard notebooks
        if stage == "dlt_bronze_silver" and _current_mode == "standard":
            logger.info("_execute_job_run: Converting dlt_bronze_silver to standard mode "
                       "(bronze + silver) for job %s", job_id)
            run["logs"].append(f"[{ts}] 🔄 Mode is 'standard' — running Bronze then Silver notebooks")
            
            # Run Bronze notebook first
            _bronze_nb = f"{ws}/02_Meta_Bronze"
            _silver_nb = f"{ws}/03_Meta_Silver"
            
            run["logs"].append(f"[{ts}] ⚡ Submitting Bronze notebook: {_bronze_nb}")
            
            _bronze_params = dict(nb_params)
            _bronze_params["stage"] = "landing_to_bronze"
            
            bronze_result = connector.run_notebook(
                notebook_path=_bronze_nb,
                cluster_id=cluster_id or None,
                params=_bronze_params,
            )
            
            if not bronze_result.get("success"):
                _err = bronze_result.get("error") or bronze_result.get("message", "Bronze notebook failed")
                run["logs"].append(f"[{ts}] ❌ Bronze failed: {_err}")
                raise RuntimeError(f"Bronze stage failed: {_err}")
            
            run["logs"].append(f"[{ts}] ✅ Bronze completed")
            run["logs"].append(f"[{ts}] ⚡ Submitting Silver notebook: {_silver_nb}")
            
            _silver_params = dict(nb_params)
            _silver_params["stage"] = "bronze_to_silver"
            
            silver_result = connector.run_notebook(
                notebook_path=_silver_nb,
                cluster_id=cluster_id or None,
                params=_silver_params,
            )
            
            if not silver_result.get("success"):
                _err = silver_result.get("error") or silver_result.get("message", "Silver notebook failed")
                run["logs"].append(f"[{ts}] ❌ Silver failed: {_err}")
                raise RuntimeError(f"Silver stage failed: {_err}")
            
            run["logs"].append(f"[{ts}] ✅ Silver completed — standard mode pipeline done")
            
            # Mark as success and exit
            run["status"] = "success"
            run["completed_at"] = datetime.now().isoformat()
            job["last_run_id"] = run_id
            job["last_run_at"] = run["completed_at"]
            job["last_status"] = "success"
            job["status"] = "success"
            job["run_count"] = job.get("run_count", 0) + 1
            _sync_job_to_dbr(job)
            return  # Done — skip the normal single-notebook execution path
        
        stage_nb_map = {
            "extract":           f"{ws}/01_Meta_Extract",
            "landing_to_bronze": f"{ws}/02_Meta_Bronze",
            "bronze_to_silver":  f"{ws}/03_Meta_Silver",
            "dlt_bronze_silver": f"{ws}/00_Meta_Orchestrator",
        }
        nb_path = stage_nb_map.get(stage)
        if not nb_path:
            raise RuntimeError(f"Unknown pipeline stage '{stage}' — no notebook mapping")

        pwd_b64 = base64.b64encode((password or "").encode("utf-8")).decode("ascii")
        tc = job.get("target_config") or {}

        # ── Fallback: if target_config is empty (app restarted, JOB_REGISTRY
        #    lost), restore it from wf_job_metadata on Databricks. ──
        if not tc.get("bronze_catalog") and host and token:
            try:
                _wh_id = None
                _wh_resp = requests.get(
                    f"{host}/api/2.0/sql/warehouses",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=15,
                )
                for _w in (_wh_resp.json() or {}).get("warehouses", []):
                    if _w.get("state") in ("RUNNING", "STARTING"):
                        _wh_id = _w["id"]; break
                if not _wh_id:
                    for _w in (_wh_resp.json() or {}).get("warehouses", []):
                        _wh_id = _w["id"]; break
                if _wh_id:
                    _grp = job.get("group_id", "")
                    _sql = (
                        f"SELECT target_config FROM `{cat}`.`{sch}`.wf_job_metadata "
                        f"WHERE group_id = '{_grp}' AND target_config IS NOT NULL "
                        f"AND LENGTH(TRIM(target_config)) > 2 LIMIT 1"
                    )
                    _sr = requests.post(
                        f"{host}/api/2.0/sql/statements",
                        headers={"Authorization": f"Bearer {token}",
                                 "Content-Type": "application/json"},
                        json={"warehouse_id": _wh_id, "statement": _sql,
                              "wait_timeout": "30s"},
                        timeout=45,
                    )
                    _sj = _sr.json()
                    _rows = (_sj.get("result", {}).get("data_array") or [])
                    if _rows:
                        tc = json.loads(_rows[0][0] or "{}")
                        logger.info("_execute_job_run: restored target_config from "
                                 "wf_job_metadata for group %s", _grp)
            except Exception as _fbe:
                logger.warning("_execute_job_run: target_config fallback failed: %s", _fbe)

        # Use UC Volumes path when volumes_catalog is set (must match extract)
        _vol_cat = tc.get("volumes_catalog", "")
        _tgt_sch = tc.get("target_schema", "")
        if _vol_cat and _tgt_sch:
            landing_path = f"/Volumes/{_vol_cat}/{_tgt_sch}/landing"
        else:
            landing_path = tc.get("landing_path", "/mnt/landing")

        nb_params = {
            "job_id":       job_id,
            "run_id":       run_id,
            "load_type":    job.get("load_type", "full"),
            "password_b64": pwd_b64,
            "catalog":      cat,
            "schema":       sch,
            "landing_path": landing_path,
        }

        # DLT orchestrator needs additional params (group_id, workspace_path,
        # target catalog/schema) so it can find the correct pipeline and
        # configure the DLT output correctly.
        if stage == "dlt_bronze_silver":
            grp_id = job.get("group_id", "")
            nb_params["group_id"]       = grp_id
            nb_params["workspace_path"] = ws
            nb_params["volumes_catalog"] = tc.get("volumes_catalog", "")
            nb_params["bronze_catalog"]  = tc.get("bronze_catalog", "")
            nb_params["silver_catalog"]  = tc.get("silver_catalog", "")
            nb_params["target_schema"]   = tc.get("target_schema", "")

            # ── Hard validation: bronze_catalog MUST be set for DLT ──
            # If still empty, read Layer Mapping from config cache (Delta table)
            if not nb_params["bronze_catalog"]:
                try:
                    from config_cache import get_config as _get_dlt_cfg
                    _dlt_dcfg = _get_dlt_cfg()
                except Exception:
                    _dlt_dcfg = _load_deploy_config()
                _dlt_lm = _dlt_dcfg.get("existing_setting", {}).get("medallion_layer_mapping", {})
                if _dlt_lm.get("bronze", {}).get("catalog"):
                    nb_params["bronze_catalog"] = _dlt_lm["bronze"]["catalog"]
                if _dlt_lm.get("silver", {}).get("catalog"):
                    nb_params["silver_catalog"] = _dlt_lm["silver"]["catalog"]
                if _dlt_lm.get("landing", {}).get("catalog"):
                    nb_params["volumes_catalog"] = _dlt_lm["landing"]["catalog"]
                # Legacy fallback if layer mapping is empty
                if not nb_params["bronze_catalog"]:
                    _dcfg_cats = _dlt_dcfg.get("catalogs", {})
                    if "bronze" in _dcfg_cats:
                        nb_params["bronze_catalog"] = "bronze"
                    if "silver" in _dcfg_cats:
                        nb_params["silver_catalog"] = "silver"
                    if "dev_volumes" in _dcfg_cats:
                        nb_params["volumes_catalog"] = "dev_volumes"
                logger.warning("SDP params recovered from config: bronze=%s silver=%s",
                            nb_params["bronze_catalog"], nb_params["silver_catalog"])
            if not nb_params["target_schema"]:
                # Prefer 'schema' from target_config (UI Default Schema) over source table_schema
                _ui_schema = tc.get("schema", "")
                _meta_schema = tc.get("metadata_schema", "")
                if _ui_schema and _ui_schema != _meta_schema:
                    nb_params["target_schema"] = _ui_schema
                else:
                    # Last resort: use metadata schema, NOT source table_schema (e.g. 'dbo')
                    nb_params["target_schema"] = sch

            if not nb_params["bronze_catalog"]:
                raise RuntimeError(
                    "Cannot run SDP pipeline: bronze_catalog is empty. "
                    "Configure bronze/silver/volumes catalogs in the Pipeline Studio settings."
                )

        run["logs"].append(f"[{ts}] ⚡ Submitting {stage} notebook to Databricks…")
        run["logs"].append(f"[{ts}] 📋 Notebook: {nb_path}")

        submit_result = connector.run_notebook(
            notebook_path=nb_path,
            cluster_id=None,
            params=nb_params,
        )

        if not submit_result.get("success"):
            err_msg = submit_result.get("error") or submit_result.get("message", "Unknown submit error")
            raise RuntimeError(f"Notebook submit failed: {err_msg}")

        dbr_run_id = submit_result.get("run_id")
        run_url = submit_result.get("run_url", "")
        run["dbr_run_id"] = dbr_run_id
        with _lock:
            _index_run_by_dbr(run_id, dbr_run_id)
        run["logs"].append(f"[{ts}] ✅ Submitted (Databricks run {dbr_run_id})")
        if run_url:
            run["logs"].append(f"[{ts}] 🔗 {run_url}")
        _sync_run_to_dbr(run)

        # ── Poll for notebook completion ──
        terminal_states = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
        for _attempt in range(360):
            time.sleep(10)
            try:
                status = connector.get_run_status(int(dbr_run_id))
            except Exception as poll_exc:
                run["logs"].append(f"[{datetime.now().isoformat()}] ⚠️ Poll error: {poll_exc}")
                continue

            if not status.get("success"):
                continue

            lifecycle    = status.get("life_cycle", "UNKNOWN")
            result_state = status.get("result_state", "")
            state_msg    = status.get("state_message", "")

            if lifecycle in terminal_states:
                end_ts = datetime.now().isoformat()
                # Fetch notebook output
                output_info = connector.get_run_output(int(dbr_run_id))
                nb_result = (output_info or {}).get("notebook_result", "")
                error_trace = (output_info or {}).get("error_trace", "")
                rows = 0
                if nb_result:
                    try:
                        parsed = json.loads(nb_result)
                        rows = int(parsed.get("rows", 0))
                    except Exception:
                        pass
                    run["logs"].append(f"[{end_ts}] 📄 Result: {nb_result[:500]}")
                if error_trace:
                    run["logs"].append(f"[{end_ts}] 📋 Trace: {error_trace[:1000]}")

                if result_state == "SUCCESS":
                    # For DLT orchestrator: Databricks reports SUCCESS (notebook
                    # completed) but the internal DLT pipeline may have FAILED.
                    # Check the notebook result JSON for the real status.
                    _internal_failed = False
                    if stage == "dlt_bronze_silver" and nb_result:
                        try:
                            _p = json.loads(nb_result)
                            _ist = (_p.get("status") or "").upper()
                            if _ist == "FAILED":
                                _internal_failed = True
                                _dlt_err = _p.get("dlt_status", "") or "SDP pipeline failed"
                                _ext_fail = _p.get("extract_failed", 0)
                                if _ext_fail:
                                    _dlt_err += f" ({_ext_fail} extract(s) failed)"
                                run["logs"].append(f"[{end_ts}] ⚠️ SDP pipeline FAILED — {_dlt_err}")
                        except Exception:
                            pass

                    if _internal_failed:
                        with _lock:
                            run["status"] = "failed"
                            run["error"] = "SDP pipeline failed — redeploy notebooks and re-run"
                            run["completed_at"] = end_ts
                            run["duration_sec"] = round(
                                (datetime.fromisoformat(end_ts) - datetime.fromisoformat(run["started_at"])).total_seconds(), 1)
                            job["status"] = "failed"
                            job["last_status"] = "failed"
                            job["fail_count"] += 1
                            job["updated_at"] = end_ts
                    else:
                        with _lock:
                            run["status"] = "success"
                            run["completed_at"] = end_ts
                            run["rows_processed"] = rows
                            run["duration_sec"] = round(
                                (datetime.fromisoformat(end_ts) - datetime.fromisoformat(run["started_at"])).total_seconds(), 1)
                            run["logs"].append(f"[{end_ts}] ✅ Job completed in {run['duration_sec']}s ({rows:,} rows)")
                            job["status"] = "success"
                            job["last_status"] = "success"
                            job["updated_at"] = end_ts
                else:
                    err = (output_info or {}).get("error") or error_trace[:500] or state_msg or result_state
                    with _lock:
                        run["status"] = "failed"
                        run["error"] = err
                        run["completed_at"] = end_ts
                        run["duration_sec"] = round(
                            (datetime.fromisoformat(end_ts) - datetime.fromisoformat(run["started_at"])).total_seconds(), 1)
                        run["logs"].append(f"[{end_ts}] ❌ Job FAILED: {err[:300]}")
                        job["status"] = "failed"
                        job["last_status"] = "failed"
                        job["fail_count"] += 1
                        job["updated_at"] = end_ts

                _sync_run_to_dbr(run)
                _sync_job_to_dbr(job)
                return
            else:
                # Still running — update log periodically
                if _attempt % 3 == 0:
                    run["logs"].append(
                        f"[{datetime.now().isoformat()}] 🔄 Databricks: {lifecycle}"
                        + (f" — {state_msg}" if state_msg else "")
                    )

        # Timed out after polling
        end_ts = datetime.now().isoformat()
        with _lock:
            run["status"] = "failed"
            run["error"] = "Timed out waiting for Databricks notebook (60 min)"
            run["completed_at"] = end_ts
            run["logs"].append(f"[{end_ts}] ❌ Polling timed out — check Databricks UI")
            job["status"] = "failed"
            job["last_status"] = "failed"
            job["fail_count"] += 1
        _sync_run_to_dbr(run)
        _sync_job_to_dbr(job)

    except Exception as e:
        end_ts = datetime.now().isoformat()
        with _lock:
            run["status"] = "failed"
            run["error"] = str(e)
            run["completed_at"] = end_ts
            run["duration_sec"] = round((datetime.fromisoformat(end_ts) - datetime.fromisoformat(run["started_at"])).total_seconds(), 1)
            run["logs"].append(f"[{end_ts}] ❌ Job FAILED: {e}")
            job["status"] = "failed"
            job["last_status"] = "failed"
            job["fail_count"] += 1
            job["updated_at"] = end_ts

        # Sync failure to Databricks
        _sync_run_to_dbr(run)
        _sync_job_to_dbr(job)


# ─────────────────────────────────────────────────────────────────────────────
#  RUN ENTIRE PIPELINE GROUP
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline_group(group_id: str, force_full: bool = False) -> dict:
    """Run all jobs in a pipeline group sequentially (extract → bronze → silver).

    Each job's background thread is monitored to completion before the
    next job is started.  If a job fails the remaining jobs are skipped.
    """
    import time as _time

    grp = PIPELINE_GROUPS.get(group_id)
    if not grp:
        return {"success": False, "error": f"Pipeline group '{group_id}' not found"}

    run_results = []
    for jid in grp.get("job_ids", []):
        r = run_job(jid, force_full=force_full)
        run_results.append(r)

        if not r.get("success"):
            break  # skip remaining stages

        # Wait for the background execution thread to finish
        run_id = r.get("run_id")
        if run_id:
            for _ in range(3600):           # up to ~60 min (1 s per iteration)
                run_rec = JOB_RUNS.get(run_id)
                if not run_rec:
                    break
                if run_rec.get("status") in ("success", "failed"):
                    break
                _time.sleep(1)

            run_rec = JOB_RUNS.get(run_id)
            if run_rec and run_rec.get("status") == "failed":
                break  # stop pipeline on failure

    return {
        "success": True,
        "group_id": group_id,
        "runs": run_results,
        "total_jobs": len(run_results),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  RERUN FROM FAILURE
# ─────────────────────────────────────────────────────────────────────────────
def rerun_from_failure(group_id: str) -> dict:
    """Rerun a pipeline group starting from the first failed job."""
    grp = PIPELINE_GROUPS.get(group_id)
    if not grp:
        return {"success": False, "error": f"Pipeline group '{group_id}' not found"}

    jobs = [JOB_REGISTRY.get(jid) for jid in grp.get("job_ids", []) if JOB_REGISTRY.get(jid)]
    jobs.sort(key=lambda j: j["order"])

    # Find first failed job
    start_from = None
    for j in jobs:
        if j["status"] == "failed":
            start_from = j["order"]
            break

    if start_from is None:
        return {"success": False, "error": "No failed jobs found in this pipeline"}

    run_results = []
    for j in jobs:
        if j["order"] >= start_from:
            r = run_job(j["job_id"])
            run_results.append(r)

    return {
        "success":     True,
        "group_id":    group_id,
        "rerun_from":  start_from,
        "runs":        run_results,
        "total_reran": len(run_results),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  GET RUN STATUS
# ─────────────────────────────────────────────────────────────────────────────
def _fetch_run_logs(run_id: str) -> list:
    """Fix 6: Lazy-load logs for a single run from Delta on demand."""
    try:
        r = _exec_sql(f"SELECT logs FROM {_fqn(TBL_RUNS)} WHERE run_id = {_esc(run_id)} LIMIT 1")
        if r.get("status", {}).get("state") != "SUCCEEDED":
            return []
        rows = r.get("result", {}).get("data_array", []) or []
        if not rows:
            return []
        logs_raw = rows[0][0] if rows[0] else None
        if not logs_raw:
            return []
        if isinstance(logs_raw, list):
            return logs_raw
        return json.loads(logs_raw)
    except Exception:
        return []


def get_run_status(run_id: str) -> dict:
    """Get status and logs of a specific run."""
    run = JOB_RUNS.get(run_id)
    if not run:
        return {"success": False, "error": f"Run '{run_id}' not found"}
    # Fix 6: hydrate logs on demand if not already loaded
    if not run.get("logs") and not run.get("logs_loaded"):
        fetched = _fetch_run_logs(run_id)
        with _lock:
            run["logs"] = fetched
            run["logs_loaded"] = True
    return {"success": True, "run": run}


# ─────────────────────────────────────────────────────────────────────────────
#  GET ALL RUNS
# ─────────────────────────────────────────────────────────────────────────────
def list_runs(job_id: str = None, group_id: str = None, status: str = None, limit: int = 50) -> dict:
    """List run history with optional filters."""
    runs = list(JOB_RUNS.values())
    if job_id:
        runs = [r for r in runs if r["job_id"] == job_id]
    if group_id:
        # Get all job_ids belonging to this pipeline group
        grp = PIPELINE_GROUPS.get(group_id)
        grp_job_ids = set(grp["job_ids"]) if grp else set()
        runs = [r for r in runs if r["job_id"] in grp_job_ids]
    if status:
        runs = [r for r in runs if r["status"] == status]
    runs.sort(key=lambda r: r["started_at"], reverse=True)
    # Fix 6: strip logs from list response (callers use get_run_status for full logs)
    trimmed = [{k: v for k, v in r.items() if k not in ("logs", "logs_loaded")} for r in runs[:limit]]
    return {"success": True, "runs": trimmed, "total": len(runs)}


# ─────────────────────────────────────────────────────────────────────────────
#  WATERMARK MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────
def get_watermarks() -> dict:
    """Get all watermark entries."""
    return {"success": True, "watermarks": dict(WATERMARKS)}


def update_watermark(table_name: str, column: str, value: str) -> dict:
    """Manually update a watermark value."""
    with _lock:
        WATERMARKS[table_name] = {
            "column":     column,
            "last_value": value,
            "updated_at": datetime.now().isoformat(),
        }
    _sync_watermark_to_dbr(table_name, WATERMARKS[table_name])
    return {"success": True, "table": table_name, "watermark": WATERMARKS[table_name]}


def reset_watermark(table_name: str) -> dict:
    """Reset watermark to force full reload on next incremental run."""
    if table_name not in WATERMARKS:
        return {"success": False, "error": f"No watermark for '{table_name}'"}
    with _lock:
        WATERMARKS[table_name]["last_value"] = None
        WATERMARKS[table_name]["updated_at"] = datetime.now().isoformat()
    _sync_watermark_to_dbr(table_name, WATERMARKS[table_name])
    return {"success": True, "table": table_name, "message": "Watermark reset — next run will do full load"}


# ─────────────────────────────────────────────────────────────────────────────
#  DASHBOARD STATS
# ─────────────────────────────────────────────────────────────────────────────
def get_dashboard_stats() -> dict:
    """Aggregate statistics for the workflow dashboard."""
    jobs = list(JOB_REGISTRY.values())
    runs = list(JOB_RUNS.values())
    groups = list(PIPELINE_GROUPS.values())

    total_jobs = len(jobs)
    running_jobs = sum(1 for j in jobs if j["status"] == "running")
    success_jobs = sum(1 for j in jobs if j["status"] == "success")
    failed_jobs = sum(1 for j in jobs if j["status"] == "failed")
    disabled_jobs = sum(1 for j in jobs if not j.get("enabled", True))

    total_runs = len(runs)
    success_runs = sum(1 for r in runs if r["status"] == "success")
    failed_runs = sum(1 for r in runs if r["status"] == "failed")
    total_rows = sum(r.get("rows_processed", 0) for r in runs)

    # Per-stage job counts
    extract_jobs = sum(1 for j in jobs if j.get("stage") == "extract")
    ingest_jobs = sum(1 for j in jobs if j.get("stage") == "landing_to_bronze")
    cleanse_jobs = sum(1 for j in jobs if j.get("stage") == "bronze_to_silver")

    # Distinct tables that have fully reached Silver — this stays meaningful even when
    # JOB_RUNS (per-run row counts) is empty, e.g. right after a restart, unlike total_rows
    # above which relies on ephemeral in-memory run history.
    _final_stages = {"bronze_to_silver", "dlt_bronze_silver", "silver"}
    tables_migrated = len({
        j.get("full_table") for j in jobs
        if j.get("status") == "success" and (j.get("stage") or "") in _final_stages and j.get("full_table")
    })

    return {
        "success": True,
        "stats": {
            "total_pipelines":  len(groups),
            "total_jobs":       total_jobs,
            "running_jobs":     running_jobs,
            "success_jobs":     success_jobs,
            "failed_jobs":      failed_jobs,
            "disabled_jobs":    disabled_jobs,
            "total_runs":       total_runs,
            "success_runs":     success_runs,
            "failed_runs":      failed_runs,
            "total_rows":       total_rows,
            "total_rows_processed": total_rows,
            "tables_migrated":  tables_migrated,
            "extract_jobs":     extract_jobs,
            "ingest_jobs":      ingest_jobs,
            "cleanse_jobs":     cleanse_jobs,
            "watermarks":       len(WATERMARKS),
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
#  ADD SINGLE CUSTOM JOB
# ─────────────────────────────────────────────────────────────────────────────
def add_custom_job(
    job_name: str,
    stage: str,
    table_schema: str = "dbo",
    table_name: str = "",
    load_type: str = "full",
    watermark_column: str = "",
    group_id: str = None,
) -> dict:
    """Add a single custom job to an existing or new group."""
    job_id = uuid.uuid4().hex[:12]
    ts = datetime.now().isoformat()
    full_table = f"{table_schema}.{table_name}" if table_name else ""

    if not group_id:
        group_id = uuid.uuid4().hex[:12]
        PIPELINE_GROUPS[group_id] = {
            "group_id":         group_id,
            "table_schema":     table_schema,
            "table_name":       table_name,
            "full_table":       full_table,
            "load_type":        load_type,
            "watermark_column": watermark_column,
            "job_ids":          [],
            "status":           "created",
            "created_at":       ts,
        }

    job = {
        "job_id":           job_id,
        "job_name":         job_name,
        "stage":            stage,
        "group_id":         group_id,
        "table_schema":     table_schema,
        "table_name":       table_name,
        "full_table":       full_table,
        "load_type":        load_type,
        "watermark_column": watermark_column,
        "status":           "created",
        "last_run_id":      None,
        "last_run_at":      None,
        "last_status":      None,
        "run_count":        0,
        "fail_count":       0,
        "created_at":       ts,
        "updated_at":       ts,
        "source_config":    {},
        "target_config":    {},
        "order":            {"extract": 1, "landing_to_bronze": 2, "bronze_to_silver": 3}.get(stage, 1),
        "enabled":          True,
    }

    with _lock:
        JOB_REGISTRY[job_id] = job
        if group_id in PIPELINE_GROUPS:
            PIPELINE_GROUPS[group_id]["job_ids"].append(job_id)

    _sync_job_to_dbr(job)
    if group_id in PIPELINE_GROUPS:
        _sync_pipeline_to_dbr(PIPELINE_GROUPS[group_id])
    return {"success": True, "job": job}


# ─────────────────────────────────────────────────────────────────────────────
#  DEPLOY METADATA-DRIVEN NOTEBOOKS TO DATABRICKS
# ─────────────────────────────────────────────────────────────────────────────
_notebooks_deployed = False          # tracks if notebooks have been uploaded
_notebooks_workspace_path = ""       # e.g. "/Shared/MetadataPipeline"

def deploy_metadata_notebooks(
    host: str = "",
    token: str = "",
    catalog: str = "main",
    schema: str = "default",
    landing_path: str = "/mnt/landing",
    workspace_path: str = "/Shared/MetadataPipeline",
    pipeline_mode: str = "standard",
    recon_catalog: str = "reconciliation",
    recon_schema: str = "hr",
    recon_table: str = "ReconcilationDetails",
    log_catalog: str = "logging",
    log_schema: str = "hr",
    log_table: str = "ExecutionLog",
    recon_location: str = "",
    log_location: str = "",
    cdc_mode: str = "watermark",
    primary_keys: list = None,
) -> dict:
    """
    Generate metadata-driven notebooks and upload them to Databricks.
    pipeline_mode: "standard" (4 notebooks) or "dlt" (3 DLT notebooks).
    cdc_mode: "watermark" or "change_tracking" (SQL Server CT).
    """
    global _notebooks_deployed, _notebooks_workspace_path

    host  = host or _dbr_host
    token = token or _dbr_token
    if not host or not token:
        return {"success": False, "error": "Databricks host and token required. Initialise MetadataFlow first."}

    # 1. Generate notebooks
    from metadata_notebooks import generate_metadata_notebooks
    gen_result = generate_metadata_notebooks(
        catalog=catalog or _dbr_catalog or "main",
        schema=schema or _dbr_schema or "default",
        landing_path=landing_path,
        workspace_path=workspace_path,
        pipeline_mode=pipeline_mode,
        recon_catalog=recon_catalog,
        recon_schema=recon_schema,
        recon_table=recon_table,
        log_catalog=log_catalog,
        log_schema=log_schema,
        log_table=log_table,
        recon_location=recon_location,
        log_location=log_location,
        cdc_mode=cdc_mode,
        primary_keys=primary_keys or [],
    )
    if not gen_result.get("success"):
        return gen_result

    # 2. Delete existing notebooks first, then upload fresh copies
    from databricks_connector import DatabricksConnector
    connector = DatabricksConnector(host, token)

    # Delete each notebook that we are about to deploy
    for nb in gen_result["notebooks"]:
        nb_path = f"{workspace_path}/{nb['name']}"
        del_r = connector.delete_notebook(nb_path)
        if del_r.get("success"):
            logger.info("Deleted existing notebook: %s", nb_path)
        else:
            logger.warning("Could not delete %s (may not exist): %s", nb_path, del_r.get("error", ""))

    results = []
    for nb in gen_result["notebooks"]:
        r = connector.upload_notebook(
            notebook_name=nb["name"],
            python_code=nb["code"],
            path=workspace_path,
        )
        results.append({
            "name":    nb["name"],
            "layer":   nb["layer"],
            "lines":   nb["lines"],
            "success": r.get("success", False),
            "path":    r.get("notebook_path") or r.get("path"),
            "url":     r.get("workspace_url"),
            "error":   r.get("error") if not r.get("success") else None,
        })

    ok = sum(1 for r in results if r["success"])
    if ok > 0:
        _notebooks_deployed = True
        _notebooks_workspace_path = workspace_path

    return {
        "success":        ok > 0,
        "uploaded":       ok,
        "total":          len(results),
        "results":        results,
        "workspace_path": workspace_path,
        "message":        f"Deployed {ok}/{len(results)} metadata notebooks to {workspace_path}",
    }


def get_notebook_status() -> dict:
    """Return whether metadata notebooks have been deployed."""
    return {
        "success":    True,
        "deployed":   _notebooks_deployed,
        "workspace_path": _notebooks_workspace_path,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  POLL DATABRICKS RUN STATUS (background thread)
# ─────────────────────────────────────────────────────────────────────────────
def _poll_databricks_run(connector, dbr_run_id, group_id: str):
    """
    Background poller — checks a Databricks run every 10s and updates
    the corresponding JOB_RUNS entries so Pipeline Logs stay current.
    """
    import time
    terminal_states = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
    grp = PIPELINE_GROUPS.get(group_id)
    grp_job_ids = set(grp["job_ids"]) if grp else set()
    dbr_run_str = str(dbr_run_id)        # normalise once for comparisons
    consecutive_errors = 0

    for _attempt in range(360):          # poll up to ~1 hour
        time.sleep(10)
        try:
            status = connector.get_run_status(int(dbr_run_id))
            consecutive_errors = 0
        except Exception as exc:
            consecutive_errors += 1
            ts = datetime.now().isoformat()
            # Log polling errors so they're visible in Pipeline Logs
            if consecutive_errors <= 3:
                with _lock:
                    for rid, run in _runs_for_dbr(dbr_run_str, grp_job_ids):
                        run["logs"].append(f"[{ts}] ⚠️ Polling error #{consecutive_errors}: {exc}")
            if consecutive_errors >= 30:
                # Give up after ~5 minutes of consecutive errors
                with _lock:
                    for rid, run in _runs_for_dbr(dbr_run_str, grp_job_ids):
                        run["status"] = "failed"
                        run["completed_at"] = ts
                        run["error"] = f"Polling abandoned after {consecutive_errors} consecutive errors"
                        run["logs"].append(f"[{ts}] ❌ Polling abandoned — check Databricks UI for run status")
                        _sync_run_to_dbr(run)
                    if grp:
                        grp["status"] = "failed"
                        _sync_pipeline_to_dbr(grp)
                return
            continue

        if not status.get("success"):
            consecutive_errors += 1
            continue

        lifecycle    = status.get("life_cycle", "UNKNOWN")
        result_state = status.get("result_state", "")
        state_msg    = status.get("state_message", "")
        ts           = datetime.now().isoformat()

        # Map Databricks states → local status
        if lifecycle in terminal_states:
            if result_state == "SUCCESS":
                local_status = "success"
                emoji = "✅"
            else:
                local_status = "failed"
                emoji = "❌"

            # ── Fetch notebook output / error trace on completion ──
            output_info = connector.get_run_output(int(dbr_run_id))
            output_lines = []
            dlt_failed = False
            extract_failed = False
            if output_info.get("success"):
                nb_result = output_info.get("notebook_result", "")
                error_trace = output_info.get("error_trace", "")
                error_msg = output_info.get("error", "")
                tasks = output_info.get("tasks", [])
                if nb_result:
                    output_lines.append(f"[{ts}] 📄 Notebook result: {nb_result[:500]}")
                    # Parse per-stage status from notebook result JSON
                    try:
                        import json as _json
                        _nr = _json.loads(nb_result)
                        _actual_dlt = _nr.get("dlt_status", "")
                        _actual_extract_fail = _nr.get("extract_failed", 0)

                        if _actual_dlt == "FAILED":
                            dlt_failed = True
                            output_lines.append(f"[{ts}] ⚠️ SDP pipeline FAILED — Bronze/Silver not processed")
                        elif _actual_dlt == "COMPLETED":
                            output_lines.append(f"[{ts}] ✅ SDP pipeline COMPLETED")

                        if _actual_extract_fail > 0:
                            extract_failed = True
                            output_lines.append(f"[{ts}] ⚠️ {_actual_extract_fail} extract(s) failed (SDP may use previous landing data)")

                        if _nr.get("silver_failed", 0) > 0:
                            output_lines.append(f"[{ts}] ⚠️ silver_failed={_nr['silver_failed']}")
                    except Exception:
                        pass
                if error_msg:
                    output_lines.append(f"[{ts}] 🔴 Error: {error_msg[:500]}")
                if error_trace:
                    output_lines.append(f"[{ts}] 📋 Trace: {error_trace[:1000]}")
                for tk in tasks:
                    t_status = f"{tk['task_key']}: {tk['result_state'] or tk['life_cycle']}"
                    if tk.get("state_message"):
                        t_status += f" — {tk['state_message'][:200]}"
                    output_lines.append(f"[{ts}] 📌 Task {t_status}")

            with _lock:
                for rid, run in _runs_for_dbr(dbr_run_str, grp_job_ids):
                        # Set per-job status based on actual stage results
                        stage = run.get("stage", "")
                        if stage == "extract":
                            run["status"] = "failed" if extract_failed else "success"
                            if extract_failed:
                                run["error"] = "Extract failed — check source table"
                        elif stage in ("dlt_bronze_silver", "landing_to_bronze", "bronze_to_silver"):
                            run["status"] = "failed" if dlt_failed else "success"
                            if dlt_failed:
                                run["error"] = "SDP pipeline failed"
                        else:
                            run["status"] = local_status
                        run["completed_at"] = ts
                        run["duration_sec"] = round(
                            (datetime.fromisoformat(ts) - datetime.fromisoformat(run["started_at"])).total_seconds(), 1
                        )
                        run["logs"].append(f"[{ts}] {emoji} Databricks run {lifecycle} — {result_state or state_msg}")
                        run["logs"].extend(output_lines)
                        if local_status == "failed":
                            run["error"] = output_info.get("error") or output_info.get("error_trace", "")[:500] or state_msg
                        _sync_run_to_dbr(run)

                        # Update JOB_REGISTRY so list_jobs() reflects final status
                        job = JOB_REGISTRY.get(run["job_id"])
                        if job:
                            job["status"] = run["status"]
                            job["run_count"] = job.get("run_count", 0) + 1
                            if run["status"] == "failed":
                                job["fail_count"] = job.get("fail_count", 0) + 1
                            _sync_job_to_dbr(job)

                if grp:
                    grp["status"] = "failed" if dlt_failed else local_status
                    _sync_pipeline_to_dbr(grp)

            # ── Notify scheduler (and any other listeners) of completion ──
            final_status = "failed" if dlt_failed else local_status
            for cb in _pipeline_complete_callbacks:
                try:
                    cb(group_id, final_status)
                except Exception as _cb_exc:
                    logger.warning("Pipeline complete callback error: %s", _cb_exc)

            return  # done

        else:
            # Still running — update log with latest lifecycle state
            with _lock:
                for rid, run in _runs_for_dbr(dbr_run_str, grp_job_ids):
                        last_log = run["logs"][-1] if run["logs"] else ""
                        status_line = f"[{ts}] 🔄 Databricks: {lifecycle}"
                        if state_msg:
                            status_line += f" — {state_msg}"
                        # Avoid duplicate consecutive status lines
                        if "🔄 Databricks:" not in last_log:
                            run["logs"].append(status_line)
                        else:
                            run["logs"][-1] = status_line


# ─────────────────────────────────────────────────────────────────────────────
#  RUN PIPELINE ON DATABRICKS (real notebook execution)
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline_on_databricks(
    group_id: str,
    host: str = "",
    token: str = "",
    cluster_id: str = "",
    load_type: str = "",
    password: str = "",
    workspace_path: str = "",
    catalog: str = "",
    schema: str = "",
    landing_path: str = "/mnt/landing",
    recon_catalog: str = "reconciliation",
    recon_schema: str = "hr",
    recon_table: str = "ReconcilationDetails",
    log_catalog: str = "logging",
    log_schema: str = "hr",
    log_table: str = "ExecutionLog",
) -> dict:
    """
    Submit the 00_Meta_Orchestrator notebook on Databricks to run a pipeline
    group (or all groups if group_id is empty).
    This is the REAL execution path — it creates a Databricks job run.
    """
    # Fallback chain: explicit arg → in-memory global → deployconfig.json → hardcoded default
    dcfg = _load_deploy_config() if (not host or not token or not catalog or not schema) else {}
    host  = host or _dbr_host or dcfg.get("databricks_host", "")
    token = token or _resolve_databricks_token(dcfg) or _dbr_token
    ws    = workspace_path or _notebooks_workspace_path or "/Shared/MetadataPipeline"
    cat   = catalog or _dbr_catalog or dcfg.get("metadata_catalog", "") or "main"
    sch   = schema or _dbr_schema or dcfg.get("metadata_schema", "") or "default"
    if not password:
        password = _resolve_source_password(dcfg)

    if not host or not token:
        return {"success": False, "error": "Databricks host and token required"}

    from databricks_connector import DatabricksConnector
    import base64
    connector = DatabricksConnector(host, token)

    # Base64-encode the password to safely pass special chars (# ; { } etc.)
    # through Databricks widget parameters.  Decoded in the notebook.
    pwd_b64 = base64.b64encode((password or "").encode("utf-8")).decode("ascii")

    params = {
        "group_id":       group_id or "",
        "load_type":      load_type or "",
        "password_b64":   pwd_b64,
        "catalog":        cat,
        "schema":         sch,
        "landing_path":   landing_path,
        "workspace_path": ws,
        "recon_catalog":  recon_catalog,
        "recon_schema":   recon_schema,
        "recon_table":    recon_table,
        "log_catalog":    log_catalog,
        "log_schema":     log_schema,
        "log_table":      log_table,
    }

    # Pass explicit data catalog params from the pipeline group's target_config
    # so the DLT orchestrator doesn't rely on querying wf_job_metadata
    grp_pre = PIPELINE_GROUPS.get(group_id, {})
    tgt_cfg = grp_pre.get("target_config") or {}

    # ── Fallback: after app restart PIPELINE_GROUPS is empty.  Read
    #    target_config from wf_job_metadata on Databricks so the DLT
    #    orchestrator still receives the correct catalog parameters. ──
    if not tgt_cfg.get("bronze_catalog") and host and token and cat and sch:
        try:
            import requests as _req
            _sql = (
                f"SELECT target_config FROM `{cat}`.`{sch}`.wf_job_metadata "
                f"WHERE group_id = '{group_id}' AND target_config IS NOT NULL "
                f"AND LENGTH(TRIM(target_config)) > 2 LIMIT 1"
            )
            _whid = dcfg.get("warehouse_id", "")
            if not _whid:
                # Try to find a warehouse
                _wh_resp = _req.get(
                    f"{host}/api/2.0/sql/warehouses",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=10,
                )
                if _wh_resp.ok:
                    for _w in _wh_resp.json().get("warehouses", []):
                        if _w.get("state") in ("RUNNING", "STARTING", ""):
                            _whid = _w["id"]
                            break
            if _whid:
                _sql_resp = _req.post(
                    f"{host}/api/2.0/sql/statements",
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json={"warehouse_id": _whid, "statement": _sql, "wait_timeout": "15s"},
                    timeout=20,
                )
                if _sql_resp.ok:
                    _rows = _sql_resp.json().get("result", {}).get("data_array", [])
                    if _rows and _rows[0] and _rows[0][0]:
                        tgt_cfg = json.loads(_rows[0][0])
                        logger.info("Restored target_config from Databricks metadata for group %s", group_id)
        except Exception as _tc_err:
            logger.warning("Could not restore target_config from Databricks: %s", _tc_err)

    params["volumes_catalog"] = tgt_cfg.get("volumes_catalog", "")
    params["bronze_catalog"]  = tgt_cfg.get("bronze_catalog", "")
    params["silver_catalog"]  = tgt_cfg.get("silver_catalog", "")
    params["target_schema"]   = tgt_cfg.get("target_schema", "")

    # Override landing_path: use UC Volumes path when volumes_catalog is set
    # (must match where the extract notebook writes data)
    _vol_cat = tgt_cfg.get("volumes_catalog", "")
    _tgt_sch = tgt_cfg.get("target_schema", "")
    if _vol_cat and _tgt_sch:
        params["landing_path"] = f"/Volumes/{_vol_cat}/{_tgt_sch}/landing"
    elif tgt_cfg.get("landing_path"):
        params["landing_path"] = tgt_cfg["landing_path"]

    # ── PERMANENT FIX: Always read Layer Mapping from config cache (Delta table) ──
    # config_cache.get_config() reads from the Delta table which is the true
    # source of truth — save_config() writes there, NOT to deployconfig.json.
    # Fallback to local JSON only if Delta is unreachable.
    try:
        from config_cache import get_config as _get_run_cfg
        _run_dcfg = _get_run_cfg()
    except Exception:
        _run_dcfg = _load_deploy_config()
    _lm = _run_dcfg.get("existing_setting", {}).get("medallion_layer_mapping", {})
    # Check if ANY layer has a catalog configured (regardless of selected_setting flag)
    _has_layer_mapping = any(_lm.get(l, {}).get("catalog") for l in ("landing", "bronze", "silver"))
    if _has_layer_mapping:
        _run_landing = _lm.get("landing", {})
        _run_bronze = _lm.get("bronze", {})
        _run_silver = _lm.get("silver", {})
        _run_recon = _lm.get("reconciliation", {})
        _run_log = _lm.get("loggingdetails", {})
        # Override with Layer Mapping values (always takes priority)
        if _run_landing.get("catalog"):
            params["volumes_catalog"] = _run_landing["catalog"]
        if _run_bronze.get("catalog"):
            params["bronze_catalog"] = _run_bronze["catalog"]
        if _run_silver.get("catalog"):
            params["silver_catalog"] = _run_silver["catalog"]
        if _run_landing.get("schema") or _run_bronze.get("schema"):
            params["target_schema"] = _run_landing.get("schema") or _run_bronze.get("schema")
        if _run_recon.get("catalog"):
            params["recon_catalog"] = _run_recon["catalog"]
        if _run_recon.get("schema"):
            params["recon_schema"] = _run_recon["schema"]
        if _run_log.get("catalog"):
            params["log_catalog"] = _run_log["catalog"]
        if _run_log.get("schema"):
            params["log_schema"] = _run_log["schema"]
        # Override landing_path with UC Volumes path
        if params.get("volumes_catalog") and params.get("target_schema"):
            params["landing_path"] = f"/Volumes/{params['volumes_catalog']}/{params['target_schema']}/landing"
        logger.info("Layer Mapping applied: volumes=%s bronze=%s silver=%s schema=%s",
                   params.get("volumes_catalog"), params.get("bronze_catalog"),
                   params.get("silver_catalog"), params.get("target_schema"))
    else:
        # Legacy fallback for non-ExistingSetting mode
        if not params["bronze_catalog"]:
            _dcfg_fb = _run_dcfg or {}
            _dcfg_cats = _dcfg_fb.get("catalogs", {})
            if "bronze" in _dcfg_cats:
                params["bronze_catalog"] = "bronze"
            if "silver" in _dcfg_cats:
                params["silver_catalog"] = "silver"
            if "dev_volumes" in _dcfg_cats:
                params["volumes_catalog"] = "dev_volumes"
        if not params["target_schema"]:
            grp_data = PIPELINE_GROUPS.get(group_id, {})
            params["target_schema"] = grp_data.get("table_schema", "") or "hr"

    # Determine pipeline_mode from config
    _pipeline_mode = (dcfg or _load_deploy_config()).get("cdc", {}).get("dlt_mode", "standard")
    # Also check the pipeline group metadata
    grp_mode = grp_pre.get("pipeline_mode", "")
    if grp_mode:
        _pipeline_mode = grp_mode
    params["pipeline_mode"] = _pipeline_mode

    if not params["bronze_catalog"] and _pipeline_mode == "dlt":
        return {"success": False, "error":
                "Cannot run pipeline: bronze_catalog is empty. "
                "Configure bronze/silver/volumes catalogs in Pipeline Studio settings."}

    # Select orchestrator based on mode
    orchestrator_nb = f"{ws}/00_Meta_Orchestrator"
    logger.info("run_pipeline_on_databricks: mode=%s, orchestrator=%s", _pipeline_mode, orchestrator_nb)

    result = connector.run_notebook(
        notebook_path=orchestrator_nb,
        cluster_id=cluster_id or None,
        params=params,
    )

    # Normalise: connector uses 'message' but frontend expects 'error'
    if not result.get("success") and "message" in result and "error" not in result:
        result["error"] = result["message"]

    ts = datetime.now().isoformat()
    grp = PIPELINE_GROUPS.get(group_id)

    if result.get("success"):
        # Update group status
        if grp:
            grp["status"] = "running"
            _sync_pipeline_to_dbr(grp)

        # ── Create JOB_RUNS entries so Pipeline Logs can display them ──
        dbr_run_id = result.get("run_id", "?")
        run_url    = result.get("run_url", "")
        if grp:
            for jid in grp.get("job_ids", []):
                job = JOB_REGISTRY.get(jid)
                if not job:
                    continue
                local_run_id = uuid.uuid4().hex[:12]
                run_entry = {
                    "run_id":           local_run_id,
                    "job_id":           jid,
                    "job_name":         job["job_name"],
                    "stage":            job["stage"],
                    "full_table":       job.get("full_table", ""),
                    "load_type":        job.get("load_type", ""),
                    "watermark_column": job.get("watermark_column", ""),
                    "watermark_value":  None,
                    "status":           "running",
                    "started_at":       ts,
                    "completed_at":     None,
                    "duration_sec":     None,
                    "rows_processed":   0,
                    "error":            None,
                    "dbr_run_id":       dbr_run_id,
                    "logs": [
                        f"[{ts}] ⚡ Submitted to Databricks (run {dbr_run_id})",
                        f"[{ts}] 📋 Stage: {job['stage']}  ·  Table: {job.get('full_table', '')}",
                        f"[{ts}] 🔗 {run_url}" if run_url else f"[{ts}] 🔄 Awaiting cluster…",
                    ],
                }
                with _lock:
                    JOB_RUNS[local_run_id] = run_entry
                    _index_run_by_dbr(local_run_id, dbr_run_id)
                    job["last_run_id"] = local_run_id
                    job["last_run_at"] = ts
                    job["status"] = "running"
                    _evict_old_runs_if_needed()
                _sync_run_to_dbr(run_entry)
                _sync_job_to_dbr(job)

        # Start background status poller for this Databricks run (Fix 10: bounded)
        _spawn_worker(
            _poll_databricks_run,
            args=(connector, dbr_run_id, group_id),
            name=f"dbr-poll-{dbr_run_id}",
        )

    else:
        # Submission failed — record a failed run so user sees the error in logs
        if grp:
            for jid in grp.get("job_ids", []):
                job = JOB_REGISTRY.get(jid)
                if not job:
                    continue
                local_run_id = uuid.uuid4().hex[:12]
                err_msg = result.get("error") or result.get("message") or "Unknown error"
                run_entry = {
                    "run_id":           local_run_id,
                    "job_id":           jid,
                    "job_name":         job["job_name"],
                    "stage":            job["stage"],
                    "full_table":       job.get("full_table", ""),
                    "load_type":        job.get("load_type", ""),
                    "watermark_column": job.get("watermark_column", ""),
                    "watermark_value":  None,
                    "status":           "failed",
                    "started_at":       ts,
                    "completed_at":     ts,
                    "duration_sec":     0,
                    "rows_processed":   0,
                    "error":            err_msg,
                    "logs": [
                        f"[{ts}] ⚡ Databricks submit attempted",
                        f"[{ts}] ❌ {err_msg}",
                    ],
                }
                with _lock:
                    JOB_RUNS[local_run_id] = run_entry
                    job["status"] = "failed"
                    _evict_old_runs_if_needed()
                _sync_run_to_dbr(run_entry)
                _sync_job_to_dbr(job)

    return result
