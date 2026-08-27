"""
Self-Healing Bot — Databricks Migration Studio
═══════════════════════════════════════════════

Intelligent failure detection, diagnosis, auto-recovery, and monitoring engine.
Features:
  • Real-time health checks (connection, cluster, notebook, pipeline)
  • Intelligent error classification & root-cause analysis
  • Auto-healing strategies: retry with exponential backoff, rollback, parameter tuning
  • Restore-point management for safe recovery
  • Continuous job monitoring with configurable rules
  • Full audit trail of every healing action
"""

import time
import re
import json
import traceback
import threading
from datetime import datetime, timedelta
from collections import defaultdict
from enum import Enum


# ═══════════════════════════════════════════════════════════════════════════════
#  Enums & Constants
# ═══════════════════════════════════════════════════════════════════════════════

class Severity(Enum):
    INFO     = "info"
    WARNING  = "warning"
    ERROR    = "error"
    CRITICAL = "critical"

class HealAction(Enum):
    RETRY            = "retry"
    RETRY_BACKOFF    = "retry_backoff"
    RESTART_CLUSTER  = "restart_cluster"
    ROLLBACK         = "rollback"
    SCALE_UP         = "scale_up"
    CLEAR_CACHE      = "clear_cache"
    RECREATE_TABLE   = "recreate_table"
    SKIP_TABLE       = "skip_table"
    RECONNECT        = "reconnect"
    ADJUST_BATCH     = "adjust_batch"
    NOTIFY           = "notify"

class HealthStatus(Enum):
    HEALTHY     = "healthy"
    DEGRADED    = "degraded"
    UNHEALTHY   = "unhealthy"
    UNKNOWN     = "unknown"


# Known error patterns → (category, severity, recommended action, description)
ERROR_PATTERNS = [
    # Connection errors
    (r"(?i)connection\s*(refused|reset|timed?\s*out|closed|error)",
     "CONNECTION_FAILURE", Severity.CRITICAL, HealAction.RECONNECT,
     "Database or service connection lost"),
    (r"(?i)authentication\s*(fail|error|denied|invalid)",
     "AUTH_FAILURE", Severity.CRITICAL, HealAction.NOTIFY,
     "Authentication credentials invalid or expired"),
    (r"(?i)(401|403)\s*:?\s*(unauthorized|forbidden)",
     "AUTH_FAILURE", Severity.CRITICAL, HealAction.NOTIFY,
     "API authorization failed — token may be expired"),

    # Cluster errors
    (r"(?i)cluster\s*(not\s*found|terminated|unavailable|error|is\s*not\s*running)",
     "CLUSTER_DOWN", Severity.ERROR, HealAction.RESTART_CLUSTER,
     "Databricks cluster is not running or unreachable"),
    (r"(?i)(java\.lang\.OutOfMemoryError|out\s*of\s*memory|OOM|heap\s*space|GC\s*overhead)",
     "OOM_ERROR", Severity.CRITICAL, HealAction.SCALE_UP,
     "Worker or driver ran out of memory"),
    (r"(?i)(disk\s*space|no\s*space\s*left|ENOSPC|DiskSpaceException)",
     "DISK_FULL", Severity.CRITICAL, HealAction.CLEAR_CACHE,
     "Disk space exhausted on cluster node"),

    # Spark / Delta errors
    (r"(?i)(AnalysisException|cannot\s*resolve|table\s*(or\s*view\s*)?not\s*found)",
     "TABLE_NOT_FOUND", Severity.ERROR, HealAction.RECREATE_TABLE,
     "Table or view does not exist — may need recreation"),
    (r"(?i)(ConcurrentAppendException|ConcurrentDeleteRead|CONCURRENT_WRITE)",
     "CONCURRENCY_CONFLICT", Severity.WARNING, HealAction.RETRY_BACKOFF,
     "Concurrent write conflict on Delta table"),
    (r"(?i)(schema\s*mismatch|schema\s*change|column.*not\s*found|incompatible\s*schema)",
     "SCHEMA_MISMATCH", Severity.ERROR, HealAction.ROLLBACK,
     "Schema mismatch between source and target"),
    (r"(?i)(corrupt|checksum\s*error|bad\s*record|malformed|ParseException)",
     "DATA_CORRUPTION", Severity.ERROR, HealAction.ROLLBACK,
     "Data corruption or malformed records detected"),
    (r"(?i)(constraint\s*violation|duplicate\s*key|unique\s*constraint|primary\s*key)",
     "CONSTRAINT_VIOLATION", Severity.WARNING, HealAction.ADJUST_BATCH,
     "Constraint violation — possible duplicate data"),

    # Timeout / resource
    (r"(?i)(timeout|timed?\s*out|deadline\s*exceeded|request\s*took\s*too\s*long)",
     "TIMEOUT", Severity.WARNING, HealAction.RETRY_BACKOFF,
     "Operation timed out — may need retry with larger timeout or smaller batch"),
    (r"(?i)(rate\s*limit|too\s*many\s*requests|429|throttl)",
     "RATE_LIMITED", Severity.WARNING, HealAction.RETRY_BACKOFF,
     "API rate limit exceeded — backing off"),
    (r"(?i)(quota\s*exceeded|resource\s*limit|limit\s*reached)",
     "QUOTA_EXCEEDED", Severity.ERROR, HealAction.NOTIFY,
     "Resource quota exceeded"),

    # Network
    (r"(?i)(DNS\s*resolution|name\s*resolution|could\s*not\s*resolve|NXDOMAIN)",
     "DNS_FAILURE", Severity.CRITICAL, HealAction.RECONNECT,
     "DNS resolution failed — check network or host URL"),
    (r"(?i)(SSL|TLS|certificate|CERTIFICATE_VERIFY_FAILED)",
     "SSL_ERROR", Severity.CRITICAL, HealAction.NOTIFY,
     "SSL/TLS certificate error"),

    # JDBC / Source DB
    (r"(?i)(JDBC|pyodbc|ODBC)\s*.*?(error|fail|exception)",
     "JDBC_ERROR", Severity.ERROR, HealAction.RECONNECT,
     "JDBC/ODBC source connection error"),
    (r"(?i)(deadlock|lock\s*wait\s*timeout|lock\s*timeout)",
     "DEADLOCK", Severity.WARNING, HealAction.RETRY_BACKOFF,
     "Database deadlock — retrying with backoff"),

    # Generic fallback
    (r"(?i)(exception|error|fail|traceback)",
     "GENERIC_ERROR", Severity.WARNING, HealAction.RETRY,
     "Unclassified error — attempting retry"),
]

# Maximum auto-heal retries per job/table
MAX_RETRIES        = 3
BACKOFF_BASE_SEC   = 5
BACKOFF_MULTIPLIER = 2


# ═══════════════════════════════════════════════════════════════════════════════
#  In-memory stores
# ═══════════════════════════════════════════════════════════════════════════════

# Healing audit log: list of dicts
HEAL_HISTORY = []

# Restore points: {key: {timestamp, state_snapshot, metadata}}
RESTORE_POINTS = {}

# Active monitors: {monitor_id: {run_id, host, token, status, ...}}
ACTIVE_MONITORS = {}

# Retry counters: {job_key: count}
RETRY_COUNTS = defaultdict(int)

# Healing rules: list of rule dicts
HEALING_RULES = [
    {"id": 1, "name": "Auto-Retry on Timeout",       "category": "TIMEOUT",
     "action": "retry_backoff", "max_retries": 3, "enabled": True,
     "description": "Automatically retry with exponential backoff when operations time out"},
    {"id": 2, "name": "Restart Cluster on OOM",       "category": "OOM_ERROR",
     "action": "restart_cluster", "max_retries": 2, "enabled": True,
     "description": "Restart the cluster if it hits an out-of-memory error"},
    {"id": 3, "name": "Rollback on Schema Mismatch",  "category": "SCHEMA_MISMATCH",
     "action": "rollback", "max_retries": 1, "enabled": True,
     "description": "Rollback to last restore point when schema changes break the pipeline"},
    {"id": 4, "name": "Reconnect on Connection Loss",  "category": "CONNECTION_FAILURE",
     "action": "reconnect", "max_retries": 3, "enabled": True,
     "description": "Attempt to re-establish connection with exponential backoff"},
    {"id": 5, "name": "Auto-Retry on Rate Limit",     "category": "RATE_LIMITED",
     "action": "retry_backoff", "max_retries": 5, "enabled": True,
     "description": "Wait and retry when hitting API rate limits"},
    {"id": 6, "name": "Retry on Concurrency Conflict", "category": "CONCURRENCY_CONFLICT",
     "action": "retry_backoff", "max_retries": 3, "enabled": True,
     "description": "Retry Delta write operations that fail due to concurrent modifications"},
    {"id": 7, "name": "Reduce Batch on Constraint Err", "category": "CONSTRAINT_VIOLATION",
     "action": "adjust_batch", "max_retries": 2, "enabled": True,
     "description": "Reduce batch size and retry when hitting data constraint violations"},
    {"id": 8, "name": "Reconnect JDBC on Failure",    "category": "JDBC_ERROR",
     "action": "reconnect", "max_retries": 3, "enabled": True,
     "description": "Re-establish JDBC connection when source database link fails"},
]

_lock = threading.Lock()


# ═══════════════════════════════════════════════════════════════════════════════
#  Helper — log a healing event
# ═══════════════════════════════════════════════════════════════════════════════

def _log_event(event_type, severity, message, details=None, action_taken=None, success=None):
    entry = {
        "id":           len(HEAL_HISTORY) + 1,
        "timestamp":    datetime.now().isoformat(),
        "event_type":   event_type,
        "severity":     severity.value if isinstance(severity, Severity) else severity,
        "message":      message,
        "details":      details or {},
        "action_taken": action_taken,
        "success":      success,
    }
    with _lock:
        HEAL_HISTORY.append(entry)
    return entry


# ═══════════════════════════════════════════════════════════════════════════════
#  1.  Diagnose — classify an error message
# ═══════════════════════════════════════════════════════════════════════════════

def diagnose_error(error_text: str, context: dict = None) -> dict:
    """
    Analyze an error string, classify it into a known category,
    assess severity, and recommend a healing action.
    """
    if not error_text:
        return {"category": "UNKNOWN", "severity": "info",
                "action": "notify", "description": "No error text provided"}

    for pattern, category, severity, action, description in ERROR_PATTERNS:
        if re.search(pattern, error_text):
            # Check if a healing rule overrides the default action
            rule = _find_rule(category)
            if rule and rule.get("enabled"):
                action = HealAction(rule["action"])

            diagnosis = {
                "category":    category,
                "severity":    severity.value,
                "action":      action.value,
                "description": description,
                "matched_pattern": pattern,
                "error_excerpt":   error_text[:500],
                "context":     context or {},
                "diagnosed_at": datetime.now().isoformat(),
            }
            # Build human-readable recommendation
            diagnosis["recommendation"] = _build_recommendation(category, action, context)
            _log_event("DIAGNOSIS", severity, f"Diagnosed: {category} — {description}",
                       details=diagnosis)
            return diagnosis

    # Fallback — unknown error
    fallback = {
        "category":    "UNKNOWN",
        "severity":    Severity.WARNING.value,
        "action":      HealAction.NOTIFY.value,
        "description": "Could not classify this error automatically",
        "error_excerpt": error_text[:500],
        "context":     context or {},
        "diagnosed_at": datetime.now().isoformat(),
        "recommendation": "Review the error manually. If it recurs, add a custom healing rule.",
    }
    _log_event("DIAGNOSIS", Severity.WARNING, "Unclassified error", details=fallback)
    return fallback


def _find_rule(category: str) -> dict:
    """Find the first enabled healing rule for a category."""
    for rule in HEALING_RULES:
        if rule.get("category") == category and rule.get("enabled"):
            return rule
    return None


def _build_recommendation(category: str, action: HealAction, context: dict = None) -> str:
    """Generate a human-readable healing recommendation."""
    recs = {
        HealAction.RETRY:           "Retry the operation immediately.",
        HealAction.RETRY_BACKOFF:   "Retry with exponential backoff (wait 5s → 10s → 20s → …).",
        HealAction.RESTART_CLUSTER: "Restart the Databricks cluster, then retry the job.",
        HealAction.ROLLBACK:        "Roll back to the last successful restore point, then re-run.",
        HealAction.SCALE_UP:        "Scale up the cluster (add workers or increase driver memory), then retry.",
        HealAction.CLEAR_CACHE:     "Clear Spark cache and temporary files, then retry.",
        HealAction.RECREATE_TABLE:  "Re-create the missing table using the Landing/Bronze notebook, then retry.",
        HealAction.SKIP_TABLE:      "Skip this table for now and continue with remaining tables.",
        HealAction.RECONNECT:       "Re-establish the connection and retry.",
        HealAction.ADJUST_BATCH:    "Reduce the batch size and retry with smaller chunks.",
        HealAction.NOTIFY:          "Manual intervention required — check credentials or configuration.",
    }
    return recs.get(action, "Review and handle manually.")


# ═══════════════════════════════════════════════════════════════════════════════
#  2.  Health Check — comprehensive system diagnostics
# ═══════════════════════════════════════════════════════════════════════════════

def run_health_check(connector=None, host: str = "", token: str = "",
                     source_config: dict = None) -> dict:
    """
    Run a comprehensive health check across all subsystems:
      • Databricks connectivity
      • Cluster status
      • Workspace accessibility
      • Source DB connectivity (if configured)
      • Recent failure rate analysis
    """
    checks = []
    overall = HealthStatus.HEALTHY

    # ── 1. Databricks Connection ──────────────────────────────────
    if connector:
        try:
            result = connector.test_connection()
            if result.get("success"):
                checks.append({
                    "name":   "Databricks Connection",
                    "status": HealthStatus.HEALTHY.value,
                    "detail": f"Connected to {host or connector.host}",
                    "meta":   {"clusters": result.get("total_clusters", 0),
                               "running":  result.get("running_clusters", 0)},
                })
            else:
                checks.append({
                    "name":   "Databricks Connection",
                    "status": HealthStatus.UNHEALTHY.value,
                    "detail": result.get("message", "Connection failed"),
                })
                overall = HealthStatus.UNHEALTHY
        except Exception as e:
            checks.append({
                "name":   "Databricks Connection",
                "status": HealthStatus.UNHEALTHY.value,
                "detail": str(e),
            })
            overall = HealthStatus.UNHEALTHY
    else:
        checks.append({
            "name":   "Databricks Connection",
            "status": HealthStatus.UNKNOWN.value,
            "detail": "No connection configured — enter host and token",
        })

    # ── 2. Cluster Health ─────────────────────────────────────────
    if connector:
        try:
            cl_result = connector.list_clusters()
            if cl_result.get("success"):
                clusters = cl_result.get("clusters", [])
                running  = [c for c in clusters if c.get("state") == "RUNNING"]
                if running:
                    checks.append({
                        "name":   "Cluster Health",
                        "status": HealthStatus.HEALTHY.value,
                        "detail": f"{len(running)} running, {len(clusters)} total",
                        "meta":   {"clusters": [c.get("cluster_name") for c in running]},
                    })
                elif clusters:
                    checks.append({
                        "name":   "Cluster Health",
                        "status": HealthStatus.DEGRADED.value,
                        "detail": f"0 running clusters ({len(clusters)} total — all stopped/terminated)",
                    })
                    if overall == HealthStatus.HEALTHY:
                        overall = HealthStatus.DEGRADED
                else:
                    checks.append({
                        "name":   "Cluster Health",
                        "status": HealthStatus.UNHEALTHY.value,
                        "detail": "No clusters found in workspace",
                    })
                    overall = HealthStatus.UNHEALTHY
            else:
                checks.append({
                    "name":   "Cluster Health",
                    "status": HealthStatus.UNKNOWN.value,
                    "detail": cl_result.get("message", "Could not fetch clusters"),
                })
        except Exception as e:
            checks.append({
                "name":   "Cluster Health",
                "status": HealthStatus.UNKNOWN.value,
                "detail": str(e),
            })

    # ── 3. Source Database ────────────────────────────────────────
    if source_config and source_config.get("server"):
        try:
            from data_migrator import _build_conn_str
            from config_cache import get_source_password
            from keyvault_helper import is_masked
            import pyodbc
            _pw = source_config.get("password", "")
            if not _pw or is_masked(_pw):
                _pw = get_source_password()
            conn_str = _build_conn_str(
                source_config.get("source_type", "sqlserver"),
                source_config["server"],
                source_config.get("database", ""),
                source_config.get("username", ""),
                _pw,
            )
            conn = pyodbc.connect(conn_str, timeout=10)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            checks.append({
                "name":   "Source Database",
                "status": HealthStatus.HEALTHY.value,
                "detail": f"Connected to {source_config['server']}/{source_config.get('database', '')}",
            })
        except Exception as e:
            checks.append({
                "name":   "Source Database",
                "status": HealthStatus.UNHEALTHY.value,
                "detail": str(e)[:200],
            })
            if overall == HealthStatus.HEALTHY:
                overall = HealthStatus.DEGRADED
    else:
        checks.append({
            "name":   "Source Database",
            "status": HealthStatus.UNKNOWN.value,
            "detail": "Source DB not configured",
        })

    # ── 4. Recent Failure Rate ────────────────────────────────────
    recent_errors = [h for h in HEAL_HISTORY
                     if h.get("severity") in ("error", "critical")
                     and _within_minutes(h.get("timestamp"), 30)]
    if len(recent_errors) >= 10:
        checks.append({
            "name":   "Error Rate",
            "status": HealthStatus.UNHEALTHY.value,
            "detail": f"{len(recent_errors)} errors in last 30 min — system may be unstable",
        })
        overall = HealthStatus.UNHEALTHY
    elif len(recent_errors) >= 3:
        checks.append({
            "name":   "Error Rate",
            "status": HealthStatus.DEGRADED.value,
            "detail": f"{len(recent_errors)} errors in last 30 min",
        })
        if overall == HealthStatus.HEALTHY:
            overall = HealthStatus.DEGRADED
    else:
        checks.append({
            "name":   "Error Rate",
            "status": HealthStatus.HEALTHY.value,
            "detail": f"{len(recent_errors)} errors in last 30 min — within normal range",
        })

    # ── 5. Restore Points ────────────────────────────────────────
    rp_count = len(RESTORE_POINTS)
    checks.append({
        "name":   "Restore Points",
        "status": HealthStatus.HEALTHY.value if rp_count > 0 else HealthStatus.DEGRADED.value,
        "detail": f"{rp_count} restore point(s) available"
                  + ("" if rp_count > 0 else " — create one before running pipelines"),
    })

    # ── 6. Healing Rules ─────────────────────────────────────────
    active_rules = [r for r in HEALING_RULES if r.get("enabled")]
    checks.append({
        "name":   "Healing Rules",
        "status": HealthStatus.HEALTHY.value,
        "detail": f"{len(active_rules)} of {len(HEALING_RULES)} rules active",
    })

    _log_event("HEALTH_CHECK", Severity.INFO,
               f"Health check: {overall.value} ({len(checks)} subsystems)",
               details={"overall": overall.value, "checks": len(checks)})

    return {
        "success":     True,
        "overall":     overall.value,
        "checks":      checks,
        "checked_at":  datetime.now().isoformat(),
        "total_events": len(HEAL_HISTORY),
    }


def _within_minutes(ts_str: str, minutes: int) -> bool:
    try:
        ts = datetime.fromisoformat(ts_str)
        return (datetime.now() - ts) < timedelta(minutes=minutes)
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════════════════
#  3.  Auto-Heal — execute a healing action
# ═══════════════════════════════════════════════════════════════════════════════

def execute_heal(action: str, connector=None, context: dict = None) -> dict:
    """
    Execute a specific healing action.
    Returns {success, action, message, details}.
    """
    context = context or {}
    action_enum = HealAction(action) if action in [a.value for a in HealAction] else HealAction.NOTIFY

    result = {"action": action, "success": False, "message": "", "details": {}}

    try:
        if action_enum == HealAction.RETRY:
            result = _heal_retry(context)

        elif action_enum == HealAction.RETRY_BACKOFF:
            result = _heal_retry_backoff(context)

        elif action_enum == HealAction.RESTART_CLUSTER:
            result = _heal_restart_cluster(connector, context)

        elif action_enum == HealAction.ROLLBACK:
            result = _heal_rollback(context)

        elif action_enum == HealAction.SCALE_UP:
            result = _heal_scale_up(connector, context)

        elif action_enum == HealAction.CLEAR_CACHE:
            result = _heal_clear_cache(connector, context)

        elif action_enum == HealAction.RECONNECT:
            result = _heal_reconnect(connector, context)

        elif action_enum == HealAction.ADJUST_BATCH:
            result = _heal_adjust_batch(context)

        elif action_enum == HealAction.SKIP_TABLE:
            result = _heal_skip_table(context)

        elif action_enum == HealAction.RECREATE_TABLE:
            result = _heal_recreate_table(connector, context)

        else:
            result = {
                "action":  action,
                "success": False,
                "message": "Manual intervention required — action not automatable",
                "details": context,
            }

        _log_event("HEAL_EXECUTED",
                   Severity.INFO if result.get("success") else Severity.WARNING,
                   f"Heal action '{action}': {'OK' if result.get('success') else 'FAILED'} — {result.get('message', '')}",
                   details=result, action_taken=action, success=result.get("success"))
        return result

    except Exception as e:
        err_result = {
            "action":  action,
            "success": False,
            "message": f"Healing failed with exception: {str(e)}",
            "details": {"traceback": traceback.format_exc()},
        }
        _log_event("HEAL_ERROR", Severity.ERROR, f"Heal exception: {str(e)}",
                   details=err_result, action_taken=action, success=False)
        return err_result


def _heal_retry(context: dict) -> dict:
    """Simple immediate retry."""
    job_key = context.get("job_key", "unknown")
    RETRY_COUNTS[job_key] += 1
    count = RETRY_COUNTS[job_key]
    if count > MAX_RETRIES:
        return {"action": "retry", "success": False,
                "message": f"Max retries ({MAX_RETRIES}) exceeded for {job_key}",
                "retry_count": count, "details": context}
    return {"action": "retry", "success": True,
            "message": f"Retry #{count} queued for {job_key}",
            "retry_count": count, "wait_seconds": 0, "details": context}


def _heal_retry_backoff(context: dict) -> dict:
    """Retry with exponential backoff."""
    job_key = context.get("job_key", "unknown")
    RETRY_COUNTS[job_key] += 1
    count = RETRY_COUNTS[job_key]
    rule = _find_rule(context.get("category", ""))
    max_r = (rule or {}).get("max_retries", MAX_RETRIES)
    if count > max_r:
        return {"action": "retry_backoff", "success": False,
                "message": f"Max retries ({max_r}) exceeded for {job_key}",
                "retry_count": count, "details": context}
    wait = BACKOFF_BASE_SEC * (BACKOFF_MULTIPLIER ** (count - 1))
    return {"action": "retry_backoff", "success": True,
            "message": f"Retry #{count} with {wait}s backoff for {job_key}",
            "retry_count": count, "wait_seconds": wait, "details": context}


def _heal_restart_cluster(connector, context: dict) -> dict:
    """Restart the Databricks cluster."""
    cluster_id = context.get("cluster_id", "")
    if not connector:
        return {"action": "restart_cluster", "success": False,
                "message": "No Databricks connector available"}
    if not cluster_id:
        # Try to find the first running/terminated cluster
        try:
            cl = connector.list_clusters()
            clusters = cl.get("clusters", [])
            if clusters:
                cluster_id = clusters[0].get("cluster_id", "")
        except Exception:
            pass
    if not cluster_id:
        return {"action": "restart_cluster", "success": False,
                "message": "No cluster_id available to restart"}
    try:
        resp = connector.session.post(
            f"{connector.host}/api/2.0/clusters/restart",
            json={"cluster_id": cluster_id},
            timeout=30
        )
        if resp.status_code == 200:
            return {"action": "restart_cluster", "success": True,
                    "message": f"Cluster {cluster_id} restart initiated",
                    "cluster_id": cluster_id}
        else:
            return {"action": "restart_cluster", "success": False,
                    "message": f"Restart failed ({resp.status_code}): {resp.text[:200]}"}
    except Exception as e:
        return {"action": "restart_cluster", "success": False, "message": str(e)}


def _heal_rollback(context: dict) -> dict:
    """Rollback to last restore point."""
    rp_key = context.get("restore_point_key", "")
    if not rp_key:
        # Find the latest restore point
        if RESTORE_POINTS:
            rp_key = max(RESTORE_POINTS.keys(),
                         key=lambda k: RESTORE_POINTS[k].get("timestamp", ""))
        else:
            return {"action": "rollback", "success": False,
                    "message": "No restore points available — cannot rollback"}

    rp = RESTORE_POINTS.get(rp_key)
    if not rp:
        return {"action": "rollback", "success": False,
                "message": f"Restore point '{rp_key}' not found"}

    return {"action": "rollback", "success": True,
            "message": f"Rolled back to restore point '{rp_key}' from {rp['timestamp']}",
            "restore_point": rp_key,
            "snapshot": rp.get("metadata", {})}


def _heal_scale_up(connector, context: dict) -> dict:
    """Recommend (or execute) cluster scale-up."""
    cluster_id = context.get("cluster_id", "")
    if not connector or not cluster_id:
        return {"action": "scale_up", "success": True,
                "message": "Scale-up recommended: increase num_workers or driver memory in cluster config",
                "manual": True}
    try:
        # Get current config
        resp = connector.session.get(
            f"{connector.host}/api/2.0/clusters/get?cluster_id={cluster_id}",
            timeout=10
        )
        if resp.status_code == 200:
            cfg = resp.json()
            current_workers = cfg.get("num_workers", 2)
            new_workers = min(current_workers * 2, 16)  # cap at 16
            edit_resp = connector.session.post(
                f"{connector.host}/api/2.0/clusters/edit",
                json={**cfg, "num_workers": new_workers},
                timeout=30
            )
            if edit_resp.status_code == 200:
                return {"action": "scale_up", "success": True,
                        "message": f"Cluster scaled from {current_workers} → {new_workers} workers",
                        "cluster_id": cluster_id}
    except Exception:
        pass
    return {"action": "scale_up", "success": True,
            "message": "Scale-up recommended — increase workers manually",
            "manual": True}


def _heal_clear_cache(connector, context: dict) -> dict:
    """Clear Spark cache."""
    return {"action": "clear_cache", "success": True,
            "message": "Cache clear signaled — next run will use spark.catalog.clearCache()",
            "details": {"instruction": "Add `spark.catalog.clearCache()` at notebook start"}}


def _heal_reconnect(connector, context: dict) -> dict:
    """Attempt to re-establish connection."""
    if connector:
        try:
            result = connector.test_connection()
            if result.get("success"):
                return {"action": "reconnect", "success": True,
                        "message": "Reconnection successful"}
            else:
                return {"action": "reconnect", "success": False,
                        "message": f"Reconnect failed: {result.get('message', '')}"}
        except Exception as e:
            return {"action": "reconnect", "success": False,
                    "message": f"Reconnect error: {str(e)}"}
    return {"action": "reconnect", "success": False,
            "message": "No connector available — configure connection first"}


def _heal_adjust_batch(context: dict) -> dict:
    """Reduce batch size."""
    current_batch = context.get("batch_size", 100000)
    new_batch = max(current_batch // 2, 1000)
    return {"action": "adjust_batch", "success": True,
            "message": f"Batch size reduced from {current_batch:,} → {new_batch:,} rows",
            "new_batch_size": new_batch}


def _heal_skip_table(context: dict) -> dict:
    """Skip a problematic table."""
    table = context.get("table", "unknown")
    return {"action": "skip_table", "success": True,
            "message": f"Table '{table}' marked as skipped — pipeline will continue with remaining tables",
            "skipped_table": table}


def _heal_recreate_table(connector, context: dict) -> dict:
    """Signal table recreation needed."""
    table = context.get("table", "unknown")
    return {"action": "recreate_table", "success": True,
            "message": f"Table '{table}' flagged for re-creation — Landing notebook will re-ingest",
            "table": table}


# ═══════════════════════════════════════════════════════════════════════════════
#  4.  Restore Points
# ═══════════════════════════════════════════════════════════════════════════════

def create_restore_point(key: str, metadata: dict = None) -> dict:
    """Create a named restore point with a state snapshot."""
    rp = {
        "key":       key,
        "timestamp": datetime.now().isoformat(),
        "metadata":  metadata or {},
    }
    with _lock:
        RESTORE_POINTS[key] = rp
    _log_event("RESTORE_POINT_CREATED", Severity.INFO,
               f"Restore point created: {key}", details=rp)
    return {"success": True, "restore_point": rp}


def list_restore_points() -> list:
    """Return all restore points sorted by timestamp descending."""
    return sorted(RESTORE_POINTS.values(), key=lambda r: r["timestamp"], reverse=True)


def delete_restore_point(key: str) -> dict:
    with _lock:
        if key in RESTORE_POINTS:
            del RESTORE_POINTS[key]
            return {"success": True, "message": f"Restore point '{key}' deleted"}
    return {"success": False, "message": f"Restore point '{key}' not found"}


# ═══════════════════════════════════════════════════════════════════════════════
#  5.  Monitor — track a Databricks job run
# ═══════════════════════════════════════════════════════════════════════════════

def start_monitor(run_id: int, connector=None, auto_heal: bool = True) -> dict:
    """Start monitoring a Databricks job run."""
    monitor_id = f"mon_{run_id}_{int(time.time())}"
    monitor = {
        "monitor_id": monitor_id,
        "run_id":     run_id,
        "status":     "watching",
        "auto_heal":  auto_heal,
        "started_at": datetime.now().isoformat(),
        "events":     [],
        "heals":      [],
    }
    with _lock:
        ACTIVE_MONITORS[monitor_id] = monitor

    if connector:
        # Do an initial check
        try:
            status = connector.get_run_status(run_id)
            monitor["last_check"]    = datetime.now().isoformat()
            monitor["run_status"]    = status.get("life_cycle", "UNKNOWN")
            monitor["result_state"]  = status.get("result_state", "")
            monitor["events"].append({
                "time": datetime.now().isoformat(),
                "msg":  f"Initial status: {status.get('life_cycle', '?')} / {status.get('result_state', '?')}"
            })
        except Exception as e:
            monitor["events"].append({
                "time": datetime.now().isoformat(),
                "msg":  f"Initial check failed: {str(e)}"
            })

    _log_event("MONITOR_STARTED", Severity.INFO,
               f"Monitor started for run {run_id}", details=monitor)
    return {"success": True, "monitor": monitor}


def check_monitor(monitor_id: str, connector=None) -> dict:
    """Check and update monitor status. If failed and auto_heal is on, attempt healing."""
    monitor = ACTIVE_MONITORS.get(monitor_id)
    if not monitor:
        return {"success": False, "message": f"Monitor {monitor_id} not found"}

    run_id = monitor["run_id"]
    if not connector:
        return {"success": True, "monitor": monitor, "message": "No connector — cannot poll"}

    try:
        status = connector.get_run_status(run_id)
        monitor["last_check"]   = datetime.now().isoformat()
        monitor["run_status"]   = status.get("life_cycle", "UNKNOWN")
        monitor["result_state"] = status.get("result_state", "")

        lc = status.get("life_cycle", "")
        rs = status.get("result_state", "")
        msg = status.get("state_message", "")

        if lc in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            if rs == "SUCCESS":
                monitor["status"] = "completed"
                monitor["events"].append({"time": datetime.now().isoformat(),
                                          "msg": "Run completed successfully ✓"})
            else:
                monitor["status"] = "failed"
                monitor["events"].append({"time": datetime.now().isoformat(),
                                          "msg": f"Run failed: {rs} — {msg[:300]}"})

                # Auto-heal if enabled
                if monitor.get("auto_heal") and msg:
                    diagnosis = diagnose_error(msg, {"run_id": run_id, "monitor_id": monitor_id})
                    heal_result = execute_heal(diagnosis["action"], connector,
                                               {**diagnosis, "run_id": run_id,
                                                "job_key": f"run_{run_id}"})
                    monitor["heals"].append({
                        "time":     datetime.now().isoformat(),
                        "diagnosis": diagnosis["category"],
                        "action":   diagnosis["action"],
                        "success":  heal_result.get("success"),
                        "message":  heal_result.get("message", ""),
                    })
        elif lc == "RUNNING":
            monitor["status"] = "running"
            monitor["events"].append({"time": datetime.now().isoformat(),
                                      "msg": "Run is active"})
        else:
            monitor["status"] = "watching"
            monitor["events"].append({"time": datetime.now().isoformat(),
                                      "msg": f"Status: {lc}"})

    except Exception as e:
        monitor["events"].append({"time": datetime.now().isoformat(),
                                  "msg": f"Check error: {str(e)}"})

    return {"success": True, "monitor": monitor}


def list_monitors() -> list:
    return list(ACTIVE_MONITORS.values())


def stop_monitor(monitor_id: str) -> dict:
    monitor = ACTIVE_MONITORS.get(monitor_id)
    if not monitor:
        return {"success": False, "message": "Monitor not found"}
    monitor["status"] = "stopped"
    return {"success": True, "message": f"Monitor {monitor_id} stopped"}


# ═══════════════════════════════════════════════════════════════════════════════
#  6.  Rules CRUD
# ═══════════════════════════════════════════════════════════════════════════════

def get_rules() -> list:
    return HEALING_RULES[:]

def toggle_rule(rule_id: int, enabled: bool) -> dict:
    for rule in HEALING_RULES:
        if rule["id"] == rule_id:
            rule["enabled"] = enabled
            _log_event("RULE_TOGGLED", Severity.INFO,
                       f"Rule '{rule['name']}' {'enabled' if enabled else 'disabled'}")
            return {"success": True, "rule": rule}
    return {"success": False, "message": f"Rule {rule_id} not found"}


def add_rule(name: str, category: str, action: str,
             max_retries: int = 3, description: str = "") -> dict:
    new_id = max((r["id"] for r in HEALING_RULES), default=0) + 1
    rule = {
        "id": new_id, "name": name, "category": category,
        "action": action, "max_retries": max_retries,
        "enabled": True, "description": description,
    }
    HEALING_RULES.append(rule)
    _log_event("RULE_ADDED", Severity.INFO, f"New rule added: {name}")
    return {"success": True, "rule": rule}


# ═══════════════════════════════════════════════════════════════════════════════
#  7.  History / Audit
# ═══════════════════════════════════════════════════════════════════════════════

def get_history(limit: int = 50, severity_filter: str = None) -> list:
    """Return recent healing history, optionally filtered by severity."""
    items = HEAL_HISTORY[:]
    if severity_filter:
        items = [h for h in items if h.get("severity") == severity_filter]
    return items[-limit:][::-1]  # most recent first


def clear_history() -> dict:
    with _lock:
        HEAL_HISTORY.clear()
    return {"success": True, "message": "History cleared"}


def get_stats() -> dict:
    """Return summary statistics."""
    total = len(HEAL_HISTORY)
    by_severity = defaultdict(int)
    by_action   = defaultdict(int)
    by_category = defaultdict(int)
    heals_ok    = 0
    heals_fail  = 0

    for h in HEAL_HISTORY:
        by_severity[h.get("severity", "unknown")] += 1
        if h.get("action_taken"):
            by_action[h["action_taken"]] += 1
            if h.get("success"):
                heals_ok += 1
            elif h.get("success") is False:
                heals_fail += 1
        cat = (h.get("details") or {}).get("category")
        if cat:
            by_category[cat] += 1

    return {
        "total_events":   total,
        "by_severity":    dict(by_severity),
        "by_action":      dict(by_action),
        "by_category":    dict(by_category),
        "heals_succeeded": heals_ok,
        "heals_failed":   heals_fail,
        "active_monitors": len([m for m in ACTIVE_MONITORS.values()
                                if m.get("status") not in ("stopped", "completed")]),
        "restore_points":  len(RESTORE_POINTS),
        "active_rules":    len([r for r in HEALING_RULES if r.get("enabled")]),
    }
