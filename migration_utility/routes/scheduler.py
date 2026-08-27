"""Scheduler blueprint — job scheduling endpoints."""
from flask import Blueprint, request, jsonify
import os, json, uuid as _uuid, threading
from datetime import datetime, timedelta, timezone
from collections import OrderedDict

from .auth import login_required
from log_config import get_logger
from config_cache import get_config, get_databricks_token, get_source_password
from audit import log_action
import workflow_manager as wfm
from unity_catalog_executor import UnityCatalogExecutor

logger = get_logger(__name__)
scheduler_bp = Blueprint("scheduler", __name__, url_prefix="/api/v1")
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PERSISTENT_DIR = "/home/migration_data" if os.path.isdir("/home") and os.access("/home", os.W_OK) else _BASE_DIR
os.makedirs(_PERSISTENT_DIR, exist_ok=True)
SCHEDULE_PATH = os.path.join(_PERSISTENT_DIR, "job_schedules.json")
_sch_file_lock = threading.Lock()          # guards concurrent JSON file writes


def _load_schedules_local():
    if os.path.isfile(SCHEDULE_PATH):
        try:
            with open(SCHEDULE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            logger.warning("Could not load schedules from %s", SCHEDULE_PATH)
    return {"schedules": [], "history": []}


def _save_schedules_local(data):
    with open(SCHEDULE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)


def _load_schedules():
    try:
        dbx_data = wfm.scheduler_load_all()
        if dbx_data.get("schedules") or dbx_data.get("history"):
            local = _load_schedules_local()
            # Merge schedules: add local-only schedules to Databricks data
            dbx_sids = {s["schedule_id"] for s in dbx_data.get("schedules", [])}
            for ls in local.get("schedules", []):
                if ls.get("schedule_id") not in dbx_sids:
                    dbx_data["schedules"].append(ls)
            # Merge history: prefer local status when local has a resolved value
            # (local file gets updated by callback/tick, Databricks table may lag)
            local_hist_map = {}
            for lh in local.get("history", []):
                key = (lh.get("timestamp", ""), lh.get("schedule_id", ""))
                local_hist_map[key] = lh
            for dh in dbx_data.get("history", []):
                key = (dh.get("timestamp", ""), dh.get("schedule_id", ""))
                lh = local_hist_map.get(key)
                if lh and lh.get("result") in ("success", "failed") and dh.get("result") in ("running", "started"):
                    dh["result"] = lh["result"]
            _save_schedules_local(dbx_data)
            return dbx_data
    except Exception:
        logger.warning("Could not load schedules from Databricks, falling back to local")
    return _load_schedules_local()


def _save_schedules(data):
    _save_schedules_local(data)


def _compute_next_run(sch_type, cron_expr, interval_value, interval_unit, once_at, last_run=None):
    now = datetime.now(timezone.utc)
    if sch_type == "once" and once_at:
        return once_at
    if sch_type == "interval" and interval_value:
        val = int(interval_value)
        base = now
        if last_run:
            try:
                base = datetime.fromisoformat(str(last_run).replace("Z", "+00:00"))
            except (ValueError, TypeError):
                base = now
            if interval_unit == "minutes":
                nxt = base + timedelta(minutes=val)
            elif interval_unit == "days":
                nxt = base + timedelta(days=val)
            else:
                nxt = base + timedelta(hours=val)
            if nxt <= now:
                if interval_unit == "minutes":
                    nxt = now + timedelta(minutes=val)
                elif interval_unit == "days":
                    nxt = now + timedelta(days=val)
                else:
                    nxt = now + timedelta(hours=val)
        else:
            if interval_unit == "minutes":
                nxt = now + timedelta(minutes=val)
            elif interval_unit == "days":
                nxt = now + timedelta(days=val)
            else:
                nxt = now + timedelta(hours=val)
        return nxt.isoformat()
    if sch_type == "cron" and cron_expr:
        parts = cron_expr.split()
        if len(parts) >= 2:
            try:
                minute = int(parts[0]) if parts[0] != "*" else now.minute
                hour = int(parts[1]) if parts[1] != "*" else now.hour
                nxt = now.replace(minute=minute, second=0, microsecond=0)
                if parts[1] != "*":
                    nxt = nxt.replace(hour=hour)
                if nxt <= now:
                    nxt += timedelta(days=1)
                return nxt.isoformat()
            except (ValueError, IndexError):
                pass
    return now.isoformat()


@scheduler_bp.route("/scheduler/schedules", methods=["GET"])
@login_required
def sch_list_schedules():
    data = _load_schedules()
    # ── Real-time reconciliation: resolve any "running" history entries ──
    history = data.get("history", [])
    changed = False
    for h in history:
        if h.get("result") not in ("running", "started"):
            continue
        # Try Databricks API to get actual run status
        dbr_run_id = _extract_run_id(h.get("details", ""))
        if not dbr_run_id:
            # No run_id — if older than 30 min, mark as unknown/failed
            try:
                entry_time = datetime.fromisoformat(str(h.get("timestamp", "")).replace("Z", "+00:00"))
                if entry_time.tzinfo is None:
                    entry_time = entry_time.replace(tzinfo=timezone.utc)
                if (datetime.now(timezone.utc) - entry_time).total_seconds() > 1800:
                    h["result"] = "failed"
                    changed = True
            except (ValueError, TypeError):
                pass
            continue
        try:
            cfg = get_config()
            _h = cfg.get("databricks_host", "")
            _t = get_databricks_token()
            if _h and _t:
                from databricks_connector import DatabricksConnector
                dc = DatabricksConnector(_h, _t)
                st = dc.get_run_status(int(dbr_run_id))
                if st.get("success"):
                    lc = st.get("life_cycle", "")
                    rs = st.get("result_state", "")
                    if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                        h["result"] = "success" if rs == "SUCCESS" else "failed"
                        changed = True
                elif "does not exist" in str(st.get("error", "")).lower():
                    # Run expired from Databricks — mark as completed (unknown)
                    h["result"] = "failed"
                    changed = True
        except Exception:
            pass
    if changed:
        _save_schedules_local(data)
        # Persist resolved statuses to DB
        for h in history:
            if h.get("result") not in ("running", "started"):
                try:
                    wfm.scheduler_update_history_result(
                        h.get("schedule_id", ""), h.get("timestamp", ""), h["result"])
                except Exception:
                    pass
    return jsonify({"success": True, "schedules": data.get("schedules", []),
                    "history": history})


@scheduler_bp.route("/scheduler/tables", methods=["GET"])
@login_required
def sch_list_tables():
    cfg = get_config()
    dbx_host = cfg.get("databricks_host", "").rstrip("/")
    dbx_token = get_databricks_token()
    if not dbx_host or not dbx_token:
        return jsonify({"success": False, "tables": [], "error": "Databricks not configured."})
    # Use metadata_catalog/metadata_schema directly from config
    meta_cat = cfg.get("metadata_catalog", "admin_source")
    meta_sch = cfg.get("metadata_schema", "configtables")
    uc = UnityCatalogExecutor(dbx_host, dbx_token, meta_cat, meta_sch)
    wh_resp = uc.list_warehouses()
    warehouses = wh_resp.get("warehouses", [])
    wh_id = next((w["id"] for w in warehouses if w.get("state") == "RUNNING"), None)
    if not wh_id and warehouses:
        wh_id = warehouses[0].get("id")
    if not wh_id:
        return jsonify({"success": False, "tables": [], "error": "No SQL Warehouse available."})
    fqn = f"`{meta_cat}`.`{meta_sch}`.`wf_job_metadata`"
    sql = f"SELECT job_id, job_name, stage, group_id, table_schema, table_name, full_table, load_type, job_order FROM {fqn} ORDER BY table_name, job_order"
    result = uc._execute_statement(sql, wh_id, wait_timeout="30s")
    if result.get("error"):
        return jsonify({"success": False, "tables": [], "error": result["error"]})
    status = result.get("status", {}).get("state", "")
    if status in ("PENDING", "RUNNING"):
        stmt_id = result.get("statement_id", "")
        if stmt_id:
            result = uc._poll_statement(stmt_id)
            status = result.get("status", {}).get("state", "")
    # Handle FAILED/CLOSED/CANCELED states
    if status in ("FAILED", "CLOSED", "CANCELED"):
        err_msg = result.get("status", {}).get("error", {}).get("message", "Query execution failed")
        logger.error("Scheduler tables query failed: %s", err_msg)
        return jsonify({"success": False, "tables": [], "error": f"Query failed: {err_msg}"})
    columns = [c.get("name", "") for c in result.get("manifest", {}).get("schema", {}).get("columns", [])]
    data_array = result.get("result", {}).get("data_array", [])
    table_map = OrderedDict()
    for row in data_array:
        obj = {}
        for i, col in enumerate(columns):
            obj[col] = row[i] if i < len(row) else None
        tname = obj.get("table_name", "")
        if not tname:
            continue
        if tname not in table_map:
            table_map[tname] = {
                "table_name": tname, "table_schema": obj.get("table_schema", "dbo"),
                "full_table": obj.get("full_table", ""), "group_id": obj.get("group_id", ""),
                "load_type": obj.get("load_type", "full"), "jobs": [],
            }
        table_map[tname]["jobs"].append({
            "job_id": obj.get("job_id", ""), "job_name": obj.get("job_name", ""),
            "stage": obj.get("stage", ""), "load_type": obj.get("load_type", "full"),
        })
    tables = []
    for tname, tinfo in table_map.items():
        tinfo["job_count"] = len(tinfo["jobs"])
        tables.append(tinfo)
    return jsonify({"success": True, "tables": tables})


@scheduler_bp.route("/scheduler/schedules", methods=["POST"])
@login_required
def sch_create_schedule():
    d = request.get_json(force=True)
    table_name = (d.get("table_name") or "").strip()
    table_schema = (d.get("table_schema") or "dbo").strip()
    group_id = (d.get("group_id") or "").strip()
    sch_type = d.get("type", "cron")
    cron_expr = (d.get("cron") or "").strip()
    interval_value = d.get("interval_value")
    interval_unit = d.get("interval_unit", "hours")
    once_at = (d.get("once_at") or "").strip()
    if not table_name:
        return jsonify({"success": False, "error": "Table selection is required"}), 400
    if sch_type == "cron" and not cron_expr:
        return jsonify({"success": False, "error": "Cron expression is required"}), 400
    if sch_type == "interval" and not interval_value:
        return jsonify({"success": False, "error": "Interval value is required"}), 400
    if sch_type == "once" and not once_at:
        return jsonify({"success": False, "error": "Date/time for one-time run is required"}), 400
    if not group_id:
        for gid, grp in wfm.PIPELINE_GROUPS.items():
            if grp.get("table_name") == table_name:
                group_id = gid
                break
    job_names = d.get("job_names", [])
    if not job_names and group_id:
        grp = wfm.PIPELINE_GROUPS.get(group_id, {})
        for jid in grp.get("job_ids", []):
            job = wfm.JOB_REGISTRY.get(jid, {})
            if job.get("job_name"):
                job_names.append(job["job_name"])
    schedule_id = _uuid.uuid4().hex[:10]
    now = datetime.now(timezone.utc).isoformat()
    if sch_type == "cron":
        schedule_desc = f"Cron: {cron_expr}"
    elif sch_type == "interval":
        schedule_desc = f"Every {interval_value} {interval_unit}"
    else:
        schedule_desc = f"Once at {once_at}"
    entry = {
        "schedule_id": schedule_id, "table_name": table_name,
        "table_schema": table_schema, "group_id": group_id,
        "job_names": job_names, "type": sch_type,
        "cron": cron_expr if sch_type == "cron" else "",
        "interval_value": int(interval_value) if interval_value else None,
        "interval_unit": interval_unit if sch_type == "interval" else "",
        "once_at": once_at if sch_type == "once" else "",
        "schedule_desc": schedule_desc, "status": "active",
        "created_at": now, "last_run": None,
        "next_run": _compute_next_run(sch_type, cron_expr, interval_value, interval_unit, once_at),
    }
    data = _load_schedules()
    for existing in data["schedules"]:
        if existing.get("table_name") == table_name and existing.get("status") == "active":
            return jsonify({"success": False, "error": f"An active schedule already exists for table '{table_name}'"}), 400
    data["schedules"].append(entry)
    _save_schedules(data)
    wfm.scheduler_upsert_config(entry)
    log_action("schedule_created", "schedule", schedule_id,
               {"table": table_name, "type": sch_type, "desc": schedule_desc})
    return jsonify({"success": True, "schedule": entry})


@scheduler_bp.route("/scheduler/schedules/<schedule_id>", methods=["PUT"])
@login_required
def sch_update_schedule(schedule_id):
    d = request.get_json(force=True)
    data = _load_schedules()
    for s in data["schedules"]:
        if s["schedule_id"] == schedule_id:
            if "status" in d and len(d) == 1:
                s["status"] = d["status"]
                # When resuming (paused → active), ensure next_run is in the future
                if d["status"] == "active":
                    nr = s.get("next_run")
                    needs_recalc = True
                    if nr:
                        try:
                            nrt = datetime.fromisoformat(str(nr).replace("Z", "+00:00"))
                            if nrt.tzinfo is None:
                                nrt = nrt.replace(tzinfo=timezone.utc)
                            if nrt > datetime.now(timezone.utc):
                                needs_recalc = False
                        except (ValueError, TypeError):
                            pass
                    if needs_recalc:
                        s["next_run"] = _compute_next_run(
                            s.get("type", "cron"), s.get("cron", ""),
                            s.get("interval_value"), s.get("interval_unit", "hours"),
                            s.get("once_at", ""),
                        )
            else:
                if "status" in d:
                    s["status"] = d["status"]
                if "type" in d:
                    s["type"] = d["type"]
                if "cron" in d:
                    s["cron"] = d["cron"]
                if "interval_value" in d:
                    s["interval_value"] = int(d["interval_value"]) if d["interval_value"] else None
                if "interval_unit" in d:
                    s["interval_unit"] = d["interval_unit"]
                if "once_at" in d:
                    s["once_at"] = d["once_at"]
                sch_type = s.get("type", "cron")
                if sch_type == "cron":
                    s["schedule_desc"] = f"Cron: {s.get('cron', '')}"
                elif sch_type == "interval":
                    s["schedule_desc"] = f"Every {s.get('interval_value', '')} {s.get('interval_unit', 'hours')}"
                else:
                    s["schedule_desc"] = f"Once at {s.get('once_at', '')}"
                s["last_run"] = None
                s["next_run"] = _compute_next_run(
                    s.get("type", "cron"), s.get("cron", ""),
                    s.get("interval_value"), s.get("interval_unit", "hours"),
                    s.get("once_at", ""),
                )
            _save_schedules(data)
            wfm.scheduler_upsert_config(s)
            return jsonify({"success": True, "schedule": s})
    return jsonify({"success": False, "error": "Schedule not found"}), 404


@scheduler_bp.route("/scheduler/schedules/<schedule_id>", methods=["DELETE"])
@login_required
def sch_delete_schedule(schedule_id):
    data = _load_schedules()
    original_len = len(data["schedules"])
    data["schedules"] = [s for s in data["schedules"] if s["schedule_id"] != schedule_id]
    if len(data["schedules"]) == original_len:
        return jsonify({"success": False, "error": "Schedule not found"}), 404
    _save_schedules(data)
    wfm.scheduler_delete_config(schedule_id)
    log_action("schedule_deleted", "schedule", schedule_id)
    return jsonify({"success": True})


@scheduler_bp.route("/scheduler/run-now/<schedule_id>", methods=["POST"])
@login_required
def sch_run_now(schedule_id):
    data = _load_schedules()
    schedule = None
    for s in data["schedules"]:
        if s["schedule_id"] == schedule_id:
            schedule = s
            break
    if not schedule:
        return jsonify({"success": False, "error": "Schedule not found"}), 404
    table_name = schedule.get("table_name", "")
    group_id = schedule.get("group_id", "")
    if not group_id:
        for gid, grp in wfm.PIPELINE_GROUPS.items():
            if grp.get("table_name") == table_name:
                group_id = gid
                schedule["group_id"] = gid
                break
    if not group_id:
        now = datetime.now(timezone.utc).isoformat()
        fail_entry = {
            "timestamp": now, "schedule_id": schedule_id, "table_name": table_name,
            "trigger": "manual", "result": "failed",
            "details": f"No pipeline group found for table '{table_name}'. Create a pipeline first.",
        }
        data.setdefault("history", []).insert(0, fail_entry)
        data["history"] = data["history"][:200]
        _save_schedules(data)
        wfm.scheduler_insert_history(fail_entry)
        return jsonify({"success": False, "error": f"No pipeline group found for table '{table_name}'"})
    cfg = get_config()
    job_names = schedule.get("job_names", [])
    if not job_names:
        grp = wfm.PIPELINE_GROUPS.get(group_id, {})
        for jid in grp.get("job_ids", []):
            job = wfm.JOB_REGISTRY.get(jid, {})
            if job.get("job_name"):
                job_names.append(job["job_name"])
    result = wfm.run_pipeline_on_databricks(
        group_id=group_id, host=cfg.get("databricks_host", ""),
        token=get_databricks_token(),
        password=get_source_password(),
        catalog=cfg.get("metadata_catalog", ""),
        schema=cfg.get("metadata_schema", ""),
        recon_catalog=cfg.get("reconciliation", {}).get("catalog", "reconciliation"),
        recon_schema=cfg.get("reconciliation", {}).get("schema", "hr"),
        recon_table=cfg.get("reconciliation", {}).get("table", "ReconcilationDetails"),
        log_catalog=cfg.get("logging", {}).get("catalog", "loggingdetails"),
        log_schema=cfg.get("logging", {}).get("schema", "hr"),
        log_table=cfg.get("logging", {}).get("table", "ExecutionLog"),
    )
    now = datetime.now(timezone.utc).isoformat()
    schedule["last_run"] = now
    if schedule.get("type") in ("cron", "interval"):
        schedule["next_run"] = _compute_next_run(
            schedule["type"], schedule.get("cron", ""),
            schedule.get("interval_value"), schedule.get("interval_unit", "hours"),
            schedule.get("once_at", ""), last_run=now,
        )
    jobs_str = " → ".join(job_names) if job_names else table_name
    detail_msg = ""
    if result.get("success"):
        detail_msg = str(result.get("run_id", result.get("message", "")))
        if result.get("run_url"):
            detail_msg += f" | {result['run_url']}"
    else:
        detail_msg = str(result.get("error", "Unknown error"))
    history_entry = {
        "timestamp": now, "schedule_id": schedule_id, "table_name": table_name,
        "jobs": jobs_str, "group_id": group_id, "trigger": "manual",
        "result": "running" if result.get("success") else "failed",
        "details": detail_msg,
    }
    data.setdefault("history", []).insert(0, history_entry)
    data["history"] = data["history"][:200]
    _save_schedules(data)
    wfm.scheduler_upsert_config(schedule)
    wfm.scheduler_insert_history(history_entry)
    return jsonify({"success": True, "run_result": result})


# ─────────────────────────────────────────────────────────────────────────────
#  BACKGROUND SCHEDULER — checks every 30s for due schedules
# ─────────────────────────────────────────────────────────────────────────────
_scheduler_running = False
_scheduler_lock = threading.Lock()
_tick_active = threading.Lock()          # ensures only one tick runs at a time


def _scheduler_tick():
    """Check all active schedules and run any that are past their next_run.
    Also update 'running' history entries with actual completion status."""
    if not _tick_active.acquire(blocking=False):
        return                             # another tick is already running
    try:
        _scheduler_tick_inner()
    finally:
        _tick_active.release()


def _scheduler_tick_inner():
    import time
    now = datetime.now(timezone.utc)
    try:
        data = _load_schedules_local()     # fast local read — no DB call
    except Exception:
        return

    # If local file has no schedules (fresh deploy), load from DB
    if not data.get("schedules"):
        try:
            data = _load_schedules()       # full DB read + merge
            if data.get("schedules"):
                _save_schedules_local(data)
                logger.info("📂 Scheduler tick: loaded %d schedules from DB (local was empty)", len(data["schedules"]))
        except Exception:
            return

    changed = False

    # ── Phase 1: Update running history entries with actual results ──
    _dbr_connector = None          # lazy — only create if we have stale entries
    for h in data.get("history", []):
        if h.get("result") not in ("running", "started"):
            continue

        gid = h.get("group_id", "")

        # (a) Fast path — check in-memory PIPELINE_GROUPS
        if gid:
            grp = wfm.PIPELINE_GROUPS.get(gid)
            if grp:
                grp_status = grp.get("status", "")
                if grp_status in ("success", "failed"):
                    h["result"] = grp_status
                    changed = True
                    logger.info("📋 History updated (in-memory): %s → %s", h.get("table_name"), grp_status)
                    # Persist resolved status to DB so it survives restarts
                    try:
                        wfm.scheduler_update_history_result(
                            h.get("schedule_id", ""), h.get("timestamp", ""), grp_status)
                    except Exception:
                        pass
                    continue           # resolved — move to next entry
                # If group is in-memory but still "running", fall through to API check

        # (b) Slow path — query Databricks for run status (handles server restarts)
        dbr_run_id = _extract_run_id(h.get("details", ""))
        if dbr_run_id:
            try:
                if _dbr_connector is None:
                    cfg = get_config()
                    _h = cfg.get("databricks_host", "")
                    _t = get_databricks_token()
                    if _h and _t:
                        from databricks_connector import DatabricksConnector
                        _dbr_connector = DatabricksConnector(_h, _t)
                if _dbr_connector:
                    st = _dbr_connector.get_run_status(int(dbr_run_id))
                    if st.get("success"):
                        lc = st.get("life_cycle", "")
                        rs = st.get("result_state", "")
                        if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                            h["result"] = "success" if rs == "SUCCESS" else "failed"
                            changed = True
                            logger.info("📋 History updated (Databricks API): %s → %s", h.get("table_name"), h["result"])
                            # Persist to DB and clear stale in-memory status
                            try:
                                wfm.scheduler_update_history_result(
                                    h.get("schedule_id", ""), h.get("timestamp", ""), h["result"])
                            except Exception:
                                pass
                            if gid and wfm.PIPELINE_GROUPS.get(gid):
                                wfm.PIPELINE_GROUPS[gid]["status"] = h["result"]
            except Exception as exc:
                logger.debug("Databricks status query failed for run %s: %s", dbr_run_id, exc)
            continue

        # (c) No group_id and no run_id — if older than 2h, mark failed
        try:
            entry_time = datetime.fromisoformat(str(h.get("timestamp", "")).replace("Z", "+00:00"))
            if entry_time.tzinfo is None:
                entry_time = entry_time.replace(tzinfo=timezone.utc)
            if (now - entry_time).total_seconds() > 7200:
                h["result"] = "failed"
                h["details"] = (h.get("details", "") + " | Timed out — status unknown").strip(" | ")
                changed = True
        except Exception:
            pass

    # ── Phase 2: Check due schedules and trigger runs ──
    for sch in data.get("schedules", []):
        if sch.get("status") != "active":
            continue
        next_run_str = sch.get("next_run")
        if not next_run_str:
            continue
        try:
            next_run = datetime.fromisoformat(str(next_run_str).replace("Z", "+00:00"))
            # Add timezone if naive
            if next_run.tzinfo is None:
                next_run = next_run.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue

        if next_run > now:
            continue                       # not due yet

        # ── Schedule is due — execute it ──
        schedule_id = sch.get("schedule_id", "")
        table_name = sch.get("table_name", "")
        group_id = sch.get("group_id", "")

        # File-based dedup: if there's a recent "running" history entry for
        # this schedule_id (< 2 min), another process already submitted it.
        already_running = False
        for h in data.get("history", []):
            if h.get("schedule_id") != schedule_id:
                continue
            if h.get("result") not in ("running", "started"):
                continue
            try:
                ht = datetime.fromisoformat(str(h.get("timestamp", "")).replace("Z", "+00:00"))
                if ht.tzinfo is None:
                    ht = ht.replace(tzinfo=timezone.utc)
                if (now - ht).total_seconds() < 120:
                    already_running = True
                    break
            except (ValueError, TypeError):
                continue
        if already_running:
            logger.info("⏭️ Scheduler: skipping %s — recent running entry found", table_name)
            # Advance next_run so we don't keep re-checking on every tick
            sch["next_run"] = _compute_next_run(
                sch.get("type", "cron"), sch.get("cron", ""),
                sch.get("interval_value"), sch.get("interval_unit", "hours"),
                sch.get("once_at", ""), last_run=now.isoformat(),
            )
            changed = True
            continue

        if not group_id:
            for gid, grp in wfm.PIPELINE_GROUPS.items():
                if grp.get("table_name") == table_name:
                    group_id = gid
                    sch["group_id"] = gid
                    break

        # Guard: don't re-trigger if the pipeline is RECENTLY running
        if group_id:
            grp = wfm.PIPELINE_GROUPS.get(group_id)
            if grp and grp.get("status") == "running":
                # Only skip if last_run is recent (< 10 min); stale status = proceed
                last_run_str = sch.get("last_run")
                skip_run = True
                if last_run_str:
                    try:
                        lr = datetime.fromisoformat(str(last_run_str).replace("Z", "+00:00"))
                        if lr.tzinfo is None:
                            lr = lr.replace(tzinfo=timezone.utc)
                        if (now - lr).total_seconds() > 600:
                            skip_run = False
                            grp["status"] = ""  # clear stale status
                    except (ValueError, TypeError):
                        pass
                if skip_run:
                    logger.info("⏭️ Scheduler: skipping %s — pipeline already running", table_name)
                    continue

        logger.info("⏰ Scheduler tick: running schedule %s for table %s", schedule_id, table_name)

        run_ts = datetime.now(timezone.utc).isoformat()

        # ── Pre-save: advance next_run and write to file BEFORE the Databricks
        #    API call so that concurrent/overlapping processes see the future
        #    next_run and won't re-trigger.
        sch["last_run"] = run_ts
        sch_type = sch.get("type", "cron")
        if sch_type == "once":
            sch["status"] = "completed"
        else:
            sch["next_run"] = _compute_next_run(
                sch_type, sch.get("cron", ""),
                sch.get("interval_value"), sch.get("interval_unit", "hours"),
                sch.get("once_at", ""), last_run=run_ts,
            )
        _save_schedules_local(data)

        if not group_id:
            hist = {
                "timestamp": run_ts, "schedule_id": schedule_id,
                "table_name": table_name, "trigger": "scheduler",
                "result": "failed",
                "details": f"No pipeline group found for table '{table_name}'",
            }
            data.setdefault("history", []).insert(0, hist)
            sch["last_run"] = run_ts
            sch["next_run"] = _compute_next_run(
                sch.get("type", "cron"), sch.get("cron", ""),
                sch.get("interval_value"), sch.get("interval_unit", "hours"),
                sch.get("once_at", ""), last_run=run_ts,
            )
            changed = True
            continue

        try:
            cfg = get_config()
            job_names = sch.get("job_names", [])
            if not job_names:
                grp = wfm.PIPELINE_GROUPS.get(group_id, {})
                for jid in grp.get("job_ids", []):
                    job = wfm.JOB_REGISTRY.get(jid, {})
                    if job.get("job_name"):
                        job_names.append(job["job_name"])

            result = wfm.run_pipeline_on_databricks(
                group_id=group_id, host=cfg.get("databricks_host", ""),
                token=get_databricks_token(),
                password=get_source_password(),
                catalog=cfg.get("metadata_catalog", ""),
                schema=cfg.get("metadata_schema", ""),
                recon_catalog=cfg.get("reconciliation", {}).get("catalog", "reconciliation"),
                recon_schema=cfg.get("reconciliation", {}).get("schema", "hr"),
                recon_table=cfg.get("reconciliation", {}).get("table", "ReconcilationDetails"),
                log_catalog=cfg.get("logging", {}).get("catalog", "loggingdetails"),
                log_schema=cfg.get("logging", {}).get("schema", "hr"),
                log_table=cfg.get("logging", {}).get("table", "ExecutionLog"),
            )

            jobs_str = " → ".join(job_names) if job_names else table_name
            detail_msg = ""
            if result.get("success"):
                detail_msg = str(result.get("run_id", result.get("message", "")))
                if result.get("run_url"):
                    detail_msg += f" | {result['run_url']}"
            else:
                detail_msg = str(result.get("error", "Unknown error"))

            hist = {
                "timestamp": run_ts, "schedule_id": schedule_id,
                "table_name": table_name, "jobs": jobs_str,
                "group_id": group_id,
                "trigger": "scheduler",
                "result": "running" if result.get("success") else "failed",
                "details": detail_msg,
            }
            data.setdefault("history", []).insert(0, hist)
            try:
                wfm.scheduler_insert_history(hist)
            except Exception:
                pass

        except Exception as exc:
            logger.error("Scheduler execution error for %s: %s", table_name, exc)
            hist = {
                "timestamp": run_ts, "schedule_id": schedule_id,
                "table_name": table_name, "trigger": "scheduler",
                "result": "failed", "details": str(exc)[:500],
            }
            data.setdefault("history", []).insert(0, hist)

        # next_run and last_run already updated in the pre-save above
        changed = True
        try:
            wfm.scheduler_upsert_config(sch)
        except Exception:
            pass

    if changed:
        data["history"] = data.get("history", [])[:200]
        _save_schedules_local(data)


def _scheduler_loop():
    """Background loop — runs _scheduler_tick every 60 seconds."""
    import time
    logger.info("🕐 Background scheduler started (60s check interval)")
    while _scheduler_running:
        try:
            _scheduler_tick()
        except Exception as exc:
            logger.error("Scheduler loop error: %s", exc)
        time.sleep(60)
    logger.info("🕐 Background scheduler stopped")


def _extract_run_id(details: str) -> str:
    """Extract Databricks run_id from details like '483566655892872 | https://...'."""
    if not details:
        return ""
    parts = details.split("|")
    candidate = parts[0].strip()
    return candidate if candidate.isdigit() else ""


def _on_pipeline_done(group_id: str, status: str):
    """Callback fired by workflow_manager when a Databricks run finishes.
    Updates matching 'running'/'started' history entries in both local file AND DB."""
    with _sch_file_lock:
        try:
            data = _load_schedules_local()
            changed = False
            for h in data.get("history", []):
                if h.get("group_id") == group_id and h.get("result") in ("running", "started"):
                    h["result"] = status
                    changed = True
                    # Also persist to DB
                    try:
                        wfm.scheduler_update_history_result(
                            h.get("schedule_id", ""), h.get("timestamp", ""), status)
                    except Exception:
                        pass
            if changed:
                data["history"] = data["history"][:200]
                _save_schedules_local(data)
                logger.info("📋 Schedule history updated via callback: group=%s → %s", group_id, status)
        except Exception as exc:
            logger.error("Error in _on_pipeline_done callback: %s", exc)


# Register the callback so the poller updates history immediately on completion
wfm.on_pipeline_complete(_on_pipeline_done)


def start_scheduler():
    """Start the background scheduler thread (call once from app.py).
    Uses file-based PID lock to prevent duplicate schedulers across gunicorn workers."""
    global _scheduler_running
    with _scheduler_lock:
        if _scheduler_running:
            return
        # Cross-process guard: only one worker should run the scheduler
        pid_file = os.path.join(_PERSISTENT_DIR, ".scheduler.pid")
        try:
            if os.path.isfile(pid_file):
                with open(pid_file, "r") as f:
                    old_pid = int(f.read().strip())
                # Check if that process is still alive
                try:
                    os.kill(old_pid, 0)
                    logger.info("Scheduler already active in worker PID %d, skipping", old_pid)
                    return
                except (OSError, ProcessLookupError):
                    pass  # Dead process — take over
            with open(pid_file, "w") as f:
                f.write(str(os.getpid()))
        except Exception:
            pass  # If lock fails, proceed anyway (better one duplicate than zero schedulers)
        _scheduler_running = True
    # ── Initialize local file from DB if missing (survives deploys) ──
    try:
        local = _load_schedules_local()
        if not local.get("schedules"):
            db_data = _load_schedules()
            if db_data.get("schedules"):
                _save_schedules_local(db_data)
                logger.info("📂 Scheduler: initialized local file from DB (%d schedules)", len(db_data["schedules"]))
    except Exception as exc:
        logger.warning("Scheduler initialization from DB failed: %s", exc)
    t = threading.Thread(target=_scheduler_loop, daemon=True, name="SchedulerBG")
    t.start()
    logger.info("✅ Scheduler background thread launched (PID %d)", os.getpid())
