"""Healer blueprint — self-healing bot endpoints."""
from flask import Blueprint, request, jsonify
import os, json, time, requests as req

from .auth import login_required
from log_config import get_logger
from config_cache import get_config, get_databricks_token, get_source_password
from databricks_connector import DatabricksConnector
import self_healing_bot as healer
from keyvault_helper import is_masked

logger = get_logger(__name__)
healer_bp = Blueprint("healer", __name__, url_prefix="/api/v1")


@healer_bp.route("/healer/health-check", methods=["POST"])
@login_required
def healer_health_check():
    try:
        d = request.get_json() or {}
        host = d.get("host", "").strip()
        token = d.get("token", "").strip()
        if not host or not token or is_masked(token):
            cfg = get_config()
            host = host or cfg.get("databricks_host", "").rstrip("/")
            token = get_databricks_token()
        connector = DatabricksConnector(host, token) if host and token else None
        server = d.get("server", "").strip()
        if not server:
            cfg = get_config()
            src_cfg = cfg.get("source", {})
            server = src_cfg.get("server", "")
            if server:
                d = {
                    "source_type": src_cfg.get("source_type", "sqlserver"),
                    "server": server, "database": src_cfg.get("database", ""),
                    "username": src_cfg.get("username", ""),
                    "password": get_source_password(),
                }
        source_config = {
            "source_type": d.get("source_type", "sqlserver"),
            "server": d.get("server", "").strip(),
            "database": d.get("database", "").strip(),
            "username": d.get("username", "").strip(),
            "password": d.get("password", ""),
        } if d.get("server") else None
        result = healer.run_health_check(connector=connector, host=host,
                                          token=token, source_config=source_config)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@healer_bp.route("/healer/diagnose", methods=["POST"])
@login_required
def healer_diagnose():
    try:
        d = request.get_json() or {}
        error_text = d.get("error_text", "").strip()
        context = d.get("context", {})
        if not error_text:
            return jsonify({"success": False, "error": "error_text is required"}), 400
        return jsonify({"success": True, **healer.diagnose_error(error_text, context)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@healer_bp.route("/healer/heal", methods=["POST"])
@login_required
def healer_heal():
    try:
        d = request.get_json() or {}
        action = d.get("action", "notify")
        host = d.get("host", "").strip()
        token = d.get("token", "").strip()
        if not token or is_masked(token):
            token = get_databricks_token()
        if not host:
            cfg = get_config()
            host = cfg.get("databricks_host", "").rstrip("/")
        context = d.get("context", {})
        connector = DatabricksConnector(host, token) if host and token else None
        return jsonify({"success": True, **healer.execute_heal(action, connector=connector, context=context)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@healer_bp.route("/healer/monitor/start", methods=["POST"])
@login_required
def healer_monitor_start():
    try:
        d = request.get_json() or {}
        run_id = d.get("run_id")
        host = d.get("host", "").strip()
        token = d.get("token", "").strip()
        if not token or is_masked(token):
            token = get_databricks_token()
        if not host:
            cfg = get_config()
            host = cfg.get("databricks_host", "").rstrip("/")
        auto_heal = d.get("auto_heal", True)
        if not run_id:
            return jsonify({"success": False, "error": "run_id is required"}), 400
        connector = DatabricksConnector(host, token) if host and token else None
        return jsonify(healer.start_monitor(int(run_id), connector=connector, auto_heal=auto_heal))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@healer_bp.route("/healer/monitor/check/<monitor_id>", methods=["POST"])
@login_required
def healer_monitor_check(monitor_id):
    try:
        d = request.get_json() or {}
        host = d.get("host", "").strip()
        token = d.get("token", "").strip()
        if not token or is_masked(token):
            token = get_databricks_token()
        if not host:
            cfg = get_config()
            host = cfg.get("databricks_host", "").rstrip("/")
        connector = DatabricksConnector(host, token) if host and token else None
        return jsonify(healer.check_monitor(monitor_id, connector=connector))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@healer_bp.route("/healer/monitors", methods=["GET"])
@login_required
def healer_list_monitors():
    return jsonify({"success": True, "monitors": healer.list_monitors()})


@healer_bp.route("/healer/recent-runs", methods=["GET"])
@login_required
def healer_recent_runs():
    try:
        cfg = get_config()
        host = cfg.get("databricks_host", "").rstrip("/")
        token = get_databricks_token()
        if not host or not token:
            return jsonify({"success": False, "error": "Databricks not configured"}), 400
        resp = req.get(
            f"{host}/api/2.1/jobs/runs/list",
            headers={"Authorization": f"Bearer {token}"},
            params={"limit": 25, "expand_tasks": "false"}, timeout=15,
        )
        if resp.status_code != 200:
            return jsonify({"success": False, "error": f"Databricks API: {resp.text[:200]}"}), 500
        data = resp.json()
        runs = []
        for r in data.get("runs", []):
            state = r.get("state", {})
            runs.append({
                "run_id": r.get("run_id"), "run_name": r.get("run_name", ""),
                "job_id": r.get("job_id"), "life_cycle": state.get("life_cycle_state", ""),
                "result_state": state.get("result_state", ""),
                "start_time": r.get("start_time"), "end_time": r.get("end_time"),
            })
        return jsonify({"success": True, "runs": runs})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@healer_bp.route("/healer/monitor/stop/<monitor_id>", methods=["POST"])
@login_required
def healer_monitor_stop(monitor_id):
    return jsonify(healer.stop_monitor(monitor_id))


@healer_bp.route("/healer/restore-point", methods=["POST"])
@login_required
def healer_create_restore_point():
    try:
        d = request.get_json() or {}
        key = d.get("key", f"rp_{int(time.time())}")
        metadata = d.get("metadata", {})
        return jsonify(healer.create_restore_point(key, metadata))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@healer_bp.route("/healer/restore-points", methods=["GET"])
@login_required
def healer_list_restore_points():
    return jsonify({"success": True, "restore_points": healer.list_restore_points()})


@healer_bp.route("/healer/restore-point/<key>", methods=["DELETE"])
@login_required
def healer_delete_restore_point(key):
    return jsonify(healer.delete_restore_point(key))


@healer_bp.route("/healer/rules", methods=["GET"])
@login_required
def healer_get_rules():
    return jsonify({"success": True, "rules": healer.get_rules()})


@healer_bp.route("/healer/rules/toggle", methods=["POST"])
@login_required
def healer_toggle_rule():
    d = request.get_json() or {}
    return jsonify(healer.toggle_rule(d.get("rule_id"), d.get("enabled", True)))


@healer_bp.route("/healer/rules/add", methods=["POST"])
@login_required
def healer_add_rule():
    d = request.get_json() or {}
    return jsonify(healer.add_rule(
        name=d.get("name", "Custom Rule"), category=d.get("category", "GENERIC_ERROR"),
        action=d.get("action", "retry"), max_retries=d.get("max_retries", 3),
        description=d.get("description", ""),
    ))


@healer_bp.route("/healer/history", methods=["GET"])
@login_required
def healer_history():
    limit = request.args.get("limit", 50, type=int)
    severity = request.args.get("severity", None)
    return jsonify({"success": True, "history": healer.get_history(limit, severity)})


@healer_bp.route("/healer/history/clear", methods=["POST"])
@login_required
def healer_clear_history():
    return jsonify(healer.clear_history())


@healer_bp.route("/healer/stats", methods=["GET"])
@login_required
def healer_stats():
    return jsonify({"success": True, **healer.get_stats()})
