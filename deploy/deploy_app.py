#!/usr/bin/env python3
"""
Deploy Console — standalone one-click deploy UI
=================================================
A separate, minimal Flask app whose only job is collecting the handful of
values needed to deploy DBX Migration Studio into a NEW client workspace,
then running deploy/one_click_deploy.py's pipeline with live progress.

Runs completely independently of the main app (migration_utility/app.py,
port 5000) — its own process, its own port, no shared state.

Usage:
    python deploy/deploy_app.py
    -> http://localhost:5050
"""
from __future__ import annotations

import os
import sys
import threading
import time
import uuid
from pathlib import Path

from flask import Flask, jsonify, render_template, request

sys.path.insert(0, str(Path(__file__).resolve().parent))
import one_click_deploy as ocd  # noqa: E402

app = Flask(__name__)

_JOBS: dict[str, dict] = {}
_JOBS_LOCK = threading.Lock()

REQUIRED_FIELDS = ["client_name", "databricks_host", "databricks_token", "catalog_admin", "app_schema"]


def _build_cfg(form: dict) -> tuple[dict, dict]:
    """Turn the submitted form into (cfg, secrets_in) — nothing here ever touches disk."""
    cfg = {
        "client_name": form["client_name"].strip(),
        "databricks_host": form["databricks_host"].strip(),
        "catalog_admin": (form.get("catalog_admin") or "admin_source").strip(),
        "app_schema": (form.get("app_schema") or "migration_app").strip(),
        "bundle_target": (form.get("bundle_target") or "client").strip(),
        "cloud_provider": (form.get("cloud_provider") or "azure").strip(),
        "sql_warehouse_id": (form.get("sql_warehouse_id") or "").strip(),
        "genie_space_id": (form.get("genie_space_id") or "").strip(),
        "with_infra": bool(form.get("with_infra")),
        "azure": {
            "subscription_id": (form.get("azure_subscription_id") or "").strip(),
            "resource_group": (form.get("azure_resource_group") or "").strip(),
            "region": (form.get("azure_region") or "").strip(),
            "storage_account": (form.get("azure_storage_account") or "").strip(),
            "container": (form.get("azure_container") or "datalake").strip(),
            "access_connector": (form.get("azure_access_connector") or "").strip(),
        },
        "source": {
            "source_type": (form.get("source_type") or "sqlserver").strip(),
            "server": (form.get("source_server") or "").strip(),
            "database": (form.get("source_database") or "").strip(),
        },
    }
    secrets_in = {
        "databricks_token": (form.get("databricks_token") or "").strip(),
        "source_password": (form.get("source_password") or "").strip(),
        "devops_pat": (form.get("devops_pat") or "").strip(),
        "azure_client_secret": (form.get("azure_client_secret") or "").strip(),
    }
    return cfg, secrets_in


@app.get("/")
def index():
    return render_template("deploy_index.html")


@app.get("/health")
def health():
    return jsonify({"app": "deploy-console", "status": "ok"})


@app.post("/api/deploy")
def start_deploy():
    form = request.get_json(silent=True) or {}
    missing = [f for f in REQUIRED_FIELDS if not str(form.get(f, "")).strip()]
    if missing:
        return jsonify({"success": False, "error": f"Missing required field(s): {', '.join(missing)}"}), 400

    cfg, secrets_in = _build_cfg(form)
    job_id = uuid.uuid4().hex[:12]
    with _JOBS_LOCK:
        _JOBS[job_id] = {"status": "running", "steps": [], "success": None, "started_at": time.time()}

    def _on_step(step):
        with _JOBS_LOCK:
            _JOBS[job_id]["steps"].append(step)

    def _worker():
        try:
            # non_interactive=True: this runs in a background thread with no
            # TTY — any input()/install-confirmation prompt would hang forever.
            report = ocd.run_pipeline(cfg, secrets_in, non_interactive=True, on_step=_on_step)
            report.finish_and_write(ocd.REPO_ROOT / "deploy" / "reports")
            with _JOBS_LOCK:
                _JOBS[job_id]["status"] = "done"
                _JOBS[job_id]["success"] = not report.hard_failures
        except Exception as e:
            with _JOBS_LOCK:
                _JOBS[job_id]["status"] = "done"
                _JOBS[job_id]["success"] = False
                _JOBS[job_id]["steps"].append({
                    "phase": "fatal", "name": "Unexpected error", "status": "fail",
                    "message": str(e)[:400], "required": True,
                })

    threading.Thread(target=_worker, daemon=True).start()
    return jsonify({"success": True, "job_id": job_id})


@app.get("/api/deploy/<job_id>/status")
def deploy_status(job_id):
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if not job:
            return jsonify({"ok": False, "error": "unknown job_id"}), 404
        return jsonify({"ok": True, **job})


if __name__ == "__main__":
    port = int(os.environ.get("DEPLOY_APP_PORT", 5050))
    print("=" * 65)
    print("  DBX Migration Studio — Deploy Console")
    print(f"  URL : http://localhost:{port}")
    print("=" * 65)
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)
