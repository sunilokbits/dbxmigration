"""
Persistent Databricks Job definitions for the metadata pipeline notebooks.

Every stage notebook gets its own named, permanent Databricks Job, so:
  * each table's Extract / Bronze / Silver / Reconciliation run executes as a
    separate job run with its own serverless driver and executors (isolation),
  * every run is visible, filterable and permissionable in the Databricks
    Jobs UI under a stable, client-readable name.

The job set is fixed per deployment (one job per stage notebook, never per
table) -- tables flow in as run parameters -- so adding/removing pipelines
never requires touching the job definitions. Jobs are matched by name +
notebook path and updated in place with jobs/reset, which keeps the job_id,
run history and any permissions an admin has granted.
"""

import json

from log_config import get_logger

logger = get_logger(__name__)

JOB_NAME_PREFIX = "Migration Studio"
ORCHESTRATOR_NB = "00_Meta_Orchestrator"

# Insertion order matters: child jobs are created before the orchestrator,
# whose default parameters carry the child job ids. 05_Meta_ExecutionLog is
# intentionally absent -- it is lightweight bookkeeping over the
# orchestrator's own results and its payload exceeds the Jobs API's 10 KB
# parameter limit, so it stays an in-process step of the orchestrator.
PIPELINE_JOBS = {
    "01_Meta_Extract": {
        "title": "01 Extract (Source to Landing)",
        "task_key": "extract",
        "layer": "extract",
        "timeout_seconds": 3600,
        "max_concurrent_runs": 100,
        "description": "Extracts one source table into the landing zone. One run per table.",
    },
    "02_Meta_Bronze": {
        "title": "02 Bronze (Landing to Bronze)",
        "task_key": "bronze",
        "layer": "bronze",
        "timeout_seconds": 3600,
        "max_concurrent_runs": 100,
        "description": "Loads one table from the landing zone into its Bronze Delta table. One run per table.",
    },
    "03_Meta_Silver": {
        "title": "03 Silver (Bronze to Silver)",
        "task_key": "silver",
        "layer": "silver",
        "timeout_seconds": 3600,
        "max_concurrent_runs": 100,
        "description": "Cleanses one table from Bronze into its Silver Delta table. One run per table.",
    },
    "04_Meta_Reconciliation": {
        "title": "04 Reconciliation (Source vs Bronze)",
        "task_key": "reconciliation",
        "layer": "reconciliation",
        "timeout_seconds": 1800,
        "max_concurrent_runs": 100,
        "description": "Reconciles one table's source aggregates against Bronze. One run per table.",
    },
    ORCHESTRATOR_NB: {
        "title": "00 Pipeline Orchestrator",
        "task_key": "orchestrator",
        "layer": "orchestrator",
        "timeout_seconds": 0,
        "max_concurrent_runs": 20,
        "description": (
            "Entry point for a pipeline group run. Reads pipeline metadata and launches "
            "each stage as its own run of the stage jobs above."
        ),
    },
}

_MANAGED_NOTE = (
    " Managed by Migration Studio -- redeploy notebooks from the app to update; "
    "manual edits to this job's settings are overwritten on the next deploy."
)

_SERVERLESS_ENV = [{"environment_key": "Default", "spec": {"client": "1"}}]


def job_display_name(notebook_name: str) -> str:
    meta = PIPELINE_JOBS.get(notebook_name)
    return f"{JOB_NAME_PREFIX} - {meta['title'] if meta else notebook_name}"


def build_job_settings(notebook_name: str, notebook_path: str, name: str = None,
                       base_parameters: dict = None, serverless_env: bool = True) -> dict:
    meta = PIPELINE_JOBS[notebook_name]
    task = {
        "task_key": meta["task_key"],
        "notebook_task": {
            "notebook_path": notebook_path,
            "source": "WORKSPACE",
            "base_parameters": base_parameters or {},
        },
    }
    settings = {
        "name": name or job_display_name(notebook_name),
        "description": meta["description"] + _MANAGED_NOTE,
        "tags": {
            "application": "migration-studio",
            "layer": meta["layer"],
            "managed_by": "migration-studio-app",
        },
        "max_concurrent_runs": meta["max_concurrent_runs"],
        "timeout_seconds": meta["timeout_seconds"],
        "queue": {"enabled": True},
        "tasks": [task],
    }
    if serverless_env:
        task["environment_key"] = "Default"
        settings["environments"] = _SERVERLESS_ENV
    return settings


def _upsert(connector, settings: dict, notebook_path: str) -> dict:
    """Find-or-create one job; falls back to the bare serverless shape if the
    workspace rejects the explicit environment spec."""
    base_name = settings["name"]
    found = connector.list_jobs_by_name(base_name)
    if not found.get("success"):
        return {"success": False, "error": found.get("error", "job lookup failed")}

    job_id = next((j["job_id"] for j in found["jobs"] if notebook_path in j["notebook_paths"]), None)
    if job_id is None and found["jobs"]:
        # Another deployment in this workspace already owns this name; keep
        # ours distinguishable instead of creating an identically-named twin.
        settings = dict(settings, name=f"{base_name} [{notebook_path.rsplit('/', 1)[0]}]")
        again = connector.list_jobs_by_name(settings["name"])
        if again.get("success"):
            job_id = next((j["job_id"] for j in again["jobs"] if notebook_path in j["notebook_paths"]), None)

    def _write(s):
        if job_id is not None:
            return connector.reset_job(job_id, s), job_id, "updated"
        r = connector.create_job(s)
        return r, r.get("job_id"), "created"

    r, jid, action = _write(settings)
    if not r.get("success") and "environment" in str(r.get("error", "")).lower():
        bare = {k: v for k, v in settings.items() if k != "environments"}
        bare["tasks"] = [{k: v for k, v in t.items() if k != "environment_key"} for t in settings["tasks"]]
        r, jid, action = _write(bare)
    if not r.get("success"):
        return {"success": False, "error": r.get("error", "job create/update failed")}
    return {"success": True, "job_id": jid, "action": action, "job_name": settings["name"]}


def ensure_pipeline_jobs(connector, workspace_path: str, notebook_names) -> dict:
    """Create or update the persistent job for every deployed stage notebook.

    Returns {"jobs": {notebook_name: job_id}, "results": [...]}. A failure for
    one job never blocks the others -- runs fall back to one-time isolated
    submissions for any stage whose job is missing.
    """
    deployed = set(notebook_names)
    jobs, results = {}, []
    for nb_name in PIPELINE_JOBS:
        if nb_name not in deployed:
            continue
        nb_path = f"{workspace_path}/{nb_name}"
        base_params = {}
        if nb_name == ORCHESTRATOR_NB:
            base_params = {"child_jobs": json.dumps(jobs), "workspace_path": workspace_path}
        try:
            res = _upsert(connector, build_job_settings(nb_name, nb_path, base_parameters=base_params), nb_path)
        except Exception as exc:
            res = {"success": False, "error": str(exc)[:300]}
        if res.get("success"):
            jobs[nb_name] = res["job_id"]
            logger.info("Pipeline job %s: %s (job_id=%s)", res["action"], res["job_name"], res["job_id"])
        else:
            logger.warning("Could not create/update pipeline job for %s: %s", nb_name, res.get("error"))
        results.append({
            "notebook": nb_name,
            "job_name": res.get("job_name") or job_display_name(nb_name),
            "job_id": res.get("job_id"),
            "action": res.get("action"),
            "success": bool(res.get("success")),
            "error": None if res.get("success") else res.get("error"),
        })
    return {"jobs": jobs, "results": results}
