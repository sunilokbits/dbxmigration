"""
Databricks Connection Manager
Tests connectivity, uploads notebooks, and runs jobs via the Databricks SDK.
"""

import base64
import os
import time
from datetime import datetime

import requests as _requests
from log_config import get_logger

logger = get_logger(__name__)


def _is_databricks_app() -> bool:
    """Detect if running inside a Databricks App (OAuth env vars present)."""
    return bool(os.environ.get("DATABRICKS_CLIENT_ID") and os.environ.get("DATABRICKS_CLIENT_SECRET"))


class DatabricksConnector:
    """Manages connections to a Databricks workspace via the Databricks SDK."""

    def __init__(self, host: str, token: str):
        self.host = host.rstrip("/")
        self.token = token
        # Prefer an explicit PAT token when valid — it grants full user permissions
        # (clusters, jobs, notebooks). Fall back to SP OAuth only when no PAT is
        # available; the SP is limited to effective_user_api_scopes (IAM read only).
        _masked_chars = set('\u2022*')
        _valid_pat = bool(
            token
            and token.strip()
            and not all(c in _masked_chars for c in token.strip())
            and len(token.strip()) > 8
        )
        if _valid_pat:
            # Use requests.Session — zero SDK, zero auth conflict.
            # The Databricks Apps runtime always injects DATABRICKS_CLIENT_ID and
            # DATABRICKS_CLIENT_SECRET env vars. Any WorkspaceClient(token=...) call
            # then raises "validate: more than one authorization method configured:
            # oauth and pat" regardless of SDK version or credentials_strategy.
            # Solution: skip the SDK entirely when a PAT is available.
            self._sess = _requests.Session()
            self._sess.headers.update({
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            })
            self._client = None
        elif _is_databricks_app():
            from databricks.sdk import WorkspaceClient
            self._client = WorkspaceClient()
            self._sess = None
        else:
            from databricks.sdk import WorkspaceClient
            self._client = WorkspaceClient(host=self.host, token=token)
            self._sess = None

    # ── REST helper (PAT path only) ───────────────────────────────────────────
    def _api(self, method: str, path: str, **kwargs) -> dict:
        """Call the Databricks REST API with the PAT session."""
        resp = self._sess.request(method, f"{self.host}{path}", timeout=60, **kwargs)
        if resp.status_code in (200, 201):
            try:
                return resp.json()
            except Exception:
                return {}
        return {"_http_error": f"HTTP {resp.status_code}: {resp.text[:300]}"}

    # ── Connection Test ───────────────────────────────────────────────────────
    def test_connection(self) -> dict:
        """Verify token and host by listing clusters."""
        try:
            if self._sess:
                data = self._api("GET", "/api/2.0/clusters/list")
                if "_http_error" in data:
                    err = data["_http_error"]
                    if "401" in err:
                        return {"success": False, "message": "Authentication Failed", "status_code": 401}
                    if "403" in err:
                        return {"success": False, "message": "Authorization Failed", "status_code": 403}
                    return {"success": False, "message": err}
                clusters = data.get("clusters", [])
                running = [c for c in clusters if c.get("state") == "RUNNING"]
                return {
                    "success": True, "message": "Connection Successful",
                    "workspace_host": self.host,
                    "total_clusters": len(clusters), "running_clusters": len(running),
                    "cluster_names": [c.get("cluster_name", "N/A") for c in clusters[:5]],
                    "timestamp": str(datetime.now()),
                }
            from databricks.sdk import WorkspaceClient as _WC
            from databricks.sdk.service.compute import State as _CS
            clusters = list(self._client.clusters.list())
            running = [c for c in clusters if c.state == _CS.RUNNING]
            return {
                "success": True, "message": "Connection Successful",
                "workspace_host": self.host,
                "total_clusters": len(clusters), "running_clusters": len(running),
                "cluster_names": [c.cluster_name or "N/A" for c in clusters[:5]],
                "timestamp": str(datetime.now()),
            }
        except Exception as e:
            err = str(e)
            if "401" in err:
                return {"success": False, "message": "Authentication Failed — Invalid Token", "status_code": 401}
            if "403" in err:
                return {"success": False, "message": "Authorization Failed — Insufficient Permissions", "status_code": 403}
            return {"success": False, "message": f"Connection error: {err[:300]}"}

    # ── Workspace Info ────────────────────────────────────────────────────────
    def get_workspace_info(self) -> dict:
        """Return basic workspace metadata."""
        try:
            me = self._client.current_user.me()
            emails = me.emails or []
            email = emails[0].value if emails else "N/A"
            return {
                "success": True,
                "user_name": me.user_name or "N/A",
                "user_email": email,
                "dbfs_root": [],
                "host": self.host,
            }
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── Delete Notebook ─────────────────────────────────────────────────────
    def delete_notebook(self, notebook_path: str) -> dict:
        """Delete a single notebook from Databricks workspace."""
        try:
            if self._sess:
                resp = self._api("POST", "/api/2.0/workspace/delete", json={"path": notebook_path, "recursive": False})
                if "_http_error" in resp:
                    err = resp["_http_error"]
                    if "RESOURCE_DOES_NOT_EXIST" in err or "404" in err:
                        return {"success": True, "path": notebook_path}
                    return {"success": False, "error": err}
                return {"success": True, "path": notebook_path}
            self._client.workspace.delete(notebook_path, recursive=False)
            return {"success": True, "path": notebook_path}
        except Exception as e:
            if "RESOURCE_DOES_NOT_EXIST" in str(e):
                return {"success": True, "path": notebook_path}
            return {"success": False, "error": str(e)[:300]}

    # ── Upload Notebook ───────────────────────────────────────────────────────
    def upload_notebook(self, notebook_name: str, python_code: str, path: str = "/Shared/Migrations") -> dict:
        """Upload a Python notebook to the Databricks workspace."""
        try:
            notebook_path = f"{path}/{notebook_name}"
            encoded_code = base64.b64encode(python_code.encode("utf-8")).decode("utf-8")

            if self._sess:
                # REST API path — used when PAT token is available (self._client is None)
                # 1. Create directory
                mkdirs_resp = self._api("POST", "/api/2.0/workspace/mkdirs", json={"path": path})
                if "_http_error" in mkdirs_resp and "RESOURCE_ALREADY_EXISTS" not in mkdirs_resp.get("_http_error", ""):
                    logger.warning("mkdirs warning for %s: %s", path, mkdirs_resp.get("_http_error", ""))

                # 2. Import notebook
                import_resp = self._api("POST", "/api/2.0/workspace/import", json={
                    "path": notebook_path,
                    "content": encoded_code,
                    "format": "SOURCE",
                    "language": "PYTHON",
                    "overwrite": True,
                })
                if "_http_error" in import_resp:
                    return {"success": False, "error": import_resp["_http_error"], "path": notebook_path}
                return {
                    "success": True,
                    "path": notebook_path,
                    "message": f"Notebook uploaded: {notebook_path}",
                }

            # SDK path
            self._client.workspace.mkdirs(path)
            from databricks.sdk.service.workspace import ImportFormat, Language
            self._client.workspace.import_(
                path=notebook_path,
                content=encoded_code,
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                overwrite=True,
            )

            return {
                "success": True,
                "message": "Notebook uploaded successfully",
                "notebook_path": notebook_path,
                "workspace_url": f"{self.host}/#workspace{notebook_path}",
                "lines_uploaded": len(python_code.splitlines()),
            }
        except Exception as e:
            return {"success": False, "message": f"Upload error: {str(e)[:300]}"}

    # ── Run Notebook via Job ──────────────────────────────────────────────────
    def run_notebook(self, notebook_path: str, cluster_id: str = None,
                     params: dict = None) -> dict:
        """Submit a one-time run for a notebook in Databricks."""
        try:
            nb_name = notebook_path.rsplit("/", 1)[-1]
            task_key = nb_name.replace(" ", "_")[:100]
            nb_task_d = {"notebook_path": notebook_path, "base_parameters": params or {}}

            if self._sess:
                task = {"task_key": task_key, "notebook_task": nb_task_d}
                if cluster_id:
                    task["existing_cluster_id"] = cluster_id
                    d = self._api("POST", "/api/2.1/jobs/runs/submit",
                                  json={"run_name": f"MigrationStudio_{nb_name}",
                                        "tasks": [task]})
                    if "_http_error" in d:
                        return {"success": False, "message": d["_http_error"]}
                    return self._fmt_run(d.get("run_id"))
                try:
                    sl = {**task, "environment_key": "Default"}
                    d = self._api("POST", "/api/2.1/jobs/runs/submit", json={
                        "run_name": f"MigrationStudio_{nb_name}", "tasks": [sl],
                        "environments": [{"environment_key": "Default",
                                          "spec": {"client": "1"}}],
                    })
                    if "run_id" in d:
                        return self._fmt_run(d["run_id"])
                except Exception:
                    pass
                fallback_cluster = None
                try:
                    cd = self._api("GET", "/api/2.0/clusters/list")
                    for c in cd.get("clusters", []):
                        if c.get("state") == "RUNNING":
                            fallback_cluster = c.get("cluster_id")
                            break
                except Exception:
                    pass
                if fallback_cluster:
                    task["existing_cluster_id"] = fallback_cluster
                else:
                    task["new_cluster"] = {"spark_version": "14.3.x-scala2.12",
                                           "node_type_id": "Standard_DS3_v2",
                                           "num_workers": 2}
                d = self._api("POST", "/api/2.1/jobs/runs/submit",
                              json={"run_name": f"MigrationStudio_{nb_name}",
                                    "tasks": [task]})
                if "_http_error" in d:
                    return {"success": False, "message": d["_http_error"]}
                return self._fmt_run(d.get("run_id"))

            from databricks.sdk.service.jobs import SubmitTask, NotebookTask, JobEnvironment
            from databricks.sdk.service.compute import (
                State as ClusterState, Environment as ComputeEnvironment, ClusterSpec)
            nb_task = NotebookTask(notebook_path=notebook_path, base_parameters=params or {})
            if cluster_id:
                run = self._client.jobs.submit(
                    run_name=f"MigrationStudio_{nb_name}",
                    tasks=[SubmitTask(task_key=task_key,
                                     existing_cluster_id=cluster_id,
                                     notebook_task=nb_task)]).result()
                return self._format_run_result(run)
            try:
                run = self._client.jobs.submit(
                    run_name=f"MigrationStudio_{nb_name}",
                    tasks=[SubmitTask(task_key=task_key, environment_key="Default",
                                     notebook_task=nb_task)],
                    environments=[JobEnvironment(environment_key="Default",
                                                 spec=ComputeEnvironment(client="1"))]).result()
                return self._format_run_result(run)
            except Exception as e:
                if not any(kw in str(e).lower() for kw in
                           ("serverless", "environment_key", "not supported", "not enabled")):
                    return {"success": False, "message": f"Run submit failed: {str(e)[:300]}"}
            fallback_cluster = None
            for c in self._client.clusters.list():
                if c.state == ClusterState.RUNNING:
                    fallback_cluster = c.cluster_id
                    break
            st = SubmitTask(task_key=task_key, notebook_task=nb_task)
            if fallback_cluster:
                st.existing_cluster_id = fallback_cluster
            else:
                st.new_cluster = ClusterSpec(spark_version="14.3.x-scala2.12",
                                             node_type_id="Standard_DS3_v2",
                                             num_workers=2)
            run = self._client.jobs.submit(
                run_name=f"MigrationStudio_{nb_name}", tasks=[st]).result()
            return self._format_run_result(run)

        except Exception as e:
            return {"success": False, "message": f"Run error: {str(e)[:300]}"}

    def _fmt_run(self, run_id) -> dict:
        return {"success": True, "run_id": run_id, "message": "Notebook run submitted",
                "run_url": f"{self.host}/#job/{run_id}/run/{run_id}",
                "submitted_at": str(datetime.now())}

    def _format_run_result(self, run) -> dict:
        return self._fmt_run(run.run_id)

    # ── Get Run Status ────────────────────────────────────────────────────────
    def get_run_status(self, run_id: int) -> dict:
        """Get job run status."""
        try:
            if self._sess:
                data = self._api("GET", f"/api/2.1/jobs/runs/get?run_id={run_id}")
                state = data.get("state", {})
                return {"success": True, "run_id": run_id,
                        "life_cycle": state.get("life_cycle_state", "UNKNOWN"),
                        "result_state": state.get("result_state", ""),
                        "state_message": state.get("state_message", ""),
                        "start_time": data.get("start_time"),
                        "end_time": data.get("end_time")}
            run = self._client.jobs.get_run(run_id)
            state = run.state
            return {"success": True, "run_id": run_id,
                    "life_cycle": state.life_cycle_state.value if state and state.life_cycle_state else "UNKNOWN",
                    "result_state": state.result_state.value if state and state.result_state else "",
                    "state_message": state.state_message or "" if state else "",
                    "start_time": run.start_time, "end_time": run.end_time}
        except Exception as e:
            return {"success": False, "message": str(e)[:300]}

    # ── Get Run Output ───────────────────────────────────────────────────────
    def get_run_output(self, run_id: int) -> dict:
        """Fetch notebook output and error details for a completed run."""
        try:
            if self._sess:
                run = self._api("GET", f"/api/2.1/jobs/runs/get?run_id={run_id}")
                tasks_list = run.get("tasks", [])
                task_summaries, task_errors, task_nb_result = [], [], ""
                for t in tasks_list:
                    ts = t.get("state", {})
                    t_run_id = t.get("run_id")
                    t_nb_result = t_error = t_trace = ""
                    if t_run_id:
                        try:
                            t_out = self._api("GET",
                                f"/api/2.1/jobs/runs/get-output?run_id={t_run_id}")
                            t_nb_result = (t_out.get("notebook_output") or {}).get("result", "")
                            t_error = t_out.get("error", "")
                            t_trace = t_out.get("error_trace", "")
                        except Exception:
                            pass
                    task_summaries.append({"task_key": t.get("task_key", ""),
                        "life_cycle": ts.get("life_cycle_state", ""),
                        "result_state": ts.get("result_state", ""),
                        "state_message": ts.get("state_message", "")})
                    if t_error or t_trace:
                        task_errors.append(f"[{t.get('task_key','task')}] {t_error}")
                        if t_trace:
                            task_errors.append(t_trace[:1500])
                    if t_nb_result and not task_nb_result:
                        task_nb_result = t_nb_result
                state = run.get("state", {})
                err_str = "\n".join(task_errors)
                return {"success": True, "run_id": run_id,
                        "notebook_result": task_nb_result[:2000] if task_nb_result else "",
                        "notebook_truncated": False, "error": err_str[:2000],
                        "error_trace": err_str[:2000],
                        "life_cycle": state.get("life_cycle_state", ""),
                        "result_state": state.get("result_state", ""),
                        "state_message": state.get("state_message", ""),
                        "tasks": task_summaries}
            run = self._client.jobs.get_run(run_id)
            tasks_list = run.tasks or []
            task_summaries, task_errors, task_nb_result = [], [], ""
            for t in tasks_list:
                ts = t.state
                t_run_id = t.run_id
                t_nb_result = t_error = t_trace = ""
                if t_run_id:
                    try:
                        t_out = self._client.jobs.get_run_output(t_run_id)
                        t_nb_result = t_out.notebook_output.result or "" if t_out.notebook_output else ""
                        t_error = t_out.error or ""
                        t_trace = t_out.error_trace or ""
                    except Exception:
                        pass
                task_summaries.append({"task_key": t.task_key or "",
                    "life_cycle": ts.life_cycle_state.value if ts and ts.life_cycle_state else "",
                    "result_state": ts.result_state.value if ts and ts.result_state else "",
                    "state_message": ts.state_message or "" if ts else ""})
                if t_error or t_trace:
                    task_errors.append(f"[{t.task_key or 'task'}] {t_error}")
                    if t_trace:
                        task_errors.append(t_trace[:1500])
                if t_nb_result and not task_nb_result:
                    task_nb_result = t_nb_result
            state = run.state
            err_str = "\n".join(task_errors)
            return {"success": True, "run_id": run_id,
                    "notebook_result": task_nb_result[:2000] if task_nb_result else "",
                    "notebook_truncated": False, "error": err_str[:2000],
                    "error_trace": err_str[:2000],
                    "life_cycle": state.life_cycle_state.value if state and state.life_cycle_state else "",
                    "result_state": state.result_state.value if state and state.result_state else "",
                    "state_message": state.state_message or "" if state else "",
                    "tasks": task_summaries}
        except Exception as e:
            return {"success": False, "message": str(e)[:300]}

    # ── List Clusters ─────────────────────────────────────────────────────────
    def list_clusters(self) -> dict:
        """Return available clusters."""
        try:
            if self._sess:
                data = self._api("GET", "/api/2.0/clusters/list")
                if "_http_error" in data:
                    return {"success": False, "message": data["_http_error"]}
                return {"success": True, "clusters": [
                    {"cluster_id": c.get("cluster_id", ""),
                     "cluster_name": c.get("cluster_name", ""),
                     "state": c.get("state", "UNKNOWN"),
                     "spark_version": c.get("spark_version", ""),
                     "num_workers": c.get("num_workers", 0)}
                    for c in data.get("clusters", [])
                ]}
            clusters = list(self._client.clusters.list())
            return {"success": True, "clusters": [
                {"cluster_id": c.cluster_id, "cluster_name": c.cluster_name,
                 "state": c.state.value if c.state else "UNKNOWN",
                 "spark_version": c.spark_version, "num_workers": c.num_workers or 0}
                for c in clusters
            ]}
        except Exception as e:
            return {"success": False, "message": str(e)[:300]}

    # ── Start Cluster ─────────────────────────────────────────────────────────
    def start_cluster(self, cluster_id: str) -> dict:
        """Start a terminated Databricks cluster."""
        try:
            if self._sess:
                r = self._api("POST", "/api/2.0/clusters/start",
                              json={"cluster_id": cluster_id})
                if "_http_error" in r:
                    return {"success": False, "message": r["_http_error"]}
                return {"success": True, "message": "Cluster start initiated"}
            self._client.clusters.start(cluster_id=cluster_id)
            return {"success": True, "message": "Cluster start initiated"}
        except Exception as e:
            return {"success": False, "message": str(e)[:300]}
