"""Persistent stage jobs + isolated child runs.

Covers the app-side job upsert (pipeline_jobs.py), the connector's run-now
preference, and the generated in-notebook launcher (_run_child_notebook),
which is exec'd here against a fake Jobs API.
"""

import json
import os
import sys
import types
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pipeline_jobs
from metadata_notebooks import _gen_common_functions, generate_metadata_notebooks


class FakeConnector:
    def __init__(self, existing=None, reject_env=False):
        self.existing = existing or {}   # name -> [(job_id, [paths])]
        self.reject_env = reject_env
        self.created, self.reset = [], []
        self._next = 100

    def list_jobs_by_name(self, name):
        return {"success": True, "jobs": [{"job_id": j, "notebook_paths": p}
                                          for j, p in self.existing.get(name, [])]}

    def create_job(self, settings):
        if self.reject_env and "environments" in settings:
            return {"success": False, "error": "INVALID_PARAMETER_VALUE: environments not supported"}
        self._next += 1
        self.created.append(settings)
        return {"success": True, "job_id": self._next}

    def reset_job(self, job_id, settings):
        self.reset.append((job_id, settings))
        return {"success": True, "job_id": job_id}


WS = "/Shared/DBX/MetadataPipeline"
STANDARD = ["_Meta_CommonFunctions", "01_Meta_Extract", "02_Meta_Bronze", "03_Meta_Silver",
            "00_Meta_Orchestrator", "04_Meta_Reconciliation"]


class TestEnsurePipelineJobs(unittest.TestCase):
    def test_creates_one_named_job_per_stage_and_wires_orchestrator(self):
        conn = FakeConnector()
        out = pipeline_jobs.ensure_pipeline_jobs(conn, WS, STANDARD)
        names = [s["name"] for s in conn.created]
        self.assertEqual(names, [
            "Migration Studio - 01 Extract (Source to Landing)",
            "Migration Studio - 02 Bronze (Landing to Bronze)",
            "Migration Studio - 03 Silver (Bronze to Silver)",
            "Migration Studio - 04 Reconciliation (Source vs Bronze)",
            "Migration Studio - 00 Pipeline Orchestrator",
        ])
        self.assertNotIn("_Meta_CommonFunctions", out["jobs"])
        orch = conn.created[-1]
        child = json.loads(orch["tasks"][0]["notebook_task"]["base_parameters"]["child_jobs"])
        self.assertEqual(set(child), {"01_Meta_Extract", "02_Meta_Bronze", "03_Meta_Silver",
                                      "04_Meta_Reconciliation"})
        self.assertEqual(orch["tasks"][0]["notebook_task"]["notebook_path"], f"{WS}/00_Meta_Orchestrator")
        self.assertTrue(all(s["tags"]["application"] == "migration-studio" for s in conn.created))

    def test_existing_job_for_same_path_is_updated_in_place(self):
        name = pipeline_jobs.job_display_name("01_Meta_Extract")
        conn = FakeConnector(existing={name: [(7, [f"{WS}/01_Meta_Extract"])]})
        out = pipeline_jobs.ensure_pipeline_jobs(conn, WS, ["01_Meta_Extract"])
        self.assertEqual(out["jobs"], {"01_Meta_Extract": 7})
        self.assertEqual(conn.reset[0][0], 7)
        self.assertEqual(conn.created, [])

    def test_name_owned_by_other_deployment_gets_distinguishing_suffix(self):
        name = pipeline_jobs.job_display_name("01_Meta_Extract")
        conn = FakeConnector(existing={name: [(7, ["/Shared/Other/01_Meta_Extract"])]})
        pipeline_jobs.ensure_pipeline_jobs(conn, WS, ["01_Meta_Extract"])
        self.assertEqual(conn.created[0]["name"], f"{name} [{WS}]")

    def test_retries_without_environment_spec_when_rejected(self):
        conn = FakeConnector(reject_env=True)
        out = pipeline_jobs.ensure_pipeline_jobs(conn, WS, ["01_Meta_Extract"])
        self.assertIn("01_Meta_Extract", out["jobs"])
        self.assertNotIn("environments", conn.created[0])
        self.assertNotIn("environment_key", conn.created[0]["tasks"][0])

    def test_dlt_mode_registers_only_deployed_stage_notebooks(self):
        conn = FakeConnector()
        out = pipeline_jobs.ensure_pipeline_jobs(conn, WS, [
            "_Meta_CommonFunctions", "01_Meta_Extract", "02_Meta_SDP_Pipeline",
            "00_Meta_Orchestrator", "04_Meta_Reconciliation", "03_Meta_Validate", "05_Meta_ExecutionLog"])
        self.assertEqual(set(out["jobs"]), {"01_Meta_Extract", "04_Meta_Reconciliation", "00_Meta_Orchestrator"})


class TestConnectorRunNotebook(unittest.TestCase):
    def _connector(self):
        from databricks_connector import DatabricksConnector
        c = DatabricksConnector("https://example.cloud.databricks.com", "dapi-test-token-123")
        c._sess = MagicMock()
        return c

    def test_prefers_persistent_job(self):
        c = self._connector()
        c._api = MagicMock(return_value={"run_id": 55})
        r = c.run_notebook(f"{WS}/01_Meta_Extract", {"job_id": "j1", "x": None}, job_id=9)
        method, path = c._api.call_args[0]
        self.assertEqual(path, "/api/2.1/jobs/run-now")
        self.assertEqual(c._api.call_args[1]["json"], {"job_id": 9, "notebook_params": {"job_id": "j1", "x": ""}})
        self.assertEqual(r["run_id"], 55)
        self.assertIn("/#job/9/run/55", r["run_url"])

    def test_falls_back_to_one_time_run_with_standard_name(self):
        c = self._connector()
        c._api = MagicMock(side_effect=[{"_http_error": "HTTP 400: RESOURCE_DOES_NOT_EXIST"}, {"run_id": 56}])
        r = c.run_notebook(f"{WS}/01_Meta_Extract", {}, job_id=9)
        submit = c._api.call_args_list[1]
        self.assertEqual(submit[0][1], "/api/2.1/jobs/runs/submit")
        self.assertEqual(submit[1]["json"]["run_name"], "Migration Studio - 01 Extract (Source to Landing)")
        self.assertEqual(r["run_id"], 56)


class _Resp:
    def __init__(self, status, body):
        self.status_code, self._body, self.ok = status, body, 200 <= status < 300

    def json(self):
        return self._body

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError(f"HTTP {self.status_code}")


class TestGeneratedChildRunner(unittest.TestCase):
    """Exec the generated _Meta_CommonFunctions code with fake notebook globals."""

    def _load(self, routes, host="https://ws.example.com", jobs=None):
        calls = []

        def _handler(method):
            def _f(url, **kw):
                path = url.split("example.com", 1)[1]
                calls.append((method, path, kw.get("json") or kw.get("params")))
                resp = routes[(method, path)]
                return resp.pop(0) if isinstance(resp, list) else resp
            return _f

        fake_requests = types.SimpleNamespace(post=_handler("POST"), get=_handler("GET"))
        sys.modules["requests"] = fake_requests
        self.addCleanup(sys.modules.pop, "requests", None)

        dbutils = MagicMock()
        dbutils.notebook.run.return_value = '{"status": "OK", "inprocess": true}'
        g = {"dbutils": dbutils, "spark": MagicMock(), "__name__": "nb"}
        exec(compile(_gen_common_functions("ts"), "_Meta_CommonFunctions", "exec"), g)
        g["_CHILD_RUNNER"].update({"host": host, "headers": {}, "jobs": jobs or {}})
        import time
        orig = time.sleep
        time.sleep = lambda s: None
        self.addCleanup(setattr, time, "sleep", orig)
        return g, calls, dbutils

    def _terminal(self, result_state, task_run=901):
        return _Resp(200, {"state": {"life_cycle_state": "TERMINATED", "result_state": result_state,
                                     "state_message": "boom" if result_state != "SUCCESS" else ""},
                           "tasks": [{"run_id": task_run}], "run_page_url": "https://ws/run/1"})

    def test_run_now_success_returns_exit_value(self):
        g, calls, dbutils = self._load({
            ("POST", "/api/2.1/jobs/run-now"): _Resp(200, {"run_id": 900}),
            ("GET", "/api/2.1/jobs/runs/get"): [_Resp(200, {"state": {"life_cycle_state": "RUNNING"}}),
                                               self._terminal("SUCCESS")],
            ("GET", "/api/2.1/jobs/runs/get-output"): _Resp(200, {"notebook_output": {"result": '{"status":"OK","rows":5}'}}),
        }, jobs={"01_Meta_Extract": 42})
        out = g["_run_child_notebook"](f"{WS}/01_Meta_Extract", 3600, {"job_id": "a"}, label="HR.Emp")
        self.assertEqual(json.loads(out)["rows"], 5)
        self.assertEqual(calls[0][2], {"job_id": 42, "notebook_params": {"job_id": "a", "source_table": "HR.Emp"}})
        self.assertEqual(calls[-1][2], {"run_id": 901})  # output read from the task run
        dbutils.notebook.run.assert_not_called()

    def test_without_stage_job_submits_one_time_isolated_run(self):
        g, calls, _ = self._load({
            ("POST", "/api/2.1/jobs/runs/submit"): _Resp(200, {"run_id": 900}),
            ("GET", "/api/2.1/jobs/runs/get"): self._terminal("SUCCESS"),
            ("GET", "/api/2.1/jobs/runs/get-output"): _Resp(200, {"notebook_output": {"result": "x"}}),
        })
        self.assertEqual(g["_run_child_notebook"](f"{WS}/02_Meta_Bronze", 3600, {}, label="HR.Emp"), "x")
        body = calls[0][2]
        self.assertEqual(body["run_name"], "Migration Studio - 02_Meta_Bronze - HR.Emp")
        self.assertEqual(body["tasks"][0]["environment_key"], "Default")

    def test_failed_child_raises_like_notebook_run(self):
        g, _, _ = self._load({
            ("POST", "/api/2.1/jobs/run-now"): _Resp(200, {"run_id": 900}),
            ("GET", "/api/2.1/jobs/runs/get"): self._terminal("FAILED"),
            ("GET", "/api/2.1/jobs/runs/get-output"): _Resp(200, {"error": "UNSUPPORTED_DATA_SOURCE", "error_trace": "trace"}),
        }, jobs={"01_Meta_Extract": 42})
        with self.assertRaises(RuntimeError) as ctx:
            g["_run_child_notebook"](f"{WS}/01_Meta_Extract", 3600, {})
        self.assertIn("UNSUPPORTED_DATA_SOURCE", str(ctx.exception))
        self.assertIn("Caused by:", str(ctx.exception))

    def test_falls_back_in_process_when_jobs_api_unavailable(self):
        g, calls, dbutils = self._load({}, host=None)
        out = g["_run_child_notebook"](f"{WS}/01_Meta_Extract", 3600, {"a": 1})
        self.assertIn("inprocess", out)
        dbutils.notebook.run.assert_called_once_with(f"{WS}/01_Meta_Extract", 3600, {"a": 1})
        self.assertEqual(calls, [])

    def test_oversized_parameters_run_in_process(self):
        g, calls, dbutils = self._load({}, jobs={"01_Meta_Extract": 42})
        g["_run_child_notebook"](f"{WS}/01_Meta_Extract", 3600, {"blob": "x" * 12000})
        dbutils.notebook.run.assert_called_once()
        self.assertEqual(calls, [])

    def test_timeout_cancels_child_run(self):
        g, calls, _ = self._load({
            ("POST", "/api/2.1/jobs/run-now"): _Resp(200, {"run_id": 900}),
            ("GET", "/api/2.1/jobs/runs/get"): _Resp(200, {"state": {"life_cycle_state": "RUNNING"}}),
            ("POST", "/api/2.1/jobs/runs/cancel"): _Resp(200, {}),
        }, jobs={"01_Meta_Extract": 42})
        with self.assertRaises(TimeoutError):
            g["_run_child_notebook"](f"{WS}/01_Meta_Extract", -10000, {})
        self.assertIn(("POST", "/api/2.1/jobs/runs/cancel", {"run_id": 900}), calls)


class TestOrchestratorsUseIsolatedRuns(unittest.TestCase):
    def _nb(self, mode, name):
        r = generate_metadata_notebooks("meta", "cfg", "/Volumes/x/y/landing", WS, mode)
        return next(n["code"] for n in r["notebooks"] if n["name"] == name)

    def _live_notebook_run_lines(self, code):
        return [l for l in code.split("\n") if "dbutils.notebook.run(" in l and not l.strip().startswith("#")]

    def test_standard_orchestrator_has_no_in_process_stage_runs(self):
        code = self._nb("standard", "00_Meta_Orchestrator")
        self.assertEqual(self._live_notebook_run_lines(code), [])
        self.assertIn("%run ./_Meta_CommonFunctions", code)
        self.assertIn('dbutils.widgets.text("child_jobs"', code)

    def test_dlt_orchestrator_keeps_only_execution_log_in_process(self):
        code = self._nb("dlt", "00_Meta_Orchestrator")
        lines = self._live_notebook_run_lines(code)
        self.assertEqual(len(lines), 1)
        self.assertIn("log_nb", lines[0])
        self.assertEqual(code.count("_run_child_notebook("), 2)


if __name__ == "__main__":
    unittest.main()
