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
        out = g["_run_child_notebook"](f"{WS}/01_Meta_Extract", 3600, {"job_id": "a"},
                                       label="HR.Emp", target="dbx_bronze.hr.emp")
        self.assertEqual(json.loads(out)["rows"], 5)
        sent = calls[0][2]["notebook_params"]
        self.assertEqual(sent, {"table": "HR.Emp", "target_table": "dbx_bronze.hr.emp", "job_id": "a"})
        self.assertEqual(list(sent)[:2], ["table", "target_table"])  # Jobs UI shows the first one
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

    def test_target_label_matches_sdp_publish_names(self):
        g, _, _ = self._load({})
        job = {"table_name": "PARTSUPP", "full_table": "TPCH_SF1.PARTSUPP",
               "target_config": json.dumps({"bronze_catalog": "dbx_bronze", "target_schema": "sales"})}
        lbl = g["_target_label"](job, "dbx_admin_source", "configtables", "dlt",
                                 "dbx_bronze", "dbx_silver", "sales")
        self.assertEqual(lbl, "dbx_bronze.sales.partsupp -> dbx_silver.sales.partsupp")
        same_cat = g["_target_label"](job, "m", "s", "dlt", "dbx_bronze", "", "sales")
        self.assertEqual(same_cat, "dbx_bronze.sales.partsupp -> dbx_bronze.sales.silver_partsupp")
        from_config = g["_target_label"](job, "m", "s", "dlt")
        self.assertEqual(from_config, "dbx_bronze.sales.partsupp -> dbx_bronze.sales.silver_partsupp")

    def test_target_label_standard_uses_bronze_resolver_and_never_raises(self):
        g, _, _ = self._load({})
        job = {"table_name": "Emp", "target_config": {"bronze_catalog": "bronze", "volumes_catalog": "vol",
                                                      "target_schema": "hr"}}
        self.assertEqual(g["_target_label"](job, "m", "s", "standard"), "bronze.hr.Emp")
        self.assertEqual(g["_target_label"]({"target_config": "{not json"}, "m", "s", "dlt"), "")
        self.assertEqual(g["_target_label"]({}, "m", "s", "dlt"), "")

    def test_display_labels_are_ascii_for_jobs_api(self):
        g, calls, _ = self._load({
            ("POST", "/api/2.1/jobs/run-now"): _Resp(200, {"run_id": 900}),
            ("GET", "/api/2.1/jobs/runs/get"): self._terminal("SUCCESS"),
            ("GET", "/api/2.1/jobs/runs/get-output"): _Resp(200, {"notebook_output": {"result": "x"}}),
        }, jobs={"01_Meta_Extract": 42})
        g["_run_child_notebook"](f"{WS}/01_Meta_Extract", 3600, {}, label="HR.Emplöyee", target="a → b")
        sent = calls[0][2]["notebook_params"]
        self.assertTrue(all(v.isascii() for v in sent.values()), sent)
        self.assertEqual(sent["target_table"], "a ? b")

    def test_source_connection_matches_source_dialect(self):
        g, _, _ = self._load({})
        fmt, opts, qtbl, qcol = g["_source_connection"](
            {"source_type": "snowflake", "account": "QHWKPTI-XS87582", "username": "u",
             "database": "SNOWFLAKE_SAMPLE_DATA", "warehouse": "WH"}, "pw")
        self.assertEqual(fmt, "snowflake")
        self.assertEqual(opts["sfUrl"], "QHWKPTI-XS87582.snowflakecomputing.com")
        self.assertEqual((opts["sfDatabase"], opts["sfWarehouse"]), ("SNOWFLAKE_SAMPLE_DATA", "WH"))
        self.assertEqual((qtbl("S", "T"), qcol("sum_X")), ('"S"."T"', '"sum_X"'))
        fmt, opts, qtbl, qcol = g["_source_connection"](
            {"source_type": "azuresql", "server": "srv.database.windows.net,1433", "database": "db"}, "pw")
        self.assertEqual((fmt, opts["host"], opts["port"], opts["encrypt"]),
                         ("sqlserver", "srv.database.windows.net", "1433", "true"))
        self.assertEqual((qtbl("dbo", "T"), qcol("c")), ("[dbo].[T]", "[c]"))

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

    def test_sdp_pipeline_renamed_in_place_never_treated_as_stale(self):
        code = self._nb("dlt", "00_Meta_Orchestrator")
        self.assertIn('DLT_NAME = f"Migration Studio - 02 SDP Bronze & Silver ({DLT_CATALOG}.{DLT_SCHEMA})"', code)
        self.assertIn('LEGACY_DLT_NAME = f"MetadataPipeline_{_safe_name(DLT_CATALOG)}_{_safe_name(DLT_SCHEMA)}"', code)
        # The stale-pipeline cleanup deletes other pipelines on the same
        # catalog.schema; the legacy-named pipeline must be excluded or its
        # Bronze/Silver tables would be dropped.
        self.assertIn('if pname in _OUR_DLT_NAMES or (existing and pid == existing["pipeline_id"]):', code)
        self.assertNotIn('p.get("name") == DLT_NAME', code)


    def test_reconciliation_uses_native_source_connector(self):
        for mode in ("standard", "dlt"):
            code = self._nb(mode, "04_Meta_Reconciliation")
            self.assertIn("%run ./_Meta_CommonFunctions", code)
            self.assertIn("_source_connection(source_config, PASSWORD)", code)
            for legacy in ("spark.read.jdbc", "jdbc:sqlserver://", "com.microsoft.sqlserver.jdbc"):
                self.assertNotIn(legacy, code)
            self.assertIn("AS {_qcol('__row_count')}", code)

    def test_extract_shares_the_same_connector(self):
        code = self._nb("dlt", "01_Meta_Extract")
        self.assertIn("_SRC_FORMAT, _SRC_OPTIONS, _qtbl, _qcol = _source_connection(source_config, PASSWORD)", code)
        self.assertNotIn('"sfUrl"', code)

    def test_sdp_orchestrator_counts_failed_reconciliation(self):
        code = self._nb("dlt", "00_Meta_Orchestrator")
        self.assertIn('if str(_recon_res.get("status", "")).upper() in ("FAILED", "ERROR"):', code)


class TestAppRunParameters(unittest.TestCase):
    def test_orchestrator_run_lists_table_first(self):
        import workflow_manager as wfm
        conn = MagicMock()
        conn.run_notebook.return_value = {"success": False, "message": "stop here"}
        with unittest.mock.patch("databricks_connector.DatabricksConnector", return_value=conn), \
             unittest.mock.patch.dict(wfm.PIPELINE_GROUPS, {"g1": {"full_table": "TPCH_SF1.PARTSUPP",
                                                                  "target_config": {"bronze_catalog": "b"},
                                                                  "job_ids": []}}), \
             unittest.mock.patch.object(wfm, "_load_deploy_config", return_value={}):
            wfm.run_pipeline_on_databricks("g1", host="https://h", token="dapi-xxxxxxxxxx",
                                           catalog="c", schema="s", workspace_path=WS)
        params = conn.run_notebook.call_args[1]["params"]
        self.assertEqual(list(params)[0], "table")
        self.assertEqual(params["table"], "TPCH_SF1.PARTSUPP")


if __name__ == "__main__":
    unittest.main()
