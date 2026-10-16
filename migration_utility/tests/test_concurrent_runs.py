"""Concurrent pipeline-group runs sharing one SDP pipeline, multi-worker run
tracking, Reconciliation aggregate reading, and notebook workspace paths."""

import os
import sys
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import workflow_manager as wfm
from metadata_notebooks import generate_metadata_notebooks

WS = "/Shared/DBX/MetadataPipeline"


def _nb(mode, name):
    r = generate_metadata_notebooks("m", "s", "/l", WS, mode)
    return next(n["code"] for n in r["notebooks"] if n["name"] == name)


def _sdp_trigger_block():
    """The generated trigger section, rendered as plain Python."""
    code = _nb("dlt", "00_Meta_Orchestrator")
    start = code.index("_ACTIVE_UPDATE_STATES = ")
    end = code.index('print(f"📋 Update ID:', start)
    return code[start:end] + "result = (update_id, _joined_update)\n"


def _table_job_conn(run_result=None):
    """Connector mock for the per-table job path (no job/pipeline exists yet)."""
    conn = mock.MagicMock()
    conn.list_jobs_by_name.return_value = {"success": True, "jobs": []}
    conn.create_job.return_value = {"success": True, "job_id": 500}
    conn.find_pipeline_by_name.return_value = {"success": True, "pipeline_id": None}
    conn.create_pipeline.return_value = {"success": True, "pipeline_id": "p-table"}
    conn.run_job_with_parameters.return_value = run_result or {"success": True, "run_id": 77, "run_url": "u"}
    return conn


class _Resp:
    def __init__(self, status, body=None):
        self.status_code, self._body, self.ok = status, body or {}, 200 <= status < 300

    def json(self):
        return self._body

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError(f"HTTP {self.status_code}")


class TestSdpTriggerNeverStopsOtherRuns(unittest.TestCase):
    EXTRACT_DONE = 1_000_000

    def _run(self, updates_sequence, post_sequence, force_full=False):
        calls = []
        lists = iter(updates_sequence)

        def get(url, **kw):
            calls.append(("GET", url))
            return _Resp(200, {"updates": next(lists)})

        def post(url, **kw):
            calls.append(("POST", url))
            return post_sequence.pop(0)

        g = {"requests": types.SimpleNamespace(get=get, post=post),
             "time": types.SimpleNamespace(time=lambda: 0, sleep=lambda s: None),
             "HOST": "https://h", "pipeline_id": "p1", "_hdrs": {}, "_force_full": force_full,
             "_EXTRACT_DONE_MS": self.EXTRACT_DONE}
        exec(compile(_sdp_trigger_block(), "sdp_trigger", "exec"), g)
        self.assertFalse([c for c in calls if c[1].endswith("/stop")], "must never stop another update")
        return g["result"], calls

    def test_idle_pipeline_starts_own_update(self):
        (uid, joined), calls = self._run([[{"update_id": "old", "state": "COMPLETED", "creation_time": 1}]],
                                          [_Resp(200, {"update_id": "mine"})])
        self.assertEqual((uid, joined), ("mine", False))

    def test_joins_update_started_after_our_extracts(self):
        later = self.EXTRACT_DONE + 60_000
        (uid, joined), calls = self._run([[{"update_id": "theirs", "state": "RUNNING", "creation_time": later}]], [])
        self.assertEqual((uid, joined), ("theirs", True))
        self.assertFalse([c for c in calls if c[0] == "POST"])

    def test_waits_for_older_update_then_starts_own(self):
        earlier = self.EXTRACT_DONE - 60_000
        (uid, joined), _ = self._run(
            [[{"update_id": "theirs", "state": "RUNNING", "creation_time": earlier}],
             [{"update_id": "theirs", "state": "COMPLETED", "creation_time": earlier}]],
            [_Resp(200, {"update_id": "mine"})])
        self.assertEqual((uid, joined), ("mine", False))

    def test_409_race_rechecks_and_joins(self):
        later = self.EXTRACT_DONE + 60_000
        (uid, joined), _ = self._run(
            [[], [{"update_id": "theirs", "state": "SETTING_UP_TABLES", "creation_time": later}]],
            [_Resp(409)])
        self.assertEqual((uid, joined), ("theirs", True))

    def test_full_refresh_never_joins(self):
        later = self.EXTRACT_DONE + 60_000
        (uid, joined), _ = self._run(
            [[{"update_id": "theirs", "state": "RUNNING", "creation_time": later}], []],
            [_Resp(200, {"update_id": "mine"})], force_full=True)
        self.assertEqual((uid, joined), ("mine", False))

    def test_orchestrator_polls_its_own_update(self):
        code = _nb("dlt", "00_Meta_Orchestrator")
        self.assertNotIn("/stop", code)
        self.assertNotIn("latest_updates", code)
        self.assertIn('/api/2.0/pipelines/{pipeline_id}/updates/{update_id}", headers=_hdrs)', code)


class TestGroupLoadedFromDelta(unittest.TestCase):
    ROWS = {
        "wf_pipeline_metadata": [{"group_id": "9a48", "full_table": "TPCDS_SF100TCL.SHIP_MODE",
                                  "table_schema": "TPCDS_SF100TCL", "table_name": "SHIP_MODE",
                                  "status": "created", "target_config": '{"bronze_catalog": "dbx_bronze"}'}],
        "wf_job_metadata": [
            {"job_id": "j2", "group_id": "9a48", "stage": "dlt_bronze_silver", "job_order": "2",
             "full_table": "TPCDS_SF100TCL.SHIP_MODE", "enabled": "true"},
            {"job_id": "j1", "group_id": "9a48", "stage": "extract", "job_order": "1",
             "full_table": "TPCDS_SF100TCL.SHIP_MODE", "enabled": "true"}],
    }

    def setUp(self):
        for d in (wfm.PIPELINE_GROUPS, wfm.JOB_REGISTRY, wfm.JOB_RUNS):
            p = mock.patch.dict(d, {}, clear=False)
            p.start()
            self.addCleanup(p.stop)
        for k in ("9a48",):
            wfm.PIPELINE_GROUPS.pop(k, None)
        for k in ("j1", "j2"):
            wfm.JOB_REGISTRY.pop(k, None)
        rows = self.ROWS

        def rows_from(sql):
            if "wf_pipeline_metadata" in sql:
                return rows["wf_pipeline_metadata"]
            if "SELECT group_id" in sql:
                return [{"group_id": "9a48"}]
            return rows["wf_job_metadata"]

        for name, val in (("_ensure_metadata_ready", mock.MagicMock(return_value=True)),
                          ("_exec_sql", mock.MagicMock(side_effect=lambda sql, *a, **k: sql)),
                          ("_rows_from_exec", mock.MagicMock(side_effect=rows_from)),
                          ("_fqn", mock.MagicMock(side_effect=lambda t: t))):
            p = mock.patch.object(wfm, name, val)
            p.start()
            self.addCleanup(p.stop)

    def test_missing_group_is_loaded_with_its_jobs(self):
        grp = wfm._get_group("9a48")
        self.assertEqual(grp["full_table"], "TPCDS_SF100TCL.SHIP_MODE")
        self.assertEqual(sorted(grp["job_ids"]), ["j1", "j2"])
        self.assertEqual(grp["pipeline_mode"], "dlt")
        self.assertEqual(grp["target_config"], {"bronze_catalog": "dbx_bronze"})
        self.assertIn("9a48", wfm.PIPELINE_GROUPS)
        self.assertEqual(wfm.JOB_REGISTRY["j1"]["stage"], "extract")

    def test_missing_job_loads_its_group(self):
        job = wfm._get_job("j1")
        self.assertEqual(job["group_id"], "9a48")
        self.assertIn("9a48", wfm.PIPELINE_GROUPS)

    def test_run_pipeline_on_databricks_tracks_a_group_from_another_worker(self):
        conn = _table_job_conn()
        with mock.patch("databricks_connector.DatabricksConnector", return_value=conn), \
             mock.patch.object(wfm, "_load_deploy_config", return_value={}), \
             mock.patch.object(wfm, "_deployed_workspace_path", return_value=WS), \
             mock.patch.object(wfm, "_sync_run_to_dbr"), mock.patch.object(wfm, "_sync_job_to_dbr"), \
             mock.patch.object(wfm, "_resolve_source_password", return_value=""), \
             mock.patch.object(wfm, "_sync_pipeline_to_dbr"), mock.patch.object(wfm, "_spawn_worker") as spawn:
            r = wfm.run_pipeline_on_databricks("9a48", host="https://h", token="dapi-xxxxxxxxxx",
                                               catalog="c", schema="s")
        self.assertTrue(r["success"])
        tracked = [x for x in wfm.JOB_RUNS.values() if x.get("dbr_run_id") == 77]
        self.assertEqual(sorted(x["job_id"] for x in tracked), ["j1", "j2"])
        spawn.assert_called_once()
        conn.run_notebook.assert_not_called()  # no shared orchestrator
        self.assertEqual(conn.create_job.call_args[0][0]["name"],
                         "Migration Studio - SQL Server - TPCDS_SF100TCL.SHIP_MODE")
        jp = conn.run_job_with_parameters.call_args[0][1]
        self.assertEqual(conn.run_job_with_parameters.call_args[0][0], 500)
        self.assertEqual({x["run_id"] for x in tracked if x["stage"] == "extract"}, {jp["extract_run_id"]})


class TestReconciliationReadsAggregatesByPosition(unittest.TestCase):
    def test_no_alias_lookup(self):
        code = _nb("dlt", "04_Meta_Reconciliation")
        self.assertNotIn("src_row[", code)
        self.assertIn("_src_vals = list(_read_source(src_query).collect()[0])", code)
        self.assertIn("src_sums = {cn: _src_vals[i + 1] for i, (cn, _) in enumerate(numeric_cols)}", code)
        self.assertIn("src_val = src_sums[cn]", code)


class TestNotebookWorkspacePath(unittest.TestCase):
    def test_deploy_rejects_storage_style_path(self):
        r = wfm.deploy_metadata_notebooks(host="https://h", token="t", workspace_path="dev/uc-managed/bronze")
        self.assertFalse(r["success"])
        self.assertIn("must start with '/'", r["error"])

    def test_run_uses_deployed_path_not_the_request_box(self):
        conn = _table_job_conn({"success": False, "message": "stop"})
        jobs = {"e1": {"job_id": "e1", "job_name": "e1", "stage": "extract", "group_id": "g"},
                "b1": {"job_id": "b1", "job_name": "b1", "stage": "landing_to_bronze", "group_id": "g"},
                "s1": {"job_id": "s1", "job_name": "s1", "stage": "bronze_to_silver", "group_id": "g"}}
        with mock.patch("databricks_connector.DatabricksConnector", return_value=conn), \
             mock.patch.object(wfm, "_load_deploy_config", return_value={}), \
             mock.patch.object(wfm, "_deployed_workspace_path", return_value=WS), \
             mock.patch.object(wfm, "_sync_run_to_dbr"), mock.patch.object(wfm, "_sync_job_to_dbr"), \
             mock.patch.object(wfm, "_resolve_source_password", return_value=""), \
             mock.patch.dict(wfm.JOB_REGISTRY, jobs), \
             mock.patch.dict(wfm.PIPELINE_GROUPS, {"g": {"group_id": "g", "full_table": "A.B",
                                                         "table_name": "B", "job_ids": list(jobs),
                                                         "target_config": {"bronze_catalog": "b"}}}):
            wfm.run_pipeline_on_databricks("g", host="https://h", token="dapi-xxxxxxxxxx", catalog="c",
                                           schema="s", workspace_path="dev/uc-managed/bronze")
        paths = [t["notebook_task"]["notebook_path"] for t in conn.create_job.call_args[0][0]["tasks"]]
        self.assertEqual(paths[0], f"{WS}/01_Meta_Extract")
        self.assertTrue(all(p.startswith(WS + "/") for p in paths))

    def test_run_rejects_relative_path_when_nothing_deployed(self):
        with mock.patch.object(wfm, "_load_deploy_config", return_value={}), \
             mock.patch.object(wfm, "_deployed_workspace_path", return_value=""):
            r = wfm.run_pipeline_on_databricks("g", host="https://h", token="dapi-xxxxxxxxxx", catalog="c",
                                               schema="s", workspace_path="dev/uc-managed/bronze")
        self.assertFalse(r["success"])
        self.assertIn("must start with '/'", r["error"])


if __name__ == "__main__":
    unittest.main()
