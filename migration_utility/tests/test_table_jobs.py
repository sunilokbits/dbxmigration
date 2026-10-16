"""One Databricks Job (and, in SDP mode, one pipeline) per table: task graph,
table moves off the shared pipeline, run/poll wiring and stage notebooks that
fail their task instead of exiting green."""

import json
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pipeline_jobs as pj
import workflow_manager as wfm
from metadata_notebooks import generate_metadata_notebooks

WS = "/Shared/DBX/MetadataPipeline"


def _nbs(mode):
    return {n["name"]: n["code"] for n in generate_metadata_notebooks("m", "s", "/l", WS, mode)["notebooks"]}


def _settings(mode, **kw):
    stage_jobs = ({"extract": "je", "dlt_bronze_silver": "jd"} if mode == "dlt" else
                  {"extract": "je", "landing_to_bronze": "jb", "bronze_to_silver": "js"})
    return pj.build_table_job_settings(
        name="Migration Studio - Snowflake - S.T", group_id="g", full_table="S.T", workspace_path=WS,
        mode=mode, stage_jobs=stage_jobs, meta_catalog="m", meta_schema="s", landing_path="/l",
        pipeline_id="p-table" if mode == "dlt" else None, bronze_catalog="bz", target_schema="sales", **kw)


def _deps(settings):
    return {t["task_key"]: [d["task_key"] for d in t.get("depends_on", [])] for t in settings["tasks"]}


class TestTableJobSettings(unittest.TestCase):
    def test_names_include_source_and_table(self):
        self.assertEqual(pj.table_job_name("Snowflake", "TPCDS_SF100TCL.INCOME_BAND"),
                         "Migration Studio - Snowflake - TPCDS_SF100TCL.INCOME_BAND")
        self.assertEqual(pj.table_pipeline_name("Snowflake", "S.T"), "Migration Studio - Snowflake - S.T (Bronze & Silver)")

    def test_sdp_graph(self):
        s = _settings("dlt")
        self.assertEqual(_deps(s), {"extract": [], "bronze_silver": ["extract"], "dq_metrics": ["bronze_silver"],
                                    "reconciliation": ["bronze_silver"], "cleanup": ["bronze_silver"]})
        bs = next(t for t in s["tasks"] if t["task_key"] == "bronze_silver")
        self.assertEqual(bs["pipeline_task"], {"pipeline_id": "p-table", "full_refresh": False})
        self.assertEqual(s["max_concurrent_runs"], 1)
        self.assertEqual(s["tags"]["migration_studio_group"], "g")
        self.assertEqual([p["name"] for p in s["parameters"]], list(pj.TABLE_JOB_PARAMETERS))

    def test_standard_graph_and_run_ids(self):
        s = _settings("standard")
        self.assertEqual(_deps(s), {"extract": [], "bronze": ["extract"], "silver": ["bronze"],
                                    "reconciliation": ["bronze"], "cleanup": ["silver"]})
        p = {t["task_key"]: t["notebook_task"]["base_parameters"] for t in s["tasks"]}
        self.assertEqual(p["extract"]["job_id"], "je")
        self.assertEqual(p["extract"]["run_id"], "{{job.parameters.extract_run_id}}")
        self.assertEqual(p["bronze"]["run_id"], "{{job.parameters.bronze_run_id}}")
        self.assertEqual(p["silver"]["job_id"], "js")
        self.assertEqual(p["cleanup"]["job_id"], "je")

    def test_sdp_needs_a_pipeline(self):
        with self.assertRaises(ValueError):
            pj.build_table_job_settings(name="n", group_id="g", full_table="S.T", workspace_path=WS, mode="dlt",
                                        stage_jobs={"extract": "je"}, meta_catalog="m", meta_schema="s",
                                        landing_path="/l")

    def test_pipeline_spec_is_scoped_to_the_group(self):
        spec = pj.build_table_pipeline_spec(name="n", group_id="g", workspace_path=WS, meta_catalog="m",
                                            meta_schema="s", landing_path="/l", bronze_catalog="bz",
                                            silver_catalog="", target_schema="sales")
        self.assertEqual(spec["configuration"]["pipeline.scope"], "group")
        self.assertEqual(spec["configuration"]["pipeline.group_id"], "g")
        self.assertEqual(spec["configuration"]["pipeline.silver_catalog"], "bz")
        self.assertEqual(spec["libraries"], [{"notebook": {"path": f"{WS}/02_Meta_SDP_Pipeline"}}])

    def test_ensure_job_updates_in_place(self):
        conn = mock.MagicMock()
        conn.list_jobs_by_name.return_value = {"success": True, "jobs": [{"job_id": 9}]}
        conn.reset_job.return_value = {"success": True, "job_id": 9}
        r = pj.ensure_table_job(conn, _settings("standard"))
        self.assertEqual((r["job_id"], r["created"]), (9, False))
        conn.create_job.assert_not_called()


class TestNotebooks(unittest.TestCase):
    def test_sdp_pipeline_filters_to_its_group(self):
        code = _nbs("dlt")["02_Meta_SDP_Pipeline"]
        self.assertIn('if PIPELINE_SCOPE == "group" and _j.get("group_id") != GROUP_ID:', code)

    def test_manifests(self):
        self.assertTrue({"06_Meta_Cleanup", "07_Meta_SDP_DQMetrics"} <= set(_nbs("dlt")))
        std = _nbs("standard")
        self.assertIn("06_Meta_Cleanup", std)
        self.assertNotIn("07_Meta_SDP_DQMetrics", std)

    def test_stage_notebooks_fail_their_task(self):
        for mode, names in (("dlt", ("01_Meta_Extract", "04_Meta_Reconciliation")),
                            ("standard", ("01_Meta_Extract", "02_Meta_Bronze", "03_Meta_Silver"))):
            nbs = _nbs(mode)
            for n in names:
                self.assertNotIn('dbutils.notebook.exit(json.dumps({"status": "FAILED"', nbs[n], n)
                self.assertIn("_fail_task(", nbs[n], n)


class TestMoveTables(unittest.TestCase):
    def _run(self, owners, alter_ok=True):
        sqls = []

        def exec_sql(sql, *a, **k):
            sqls.append(sql)
            if sql.startswith("ALTER"):
                return {"status": {"state": "SUCCEEDED" if alter_ok else "FAILED",
                                   "error": {"message": "permission denied"}}}
            return {"sql": sql}

        def rows(res):
            fqn = res["sql"].split("SHOW TBLPROPERTIES ")[1].split(" (")[0]
            owner = owners.get(fqn)
            if owner is None:
                raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
            return [{"key": "pipelines.pipelineId", "value": owner}]

        conn = mock.MagicMock()
        conn.get_pipeline.return_value = {"success": True, "state": "IDLE"}
        with mock.patch.object(wfm, "_exec_sql", side_effect=exec_sql), \
             mock.patch.object(wfm, "_rows_from_exec", side_effect=rows):
            moved = wfm._move_tables_to_pipeline(conn, "1" * 36, [("bz.sales.t", "STREAMING TABLE"),
                                                                  ("sv.sales.t", "MATERIALIZED VIEW")])
        return moved, [s for s in sqls if s.startswith("ALTER")], conn

    def test_moves_tables_owned_by_the_shared_pipeline(self):
        shared = "a" * 36
        moved, alters, conn = self._run({"`bz`.`sales`.`t`": shared, "`sv`.`sales`.`t`": shared})
        self.assertEqual(moved, ["bz.sales.t", "sv.sales.t"])
        self.assertIn("ALTER STREAMING TABLE `bz`.`sales`.`t` SET TBLPROPERTIES ('pipelines.pipelineId' = '"
                      + "1" * 36 + "')", alters)
        self.assertTrue(alters[1].startswith("ALTER MATERIALIZED VIEW `sv`.`sales`.`t`"))
        conn.stop_pipeline.assert_not_called()  # shared pipeline was idle

    def test_already_owned_or_missing_tables_are_left_alone(self):
        moved, alters, _ = self._run({"`bz`.`sales`.`t`": "1" * 36})
        self.assertEqual((moved, alters), ([], []))

    def test_stops_a_running_owner_first(self):
        shared = "a" * 36
        sqls = {"`bz`.`sales`.`t`": shared}
        with mock.patch("time.sleep"):
            conn = mock.MagicMock()
            conn.get_pipeline.side_effect = [{"state": "RUNNING"}, {"state": "IDLE"}]
            with mock.patch.object(wfm, "_exec_sql", side_effect=lambda s, *a, **k:
                                   {"status": {"state": "SUCCEEDED"}} if s.startswith("ALTER") else {"sql": s}), \
                 mock.patch.object(wfm, "_rows_from_exec", side_effect=lambda r: (
                     [{"value": sqls[r["sql"].split("SHOW TBLPROPERTIES ")[1].split(" (")[0]]}]
                     if r["sql"].split("SHOW TBLPROPERTIES ")[1].split(" (")[0] in sqls
                     else (_ for _ in ()).throw(RuntimeError("missing")))):
                wfm._move_tables_to_pipeline(conn, "1" * 36, [("bz.sales.t", "STREAMING TABLE")])
        conn.stop_pipeline.assert_called_once_with(shared)

    def test_failed_move_is_a_clear_error(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._run({"`bz`.`sales`.`t`": "a" * 36}, alter_ok=False)
        self.assertIn("Could not move bz.sales.t", str(ctx.exception))
        self.assertIn("permission denied", str(ctx.exception))


class TestRunPaths(unittest.TestCase):
    def setUp(self):
        for d in (wfm.PIPELINE_GROUPS, wfm.JOB_REGISTRY, wfm.JOB_RUNS, wfm.DBR_RUN_INDEX):
            p = mock.patch.dict(d, {}, clear=True)
            p.start()
            self.addCleanup(p.stop)
        wfm.PIPELINE_GROUPS["g"] = {"group_id": "g", "full_table": "S.T", "table_name": "T",
                                    "source_config": {"source_type": "snowflake"},
                                    "job_ids": ["je", "jb", "js"], "target_config": {"bronze_catalog": "bz"}}
        for jid, stage in (("je", "extract"), ("jb", "landing_to_bronze"), ("js", "bronze_to_silver")):
            wfm.JOB_REGISTRY[jid] = {"job_id": jid, "job_name": jid, "stage": stage, "group_id": "g",
                                     "enabled": True, "run_count": 0, "fail_count": 0}
        for name in ("_sync_run_to_dbr", "_sync_job_to_dbr", "_sync_pipeline_to_dbr", "_spawn_worker"):
            p = mock.patch.object(wfm, name)
            p.start()
            self.addCleanup(p.stop)
        for name, val in (("_load_deploy_config", {}), ("_deployed_workspace_path", WS),
                          ("_resolve_source_password", ""), ("_resolve_databricks_token", "dapi-xxxxxxxxxx")):
            p = mock.patch.object(wfm, name, return_value=val)
            p.start()
            self.addCleanup(p.stop)
        # run_job passes no host: it comes from the app's configured workspace.
        p = mock.patch.object(wfm, "_dbr_host", "https://h")
        p.start()
        self.addCleanup(p.stop)
        self.conn = mock.MagicMock()
        self.conn.list_jobs_by_name.return_value = {"success": True, "jobs": [{"job_id": 9}]}
        self.conn.reset_job.return_value = {"success": True, "job_id": 9}
        self.conn.run_job_with_parameters.return_value = {"success": True, "run_id": 55, "run_url": "u"}
        p = mock.patch("databricks_connector.DatabricksConnector", return_value=self.conn)
        p.start()
        self.addCleanup(p.stop)

    def _run(self, group="g", **kw):
        return wfm.run_pipeline_on_databricks(group, host="https://h", token="dapi-xxxxxxxxxx",
                                              catalog="m", schema="s", **kw)

    def test_full_run_tracks_every_stage_with_the_job_run_ids(self):
        r = self._run()
        self.assertTrue(r["success"])
        _, params = self.conn.run_job_with_parameters.call_args[0][:2]
        self.assertIsNone(self.conn.run_job_with_parameters.call_args[1].get("only"))
        ids = r["local_run_ids"]
        self.assertEqual(ids, {"extract": params["extract_run_id"], "landing_to_bronze": params["bronze_run_id"],
                               "bronze_to_silver": params["silver_run_id"]})
        self.assertEqual({wfm.JOB_RUNS[i]["stage"] for i in ids.values()}, set(ids))
        self.assertTrue(all(wfm.JOB_RUNS[i]["dbr_run_id"] == 55 for i in ids.values()))

    def test_single_stage_runs_only_that_task(self):
        r = wfm.run_job("jb")
        self.assertTrue(r["success"])
        self.assertEqual(self.conn.run_job_with_parameters.call_args[1]["only"], ["bronze"])
        self.assertEqual([x["stage"] for x in wfm.JOB_RUNS.values()], ["landing_to_bronze"])
        self.assertEqual(r["run"]["stage"], "landing_to_bronze")

    def test_unknown_group_never_runs_everything(self):
        r = self._run(group="nope")
        self.assertFalse(r["success"])
        self.assertIn("not found", r["error"])
        self.conn.run_notebook.assert_not_called()
        self.conn.run_job_with_parameters.assert_not_called()

    def test_group_missing_a_stage_job_is_rejected(self):
        wfm.JOB_REGISTRY["js"]["enabled"] = False
        r = self._run()
        self.assertFalse(r["success"])
        self.assertIn("bronze_to_silver", r["error"])

    def test_delete_group_removes_job_but_keeps_pipeline(self):
        self.conn.list_jobs_by_name.return_value = {"success": True, "jobs": [{"job_id": 9}]}
        with mock.patch.object(wfm, "_delete_pipeline_from_dbr"), \
             mock.patch.object(wfm, "_resolve_databricks_token", return_value="t"), \
             mock.patch.object(wfm, "_dbr_host", "https://h"):
            wfm._delete_table_job(wfm.PIPELINE_GROUPS["g"])
        self.conn.list_jobs_by_name.assert_called_with("Migration Studio - Snowflake - S.T")
        self.conn.delete_job.assert_called_once_with(9)
        self.conn.delete_pipeline.assert_not_called()


class TestPollerReadsTableTasks(unittest.TestCase):
    def setUp(self):
        for d in (wfm.PIPELINE_GROUPS, wfm.JOB_REGISTRY, wfm.JOB_RUNS, wfm.DBR_RUN_INDEX):
            p = mock.patch.dict(d, {}, clear=True)
            p.start()
            self.addCleanup(p.stop)
        wfm.PIPELINE_GROUPS["g"] = {"group_id": "g", "job_ids": ["je", "jd"], "full_table": "S.T"}
        for jid, stage in (("je", "extract"), ("jd", "dlt_bronze_silver")):
            wfm.JOB_REGISTRY[jid] = {"job_id": jid, "stage": stage, "run_count": 0, "fail_count": 0}
            wfm.JOB_RUNS["r" + jid] = {"run_id": "r" + jid, "job_id": jid, "stage": stage, "status": "running",
                                       "started_at": "2026-10-16T10:00:00", "logs": []}
            wfm._index_run_by_dbr("r" + jid, 55)
        for name in ("_sync_run_to_dbr", "_sync_job_to_dbr", "_sync_pipeline_to_dbr"):
            p = mock.patch.object(wfm, name)
            p.start()
            self.addCleanup(p.stop)

    def _poll(self, tasks, run_state="FAILED"):
        conn = mock.MagicMock()
        conn.get_run_status.return_value = {"success": True, "life_cycle": "TERMINATED",
                                            "result_state": run_state, "state_message": ""}
        conn.get_run_output.return_value = {"success": True, "notebook_result": "", "error": "",
                                            "error_trace": "", "tasks": tasks}
        with mock.patch("time.sleep"):
            wfm._poll_databricks_run(conn, 55, "g")
        return wfm.JOB_RUNS["rje"], wfm.JOB_RUNS["rjd"]

    @staticmethod
    def _t(key, state, **kw):
        return dict({"task_key": key, "result_state": state, "life_cycle": "TERMINATED", "state_message": ""}, **kw)

    def test_each_stage_takes_its_own_task_result(self):
        ext, dlt = self._poll([self._t("extract", "SUCCESS", notebook_result=json.dumps({"rows": 42})),
                               self._t("bronze_silver", "FAILED", state_message="Update failed: schema mismatch")])
        self.assertEqual((ext["status"], ext["rows_processed"]), ("success", 42))
        self.assertEqual(dlt["status"], "failed")
        self.assertEqual(dlt["error"], "Update failed: schema mismatch")
        self.assertEqual(wfm.PIPELINE_GROUPS["g"]["status"], "failed")

    def test_failed_extract_reports_its_error_and_skips_downstream(self):
        ext, dlt = self._poll([self._t("extract", "FAILED", error="RuntimeError: extract failed: timeout"),
                               self._t("bronze_silver", "UPSTREAM_FAILED")])
        self.assertEqual(ext["error"], "RuntimeError: extract failed: timeout")
        self.assertEqual(dlt["status"], "failed")
        self.assertIn("UPSTREAM_FAILED", dlt["error"])

    def test_reconciliation_failure_does_not_fail_the_load(self):
        ext, dlt = self._poll([self._t("extract", "SUCCESS"), self._t("bronze_silver", "SUCCESS"),
                               self._t("reconciliation", "FAILED"), self._t("cleanup", "SUCCESS")])
        self.assertEqual((ext["status"], dlt["status"]), ("success", "success"))
        self.assertEqual(wfm.PIPELINE_GROUPS["g"]["status"], "success")
        self.assertTrue(any("reconciliation: FAILED" in l for l in dlt["logs"]))


if __name__ == "__main__":
    unittest.main()
