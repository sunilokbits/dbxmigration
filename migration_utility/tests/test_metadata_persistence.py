"""A pipeline must never exist only in one gunicorn worker's memory.

Regression for REGION/NATION: created on a worker whose metadata connection
was never initialised, the saves were silently skipped, and the orchestrator
(which reads Delta only) "succeeded" with nothing extracted.
"""

import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import workflow_manager as wfm
from metadata_notebooks import generate_metadata_notebooks

TC = {"bronze_catalog": "dbx_bronze", "silver_catalog": "dbx_silver",
      "target_schema": "sales", "volumes_catalog": "dbx_volumes"}


class TestOnDemandMetadataInit(unittest.TestCase):
    def setUp(self):
        names = ("_dbr_host", "_dbr_token", "_dbr_catalog", "_dbr_schema", "_dbr_warehouse_id",
                 "_metadata_initialized", "_lazy_init_last_try")
        saved = {n: getattr(wfm, n) for n in names}
        self.addCleanup(lambda: [setattr(wfm, n, v) for n, v in saved.items()])
        for n in names[:-2]:
            setattr(wfm, n, None)
        wfm._metadata_initialized = False
        wfm._lazy_init_last_try = 0.0
        env = mock.patch.dict(os.environ, {"MIGRATION_STUDIO_NO_LAZY_METADATA_INIT": "0",
                                           "DATABRICKS_HOST": "adb-1.azuredatabricks.net",
                                           "DATABRICKS_SQL_WAREHOUSE_ID": "wh1"})
        env.start()
        self.addCleanup(env.stop)
        for name, value in (("_auto_hydrate_from_dbr", mock.MagicMock()),
                            ("_load_deploy_config", mock.MagicMock(return_value={}))):
            p = mock.patch.object(wfm, name, value)
            p.start()
            self.addCleanup(p.stop)

    def _cfg(self, cfg):
        p = mock.patch("config_cache.get_config", return_value=cfg)
        p.start()
        self.addCleanup(p.stop)

    def test_uninitialised_worker_restores_connection_from_durable_config(self):
        self._cfg({"metadata_catalog": "dbx_admin_source", "metadata_schema": "configtables"})
        with mock.patch.object(wfm, "_resolve_databricks_token", return_value="tok"):
            self.assertTrue(wfm._ensure_metadata_ready())
        self.assertEqual((wfm._dbr_catalog, wfm._dbr_schema, wfm._dbr_warehouse_id),
                         ("dbx_admin_source", "configtables", "wh1"))
        self.assertEqual(wfm._dbr_host, "https://adb-1.azuredatabricks.net")
        self.assertTrue(wfm._metadata_initialized)

    def test_missing_catalog_stays_not_ready_and_retries_are_throttled(self):
        self._cfg({})
        with mock.patch.object(wfm, "_resolve_databricks_token", return_value="tok") as tok:
            self.assertFalse(wfm._ensure_metadata_ready())
            self.assertFalse(wfm._ensure_metadata_ready())
            self.assertEqual(tok.call_count, 1)

    def test_save_reports_failure_when_connection_unavailable(self):
        self._cfg({})
        with mock.patch.object(wfm, "_resolve_databricks_token", return_value=""):
            self.assertFalse(wfm._sync_pipeline_to_dbr({"group_id": "g"}))
            self.assertFalse(wfm._sync_job_to_dbr({"job_id": "j"}))


class TestCreatePipelineFailsLoudly(unittest.TestCase):
    def _create(self, pipe_ok, job_ok=True, table="REGION"):
        with mock.patch.object(wfm, "_sync_pipeline_to_dbr", return_value=pipe_ok), \
             mock.patch.object(wfm, "_sync_job_to_dbr", return_value=job_ok), \
             mock.patch.object(wfm, "_delete_pipeline_from_dbr") as dp, \
             mock.patch.object(wfm, "_delete_job_from_dbr") as dj:
            r = wfm.create_pipeline_for_table(table_schema="TPCH_SF1", table_name=table, load_type="full",
                                              target_config=dict(TC), pipeline_mode="dlt")
        return r, dp, dj

    def test_unsaved_pipeline_is_rejected_and_rolled_back(self):
        r, dp, dj = self._create(pipe_ok=False)
        self.assertFalse(r["success"])
        self.assertIn("NOT saved to the Databricks metadata tables", r["error"])
        self.assertNotIn(r["group_id"], wfm.PIPELINE_GROUPS)
        self.assertFalse(any(j.get("group_id") == r["group_id"] for j in wfm.JOB_REGISTRY.values()))
        dp.assert_called_once_with(r["group_id"])
        self.assertGreaterEqual(dj.call_count, 1)

    def test_one_unsaved_job_also_fails_the_create(self):
        r, _, _ = self._create(pipe_ok=True, job_ok=False)
        self.assertFalse(r["success"])

    def test_saved_pipeline_succeeds(self):
        r, dp, _ = self._create(pipe_ok=True)
        self.addCleanup(wfm.PIPELINE_GROUPS.pop, r["group_id"], None)
        self.assertTrue(r["success"])
        self.assertIn(r["group_id"], wfm.PIPELINE_GROUPS)
        dp.assert_not_called()

    def test_bulk_reports_failed_tables_without_crashing(self):
        results = iter([
            {"success": True, "group": {"group_id": "g1"}, "jobs": [1, 2], "archived_jobs": []},
            {"success": False, "group_id": "g2", "error": "Pipeline for TPCH_SF10.NATION was NOT saved"},
        ])
        with mock.patch.object(wfm, "create_pipeline_for_table", side_effect=lambda **kw: next(results)):
            out = wfm.create_pipelines_bulk([{"schema": "TPCH_SF1", "table": "REGION"},
                                             {"schema": "TPCH_SF10", "table": "NATION"}], target_config=dict(TC))
        self.assertFalse(out["success"])
        self.assertEqual((out["created"], out["failed"], out["total_jobs"]), (1, 1, 2))
        self.assertIn("NATION was NOT saved", out["error"])


class TestGroupDisplayName(unittest.TestCase):
    def test_uses_memory_then_delta_never_the_group_id(self):
        with mock.patch.dict(wfm.PIPELINE_GROUPS, {"g1": {"full_table": "TPCH_SF1.REGION"}}):
            self.assertEqual(wfm._group_display_name("g1"), "TPCH_SF1.REGION")
        with mock.patch.object(wfm, "_ensure_metadata_ready", return_value=True), \
             mock.patch.object(wfm, "_exec_sql", return_value={}), \
             mock.patch.object(wfm, "_rows_from_exec", return_value=[{"full_table": "TPCDS_SF100TCL.WAREHOUSE"}]):
            self.assertEqual(wfm._group_display_name("9170f9e89d56"), "TPCDS_SF100TCL.WAREHOUSE")

    def test_falls_back_to_group_id_and_is_ascii(self):
        with mock.patch.object(wfm, "_ensure_metadata_ready", return_value=False):
            self.assertEqual(wfm._group_display_name("abc123"), "abc123")
        with mock.patch.dict(wfm.PIPELINE_GROUPS, {"g2": {"full_table": "HR.Emplöyee"}}):
            self.assertTrue(wfm._group_display_name("g2").isascii())


class TestOrchestratorsFailWhenNothingToRun(unittest.TestCase):
    def _nb(self, mode):
        r = generate_metadata_notebooks("m", "s", "/l", "/Shared/DBX/MetadataPipeline", mode)
        return next(n["code"] for n in r["notebooks"] if n["name"] == "00_Meta_Orchestrator")

    def test_sdp_orchestrator_raises_when_no_extract_jobs(self):
        code = self._nb("dlt")
        self.assertIn("if not extract_jobs:\n    raise RuntimeError(", code)
        self.assertLess(code.index("if not extract_jobs:"), code.index("Phase 1 — Run Extract Notebooks"))

    def test_standard_orchestrator_raises_when_group_or_jobs_missing(self):
        code = self._nb("standard")
        self.assertIn("if not groups:\n    raise RuntimeError(", code)
        self.assertIn('"error": f"No enabled jobs found in metadata for group {gid}"', code)
        self.assertIn('all(str(r.get("error", "")).startswith("No enabled jobs found") for r in results)', code)


if __name__ == "__main__":
    unittest.main()
