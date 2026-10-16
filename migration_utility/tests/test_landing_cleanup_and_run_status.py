"""Landing is transit-only (cleaned after Bronze loads it) and an orchestrator
run with any failed stage ends FAILED in the Jobs UI while the app still gets
per-stage detail from the result attached to the error."""

import json
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import workflow_manager as wfm
from metadata_notebooks import _gen_common_functions, generate_metadata_notebooks

WS = "/Shared/DBX/MetadataPipeline"


def _nb(mode, name):
    return next(n["code"] for n in generate_metadata_notebooks("m", "s", "/l", WS, mode)["notebooks"]
                if n["name"] == name)


class TestClearLanding(unittest.TestCase):
    def _common(self):
        spark = mock.MagicMock()
        g = {"spark": spark, "dbutils": mock.MagicMock(), "__name__": "nb"}
        exec(compile(_gen_common_functions("ts"), "common", "exec"), g)
        return g, spark

    def test_landing_dir_mirrors_extract_path_rule(self):
        g, _ = self._common()
        job = {"table_name": "PROMOTION",
               "target_config": json.dumps({"volumes_catalog": "dbx_volumes", "target_schema": "sales"})}
        self.assertEqual(g["_landing_dir_for"](job, "/x/landing"), "/Volumes/dbx_volumes/sales/landing/PROMOTION")
        self.assertEqual(g["_landing_dir_for"]({"table_name": "T"}, "/x/landing/"), "/x/landing/T")

    def test_clear_replaces_files_with_zero_row_schema_file(self):
        g, spark = self._common()
        job = {"table_name": "T", "target_config": {}}
        self.assertTrue(g["_clear_landing"](job, "/Volumes/c/s/landing"))
        spark.read.parquet.assert_called_with("/Volumes/c/s/landing/T")
        schema = spark.read.parquet.return_value.schema
        spark.createDataFrame.assert_called_with([], schema)
        spark.createDataFrame.return_value.write.mode.assert_called_with("overwrite")
        spark.createDataFrame.return_value.write.mode.return_value.parquet.assert_called_with("/Volumes/c/s/landing/T")

    def test_clear_never_raises(self):
        g, spark = self._common()
        spark.read.parquet.side_effect = RuntimeError("PATH_NOT_FOUND")
        self.assertFalse(g["_clear_landing"]({"table_name": "T"}, "/l"))


class TestNotebookWiring(unittest.TestCase):
    def test_sdp_bronze_is_never_reset_and_stays_defined_when_cleaned(self):
        code = _nb("dlt", "02_Meta_SDP_Pipeline")
        self.assertIn('"pipelines.reset.allowed": "false"', code)
        self.assertIn("if _check > 0 or spark.catalog.tableExists(_btbl):", code)

    def test_sdp_orchestrator_cleans_only_after_completed_update(self):
        code = _nb("dlt", "00_Meta_Orchestrator")
        a = code.index("Phase 6 — Clean Up Landing")
        block = code[a:code.index("Orchestration Summary", a)]
        self.assertIn('if dlt_status == "COMPLETED":', block)
        self.assertIn('extract_results[_idx].get("status") == "OK"', block)
        self.assertLess(code.index("Phase 4 — Run Reconciliation"), a)

    def test_standard_orchestrator_cleans_only_successful_groups(self):
        code = _nb("standard", "00_Meta_Orchestrator")
        self.assertIn("if group_ok and _extract_job and any(j.get(\"stage\") != \"extract\" for j in jobs):", code)


class TestOrchestratorFailsVisibly(unittest.TestCase):
    def _final_cell(self):
        code = _nb("dlt", "00_Meta_Orchestrator")
        start = code.index('_overall = "COMPLETED"')
        return code[start:code.index("dbutils.notebook.exit(exit_payload)", start)]

    def _run(self, **state):
        g = {"json": json, "extract_ok": 1, "extract_fail": 0, "dlt_status": "COMPLETED", "silver_relocated": 0,
             "silver_failed": 0, "silver_errors": [], "recon_status": "1 ok / 0 failed", "log_status": "COMPLETED",
             "pipeline_id": "p1", "total_rows": 10, "extract_results": [{"job": "ExtractTo_T", "status": "OK"}]}
        g.update(state)
        exec(compile(self._final_cell(), "summary", "exec"), g)
        return g

    def test_success_exits_normally(self):
        self.assertEqual(json.loads(self._run()["exit_payload"])["status"], "COMPLETED")

    def test_failed_extract_raises_with_attached_result(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._run(extract_ok=0, extract_fail=1, extract_results=[
                {"job": "ExtractTo_CATALOG_SALES", "status": "FAILED", "error": "Run timed out."}])
        msg = str(ctx.exception)
        self.assertIn("Pipeline run PARTIAL", msg)
        self.assertIn("ExtractTo_CATALOG_SALES: Run timed out.", msg)
        human, payload = wfm._split_orchestrator_error("[orchestrator] RuntimeError: " + msg + "\nTraceback ...")
        self.assertNotIn("MIGRATION_STUDIO_RESULT", human)
        self.assertEqual(json.loads(payload)["extract_failed"], 1)

    def test_split_ignores_ordinary_errors(self):
        self.assertEqual(wfm._split_orchestrator_error("boom"), ("boom", ""))


class TestPollerUsesAttachedResult(unittest.TestCase):
    def setUp(self):
        for d in (wfm.PIPELINE_GROUPS, wfm.JOB_REGISTRY, wfm.JOB_RUNS, wfm.DBR_RUN_INDEX):
            p = mock.patch.dict(d, {})
            p.start()
            self.addCleanup(p.stop)
        wfm.PIPELINE_GROUPS["g"] = {"group_id": "g", "job_ids": ["je", "jd"], "full_table": "S.T"}
        for jid, stage in (("je", "extract"), ("jd", "dlt_bronze_silver")):
            wfm.JOB_REGISTRY[jid] = {"job_id": jid, "stage": stage, "run_count": 0, "fail_count": 0}
            wfm.JOB_RUNS["r" + jid] = {"run_id": "r" + jid, "job_id": jid, "stage": stage, "status": "running",
                                       "started_at": "2026-10-16T10:00:00", "logs": []}
            wfm._index_run_by_dbr("r" + jid, 77)
        for name in ("_sync_run_to_dbr", "_sync_job_to_dbr", "_sync_pipeline_to_dbr"):
            p = mock.patch.object(wfm, name)
            p.start()
            self.addCleanup(p.stop)

    def _poll(self, payload, human):
        conn = mock.MagicMock()
        conn.get_run_status.return_value = {"success": True, "life_cycle": "TERMINATED", "result_state": "FAILED",
                                            "state_message": "Workload failed"}
        conn.get_run_output.return_value = {
            "success": True, "notebook_result": "", "tasks": [], "error_trace": "",
            "error": f"[orchestrator] RuntimeError: {human} MIGRATION_STUDIO_RESULT={json.dumps(payload)}\nTraceback"}
        with mock.patch("time.sleep"):
            wfm._poll_databricks_run(conn, 77, "g")
        return wfm.JOB_RUNS["rje"], wfm.JOB_RUNS["rjd"]

    def test_sdp_failure_keeps_successful_extract_green(self):
        ext, dlt = self._poll({"status": "PARTIAL", "extract_ok": 1, "extract_failed": 0, "dlt_status": "COMPLETED",
                               "silver_failed": 1, "pipeline_id": "p"},
                              "Pipeline run PARTIAL: extracts 1 ok / 0 failed, SDP COMPLETED, silver 1 failed")
        self.assertEqual(ext["status"], "success")
        self.assertEqual(dlt["status"], "failed")
        self.assertTrue(dlt["error"].startswith("Pipeline run PARTIAL"))
        self.assertNotIn("MIGRATION_STUDIO_RESULT", dlt["error"])
        self.assertEqual(wfm.PIPELINE_GROUPS["g"]["status"], "failed")

    def test_failed_extract_marks_extract_failed(self):
        ext, dlt = self._poll({"status": "PARTIAL", "extract_ok": 0, "extract_failed": 1, "dlt_status": "COMPLETED",
                               "silver_failed": 0, "pipeline_id": "p"},
                              "Pipeline run PARTIAL: extracts 0 ok / 1 failed. Failed: ExtractTo_T: Run timed out.")
        self.assertEqual(ext["status"], "failed")
        self.assertIn("Run timed out", ext["error"])


if __name__ == "__main__":
    unittest.main()
