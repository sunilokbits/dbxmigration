"""The Reconciliation Report reads the table the reconciliation notebook writes:
{metadata_catalog}.reconciliation.reconcilationdetails."""

import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import workflow_manager as wfm
from metadata_notebooks import generate_metadata_notebooks

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestReconLocation(unittest.TestCase):
    def test_app_table_is_metadata_catalog_reconciliation_schema(self):
        with mock.patch("config_cache.get_config",
                        return_value={"metadata_catalog": "dbx_admin_source", "metadata_schema": "configtables"}):
            self.assertEqual(wfm._recon_fqn(), "`dbx_admin_source`.`reconciliation`.`reconcilationdetails`")

    def test_notebook_writes_to_the_same_place(self):
        for mode in ("dlt", "standard"):
            code = next(n["code"] for n in generate_metadata_notebooks("m", "s", "/l", "/W", mode)["notebooks"]
                        if n["name"] == "04_Meta_Reconciliation")
            self.assertIn("RECON_CATALOG= CATALOG", code)
            self.assertIn(f'RECON_SCHEMA = "{wfm.RECON_SCHEMA_NAME}"', code)
            self.assertIn(f'RECON_TABLE  = "{wfm.RECON_TABLE}"', code)

    def test_report_endpoints_and_label_use_the_reconciliation_schema(self):
        for rel in ("routes/workflow.py", "routes/schema.py"):
            src = open(os.path.join(HERE, rel), encoding="utf-8").read()
            self.assertIn("RECON_SCHEMA_NAME", src, rel)
            self.assertNotIn('recon_sch = cfg.get("metadata_schema"', src, rel)
        js = open(os.path.join(HERE, "static/reports.js"), encoding="utf-8").read()
        self.assertIn("'.reconciliation.reconcilationdetails'", js)


if __name__ == "__main__":
    unittest.main()
