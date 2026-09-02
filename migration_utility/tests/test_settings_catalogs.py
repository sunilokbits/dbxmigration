"""Catalogs created by this app must always be selectable in Pipeline Studio."""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

APP_CONFIG = {
    "databricks_host": "https://example.cloud.databricks.com",
    "metadata_catalog": "dbx_admin_source",
    "metadata_schema": "sales",
    "catalogs": {
        "dbx_bronze": {"location": "abfss://c@sa.dfs.core.windows.net/dev/uc-managed/dbx_bronze"},
        "dbx_silver": {"location": "abfss://c@sa.dfs.core.windows.net/dev/uc-managed/dbx_silver"},
    },
    "reconciliation": {"catalog": "dbx_reconciliation", "schema": "sales"},
    "logging": {"catalog": "dbx_logging", "schema": "sales"},
    "volume_catalog": "dbx_volumes",
}


class TestListCatalogs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import app as flask_app
        cls.app = flask_app.app
        cls.app.config["TESTING"] = True

    def _client(self):
        client = self.app.test_client()
        with client.session_transaction() as sess:
            sess["role"] = "Admin"
            sess["user"] = "admin@test.com"
        return client

    def _uc_returning(self, catalog_rows):
        uc = MagicMock()
        uc.list_warehouses.return_value = {"warehouses": [{"id": "wh1", "state": "RUNNING"}]}
        uc._execute_statement.return_value = {
            "status": {"state": "SUCCEEDED"},
            "result": {"data_array": [[name] for name in catalog_rows]},
        }
        return uc

    @patch("routes.auth.get_current_user", return_value={"email": "a@t.com", "role": "Admin", "display_name": "A"})
    @patch("routes.settings.get_databricks_token", return_value="token")
    @patch("routes.settings.get_config", return_value=APP_CONFIG)
    def test_app_created_catalogs_are_merged_into_listing(self, _cfg, _tok, _user):
        with patch("unity_catalog_executor.UnityCatalogExecutor",
                   return_value=self._uc_returning(["main", "system", "az_lakebase-dbx"])):
            resp = self._client().get("/api/v1/settings/catalogs")

        data = resp.get_json()
        self.assertTrue(data["success"])
        self.assertNotIn("system", data["catalogs"])
        for expected in ("dbx_bronze", "dbx_silver", "dbx_admin_source",
                         "dbx_reconciliation", "dbx_logging", "dbx_volumes"):
            self.assertIn(expected, data["catalogs"])
        self.assertIn("main", data["catalogs"])
        self.assertEqual(data["catalogs"], sorted(set(data["catalogs"])))

    @patch("routes.auth.get_current_user", return_value={"email": "a@t.com", "role": "Admin", "display_name": "A"})
    @patch("routes.settings.get_databricks_token", return_value="token")
    @patch("routes.settings.get_config", return_value=APP_CONFIG)
    def test_configured_catalogs_survive_warehouse_failure(self, _cfg, _tok, _user):
        uc = MagicMock()
        uc.list_warehouses.return_value = {"warehouses": []}

        with patch("unity_catalog_executor.UnityCatalogExecutor", return_value=uc):
            resp = self._client().get("/api/v1/settings/catalogs")

        data = resp.get_json()
        self.assertTrue(data["success"])
        self.assertIn("dbx_bronze", data["catalogs"])
        self.assertIn("dbx_volumes", data["catalogs"])


if __name__ == "__main__":
    unittest.main()
