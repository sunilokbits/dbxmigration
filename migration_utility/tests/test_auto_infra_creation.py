import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import AutoInfraCreation


class TestInfraOrchestration(unittest.TestCase):
    def test_create_folders_falls_back_to_an_existing_catalog_schema(self):
        cfg = {
            "storage_account": "sa",
            "container": "datalake",
            "metadata_catalog": "missing_admin",
            "metadata_schema": "configtables",
            "catalogs": {
                "dbx_bronze": {
                    "location": "abfss://datalake@sa.dfs.core.windows.net/dev/bronze",
                    "schemas": ["sales"],
                }
            },
        }

        def api_response(method, path, _cfg, payload=None):
            if method == "GET":
                return (path.endswith("dbx_bronze.sales"), {})
            return True, {}

        with patch.object(AutoInfraCreation, "_databricks_api", side_effect=api_response) as mock_api:
            AutoInfraCreation.create_folders(cfg)

        create_calls = [call for call in mock_api.call_args_list if call.args[0] == "POST"]
        self.assertTrue(create_calls)
        self.assertEqual(create_calls[0].args[3]["catalog_name"], "dbx_bronze")
        self.assertEqual(create_calls[0].args[3]["schema_name"], "sales")

    def test_create_folders_materializes_nested_paths_parent_first(self):
        cfg = {
            "storage_account": "sa",
            "container": "datalake",
            "folders": [],
            "metadata_catalog": "admin_source",
            "metadata_schema": "configtables",
            "catalogs": {
                "dbx_admin_source": {
                    "location": "abfss://datalake@sa.dfs.core.windows.net/dev/uc-managed/dbx_admin_source"
                }
            },
        }

        with patch.object(AutoInfraCreation, "_databricks_api", return_value=(True, {})) as mock_api:
            AutoInfraCreation.create_folders(cfg)

        created_paths = [
            call.args[3]["storage_location"]
            for call in mock_api.call_args_list
            if call.args[0] == "POST"
        ]
        self.assertEqual(created_paths, [
            "abfss://datalake@sa.dfs.core.windows.net/dev",
            "abfss://datalake@sa.dfs.core.windows.net/dev/uc-managed",
            "abfss://datalake@sa.dfs.core.windows.net/dev/uc-managed/dbx_admin_source",
        ])

    def test_directory_hierarchy_expands_full_paths_parent_first(self):
        self.assertEqual(
            AutoInfraCreation._directory_hierarchy([
                "dev/uc-managed/dbx_admin_source",
                "dev/uc-managed/dbx_bronze",
                "dev/landing",
            ]),
            [
                "dev",
                "dev/uc-managed",
                "dev/uc-managed/dbx_admin_source",
                "dev/uc-managed/dbx_bronze",
                "dev/landing",
            ],
        )

    def test_storage_folders_are_derived_when_explicit_list_is_empty(self):
        cfg = {
            "storage_account": "sa",
            "container": "datalake",
            "folders": [],
            "catalogs": {
                "dbx_bronze": {
                    "location": "abfss://datalake@sa.dfs.core.windows.net/dev/uc-managed/dbx_bronze"
                }
            },
            "reconciliation": {
                "location": "abfss://datalake@sa.dfs.core.windows.net/dev/reconciliation"
            },
            "logging": {
                "location": "abfss://datalake@sa.dfs.core.windows.net/dev/logging"
            },
            "volume_catalog": "dbx_volumes",
            "volume_path": "abfss://datalake@sa.dfs.core.windows.net/dev/dbx_landing",
        }

        self.assertEqual(
            AutoInfraCreation._configured_storage_folders(cfg),
            [
                "dev/uc-managed/dbx_bronze",
                "dev/reconciliation",
                "dev/logging",
                "dev/uc-managed/dbx_volumes",
                "dev/dbx_landing",
            ],
        )

    def test_create_volume_creates_missing_catalog_before_schema(self):
        cfg = {
            "storage_account": "sa",
            "container": "datalake",
            "volume_name": "dbx_landing",
            "volume_catalog": "dbx_volumes",
            "volume_schema": "sales",
            "volume_path": "abfss://datalake@sa.dfs.core.windows.net/dev/dbx_landing",
        }

        with patch.object(AutoInfraCreation, "_databricks_api", return_value=(True, {})) as mock_api:
            AutoInfraCreation.create_volume(cfg)

        calls = mock_api.call_args_list
        self.assertEqual(calls[0].args[1], "/api/2.1/unity-catalog/catalogs")
        self.assertEqual(calls[0].args[3]["name"], "dbx_volumes")
        self.assertEqual(
            calls[0].args[3]["storage_root"],
            "abfss://datalake@sa.dfs.core.windows.net/dev/uc-managed/dbx_volumes",
        )
        self.assertEqual(calls[1].args[1], "/api/2.1/unity-catalog/schemas")
        self.assertEqual(calls[2].args[1], "/api/2.1/unity-catalog/volumes")

    def test_create_volume_raises_when_catalog_creation_fails(self):
        cfg = {
            "storage_account": "sa",
            "container": "datalake",
            "volume_name": "dbx_landing",
            "volume_catalog": "dbx_volumes",
            "volume_schema": "sales",
            "volume_path": "abfss://datalake@sa.dfs.core.windows.net/dev/dbx_landing",
        }

        with patch.object(
            AutoInfraCreation,
            "_databricks_api",
            return_value=(False, {"error_code": "PERMISSION_DENIED"}),
        ):
            with self.assertRaises(RuntimeError):
                AutoInfraCreation.create_volume(cfg)

    def test_run_all_streaming_creates_catalogs_before_materializing_folders(self):
        cfg = {
            "infra_mode": "existing",
            "storage_account": "sa",
            "container": "datalake",
            "folders": ["dev/landing", "dev/reconciliation"],
            "volume_name": "landing",
            "volume_catalog": "dev_volumes",
            "volume_schema": "landing",
            "volume_path": "abfss://datalake@sa.dfs.core.windows.net/dev/landing",
            "catalogs": {"dev_raw": {"location": "abfss://datalake@sa.dfs.core.windows.net/dev/raw", "schemas": ["bronze"]}},
            "reconciliation": {"catalog": "reconciliation", "schema": "hr", "location": "abfss://datalake@sa.dfs.core.windows.net/dev/reconciliation"},
            "logging": {"catalog": "logging", "schema": "hr", "location": "abfss://datalake@sa.dfs.core.windows.net/dev/logging"},
            "databricks_host": "https://example.cloud.databricks.com",
            "databricks_token": "token",
        }

        with patch.object(AutoInfraCreation, "_azure_credentials_available", return_value=False), \
             patch.object(AutoInfraCreation, "_lookup_credential_connector_id", return_value=""), \
             patch.object(AutoInfraCreation, "_derive_connector_id", return_value=""), \
             patch.object(AutoInfraCreation, "create_folders") as mock_folders, \
             patch.object(AutoInfraCreation, "create_catalogs") as mock_catalogs, \
             patch.object(AutoInfraCreation, "create_volume") as mock_volume:
            steps = list(AutoInfraCreation.run_all_streaming(cfg))

        step_names = [step.get("name") for step in steps if step.get("event") == "step"]
        self.assertIn("Create Folder Paths", step_names)
        self.assertLess(step_names.index("Create Unity Catalogs"), step_names.index("Create Folder Paths"))
        self.assertLess(step_names.index("Create Folder Paths"), step_names.index("Create Volume"))
        mock_folders.assert_called_once_with(cfg)
        mock_catalogs.assert_called_once_with(cfg)
        mock_volume.assert_called_once_with(cfg)


if __name__ == "__main__":
    unittest.main()
