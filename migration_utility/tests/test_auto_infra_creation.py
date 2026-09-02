import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import AutoInfraCreation


class TestInfraOrchestration(unittest.TestCase):
    def test_run_all_streaming_materializes_folder_paths_before_catalogs(self):
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
        self.assertLess(step_names.index("Create Folder Paths"), step_names.index("Create Unity Catalogs"))
        mock_folders.assert_called_once_with(cfg)
        mock_catalogs.assert_called_once_with(cfg)
        mock_volume.assert_called_once_with(cfg)


if __name__ == "__main__":
    unittest.main()
