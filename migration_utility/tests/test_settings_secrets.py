"""Tests for the /api/v1/settings/secrets endpoints (GET list, POST update).

Safety: _get_ws_client is patched at class level for EVERY test in this
file (not just the ones that call it directly), so no test here can ever
reach a real Databricks workspace, even if a more specific per-test mock
is wrong. Individual tests then layer get_secret/set_secret mocks on top
to control behavior precisely.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestSecretsSettingsRoutes(unittest.TestCase):
    """Admin-only, whitelisted, audited secret update endpoint."""

    @classmethod
    def setUpClass(cls):
        import app as flask_app
        cls.app = flask_app.app
        cls.app.config["TESTING"] = True

    def setUp(self):
        # Class-wide safety net: no test in this file can reach a real
        # workspace, regardless of what any individual test also patches.
        self._ws_patch = patch("secrets_helper._get_ws_client", return_value=None)
        self._ws_patch.start()
        self.addCleanup(self._ws_patch.stop)

    def _client_as(self, role):
        client = self.app.test_client()
        with client.session_transaction() as sess:
            sess["role"] = role
            sess["user"] = "test@test.com"
        return client

    @patch("routes.auth.get_current_user", return_value={"email": "viewer@test.com", "role": "Viewer", "display_name": "V"})
    def test_get_requires_admin(self, mock_user):
        client = self._client_as("Viewer")
        resp = client.get("/api/v1/settings/secrets")
        self.assertEqual(resp.status_code, 403)

    @patch("routes.auth.get_current_user", return_value={"email": "admin@test.com", "role": "Admin", "display_name": "A"})
    def test_get_lists_known_keys_without_values(self, mock_user):
        client = self._client_as("Admin")
        resp = client.get("/api/v1/settings/secrets")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["success"])
        keys = {k["key"] for k in data["keys"]}
        self.assertIn("databricks-token", keys)
        self.assertIn("source-sql-password", keys)
        self.assertEqual(len(keys), 10)
        self.assertNotIn("value", str(data))

    @patch("routes.auth.get_current_user", return_value={"email": "admin@test.com", "role": "Admin", "display_name": "A"})
    def test_post_rejects_unknown_key(self, mock_user):
        client = self._client_as("Admin")
        resp = client.post("/api/v1/settings/secrets", json={"key": "not-a-real-key", "value": "x"})
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(resp.get_json()["success"])

    @patch("routes.auth.get_current_user", return_value={"email": "admin@test.com", "role": "Admin", "display_name": "A"})
    def test_post_rejects_empty_value(self, mock_user):
        client = self._client_as("Admin")
        resp = client.post("/api/v1/settings/secrets", json={"key": "source-sql-password", "value": ""})
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(resp.get_json()["success"])

    @patch("routes.auth.get_current_user", return_value={"email": "admin@test.com", "role": "Admin", "display_name": "A"})
    def test_post_rejects_replace_me_placeholder(self, mock_user):
        client = self._client_as("Admin")
        resp = client.post("/api/v1/settings/secrets", json={"key": "source-sql-password", "value": "REPLACE_ME"})
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(resp.get_json()["success"])

    @patch("routes.auth.get_current_user", return_value={"email": "viewer@test.com", "role": "Viewer", "display_name": "V"})
    def test_post_rejects_non_admin(self, mock_user):
        client = self._client_as("Viewer")
        resp = client.post("/api/v1/settings/secrets", json={"key": "source-sql-password", "value": "real-pw"})
        self.assertEqual(resp.status_code, 403)

    @patch("routes.auth.get_current_user", return_value={"email": "admin@test.com", "role": "Admin", "display_name": "A"})
    @patch("audit.log_action")
    @patch("secrets_helper.set_secret", return_value=True)
    def test_post_updates_whitelisted_key(self, mock_set, mock_log, mock_user):
        client = self._client_as("Admin")
        resp = client.post("/api/v1/settings/secrets", json={"key": "source-sql-password", "value": "real-pw"})
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.get_json()["success"])
        mock_set.assert_called_once_with("source-sql-password", "real-pw")
        mock_log.assert_called_once()
        _, kwargs = mock_log.call_args
        self.assertEqual(kwargs.get("resource_id"), "source-sql-password")
        self.assertNotIn("real-pw", str(kwargs))


if __name__ == "__main__":
    unittest.main()
