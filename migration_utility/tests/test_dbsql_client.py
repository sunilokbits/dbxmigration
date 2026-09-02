"""Databricks SQL connections must be closed deterministically.

The connector's Connection.__del__ touches attributes that do not exist when
__init__ failed, which surfaces as an unraisable AttributeError and, during
interpreter shutdown on Linux CI, a segfault (exit 139).
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dbsql_client


class TestConnectionLifecycle(unittest.TestCase):
    def setUp(self):
        dbsql_client.close_all_connections()
        self.addCleanup(dbsql_client.close_all_connections)
        env = patch.dict(os.environ, {
            "DATABRICKS_SERVER_HOSTNAME": "example.cloud.databricks.com",
            "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/wh1",
        })
        env.start()
        self.addCleanup(env.stop)

    def test_close_all_connections_closes_open_connections(self):
        conn = MagicMock()
        with patch("databricks.sql.connect", return_value=conn), \
             patch("secrets_helper.get_databricks_token", return_value="token"):
            self.assertIs(dbsql_client.get_connection(), conn)

        dbsql_client.close_all_connections()

        conn.close.assert_called_once()
        self.assertIsNone(getattr(dbsql_client._local, "conn", None))

    def test_failed_connect_does_not_retain_a_broken_connection(self):
        with patch("databricks.sql.connect", side_effect=OSError("handshake failed")), \
             patch("secrets_helper.get_databricks_token", return_value="token"):
            with self.assertRaises(RuntimeError):
                dbsql_client.get_connection()

        self.assertIsNone(getattr(dbsql_client._local, "conn", None))

    def test_close_all_connections_is_safe_to_call_repeatedly(self):
        conn = MagicMock()
        conn.close.side_effect = RuntimeError("already closed")
        with patch("databricks.sql.connect", return_value=conn), \
             patch("secrets_helper.get_databricks_token", return_value="token"):
            dbsql_client.get_connection()

        dbsql_client.close_all_connections()
        dbsql_client.close_all_connections()


if __name__ == "__main__":
    unittest.main()
