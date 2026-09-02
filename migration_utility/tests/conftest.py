"""Unit tests must never open real Databricks SQL connections.

CI sets DATABRICKS_* secrets, so audit/persistence writes would otherwise dial
the workspace. A failed connect leaves a Connection whose __del__ raises, which
crashes the interpreter at shutdown (segfault, exit 139).

Only the SQL-warehouse settings are cleared: DATABRICKS_HOST/TOKEN stay, because
identity.py needs them to resolve the caller (without them every route 401s).
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dbsql_client

_SQL_WAREHOUSE_ENV = (
    "DATABRICKS_SERVER_HOSTNAME",
    "DATABRICKS_HTTP_PATH",
    "DATABRICKS_SQL_WAREHOUSE_ID",
)


@pytest.fixture(autouse=True, scope="session")
def _isolate_databricks_connections():
    saved = {name: os.environ.pop(name, None) for name in _SQL_WAREHOUSE_ENV}
    # Empty string short-circuits warehouse auto-discovery without a network
    # call, so get_connection raises before databricks.sql.connect is reached.
    dbsql_client._discovered_warehouse_id = ""
    try:
        yield
    finally:
        dbsql_client.close_all_connections()
        for name, value in saved.items():
            if value is not None:
                os.environ[name] = value
