"""Snowflake connector — test connection, load SQL objects (procedures, views, UDFs).

This module mirrors the SQL Server flow in source.py but targets Snowflake.
It uses the snowflake-connector-python library for direct connections,
and falls back to JDBC via SQL Warehouse when the library is unavailable.
"""
import threading
from log_config import get_logger

logger = get_logger(__name__)


def get_snowflake_connection(account: str, username: str, password: str,
                             database: str = "", warehouse: str = "",
                             role: str = "", timeout: int = 30):
    """Return a Snowflake connection using snowflake-connector-python."""
    import snowflake.connector

    conn_params = {
        "account": account,
        "user": username,
        "password": password,
        "login_timeout": timeout,
        "network_timeout": timeout,
    }
    if database:
        conn_params["database"] = database
    if warehouse:
        conn_params["warehouse"] = warehouse
    if role:
        conn_params["role"] = role

    conn = snowflake.connector.connect(**conn_params)
    return conn


def test_connection(account: str, username: str, password: str,
                    database: str = "", warehouse: str = "",
                    role: str = "") -> dict:
    """Test connectivity to Snowflake and return version info."""
    try:
        conn = get_snowflake_connection(
            account=account, username=username, password=password,
            database=database, warehouse=warehouse, role=role,
            timeout=20
        )
        try:
            cur = conn.cursor()
            cur.execute("SELECT CURRENT_VERSION(), CURRENT_ACCOUNT(), CURRENT_DATABASE()")
            row = cur.fetchone()
            version = row[0] if row else "Connected"
            acct = row[1] if row and len(row) > 1 else account
            db = row[2] if row and len(row) > 2 else database
            return {
                "success": True,
                "server_version": f"Snowflake v{version}",
                "account": acct,
                "database": db,
                "method": "snowflake_connector",
            }
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as e:
        msg = str(e)
        hint = ""
        low = msg.lower()
        if "incorrect username or password" in low or "250001" in low:
            hint = " — Username/password rejected by Snowflake."
        elif "could not connect" in low or "connection refused" in low:
            hint = " — Cannot reach Snowflake account. Check account identifier."
        elif "account" in low and "not found" in low:
            hint = " — Account identifier not found. Use format: <orgname>-<account_name>."
        elif "timeout" in low:
            hint = " — Connection timed out. Check network/firewall."
        logger.error("Snowflake connection test failed: %s", msg)
        return {"success": False, "error": msg + hint}


def test_connection_via_warehouse(account: str, database: str,
                                  username: str, password: str,
                                  warehouse: str = "", role: str = "") -> dict:
    """Test Snowflake connection through Databricks SQL Warehouse using JDBC."""
    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        warehouses = list(ws.warehouses.list())
        running = [w for w in warehouses
                   if "RUNNING" in (w.state.value if hasattr(w.state, 'value') else str(w.state)).upper()]
        wh = running[0] if running else (warehouses[0] if warehouses else None)
        if not wh:
            return {"success": False, "error": "No SQL Warehouse available to test JDBC connection."}

        jdbc_url = f"jdbc:snowflake://{account}.snowflakecomputing.com/"
        jdbc_props = f"user={username};password={password}"
        if database:
            jdbc_props += f";db={database}"
        if warehouse:
            jdbc_props += f";warehouse={warehouse}"
        if role:
            jdbc_props += f";role={role}"

        test_sql = f"""
            SELECT * FROM jdbc(
                url => '{jdbc_url}',
                user => '{username}',
                password => '{password}',
                dbtable => '(SELECT CURRENT_VERSION() AS ver) t'
            ) LIMIT 1
        """

        stmt = ws.statement_execution.execute_statement(
            warehouse_id=wh.id,
            statement=test_sql,
            wait_timeout="60s",
        )
        state = str(stmt.status.state).upper() if stmt.status else ""
        if state == "SUCCEEDED":
            rows = stmt.result.data_array if stmt.result and stmt.result.data_array else []
            version = rows[0][0] if rows and rows[0] else "Connected via JDBC"
            return {
                "success": True,
                "server_version": f"Snowflake v{version}",
                "method": "jdbc_via_sql_warehouse",
                "warehouse": wh.name,
            }
        elif state == "FAILED":
            err = stmt.status.error.message if stmt.status.error else "Unknown error"
            return {"success": False, "error": f"JDBC test failed: {err}", "method": "jdbc_via_sql_warehouse"}
        else:
            return {"success": False, "error": f"Statement ended in state: {state}", "method": "jdbc_via_sql_warehouse"}
    except Exception as e:
        return {"success": False, "error": f"SQL Warehouse JDBC test failed: {str(e)[:300]}", "method": "jdbc_via_sql_warehouse"}


def load_objects(account: str, username: str, password: str,
                 database: str = "", warehouse: str = "",
                 role: str = "") -> dict:
    """Load stored procedures, views, and UDFs from Snowflake."""
    try:
        conn = get_snowflake_connection(
            account=account, username=username, password=password,
            database=database, warehouse=warehouse, role=role,
            timeout=30
        )
    except ImportError:
        return load_objects_via_warehouse(
            account=account, username=username, password=password,
            database=database, warehouse=warehouse, role=role
        )
    except Exception as e:
        logger.warning("Snowflake direct connection failed: %s", str(e)[:200])
        return load_objects_via_warehouse(
            account=account, username=username, password=password,
            database=database, warehouse=warehouse, role=role
        )

    grouped = {"stored_procedure": [], "view": [], "udf": []}
    try:
        cur = conn.cursor()

        # ── Stored Procedures ──
        cur.execute("""
            SELECT PROCEDURE_SCHEMA || '.' || PROCEDURE_NAME AS key,
                   PROCEDURE_NAME AS name,
                   COALESCE(PROCEDURE_DEFINITION, '') AS code
            FROM INFORMATION_SCHEMA.PROCEDURES
            WHERE PROCEDURE_CATALOG = CURRENT_DATABASE()
            ORDER BY PROCEDURE_NAME
        """)
        for row in cur.fetchall():
            grouped["stored_procedure"].append({
                "key": row[0], "name": row[1],
                "description": "Stored procedure", "code": row[2],
                "object_type": "stored_procedure"
            })

        # ── Views ──
        cur.execute("""
            SELECT TABLE_SCHEMA || '.' || TABLE_NAME AS key,
                   TABLE_NAME AS name,
                   COALESCE(VIEW_DEFINITION, '') AS code
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_CATALOG = CURRENT_DATABASE()
              AND TABLE_SCHEMA != 'INFORMATION_SCHEMA'
            ORDER BY TABLE_NAME
        """)
        for row in cur.fetchall():
            grouped["view"].append({
                "key": row[0], "name": row[1],
                "description": "SQL View", "code": row[2],
                "object_type": "view"
            })

        # ── User-Defined Functions ──
        cur.execute("""
            SELECT FUNCTION_SCHEMA || '.' || FUNCTION_NAME AS key,
                   FUNCTION_NAME AS name,
                   COALESCE(FUNCTION_DEFINITION, '') AS code
            FROM INFORMATION_SCHEMA.FUNCTIONS
            WHERE FUNCTION_CATALOG = CURRENT_DATABASE()
              AND FUNCTION_SCHEMA != 'INFORMATION_SCHEMA'
            ORDER BY FUNCTION_NAME
        """)
        for row in cur.fetchall():
            grouped["udf"].append({
                "key": row[0], "name": row[1],
                "description": "User-defined function", "code": row[2],
                "object_type": "udf"
            })

        conn.close()
        total = sum(len(v) for v in grouped.values())
        return {
            "success": True, "grouped": grouped, "total": total,
            "source_type": "snowflake", "database": database
        }
    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        logger.exception("Failed to load Snowflake objects")
        return {"success": False, "error": str(e)}


def load_objects_via_warehouse(account: str, username: str, password: str,
                               database: str = "", warehouse: str = "",
                               role: str = "") -> dict:
    """Load Snowflake objects via Databricks SQL Warehouse JDBC when no connector available."""
    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        warehouses = list(ws.warehouses.list())
        running = [w for w in warehouses
                   if "RUNNING" in (w.state.value if hasattr(w.state, 'value') else str(w.state)).upper()]
        wh = running[0] if running else (warehouses[0] if warehouses else None)
        if not wh:
            return {"success": False, "error": "No SQL Warehouse available"}

        jdbc_url = f"jdbc:snowflake://{account}.snowflakecomputing.com/?db={database}"
        if warehouse:
            jdbc_url += f"&warehouse={warehouse}"
        if role:
            jdbc_url += f"&role={role}"

        grouped = {"stored_procedure": [], "view": [], "udf": []}
        errors = []

        queries = {
            "stored_procedure": f"SELECT PROCEDURE_SCHEMA || '.' || PROCEDURE_NAME, PROCEDURE_NAME, COALESCE(PROCEDURE_DEFINITION, '') FROM {database}.INFORMATION_SCHEMA.PROCEDURES WHERE PROCEDURE_CATALOG = '{database}' ORDER BY PROCEDURE_NAME",
            "view": f"SELECT TABLE_SCHEMA || '.' || TABLE_NAME, TABLE_NAME, COALESCE(VIEW_DEFINITION, '') FROM {database}.INFORMATION_SCHEMA.VIEWS WHERE TABLE_CATALOG = '{database}' AND TABLE_SCHEMA != 'INFORMATION_SCHEMA' ORDER BY TABLE_NAME",
            "udf": f"SELECT FUNCTION_SCHEMA || '.' || FUNCTION_NAME, FUNCTION_NAME, COALESCE(FUNCTION_DEFINITION, '') FROM {database}.INFORMATION_SCHEMA.FUNCTIONS WHERE FUNCTION_CATALOG = '{database}' AND FUNCTION_SCHEMA != 'INFORMATION_SCHEMA' ORDER BY FUNCTION_NAME",
        }

        def _esc(val: str) -> str:
            return val.replace("\\", "\\\\").replace("'", "\\'")

        for obj_type, query in queries.items():
            sql = f"""SELECT * FROM jdbc(url => '{_esc(jdbc_url)}', user => '{_esc(username)}', password => '{_esc(password)}', dbtable => '({query}) t')"""
            try:
                stmt = ws.statement_execution.execute_statement(
                    warehouse_id=wh.id, statement=sql, wait_timeout="60s"
                )
                _st = (stmt.status.state.value if hasattr(stmt.status.state, 'value')
                       else str(stmt.status.state)).upper() if stmt.status and stmt.status.state else ""
                if "SUCCEEDED" in _st and stmt.result and stmt.result.data_array:
                    for row in stmt.result.data_array:
                        code_val = row[2] if (len(row) > 2 and row[2]) else ""
                        grouped[obj_type].append({
                            "key": row[0], "name": row[1],
                            "description": obj_type.replace("_", " ").title(),
                            "code": code_val,
                            "object_type": obj_type,
                        })
                elif "FAILED" in _st:
                    err_msg = stmt.status.error.message if stmt.status and stmt.status.error else ""
                    errors.append(f"{obj_type}: {err_msg or 'statement failed'}")
            except Exception as e:
                errors.append(f"{obj_type}: {str(e)[:200]}")
                logger.warning("Failed to load %s via JDBC: %s", obj_type, str(e)[:200])

        total = sum(len(v) for v in grouped.values())
        result = {
            "success": True, "grouped": grouped, "total": total,
            "source_type": "snowflake", "database": database,
            "method": "jdbc_via_sql_warehouse"
        }
        if errors:
            result["warnings"] = errors
        return result
    except Exception as e:
        logger.exception("Snowflake JDBC load failed")
        return {"success": False, "error": str(e)}
