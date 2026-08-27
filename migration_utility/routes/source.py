"""Source DB blueprint — test connection, load SQL objects."""
import os
import socket
import threading
from flask import Blueprint, request, jsonify

from .auth import login_required
from log_config import get_logger
from config_cache import get_source_password
from keyvault_helper import is_masked

logger = get_logger(__name__)
source_bp = Blueprint("source", __name__, url_prefix="/api/v1")


def _is_databricks_app() -> bool:
    return bool(os.environ.get("DATABRICKS_CLIENT_ID") and os.environ.get("DATABRICKS_CLIENT_SECRET"))


def _tcp_test(host: str, port: int, timeout: int = 10) -> tuple[bool, str]:
    """Quick TCP connectivity check — verifies server is reachable."""
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        return True, f"Server {host}:{port} is reachable"
    except socket.timeout:
        return False, f"Connection timed out — cannot reach {host}:{port}. Check firewall rules."
    except socket.gaierror:
        return False, f"DNS resolution failed for '{host}'. Check the server name."
    except OSError as e:
        return False, f"Cannot connect to {host}:{port}: {e}"


def _test_via_sql_warehouse(server: str, database: str, username: str, password: str) -> dict:
    """Test source connection through a Databricks SQL Warehouse using JDBC."""
    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        warehouses = list(ws.warehouses.list())
        running = [w for w in warehouses if "RUNNING" in (w.state.value if hasattr(w.state, 'value') else str(w.state)).upper()]
        wh = running[0] if running else (warehouses[0] if warehouses else None)
        if not wh:
            return {"success": False, "error": "No SQL Warehouse available to test JDBC connection."}

        jdbc_url = f"jdbc:sqlserver://{server};databaseName={database};encrypt=true;trustServerCertificate=true"
        test_sql = f"""
            SELECT * FROM jdbc(
                url => '{jdbc_url}',
                user => '{username}',
                password => '{password}',
                dbtable => '(SELECT @@VERSION AS ver) t'
            ) LIMIT 1
        """

        stmt = ws.statement_execution.execute_statement(
            warehouse_id=wh.id,
            statement=test_sql,
            wait_timeout="60s",
        )
        state = str(stmt.status.state).upper() if stmt.status else ""
        if state == "SUCCEEDED":
            rows = []
            if stmt.result and stmt.result.data_array:
                rows = stmt.result.data_array
            version = rows[0][0].split("\n")[0] if rows and rows[0] else "Connected via JDBC"
            return {
                "success": True,
                "server_version": version,
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


@source_bp.route("/source/test-connection", methods=["POST"])
@login_required
def source_test_connection():
    try:
        data = request.get_json(silent=True) or {}
        source_type = data.get("source_type", "sqlserver")
        server = (data.get("server") or "").strip()
        database = (data.get("database") or "").strip()
        username = (data.get("username") or "").strip()
        password = data.get("password", "")
        if not password or is_masked(password):
            password = get_source_password(source_type=source_type)

        # ── Snowflake path ──
        if source_type == "snowflake":
            account = (data.get("account") or "").strip()
            warehouse = (data.get("warehouse") or "").strip()
            role = (data.get("role") or "").strip()
            if not account or not username:
                return jsonify({"success": False, "error": "account and username are required for Snowflake"}), 400
            try:
                from snowflake_connector import test_connection as sf_test
                result = sf_test(
                    account=account, username=username, password=password,
                    database=database, warehouse=warehouse, role=role
                )
                return jsonify(result)
            except ImportError:
                from snowflake_connector import test_connection_via_warehouse as sf_test_wh
                result = sf_test_wh(
                    account=account, database=database,
                    username=username, password=password,
                    warehouse=warehouse, role=role
                )
                return jsonify(result)

        # ── Redshift path ──
        if source_type == "redshift":
            if not server or not username:
                return jsonify({"success": False, "error": "server and username are required for Redshift"}), 400
            from redshift_client import test_connection as rs_test
            result = rs_test(server=server, username=username, password=password, database=database)
            return jsonify(result)

        # ── SharePoint path ──
        if source_type == "sharepoint":
            site_url = server
            tenant_id = (data.get("tenant_id") or "").strip()
            client_id = username  # username field holds the Azure AD Client ID
            if not site_url:
                return jsonify({"success": False, "error": "SharePoint site URL is required"}), 400
            if not tenant_id or not client_id:
                return jsonify({"success": False, "error": "Tenant ID and Client ID are required for SharePoint"}), 400
            try:
                from sharepoint_connector import test_connection as sp_test
                result = sp_test(server=site_url, username=client_id, password=password,
                                 database=database, tenant_id=tenant_id)
                return jsonify(result)
            except ImportError as ie:
                return jsonify({"success": False, "error": f"SharePoint connector unavailable: {ie}"}), 500

        # ── Generic REST API path ──
        if source_type == "api":
            base_url = server
            auth_type = (data.get("api_auth_type") or "none").strip().lower()
            api_key_header = (data.get("api_key_header") or "").strip()
            if not base_url:
                return jsonify({"success": False, "error": "API Base URL is required"}), 400
            if auth_type == "basic" and not username:
                return jsonify({"success": False, "error": "Username is required for Basic Auth"}), 400
            if auth_type in ("api_key", "bearer", "basic") and not password:
                return jsonify({"success": False, "error": f"A secret is required for {auth_type} auth"}), 400
            try:
                from api_source_client import test_connection as api_test
                result = api_test(server=base_url, username=username, password=password,
                                  database=database, auth_type=auth_type,
                                  api_key_header=api_key_header)
                return jsonify(result)
            except ImportError as ie:
                return jsonify({"success": False, "error": f"API connector unavailable: {ie}"}), 500

        if not all([server, database, username]):
            return jsonify({"success": False, "error": "server, database and username are required"}), 400
        if source_type in ("azuresql", "synapse") and "." not in server:
            server = server + ".database.windows.net"

        # Try direct connection first (pymssql/pyodbc), fall back to SQL Warehouse JDBC
        try:
            from sql_pool import get_connection
            conn = get_connection(source_type, server, database, username, password, timeout=15)
        except ImportError:
            # No pymssql/pyodbc — fall back to SQL Warehouse JDBC
            port = 1433
            if ":" in server:
                parts = server.rsplit(":", 1)
                server = parts[0]
                try:
                    port = int(parts[1])
                except ValueError:
                    pass
            tcp_ok, tcp_msg = _tcp_test(server, port)
            if not tcp_ok:
                return jsonify({"success": False, "error": tcp_msg}), 200
            result = _test_via_sql_warehouse(server, database, username, password)
            return jsonify(result)
        except Exception as ce:
            msg = str(ce)
            hint = ""
            low = msg.lower()
            if "40925" in low or "current state" in low or "40613" in low:
                hint = " — Database is paused (serverless auto-pause). It was resuming but didn't finish in time. Please try again in 30 seconds."
            elif "login failed" in low or "18456" in low:
                hint = " — Username/password rejected by SQL Server."
            elif "im002" in low or "data source name" in low or "libodbc" in low:
                hint = " — ODBC driver not available. Testing via SQL Warehouse instead."
                result = _test_via_sql_warehouse(server, database, username, password)
                return jsonify(result)
            elif "timeout" in low or "08001" in low or "could not open" in low or "cannot reach" in low:
                hint = " — Cannot reach server. Check firewall, server name, and that your IP is allow-listed in Azure SQL."
            elif "tls" in low or "ssl" in low or "certificate" in low:
                hint = " — TLS/SSL handshake failed. Try Encrypt=yes;TrustServerCertificate=yes."
            logger.error("Source connection failed: %s", msg)
            return jsonify({"success": False, "error": msg + hint}), 200
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            row = cursor.fetchone()
            version = row[0].split("\n")[0].strip() if row else "Connected"
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return jsonify({"success": True, "server_version": version})
    except Exception as e:
        logger.exception("Unhandled error in test-connection")
        return jsonify({"success": False, "error": "Unexpected server error: " + str(e)}), 200


@source_bp.route("/source/load-objects", methods=["POST"])
@login_required
def source_load_objects():
    try:
        data = request.get_json()
        source_type = data.get("source_type", "sqlserver")
        server = data.get("server", "").strip()
        database = data.get("database", "").strip()
        username = data.get("username", "").strip()
        password = data.get("password", "")
        if not password or is_masked(password):
            password = get_source_password(source_type=source_type)

        # ── Snowflake path ──
        if source_type == "snowflake":
            account = (data.get("account") or "").strip()
            warehouse = (data.get("warehouse") or "").strip()
            role = (data.get("role") or "").strip()
            if not account or not username:
                return jsonify({"success": False, "error": "account and username are required for Snowflake"}), 400
            from snowflake_connector import load_objects as sf_load
            result = sf_load(
                account=account, username=username, password=password,
                database=database, warehouse=warehouse, role=role
            )
            if result.get("success"):
                return jsonify(result)
            else:
                return jsonify(result), 500

        # ── Redshift path ──
        if source_type == "redshift":
            if not server or not username:
                return jsonify({"success": False, "error": "server and username are required for Redshift"}), 400
            from redshift_client import load_objects as rs_load
            result = rs_load(server=server, username=username, password=password, database=database)
            if result.get("success"):
                return jsonify(result)
            else:
                return jsonify(result), 500

        # ── SharePoint path ──
        if source_type == "sharepoint":
            site_url = server
            tenant_id = (data.get("tenant_id") or "").strip()
            client_id = username  # username field holds the Azure AD Client ID
            if not site_url:
                return jsonify({"success": False, "error": "SharePoint site URL is required"}), 400
            if not tenant_id or not client_id:
                return jsonify({"success": False, "error": "Tenant ID and Client ID are required for SharePoint"}), 400
            from sharepoint_connector import load_objects as sp_load
            result = sp_load(server=site_url, username=client_id, password=password,
                             database=database, tenant_id=tenant_id)
            if result.get("success"):
                return jsonify(result)
            else:
                return jsonify(result), 500

        # ── Generic REST API path ──
        if source_type == "api":
            base_url = server
            auth_type = (data.get("api_auth_type") or "none").strip().lower()
            api_key_header = (data.get("api_key_header") or "").strip()
            if not base_url:
                return jsonify({"success": False, "error": "API Base URL is required"}), 400
            from api_source_client import load_objects as api_load
            result = api_load(server=base_url, username=username, password=password,
                              database=database, auth_type=auth_type,
                              api_key_header=api_key_header)
            if result.get("success"):
                return jsonify(result)
            else:
                return jsonify(result), 500

        if not all([server, database, username]):
            return jsonify({"success": False, "error": "server, database and username are required"}), 400

        if source_type in ("azuresql", "synapse") and "." not in server:
            server = server + ".database.windows.net"

        # Always try direct pymssql first (proven to work from Discovery tab)
        try:
            from sql_pool import get_connection
            conn = get_connection(source_type, server, database, username, password)
        except ImportError:
            return _load_objects_via_warehouse(source_type, server, database, username, password)
        except Exception as conn_err:
            logger.warning("Direct conn failed, trying warehouse: %s", str(conn_err)[:200])
            if _is_databricks_app():
                return _load_objects_via_warehouse(source_type, server, database, username, password)
            raise conn_err
        cursor = conn.cursor()
        cursor.execute("SET NOCOUNT ON")
        grouped = {"stored_procedure": [], "view": [], "udf": []}

        cursor.execute("""
            SELECT SCHEMA_NAME(schema_id) + '.' + name AS [key],
                   name,
                   ISNULL(OBJECT_DEFINITION(object_id), '') AS code
            FROM   sys.procedures
            WHERE  is_ms_shipped = 0
            ORDER  BY name
        """)
        for row in cursor.fetchall():
            grouped["stored_procedure"].append({
                "key": row[0], "name": row[1],
                "description": "Stored procedure", "code": row[2],
                "object_type": "stored_procedure"
            })

        cursor.execute("""
            SELECT SCHEMA_NAME(schema_id) + '.' + name AS [key],
                   name,
                   ISNULL(OBJECT_DEFINITION(object_id), '') AS code
            FROM   sys.views
            WHERE  is_ms_shipped = 0
            ORDER  BY name
        """)
        for row in cursor.fetchall():
            grouped["view"].append({
                "key": row[0], "name": row[1],
                "description": "SQL View", "code": row[2],
                "object_type": "view"
            })

        cursor.execute("""
            SELECT SCHEMA_NAME(schema_id) + '.' + name AS [key],
                   name,
                   ISNULL(OBJECT_DEFINITION(object_id), '') AS code
            FROM   sys.objects
            WHERE  type IN ('FN', 'IF', 'TF')
              AND  is_ms_shipped = 0
            ORDER  BY name
        """)
        for row in cursor.fetchall():
            grouped["udf"].append({
                "key": row[0], "name": row[1],
                "description": "User-defined function", "code": row[2],
                "object_type": "udf"
            })

        conn.close()
        total = sum(len(v) for v in grouped.values())
        return jsonify({"success": True, "grouped": grouped, "total": total,
                        "source_type": source_type, "database": database})
    except Exception as e:
        logger.exception("Failed to load source objects")
        return jsonify({"success": False, "error": str(e)}), 500


def _escape_spark_string(val: str) -> str:
    """Escape a value for embedding inside a Spark SQL single-quoted string literal."""
    return val.replace("\\", "\\\\").replace("'", "\\'")


def _load_objects_via_warehouse(source_type, server, database, username, password):
    """Load SQL objects via Databricks SQL Warehouse JDBC when no ODBC available."""
    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        warehouses = list(ws.warehouses.list())
        running = [w for w in warehouses if "RUNNING" in (w.state.value if hasattr(w.state, 'value') else str(w.state)).upper()]
        wh = running[0] if running else (warehouses[0] if warehouses else None)
        if not wh:
            return jsonify({"success": False, "error": "No SQL Warehouse available"}), 500

        if source_type in ("azuresql", "synapse") and "." not in server:
            server = server + ".database.windows.net"

        jdbc_url = f"jdbc:sqlserver://{server};databaseName={database};encrypt=true;trustServerCertificate=true"
        grouped = {"stored_procedure": [], "view": [], "udf": []}
        errors = []

        # Use CONCAT + CHAR(46) to avoid single quotes inside the subquery pushed to SQL Server
        # NULL from OBJECT_DEFINITION is handled in Python (no ISNULL needed)
        queries = {
            "stored_procedure": "SELECT CONCAT(SCHEMA_NAME(schema_id), CHAR(46), name), name, OBJECT_DEFINITION(object_id) FROM sys.procedures WHERE is_ms_shipped = 0 ORDER BY name",
            "view": "SELECT CONCAT(SCHEMA_NAME(schema_id), CHAR(46), name), name, OBJECT_DEFINITION(object_id) FROM sys.views WHERE is_ms_shipped = 0 ORDER BY name",
            "udf": "SELECT CONCAT(SCHEMA_NAME(schema_id), CHAR(46), name), name, OBJECT_DEFINITION(object_id) FROM sys.objects WHERE type IN (CHAR(70)+CHAR(78), CHAR(73)+CHAR(70), CHAR(84)+CHAR(70)) AND is_ms_shipped = 0 ORDER BY name",
        }

        safe_user = _escape_spark_string(username)
        safe_pass = _escape_spark_string(password)
        safe_url = _escape_spark_string(jdbc_url)

        for obj_type, query in queries.items():
            sql = f"""SELECT * FROM jdbc(url => '{safe_url}', user => '{safe_user}', password => '{safe_pass}', dbtable => '({query}) t')"""
            try:
                stmt = ws.statement_execution.execute_statement(
                    warehouse_id=wh.id, statement=sql, wait_timeout="60s"
                )
                _st = (stmt.status.state.value if hasattr(stmt.status.state, 'value') else str(stmt.status.state)).upper() if stmt.status and stmt.status.state else ""
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
                    err_msg = ""
                    if stmt.status and stmt.status.error:
                        err_msg = stmt.status.error.message or ""
                    errors.append(f"{obj_type}: {err_msg or 'statement failed'}")
                    logger.error("JDBC statement FAILED for %s: %s", obj_type, err_msg)
            except Exception as e:
                errors.append(f"{obj_type}: {str(e)[:200]}")
                logger.warning("Failed to load %s via JDBC: %s", obj_type, str(e)[:200])

        total = sum(len(v) for v in grouped.values())
        result = {"success": True, "grouped": grouped, "total": total,
                  "source_type": source_type, "database": database, "method": "jdbc_via_sql_warehouse"}
        if errors and total == 0:
            result["success"] = False
            result["error"] = "Failed to load objects: " + "; ".join(errors)
        elif errors:
            result["warnings"] = errors
        return jsonify(result)
    except Exception as e:
        logger.exception("_load_objects_via_warehouse failed")
        return jsonify({"success": False, "error": str(e)}), 500
