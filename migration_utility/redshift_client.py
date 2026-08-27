"""Amazon Redshift connector — test connection, load SQL objects (procedures, views, UDFs).

Mirrors the Snowflake flow in snowflake_connector.py but targets Redshift via the
official `redshift_connector` library. Module is named redshift_client.py (not
redshift_connector.py) so it doesn't shadow the pip package of the same name.
"""
from log_config import get_logger

logger = get_logger(__name__)

DEFAULT_PORT = 5439


def _split_host_port(server: str) -> tuple[str, int]:
    """Redshift host field may be 'host' or 'host:port' — default port 5439."""
    if ":" in server:
        host, _, port_str = server.rpartition(":")
        try:
            return host, int(port_str)
        except ValueError:
            return server, DEFAULT_PORT
    return server, DEFAULT_PORT


def get_redshift_connection(server: str, username: str, password: str,
                            database: str = "", timeout: int = 30):
    """Return a Redshift connection using redshift_connector."""
    import redshift_connector

    host, port = _split_host_port(server)
    conn = redshift_connector.connect(
        host=host, port=port, database=database or "dev",
        user=username, password=password, timeout=timeout,
    )
    return conn


def test_connection(server: str, username: str, password: str,
                    database: str = "") -> dict:
    """Test connectivity to Redshift and return version info."""
    try:
        conn = get_redshift_connection(server, username, password, database, timeout=20)
        try:
            cur = conn.cursor()
            cur.execute("SELECT version()")
            row = cur.fetchone()
            version = row[0] if row else "Connected"
            return {"success": True, "server_version": str(version), "method": "redshift_connector"}
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as e:
        msg = str(e)
        hint = ""
        low = msg.lower()
        if "password authentication failed" in low or "invalid_password" in low:
            hint = " — Username/password rejected by Redshift."
        elif "timeout" in low or "could not connect" in low or "connection refused" in low:
            hint = " — Cannot reach Redshift cluster. Check host/port and that your IP is allow-listed in the cluster's security group / VPC."
        elif "database" in low and ("does not exist" in low or "unknown" in low):
            hint = " — Database not found on this cluster."
        logger.error("Redshift connection test failed: %s", msg)
        return {"success": False, "error": msg + hint}


def load_objects(server: str, username: str, password: str, database: str = "") -> dict:
    """Load stored procedures, views, and UDFs from Redshift."""
    try:
        conn = get_redshift_connection(server, username, password, database, timeout=30)
    except Exception as e:
        logger.warning("Redshift connection failed: %s", str(e)[:200])
        return {"success": False, "error": str(e)}

    grouped = {"stored_procedure": [], "view": [], "udf": []}
    try:
        cur = conn.cursor()

        # ── Stored Procedures (Redshift procedures live in pg_catalog, prokind='p') ──
        cur.execute("""
            SELECT n.nspname || '.' || p.proname AS key, p.proname AS name,
                   COALESCE(pg_get_functiondef(p.oid), '') AS code
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE p.prokind = 'p' AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY p.proname
        """)
        for row in cur.fetchall():
            grouped["stored_procedure"].append({
                "key": row[0], "name": row[1],
                "description": "Stored procedure", "code": row[2],
                "object_type": "stored_procedure",
            })

        # ── Views ──
        cur.execute("""
            SELECT table_schema || '.' || table_name AS key, table_name AS name,
                   COALESCE(view_definition, '') AS code
            FROM information_schema.views
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_name
        """)
        for row in cur.fetchall():
            grouped["view"].append({
                "key": row[0], "name": row[1],
                "description": "SQL View", "code": row[2],
                "object_type": "view",
            })

        # ── User-Defined Functions ──
        cur.execute("""
            SELECT n.nspname || '.' || p.proname AS key, p.proname AS name,
                   COALESCE(pg_get_functiondef(p.oid), '') AS code
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE p.prokind = 'f' AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY p.proname
        """)
        for row in cur.fetchall():
            grouped["udf"].append({
                "key": row[0], "name": row[1],
                "description": "User-defined function", "code": row[2],
                "object_type": "udf",
            })

        conn.close()
        total = sum(len(v) for v in grouped.values())
        return {"success": True, "grouped": grouped, "total": total, "source_type": "redshift", "database": database}
    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        logger.exception("Failed to load Redshift objects")
        return {"success": False, "error": str(e)}
