"""Shared SQL Server helpers — connection string builder + connection pooling.

Consolidates the duplicate _build_sql_conn_str / _odbc_escape functions that
were copy-pasted across source.py, schema.py, and data_migrator.py.
Uses pyodbc connection pooling to avoid creating a fresh TCP connection on
every request.
"""
import threading
from log_config import get_logger

logger = get_logger(__name__)

pyodbc = None

def _ensure_pyodbc():
    global pyodbc
    if pyodbc is None:
        import importlib
        try:
            pyodbc = importlib.import_module("pyodbc")
            pyodbc.pooling = True
        except (ImportError, OSError):
            logger.warning("pyodbc/libodbc not available — pymssql will be used for SQL Server connections")
            pyodbc = False  # mark as unavailable
    return None if pyodbc is False else pyodbc


def _odbc_escape(val: str) -> str:
    """Escape braces for ODBC connection-string values."""
    return "{" + val.replace("}", "}}") + "}"


def _detect_driver() -> str:
    """Return the best available ODBC driver name (cached after first call)."""
    if not hasattr(_detect_driver, "_cached"):
        try:
            installed = _ensure_pyodbc().drivers()
        except Exception:
            logger.warning("pyodbc.drivers() failed — falling back to default driver")
            installed = []
        _detect_driver._cached = (
            next((d for d in installed if "ODBC Driver 17 for SQL Server" in d), None)
            or next((d for d in installed if "ODBC Driver 18 for SQL Server" in d), None)
            or next((d for d in installed if "SQL Server" in d), None)
            or "ODBC Driver 17 for SQL Server"
        )
    return _detect_driver._cached


def build_sql_conn_str(source_type: str, server: str, database: str,
                       username: str, password: str, timeout: int = 45) -> str:
    """Build a pyodbc connection string for SQL Server / Azure SQL / Synapse."""
    driver = _detect_driver()
    safe_pwd = _odbc_escape(password) if password else ""
    safe_user = _odbc_escape(username) if username else ""
    # Auto-append FQDN for Azure SQL if user only provided the server name
    if source_type in ("azuresql", "synapse") and "." not in server:
        server = server + ".database.windows.net"
    base = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={safe_user};PWD={safe_pwd}"
    is_v18 = "18" in driver
    if source_type in ("azuresql", "synapse"):
        base += f";Encrypt=yes;TrustServerCertificate=no;Connection Timeout={timeout}"
    else:
        if is_v18:
            base += f";Encrypt=optional;TrustServerCertificate=yes;Connection Timeout={timeout}"
        else:
            base += f";Encrypt=no;TrustServerCertificate=yes;Connection Timeout={timeout}"
    return base


def get_connection(source_type: str, server: str, database: str,
                   username: str, password: str, timeout: int = 15):
    """Return a pooled pyodbc connection with a hard thread-based timeout.
    
    On Linux/unixODBC, pyodbc's timeout parameter and ODBC Connection Timeout
    are sometimes ignored during DNS resolution or TCP handshake. We wrap the
    connect call in a thread to guarantee it returns within `timeout` seconds.
    
    Handles Azure SQL Serverless auto-pause (error 40925/40613) by retrying up
    to 5 times with increasing delays to allow the database to resume (~30-60s).
    """
    import time as _time

    # Route to pymssql when pyodbc/libodbc is unavailable (non-Docker runtime)
    if _ensure_pyodbc() is None:
        return _connect_pymssql(source_type, server, database, username, password, timeout)

    conn_str = build_sql_conn_str(source_type, server, database, username, password, timeout=timeout)

    max_retries = 6
    retry_delays = [5, 10, 15, 20, 25, 30]  # total wait: ~105s + connect attempts

    for attempt in range(max_retries + 1):
        result = [None]
        error = [None]

        def _connect():
            try:
                result[0] = _ensure_pyodbc().connect(conn_str, timeout=timeout)
            except Exception as e:
                error[0] = e

        t = threading.Thread(target=_connect, daemon=True)
        t.start()
        t.join(timeout=timeout + 5)  # give 5s grace beyond ODBC timeout

        if t.is_alive():
            if attempt < max_retries:
                logger.info("Connection attempt %d/%d timed out, retrying...", attempt + 1, max_retries)
                _time.sleep(retry_delays[attempt])
                continue
            raise Exception(f"Connection timed out after {timeout}s — cannot reach server '{server}'. Check firewall rules and that the App Service IPs are allow-listed.")

        if error[0]:
            err_msg = str(error[0]).lower()
            # 40925 = database paused (serverless), 40613 = database unavailable
            is_resuming = ("40925" in err_msg or "40613" in err_msg or
                          "not currently available" in err_msg or
                          "current state" in err_msg or
                          "is not accessible" in err_msg)
            if is_resuming and attempt < max_retries:
                logger.info("Database is resuming from pause (attempt %d/%d), retrying in %ds...",
                            attempt + 1, max_retries, retry_delays[attempt])
                _time.sleep(retry_delays[attempt])
                continue
            raise error[0]

        # ── Permanent fix: suppress ALL SQL Server informational messages ──
        # autocommit=True prevents implicit transaction BEGIN/COMMIT messages.
        # SET NOCOUNT ON + ANSI_WARNINGS OFF suppresses row-count and warning
        # messages that cause pyodbc "Check messages from the SQL Server" errors.
        try:
            result[0].autocommit = True
            _cur = result[0].cursor()
            _cur.execute("SET NOCOUNT ON; SET ANSI_WARNINGS OFF")
            _cur.close()
        except Exception:
            pass  # non-fatal — proceed with connection as-is
        return result[0]


def _connect_pymssql(source_type: str, server: str, database: str,
                     username: str, password: str, timeout: int = 15):
    """Pure-Python fallback when pyodbc/libodbc is not available (non-Docker runtime).
    Returns tuples (not dicts) to match pyodbc row access patterns.
    """
    import pymssql
    if source_type in ("azuresql", "synapse") and "." not in server:
        server = server + ".database.windows.net"
    conn = pymssql.connect(
        server=server,
        user=username,
        password=password,
        database=database,
        port=1433,
        login_timeout=timeout,
        timeout=timeout,
        tds_version="7.4",
    )
    # Permanent fix: suppress informational messages on pymssql path
    try:
        conn.autocommit(True)
        _cur = conn.cursor()
        _cur.execute("SET NOCOUNT ON; SET ANSI_WARNINGS OFF")
        _cur.close()
    except Exception:
        pass
    return conn
