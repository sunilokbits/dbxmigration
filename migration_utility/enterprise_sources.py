"""
Connection test and object discovery for the two enterprise connectors that
have no client library of their own:

  * "adls"     -- files written to ADLS / a UC Volume (SNP Glue for SAP ECC,
                  TMS and EHS; file drops for Price Navigator / Adobe). Each
                  sub-folder of the landing folder is one source object.
  * "lakeflow" -- Databricks Lakeflow Connect. Credentials live in a Unity
                  Catalog connection, so the app only verifies the connection
                  exists; the objects to ingest are listed by the user.

Results use the same {success, grouped, total, source_type} contract as the
other connector modules (snowflake_connector, api_source_client, ...).
"""

import re

from log_config import get_logger

logger = get_logger(__name__)

_PATH_RE = re.compile(r"^(abfss://[^'\s]+|/Volumes/[^'\s]+)$")
_CONN_RE = re.compile(r"^[A-Za-z0-9_\-]{1,255}$")


def _landing_path(cfg: dict) -> str:
    return (cfg.get("server") or "").strip().rstrip("/")


def _list_folders(path: str) -> list:
    """Sub-folder names under an ADLS / Volume path, via the SQL warehouse."""
    from dbsql_client import execute_query
    rows = execute_query(f"LIST '{path}'")
    # LIST returns path, name, size, modification_time; folder names end with "/".
    return sorted({str(r.get("name", "")).rstrip("/") for r in rows if str(r.get("name", "")).endswith("/")})


def _parse_objects(cfg: dict) -> list:
    raw = cfg.get("objects") or ""
    items = raw if isinstance(raw, list) else re.split(r"[,\n;]+", str(raw))
    return [o.strip() for o in items if o and o.strip()]


def _grouped(source_type: str, names: list, description: str, code_fn) -> dict:
    view = [{"key": f"{source_type}.{n}", "name": n, "description": description,
             "code": code_fn(n), "object_type": "view"} for n in names]
    return {"stored_procedure": [], "view": view, "udf": []}


def test_connection(connector: str, cfg: dict) -> dict:
    try:
        if connector == "adls":
            path = _landing_path(cfg)
            if not _PATH_RE.match(path):
                return {"success": False, "error": "Landing folder must be an abfss:// URL or a /Volumes/... path"}
            folders = _list_folders(path)
            return {"success": True, "method": "adls_list",
                    "server_version": f"Landing folder reachable — {len(folders)} object folder(s) found"}
        if connector == "lakeflow":
            name = (cfg.get("server") or "").strip()
            if not _CONN_RE.match(name):
                return {"success": False, "error": "Enter the Unity Catalog connection name (letters, digits, _ or -)"}
            from databricks.sdk import WorkspaceClient
            info = WorkspaceClient().connections.get(name)
            ctype = getattr(getattr(info, "connection_type", None), "value", None) or str(getattr(info, "connection_type", ""))
            return {"success": True, "method": "lakeflow_connection",
                    "server_version": f"Unity Catalog connection '{name}' found ({ctype or 'type unknown'})"}
        return {"success": False, "error": f"No connection test for connector '{connector}'"}
    except Exception as e:
        logger.warning("Enterprise source test (%s) failed: %s", connector, e)
        return {"success": False, "error": str(e)[:500]}


def load_objects(connector: str, cfg: dict) -> dict:
    source_type = cfg.get("source_type", connector)
    try:
        if connector == "adls":
            path = _landing_path(cfg)
            if not _PATH_RE.match(path):
                return {"success": False, "error": "Landing folder must be an abfss:// URL or a /Volumes/... path"}
            fmt = (cfg.get("file_format") or "parquet").lower()
            names = _list_folders(path)
            grouped = _grouped(source_type, names, f"{fmt} files",
                               lambda n: f"-- {fmt} files\n-- {path}/{n}")
            result = {"success": True, "grouped": grouped, "total": len(names), "source_type": source_type}
            if not names:
                result["note"] = "No object folders found yet — each source object should be a sub-folder of the landing folder."
            return result
        if connector == "lakeflow":
            names = _parse_objects(cfg)
            conn = (cfg.get("server") or "").strip()
            grouped = _grouped(source_type, names, "Lakeflow Connect object",
                               lambda n: f"-- Lakeflow Connect via UC connection '{conn}'\n-- object: {n}")
            result = {"success": True, "grouped": grouped, "total": len(names), "source_type": source_type}
            if not names:
                result["note"] = "List the source objects to ingest (comma-separated) in the source settings."
            return result
        return {"success": False, "error": f"No object discovery for connector '{connector}'"}
    except Exception as e:
        logger.warning("Enterprise source load (%s) failed: %s", connector, e)
        return {"success": False, "error": str(e)[:500]}


def list_tables(connector: str, cfg: dict) -> dict:
    """Pipeline Studio shape: [{schema, table, full_name, col_count, row_estimate}]."""
    res = load_objects(connector, cfg)
    if not res.get("success"):
        return res
    schema = cfg.get("source_type", connector)
    tables = [{"schema": schema, "table": o["name"], "full_name": f"{schema}.{o['name']}",
               "col_count": 0, "row_estimate": 0} for o in res["grouped"]["view"]]
    return {"success": True, "tables": tables, "total": len(tables)}
