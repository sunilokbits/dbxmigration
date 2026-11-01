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


def _saved_servicenow_client_id() -> str:
    """OAuth Client ID entered in Settings (saved as the source's username)."""
    try:
        from config_cache import get_config
        src = (get_config() or {}).get("source") or {}
    except Exception:
        return ""
    return (src.get("username") or "").strip() if src.get("source_type") == "servicenow" else ""


def _servicenow_live(url: str = "", cid: str = "", csec: str = ""):
    """(instance_url, authed session) for the ServiceNow instance at `url` (default:
    the instance-url secret), or None when the client ID/secret aren't configured.
    Raises if the login fails."""
    from secrets_helper import get_secret
    url = (url or get_secret("source-servicenow-instance-url") or "").rstrip("/")
    cid = cid or _saved_servicenow_client_id()
    csec = csec or get_secret("source-servicenow-client-secret")
    if not (url and cid and csec):
        return None
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    sess = requests.Session()
    # Developer instances drop TLS handshakes intermittently
    sess.mount("https://", HTTPAdapter(max_retries=Retry(total=3, connect=3, read=3, backoff_factor=2)))
    # Newer releases reject basic auth on REST. Prefer app-to-app client credentials,
    # fall back to a user password grant; scope=useraccount grants the Table API.
    r = None
    for grant in ("client_credentials", "password"):
        data = {"grant_type": grant, "client_id": cid, "client_secret": csec, "scope": "useraccount"}
        if grant == "password":
            pwd = get_secret("source-servicenow-password")
            if not pwd:
                break
            data.update(username=get_secret("source-servicenow-username") or "admin", password=pwd)
        r = sess.post(f"{url}/oauth_token.do", timeout=(20, 60), data=data)
        if r.ok and "access_token" in r.text:
            sess.headers.update({"Authorization": f"Bearer {r.json()['access_token']}",
                                 "Accept": "application/json"})
            return url, sess
    raise RuntimeError(f"ServiceNow OAuth login failed (HTTP {getattr(r, 'status_code', '?')})")


# ── REST sources with a registry profile (ServiceNow Table API, Dataverse OData, SharePoint) ──
_REST_URL_RE = re.compile(r"^https://[A-Za-z0-9.\-]+(:\d+)?(/[A-Za-z0-9._~%\-/]*)?$")
_AUTH_LABELS = {"oauth2": "OAuth 2.0 (client credentials)", "none": "No Auth"}
_COUNT_PAGE_CAP = 50  # pages of 999 ids when a source has no count endpoint


def _label(source_type: str) -> str:
    from source_registry import BUILTIN, ENTERPRISE
    return (ENTERPRISE.get(source_type) or BUILTIN.get(source_type) or {}).get("label", source_type)


def _origin(url: str) -> str:
    """scheme://host[:port] of a URL."""
    return "/".join(url.split("/", 3)[:3])


def _retry_session():
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    sess = requests.Session()
    # Developer / demo instances drop TLS handshakes intermittently
    sess.mount("https://", HTTPAdapter(max_retries=Retry(total=3, connect=3, read=3, backoff_factor=2)))
    return sess


def _rest_base(cfg: dict, prof: dict) -> tuple[str, str]:
    """(base_url, auth_type) validated against the source's REST profile."""
    st = cfg.get("source_type", "")
    base = (cfg.get("server") or "").strip().rstrip("/")
    if not _REST_URL_RE.match(base):
        raise ValueError(f"Enter the {_label(st)} API base URL (https://...)")
    allowed = prof.get("auth_types") or ["oauth2"]
    auth = (cfg.get("api_auth_type") or allowed[0]).strip().lower()
    if auth not in allowed:
        raise ValueError(f"{_label(st)} REST API supports Auth Type: "
                         + " or ".join(f"'{_AUTH_LABELS.get(a, a)}'" for a in allowed))
    return base, auth


def _saved_source() -> dict:
    try:
        from config_cache import get_config
        return (get_config() or {}).get("source") or {}
    except Exception:
        return {}


def _rest_session(cfg: dict):
    """(base_url, authed session, profile) for a REST source with a registry profile."""
    from secrets_helper import get_secret, is_masked
    from source_registry import rest_api_base, rest_oauth_secret_key, rest_profile
    st = cfg.get("source_type", "")
    prof = rest_profile(st)
    if not prof:
        raise ValueError(f"No REST API profile for source '{st}'")
    base, auth = _rest_base(cfg, prof)
    prof = rest_profile(st, auth)
    # Values typed in Settings (not yet saved) win; otherwise the stored config / secret.
    typed = cfg.get("password") or ""
    csec = "" if is_masked(typed) else typed
    if st == "servicenow":
        live = _servicenow_live(base, cid=(cfg.get("username") or "").strip(), csec=csec)
        if not live:
            raise ValueError("ServiceNow OAuth not configured — enter the Client ID in Settings and save the "
                             "Client Secret (stored as secret source-servicenow-client-secret)")
        return live[0], live[1], prof
    sess = _retry_session()
    sess.headers.update({"Accept": "application/json", **(prof.get("headers") or {})})
    if auth == "oauth2":
        saved = _saved_source()
        saved = saved if saved.get("source_type") == st else {}
        cid = (cfg.get("username") or saved.get("username") or "").strip()
        tenant = (cfg.get("tenant_id") or saved.get("tenant_id") or "").strip()
        csec = csec or get_secret(rest_oauth_secret_key(st))
        token_url = prof.get("token_url") or base + prof.get("token_path", "/oauth/token")
        if "{tenant}" in token_url and not tenant:
            raise ValueError("Enter the Tenant ID (Microsoft Entra) for OAuth")
        if not (cid and csec):
            raise ValueError(f"{_label(st)} OAuth not configured — enter the Client ID and Client Secret in Settings")
        token_url = token_url.replace("{tenant}", tenant).replace("{origin}", _origin(base))
        r = sess.post(token_url, timeout=(20, 60), data={
            "grant_type": "client_credentials", "client_id": cid, "client_secret": csec,
            "scope": (prof.get("scope") or "").replace("{origin}", _origin(base))})
        if not (r.ok and "access_token" in r.text):
            raise RuntimeError(f"{_label(st)} OAuth login failed (HTTP {r.status_code})")
        sess.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
    return rest_api_base(base, prof), sess, prof


def _rest_records(doc) -> list:
    if isinstance(doc, list):
        return doc
    for k in ("result", "value"):
        if isinstance((doc or {}).get(k), list):
            return doc[k]
    return []


def _is_annotation(key) -> bool:
    return str(key).startswith(("@odata.", "odata."))


def _count_by_paging(url: str, sess, path: str) -> int:
    rows, nxt, params = 0, f"{url}{path}", {"$select": "id", "$top": 999}
    for _ in range(_COUNT_PAGE_CAP):
        r = sess.get(nxt, params=params, timeout=(20, 90))
        r.raise_for_status()
        doc = r.json()
        rows += len(_rest_records(doc))
        nxt, params = doc.get("@odata.nextLink"), None
        if not nxt:
            break
    return rows


def _rest_table_stats(prof: dict, url: str, sess, table: str) -> tuple[int, int]:
    """(col_count, row_count) of one object, from a 1-row sample plus the
    profile's count endpoint (or the OData count on the sample)."""
    path = prof["table_path"].replace("{table}", table)
    r = sess.get(f"{url}{path}", params=prof.get("sample_params") or {}, timeout=(20, 90))
    r.raise_for_status()
    doc = r.json()
    rec = (_rest_records(doc) or [{}])[0]
    if prof.get("record_key") and isinstance(rec.get(prof["record_key"]), dict):
        rec = rec[prof["record_key"]]
    cols = len([k for k in rec if not _is_annotation(k)])
    rows = 0
    count = doc.get("@odata.count", doc.get("odata.count")) if isinstance(doc, dict) else None
    if count is not None:
        rows = int(count or 0)
    elif prof.get("count") == "paging":
        rows = _count_by_paging(url, sess, path)
    elif prof.get("count_path"):
        s = sess.get(f"{url}{prof['count_path'].replace('{table}', table)}",
                      params=prof.get("count_params") or {}, timeout=(20, 90))
        rows = int(((s.json().get("result") or {}).get("stats") or {}).get("count") or 0) if s.ok else 0
    return cols, rows


def _servicenow_table_stats(url: str, sess, table: str) -> tuple[int, int]:
    from source_registry import rest_profile
    return _rest_table_stats(rest_profile("servicenow"), url, sess, table)


def rest_profile_test(cfg: dict) -> dict:
    st = cfg.get("source_type", "")
    try:
        url, sess, prof = _rest_session(cfg)
        probe = prof.get("test_table") or (_parse_objects(cfg) or [""])[0]
        auth = "OAuth" if "Authorization" in sess.headers else "no auth"
        msg = f"{_label(st)} REST API reachable via {auth} ({url.split('//', 1)[-1]})"
        if probe:
            cols, rows = _rest_table_stats(prof, url, sess, probe)
            msg += f" — {probe}: {rows:,} rows, {cols} cols"
        else:
            sess.get(url, timeout=(20, 60)).raise_for_status()
        return {"success": True, "method": "rest_oauth2" if auth == "OAuth" else "rest", "server_version": msg}
    except Exception as e:
        logger.warning("%s REST test failed: %s", st, e)
        return {"success": False, "error": str(e)[:500]}


def _discover_objects(cfg: dict) -> list:
    """Object names listed by the source itself (e.g. SharePoint lists via Graph), else []."""
    url, sess, prof = _rest_session(cfg)
    if not prof.get("discover_path"):
        return []
    r = sess.get(f"{url}{prof['discover_path']}", params={"$select": "displayName,list"}, timeout=(20, 90))
    r.raise_for_status()
    return [o["displayName"] for o in _rest_records(r.json())
            if o.get("displayName") and not (o.get("list") or {}).get("hidden")]


def rest_profile_objects(cfg: dict) -> dict:
    from source_registry import rest_api_base, rest_profile
    st = cfg.get("source_type", "")
    prof = rest_profile(st, (cfg.get("api_auth_type") or "").strip().lower()) or {}
    path = prof.get("table_path", "/{table}")
    names = _parse_objects(cfg)
    if not names and prof.get("discover_path"):
        try:
            names = _discover_objects(cfg)
        except Exception as e:
            return {"success": False, "error": str(e)[:500]}
    base = rest_api_base(cfg.get("server") or "", prof)
    grouped = _grouped(st, names, f"{_label(st)} object (REST API)",
                       lambda n: f"-- {_label(st)} REST API\n-- GET {base}{path.replace('{table}', n)}")
    result = {"success": True, "grouped": grouped, "total": len(names), "source_type": st}
    if not names:
        result["note"] = f"List the {_label(st)} objects to extract (comma-separated) in Source Objects."
    return result


def rest_profile_tables(cfg: dict) -> dict:
    """Pipeline Studio shape, with live row/column counts from the source."""
    st = cfg.get("source_type", "")
    try:
        url, sess, prof = _rest_session(cfg)
    except Exception as e:
        return {"success": False, "error": str(e)[:500]}
    tables = []
    names = _parse_objects(cfg)
    if not names and prof.get("discover_path"):
        try:
            names = _discover_objects(cfg)
        except Exception as e:
            return {"success": False, "error": str(e)[:500]}
    for i, n in enumerate(names):
        cols = rows = 0
        if i < _STATS_MAX_OBJECTS:
            try:
                cols, rows = _rest_table_stats(prof, url, sess, n)
            except Exception as e:
                logger.warning("%s stats for %s failed: %s", st, n, e)
        tables.append({"schema": st, "table": n, "full_name": f"{st}.{n}",
                       "col_count": cols, "row_estimate": rows})
    return {"success": True, "tables": tables, "total": len(tables)}


_LF_CACHE = {"at": 0.0, "map": {}}
_LF_CACHE_TTL = 120


def lakeflow_destination(connection: str, table: str) -> str:
    """Fully-qualified Delta table a Lakeflow Connect ingestion pipeline writes
    `table` to for UC `connection`, or "" when no such pipeline exists."""
    import time
    if time.time() - _LF_CACHE["at"] > _LF_CACHE_TTL:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        mapping = {}
        for p in w.pipelines.list_pipelines():
            try:
                spec = w.api_client.do("GET", f"/api/2.0/pipelines/{p.pipeline_id}").get("spec") or {}
            except Exception:
                continue
            ing = spec.get("ingestion_definition") or {}
            conn = (ing.get("connection_name") or "").lower()
            if not conn:
                continue
            for obj in ing.get("objects") or []:
                t = obj.get("table") or {}
                if t.get("source_table"):
                    dest = t.get("destination_table") or t["source_table"]
                    mapping[(conn, t["source_table"].lower())] = (
                        f"{t['destination_catalog']}.{t['destination_schema']}.{dest}")
                s = obj.get("schema") or {}
                if s.get("destination_schema"):
                    mapping[(conn, "*")] = f"{s['destination_catalog']}.{s['destination_schema']}"
        _LF_CACHE.update(at=time.time(), map=mapping)
    m = _LF_CACHE["map"]
    key = (connection.lower(), table.lower())
    if key in m:
        return m[key]
    whole = m.get((connection.lower(), "*"))
    return f"{whole}.{table}" if whole else ""


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
            msg = f"Unity Catalog connection '{name}' found ({ctype or 'type unknown'})"
            if cfg.get("source_type") == "servicenow":
                live = _servicenow_live()
                if live:
                    url, sess = live
                    cols, rows = _servicenow_table_stats(url, sess, "incident")
                    host = url.split("//", 1)[-1]
                    msg += f" · Live instance {host} reachable — incident: {rows:,} rows, {cols} cols"
            return {"success": True, "method": "lakeflow_connection", "server_version": msg}
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


_STATS_MAX_OBJECTS = 25  # cap warehouse round-trips when a landing folder has many objects


def _folder_stats(path: str, fmt: str) -> tuple[int, int]:
    """(col_count, row_count) for one object folder, read through the warehouse."""
    from dbsql_client import execute_query
    fmt = fmt if fmt in ("parquet", "csv", "json", "delta") else "parquet"
    rel = f"{fmt}.`{path}`"
    try:
        sample = execute_query(f"SELECT * FROM {rel} LIMIT 1")
        cols = len(sample[0]) if sample else 0
        cnt = execute_query(f"SELECT COUNT(*) AS c FROM {rel}")
        rows = int(next(iter(cnt[0].values()))) if cnt else 0
        return cols, rows
    except Exception as e:
        logger.debug("Folder stats failed for %s: %s", path, e)
        return 0, 0


def list_tables(connector: str, cfg: dict) -> dict:
    """Pipeline Studio shape: [{schema, table, full_name, col_count, row_estimate}]."""
    res = load_objects(connector, cfg)
    if not res.get("success"):
        return res
    schema = cfg.get("source_type", connector)
    base = _landing_path(cfg)
    fmt = (cfg.get("file_format") or "parquet").lower()
    live = None
    if connector == "lakeflow" and schema == "servicenow":
        try:
            live = _servicenow_live()
        except Exception as e:
            logger.warning("ServiceNow live stats skipped: %s", e)
    tables = []
    for i, o in enumerate(res["grouped"]["view"]):
        cols = rows = 0
        if connector == "adls" and i < _STATS_MAX_OBJECTS:
            cols, rows = _folder_stats(f"{base}/{o['name']}", fmt)
        elif live and i < _STATS_MAX_OBJECTS:
            try:
                cols, rows = _servicenow_table_stats(live[0], live[1], o["name"])
            except Exception as e:
                logger.warning("ServiceNow stats for %s failed: %s", o["name"], e)
        tables.append({"schema": schema, "table": o["name"], "full_name": f"{schema}.{o['name']}",
                       "col_count": cols, "row_estimate": rows})
    return {"success": True, "tables": tables, "total": len(tables)}
