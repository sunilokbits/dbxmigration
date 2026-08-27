"""Generic REST API source connector — test connection, load endpoints.

Named api_source_client.py (not api_connector.py) to avoid any shadowing risk.
Mirrors the redshift_client.py interface:
    test_connection() -> {"success": bool, "server_version": str, ...}
    load_objects()    -> {"success": bool, "grouped": {...}, "total": int, ...}

Field mapping (same convention as the other source types):
    server   -> API base URL             (e.g. https://api.contoso.com/v1)
    username -> basic-auth username      (auth_type=basic only)
    password -> secret: API key / bearer token / basic password
    api_auth_type    -> none | api_key | bearer | basic
    api_key_header   -> header name when auth_type=api_key (default X-API-Key)
"""
import json

import requests

from log_config import get_logger

logger = get_logger(__name__)

DEFAULT_TIMEOUT = 20
_SWAGGER_PATHS = ("openapi.json", "swagger/v1/swagger.json", "v2/swagger.json",
                  "v3/swagger.json", "swagger.json")


def _normalise_base_url(base_url: str) -> str:
    """Ensure scheme and no trailing slash."""
    url = (base_url or "").strip().rstrip("/")
    if url and not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def build_auth_headers(auth_type: str, secret: str,
                       key_header: str = "", username: str = "") -> dict:
    """Build request headers for the configured auth mode."""
    import base64
    auth_type = (auth_type or "none").strip().lower()
    headers = {"Accept": "application/json"}
    if auth_type == "api_key":
        if secret:
            headers[(key_header or "X-API-Key").strip()] = secret
    elif auth_type == "bearer":
        if secret:
            tok = secret.strip()
            headers["Authorization"] = tok if tok.lower().startswith("bearer ") else f"Bearer {tok}"
    elif auth_type == "basic":
        if secret or username:
            raw = f"{username or ''}:{secret or ''}".encode("utf-8")
            headers["Authorization"] = "Basic " + base64.b64encode(raw).decode("ascii")
    return headers


def test_connection(server: str, username: str, password: str,
                    database: str = "", auth_type: str = "none",
                    api_key_header: str = "") -> dict:
    """Test connectivity to the API base URL with the configured auth."""
    try:
        base_url = _normalise_base_url(server)
        if not base_url:
            return {"success": False, "error": "API Base URL is required"}
        headers = build_auth_headers(auth_type, password, api_key_header, username)
        resp = requests.get(base_url, headers=headers, timeout=DEFAULT_TIMEOUT,
                            allow_redirects=True)
        status = resp.status_code
        if status in (401, 403):
            return {"success": False,
                    "error": f"Authentication failed (HTTP {status}) — check the auth type and credentials."}
        # Any well-formed HTTP response proves reachability + TLS
        server_hdr = resp.headers.get("Server", "")
        desc = f"API reachable — HTTP {status}"
        if server_hdr:
            desc += f" ({server_hdr})"
        return {"success": True, "server_version": desc, "method": f"rest_{(auth_type or 'none').lower()}"}
    except requests.exceptions.SSLError as e:
        return {"success": False, "error": f"TLS/SSL error connecting to {base_url}: {str(e)[:200]}"}
    except Exception as e:
        msg = str(e)
        logger.error("API connection test failed: %s", msg)
        hint = ""
        low = msg.lower()
        if "timeout" in low:
            hint = " — Request timed out. Check the URL and network/firewall."
        elif "name or service not known" in low or "getaddrinfo failed" in low or "nodename" in low:
            hint = " — DNS resolution failed. Check the host name."
        return {"success": False, "error": msg + hint}


def _fetch_swagger_paths(base_url: str, headers: dict) -> list:
    """Try common OpenAPI/Swagger locations; return sorted GET paths."""
    for p in _SWAGGER_PATHS:
        url = f"{base_url}/{p.lstrip('/')}"
        try:
            resp = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
            if resp.status_code != 200:
                continue
            doc = resp.json()
            paths = doc.get("paths") or {}
            found = []
            for path, methods in paths.items():
                if isinstance(methods, dict) and "get" in methods:
                    summary = ""
                    info = methods.get("get") or {}
                    if isinstance(info, dict):
                        summary = info.get("summary") or ""
                    found.append((path, summary))
            if found:
                return sorted(found)
        except (ValueError, requests.RequestException):
            continue
    return []


def load_objects(server: str, username: str, password: str,
                 database: str = "", auth_type: str = "none",
                 api_key_header: str = "") -> dict:
    """Discover API endpoints via OpenAPI/Swagger (best effort).

    GET endpoints are surfaced as 'view' objects whose code is a sample curl,
    matching the grouped {stored_procedure, view, udf} contract used elsewhere."""
    try:
        base_url = _normalise_base_url(server)
        if not base_url:
            return {"success": False, "error": "API Base URL is required"}
        headers = build_auth_headers(auth_type, password, api_key_header, username)

        endpoints = _fetch_swagger_paths(base_url, headers)
        grouped = {"stored_procedure": [], "view": [], "udf": []}
        for path, summary in endpoints:
            safe_path = path.replace("'", "%27")
            code = (
                f"-- REST endpoint: {path}\n"
                + (f"-- {summary}\n" if summary else "")
                + f"curl -H 'Accept: application/json' \\\n"
                f"     '{base_url}{safe_path}'"
            )
            grouped["view"].append({
                "key": f"api.{path}",
                "name": f"GET {path}",
                "description": summary or "REST endpoint",
                "code": code,
                "object_type": "view",
            })

        total = sum(len(v) for v in grouped.values())
        result = {"success": True, "grouped": grouped, "total": total,
                  "source_type": "api", "database": database}
        if total == 0:
            result["note"] = ("No OpenAPI/Swagger spec found at the base URL — "
                              "endpoints can still be migrated by registering them manually.")
        return result
    except Exception as e:
        logger.exception("Failed to load API objects")
        return {"success": False, "error": str(e)}
