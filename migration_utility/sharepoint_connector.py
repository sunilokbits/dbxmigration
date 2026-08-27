"""Microsoft SharePoint Online connector — test connection, load lists.

Mirrors the Redshift flow in redshift_client.py but targets SharePoint Online
via its REST API (`_api/web`) authenticated with Azure AD client credentials
(OAuth 2.0 v2 token endpoint). Uses only `requests` — no extra dependencies.

Field mapping (same convention as the other source types):
    server   -> SharePoint site URL      (e.g. https://contoso.sharepoint.com/sites/mysite)
    username -> Azure AD Client ID
    password -> Azure AD Client Secret
    tenant_id-> Azure AD Tenant ID (GUID or domain)
"""
import re

import requests

from log_config import get_logger

logger = get_logger(__name__)

TOKEN_TIMEOUT = 20
API_TIMEOUT = 30


def _normalise_site_url(site_url: str) -> str:
    """Ensure the site URL has a scheme and no trailing slash."""
    url = (site_url or "").strip().rstrip("/")
    if url and not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def _sharepoint_scope(site_url: str) -> str:
    """Build the OAuth scope for the SharePoint tenant, e.g.
    https://contoso.sharepoint.com/.default"""
    m = re.match(r"https?://([^/]+)", _normalise_site_url(site_url))
    host = m.group(1) if m else ""
    if not host:
        return ""
    # Site collection app principal scope uses the root tenant host
    parts = host.split(".")
    tenant_root = ".".join(parts[1:]) if len(parts) > 2 else host
    return f"https://{tenant_root}/.default"


def get_access_token(tenant_id: str, client_id: str, client_secret: str,
                     site_url: str = "") -> str:
    """Acquire an access token via Azure AD client-credentials flow."""
    tenant = (tenant_id or "").strip()
    if not tenant:
        raise ValueError("Tenant ID is required for SharePoint authentication")
    token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    resp = requests.post(token_url, data={
        "grant_type": "client_credentials",
        "client_id": (client_id or "").strip(),
        "client_secret": client_secret or "",
        "scope": _sharepoint_scope(site_url),
    }, timeout=TOKEN_TIMEOUT)
    if resp.status_code != 200:
        detail = ""
        try:
            detail = resp.json().get("error_description", "")[:300]
        except Exception:
            detail = resp.text[:300]
        raise RuntimeError(f"Azure AD token request failed (HTTP {resp.status_code}): {detail}")
    tok = resp.json().get("access_token", "")
    if not tok:
        raise RuntimeError("Azure AD returned no access token")
    return tok


def _sp_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}",
            "Accept": "application/json;odata=nometadata"}


def test_connection(server: str, username: str, password: str,
                    database: str = "", tenant_id: str = "") -> dict:
    """Test connectivity to a SharePoint site. Returns the site title as
    server_version so the UI shows something meaningful."""
    try:
        site_url = _normalise_site_url(server)
        if not site_url:
            return {"success": False, "error": "SharePoint site URL is required"}
        token = get_access_token(tenant_id, username, password, site_url)
        resp = requests.get(f"{site_url}/_api/web?$select=Title,ServerRelativeUrl",
                            headers=_sp_headers(token), timeout=API_TIMEOUT)
        if resp.status_code in (401, 403):
            return {"success": False,
                    "error": f"Access denied by SharePoint (HTTP {resp.status_code}) — check that the app has Sites.Read.All / Sites.FullControl.All permission and admin consent."}
        if resp.status_code == 404:
            return {"success": False,
                    "error": f"Site not found at {site_url} — check the site URL."}
        resp.raise_for_status()
        web = resp.json()
        title = web.get("Title") or "Connected"
        return {"success": True, "server_version": f"SharePoint site '{title}'",
                "method": "sharepoint_rest"}
    except Exception as e:
        msg = str(e)
        hint = ""
        low = msg.lower()
        if "timeout" in low or "connection" in low and "refused" in low:
            hint = " — Cannot reach the site. Check the URL and network/firewall."
        elif "aadsts" in low:
            hint = " — Azure AD rejected the credentials. Verify Tenant ID, Client ID and Secret."
        logger.error("SharePoint connection test failed: %s", msg)
        return {"success": False, "error": msg + hint}


def load_objects(server: str, username: str, password: str,
                 database: str = "", tenant_id: str = "",
                 include_hidden: bool = False) -> dict:
    """Load SharePoint lists & document libraries as migratable objects.

    Each list is surfaced as a 'view' object whose code is a sample REST call,
    matching the grouped {stored_procedure, view, udf} contract used elsewhere."""
    try:
        site_url = _normalise_site_url(server)
        if not site_url:
            return {"success": False, "error": "SharePoint site URL is required"}
        token = get_access_token(tenant_id, username, password, site_url)

        select = "Title,ItemCount,BaseTemplate,Hidden"
        filt = "" if include_hidden else "&$filter=Hidden eq false"
        url = (f"{site_url}/_api/web/lists"
               f"?$select={select}&$top=500{filt}")
        items = []
        while url:
            resp = requests.get(url, headers=_sp_headers(token), timeout=API_TIMEOUT)
            if resp.status_code in (401, 403):
                return {"success": False, "error": f"Access denied listing lists (HTTP {resp.status_code})."}
            resp.raise_for_status()
            data = resp.json()
            items.extend(data.get("value", []))
            url = data.get("odata.nextLink") or ""

        grouped = {"stored_procedure": [], "view": [], "udf": []}
        for lst in items:
            title = lst.get("Title", "")
            item_count = int(lst.get("ItemCount") or 0)
            base_tpl = lst.get("BaseTemplate")
            kind = ("Document Library" if base_tpl == 101
                    else "List")
            safe_title = title.replace("'", "''")
            code = (
                f"-- SharePoint {kind}: {title}\n"
                f"-- Items: {item_count}\n"
                f"-- REST: GET {site_url}/_api/web/lists/getbytitle('{safe_title}')/items\n"
                f"-- Graph: GET https://graph.microsoft.com/v1.0/sites/<site-id>/lists"
            )
            grouped["view"].append({
                "key": f"sharepoint.{title}",
                "name": title,
                "description": f"{kind} ({item_count} items)",
                "code": code,
                "object_type": "view",
            })

        total = sum(len(v) for v in grouped.values())
        return {"success": True, "grouped": grouped, "total": total,
                "source_type": "sharepoint", "database": database}
    except Exception as e:
        logger.exception("Failed to load SharePoint objects")
        return {"success": False, "error": str(e)}
