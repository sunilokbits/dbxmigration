"""Store ServiceNow OAuth client creds in the secret scope, get a token, and
test the live Table API.

    python servicenow_oauth_test.py          # prompts for client id + secret
    python servicenow_oauth_test.py --test   # reuse stored creds
"""
import base64
import getpass
import sys

import requests
from databricks.sdk import WorkspaceClient

INSTANCE = "https://dev405660.service-now.com"
SCOPE = "migration-studio"
TABLES = ("incident", "sys_user", "change_request")

w = WorkspaceClient()


def secret(key):
    return base64.b64decode(w.secrets.get_secret(SCOPE, key).value).decode()


if "--test" not in sys.argv:
    cid = input(">>> Paste ServiceNow Client ID, then Enter: ").strip()
    csec = getpass.getpass(">>> Paste ServiceNow Client Secret (hidden), then Enter: ").strip()
    w.secrets.put_secret(SCOPE, "source-servicenow-client-id", string_value=cid)
    w.secrets.put_secret(SCOPE, "source-servicenow-client-secret", string_value=csec)
    print(f"Stored client id ({len(cid)} chars) and secret ({len(csec)} chars)")

cid = secret("source-servicenow-client-id")
csec = secret("source-servicenow-client-secret")
pwd = secret("source-servicenow-password")

token = None
for grant in ("password", "client_credentials"):
    data = {"grant_type": grant, "client_id": cid, "client_secret": csec, "scope": "useraccount"}
    if grant == "password":
        data.update({"username": "admin", "password": pwd})
    r = requests.post(f"{INSTANCE}/oauth_token.do", data=data, timeout=60)
    ok = r.ok and "access_token" in r.text
    print(f"Token via {grant:18s} -> HTTP {r.status_code} {'OK' if ok else r.text[:160]}")
    if ok:
        token = r.json()["access_token"]
        break

if not token:
    sys.exit("No OAuth token obtained.")

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(total=3, connect=3, read=3, backoff_factor=3)))

for tbl in TABLES:
    try:
        r = sess.get(f"{INSTANCE}/api/now/table/{tbl}", params={"sysparm_limit": 5},
                     headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                     timeout=(30, 120))
    except requests.RequestException as e:
        print(f"{tbl:15s} -> network error: {type(e).__name__}")
        continue
    rows = r.json().get("result", []) if r.ok else []
    cols = len(rows[0]) if rows else 0
    detail = "" if r.ok else f" | {r.text[:300]}"
    print(f"{tbl:15s} -> HTTP {r.status_code} | sample rows: {len(rows)} | cols: {cols}{detail}")
