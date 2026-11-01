"""Test all 17 enterprise source connections against the locally running app.

    python test_all_sources.py   (app on :5000, mock APIs on :9900)
"""
import json
import requests

APP = "http://localhost:5000/api/v1/source/test-connection"
MOCK = "http://127.0.0.1:9900"
VOL = "/Volumes/dev_volumes/default/landing"

def adls(st, method="adls_landing"):
    return {"source_type": st, "ingestion_method": method,
            "server": f"{VOL}/{st}", "file_format": "parquet"}

def api(st, method="rest_api"):
    return {"source_type": st, "ingestion_method": method,
            "server": f"{MOCK}/{st}", "api_auth_type": "none",
            "api_key_header": "", "username": "", "password": "", "database": ""}

def lakeflow(st, objects):
    return {"source_type": st, "ingestion_method": "lakeflow_connect",
            "server": st, "objects": objects}

TESTS = [
    ("1  SAP ECC",                adls("sap_ecc")),
    ("2  ServiceNow",             lakeflow("servicenow", "incident,sys_user,change_request")),
    ("3  SharePoint",             None),  # needs real Entra app + admin consent
    ("4  MS Dynamics CRM",        lakeflow("ms_dynamics_crm", "accounts,contacts,opportunities")),
    ("5  SuccessFactors",         api("successfactors")),
    ("6  MS Defender",            api("ms_defender")),
    ("7  Arkieva (JDBC)",         {"source_type": "arkieva", "ingestion_method": "jdbc",
                                   "engine": "sqlserver",
                                   "server": "poc-az-sqlserver-rksandbox-db.database.windows.net",
                                   "database": "master", "username": "sqladminuser", "password": ""}),
    ("8  Project44",              api("project44")),
    ("9  Outpost24",              api("outpost24")),
    ("10 Onapsis",                api("onapsis")),
    ("11 EPI Historian",          api("epi_historian", "streaming")),
    ("12 SAP Ariba",              api("sap_ariba")),
    ("13 Anaplan",                api("anaplan")),
    ("14 SAP TMS",                adls("sap_tms")),
    ("15 Price Navigator",        adls("price_navigator", "file")),
    ("16 SAP EHS",                adls("sap_ehs")),
    ("17 Adobe",                  adls("adobe", "file")),
]

passed = failed = skipped = 0
for label, payload in TESTS:
    if payload is None:
        print(f"SKIP {label:24s} -- needs a real Entra app registration (tenant/client/secret)")
        skipped += 1
        continue
    try:
        r = requests.post(APP, json=payload, timeout=120)
        d = r.json()
    except Exception as e:
        d = {"success": False, "error": str(e)[:200]}
    if d.get("success"):
        print(f"PASS {label:24s} -- {d.get('server_version','')[:90]}")
        passed += 1
    else:
        print(f"FAIL {label:24s} -- {d.get('error','')[:160]}")
        failed += 1

print(f"\n{passed} passed, {failed} failed, {skipped} skipped (SharePoint)")
