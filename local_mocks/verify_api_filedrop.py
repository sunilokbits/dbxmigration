import requests

BASE = "http://localhost:5000"
SYSTEMS = ["successfactors", "ms_defender", "project44", "outpost24",
           "onapsis", "epi_historian", "sap_ariba", "anaplan"]

for s in SYSTEMS:
    r = requests.post(BASE + "/api/v1/workflow/list-tables", json={
        "source_type": s, "ingestion_method": "file", "file_format": "parquet",
        "server": f"/Volumes/dev_volumes/default/landing/{s}",
        "database": "", "username": ""}, timeout=180)
    d = r.json()
    if d.get("success"):
        ts = ", ".join(f"{t['table']}({t['row_estimate']}r/{t['col_count']}c)" for t in d["tables"])
        print(f"{s:15s} OK  {d['total']} tables: {ts}")
    else:
        print(f"{s:15s} FAIL {d.get('error', '')[:120]}")
