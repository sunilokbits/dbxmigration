"""Simulate the production integration job: pull records from a REST API
source and land them as parquet in the UC landing volume, where Migration
Studio's file-drop extract path takes over (API -> landing -> Bronze -> Silver).

    python land_api_data.py successfactors            # one system
    python land_api_data.py all                       # every mock API system
"""
import io
import sys

import pandas as pd
import requests
from databricks.sdk import WorkspaceClient

MOCK = "http://127.0.0.1:9900"
VOL = "/Volumes/dev_volumes/default/landing"


def land(system: str, w: WorkspaceClient) -> None:
    spec = requests.get(f"{MOCK}/{system}/openapi.json", timeout=20).json()
    paths = [p for p, m in spec.get("paths", {}).items() if "get" in m]
    if not paths:
        print(f"{system}: no GET endpoints in spec")
        return
    for path in paths:
        doc = requests.get(f"{MOCK}/{system}{path}", timeout=20).json()
        items = doc.get("items") or []
        if not items:
            print(f"{system}{path}: no records")
            continue
        entity = path.rstrip("/").rsplit("/", 1)[-1]
        buf = io.BytesIO()
        pd.DataFrame(items).to_parquet(buf, index=False)
        buf.seek(0)
        target = f"{VOL}/{system}/{entity}/part-0000.parquet"
        w.files.upload(target, buf, overwrite=True)
        print(f"landed {target} ({len(items)} rows)")


if __name__ == "__main__":
    which = (sys.argv[1] if len(sys.argv) > 1 else "successfactors").lower()
    systems = (["successfactors", "ms_defender", "project44", "outpost24", "onapsis",
                "epi_historian", "sap_ariba", "anaplan"] if which == "all" else [which])
    w = WorkspaceClient()
    for s in systems:
        land(s, w)
