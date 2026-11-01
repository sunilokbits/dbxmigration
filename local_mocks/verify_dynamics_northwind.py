"""Live check of the MS Dynamics CRM REST profile against the public Northwind
OData v4 service (No Auth): app-side stats + Extract-notebook nextLink paging."""
import os
import re
import sys
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "migration_utility"))

import enterprise_sources as es  # noqa: E402

BASE = "https://services.odata.org/V4/Northwind/Northwind.svc"
cfg = {"source_type": "ms_dynamics_crm", "ingestion_method": "rest_api", "server": BASE,
       "api_auth_type": "none", "objects": "Customers, Orders, Products, Order_Details"}

with mock.patch.object(es, "_saved_source", return_value={}):
    print("TEST   :", es.rest_profile_test(cfg))
    t = es.rest_profile_tables(cfg)
for x in t["tables"]:
    print(f"TABLE  : {x['full_name']:32s} cols={x['col_count']:3d} rows={x['row_estimate']}")

# Run the notebook's API block (paging helpers) against the live service
import source_registry  # noqa: E402
from metadata_notebooks import generate_metadata_notebooks  # noqa: E402
nbs = generate_metadata_notebooks(catalog="c", schema="s", landing_path="/Volumes/x/y/landing",
                                  pipeline_mode="dlt")["notebooks"]
text = next(n for n in nbs if n["name"] == "01_Meta_Extract")["code"]
start = text.index("if IS_API_SOURCE:\n    import requests as _requests")
end = text.index("def _read_source(")
block = text[start:end]
import base64, requests  # noqa: E401,E402
for table, expected in [(x["table"], x["row_estimate"]) for x in t["tables"]]:
    g = {"IS_API_SOURCE": True, "SERVER": BASE, "USERNAME": "", "PASSWORD": "", "TABLE_NAME": table,
         "base64": base64, "_re": re, "dbutils": None,
         "source_config": {"api_auth_type": "none", "api_profile": source_registry.rest_profile("ms_dynamics_crm")}}
    exec(block, g)
    recs = g["_api_records"](g["_api_endpoint_for_table"]())
    print(f"EXTRACT: {table:14s} records={len(recs):5d} expected={expected:5d} {'OK' if len(recs) == expected else 'MISMATCH'}")
