"""Probe Northwind OData V3 (SharePoint REST-style JSON): count + nextLink format."""
import requests

BASE = "https://services.odata.org/V3/Northwind/Northwind.svc"
for accept in ("application/json", "application/json;odata=nometadata", "application/json;odata=minimalmetadata"):
    r = requests.get(f"{BASE}/Customers", headers={"Accept": accept},
                     params={"$top": 1, "$inlinecount": "allpages"}, timeout=60)
    d = r.json() if r.ok else {}
    print(accept, r.status_code, {k: v for k, v in d.items() if k != "value"}, "cols",
          len((d.get("value") or [{}])[0]))
p = requests.get(f"{BASE}/Customers", headers={"Accept": "application/json"}, timeout=60).json()
print("page", len(p.get("value", [])), "next:", p.get("odata.nextLink"))
s = requests.get(f"{BASE}/Customers", headers={"Accept": "application/json"},
                 params={"$top": 1, "$count": "true"}, timeout=60)
print("$count=true on V3:", s.status_code, s.text[:120])
