"""Probe the public Northwind OData v4 service: count endpoint, page size, nextLink format."""
import requests

BASE = "https://services.odata.org/V4/Northwind/Northwind.svc"
h = {"Accept": "application/json"}
for t in ("Customers", "Orders", "Products", "Order_Details"):
    s = requests.get(f"{BASE}/{t}", headers=h, params={"$top": 1, "$count": "true"}, timeout=60).json()
    print(f"{t:14s} @odata.count={s.get('@odata.count')} cols={len((s.get('value') or [{}])[0])}")
root = requests.get(BASE, headers=h, timeout=60)
print("root:", root.status_code, root.headers.get("Content-Type"))
