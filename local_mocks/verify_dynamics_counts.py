"""Row counts of the Dynamics (Northwind) tables in Bronze and Silver."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "migration_utility"))
from dbsql_client import execute_query  # noqa: E402

EXPECTED = {"customers": 91, "orders": 830, "products": 77, "order_details": 2155}
for cat in ("dbx_bronze", "dbx_silver"):
    names = {r.get("tableName") or r.get("tablename") for r in execute_query(f"SHOW TABLES IN {cat}.dynamics_crm")}
    for t, exp in EXPECTED.items():
        if t not in names:
            print(f"{cat}.dynamics_crm.{t:14s} MISSING  (have: {sorted(names)})")
            continue
        n = int(next(iter(execute_query(f"SELECT COUNT(*) AS c FROM {cat}.dynamics_crm.{t}")[0].values())))
        print(f"{cat}.dynamics_crm.{t:14s} {n:>5}  expected {exp:>5}  {'OK' if n == exp else 'MISMATCH'}")
