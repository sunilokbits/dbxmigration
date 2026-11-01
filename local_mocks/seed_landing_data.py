"""Generate sample parquet files and upload them into the UC landing volume
so file-drop pipelines (Adobe, Price Navigator, SAP ECC/TMS/EHS) ingest real rows.

    python seed_landing_data.py
"""
import io
import random
from datetime import datetime, timedelta

import pandas as pd
from databricks.sdk import WorkspaceClient

VOL = "/Volumes/dev_volumes/default/landing"
random.seed(42)
NOW = datetime(2026, 10, 31)


def days_ago(n):
    return (NOW - timedelta(days=n)).strftime("%Y-%m-%d")


DATASETS = {
    "adobe/analytics_reports": pd.DataFrame({
        "report_id": range(1001, 1041),
        "report_date": [days_ago(i % 30) for i in range(40)],
        "page": [f"/products/item-{i%7}" for i in range(40)],
        "visits": [random.randint(100, 9000) for _ in range(40)],
        "unique_visitors": [random.randint(50, 5000) for _ in range(40)],
        "bounce_rate": [round(random.uniform(0.2, 0.8), 3) for _ in range(40)],
    }),
    "adobe/campaigns": pd.DataFrame({
        "campaign_id": [f"CMP-{i:04d}" for i in range(1, 26)],
        "name": [f"Campaign {i}" for i in range(1, 26)],
        "channel": [random.choice(["email", "social", "display", "search"]) for _ in range(25)],
        "start_date": [days_ago(random.randint(10, 90)) for _ in range(25)],
        "budget_usd": [random.randint(5000, 250000) for _ in range(25)],
        "status": [random.choice(["active", "paused", "completed"]) for _ in range(25)],
    }),
    "price_navigator/price_lists": pd.DataFrame({
        "price_list_id": range(1, 31),
        "sku": [f"SKU-{i:05d}" for i in range(1, 31)],
        "currency": [random.choice(["USD", "EUR", "GBP"]) for _ in range(30)],
        "list_price": [round(random.uniform(10, 2000), 2) for _ in range(30)],
        "effective_date": [days_ago(random.randint(0, 60)) for _ in range(30)],
    }),
    "price_navigator/products": pd.DataFrame({
        "product_id": [f"P-{i:04d}" for i in range(1, 31)],
        "name": [f"Product {i}" for i in range(1, 31)],
        "category": [random.choice(["hardware", "software", "services"]) for _ in range(30)],
        "uom": ["EA"] * 30,
    }),
    "sap_ecc/MARA": pd.DataFrame({
        "MATNR": [f"{i:018d}" for i in range(1, 51)],
        "MTART": [random.choice(["FERT", "ROH", "HALB"]) for _ in range(50)],
        "MATKL": [f"MG{random.randint(1,9):02d}" for _ in range(50)],
        "MEINS": ["EA"] * 50,
        "ERSDA": [days_ago(random.randint(0, 365)) for _ in range(50)],
    }),
    "sap_ecc/VBAK": pd.DataFrame({
        "VBELN": [f"{i:010d}" for i in range(1, 51)],
        "ERDAT": [days_ago(random.randint(0, 120)) for _ in range(50)],
        "AUART": [random.choice(["OR", "TA", "RE"]) for _ in range(50)],
        "NETWR": [round(random.uniform(100, 100000), 2) for _ in range(50)],
        "WAERK": ["USD"] * 50,
        "KUNNR": [f"{random.randint(1, 20):010d}" for _ in range(50)],
    }),
    "sap_ecc/BSEG": pd.DataFrame({
        "BELNR": [f"{i:010d}" for i in range(1, 51)],
        "BUZEI": [random.randint(1, 5) for _ in range(50)],
        "BSCHL": [random.choice(["40", "50", "01", "31"]) for _ in range(50)],
        "DMBTR": [round(random.uniform(10, 50000), 2) for _ in range(50)],
        "SHKZG": [random.choice(["S", "H"]) for _ in range(50)],
    }),
    "sap_tms/shipments": pd.DataFrame({
        "shipment_id": [f"SHP-{i:06d}" for i in range(1, 41)],
        "origin": [random.choice(["ATL", "ORD", "DFW", "LAX"]) for _ in range(40)],
        "destination": [random.choice(["JFK", "SEA", "MIA", "DEN"]) for _ in range(40)],
        "carrier": [random.choice(["UPS", "FedEx", "XPO", "JBHunt"]) for _ in range(40)],
        "ship_date": [days_ago(random.randint(0, 45)) for _ in range(40)],
        "weight_kg": [round(random.uniform(10, 2500), 1) for _ in range(40)],
    }),
    "sap_tms/freight_orders": pd.DataFrame({
        "order_id": [f"FO-{i:06d}" for i in range(1, 41)],
        "mode": [random.choice(["ROAD", "RAIL", "AIR", "OCEAN"]) for _ in range(40)],
        "cost_usd": [round(random.uniform(200, 40000), 2) for _ in range(40)],
        "status": [random.choice(["planned", "in_transit", "delivered"]) for _ in range(40)],
    }),
    "sap_ehs/incidents": pd.DataFrame({
        "incident_id": [f"INC-{i:05d}" for i in range(1, 31)],
        "site": [random.choice(["Plant A", "Plant B", "Warehouse 1"]) for _ in range(30)],
        "severity": [random.choice(["low", "medium", "high"]) for _ in range(30)],
        "reported_date": [days_ago(random.randint(0, 180)) for _ in range(30)],
        "status": [random.choice(["open", "investigating", "closed"]) for _ in range(30)],
    }),
    "sap_ehs/safety_data_sheets": pd.DataFrame({
        "sds_id": [f"SDS-{i:05d}" for i in range(1, 31)],
        "substance": [f"Substance {i}" for i in range(1, 31)],
        "hazard_class": [random.choice(["flammable", "corrosive", "toxic", "none"]) for _ in range(30)],
        "revision_date": [days_ago(random.randint(0, 400)) for _ in range(30)],
    }),
}

w = WorkspaceClient()
for rel, df in DATASETS.items():
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    path = f"{VOL}/{rel}/part-0000.parquet"
    w.files.upload(path, buf, overwrite=True)
    print(f"uploaded {path} ({len(df)} rows)")
print("done")
