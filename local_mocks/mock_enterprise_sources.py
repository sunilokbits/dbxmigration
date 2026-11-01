"""Local mock server that impersonates the 9 REST/streaming enterprise
source systems so Migration Studio's "Test Connection" and "Load Objects"
can run without the real systems.

    python mock_enterprise_sources.py          # listens on http://127.0.0.1:9900

Point each system's Base URL at http://127.0.0.1:9900/<system_key>, e.g.
    http://127.0.0.1:9900/successfactors
"""
from flask import Flask, jsonify, request

app = Flask(__name__)

# system key -> (server header, [(GET path, summary), ...])
SYSTEMS = {
    "successfactors": ("SAP-SuccessFactors-OData", [
        ("/odata/v2/User", "Employee master records"),
        ("/odata/v2/EmpJob", "Job information"),
        ("/odata/v2/EmpCompensation", "Compensation records"),
        ("/odata/v2/PerPersonal", "Personal information"),
    ]),
    "ms_defender": ("MS-Defender-for-Endpoint-API", [
        ("/api/machines", "Onboarded devices"),
        ("/api/alerts", "Security alerts"),
        ("/api/vulnerabilities", "Vulnerability inventory"),
        ("/api/recommendations", "Security recommendations"),
    ]),
    "project44": ("project44-Movement-API", [
        ("/api/v4/shipments", "Shipment tracking"),
        ("/api/v4/locations", "Location master"),
        ("/api/v4/carriers", "Carrier network"),
        ("/api/v4/eta", "Predictive ETAs"),
    ]),
    "outpost24": ("Outpost24-REST", [
        ("/rest/assets", "Scanned assets"),
        ("/rest/findings", "Vulnerability findings"),
        ("/rest/scans", "Scan jobs"),
    ]),
    "onapsis": ("Onapsis-Platform-API", [
        ("/api/assets", "SAP system inventory"),
        ("/api/issues", "Security issues"),
        ("/api/assessments", "Assessment runs"),
    ]),
    "epi_historian": ("EPI-Historian-Streaming", [
        ("/api/tags", "Historian tag catalogue"),
        ("/api/timeseries", "Time-series readings"),
        ("/api/events", "Process events stream"),
    ]),
    "sap_ariba": ("SAP-Ariba-OpenAPI", [
        ("/api/sourcing/v2/events", "Sourcing events"),
        ("/api/procurement/v2/orders", "Purchase orders"),
        ("/api/supplier/v4/suppliers", "Supplier master"),
    ]),
    "anaplan": ("Anaplan-Integration-API", [
        ("/api/2/0/workspaces", "Workspaces"),
        ("/api/2/0/models", "Planning models"),
        ("/api/2/0/exports", "Export definitions"),
    ]),
    "price_navigator": ("PriceNavigator-API", [
        ("/api/prices", "Price lists"),
        ("/api/products", "Product catalogue"),
    ]),
    "adobe": ("Adobe-Experience-API", [
        ("/api/analytics/reports", "Analytics reports"),
        ("/api/campaigns", "Campaign data"),
    ]),
}


def _spec(system, paths):
    return {
        "openapi": "3.0.0",
        "info": {"title": f"{system} (mock)", "version": "1.0.0"},
        "paths": {p: {"get": {"summary": s, "responses": {"200": {"description": "OK"}}}}
                  for p, s in paths},
    }


@app.route("/<system>", methods=["GET"])
@app.route("/<system>/", methods=["GET"])
def root(system):
    if system not in SYSTEMS:
        return jsonify({"error": "unknown mock system"}), 404
    header, paths = SYSTEMS[system]
    resp = jsonify({"system": system, "status": "ok", "mock": True,
                    "endpoints": [p for p, _ in paths]})
    resp.headers["Server"] = header
    return resp


@app.route("/<system>/openapi.json", methods=["GET"])
def openapi(system):
    if system not in SYSTEMS:
        return jsonify({"error": "unknown mock system"}), 404
    header, paths = SYSTEMS[system]
    resp = jsonify(_spec(system, paths))
    resp.headers["Server"] = header
    return resp


@app.route("/<system>/<path:rest>", methods=["GET"])
def data(system, rest):
    if system not in SYSTEMS:
        return jsonify({"error": "unknown mock system"}), 404
    entity = rest.rstrip("/").rsplit("/", 1)[-1] or "record"
    items = [{
        "id": i,
        "name": f"{entity}-{i:03d}",
        "status": ["active", "inactive", "pending"][i % 3],
        "created_date": f"2026-10-{(i % 28) + 1:02d}",
        "amount": round(100.0 + i * 37.5, 2),
        "source_system": system,
    } for i in range(1, 26)]
    return jsonify({"system": system, "path": "/" + rest, "mock": True, "items": items})


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=9900, debug=False)
