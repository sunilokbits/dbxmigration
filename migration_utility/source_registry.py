"""
Single catalogue of every source system Migration Studio can connect to.

Every screen, route, secret and notebook resolves a source through this module
instead of comparing source_type strings ad hoc. Two layers:

  * source_type -- WHAT the system is (e.g. "sap_ecc", "servicenow").
  * connector   -- HOW the app talks to it. The seven built-in types are
    their own connector, so their behaviour is unchanged; enterprise
    systems map onto a connector through their ingestion method:

      jdbc             -> the chosen database engine (sqlserver / azuresql)
      adls_landing     -> "adls"     (SNP Glue writes files to ADLS)
      file             -> "adls"     (files dropped in ADLS / a UC Volume)
      rest_api         -> "api"
      streaming        -> "api"
      lakeflow_connect -> "lakeflow" (Databricks managed connector)

Unknown types resolve to the "unknown" connector so they can never silently
fall through to the SQL Server code path.
"""

METHODS = {
    "jdbc":             {"label": "JDBC connector",             "connector": None,       "extract": True},
    "adls_landing":     {"label": "SNP Glue → ADLS landing",    "connector": "adls",     "extract": True},
    "file":             {"label": "File drop (ADLS / Volume)",  "connector": "adls",     "extract": True},
    "rest_api":         {"label": "REST API",                   "connector": "api",      "extract": False},
    "streaming":        {"label": "API / streaming",            "connector": "api",      "extract": False},
    "lakeflow_connect": {"label": "Lakeflow Connect",           "connector": "lakeflow", "extract": False},
}

# Database engines a "jdbc" enterprise source can run on. Limited to the SQL
# Server family: their connection fields and Extract path already exist.
JDBC_ENGINES = {"sqlserver": "SQL Server", "azuresql": "Azure SQL"}

FILE_FORMATS = ("parquet", "delta", "csv", "json")

CATEGORIES = {
    "database":  "Databases & warehouses",
    "sap":       "SAP (SNP Glue → ADLS)",
    "lakeflow":  "Lakeflow Connect",
    "saas":      "SaaS & APIs",
    "files":     "Collaboration & files",
}

# Built-in connectors -- behaviour unchanged. secret_key values are the
# historical keys already used in the secret scope; never rename them.
BUILTIN = {
    "azuresql":   {"label": "Azure SQL",     "icon": "🔷", "category": "database", "secret_key": "source-azuresql-password"},
    "sqlserver":  {"label": "SQL Server",    "icon": "🗄️", "category": "database", "secret_key": "source-sql-password"},
    "synapse":    {"label": "Azure Synapse", "icon": "🟪", "category": "database", "secret_key": "source-synapse-password"},
    "snowflake":  {"label": "Snowflake",     "icon": "❄️", "category": "database", "secret_key": "source-snowflake-password"},
    "redshift":   {"label": "Redshift",      "icon": "🟥", "category": "database", "secret_key": "source-redshift-password"},
    "sharepoint": {"label": "SharePoint",    "icon": "📄", "category": "files",    "secret_key": "source-sharepoint-password"},
    "api":        {"label": "REST API",      "icon": "🌐", "category": "saas",     "secret_key": "source-api-password"},
}


def _rfp(no, objects, volume, change, cdc, frequency, ingestion, complexity):
    return {"no": no, "objects": objects, "volume": volume, "change": change, "cdc": cdc,
            "frequency": frequency, "ingestion": ingestion, "complexity": complexity}


_SNP = "SNP Glue (transactional) + direct connection (static) → ADLS (retained)"

# The 17 Project Bedrock Phase I (NRY-RFP-2026-BEDROCK-P1) source systems.
# Microsoft SharePoint (#3) reuses the built-in "sharepoint" connector and
# keeps its existing secret; the other 16 each get their own secret key.
ENTERPRISE = {
    "sap_ecc": {"label": "SAP ECC", "icon": "🟦", "category": "sap", "methods": ["adls_landing"],
                "rfp": _rfp(1, "500", "2 TB", "High", True, "10 min", _SNP, "High")},
    "servicenow": {"label": "ServiceNow", "icon": "🟩", "category": "lakeflow", "methods": ["lakeflow_connect"],
                   "rfp": _rfp(2, "50", "500 GB", "High", True, "1 hour", "Lakeflow Connect", "Medium")},
    "ms_dynamics_crm": {"label": "MS Dynamics CRM", "icon": "🟪", "category": "lakeflow", "methods": ["lakeflow_connect"],
                        "rfp": _rfp(4, "200", "500 GB", "High", True, "1 hour", "Lakeflow Connect", "Medium")},
    "successfactors": {"label": "SuccessFactors", "icon": "👥", "category": "saas", "methods": ["rest_api"],
                       "rfp": _rfp(5, "100", "100 GB", "Medium", True, "1 hour", "API", "Medium")},
    "ms_defender": {"label": "MS Defender for Endpoint", "icon": "🛡️", "category": "saas", "methods": ["rest_api"],
                    "rfp": _rfp(6, "100", "100 GB", "Medium", True, "1 day", "API", "Medium")},
    "arkieva": {"label": "Arkieva (via NDP)", "icon": "📈", "category": "database", "methods": ["jdbc"],
                "engine": "sqlserver",
                "rfp": _rfp(7, "200", "500 GB", "Medium", False, "12× per day", "JDBC connector", "High")},
    "project44": {"label": "Project44", "icon": "🚚", "category": "saas", "methods": ["rest_api"],
                  "rfp": _rfp(8, "100", "50 GB", "High", True, "1 hour", "API", "High")},
    "outpost24": {"label": "Outpost24", "icon": "🔐", "category": "saas", "methods": ["rest_api"],
                  "rfp": _rfp(9, "100", "50 GB", "Low", False, "1 day", "API", "Medium")},
    "onapsis": {"label": "Onapsis", "icon": "🧰", "category": "saas", "methods": ["rest_api"],
                "rfp": _rfp(10, "100", "50 GB", "Low", False, "1 day", "API", "Medium")},
    "epi_historian": {"label": "EPI Historian", "icon": "📡", "category": "saas", "methods": ["streaming"],
                      "rfp": _rfp(11, "TBD", "5 TB*", "High", True, "5 min", "API / streaming", "High")},
    "sap_ariba": {"label": "SAP Ariba", "icon": "🛒", "category": "saas", "methods": ["rest_api"],
                  "rfp": _rfp(12, "100", "100 GB", "High", True, "1 hour", "API", "High")},
    "anaplan": {"label": "Anaplan", "icon": "📊", "category": "saas", "methods": ["rest_api"],
                "rfp": _rfp(13, "50", "500 GB", "High", False, "1 hour", "API", "High")},
    "sap_tms": {"label": "SAP TMS", "icon": "🟦", "category": "sap", "methods": ["adls_landing"],
                "rfp": _rfp(14, "100", "100 GB", "High", True, "10 min", _SNP, "Medium")},
    "price_navigator": {"label": "Price Navigator", "icon": "💲", "category": "files", "methods": ["file", "rest_api"],
                        "rfp": _rfp(15, "10", "50 GB", "Low", False, "1 day", "API / file-based", "Medium")},
    "sap_ehs": {"label": "SAP EHS", "icon": "🟦", "category": "sap", "methods": ["adls_landing"],
                "rfp": _rfp(16, "200", "50 GB", "Medium", False, "1 day", _SNP, "Medium")},
    "adobe": {"label": "Adobe", "icon": "🅰️", "category": "files", "methods": ["file", "rest_api"],
              "rfp": _rfp(17, "200", "50 GB", "Medium", False, "1 day", "API / file-based", "Medium")},
}

SHAREPOINT_RFP = _rfp(3, "100", "500 GB", "Low", False, "1 hour", "To be proposed by bidder", "Medium")


def _secret_key_for(source_type: str) -> str:
    return f"source-{source_type.replace('_', '-')}-password"


def get(source_type: str):
    """Registry entry for a source type (built-in or enterprise), else None."""
    if source_type in BUILTIN:
        return dict(BUILTIN[source_type], key=source_type, builtin=True)
    if source_type in ENTERPRISE:
        e = ENTERPRISE[source_type]
        return dict(e, key=source_type, builtin=False, secret_key=_secret_key_for(source_type))
    return None


def method_for(cfg: dict) -> str:
    """Ingestion method chosen for an enterprise source (its default if unset/invalid)."""
    entry = ENTERPRISE.get((cfg or {}).get("source_type", ""))
    if not entry:
        return ""
    chosen = (cfg or {}).get("ingestion_method", "")
    return chosen if chosen in entry["methods"] else entry["methods"][0]


def connector_type(cfg) -> str:
    """How the app talks to the source. Accepts a source_config dict or a type string."""
    cfg = {"source_type": cfg} if isinstance(cfg, str) else (cfg or {})
    st = cfg.get("source_type") or "sqlserver"
    if st in BUILTIN:
        return st
    if st not in ENTERPRISE:
        return "unknown"
    method = method_for(cfg)
    if method == "jdbc":
        engine = cfg.get("engine") or ENTERPRISE[st].get("engine", "sqlserver")
        return engine if engine in JDBC_ENGINES else "sqlserver"
    return METHODS[method]["connector"]


def can_extract(cfg: dict):
    """(ok, reason). Built-in types keep their existing behaviour (always allowed)."""
    st = (cfg or {}).get("source_type") or "sqlserver"
    if st in BUILTIN:
        return True, ""
    if st not in ENTERPRISE:
        return False, f"Unknown source type '{st}'."
    method = method_for(cfg)
    if METHODS[method]["extract"]:
        return True, ""
    return False, (f"{ENTERPRISE[st]['label']} via {METHODS[method]['label']} can be configured and tested, "
                   "but automated extraction for this ingestion method is planned and not available yet.")


def secret_key(source_type: str) -> str:
    """Secret-scope key holding this source's password / API secret. Every
    enterprise source has its own key; unknown types keep the historical
    SQL Server fallback."""
    if source_type in BUILTIN:
        return BUILTIN[source_type]["secret_key"]
    if source_type in ENTERPRISE:
        return _secret_key_for(source_type)
    return "source-sql-password"


def all_secret_keys() -> dict:
    """{secret_key: description} for every source (used to whitelist Settings writes)."""
    keys = {v["secret_key"]: f"{v['label']} source password" for v in BUILTIN.values()}
    keys.update({_secret_key_for(k): f"{v['label']} source secret" for k, v in ENTERPRISE.items()})
    return keys


def readiness(source_type: str, method: str = "") -> str:
    """'ready' (data flows end to end), 'configure' (connect + test only) or 'planned'."""
    if source_type in BUILTIN:
        return "ready"
    entry = ENTERPRISE.get(source_type)
    if not entry:
        return "planned"
    m = method if method in entry["methods"] else entry["methods"][0]
    if METHODS[m]["extract"]:
        return "ready"
    return "planned" if m == "streaming" else "configure"


def client_payload() -> dict:
    """Everything the frontend needs, as plain JSON (no secrets)."""
    sources = []
    for k, v in BUILTIN.items():
        sources.append({"key": k, "label": v["label"], "icon": v["icon"], "category": v["category"],
                        "builtin": True, "methods": [], "connector": k, "secret_key": v["secret_key"],
                        "readiness": "ready", "rfp": SHAREPOINT_RFP if k == "sharepoint" else None})
    for k, v in ENTERPRISE.items():
        sources.append({"key": k, "label": v["label"], "icon": v["icon"], "category": v["category"],
                        "builtin": False, "methods": list(v["methods"]), "engine": v.get("engine", ""),
                        "connector": connector_type(k), "secret_key": _secret_key_for(k),
                        "readiness": {m: readiness(k, m) for m in v["methods"]}, "rfp": v["rfp"]})
    return {
        "categories": CATEGORIES,
        "methods": {k: {"label": v["label"], "connector": v["connector"], "extract": v["extract"]}
                    for k, v in METHODS.items()},
        "jdbc_engines": JDBC_ENGINES,
        "file_formats": list(FILE_FORMATS),
        "sources": sources,
    }
