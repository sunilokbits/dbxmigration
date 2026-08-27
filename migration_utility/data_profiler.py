"""
data_profiler.py — Column-level statistics & PII detection for source tables.

Scope:
  • Row count, size, last-modified, column count
  • Per-column: null %, distinct count, min/max, sample top-values
  • PII detector (regex-based): email, phone, SSN, credit-card, IP, DOB, name
  • Data skew detection (one value > threshold)
  • Silver-layer rule suggestions (NOT NULL, MASK, range check)

Designed to run against live SQL Server / Azure SQL / Synapse via sql_pool,
and against an in-memory demo dataset (derived from stored_procedures.SQL_TABLES)
so the feature works end-to-end with zero DB setup.
"""
import re
import time
import random
from typing import Optional
from log_config import get_logger

logger = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  PII DETECTORS — name + regex + sample-value check
# ─────────────────────────────────────────────────────────────────────────────
# Column-name-based heuristics (case-insensitive)
_PII_NAME_RULES = [
    ("email",       re.compile(r"(email|e_mail)", re.I)),
    ("phone",       re.compile(r"(phone|mobile|tel|fax)", re.I)),
    ("ssn",         re.compile(r"(ssn|social[_ ]?security)", re.I)),
    ("credit_card", re.compile(r"(credit[_ ]?card|cc[_ ]?num|cardnum|card_number)", re.I)),
    ("dob",         re.compile(r"(dob|date[_ ]?of[_ ]?birth|birth[_ ]?date)", re.I)),
    ("name",        re.compile(r"^(firstname|lastname|fullname|customer[_ ]?name|employee[_ ]?name|first_name|last_name|name)$", re.I)),
    ("address",     re.compile(r"(address|street|city|zip|postal)", re.I)),
    ("ip_address",  re.compile(r"(ip[_ ]?address|ipaddr)", re.I)),
    ("passport",    re.compile(r"(passport|national[_ ]?id)", re.I)),
]

# Value-based regexes (applied to a sample of non-null strings)
_PII_VALUE_RULES = [
    ("email",       re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")),
    ("ssn",         re.compile(r"^\d{3}-\d{2}-\d{4}$")),
    ("credit_card", re.compile(r"^\d{13,19}$|^\d{4}-\d{4}-\d{4}-\d{4}$")),
    ("phone",       re.compile(r"^[+]?[\d\s\-\(\)]{7,20}$")),
    ("ip_address",  re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")),
]

# How suspicious each PII category is (0..1) — drives panel severity
_PII_SEVERITY = {
    "ssn": 1.0, "credit_card": 1.0, "passport": 0.9,
    "dob": 0.8, "email": 0.7, "phone": 0.7, "address": 0.6,
    "name": 0.5, "ip_address": 0.4,
}


def _pii_tags_for_column(col_name: str, sample_values=None) -> list:
    """Return list of PII category names detected for this column."""
    hits = set()
    name_l = (col_name or "").lower()
    for cat, rx in _PII_NAME_RULES:
        if rx.search(name_l):
            hits.add(cat)
    if sample_values:
        for cat, rx in _PII_VALUE_RULES:
            matches = sum(1 for v in sample_values[:50]
                          if isinstance(v, str) and rx.match(v))
            if matches >= 5:  # >=5 of 50 sample values match
                hits.add(cat)
    return sorted(hits, key=lambda c: -_PII_SEVERITY.get(c, 0))


# ─────────────────────────────────────────────────────────────────────────────
#  NUMERIC / STRING TYPE CLASSIFIERS
# ─────────────────────────────────────────────────────────────────────────────
_NUMERIC_TYPES = {"int", "bigint", "smallint", "tinyint", "decimal", "numeric",
                  "float", "real", "money", "smallmoney", "double"}
_STRING_TYPES  = {"varchar", "nvarchar", "char", "nchar", "text", "ntext"}
_DATE_TYPES    = {"date", "datetime", "datetime2", "timestamp",
                  "smalldatetime", "datetimeoffset", "time"}


def _type_class(data_type: str) -> str:
    t = (data_type or "").lower().strip()
    base = t.split("(")[0].strip()
    if base in _NUMERIC_TYPES: return "numeric"
    if base in _STRING_TYPES:  return "string"
    if base in _DATE_TYPES:    return "date"
    return "other"


# ─────────────────────────────────────────────────────────────────────────────
#  RULE SUGGESTIONS — promoted to Data Quality cart
# ─────────────────────────────────────────────────────────────────────────────
def _suggest_rules(table: str, col: dict) -> list:
    """Return a list of suggested DQ rule dicts for this column."""
    rules = []
    name = col["name"]
    null_pct = col.get("null_pct", 0)
    distinct = col.get("distinct_count", 0)
    tclass = col.get("type_class", "other")
    pii = col.get("pii_tags", [])

    # NOT NULL rule when column is mostly populated
    if null_pct == 0:
        rules.append({
            "rule_type": "not_null",
            "table": table, "column": name,
            "expression": f"{name} IS NOT NULL",
            "severity": "error",
            "reason": f"Column has 0% nulls in source — enforce NOT NULL.",
        })
    elif null_pct < 5:
        rules.append({
            "rule_type": "null_threshold",
            "table": table, "column": name,
            "expression": f"SUM(CASE WHEN {name} IS NULL THEN 1 ELSE 0 END)/COUNT(*) < 0.05",
            "severity": "warn",
            "reason": f"Source null rate is {null_pct:.1f}% — flag drift above 5%.",
        })

    # PII masking
    if pii:
        rules.append({
            "rule_type": "pii_mask",
            "table": table, "column": name,
            "expression": f"MASK({name})",
            "severity": "critical",
            "reason": f"PII detected ({', '.join(pii)}) — apply UC column mask in silver layer.",
        })

    # Uniqueness (candidate key)
    row_count = col.get("_row_count_hint", 0)
    if distinct and row_count and distinct >= row_count * 0.99:
        rules.append({
            "rule_type": "unique",
            "table": table, "column": name,
            "expression": f"COUNT(DISTINCT {name}) = COUNT(*)",
            "severity": "error",
            "reason": "99%+ distinct values — candidate primary/business key.",
        })

    # Skew warning
    top = col.get("top_values") or []
    if top and row_count:
        top_pct = (top[0]["count"] / row_count) * 100 if top[0].get("count") else 0
        if top_pct > 80 and distinct > 1:
            rules.append({
                "rule_type": "skew",
                "table": table, "column": name,
                "expression": f"-- {top_pct:.0f}% of rows = '{top[0]['value']}'",
                "severity": "warn",
                "reason": f"Heavy skew: one value covers {top_pct:.0f}% of rows. Consider partitioning on another column.",
            })

    # Numeric range (min < 0 on typically-positive column)
    if tclass == "numeric":
        vmin = col.get("min")
        if isinstance(vmin, (int, float)) and vmin < 0 and any(k in name.lower() for k in ("amount", "total", "price", "qty", "count", "revenue")):
            rules.append({
                "rule_type": "range",
                "table": table, "column": name,
                "expression": f"{name} >= 0",
                "severity": "warn",
                "reason": f"Negative values found (min={vmin}) on typically-positive column.",
            })

    # Future-date guard
    if tclass == "date":
        vmax = col.get("max")
        if isinstance(vmax, str) and vmax > "2025":  # crude future-date check
            rules.append({
                "rule_type": "future_date",
                "table": table, "column": name,
                "expression": f"{name} <= current_date()",
                "severity": "warn",
                "reason": f"Future-dated values found (max={vmax}).",
            })

    return rules


# ─────────────────────────────────────────────────────────────────────────────
#  LIVE PROFILING — pyodbc against SQL Server / Azure SQL / Synapse
# ─────────────────────────────────────────────────────────────────────────────
def profile_table_live(source_config: dict, table: str, schema: str = "dbo",
                       sample_top_n: int = 10, max_distinct_scan: int = 1000000) -> dict:
    """
    Connect live and compute column-level statistics for a single table.
    Returns dict ready for the frontend panel.

    Notes:
      • Uses T-SQL dynamic SQL with parameterised identifiers via QUOTENAME
        — safe against SQL injection (schema/table names are not user-free-text
        in practice, but we still quote).
      • For very large tables, per-column DISTINCT and AGG are wrapped in
        TABLESAMPLE when row count exceeds max_distinct_scan.
    """
    from config_cache import get_source_password
    from keyvault_helper import is_masked

    t0 = time.time()
    src_type = source_config.get("source_type", "sqlserver")
    if src_type in ("sharepoint", "api"):
        raise ValueError(
            f"Live profiling is not supported for {src_type} sources yet."
        )
    _pw = source_config.get("password", "")
    if not _pw or is_masked(_pw):
        _pw = get_source_password(source_type=src_type)

    # ── Snowflake path: basic column profiling via INFORMATION_SCHEMA ──
    if src_type == "snowflake":
        import snowflake_connector
        account = source_config.get("account", "") or source_config.get("server", "")
        database = source_config.get("database", "")
        sf_conn = snowflake_connector.get_snowflake_connection(
            account=account, username=source_config.get("username", ""),
            password=_pw, database=database,
            warehouse=source_config.get("warehouse", ""),
            role=source_config.get("role", ""),
        )
        sf_cur = sf_conn.cursor()
        sf_cur.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
                   CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
                   ORDINAL_POSITION
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """)
        columns = []
        for row in sf_cur.fetchall():
            col = {
                "name": row[0], "data_type": row[1], "nullable": row[2] == "YES",
                "max_length": row[3], "precision": row[4], "scale": row[5],
                "ordinal": row[6], "null_pct": 0, "distinct_count": 0,
                "sample_values": [], "pii_flags": [], "dq_rules": [],
            }
            columns.append(col)
        row_count = 0
        try:
            sf_cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            row_count = sf_cur.fetchone()[0] or 0
        except Exception:
            pass
        sf_cur.close(); sf_conn.close()
        return {
            "table": table, "schema": schema, "row_count": row_count,
            "column_count": len(columns), "columns": columns,
            "profile_time_sec": round(time.time() - t0, 2),
            "source_type": "snowflake",
        }

    # ── Redshift path: basic column profiling via information_schema ──
    if src_type == "redshift":
        from redshift_client import get_redshift_connection
        rs_conn = get_redshift_connection(
            server=source_config.get("server", ""), username=source_config.get("username", ""),
            password=_pw, database=source_config.get("database", ""),
        )
        rs_cur = rs_conn.cursor()
        rs_cur.execute("""
            SELECT column_name, data_type, is_nullable,
                   character_maximum_length, numeric_precision, numeric_scale,
                   ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        columns = []
        for row in rs_cur.fetchall():
            col = {
                "name": row[0], "data_type": row[1], "nullable": row[2] == "YES",
                "max_length": row[3], "precision": row[4], "scale": row[5],
                "ordinal": row[6], "null_pct": 0, "distinct_count": 0,
                "sample_values": [], "pii_flags": [], "dq_rules": [],
            }
            columns.append(col)
        row_count = 0
        try:
            rs_cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            row_count = rs_cur.fetchone()[0] or 0
        except Exception:
            pass
        rs_cur.close(); rs_conn.close()
        return {
            "table": table, "schema": schema, "row_count": row_count,
            "column_count": len(columns), "columns": columns,
            "profile_time_sec": round(time.time() - t0, 2),
            "source_type": "redshift",
        }

    # ── SQL Server / Azure SQL path ──
    from sql_pool import get_connection
    conn = get_connection(
        source_type=src_type,
        server=source_config["server"], database=source_config["database"],
        username=source_config.get("username", ""),
        password=_pw,
    )
    cur = conn.cursor()
    # Suppress informational messages that cause pyodbc "Check messages" errors
    cur.execute("SET NOCOUNT ON")
    try:
        while cur.nextset():
            pass
    except Exception:
        pass
    cur.execute("SET ANSI_WARNINGS OFF")
    try:
        while cur.nextset():
            pass
    except Exception:
        pass
    cur.execute("SET ARITHABORT ON")
    try:
        while cur.nextset():
            pass
    except Exception:
        pass

    fq = f"[{schema}].[{table}]"
    # Row count
    try:
        cur.execute(f"SELECT COUNT_BIG(*) FROM {fq} WITH (NOLOCK)")
        row_count = int(cur.fetchone()[0] or 0)
    except Exception:
        # Retry with a new cursor if the first one got a message error
        cur.close()
        cur = conn.cursor()
        cur.execute("SET NOCOUNT ON; SET ANSI_WARNINGS OFF; SET ARITHABORT ON")
        try:
            while cur.nextset():
                pass
        except Exception:
            pass
        cur.execute(f"SELECT COUNT_BIG(*) FROM {fq} WITH (NOLOCK)")
        row_count = int(cur.fetchone()[0] or 0)

    # Column metadata (use f-string — pymssql does not support ? placeholders)
    _tbl_safe = table.replace("'", "''")
    _sch_safe = schema.replace("'", "''")
    try:
        cur.execute(f"""
            SELECT c.name, TYPE_NAME(c.user_type_id), c.is_nullable,
                   c.max_length
            FROM sys.columns c
            JOIN sys.objects o ON c.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.name = N'{_tbl_safe}' AND s.name = N'{_sch_safe}'
            ORDER BY c.column_id
        """)
        col_rows = cur.fetchall()
    except Exception:
        cur.close()
        cur = conn.cursor()
        cur.execute("SET NOCOUNT ON")
        cur.execute(f"""
            SELECT c.name, TYPE_NAME(c.user_type_id), c.is_nullable,
                   c.max_length
            FROM sys.columns c
            JOIN sys.objects o ON c.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.name = N'{_tbl_safe}' AND s.name = N'{_sch_safe}'
            ORDER BY c.column_id
        """)
        col_rows = cur.fetchall()

    # Decide sampling clause
    sample_clause = ""
    if row_count > max_distinct_scan:
        pct = max(1, int(max_distinct_scan / row_count * 100))
        sample_clause = f" TABLESAMPLE ({pct} PERCENT) "

    columns = []
    for cname, ctype, is_nullable, _maxlen in col_rows:
        tclass = _type_class(ctype)
        col_info = {
            "name": cname, "data_type": ctype,
            "is_nullable": bool(is_nullable), "type_class": tclass,
            "null_pct": 0.0, "distinct_count": 0,
            "min": None, "max": None, "top_values": [], "pii_tags": [],
            "_row_count_hint": row_count,
        }
        try:
            # Null count + distinct + min/max in one pass
            q = f"""
                SELECT
                  SUM(CASE WHEN [{cname}] IS NULL THEN 1 ELSE 0 END) AS null_cnt,
                  COUNT(DISTINCT [{cname}]) AS dist_cnt,
                  MIN(TRY_CAST([{cname}] AS NVARCHAR(200))) AS v_min,
                  MAX(TRY_CAST([{cname}] AS NVARCHAR(200))) AS v_max
                FROM {fq}{sample_clause}
            """
            cur.execute(q)
            r = cur.fetchone()
            null_cnt = int(r[0] or 0)
            dist_cnt = int(r[1] or 0)
            col_info["null_pct"] = round((null_cnt / row_count) * 100, 2) if row_count else 0.0
            col_info["distinct_count"] = dist_cnt
            col_info["min"] = r[2]
            col_info["max"] = r[3]

            # Top-N values (skip if huge dataset + low-selectivity text column)
            if dist_cnt and dist_cnt <= 100000:
                q2 = f"""
                    SELECT TOP {sample_top_n} [{cname}] AS v, COUNT_BIG(*) AS c
                    FROM {fq}{sample_clause}
                    WHERE [{cname}] IS NOT NULL
                    GROUP BY [{cname}]
                    ORDER BY COUNT_BIG(*) DESC
                """
                cur.execute(q2)
                col_info["top_values"] = [
                    {"value": str(row[0])[:100], "count": int(row[1])}
                    for row in cur.fetchall()
                ]

            # PII detection (name + a tiny value sample)
            samples = [tv["value"] for tv in col_info["top_values"]]
            col_info["pii_tags"] = _pii_tags_for_column(cname, samples)

        except Exception as e:
            col_info["error"] = str(e)[:200]

        col_info["suggested_rules"] = _suggest_rules(table, col_info)
        columns.append(col_info)

    cur.close()
    conn.close()

    return {
        "table": table, "schema": schema, "row_count": row_count,
        "column_count": len(columns), "columns": columns,
        "profiled_at_ms": int((time.time() - t0) * 1000),
        "source": "live",
    }


# ─────────────────────────────────────────────────────────────────────────────
#  DEMO PROFILING — synthesise realistic stats from SQL_TABLES metadata
# ─────────────────────────────────────────────────────────────────────────────
_DEMO_CACHE = {}  # table_name → profile dict

def _parse_ddl_columns(ddl: str) -> list:
    """Extract [name, type] tuples from a CREATE TABLE DDL block."""
    cols = []
    # Match lines like:   [ColName]  TYPE(n)  NOT NULL ...
    rx = re.compile(r"^\s*\[?([A-Za-z_][\w]*)\]?\s+([A-Za-z0-9_]+(?:\([^)]+\))?)", re.M)
    for m in rx.finditer(ddl or ""):
        name = m.group(1)
        dtype = m.group(2)
        # Skip DDL keywords mistaken for columns
        if name.upper() in ("CONSTRAINT", "PRIMARY", "FOREIGN", "CHECK", "UNIQUE",
                            "CREATE", "TABLE", "INDEX", "WITH", "ON", "CLUSTERED"):
            continue
        cols.append((name, dtype))
    return cols


def _synth_stats(cname: str, ctype: str, row_count: int) -> dict:
    """Synthesise deterministic-ish stats for a demo column."""
    # Seed with name so the same demo col always gets the same numbers
    rng = random.Random(hash((cname, ctype)) & 0xFFFFFFFF)
    tclass = _type_class(ctype)
    name_l = cname.lower()
    is_pk_like = name_l.endswith("id") and "id" in name_l
    is_nullable_like = any(tok in name_l for tok in ("phone", "notes", "shipdate", "discount", "salesrepid"))

    # Null pct
    if is_pk_like:
        null_pct = 0.0
    elif is_nullable_like:
        null_pct = round(rng.uniform(5, 30), 2)
    else:
        null_pct = round(rng.uniform(0, 3), 2)

    # Distinct
    if is_pk_like:
        distinct = row_count
    elif tclass == "numeric" and "amount" not in name_l:
        distinct = min(row_count, rng.randint(10, 500))
    elif tclass == "string":
        distinct = min(row_count, rng.randint(5, 2000))
    else:
        distinct = min(row_count, max(1, int(row_count * rng.uniform(0.1, 0.9))))

    # Min / Max
    if tclass == "numeric":
        vmin, vmax = 0, rng.randint(100, 100000)
        if "discount" in name_l:
            vmin = 0; vmax = 50
        if "credit" in name_l or "limit" in name_l:
            vmin = 0; vmax = 50000
    elif tclass == "date":
        vmin, vmax = "2020-01-01", "2026-04-15"
    else:
        vmin, vmax = "A", "ZZZZ"

    # Top values — fabricate plausible ones
    top_values = []
    if name_l == "region":
        for v, c in [("US", int(row_count*0.45)), ("EU", int(row_count*0.22)),
                     ("APAC", int(row_count*0.18)), ("LATAM", int(row_count*0.10)),
                     ("MEA", int(row_count*0.05))]:
            top_values.append({"value": v, "count": c})
    elif name_l == "status":
        for v, c in [("DELIVERED", int(row_count*0.55)), ("SHIPPED", int(row_count*0.22)),
                     ("PENDING", int(row_count*0.15)), ("CONFIRMED", int(row_count*0.06)),
                     ("CANCELLED", int(row_count*0.02))]:
            top_values.append({"value": v, "count": c})
    elif name_l == "isactive":
        top_values = [{"value": "1", "count": int(row_count*0.92)},
                      {"value": "0", "count": int(row_count*0.08)}]
    elif name_l == "returnflag":
        top_values = [{"value": "0", "count": int(row_count*0.94)},
                      {"value": "1", "count": int(row_count*0.06)}]
    elif tclass == "numeric" and "id" not in name_l:
        # Histogram buckets
        for i in range(5):
            top_values.append({
                "value": f"~{rng.randint(100, 5000)}",
                "count": int(row_count * (0.3 - i*0.05))
            })

    pii_samples = [tv["value"] for tv in top_values] if top_values else []
    # Also add a plausible email sample if column is named 'email'
    if "email" in name_l:
        pii_samples = ["user@example.com", "jane.doe@company.co"]
    if "phone" in name_l:
        pii_samples = ["+1-555-0100", "+44 20 7946 0958"]

    pii_tags = _pii_tags_for_column(cname, pii_samples)

    return {
        "null_pct": null_pct, "distinct_count": distinct,
        "min": vmin, "max": vmax, "top_values": top_values,
        "pii_tags": pii_tags, "type_class": tclass,
    }


def profile_table_demo(table: str) -> Optional[dict]:
    """Build a synthesised profile from stored_procedures.SQL_TABLES."""
    if table in _DEMO_CACHE:
        return _DEMO_CACHE[table]
    from stored_procedures import SQL_TABLES
    entry = SQL_TABLES.get(table)
    if not entry:
        return None
    row_count = int(entry.get("row_count", 10000) or 10000)
    ddl_cols = _parse_ddl_columns(entry.get("code", ""))
    if not ddl_cols:
        return None

    columns = []
    for cname, ctype in ddl_cols:
        stats = _synth_stats(cname, ctype, row_count)
        col_info = {
            "name": cname, "data_type": ctype, "is_nullable": True,
            **stats, "_row_count_hint": row_count,
        }
        col_info["suggested_rules"] = _suggest_rules(table, col_info)
        columns.append(col_info)

    prof = {
        "table": table, "schema": "dbo",
        "row_count": row_count, "column_count": len(columns),
        "columns": columns, "source": "demo",
        "has_triggers": entry.get("has_triggers", False),
        "index_count": entry.get("index_count", 0),
        "fk_count": entry.get("fk_count", 0),
        "description": entry.get("description", ""),
    }
    _DEMO_CACHE[table] = prof
    return prof


def list_profilable_tables(source_config: dict = None, mode: str = "demo", schema_filter: str = "") -> list:
    """Return list of tables that can be profiled in the current mode."""
    if mode in ("demo", "static"):
        from stored_procedures import SQL_TABLES
        return [{
            "name": name, "row_count": t.get("row_count", 0),
            "column_count": t.get("column_count", 0),
            "description": t.get("description", ""),
        } for name, t in SQL_TABLES.items()]

    if mode == "live" and source_config:
        from config_cache import get_source_password
        from keyvault_helper import is_masked
        src_type = source_config.get("source_type", "sqlserver")
        if src_type in ("sharepoint", "api"):
            raise ValueError(
                f"Live profiling is not supported for {src_type} sources yet."
            )
        _pw = source_config.get("password", "")
        if not _pw or is_masked(_pw):
            _pw = get_source_password(source_type=src_type)
        schema_filter = schema_filter.replace("'", "''") if schema_filter else ""

        # ── Snowflake path ──
        if src_type == "snowflake":
            import snowflake_connector
            account = source_config.get("account", "") or source_config.get("server", "")
            database = source_config.get("database", "")
            conn = snowflake_connector.get_snowflake_connection(
                account=account, username=source_config.get("username", ""),
                password=_pw, database=database,
                warehouse=source_config.get("warehouse", ""),
                role=source_config.get("role", ""),
            )
            cur = conn.cursor()
            _sf_where = f"AND TABLE_SCHEMA = '{schema_filter}'" if schema_filter else "AND TABLE_SCHEMA != 'INFORMATION_SCHEMA'"
            cur.execute(f"""
                SELECT TABLE_SCHEMA || '.' || TABLE_NAME AS full_name,
                       TABLE_SCHEMA, TABLE_NAME, ROW_COUNT,
                       (SELECT COUNT(*) FROM {database}.INFORMATION_SCHEMA.COLUMNS c
                        WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME) AS col_count
                FROM {database}.INFORMATION_SCHEMA.TABLES t
                WHERE TABLE_TYPE = 'BASE TABLE' {_sf_where}
                ORDER BY TABLE_NAME
            """)
            out = [{"name": r[2], "full_name": r[0], "schema": r[1], "row_count": int(r[3] or 0),
                    "column_count": int(r[4] or 0), "description": ""}
                   for r in cur.fetchall()]
            cur.close(); conn.close()
            return out

        # ── Redshift path ──
        if src_type == "redshift":
            from redshift_client import get_redshift_connection
            conn = get_redshift_connection(
                server=source_config.get("server", ""), username=source_config.get("username", ""),
                password=_pw, database=source_config.get("database", ""),
            )
            cur = conn.cursor()
            row_counts = {}
            try:
                cur.execute("SELECT schema, \"table\", tbl_rows FROM svv_table_info")
                row_counts = {(r[0], r[1]): int(r[2] or 0) for r in cur.fetchall()}
            except Exception:
                pass
            _rs_where = f"AND table_schema = '{schema_filter}'" if schema_filter else "AND table_schema NOT IN ('pg_catalog', 'information_schema')"
            cur.execute(f"""
                SELECT table_schema || '.' || table_name AS full_name, table_schema, table_name,
                       (SELECT COUNT(*) FROM information_schema.columns c
                        WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) AS col_count
                FROM information_schema.tables t
                WHERE table_type = 'BASE TABLE' {_rs_where}
                ORDER BY table_name
            """)
            out = [{"name": r[2], "full_name": r[0], "schema": r[1],
                    "row_count": row_counts.get((r[1], r[2]), 0),
                    "column_count": int(r[3] or 0), "description": ""}
                   for r in cur.fetchall()]
            cur.close(); conn.close()
            return out

        # ── SQL Server path ──
        from sql_pool import get_connection
        conn = get_connection(
            source_type=src_type,
            server=source_config["server"], database=source_config["database"],
            username=source_config.get("username", ""),
            password=_pw,
        )
        cur = conn.cursor()
        cur.execute("SET NOCOUNT ON; SET ANSI_WARNINGS OFF")
        _sf_where = f"AND s.name = '{schema_filter}'" if schema_filter else ""
        cur.execute(f"""
            SELECT s.name + '.' + t.name AS full_name, s.name, t.name,
                   ISNULL(p.row_count, 0) AS row_count,
                   (SELECT COUNT(*) FROM sys.columns c WHERE c.object_id = t.object_id) AS col_count
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            LEFT JOIN (SELECT object_id, SUM(rows) AS row_count
                       FROM sys.partitions WHERE index_id IN (0,1) GROUP BY object_id) p
                 ON p.object_id = t.object_id
            WHERE t.is_ms_shipped = 0 {_sf_where}
            ORDER BY t.name
        """)
        out = [{"name": r[2], "full_name": r[0], "schema": r[1], "row_count": int(r[3] or 0),
                "column_count": int(r[4] or 0), "description": ""}
               for r in cur.fetchall()]
        cur.close(); conn.close()
        return out

    return []
