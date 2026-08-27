"""
data_modeling.py — AI-driven Star / Snowflake Schema Data Modeling Engine

Analyses source table metadata (columns, types, constraints) and produces:
  • Fact / Dimension classification
  • Star or Snowflake schema recommendation
  • ER relationship graph (JSON → rendered client-side)
  • Databricks Delta DDL scripts
"""

import re, json, hashlib
from collections import OrderedDict

# ── column-type heuristics ────────────────────────────────────────────────────
_MEASURE_TYPES = {"int", "bigint", "float", "double", "decimal", "numeric", "smallint", "tinyint", "real", "money", "smallmoney"}
_DATE_TYPES    = {"date", "datetime", "datetime2", "timestamp", "smalldatetime", "datetimeoffset"}

_FACT_KEYWORDS = re.compile(
    r"(transaction|order|sale|invoice|payment|event|log|detail|line_?item|entry|fact)", re.I
)
_DIM_KEYWORDS = re.compile(
    r"(customer|employee|product|department|location|region|store|category|status|type|dim|lookup|geo|channel|vendor|supplier|account|currency|calendar|date)", re.I
)
_FK_PATTERN = re.compile(r"(.+?)(_?id|_?key|_?fk|_?code)$", re.I)

# ── public helpers ────────────────────────────────────────────────────────────

def classify_tables(tables_meta: list[dict], schema_choice: str = "auto") -> dict:
    """
    Accepts list of dicts with keys:
        table_name, columns: [{name, data_type, is_nullable, is_pk, fk_table?}]
    schema_choice: "auto" (AI decides), "star", or "snowflake"
    Returns:
        {facts: [...], dimensions: [...], relationships: [...], schema_type: "star"|"snowflake"}
    """
    facts, dims = [], []
    fk_map = {}  # table → [referenced tables]

    for tbl in tables_meta:
        tname = tbl["table_name"]
        cols  = tbl.get("columns", [])
        score = _score_table(tname, cols)
        entry = {
            "table_name": tname,
            "columns": cols,
            "score": score,
            "role": "fact" if score >= 0 else "dimension",
        }
        if score >= 0:
            facts.append(entry)
        else:
            dims.append(entry)

        # Collect FK references
        refs = set()
        for c in cols:
            if c.get("fk_table"):
                refs.add(c["fk_table"])
            elif _FK_PATTERN.match(c["name"]) and c["name"].lower() not in ("id",):
                # Heuristic: column ends in _id/_key could reference a dim
                stem = _FK_PATTERN.match(c["name"]).group(1)
                for other in tables_meta:
                    if other["table_name"].lower().replace("dim_", "").replace("dim", "") == stem.lower():
                        refs.add(other["table_name"])
        if refs:
            fk_map[tname] = list(refs)

    # Build relationships
    relationships = []
    for src, targets in fk_map.items():
        for tgt in targets:
            relationships.append({"from": src, "to": tgt, "type": "many-to-one"})

    # Determine Star vs Snowflake
    if schema_choice in ("star", "snowflake"):
        schema_type = schema_choice
        # For star: drop dim-to-dim relationships to enforce star topology
        if schema_choice == "star":
            dim_names = {d["table_name"] for d in dims}
            relationships = [r for r in relationships if not (r["from"] in dim_names and r["to"] in dim_names)]
    else:
        dim_names = {d["table_name"] for d in dims}
        dim_to_dim = any(r["from"] in dim_names and r["to"] in dim_names for r in relationships)
        schema_type = "snowflake" if dim_to_dim else "star"

    return {
        "facts": facts,
        "dimensions": dims,
        "relationships": relationships,
        "schema_type": schema_type,
    }


def generate_er_json(model: dict) -> dict:
    """
    Converts the classified model into a JSON structure suitable for
    client-side ER rendering (nodes + edges).
    """
    nodes = []
    edges = []
    # Positions — simple auto-layout: facts in center, dims around
    fact_names = {f["table_name"] for f in model["facts"]}

    fx, fy = 500, 300
    for i, f in enumerate(model["facts"]):
        nodes.append({
            "id": f["table_name"],
            "label": f["table_name"],
            "type": "fact",
            "x": fx + i * 320,
            "y": fy,
            "columns": f["columns"],
        })

    angle_step = 360 / max(len(model["dimensions"]), 1)
    import math
    for i, d in enumerate(model["dimensions"]):
        angle = math.radians(angle_step * i - 90)
        cx = fx + math.cos(angle) * 300
        cy = fy + math.sin(angle) * 250
        nodes.append({
            "id": d["table_name"],
            "label": d["table_name"],
            "type": "dimension",
            "x": round(cx),
            "y": round(cy),
            "columns": d["columns"],
        })

    for r in model["relationships"]:
        edges.append({
            "from": r["from"],
            "to": r["to"],
            "label": r.get("type", "many-to-one"),
            "via_column": r.get("via_column", ""),
        })

    # Views as nodes (type='view')
    views = model.get("views", [])
    for i, v in enumerate(views):
        nodes.append({
            "id": v["view_name"],
            "label": v["view_name"],
            "type": "view",
            "x": fx + i * 280,
            "y": fy + 350,
            "columns": v.get("columns", []),
        })

    # Load persisted metadata if available
    import json, os
    meta_path = os.path.join(os.path.dirname(__file__), "logs", "dm_metadata.json")
    saved_meta = {}
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                saved_meta = json.load(f)
        except Exception:
            pass

    default_meta = {
        "diagram": model.get("name", "Data Model"),
        "author": model.get("author", "Migration Studio"),
        "created_on": model.get("created_on", ""),
        "modified_on": model.get("modified_on", ""),
        "modified_by": model.get("modified_by", ""),
        "design": model.get("design_version", "v1.0"),
        "model_type": "RelationalModel",
        "scope": model.get("scope", ""),
    }
    # Merge: saved values override defaults
    final_meta = {**default_meta, **saved_meta}

    return {
        "nodes": nodes,
        "edges": edges,
        "schema_type": model["schema_type"],
        "metadata": final_meta,
    }


def generate_ddl(model: dict, catalog: str = "main", schema: str = "default") -> str:
    """Generate Databricks Delta DDL for all tables and views in the model."""
    lines = []
    lines.append(f"-- ═══════════════════════════════════════════════════════")
    lines.append(f"-- Data Model DDL — Catalog: {catalog}, Schema: {schema}")
    lines.append(f"-- Schema Type: {model['schema_type'].upper()}")
    lines.append(f"-- Generated by Migration Studio — AI Data Modeling")
    lines.append(f"-- ═══════════════════════════════════════════════════════\n")
    lines.append(f"USE CATALOG {catalog};")
    lines.append(f"USE SCHEMA {schema};\n")

    # Dimensions first (so facts can reference them)
    for tbl in model["dimensions"] + model["facts"]:
        tname = tbl["table_name"]
        role = tbl.get("role", "dimension")
        lines.append(f"-- {'FACT' if role == 'fact' else 'DIMENSION'}: {tname}")
        lines.append(f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{tname} (")

        col_defs = []
        pk_cols = []
        unique_cols = []
        for c in tbl["columns"]:
            dtype = _map_to_databricks_type(c.get("data_type", "STRING"))
            nullable = "" if c.get("is_nullable", True) else " NOT NULL"
            col_comment = ""
            if c.get("comment"):
                safe_comment = c["comment"].replace("'", "''")
                col_comment = f" COMMENT '{safe_comment}'"
            col_defs.append(f"  {c['name']} {dtype}{nullable}{col_comment}")
            if c.get("is_pk"):
                pk_cols.append(c["name"])
            if c.get("is_unique"):
                unique_cols.append(c["name"])

        lines.append(",\n".join(col_defs))
        lines.append(f") USING DELTA")

        # Table comment
        tbl_comment = tbl.get("comment") or f"{role.upper()} table: {tname}"
        safe_tbl_comment = tbl_comment.replace("'", "''")
        lines.append(f"COMMENT '{safe_tbl_comment}';\n")

    # Primary Key constraints (Databricks Unity Catalog supports informational PK)
    for tbl in model["dimensions"] + model["facts"]:
        tname = tbl["table_name"]
        pk_cols = [c["name"] for c in tbl["columns"] if c.get("is_pk")]
        if pk_cols:
            cname = f"pk_{tname}"[:60]
            lines.append(
                f"ALTER TABLE {catalog}.{schema}.{tname} "
                f"ADD CONSTRAINT {cname} PRIMARY KEY ({', '.join(pk_cols)});"
            )

    lines.append("")

    # Unique constraints
    for tbl in model["dimensions"] + model["facts"]:
        tname = tbl["table_name"]
        uq_cols = [c["name"] for c in tbl["columns"] if c.get("is_unique") and not c.get("is_pk")]
        for uc in uq_cols:
            cname = f"uq_{tname}_{uc}"[:60]
            lines.append(
                f"ALTER TABLE {catalog}.{schema}.{tname} "
                f"ADD CONSTRAINT {cname} UNIQUE ({uc});"
            )

    lines.append("")

    # Foreign Key constraints
    for r in model["relationships"]:
        fk_col = r.get("via_column") or _guess_fk_column(r["from"], r["to"], model)
        if fk_col:
            # Find the PK of the target table
            to_pk = _get_table_pk(r["to"], model)
            cname = f"fk_{r['from']}_{r['to']}"[:60]
            lines.append(
                f"ALTER TABLE {catalog}.{schema}.{r['from']} "
                f"ADD CONSTRAINT {cname} FOREIGN KEY ({fk_col}) "
                f"REFERENCES {catalog}.{schema}.{r['to']}({to_pk});"
            )

    # Views
    views = model.get("views", [])
    if views:
        lines.append("")
        lines.append("-- ═══════════════════════════════════════════════════════")
        lines.append("-- VIEWS")
        lines.append("-- ═══════════════════════════════════════════════════════\n")
        for v in views:
            vname = v["view_name"]
            vsql = v.get("definition", f"SELECT * FROM {catalog}.{schema}.{vname}")
            vcomment = v.get("comment", "")
            lines.append(f"-- VIEW: {vname}")
            lines.append(f"CREATE OR REPLACE VIEW {catalog}.{schema}.{vname} AS")
            lines.append(f"{vsql};\n")
            if vcomment:
                safe_vc = vcomment.replace("'", "''")
                lines.append(f"COMMENT ON VIEW {catalog}.{schema}.{vname} IS '{safe_vc}';\n")

    return "\n".join(lines)


def _get_table_pk(table_name: str, model: dict) -> str:
    """Get the primary key column name of a table."""
    for tbl in model["facts"] + model["dimensions"]:
        if tbl["table_name"] == table_name:
            pks = [c["name"] for c in tbl["columns"] if c.get("is_pk")]
            return pks[0] if pks else "id"
    return "id"


def apply_manual_edits(model: dict, edits: dict) -> dict:
    """
    Apply manual edits from the UI.
    edits may contain:
        role_changes:         [{table_name, new_role}]          — move table fact↔dim
        column_edits:         [{table_name, col_name, changes}] — rename, retype, nullable, pk, unique, comment, fk_table, fk_column
        column_adds:          [{table_name, column}]            — add new column to a table
        column_removes:       [{table_name, col_name}]          — remove column from a table
        relationship_adds:    [{from, to, type, via_column?}]
        relationship_removes: [{from, to}]
        table_adds:           [{table_name, role, columns}]     — add a new custom table
        table_removes:        [{table_name}]                    — remove a table from the model
        table_renames:        [{old_name, new_name}]            — rename a table
        table_comments:       [{table_name, comment}]           — set table comment
        view_adds:            [{view_name, definition, comment}] — add a new view
        view_removes:         [{view_name}]                      — remove a view
        view_edits:           [{view_name, definition?, comment?}] — edit view
    """
    # ── Table adds (before other ops so subsequent edits can target new tables)
    for ta in edits.get("table_adds", []):
        entry = {
            "table_name": ta["table_name"],
            "columns": ta.get("columns", [{"name": "id", "data_type": "BIGINT", "is_nullable": False, "is_pk": True}]),
            "score": 0,
            "role": ta.get("role", "dimension"),
            "comment": ta.get("comment", ""),
        }
        if entry["role"] == "fact":
            model["facts"].append(entry)
        else:
            model["dimensions"].append(entry)

    # ── Table removes
    for tr in edits.get("table_removes", []):
        tname = tr["table_name"] if isinstance(tr, dict) else tr
        model["facts"] = [t for t in model["facts"] if t["table_name"] != tname]
        model["dimensions"] = [t for t in model["dimensions"] if t["table_name"] != tname]
        model["relationships"] = [r for r in model["relationships"] if r["from"] != tname and r["to"] != tname]

    # ── Table renames
    for rn in edits.get("table_renames", []):
        old, new = rn["old_name"], rn["new_name"]
        for tbl in model["facts"] + model["dimensions"]:
            if tbl["table_name"] == old:
                tbl["table_name"] = new
        for rel in model["relationships"]:
            if rel["from"] == old:
                rel["from"] = new
            if rel["to"] == old:
                rel["to"] = new

    # ── Role changes
    for rc in edits.get("role_changes", []):
        _move_table_role(model, rc["table_name"], rc["new_role"])

    # ── Column edits
    for ce in edits.get("column_edits", []):
        tname = ce["table_name"]
        col_name = ce.get("col_name") or ce.get("column_name", "")
        # Support both {changes: {field: value}} and {field: ..., value: ...}
        changes = ce.get("changes", {})
        if not changes and "field" in ce:
            field = ce["field"]
            val = ce["value"]
            if field == "name":
                changes = {"new_name": val}
            else:
                changes = {field: val}
        _edit_column(model, tname, col_name, changes)

    # ── Column adds
    for ca in edits.get("column_adds", []):
        for tbl in model["facts"] + model["dimensions"]:
            if tbl["table_name"] == ca["table_name"]:
                tbl["columns"].append(ca["column"])
                break

    # ── Column removes
    for cr in edits.get("column_removes", []):
        cname = cr.get("col_name") or cr.get("column_name", "")
        for tbl in model["facts"] + model["dimensions"]:
            if tbl["table_name"] == cr["table_name"]:
                tbl["columns"] = [c for c in tbl["columns"] if c["name"] != cname]
                break

    # ── Relationship removes (MUST happen before adds so type-change works)
    rel_removes = edits.get("relationship_removes", [])
    model["relationships"] = [
        r for r in model["relationships"]
        if not any(r["from"] == rr["from"] and r["to"] == rr["to"] for rr in rel_removes)
    ]

    # ── Relationship adds
    for ra in edits.get("relationship_adds", []):
        if not any(r["from"] == ra["from"] and r["to"] == ra["to"] for r in model["relationships"]):
            rel_entry = {"from": ra["from"], "to": ra["to"], "type": ra.get("type", "many-to-one")}
            if ra.get("via_column"):
                rel_entry["via_column"] = ra["via_column"]
            model["relationships"].append(rel_entry)

    # ── Table comments
    for tc in edits.get("table_comments", []):
        for tbl in model["facts"] + model["dimensions"]:
            if tbl["table_name"] == tc["table_name"]:
                tbl["comment"] = tc["comment"]
                break

    # ── View adds
    if "views" not in model:
        model["views"] = []
    for va in edits.get("view_adds", []):
        if not any(v["view_name"] == va["view_name"] for v in model["views"]):
            model["views"].append({
                "view_name": va["view_name"],
                "definition": va.get("definition", ""),
                "comment": va.get("comment", ""),
                "columns": va.get("columns", []),
            })

    # ── View removes
    for vr in edits.get("view_removes", []):
        vname = vr["view_name"] if isinstance(vr, dict) else vr
        model["views"] = [v for v in model.get("views", []) if v["view_name"] != vname]

    # ── View edits
    for ve in edits.get("view_edits", []):
        for v in model.get("views", []):
            if v["view_name"] == ve["view_name"]:
                if "definition" in ve:
                    v["definition"] = ve["definition"]
                if "comment" in ve:
                    v["comment"] = ve["comment"]
                break

    # Re-evaluate schema type
    dim_names = {d["table_name"] for d in model["dimensions"]}
    dim_to_dim = any(r["from"] in dim_names and r["to"] in dim_names for r in model["relationships"])
    model["schema_type"] = "snowflake" if dim_to_dim else "star"

    return model


# ── table metadata fetcher ────────────────────────────────────────────────────

def fetch_table_metadata(executor, catalog: str, schema: str, table_names: list[str], warehouse_id: str = None) -> list[dict]:
    """
    Query Databricks INFORMATION_SCHEMA to get column details for each table.
    Returns list of {table_name, columns: [{name, data_type, is_nullable, is_pk}]}
    """
    results = []
    wh = warehouse_id or (executor.warehouse_id if hasattr(executor, 'warehouse_id') else None)
    for tname in table_names:
        sql = (
            f"SELECT column_name, full_data_type, is_nullable "
            f"FROM {catalog}.information_schema.columns "
            f"WHERE table_schema = '{_esc(schema)}' AND table_name = '{_esc(tname)}' "
            f"ORDER BY ordinal_position"
        )
        try:
            resp = executor.execute_custom_sql(sql, wh)
            cols = []
            if resp.get("success") and resp.get("rows"):
                for row in resp["rows"]:
                    cols.append({
                        "name": row[0],
                        "data_type": row[1],
                        "is_nullable": row[2] == "YES",
                        "is_pk": row[0].lower().endswith("_id") or row[0].lower() == "id",
                    })
            if not cols:
                cols = [{"name": "id", "data_type": "BIGINT", "is_nullable": False, "is_pk": True}]
            results.append({"table_name": tname, "columns": cols})
        except Exception:
            results.append({"table_name": tname, "columns": [
                {"name": "id", "data_type": "BIGINT", "is_nullable": False, "is_pk": True}
            ]})
    return results


def list_available_tables(executor, catalog: str, schema: str, warehouse_id: str = None) -> list[str]:
    """List all table names in the given catalog.schema."""
    wh = warehouse_id or (executor.warehouse_id if hasattr(executor, 'warehouse_id') else None)
    sql = (
        f"SELECT table_name FROM {catalog}.information_schema.tables "
        f"WHERE table_schema = '{_esc(schema)}' "
        f"ORDER BY table_name"
    )
    try:
        resp = executor.execute_custom_sql(sql, wh)
        if resp.get("success") and resp.get("rows"):
            return [row[0] for row in resp["rows"]]
    except Exception:
        pass
    return []


# ── private helpers ───────────────────────────────────────────────────────────

def _esc(val: str) -> str:
    """Sanitize value to prevent SQL injection."""
    return str(val).replace("'", "''").replace("\\", "\\\\").replace(";", "")


def _score_table(name: str, columns: list[dict]) -> int:
    """
    Score a table: positive = fact, negative = dimension.
    Heuristics: keyword matches, column type ratios, FK density.
    """
    score = 0

    # Name heuristics
    if _FACT_KEYWORDS.search(name):
        score += 3
    if _DIM_KEYWORDS.search(name):
        score -= 3

    # Column analysis
    measures = sum(1 for c in columns if c.get("data_type", "").lower().split("(")[0] in _MEASURE_TYPES)
    dates    = sum(1 for c in columns if c.get("data_type", "").lower().split("(")[0] in _DATE_TYPES)
    fk_cols  = sum(1 for c in columns if _FK_PATTERN.match(c["name"]) and c["name"].lower() != "id")

    total = max(len(columns), 1)
    if measures / total > 0.3:
        score += 2  # many numeric = likely fact
    if fk_cols >= 3:
        score += 2  # many FKs = likely fact (references dims)
    if total <= 5 and measures / total < 0.2:
        score -= 2  # few columns, few numbers = likely dim

    return score


def _map_to_databricks_type(src_type: str) -> str:
    """Map source SQL types to Databricks SQL types."""
    t = src_type.upper().split("(")[0].strip()
    mapping = {
        "INT": "INT", "INTEGER": "INT", "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT", "TINYINT": "TINYINT",
        "FLOAT": "FLOAT", "REAL": "FLOAT", "DOUBLE": "DOUBLE",
        "DECIMAL": src_type.upper(), "NUMERIC": src_type.upper().replace("NUMERIC", "DECIMAL"),
        "MONEY": "DECIMAL(19,4)", "SMALLMONEY": "DECIMAL(10,4)",
        "VARCHAR": "STRING", "NVARCHAR": "STRING", "CHAR": "STRING", "NCHAR": "STRING",
        "TEXT": "STRING", "NTEXT": "STRING",
        "DATE": "DATE", "DATETIME": "TIMESTAMP", "DATETIME2": "TIMESTAMP",
        "SMALLDATETIME": "TIMESTAMP", "TIMESTAMP": "TIMESTAMP",
        "DATETIMEOFFSET": "TIMESTAMP",
        "BIT": "BOOLEAN", "BOOLEAN": "BOOLEAN",
        "BINARY": "BINARY", "VARBINARY": "BINARY", "IMAGE": "BINARY",
        "UNIQUEIDENTIFIER": "STRING",
    }
    return mapping.get(t, "STRING")


def _guess_fk_column(from_table: str, to_table: str, model: dict) -> str | None:
    """Find the FK column in from_table that points to to_table."""
    all_tables = model["facts"] + model["dimensions"]
    for tbl in all_tables:
        if tbl["table_name"] == from_table:
            to_lower = to_table.lower().replace("dim_", "").replace("dim", "")
            for c in tbl["columns"]:
                cname = c["name"].lower()
                if cname == to_lower + "_id" or cname == to_lower + "_key":
                    return c["name"]
                if c.get("fk_table", "").lower() == to_table.lower():
                    return c["name"]
            # Fallback: any _id that contains the target name
            for c in tbl["columns"]:
                if to_lower in c["name"].lower() and _FK_PATTERN.match(c["name"]):
                    return c["name"]
    return None


def _move_table_role(model: dict, table_name: str, new_role: str):
    """Move table between facts and dimensions lists."""
    source = model["facts"] if new_role == "dimension" else model["dimensions"]
    target = model["dimensions"] if new_role == "dimension" else model["facts"]

    for i, t in enumerate(source):
        if t["table_name"] == table_name:
            t["role"] = new_role
            target.append(source.pop(i))
            return


def suggest_relationships(model: dict) -> list[dict]:
    """
    AI-powered relationship suggestions based on column name patterns,
    foreign key conventions, and table name analysis.
    """
    suggestions = []
    all_tables = model.get("facts", []) + model.get("dimensions", [])
    existing_rels = {(r["from"], r["to"]) for r in model.get("relationships", [])}
    table_names = {t["table_name"]: t for t in all_tables}

    for tbl in all_tables:
        for col in tbl.get("columns", []):
            cname = col["name"].lower()
            # Pattern: column ends with _id, _key, _fk, _code
            m = _FK_PATTERN.match(cname)
            if not m or cname == "id":
                continue
            stem = m.group(1).lower()
            # Find a matching table
            for other_name, other_tbl in table_names.items():
                if other_name == tbl["table_name"]:
                    continue
                other_lower = other_name.lower().replace("dim_", "").replace("fact_", "")
                if stem == other_lower or stem.replace("_", "") == other_lower.replace("_", ""):
                    pair = (tbl["table_name"], other_name)
                    reverse_pair = (other_name, tbl["table_name"])
                    if pair not in existing_rels and reverse_pair not in existing_rels:
                        # Determine relationship type
                        is_fact_to_dim = tbl.get("role") == "fact" and other_tbl.get("role") == "dimension"
                        rel_type = "many-to-one" if is_fact_to_dim else "one-to-many"
                        confidence = 0.9 if stem == other_lower else 0.7
                        suggestions.append({
                            "from": tbl["table_name"],
                            "to": other_name,
                            "type": rel_type,
                            "via_column": col["name"],
                            "confidence": confidence,
                            "reason": f"Column '{col['name']}' likely references '{other_name}'"
                        })
                        existing_rels.add(pair)  # avoid duplicates

    # Also suggest based on shared column names across tables
    for tbl in model.get("facts", []):
        for dim in model.get("dimensions", []):
            # Check if dim has a PK that matches a column in the fact
            dim_pks = [c["name"] for c in dim.get("columns", []) if c.get("is_pk")]
            for pk in dim_pks:
                for fc in tbl.get("columns", []):
                    if fc["name"].lower() == pk.lower() and not fc.get("is_pk"):
                        pair = (tbl["table_name"], dim["table_name"])
                        if pair not in existing_rels:
                            suggestions.append({
                                "from": tbl["table_name"],
                                "to": dim["table_name"],
                                "type": "many-to-one",
                                "via_column": fc["name"],
                                "confidence": 0.85,
                                "reason": f"Fact column '{fc['name']}' matches PK of '{dim['table_name']}'"
                            })
                            existing_rels.add(pair)

    # Sort by confidence descending
    suggestions.sort(key=lambda s: s["confidence"], reverse=True)
    return suggestions


def _edit_column(model: dict, table_name: str, col_name: str, changes: dict):
    """Apply column edits (rename, retype, nullable, pk, unique, comment, fk)."""
    for tbl in model["facts"] + model["dimensions"]:
        if tbl["table_name"] == table_name:
            for c in tbl["columns"]:
                if c["name"] == col_name:
                    if "new_name" in changes:
                        c["name"] = changes["new_name"]
                    if "data_type" in changes:
                        c["data_type"] = changes["data_type"]
                    if "is_nullable" in changes:
                        c["is_nullable"] = changes["is_nullable"]
                    if "is_pk" in changes:
                        c["is_pk"] = changes["is_pk"]
                    if "is_unique" in changes:
                        c["is_unique"] = changes["is_unique"]
                    if "comment" in changes:
                        c["comment"] = changes["comment"]
                    if "fk_table" in changes:
                        c["fk_table"] = changes["fk_table"]
                    if "fk_column" in changes:
                        c["fk_column"] = changes["fk_column"]
                    return


def list_available_views(executor, catalog: str, schema: str, warehouse_id: str = None) -> list[dict]:
    """List all views in the given catalog.schema with their definitions."""
    wh = warehouse_id or (executor.warehouse_id if hasattr(executor, 'warehouse_id') else None)
    sql = (
        f"SELECT table_name, view_definition FROM {catalog}.information_schema.views "
        f"WHERE table_schema = '{_esc(schema)}' "
        f"ORDER BY table_name"
    )
    try:
        resp = executor.execute_custom_sql(sql, wh)
        if resp.get("success") and resp.get("rows"):
            return [{"view_name": row[0], "definition": row[1] or ""} for row in resp["rows"]]
    except Exception:
        pass
    return []
