"""
Discovery — SQL Object Analysis & Migration Readiness Engine
===================================================================
Scans SQL objects (live DB via ODBC or static demo), performs multi-dimension
complexity analysis, builds dependency graphs, and generates migration reports.
"""

import re
import uuid
import csv
import io
import json
from datetime import datetime

# ── Construct detection regex patterns ──
_RE_CTE        = re.compile(r'\bWITH\s+\w+\s+AS\s*\(', re.I)
_RE_CURSOR     = re.compile(r'\bDECLARE\s+\w+\s+CURSOR\b', re.I)
_RE_TEMP_TABLE = re.compile(r'(?:INTO|FROM|JOIN|UPDATE)\s+#\w+', re.I)
_RE_MERGE      = re.compile(r'\bMERGE\s+(?:INTO\s+)?[\[\w]', re.I)
_RE_DYNAMIC    = re.compile(r'\bEXEC(?:UTE)?\s*\(?\s*sp_executesql\b', re.I)
_RE_WINDOW     = re.compile(r'\bOVER\s*\(', re.I)
_RE_PIVOT      = re.compile(r'\b(?:PIVOT|UNPIVOT)\s*\(', re.I)
_RE_TRY_CATCH  = re.compile(r'\bBEGIN\s+TRY\b', re.I)
_RE_TRAN       = re.compile(r'\bBEGIN\s+TRAN(?:SACTION)?\b', re.I)
_RE_CROSS_APP  = re.compile(r'\b(?:CROSS|OUTER)\s+APPLY\b', re.I)
_RE_EXISTS     = re.compile(r'\bEXISTS\s*\(', re.I)
_RE_OPENQUERY  = re.compile(r'\b(?:OPENQUERY|OPENROWSET|OPENDATASOURCE)\b', re.I)
_RE_GOTO       = re.compile(r'\bGOTO\s+\w+', re.I)
_RE_WHILE      = re.compile(r'\bWHILE\s+', re.I)
_RE_EXEC_OBJ   = re.compile(r'\bEXEC(?:UTE)?\s+(?:\[?[\w.]+\]?\.)*\[?(\w+)\]?\b', re.I)
_RE_TABLE_REF  = re.compile(r'\b(?:FROM|JOIN|INTO|UPDATE|MERGE\s+(?:INTO\s+)?)\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?(?:\.\[?(\w+)\]?)?', re.I)
_RE_PARAM      = re.compile(r'@(\w+)\s+(?:INT|BIGINT|NVARCHAR|VARCHAR|DATETIME|DATE|BIT|DECIMAL|FLOAT|NUMERIC|CHAR|MONEY|SMALLINT|TINYINT|UNIQUEIDENTIFIER|XML|TABLE|VARBINARY|IMAGE|TEXT|NTEXT)\b', re.I)
_RE_NOLOCK     = re.compile(r'\bWITH\s*\(\s*NOLOCK\s*\)', re.I)
_RE_TABLESAMPLE = re.compile(r'\bTABLESAMPLE\b', re.I)

# ── Table-specific regex patterns ──
_RE_TBL_COL       = re.compile(r'^\s*\[?(\w+)\]?\s+([\w()., ]+)', re.M)
_RE_TBL_IDENTITY  = re.compile(r'\bIDENTITY\s*\(', re.I)
_RE_TBL_COMPUTED  = re.compile(r'\bAS\s*\(.*?\)', re.I)
_RE_TBL_FK        = re.compile(r'\bFOREIGN\s+KEY\b', re.I)
_RE_TBL_CHECK     = re.compile(r'\bCHECK\s*\(', re.I)
_RE_TBL_UNIQUE    = re.compile(r'\bUNIQUE\b', re.I)
_RE_TBL_TRIGGER   = re.compile(r'\bTRIGGER\b', re.I)
_RE_TBL_TEMPORAL  = re.compile(r'\bSYSTEM_VERSIONING\b|\bFOR\s+SYSTEM_TIME\b', re.I)
_RE_TBL_FILESTRM  = re.compile(r'\bFILESTREAM\b|\bFILETABLE\b', re.I)
_RE_TBL_MEM_OPT   = re.compile(r'\bMEMORY_OPTIMIZED\b', re.I)
_RE_TBL_PARTITION  = re.compile(r'\bPARTITION\b', re.I)
_RE_TBL_CLUSTERED  = re.compile(r'\bCLUSTERED\s+INDEX\b', re.I)
_RE_TBL_NONCLUSTERED = re.compile(r'\bNONCLUSTERED\s+INDEX\b', re.I)
_RE_TBL_COLSTORE   = re.compile(r'\bCOLUMNSTORE\b', re.I)
_RE_TBL_DEFAULT    = re.compile(r'\bDEFAULT\s', re.I)
_RE_TBL_PK         = re.compile(r'\bPRIMARY\s+KEY\b', re.I)
_RE_TBL_XML_IDX    = re.compile(r'\bXML\s+INDEX\b', re.I)
_RE_TBL_SPATIAL_IDX = re.compile(r'\bSPATIAL\s+INDEX\b', re.I)

# Unsupported / problematic data types for Databricks migration
_UNSUPPORTED_DTYPES_RE = [
    (re.compile(r'\bgeography\b', re.I),    'GEOGRAPHY — no native Databricks equivalent; convert to WKT/GeoJSON string'),
    (re.compile(r'\bgeometry\b', re.I),     'GEOMETRY — no native Databricks equivalent; convert to WKT/GeoJSON string'),
    (re.compile(r'\bhierarchyid\b', re.I),  'HIERARCHYID — no Delta Lake equivalent; flatten to string path'),
    (re.compile(r'\bsql_variant\b', re.I),  'SQL_VARIANT — dynamic type not supported; determine actual type per column'),
    (re.compile(r'(?<!\w)image(?!\w)', re.I), 'IMAGE — deprecated LOB; convert to VARBINARY/BINARY before migration'),
    (re.compile(r'\bntext\b', re.I),        'NTEXT — deprecated; convert to STRING in Delta'),
    (re.compile(r'(?<!\w)(?<!\bN)text(?!\w)(?!\s*\()', re.I), 'TEXT — deprecated; convert to STRING in Delta'),
    (re.compile(r'\btimestamp\b(?!\s*\()', re.I), 'TIMESTAMP/ROWVERSION — auto-generated; drops during migration, not needed in Delta'),
    (re.compile(r'\browversion\b', re.I),   'ROWVERSION — auto-generated; drops during migration, not needed in Delta'),
    (re.compile(r'\]\s+xml\b|\bxml\s+(?:null|not\s+null)', re.I), 'XML — store as STRING in Delta; XQuery logic must be rewritten'),
    (re.compile(r'\bcursor\b(?!\s+for)', re.I), 'CURSOR type — not applicable in Delta tables'),
]

# Table RCA map: issue → (impact, reason)
_TABLE_RCA_MAP = {
    'unsupported_dtypes':  ('High — columns with unsupported types will fail ingestion',
                            'Certain SQL Server data types (geography, hierarchyid, sql_variant, CLR types) have no '
                            'direct Delta Lake equivalent. These columns must be converted or dropped before migration.'),
    'computed_columns':    ('Medium — computed columns need manual rewrite as generated columns or views',
                            'SQL Server computed columns use T-SQL expressions. Delta Lake supports generated columns '
                            '(Databricks runtime 10.4+) but expression syntax differs. Complex formulas may need views.'),
    'triggers':            ('High — triggers do not exist in Delta Lake',
                            'SQL Server AFTER/INSTEAD OF triggers have no Delta equivalent. Logic must be moved to '
                            'application layer, streaming pipelines, or Databricks workflows.'),
    'temporal_table':      ('Medium — system-versioned tables need manual Delta history setup',
                            'SQL Server temporal tables auto-track row history. In Databricks, use Delta Time Travel '
                            'or a custom SCD Type-2 pattern to achieve similar auditing.'),
    'filestream':          ('High — FILESTREAM/FileTable requires alternative storage strategy',
                            'FILESTREAM stores BLOBs in the file system linked to SQL rows. Migrate BLOBs to ADLS '
                            'and store paths in Delta. FileTable directory semantics are not supported.'),
    'memory_optimized':    ('Medium — memory-optimized tables need standard Delta conversion',
                            'In-Memory OLTP tables are SQL Server-specific. Convert to standard Delta tables. '
                            'Natively compiled procedures referencing them also need rewriting.'),
    'foreign_keys':        ('Low — informational only; Delta Lake does not enforce FK constraints',
                            'Delta Lake supports FK declarations (informational) but does NOT enforce them. '
                            'If referential integrity is critical, enforce it in the ETL/application layer.'),
    'check_constraints':   ('Low-Medium — CHECK constraints are not enforced in Delta',
                            'Delta Lake supports CHECK constraints (enforced on write in Databricks Runtime 12.2+) '
                            'but complex expressions may need adjustment.'),
    'identity_column':     ('Low — IDENTITY maps to GENERATED ALWAYS AS IDENTITY in Delta',
                            'Delta Lake supports identity columns (GENERATED ALWAYS / BY DEFAULT AS IDENTITY). '
                            'Existing identity values migrate cleanly; just ensure the DDL uses the right syntax.'),
    'xml_index':           ('Medium — XML indexes have no Delta equivalent',
                            'XML indexes are SQL Server-specific. Store XML as STRING and create standard indexes or '
                            'parse into structured columns for filtering.'),
    'spatial_index':       ('Medium — spatial indexes have no Delta equivalent',
                            'Spatial indexes accelerate geography/geometry queries. In Databricks, use H3 or Mosaic '
                            'spatial libraries for geospatial indexing.'),
    'partitioning':        ('Low — table partitioning maps to Delta partitioning',
                            'SQL Server partition schemes/functions map conceptually to Delta PARTITIONED BY. Choose '
                            'the partition column carefully — Delta prefers low-cardinality date-based columns.'),
    'deprecated_lobs':     ('Low-Medium — deprecated LOB types (TEXT/NTEXT/IMAGE) need type migration',
                            'These deprecated types should be converted to VARCHAR(MAX)/NVARCHAR(MAX)/VARBINARY(MAX) '
                            'before migration. In Delta they become STRING or BINARY.'),
}

# System tables/objects to ignore in reference counting
_SYSTEM_TABLES = {
    'inserted', 'deleted', 'sys', 'information_schema',
    'sysobjects', 'syscolumns', 'sysindexes',
}

# Unsupported constructs map: pattern → flag label
_UNSUPPORTED_MAP = [
    (_RE_CURSOR,     'CURSOR — no direct PySpark equivalent; rewrite as set-based'),
    (_RE_DYNAMIC,    'Dynamic SQL (sp_executesql) — requires manual rewrite'),
    (_RE_PIVOT,      'PIVOT/UNPIVOT — use groupBy + pivot() in PySpark'),
    (_RE_CROSS_APP,  'CROSS/OUTER APPLY — rewrite as lateral join or explode()'),
    (_RE_OPENQUERY,  'OPENQUERY/OPENROWSET — Databricks uses different connectors'),
    (_RE_GOTO,       'GOTO — restructure control flow to use functions/loops'),
    (_RE_TABLESAMPLE,'TABLESAMPLE — use .sample() in PySpark'),
]

# RCA reason map: construct → (impact, reason)
_RCA_MAP = {
    'cursor':       ('High — row-by-row processing defeats Spark parallelism',
                     'Cursors iterate row-by-row. Spark is designed for bulk set-based operations. Rewrite as DataFrame/SQL transforms.'),
    'dynamic_sql':  ('High — SQL strings cannot be statically translated',
                     'sp_executesql builds SQL at runtime. PySpark has no equivalent; each dynamic pattern must be analyzed and rewritten.'),
    'merge':        ('Medium — MERGE maps to Delta MERGE INTO but column mapping differs',
                     'T-SQL MERGE has MATCHED/NOT MATCHED/NOT MATCHED BY SOURCE semantics that need careful mapping to Delta Lake merge.'),
    'temp_table':   ('Medium — temp tables need conversion to temporary views or persist logic',
                     '#TempTables are session-scoped in SQL Server. In PySpark, use createOrReplaceTempView() or cache intermediate DataFrames.'),
    'pivot':        ('Medium — PIVOT syntax differs significantly',
                     'T-SQL PIVOT requires static column lists. PySpark pivot() is similar but expression syntax and null handling differ.'),
    'cross_apply':  ('Medium — requires lateral join or explode rewrite',
                     'CROSS/OUTER APPLY is a correlated table expression. Rewrite using explode(), lateral views, or UDF-based approaches.'),
    'window_functions': ('Low-Medium — mostly maps directly but edge cases exist',
                         'Most window functions (ROW_NUMBER, RANK, etc.) map 1:1. Complex frames and custom aggregations may need adjustment.'),
    'transactions': ('Medium — Spark has no multi-statement transactions outside Delta',
                     'BEGIN TRAN / COMMIT patterns must be restructured. Delta Lake provides ACID on single-table operations only.'),
    'try_catch':    ('Low — error handling restructured around try/except in Python',
                     'BEGIN TRY/CATCH maps to Python try/except. The main risk is losing SQL Server error metadata (ERROR_NUMBER, etc.).'),
    'cte':          ('Low — CTEs map cleanly to PySpark chained DataFrames or temp views',
                     'WITH...AS CTEs translate well. Recursive CTEs are the exception — Spark SQL supports them only in recent versions.'),
    'exists_subquery': ('Low — EXISTS maps to semi-join in PySpark',
                        'WHERE EXISTS(...) translates to .join(..., "left_semi"). NOT EXISTS becomes "left_anti" join.'),
    'openquery':    ('High — linked server queries need entirely different connectivity',
                     'OPENQUERY/OPENROWSET use SQL Server linked servers. Databricks uses JDBC/connectors configured differently.'),
    'goto':         ('High — GOTO creates spaghetti control flow that must be restructured',
                     'GOTO is unsupported in PySpark. The entire control flow must be refactored into functions and conditionals.'),
}


# ─────────────────────────────────────────────────────────────────────────────
#  PER-OBJECT ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────
def analyse_sql_object(name: str, object_type: str, sql_code: str) -> dict:
    """
    Perform full analysis on a single SQL object.
    Returns a dict with complexity score, constructs, dependencies, RCA, etc.
    """
    code = sql_code or ""
    lines = code.splitlines()
    line_count = len(lines)

    # ── Construct detection ──
    cte_count    = len(_RE_CTE.findall(code))
    cursor_count = len(_RE_CURSOR.findall(code))
    temp_tables  = len(set(_RE_TEMP_TABLE.findall(code)))
    merge_count  = len(_RE_MERGE.findall(code))
    dynamic_sql  = bool(_RE_DYNAMIC.search(code))
    window_count = len(_RE_WINDOW.findall(code))
    pivot_count  = len(_RE_PIVOT.findall(code))
    try_catch    = bool(_RE_TRY_CATCH.search(code))
    transactions = bool(_RE_TRAN.search(code))
    cross_apply  = bool(_RE_CROSS_APP.search(code))
    exists_sub   = bool(_RE_EXISTS.search(code))
    openquery    = bool(_RE_OPENQUERY.search(code))
    goto         = bool(_RE_GOTO.search(code))
    while_loop   = bool(_RE_WHILE.search(code))
    nolock       = bool(_RE_NOLOCK.search(code))

    constructs = {
        'cte':              cte_count > 0,
        'cte_count':        cte_count,
        'cursor':           cursor_count > 0,
        'cursor_count':     cursor_count,
        'temp_table':       temp_tables > 0,
        'temp_table_count': temp_tables,
        'merge':            merge_count > 0,
        'merge_count':      merge_count,
        'dynamic_sql':      dynamic_sql,
        'window_functions': window_count > 0,
        'window_count':     window_count,
        'pivot':            pivot_count > 0,
        'try_catch':        try_catch,
        'transactions':     transactions,
        'cross_apply':      cross_apply,
        'exists_subquery':  exists_sub,
        'openquery':        openquery,
        'goto':             goto,
        'while_loop':       while_loop,
        'nolock':           nolock,
    }

    # ── Parameter count ──
    params = _RE_PARAM.findall(code)
    param_count = len(set(params))

    # ── Table references ──
    raw_refs = _RE_TABLE_REF.findall(code)
    table_refs = set()
    for schema_part, tbl_part, col_part in raw_refs:
        tbl_name = col_part or tbl_part
        if tbl_name and tbl_name.lower() not in _SYSTEM_TABLES and not tbl_name.startswith('#'):
            table_refs.add(tbl_name)
    table_references = sorted(table_refs)

    # ── Object references (EXEC other_sp) ──
    exec_refs = _RE_EXEC_OBJ.findall(code)
    obj_refs = set()
    for ref in exec_refs:
        if ref.lower() not in ('sp_executesql', 'sp_xml_preparedocument', 'sp_xml_removedocument'):
            obj_refs.add(ref)
    object_references = sorted(obj_refs)

    # ── Unsupported flags ──
    unsupported_flags = []
    for pattern, label in _UNSUPPORTED_MAP:
        if pattern.search(code):
            unsupported_flags.append(label)

    # ── Complexity scoring ──
    score = 0
    if line_count > 300:
        score += 10
    elif line_count > 100:
        score += 5

    score += min(param_count, 10)
    score += cte_count * 3
    score += cursor_count * 15
    score += min(temp_tables * 5, 15)
    score += merge_count * 10
    score += 15 if dynamic_sql else 0
    score += min(window_count * 3, 9)
    score += pivot_count * 10
    score += 5 if try_catch else 0
    score += 5 if transactions else 0
    score += 8 if cross_apply else 0
    score += 2 if exists_sub else 0
    score += 12 if openquery else 0
    score += 10 if goto else 0
    score += min(len(table_references), 10)
    score += len(object_references) * 5

    # ── Tier mapping ──
    if score <= 15:
        tier = 'Simple'
        effort_hours = '4–8'
        effort_points = 3
    elif score <= 30:
        tier = 'Moderate'
        effort_hours = '8–16'
        effort_points = 5
    elif score <= 50:
        tier = 'Complex'
        effort_hours = '16–24'
        effort_points = 8
    else:
        tier = 'Very Complex'
        effort_hours = '24–40'
        effort_points = 13

    # ── Risk assessment ──
    risk_factors = []
    if cursor_count > 0:
        risk_factors.append('Cursor-based logic — performance risk after migration')
    if dynamic_sql:
        risk_factors.append('Dynamic SQL — incomplete auto-conversion, needs manual review')
    if openquery:
        risk_factors.append('Linked server queries — connectivity must be redesigned')
    if goto:
        risk_factors.append('GOTO statements — control flow must be fully restructured')
    if transactions:
        risk_factors.append('Multi-statement transactions — Spark transactional model differs')
    if temp_tables > 3:
        risk_factors.append(f'{temp_tables} temp tables — complex intermediate state management')
    if merge_count > 1:
        risk_factors.append(f'{merge_count} MERGE statements — each needs column-level mapping review')
    if line_count > 500:
        risk_factors.append(f'{line_count} lines — large object, higher test effort')
    if cross_apply:
        risk_factors.append('CROSS/OUTER APPLY — requires lateral join rewrite')

    if len(risk_factors) >= 4:
        risk_level = 'Critical'
    elif len(risk_factors) >= 2:
        risk_level = 'High'
    elif len(risk_factors) >= 1:
        risk_level = 'Medium'
    else:
        risk_level = 'Low'

    # ── Migration readiness ──
    if tier in ('Simple', 'Moderate') and risk_level in ('Low', 'Medium'):
        readiness = 'Auto-Convert Ready'
    elif tier == 'Complex' or risk_level == 'High':
        readiness = 'Manual Review Required'
    else:
        readiness = 'Requires Rewrite'

    # ── RCA — root cause analysis for complexity ──
    rca = []
    rca_keys = []
    if constructs['cursor']:      rca_keys.append('cursor')
    if constructs['dynamic_sql']: rca_keys.append('dynamic_sql')
    if constructs['merge']:       rca_keys.append('merge')
    if constructs['temp_table']:  rca_keys.append('temp_table')
    if constructs['pivot']:       rca_keys.append('pivot')
    if constructs['cross_apply']: rca_keys.append('cross_apply')
    if constructs['transactions']:rca_keys.append('transactions')
    if constructs['try_catch']:   rca_keys.append('try_catch')
    if constructs['cte']:         rca_keys.append('cte')
    if constructs['window_functions']: rca_keys.append('window_functions')
    if constructs['exists_subquery']:  rca_keys.append('exists_subquery')
    if constructs['openquery']:   rca_keys.append('openquery')
    if constructs['goto']:        rca_keys.append('goto')

    for key in rca_keys:
        impact, reason = _RCA_MAP.get(key, ('Unknown', 'No details available'))
        rca.append({
            'construct': key.replace('_', ' ').title(),
            'impact': impact,
            'reason': reason,
        })

    return {
        'name':              name,
        'object_type':       object_type,
        'code':              code,
        'line_count':        line_count,
        'param_count':       param_count,
        'table_references':  table_references,
        'table_ref_count':   len(table_references),
        'object_references': object_references,
        'obj_ref_count':     len(object_references),
        'constructs':        constructs,
        'unsupported_flags': unsupported_flags,
        'complexity_score':  score,
        'complexity_tier':   tier,
        'effort_hours':      effort_hours,
        'effort_points':     effort_points,
        'risk_level':        risk_level,
        'risk_factors':      risk_factors,
        'migration_readiness': readiness,
        'rca':               rca,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  PER-TABLE ANALYSIS — DDL-based migration readiness for tables
# ─────────────────────────────────────────────────────────────────────────────
def analyse_table_object(name: str, ddl: str, column_count: int = 0,
                         row_count: int = 0, has_triggers: bool = False,
                         index_count: int = 0, fk_count: int = 0,
                         check_count: int = 0) -> dict:
    """
    Analyse a SQL Server table for Databricks / Delta Lake migration readiness.
    Returns a dict with same shape as analyse_sql_object() for unified BOM.
    """
    code = ddl or ""
    lines = code.splitlines()
    line_count = len(lines)

    # ── Detect columns & data types ──
    col_matches = _RE_TBL_COL.findall(code)
    if column_count == 0:
        column_count = max(len(col_matches), 1)

    # ── Unsupported data-type detection ──
    unsupported_dtype_flags = []
    for pattern, desc in _UNSUPPORTED_DTYPES_RE:
        if pattern.search(code):
            unsupported_dtype_flags.append(desc)

    # ── Deprecated LOB types ──
    _re_deprecated_lob = re.compile(r'\b(?:ntext|image)\b|\]?\s+text\b', re.I)
    has_deprecated_lobs = bool(_re_deprecated_lob.search(code))

    # ── Structural feature detection ──
    has_identity    = bool(_RE_TBL_IDENTITY.search(code))
    has_computed    = bool(_RE_TBL_COMPUTED.search(code))
    has_fk          = fk_count > 0 or bool(_RE_TBL_FK.search(code))
    has_check       = check_count > 0 or bool(_RE_TBL_CHECK.search(code))
    has_unique      = bool(_RE_TBL_UNIQUE.search(code))
    has_pk          = bool(_RE_TBL_PK.search(code))
    has_temporal    = bool(_RE_TBL_TEMPORAL.search(code))
    has_filestream  = bool(_RE_TBL_FILESTRM.search(code))
    has_mem_opt     = bool(_RE_TBL_MEM_OPT.search(code))
    has_partition   = bool(_RE_TBL_PARTITION.search(code))
    has_colstore    = bool(_RE_TBL_COLSTORE.search(code))
    has_defaults    = bool(_RE_TBL_DEFAULT.search(code))
    has_xml_idx     = bool(_RE_TBL_XML_IDX.search(code))
    has_spatial_idx = bool(_RE_TBL_SPATIAL_IDX.search(code))

    constructs = {
        'column_count':        column_count,
        'row_count':           row_count,
        'has_identity':        has_identity,
        'has_computed_cols':   has_computed,
        'has_primary_key':     has_pk,
        'has_foreign_keys':    has_fk,
        'fk_count':            fk_count,
        'has_check_constraints': has_check,
        'check_count':         check_count,
        'has_unique':          has_unique,
        'has_defaults':        has_defaults,
        'has_triggers':        has_triggers,
        'has_temporal':        has_temporal,
        'has_filestream':      has_filestream,
        'has_memory_optimized': has_mem_opt,
        'has_partitioning':    has_partition,
        'has_columnstore':     has_colstore,
        'has_xml_index':       has_xml_idx,
        'has_spatial_index':   has_spatial_idx,
        'index_count':         index_count,
        'unsupported_dtype_count': len(unsupported_dtype_flags),
    }

    # ── Complexity scoring ──
    score = 0
    score += min(column_count // 10, 5)        # wide tables add complexity
    score += len(unsupported_dtype_flags) * 8   # each bad type is costly
    score += 12 if has_computed else 0
    score += 15 if has_triggers else 0          # triggers can't migrate to Delta
    score += 8  if has_temporal else 0
    score += 15 if has_filestream else 0
    score += 10 if has_mem_opt else 0
    score += 3  if has_partition else 0
    score += 2  if has_fk else 0
    score += 2  if has_check else 0
    score += 3  if has_xml_idx else 0
    score += 3  if has_spatial_idx else 0
    if row_count > 100_000_000:
        score += 10                              # very large table
    elif row_count > 10_000_000:
        score += 5

    # ── Tier mapping ──
    if score <= 8:
        tier = 'Simple'
        effort_hours = '1–2'
        effort_points = 1
    elif score <= 20:
        tier = 'Moderate'
        effort_hours = '2–4'
        effort_points = 3
    elif score <= 35:
        tier = 'Complex'
        effort_hours = '4–8'
        effort_points = 5
    else:
        tier = 'Very Complex'
        effort_hours = '8–16'
        effort_points = 8

    # ── Risk assessment ──
    risk_factors = []
    if len(unsupported_dtype_flags) > 0:
        risk_factors.append(f'{len(unsupported_dtype_flags)} unsupported data type(s) — columns need conversion or removal')
    if has_triggers:
        risk_factors.append('Table has triggers — trigger logic must be moved to ETL/application layer')
    if has_computed:
        risk_factors.append('Computed columns — expressions must be rewritten for Delta generated columns or views')
    if has_filestream:
        risk_factors.append('FILESTREAM/FileTable — BLOB storage strategy must be redesigned for ADLS')
    if has_temporal:
        risk_factors.append('System-versioned temporal table — history tracking needs Delta Time Travel or SCD pattern')
    if has_mem_opt:
        risk_factors.append('Memory-optimized table — convert to standard Delta table')
    if row_count > 100_000_000:
        risk_factors.append(f'{row_count:,} rows — very large table, partition strategy critical')
    if has_xml_idx or has_spatial_idx:
        risk_factors.append('Specialized indexes (XML/Spatial) — no Delta equivalent')

    if len(risk_factors) >= 4:
        risk_level = 'Critical'
    elif len(risk_factors) >= 2:
        risk_level = 'High'
    elif len(risk_factors) >= 1:
        risk_level = 'Medium'
    else:
        risk_level = 'Low'

    # ── Migration readiness ──
    if score <= 8 and risk_level in ('Low', 'Medium'):
        readiness = 'Good to Migrate'
    elif score <= 20 or risk_level == 'Medium':
        readiness = 'Minor Fix Needed'
    elif score <= 35 or risk_level == 'High':
        readiness = 'Needs Fix Before Migration'
    else:
        readiness = 'Major Rework Required'

    # ── RCA — root cause analysis ──
    rca = []
    rca_keys = []
    if unsupported_dtype_flags:  rca_keys.append('unsupported_dtypes')
    if has_computed:             rca_keys.append('computed_columns')
    if has_triggers:             rca_keys.append('triggers')
    if has_temporal:             rca_keys.append('temporal_table')
    if has_filestream:           rca_keys.append('filestream')
    if has_mem_opt:              rca_keys.append('memory_optimized')
    if has_fk:                   rca_keys.append('foreign_keys')
    if has_check:                rca_keys.append('check_constraints')
    if has_identity:             rca_keys.append('identity_column')
    if has_xml_idx:              rca_keys.append('xml_index')
    if has_spatial_idx:          rca_keys.append('spatial_index')
    if has_partition:            rca_keys.append('partitioning')
    if has_deprecated_lobs:      rca_keys.append('deprecated_lobs')

    for key in rca_keys:
        impact, reason = _TABLE_RCA_MAP.get(key, ('Unknown', 'No details available'))
        rca.append({
            'construct': key.replace('_', ' ').title(),
            'impact': impact,
            'reason': reason,
        })

    return {
        'name':              name,
        'object_type':       'table',
        'line_count':        line_count,
        'param_count':       0,
        'column_count':      column_count,
        'row_count':         row_count,
        'table_references':  [],
        'table_ref_count':   0,
        'object_references': [],
        'obj_ref_count':     0,
        'constructs':        constructs,
        'unsupported_flags': unsupported_dtype_flags,
        'complexity_score':  score,
        'complexity_tier':   tier,
        'effort_hours':      effort_hours,
        'effort_points':     effort_points,
        'risk_level':        risk_level,
        'risk_factors':      risk_factors,
        'migration_readiness': readiness,
        'rca':               rca,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  DEPENDENCY GRAPH BUILDER
# ─────────────────────────────────────────────────────────────────────────────
_TIER_COLORS = {
    'Simple':       '#38a169',
    'Moderate':     '#d69e2e',
    'Complex':      '#dd6b20',
    'Very Complex': '#e53e3e',
}
_TYPE_COLORS = {
    'stored_procedure': '#3182ce',
    'view':             '#38a169',
    'udf':              '#805ad5',
    'table':            '#D97706',
}

def build_dependency_graph(analyses: list) -> dict:
    """
    Build a dependency graph from analysed objects.
    Uses object_references (EXEC patterns) and table_references for edges.
    Returns {nodes: [...], edges: [...]} for D3.js force graph.
    """
    name_set = {a['name'].lower(): a for a in analyses}
    nodes = []
    edges = []

    for a in analyses:
        nodes.append({
            'id':       a['name'],
            'label':    a['name'],
            'type':     a['object_type'],
            'tier':     a['complexity_tier'],
            'score':    a['complexity_score'],
            'color':    _TYPE_COLORS.get(a['object_type'], '#718096'),
            'tierColor': _TIER_COLORS.get(a['complexity_tier'], '#718096'),
        })

        # Edge: this object calls another object via EXEC
        for ref in a.get('object_references', []):
            if ref.lower() in name_set:
                edges.append({
                    'source': a['name'],
                    'target': ref,
                    'type':   'calls',
                })

        # Edge: this object references a table that is also a view name
        for tref in a.get('table_references', []):
            if tref.lower() in name_set and tref.lower() != a['name'].lower():
                edges.append({
                    'source': a['name'],
                    'target': tref,
                    'type':   'references',
                })

    # Deduplicate edges
    seen = set()
    unique_edges = []
    for e in edges:
        key = (e['source'], e['target'], e['type'])
        if key not in seen:
            seen.add(key)
            unique_edges.append(e)

    return {'nodes': nodes, 'edges': unique_edges}


# ─────────────────────────────────────────────────────────────────────────────
#  SCAN: SNOWFLAKE LIVE SOURCE
# ─────────────────────────────────────────────────────────────────────────────
def _scan_live_snowflake(source_config: dict, password: str, schema_filter: str = "") -> list:
    """Discover objects from a live Snowflake database via snowflake-connector."""
    import snowflake_connector
    account = source_config.get('account', '') or source_config.get('server', '')
    username = source_config.get('username', '')
    database = source_config.get('database', '')
    warehouse = source_config.get('warehouse', '')
    role = source_config.get('role', '')

    conn = snowflake_connector.get_snowflake_connection(
        account=account, username=username, password=password,
        database=database, warehouse=warehouse, role=role,
    )
    cursor = conn.cursor()
    objects = []
    _sf = f"AND PROCEDURE_SCHEMA = '{schema_filter}'" if schema_filter else "AND PROCEDURE_SCHEMA != 'INFORMATION_SCHEMA'"
    _sv = f"AND TABLE_SCHEMA = '{schema_filter}'" if schema_filter else "AND TABLE_SCHEMA != 'INFORMATION_SCHEMA'"
    _sfn = f"AND FUNCTION_SCHEMA = '{schema_filter}'" if schema_filter else "AND FUNCTION_SCHEMA != 'INFORMATION_SCHEMA'"

    # Stored Procedures
    try:
        cursor.execute(f"""
            SELECT PROCEDURE_SCHEMA, PROCEDURE_NAME, PROCEDURE_DEFINITION
            FROM {database}.INFORMATION_SCHEMA.PROCEDURES
            WHERE 1=1 {_sf}
            ORDER BY PROCEDURE_NAME
        """)
        for row in cursor.fetchall():
            objects.append(('stored_procedure', f"{row[0]}.{row[1]}", row[2] or ''))
    except Exception:
        pass

    # Views
    try:
        cursor.execute(f"""
            SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION
            FROM {database}.INFORMATION_SCHEMA.VIEWS
            WHERE 1=1 {_sv}
            ORDER BY TABLE_NAME
        """)
        for row in cursor.fetchall():
            objects.append(('view', f"{row[0]}.{row[1]}", row[2] or ''))
    except Exception:
        pass

    # Functions (UDFs)
    try:
        cursor.execute(f"""
            SELECT FUNCTION_SCHEMA, FUNCTION_NAME, FUNCTION_DEFINITION
            FROM {database}.INFORMATION_SCHEMA.FUNCTIONS
            WHERE 1=1 {_sfn}
            ORDER BY FUNCTION_NAME
        """)
        for row in cursor.fetchall():
            objects.append(('udf', f"{row[0]}.{row[1]}", row[2] or ''))
    except Exception:
        pass

    # Tables
    try:
        _st = f"AND t.TABLE_SCHEMA = '{schema_filter}'" if schema_filter else "AND t.TABLE_SCHEMA != 'INFORMATION_SCHEMA'"
        cursor.execute(f"""
            SELECT t.TABLE_SCHEMA, t.TABLE_NAME,
                   (SELECT COUNT(*) FROM {database}.INFORMATION_SCHEMA.COLUMNS c
                    WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME) AS col_count,
                   t.ROW_COUNT
            FROM {database}.INFORMATION_SCHEMA.TABLES t
            WHERE t.TABLE_TYPE = 'BASE TABLE' {_st}
            ORDER BY t.TABLE_NAME
        """)
        for row in cursor.fetchall():
            schema_name, tbl_name, col_count, row_count = row[0], row[1], row[2] or 0, row[3] or 0
            ddl = f"CREATE TABLE {schema_name}.{tbl_name} (/* {col_count} columns, ~{row_count} rows */)"
            objects.append(('table', f"{schema_name}.{tbl_name}", ddl))
    except Exception:
        pass

    try:
        conn.close()
    except Exception:
        pass

    # Analyse each object
    analyses = []
    for obj_type, name, code in objects:
        if obj_type == 'table':
            a = analyse_table_object(name, code)
        else:
            a = analyse_sql_object(name, obj_type, code)
        analyses.append(a)
    return analyses


def _scan_live_redshift(source_config: dict, password: str, schema_filter: str = "") -> list:
    """Discover objects from a live Redshift cluster via redshift_connector."""
    from redshift_client import get_redshift_connection
    server = source_config.get('server', '')
    username = source_config.get('username', '')
    database = source_config.get('database', '')

    conn = get_redshift_connection(server=server, username=username, password=password, database=database)
    cursor = conn.cursor()
    objects = []
    _schema_safe = schema_filter.replace("'", "''") if schema_filter else ''
    _ns_filter = f"AND n.nspname = '{_schema_safe}'" if schema_filter else "AND n.nspname NOT IN ('pg_catalog', 'information_schema')"
    _sv_filter = f"AND table_schema = '{_schema_safe}'" if schema_filter else "AND table_schema NOT IN ('pg_catalog', 'information_schema')"

    # Stored Procedures
    try:
        cursor.execute(f"""
            SELECT n.nspname, p.proname, COALESCE(pg_get_functiondef(p.oid), '')
            FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE p.prokind = 'p' {_ns_filter}
            ORDER BY p.proname
        """)
        for row in cursor.fetchall():
            objects.append(('stored_procedure', f"{row[0]}.{row[1]}", row[2] or ''))
    except Exception:
        pass

    # Views
    try:
        cursor.execute(f"""
            SELECT table_schema, table_name, COALESCE(view_definition, '')
            FROM information_schema.views
            WHERE 1=1 {_sv_filter}
            ORDER BY table_name
        """)
        for row in cursor.fetchall():
            objects.append(('view', f"{row[0]}.{row[1]}", row[2] or ''))
    except Exception:
        pass

    # Functions (UDFs)
    try:
        cursor.execute(f"""
            SELECT n.nspname, p.proname, COALESCE(pg_get_functiondef(p.oid), '')
            FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE p.prokind = 'f' {_ns_filter}
            ORDER BY p.proname
        """)
        for row in cursor.fetchall():
            objects.append(('udf', f"{row[0]}.{row[1]}", row[2] or ''))
    except Exception:
        pass

    # Tables
    try:
        _tbl_filter = f"AND table_schema = '{_schema_safe}'" if schema_filter else "AND table_schema NOT IN ('pg_catalog', 'information_schema')"
        cursor.execute(f"""
            SELECT table_schema, table_name,
                   (SELECT COUNT(*) FROM information_schema.columns c
                    WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) AS col_count
            FROM information_schema.tables t
            WHERE table_type = 'BASE TABLE' {_tbl_filter}
            ORDER BY table_name
        """)
        for row in cursor.fetchall():
            schema_name, tbl_name, col_count = row[0], row[1], row[2] or 0
            ddl = f"CREATE TABLE {schema_name}.{tbl_name} (/* {col_count} columns */)"
            objects.append(('table', f"{schema_name}.{tbl_name}", ddl))
    except Exception:
        pass

    try:
        conn.close()
    except Exception:
        pass

    analyses = []
    for obj_type, name, code in objects:
        if obj_type == 'table':
            a = analyse_table_object(name, code)
        else:
            a = analyse_sql_object(name, obj_type, code)
        analyses.append(a)
    return analyses


# ─────────────────────────────────────────────────────────────────────────────
#  SCAN: LIVE SHAREPOINT (lists & document libraries as tables)
# ─────────────────────────────────────────────────────────────────────────────
def _scan_live_sharepoint(source_config: dict, password: str, schema_filter: str = "") -> list:
    """Discover SharePoint lists / document libraries and analyse each as a
    table-like migration object (ItemCount feeds the row-count estimate)."""
    analyses = []
    try:
        from sharepoint_connector import load_objects as sp_load
        result = sp_load(
            server=source_config.get('server', ''),
            username=source_config.get('username', ''),   # Client ID
            password=password,
            database=source_config.get('database', ''),
            tenant_id=source_config.get('tenant_id', ''),
        )
        if not result.get('success'):
            raise RuntimeError(result.get('error', 'SharePoint scan failed'))
        for obj in result.get('grouped', {}).get('view', []):
            # Item count is embedded in the description: "List (N items)"
            import re as _re
            m = _re.search(r'\((\d+) items?\)', obj.get('description') or '')
            row_count = int(m.group(1)) if m else 0
            ddl = (
                f"CREATE TABLE [{obj['name']}] (\n"
                f"  -- SharePoint source — see REST sample below\n"
                f")"
                f"\n/* {obj.get('code', '')} */"
            )
            analyses.append(analyse_table_object(
                name=obj['name'], ddl=ddl,
                column_count=0, row_count=row_count,
            ))
    except Exception as e:
        logger.warning("SharePoint live scan failed: %s", str(e)[:300])
        raise
    return analyses


# ─────────────────────────────────────────────────────────────────────────────
#  SCAN: LIVE REST API (GET endpoints as tables)
# ─────────────────────────────────────────────────────────────────────────────
def _scan_live_api(source_config: dict, password: str, schema_filter: str = "") -> list:
    """Discover REST GET endpoints (via OpenAPI/Swagger when available) and
    analyse each endpoint payload as a table-like migration object."""
    analyses = []
    try:
        from api_source_client import load_objects as api_load
        result = api_load(
            server=source_config.get('server', ''),
            username=source_config.get('username', ''),
            password=password,
            database=source_config.get('database', ''),
            auth_type=source_config.get('api_auth_type', 'none'),
            api_key_header=source_config.get('api_key_header', ''),
        )
        if not result.get('success'):
            raise RuntimeError(result.get('error', 'API scan failed'))
        for obj in result.get('grouped', {}).get('view', []):
            ddl = (
                f"CREATE TABLE [{obj['name']}] (\n"
                f"  -- REST API endpoint payload\n"
                f")"
                f"\n/* {obj.get('code', '')} */"
            )
            analyses.append(analyse_table_object(
                name=obj['name'].replace('GET ', '', 1), ddl=ddl,
                column_count=0, row_count=0,
            ))
        if not analyses:
            logger.info("API live scan found no OpenAPI spec at %s", source_config.get('server', ''))
    except Exception as e:
        logger.warning("API live scan failed: %s", str(e)[:300])
        raise
    return analyses


# ─────────────────────────────────────────────────────────────────────────────
#  SCAN: LIVE SOURCE DB (SQL Server / Azure SQL / Synapse)
# ─────────────────────────────────────────────────────────────────────────────
def scan_live_source(source_config: dict, schema_filter: str = "") -> list:
    """
    Connect to a live source and discover + analyse all objects.
    source_config: {source_type, server, database, username, password, account?, warehouse?, role?}
    schema_filter: optional schema name to restrict discovery to.
    Returns list of analysis dicts.
    """
    from config_cache import get_source_password
    from keyvault_helper import is_masked

    src_type = source_config.get('source_type', 'sqlserver')
    _pw = source_config.get('password', '')
    if not _pw or is_masked(_pw):
        _pw = get_source_password(source_type=src_type)

    # ── Snowflake path ──
    if src_type == 'snowflake':
        return _scan_live_snowflake(source_config, _pw, schema_filter=schema_filter)

    # ── Redshift path ──
    if src_type == 'redshift':
        return _scan_live_redshift(source_config, _pw, schema_filter=schema_filter)

    # ── SharePoint path ──
    if src_type == 'sharepoint':
        return _scan_live_sharepoint(source_config, _pw, schema_filter=schema_filter)

    # ── Generic REST API path ──
    if src_type == 'api':
        return _scan_live_api(source_config, _pw, schema_filter=schema_filter)

    # ── SQL Server / Azure SQL / Synapse path ──
    from sql_pool import get_connection
    conn = get_connection(
        source_type=src_type,
        server=source_config['server'],
        database=source_config['database'],
        username=source_config['username'],
        password=_pw,
        timeout=60,
    )
    cursor = conn.cursor()
    objects = []
    _schema_safe = schema_filter.replace("'", "''") if schema_filter else ''
    _sp_filter = f"AND SCHEMA_NAME(schema_id) = '{_schema_safe}'" if schema_filter else ""
    _tbl_filter = f"AND SCHEMA_NAME(t.schema_id) = '{_schema_safe}'" if schema_filter else ""

    # Stored Procedures
    cursor.execute(f"""
        SELECT SCHEMA_NAME(schema_id) + '.' + name AS [key],
               name, type_desc,
               ISNULL(OBJECT_DEFINITION(object_id), '') AS code
        FROM   sys.procedures
        WHERE  is_ms_shipped = 0 {_sp_filter}
        ORDER  BY name
    """)
    for row in cursor.fetchall():
        objects.append(('stored_procedure', row[0], row[3]))

    # Views
    cursor.execute(f"""
        SELECT SCHEMA_NAME(schema_id) + '.' + name AS [key],
               name,
               ISNULL(OBJECT_DEFINITION(object_id), '') AS code
        FROM   sys.views
        WHERE  is_ms_shipped = 0 {_sp_filter}
        ORDER  BY name
    """)
    for row in cursor.fetchall():
        objects.append(('view', row[0], row[2]))

    # UDFs
    cursor.execute(f"""
        SELECT SCHEMA_NAME(schema_id) + '.' + name AS [key],
               name,
               ISNULL(OBJECT_DEFINITION(object_id), '') AS code
        FROM   sys.objects
        WHERE  type IN ('FN', 'IF', 'TF')
          AND  is_ms_shipped = 0 {_sp_filter}
        ORDER  BY name
    """)
    for row in cursor.fetchall():
        objects.append(('udf', row[0], row[2]))

    # Tables — fetch DDL-like metadata for each user table
    table_meta = []  # (qualified_name, bare_name, col_count, row_count, has_triggers, idx_count, fk_count, chk_count, ddl_sketch)
    try:
        cursor.execute(f"""
            SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name,
                   (SELECT COUNT(*) FROM sys.columns c WHERE c.object_id = t.object_id) AS col_count,
                   ISNULL(p.row_count, 0) AS row_count,
                   CASE WHEN EXISTS(SELECT 1 FROM sys.triggers tr WHERE tr.parent_id = t.object_id) THEN 1 ELSE 0 END AS has_triggers,
                   (SELECT COUNT(*) FROM sys.indexes i WHERE i.object_id = t.object_id AND i.index_id > 0) AS idx_count,
                   (SELECT COUNT(*) FROM sys.foreign_keys fk WHERE fk.parent_object_id = t.object_id) AS fk_count,
                   (SELECT COUNT(*) FROM sys.check_constraints ck WHERE ck.parent_object_id = t.object_id) AS chk_count
            FROM   sys.tables t
            LEFT JOIN (SELECT object_id, SUM(rows) AS row_count
                       FROM sys.partitions WHERE index_id IN (0,1) GROUP BY object_id) p
                  ON p.object_id = t.object_id
            WHERE  t.is_ms_shipped = 0 {_tbl_filter}
            ORDER  BY t.name
        """)
        for row in cursor.fetchall():
            schema_name = row[0]
            tbl_name = row[1]
            qualified_name = f"{schema_name}.{tbl_name}"
            col_count = row[2] or 0
            tbl_row_count = int(row[3]) if row[3] else 0
            trig = bool(row[4])
            idx_cnt = row[5] or 0
            fk_cnt = row[6] or 0
            chk_cnt = row[7] or 0
            # Build a DDL sketch using column info
            col_rows = []
            try:
                _tbl_safe = qualified_name.replace("'", "''")
                cursor.execute(f"""
                    SELECT c.name, TYPE_NAME(c.user_type_id),
                           CASE WHEN c.is_identity = 1 THEN 'IDENTITY' ELSE '' END,
                           CASE WHEN c.is_computed = 1 THEN 'COMPUTED' ELSE '' END,
                           CASE WHEN c.is_nullable = 0 THEN 'NOT NULL' ELSE 'NULL' END
                    FROM sys.columns c WHERE c.object_id = OBJECT_ID(N'{_tbl_safe}') ORDER BY c.column_id
                """)
                for cr in cursor.fetchall():
                    col_rows.append(f'  [{cr[0]}] {cr[1]} {cr[2]} {cr[3]} {cr[4]}'.strip())
            except Exception:
                pass
            ddl_sketch = f'CREATE TABLE [{qualified_name}] (\n' + ',\n'.join(col_rows) + '\n)' if col_rows else ''
            table_meta.append((qualified_name, tbl_name, col_count, tbl_row_count, trig, idx_cnt, fk_cnt, chk_cnt, ddl_sketch))
    except Exception:
        pass

    # Dependency info from sys.sql_expression_dependencies
    dep_map = {}
    try:
        cursor.execute("""
            SELECT OBJECT_NAME(referencing_id) AS referencing,
                   COALESCE(referenced_entity_name, '') AS referenced
            FROM   sys.sql_expression_dependencies
            WHERE  referencing_id IS NOT NULL
              AND  referenced_entity_name IS NOT NULL
        """)
        for row in cursor.fetchall():
            dep_map.setdefault(row[0], []).append(row[1])
    except Exception:
        pass

    conn.close()

    analyses = []
    for obj_type, obj_name, obj_code in objects:
        a = analyse_sql_object(obj_name, obj_type, obj_code)
        # Enrich with live dependency data (dep_map is keyed by bare object name)
        bare_name = obj_name.rsplit('.', 1)[-1]
        if bare_name in dep_map:
            extra_refs = [r for r in dep_map[bare_name] if r.lower() != bare_name.lower()]
            existing = set(r.lower() for r in a['object_references'])
            for r in extra_refs:
                if r.lower() not in existing:
                    a['object_references'].append(r)
                    existing.add(r.lower())
            a['obj_ref_count'] = len(a['object_references'])
        analyses.append(a)

    # Analyse tables
    for qualified_name, tbl_name, col_cnt, tbl_rows, trig, idx_cnt, fk_cnt, chk_cnt, ddl in table_meta:
        a = analyse_table_object(
            name=qualified_name, ddl=ddl,
            column_count=col_cnt, row_count=tbl_rows,
            has_triggers=trig, index_count=idx_cnt,
            fk_count=fk_cnt, check_count=chk_cnt,
        )
        analyses.append(a)

    return analyses


# ─────────────────────────────────────────────────────────────────────────────
#  SCAN: STATIC DEMO OBJECTS
# ─────────────────────────────────────────────────────────────────────────────
def scan_static_objects() -> list:
    """Analyse all pre-loaded demo objects from stored_procedures.py."""
    from stored_procedures import ALL_OBJECTS
    analyses = []
    for name, obj in ALL_OBJECTS.items():
        obj_type = obj.get('object_type', 'stored_procedure')
        if obj_type == 'table':
            a = analyse_table_object(
                name=obj.get('name', name),
                ddl=obj.get('code', ''),
                column_count=obj.get('column_count', 0),
                row_count=obj.get('row_count', 0),
                has_triggers=obj.get('has_triggers', False),
                index_count=obj.get('index_count', 0),
                fk_count=obj.get('fk_count', 0),
                check_count=obj.get('check_count', 0),
            )
        else:
            a = analyse_sql_object(
                name=obj.get('name', name),
                object_type=obj_type,
                sql_code=obj.get('code', ''),
            )
        analyses.append(a)
    return analyses


# ─────────────────────────────────────────────────────────────────────────────
#  REPORT GENERATION: SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
def generate_discovery_report(analyses: list, dep_graph: dict = None) -> dict:
    """
    Aggregate analyses into a summary report dict.
    """
    total = len(analyses)
    by_type = {}
    by_tier = {'Simple': 0, 'Moderate': 0, 'Complex': 0, 'Very Complex': 0}
    by_risk = {'Low': 0, 'Medium': 0, 'High': 0, 'Critical': 0}
    by_readiness = {}
    total_effort = 0
    total_score = 0

    for a in analyses:
        ot = a['object_type']
        by_type[ot] = by_type.get(ot, 0) + 1
        by_tier[a['complexity_tier']] = by_tier.get(a['complexity_tier'], 0) + 1
        by_risk[a['risk_level']] = by_risk.get(a['risk_level'], 0) + 1
        rd = a['migration_readiness']
        by_readiness[rd] = by_readiness.get(rd, 0) + 1
        total_effort += a['effort_points']
        total_score += a['complexity_score']

    auto_ready = by_readiness.get('Auto-Convert Ready', 0) + by_readiness.get('Good to Migrate', 0)
    readiness_pct = round((auto_ready / total) * 100, 1) if total > 0 else 0
    avg_score = round(total_score / total, 1) if total > 0 else 0

    return {
        'scan_id':          uuid.uuid4().hex[:12],
        'scan_timestamp':   datetime.now().isoformat(),
        'total_objects':    total,
        'by_type':          by_type,
        'by_tier':          by_tier,
        'by_risk':          by_risk,
        'by_readiness':     by_readiness,
        'total_effort_points': total_effort,
        'avg_complexity_score': avg_score,
        'readiness_pct':    readiness_pct,
        'dependency_graph': dep_graph,
        'objects':          analyses,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  BOM EXPORT — CSV
# ─────────────────────────────────────────────────────────────────────────────
def generate_bom_csv(analyses: list) -> str:
    """Generate a CSV string for Bill of Materials export with SQL CREATE statements."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        'Object Name', 'Type', 'Lines', 'Parameters', 'Tables Referenced',
        'Objects Referenced', 'Complexity Score', 'Tier', 'Effort (hrs)',
        'Effort Points', 'Risk Level', 'Readiness', 'Unsupported Flags',
        'SQL CREATE Statement',
    ])
    for a in analyses:
        writer.writerow([
            a['name'],
            a['object_type'],
            a['line_count'],
            a['param_count'],
            a['table_ref_count'],
            a['obj_ref_count'],
            a['complexity_score'],
            a['complexity_tier'],
            a['effort_hours'],
            a['effort_points'],
            a['risk_level'],
            a['migration_readiness'],
            '; '.join(a.get('unsupported_flags', [])),
            ' ' + (a.get('code', '') or '').replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ').strip()[:500],
        ])
    # Add UTF-8 BOM so Excel displays characters correctly
    return '\ufeff' + output.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
#  HTML REPORT EXPORT — standalone self-contained HTML
# ─────────────────────────────────────────────────────────────────────────────
def generate_html_report(report: dict) -> str:
    """Generate a self-contained HTML report with inline data."""
    objects = report.get('objects', [])
    summary = {k: v for k, v in report.items() if k != 'objects' and k != 'dependency_graph'}

    # Build BOM rows
    bom_rows = ''
    tier_cls = {'Simple': 'green', 'Moderate': 'yellow', 'Complex': 'orange', 'Very Complex': 'red'}
    risk_cls = {'Low': 'green', 'Medium': 'yellow', 'High': 'orange', 'Critical': 'red'}
    ready_cls = {'Auto-Convert Ready': 'green', 'Manual Review Required': 'yellow', 'Requires Rewrite': 'red',
                 'Good to Migrate': 'green', 'Minor Fix Needed': 'yellow', 'Needs Fix Before Migration': 'orange', 'Major Rework Required': 'red'}
    for a in objects:
        is_table = a['object_type'] == 'table'
        col4 = a.get('column_count', a.get('constructs', {}).get('column_count', '-')) if is_table else a['param_count']
        col5_raw = a.get('row_count', a.get('constructs', {}).get('row_count', 0)) if is_table else a['table_ref_count']
        col5 = f'{col5_raw:,}' if is_table and isinstance(col5_raw, int) else col5_raw
        bom_rows += f"""<tr>
  <td><strong>{_h(a['name'])}</strong></td>
  <td>{_h(a['object_type'])}</td>
  <td>{a['line_count']}</td>
  <td>{col4}</td>
  <td>{col5}</td>
  <td>{a['complexity_score']}</td>
  <td><span class="tag {tier_cls.get(a['complexity_tier'],'')}">{_h(a['complexity_tier'])}</span></td>
  <td>{_h(a['effort_hours'])}h</td>
  <td><span class="tag {risk_cls.get(a['risk_level'],'')}">{_h(a['risk_level'])}</span></td>
  <td><span class="tag {ready_cls.get(a['migration_readiness'],'')}">{_h(a['migration_readiness'])}</span></td>
</tr>\n"""

    # Build RCA section
    rca_html = ''
    for a in objects:
        if not a.get('rca'):
            continue
        rca_html += f'<div class="rca-block"><h3>{_h(a["name"])} <span class="tag {tier_cls.get(a["complexity_tier"],"")}">{_h(a["complexity_tier"])}</span></h3><table><thead><tr><th>Construct</th><th>Impact</th><th>Root Cause</th></tr></thead><tbody>'
        for r in a['rca']:
            rca_html += f'<tr><td><strong>{_h(r["construct"])}</strong></td><td>{_h(r["impact"])}</td><td>{_h(r["reason"])}</td></tr>'
        rca_html += '</tbody></table></div>\n'

    by_tier = report.get('by_tier', {})
    by_risk = report.get('by_risk', {})
    by_type = report.get('by_type', {})

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Migration Discovery Report</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Segoe UI',system-ui,sans-serif;background:#f7fafc;color:#2d3748;line-height:1.6}}
.hdr{{background:linear-gradient(135deg,#1a202c,#2d3748 60%,#4a5568);color:#fff;padding:40px}}
.hdr h1{{font-size:28px}} .hdr .sub{{opacity:.7;margin-top:6px}}
.hdr .meta{{margin-top:16px;font-size:12px;opacity:.5;display:flex;gap:20px;flex-wrap:wrap}}
.wrap{{max-width:1200px;margin:0 auto;padding:0 24px 60px}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin:24px 0}}
.card{{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:16px;text-align:center;box-shadow:0 1px 3px rgba(0,0,0,.06)}}
.card .n{{font-size:32px;font-weight:700;line-height:1}} .card .l{{font-size:11px;color:#718096;text-transform:uppercase;margin-top:4px}}
.n.green{{color:#38a169}} .n.yellow{{color:#d69e2e}} .n.orange{{color:#dd6b20}} .n.red{{color:#e53e3e}} .n.blue{{color:#3182ce}}
h2{{font-size:18px;font-weight:700;margin:32px 0 12px;padding-bottom:8px;border-bottom:2px solid #e2e8f0}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
thead tr{{background:#2d3748;color:#fff}} th{{padding:10px 12px;text-align:left;font-weight:600}}
tbody tr{{border-bottom:1px solid #e2e8f0}} tbody tr:hover{{background:#f7fafc}} td{{padding:9px 12px;vertical-align:top}}
.tag{{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;text-transform:uppercase}}
.tag.green{{background:#f0fff4;color:#38a169}} .tag.yellow{{background:#fffff0;color:#d69e2e}}
.tag.orange{{background:#fffaf0;color:#dd6b20}} .tag.red{{background:#fff5f5;color:#e53e3e}}
.rca-block{{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:16px;margin-bottom:16px}}
.rca-block h3{{font-size:15px;margin-bottom:10px;display:flex;align-items:center;gap:8px}}
footer{{text-align:center;padding:24px;font-size:11px;color:#718096;border-top:1px solid #e2e8f0}}
</style>
</head>
<body>
<div class="hdr"><div class="wrap" style="padding-bottom:0">
<h1>SQL Object Discovery — Migration Readiness Report</h1>
<div class="sub">Automated analysis of {report.get('total_objects',0)} SQL objects with complexity scoring, risk assessment, and root-cause analysis</div>
<div class="meta"><span>Scan ID: {_h(report.get('scan_id',''))}</span><span>Scanned: {_h(report.get('scan_timestamp',''))}</span><span>Objects: {report.get('total_objects',0)}</span></div>
</div></div>
<div class="wrap">
<div class="cards">
<div class="card"><div class="n blue">{report.get('total_objects',0)}</div><div class="l">Total Objects</div></div>
<div class="card"><div class="n blue">{by_type.get('stored_procedure',0)}</div><div class="l">Stored Procs</div></div>
<div class="card"><div class="n blue">{by_type.get('view',0)}</div><div class="l">Views</div></div>
<div class="card"><div class="n blue">{by_type.get('udf',0)}</div><div class="l">UDFs</div></div>
<div class="card"><div class="n blue">{by_type.get('table',0)}</div><div class="l">Tables</div></div>
<div class="card"><div class="n green">{by_tier.get('Simple',0)}</div><div class="l">Simple</div></div>
<div class="card"><div class="n yellow">{by_tier.get('Moderate',0)}</div><div class="l">Moderate</div></div>
<div class="card"><div class="n orange">{by_tier.get('Complex',0)}</div><div class="l">Complex</div></div>
<div class="card"><div class="n red">{by_tier.get('Very Complex',0)}</div><div class="l">Very Complex</div></div>
</div>
<div class="cards">
<div class="card"><div class="n blue">{report.get('total_effort_points',0)}</div><div class="l">Total Story Points</div></div>
<div class="card"><div class="n blue">{report.get('avg_complexity_score',0)}</div><div class="l">Avg Complexity</div></div>
<div class="card"><div class="n green">{report.get('readiness_pct',0)}%</div><div class="l">Auto-Convert Ready</div></div>
<div class="card"><div class="n red">{by_risk.get('Critical',0)+by_risk.get('High',0)}</div><div class="l">High/Critical Risk</div></div>
</div>
<h2>Bill of Materials</h2>
<div style="overflow-x:auto"><table>
<thead><tr><th>Object</th><th>Type</th><th>Lines</th><th>Params/Cols</th><th>Tbl Refs/Rows</th><th>Score</th><th>Tier</th><th>Effort</th><th>Risk</th><th>Readiness</th></tr></thead>
<tbody>{bom_rows}</tbody>
</table></div>
<h2>Root Cause Analysis — Why Objects Are Complex</h2>
{rca_html if rca_html else '<p style="color:#718096;">All objects are simple — no blocking constructs detected.</p>'}
</div>
<footer>SQL → Databricks Migration Studio — Discovery Report — Generated {_h(report.get('scan_timestamp',''))}</footer>
</body></html>"""
    return html


def _h(s) -> str:
    """HTML-escape a string."""
    if s is None:
        return ''
    return str(s).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')
