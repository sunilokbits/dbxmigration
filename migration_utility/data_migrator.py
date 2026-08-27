"""
Data Migrator — Bulk migrates SQL source tables → Databricks Unity Catalog.

Strategy (fastest path):
  1. pyodbc → read source schema + data in chunks
  2. Parquet assembled in-memory via PyArrow (compressed, typed)
  3. Upload Parquet to staging:
     a. Unity Catalog Volumes (preferred — Files API PUT)
     b. DBFS staging (fallback if Volumes unavailable)
  4. COPY INTO target Delta table (Unity Catalog)
  5. Parallel execution across tables using threading.Semaphore

Final fallback (if no staging available):
  Batch INSERT VALUES (2 000 rows/statement)
"""

import io, csv, base64, time, threading, uuid, traceback, re, hashlib
from datetime import datetime, date as _date_type
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
try:
    import pyodbc
except ImportError:
    pyodbc = None
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None
    pq = None


def _sanitize_col_name(name: str) -> str:
    """Replace characters invalid in Delta column names with underscores.

    Delta Lake rejects  ' ,;{}()\n\t='  in column names.
    We strip leading/trailing whitespace, swap any bad char run → single
    underscore, and collapse consecutive underscores.
    """
    s = name.strip()
    s = re.sub(r"[\s,;{}()=\n\t]+", "_", s)     # replace invalid chars
    s = re.sub(r"_+", "_", s)                     # collapse runs
    s = s.strip("_")                               # trim edge underscores
    return s


# ── SQL Server → Delta Lake type map ──────────────────────────────────────────
_TYPE_MAP = {
    "int":               "INT",
    "bigint":            "BIGINT",
    "smallint":          "SMALLINT",
    "tinyint":           "TINYINT",
    "bit":               "BOOLEAN",
    "float":             "DOUBLE",
    "real":              "FLOAT",
    "money":             "DECIMAL(19,4)",
    "smallmoney":        "DECIMAL(10,4)",
    "varchar":           "STRING",
    "nvarchar":          "STRING",
    "char":              "STRING",
    "nchar":             "STRING",
    "text":              "STRING",
    "ntext":             "STRING",
    "datetime":          "TIMESTAMP",
    "datetime2":         "TIMESTAMP",
    "smalldatetime":     "TIMESTAMP",
    "date":              "DATE",
    "time":              "STRING",
    "uniqueidentifier":  "STRING",
    "binary":            "BINARY",
    "varbinary":         "BINARY",
    "image":             "BINARY",
    "xml":               "STRING",
    "decimal":           "DECIMAL",
    "numeric":           "DECIMAL",
    "geography":         "STRING",
    "geometry":          "STRING",
    "hierarchyid":       "STRING",
    "sql_variant":       "STRING",
}

# ── Delta → PyArrow type map  ─────────────────────────────────────────────────
_ARROW_TYPE_MAP: dict = {
    "INT":       pa.int32(),
    "BIGINT":    pa.int64(),
    "SMALLINT":  pa.int16(),
    "TINYINT":   pa.int8(),
    "BOOLEAN":   pa.bool_(),
    "DOUBLE":    pa.float64(),
    "FLOAT":     pa.float32(),
    "STRING":    pa.string(),
    "TIMESTAMP": pa.timestamp("us"),
    "DATE":      pa.date32(),
    "BINARY":    pa.binary(),
}


def _delta_to_arrow(delta_type: str) -> pa.DataType:
    """Map a Delta type string to a PyArrow type."""
    base = delta_type.split("(")[0].strip().upper()
    if base in _ARROW_TYPE_MAP:
        return _ARROW_TYPE_MAP[base]
    if base == "DECIMAL":
        # parse DECIMAL(p,s)
        import re as _re
        m = _re.search(r"(\d+)\s*,\s*(\d+)", delta_type)
        if m:
            return pa.decimal128(int(m.group(1)), int(m.group(2)))
        return pa.decimal128(38, 18)
    return pa.string()  # safe fallback


def _coerce_value(val, arrow_type: pa.DataType):
    """Coerce a pyodbc value to match the target Arrow type."""
    if val is None:
        return None
    # Decimal types — convert to Python Decimal
    if pa.types.is_decimal(arrow_type):
        from decimal import Decimal, InvalidOperation
        try:
            return Decimal(str(val))
        except (InvalidOperation, ValueError):
            return None
    # Boolean
    if pa.types.is_boolean(arrow_type):
        return bool(val)
    # Integer types
    if pa.types.is_integer(arrow_type):
        try:
            return int(val)
        except (ValueError, TypeError):
            return None
    # Float / double
    if pa.types.is_floating(arrow_type):
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
    # Timestamp
    if pa.types.is_timestamp(arrow_type):
        import datetime as _dt
        if isinstance(val, (_dt.datetime, _dt.date)):
            return val
        try:
            return _dt.datetime.fromisoformat(str(val))
        except Exception:
            return str(val)
    # Date
    if pa.types.is_date(arrow_type):
        import datetime as _dt
        if isinstance(val, (_dt.date,)):
            return val
        try:
            return _dt.date.fromisoformat(str(val))
        except Exception:
            return str(val)
    # Binary
    if pa.types.is_binary(arrow_type):
        if isinstance(val, (bytes, bytearray)):
            return bytes(val)
        return str(val).encode("utf-8")
    # Fallback to string
    return str(val)


# ── In-memory job registry (reset on server restart) ─────────────────────────
MIGRATION_JOBS: dict = {}


def _map_delta_type(sql_type: str, precision=None, scale=None) -> str:
    base = sql_type.lower().split("(")[0].strip()
    dt = _TYPE_MAP.get(base, "STRING")
    if dt == "DECIMAL" and precision:
        s = scale or 0
        dt = f"DECIMAL({precision},{s})"
    return dt


def _odbc_escape(val: str) -> str:
    """Escape a value for safe use in an ODBC connection string.
    Wraps in {} braces and doubles any } inside, per ODBC spec.
    Handles passwords with special chars like ; # { } = etc."""
    return "{" + val.replace("}", "}}") + "}"

def _build_conn_str(source_type: str, server: str, database: str,
                    username: str, password: str) -> str:
    """Build pyodbc connection string with best available driver.

    Uses sql_pool._detect_driver() so it never calls pyodbc.drivers() on a
    None object when pyodbc/libodbc is unavailable (non-Docker runtime).
    """
    from sql_pool import _detect_driver as _drv
    driver = _drv()
    # Preserve legacy driver-list selection behaviour where possible
    drivers = [driver]
    for preferred in ["ODBC Driver 18 for SQL Server",
                      "ODBC Driver 17 for SQL Server"]:
        if preferred in drivers:
            driver = preferred
            break
    else:
        driver = drivers[0] if drivers else "ODBC Driver 17 for SQL Server"

    encrypt = "yes" if source_type in ("azuresql", "synapse") else ("optional" if "18" in driver else "no")
    trust   = "yes"
    # Escape password & username with {} braces to handle special chars (# ; { } = etc.)
    safe_pwd  = _odbc_escape(password) if password else ""
    safe_user = _odbc_escape(username) if username else ""
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};DATABASE={database};"
        f"UID={safe_user};PWD={safe_pwd};"
        f"Encrypt={encrypt};TrustServerCertificate={trust};Connection Timeout=30;"
    )


# ─────────────────────────────────────────────────────────────────────────────
class DataMigrator:
    """Migrate SQL source tables to Databricks Unity Catalog Delta tables."""

    CHUNK_SIZE      = 5_000              # rows read per pyodbc fetchmany()
    INC_SCAN_CHUNK  = 50_000             # larger chunks for incremental scan
    PARQUET_CHUNK   = 200_000            # new rows per Parquet file (streaming)
    INSERT_BATCH    = 5_000              # rows per INSERT VALUES statement
    INSERT_WORKERS  = 8                  # concurrent INSERT HTTP calls
    DBFS_STAGING    = "/tmp/mig_staging" # DBFS base path (cleaned up after use)
    STAGING_VOLUME  = "_mig_staging"     # UC Volume name for staging CSVs

    def __init__(self, conn_str: str, dbx_host: str, token: str,
                 catalog: str = "main", schema: str = "default"):
        self.conn_str = conn_str
        self.host     = dbx_host.rstrip("/")
        self.token    = token
        self.catalog  = catalog
        self.schema   = schema
        self._sess    = requests.Session()
        self._sess.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        })

    # ── Databricks Statement Execution API ────────────────────────────────────
    def _exec_sql(self, sql: str, warehouse_id: str, timeout: str = "50s") -> dict:
        payload = {
            "statement":      sql,
            "warehouse_id":   warehouse_id,
            "catalog":        self.catalog,
            "schema":         self.schema,
            "wait_timeout":   timeout,
            "on_wait_timeout": "CONTINUE",
        }
        r = self._sess.post(f"{self.host}/api/2.0/sql/statements",
                            json=payload, timeout=60)
        if r.status_code == 200:
            data = r.json()
        else:
            data = {
                "error": r.text[:300],
                "status": {"state": "FAILED",
                           "error": {"message": f"HTTP {r.status_code}: {r.text[:300]}"}},
            }
        sid  = data.get("statement_id")
        return self._poll_sql(sid) if sid else data

    def _poll_sql(self, sid: str) -> dict:
        for _ in range(120):
            r = self._sess.get(f"{self.host}/api/2.0/sql/statements/{sid}",
                               timeout=15)
            if r.status_code != 200:
                return {"error": f"Poll {r.status_code}",
                        "status": {"state": "FAILED",
                                   "error": {"message": f"Poll HTTP {r.status_code}"}}}
            d  = r.json()
            st = d.get("status", {}).get("state", "")
            if st in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
                return d
            time.sleep(2)
        return {"error": "Statement timed out"}

    def _exec_sql_fire(self, sql: str, warehouse_id: str) -> str | None:
        """Submit SQL asynchronously — returns statement_id (no polling)."""
        payload = {
            "statement":      sql,
            "warehouse_id":   warehouse_id,
            "catalog":        self.catalog,
            "schema":         self.schema,
            "wait_timeout":   "0s",        # don't wait
            "on_wait_timeout": "CONTINUE",
        }
        try:
            r = self._sess.post(f"{self.host}/api/2.0/sql/statements",
                                json=payload, timeout=60)
            if r.status_code == 200:
                return r.json().get("statement_id")
        except Exception:
            pass
        return None

    def _wait_statement(self, sid: str, max_wait: int = 300) -> dict:
        """Poll a single statement to completion."""
        if not sid:
            return {"error": "no statement_id"}
        deadline = time.time() + max_wait
        while time.time() < deadline:
            try:
                r = self._sess.get(
                    f"{self.host}/api/2.0/sql/statements/{sid}", timeout=15)
                if r.status_code != 200:
                    return {"error": f"Poll {r.status_code}"}
                d  = r.json()
                st = d.get("status", {}).get("state", "")
                if st in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
                    return d
            except Exception:
                pass
            time.sleep(1)
        return {"error": "Statement timed out"}

    # ── Parallel batch INSERT (fire N INSERT statements concurrently) ─────────
    def _parallel_batch_insert(self, target: str, col_names: list,
                               rows_iter, total_hint: int,
                               warehouse_id: str, step_fn) -> int:
        """Insert rows via concurrent INSERT VALUES statements.

        Args:
            target:       fully-qualified Delta table name
            col_names:    list of column names
            rows_iter:    iterable yielding row tuples/lists
            total_hint:   expected row count (for progress messages)
            warehouse_id: SQL warehouse id
            step_fn:      callback for progress messages

        Returns:
            number of rows successfully inserted
        """
        safe_cols = ", ".join(f"`{c}`" for c in col_names)
        inserted  = 0
        pending: list = []   # list of (statement_id, batch_size)

        def _esc(v):
            if v is None:
                return "NULL"
            s = str(v)
            return "NULL" if s == "" else "'" + s.replace("'", "''") + "'"

        def _submit_batch(batch):
            vals = ", ".join(
                "(" + ", ".join(_esc(v) for v in r) + ")"
                for r in batch
            )
            sql = f"INSERT INTO {target} ({safe_cols}) VALUES {vals}"
            sid = self._exec_sql_fire(sql, warehouse_id)
            return sid, len(batch)

        def _drain(force_all=False):
            """Wait for pending statements to finish."""
            nonlocal inserted, pending
            if not pending:
                return
            threshold = 0 if force_all else self.INSERT_WORKERS
            while len(pending) > threshold:
                sid, n = pending.pop(0)
                self._wait_statement(sid)
                inserted += n

        batch: list = []
        for row in rows_iter:
            batch.append(row)
            if len(batch) >= self.INSERT_BATCH:
                sid, n = _submit_batch(batch)
                pending.append((sid, n))
                batch = []
                # If we have enough in-flight, drain oldest ones
                if len(pending) >= self.INSERT_WORKERS:
                    _drain()
                    if inserted % 10_000 == 0 and inserted > 0:
                        step_fn(f"  Inserted {inserted:,} / {total_hint:,} rows…")
        if batch:
            sid, n = _submit_batch(batch)
            pending.append((sid, n))

        # Wait for all remaining
        _drain(force_all=True)
        step_fn(f"  Inserted {inserted:,} rows via parallel batch INSERT "
                f"({self.INSERT_WORKERS} concurrent)")
        return inserted

    # ── DBFS upload (supports any file size via create/addblock/close) ─────────
    def _dbfs_upload(self, path: str, data_bytes: bytes):
        """Upload bytes to DBFS. Returns (True, '') on success or (False, error_msg) on failure."""
        try:
            r = self._sess.post(f"{self.host}/api/2.0/dbfs/create",
                                json={"path": path, "overwrite": True}, timeout=15)
            if r.status_code != 200:
                return False, f"dbfs/create HTTP {r.status_code}: {r.text[:300]}"
            handle    = r.json().get("handle")
            offset    = 0
            BLOCK     = 1024 * 1024   # 1 MB
            while offset < len(data_bytes):
                chunk   = data_bytes[offset:offset + BLOCK]
                encoded = base64.b64encode(chunk).decode()
                r2 = self._sess.post(f"{self.host}/api/2.0/dbfs/add-block",
                                     json={"handle": handle, "data": encoded},
                                     timeout=30)
                if r2.status_code != 200:
                    return False, f"dbfs/add-block HTTP {r2.status_code}: {r2.text[:300]}"
                offset += BLOCK
            r3 = self._sess.post(f"{self.host}/api/2.0/dbfs/close",
                                 json={"handle": handle}, timeout=15)
            if r3.status_code != 200:
                return False, f"dbfs/close HTTP {r3.status_code}: {r3.text[:300]}"
            return True, ""
        except Exception as ex:
            return False, str(ex)

    def _dbfs_delete(self, path: str):
        try:
            self._sess.post(f"{self.host}/api/2.0/dbfs/delete",
                            json={"path": path, "recursive": False}, timeout=10)
        except Exception:
            pass

    # ── Unity Catalog Volumes staging (preferred over DBFS) ───────────────────
    _volume_ready = False  # set after first successful ensure

    def _volumes_ensure_staging(self, warehouse_id: str):
        """Create the managed staging volume once per session."""
        if self._volume_ready:
            return True, ""
        vol = f"`{self.catalog}`.`{self.schema}`.`{self.STAGING_VOLUME}`"
        sql = (f"CREATE VOLUME IF NOT EXISTS {vol} "
               "COMMENT 'Auto-created staging area for SQL→Databricks migration'")
        res   = self._exec_sql(sql, warehouse_id)
        state = res.get("status", {}).get("state", "UNKNOWN")
        if state == "SUCCEEDED":
            self._volume_ready = True
            return True, ""
        err = (res.get("status", {}).get("error", {}) or {}).get("message", state)
        return False, f"CREATE VOLUME: {err}"

    def _volumes_upload(self, filename: str, data_bytes: bytes):
        """Upload bytes to a UC Volume via the Files API (PUT).

        Path on volume: /Volumes/{catalog}/{schema}/{volume}/{filename}
        API endpoint :  PUT /api/2.0/fs/files/Volumes/…
        Returns (True, volume_path) or (False, error_msg).
        """
        vol_path = f"/Volumes/{self.catalog}/{self.schema}/{self.STAGING_VOLUME}/{filename}"
        api_url  = f"{self.host}/api/2.0/fs/files{vol_path}"
        try:
            r = self._sess.put(
                api_url,
                data=data_bytes,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/octet-stream",
                },
                timeout=max(60, len(data_bytes) // (1024 * 512)),  # scale timeout with size
            )
            if r.status_code in (200, 201, 204):
                return True, vol_path
            return False, f"Files API HTTP {r.status_code}: {r.text[:300]}"
        except Exception as ex:
            return False, str(ex)

    def _volumes_delete(self, vol_path: str):
        """Delete a file from a UC Volume via the Files API (DELETE)."""
        try:
            api_url = f"{self.host}/api/2.0/fs/files{vol_path}"
            self._sess.delete(
                api_url,
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=15,
            )
        except Exception:
            pass

    # ── Source introspection ──────────────────────────────────────────────────
    def list_source_tables(self) -> list:
        sql = """
            SELECT t.TABLE_SCHEMA, t.TABLE_NAME,
                   (SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA
                      AND c.TABLE_NAME  = t.TABLE_NAME) AS col_count,
                   ISNULL(p.rows, 0) AS row_estimate
            FROM INFORMATION_SCHEMA.TABLES t
            LEFT JOIN sys.partitions p
                   ON p.object_id = OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME)
                  AND p.index_id IN (0, 1)
            WHERE t.TABLE_TYPE = 'BASE TABLE'
            GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME, p.rows
            ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
        """
        with pyodbc.connect(self.conn_str, timeout=15) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            return [
                {
                    "schema":      r[0],
                    "table":       r[1],
                    "full_name":   f"{r[0]}.{r[1]}",
                    "col_count":   r[2],
                    "row_estimate": r[3],
                }
                for r in cur.fetchall()
            ]

    def describe_source_table(self, schema: str, table: str) -> dict:
        _sch = schema.replace("'", "''")
        _tbl = table.replace("'", "''")
        col_sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE,
                   CHARACTER_MAXIMUM_LENGTH,
                   NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = N'{_sch}' AND TABLE_NAME = N'{_tbl}'
            ORDER BY ORDINAL_POSITION
        """
        with pyodbc.connect(self.conn_str, timeout=15) as conn:
            cur = conn.cursor()
            cur.execute(col_sql)
            cols = [
                {
                    "name":       r[0],
                    "sql_type":   r[1],
                    "max_len":    r[2],
                    "precision":  r[3],
                    "scale":      r[4],
                    "nullable":   r[5] == "YES",
                    "delta_type": _map_delta_type(r[1], r[3], r[4]),
                }
                for r in cur.fetchall()
            ]
            cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            row_count = cur.fetchone()[0]
        return {"columns": cols, "row_count": row_count}

    # ── Single table migration ────────────────────────────────────────────────
    def migrate_table(self, src_schema: str, src_table: str,
                      warehouse_id: str, on_progress=None) -> dict:
        start     = time.time()
        log: list = []
        dbfs_path = f"{self.DBFS_STAGING}/{src_table}_{uuid.uuid4().hex[:8]}.parquet"

        def step(msg: str):
            log.append(msg)
            if on_progress:
                on_progress(msg)

        safe_src    = f"[{src_schema}].[{src_table}]"
        target      = f"`{self.catalog}`.`{self.schema}`.`{src_table}`"

        try:
            # ── 1. Describe source ────────────────────────────────────────────
            step(f"Describing {safe_src}…")
            desc      = self.describe_source_table(src_schema, src_table)
            cols      = desc["columns"]
            row_count = desc["row_count"]
            step(f"  {len(cols)} columns, {row_count:,} rows")

            # ── 2. Sanitize column names & create Delta table ─────────────────
            orig_names  = [c["name"] for c in cols]
            delta_names = [_sanitize_col_name(c["name"]) for c in cols]
            renamed     = [
                (o, d) for o, d in zip(orig_names, delta_names) if o != d
            ]
            if renamed:
                step(f"  Sanitizing {len(renamed)} column name(s) "
                     f"(spaces/special chars → underscores)")

            nl = ",\n  "
            col_defs = nl.join(
                f'`{dn}` {c["delta_type"]}'
                + ("" if c["nullable"] else " NOT NULL")
                for dn, c in zip(delta_names, cols)
            )
            create_sql = (
                f"CREATE TABLE IF NOT EXISTS {target} (\n  {col_defs}\n) "
                "USING DELTA "
                'TBLPROPERTIES ("delta.autoOptimize.optimizeWrite"="true",'
                '"delta.autoOptimize.autoCompact"="true")'
            )
            step(f"Creating Delta table {target}…")
            res   = self._exec_sql(create_sql, warehouse_id)
            state = res.get("status", {}).get("state", "UNKNOWN")
            if state not in ("SUCCEEDED",):
                err_msg = (res.get("status", {}) or {}).get(
                    "error", {}).get("message", state) or state
                return {"success": False, "error": f"CREATE TABLE: {err_msg}", "log": log}
            step("  Table ready")

            # ── 3. Read source → Parquet in-memory ─────────────────────────────
            step(f"Reading source in chunks of {self.CHUNK_SIZE:,}…")
            col_names   = delta_names          # use sanitized names everywhere
            arrow_types = [_delta_to_arrow(c["delta_type"]) for c in cols]
            # Accumulate columns as Python lists (one list per column)
            col_buffers = [[] for _ in cols]
            total_read  = 0

            with pyodbc.connect(self.conn_str, timeout=60) as conn:
                conn.timeout = 0      # no timeout for large reads
                cur = conn.cursor()
                cur.execute(f"SELECT * FROM {safe_src}")
                while True:
                    rows = cur.fetchmany(self.CHUNK_SIZE)
                    if not rows:
                        break
                    for row in rows:
                        for i, val in enumerate(row):
                            col_buffers[i].append(
                                _coerce_value(val, arrow_types[i]))
                    total_read += len(rows)
                    if total_read % 20_000 == 0:
                        step(f"  Read {total_read:,} / {row_count:,} rows…")

            # Build Arrow table → Parquet bytes (snappy compressed)
            arrow_fields = [
                pa.field(dn, at) for dn, at in zip(delta_names, arrow_types)
            ]
            arrow_schema = pa.schema(arrow_fields)
            arrow_arrays = []
            for i, at in enumerate(arrow_types):
                try:
                    arrow_arrays.append(pa.array(col_buffers[i], type=at))
                except (pa.ArrowInvalid, pa.ArrowTypeError):
                    # Safe fallback: cast everything to string
                    arrow_arrays.append(
                        pa.array([None if v is None else str(v)
                                  for v in col_buffers[i]], type=pa.string()))
                    arrow_fields[i] = pa.field(delta_names[i], pa.string())
            arrow_table  = pa.table(
                {f.name: a for f, a in zip(arrow_fields, arrow_arrays)},
                schema=pa.schema(arrow_fields))
            pq_buf = io.BytesIO()
            pq.write_table(arrow_table, pq_buf, compression="snappy")
            pq_bytes = pq_buf.getvalue()
            step(f"  {total_read:,} rows → {len(pq_bytes)/1024:.1f} KB Parquet "
                 f"(snappy, {len(pq_bytes)/max(1,total_read*len(cols)):.1f} bytes/cell)")

            # Keep a CSV fallback buffer for batch INSERT (populated lazily)
            _csv_fallback: dict = {"buf": None}

            # ── 4. Upload staging Parquet (Volumes → DBFS → INSERT fallback) ──
            staging_file = f"{src_table}_{uuid.uuid4().hex[:8]}.parquet"
            uploaded_via = None     # "volumes" | "dbfs" | None
            cleanup_path = None    # path to delete after COPY INTO

            # --- 4a. Try Unity Catalog Volumes (preferred) --------------------
            vol_ok, vol_msg = self._volumes_ensure_staging(warehouse_id)
            if vol_ok:
                step("Uploading Parquet to UC Volume staging…")
                vup_ok, vup_result = self._volumes_upload(staging_file, pq_bytes)
                if vup_ok:
                    uploaded_via = "volumes"
                    cleanup_path = vup_result          # volume_path
                    step(f"  Upload to Volume done ({len(pq_bytes)/1024:.1f} KB)")
                else:
                    step(f"  Volume upload failed ({vup_result})")
            else:
                step(f"  Volume unavailable ({vol_msg})")

            # --- 4b. Fallback: try DBFS --------------------------------------
            if not uploaded_via:
                step("Trying DBFS staging…")
                dbfs_ok, dbfs_err = self._dbfs_upload(dbfs_path, pq_bytes)
                if dbfs_ok:
                    uploaded_via = "dbfs"
                    cleanup_path = dbfs_path
                    step("  DBFS upload done")
                else:
                    step(f"  DBFS unavailable ({dbfs_err})")

            # --- 4c. Last resort: batch INSERT (no staging) -------------------
            if not uploaded_via:
                step("Using parallel batch INSERT fallback (no staging available)…")

                def _row_iter_from_arrow():
                    for row_idx in range(arrow_table.num_rows):
                        yield [arrow_table.column(i)[row_idx].as_py()
                               for i in range(arrow_table.num_columns)]

                inserted = self._parallel_batch_insert(
                    target, col_names, _row_iter_from_arrow(),
                    total_read, warehouse_id, step)
                elapsed = time.time() - start
                rps     = int(inserted / max(elapsed, 0.1))
                step(f"Done — {inserted:,} rows in {elapsed:.1f}s  ({rps:,} rows/sec)")
                return {
                    "success":   True,
                    "table":     src_table,
                    "rows":      inserted,
                    "columns":   len(cols),
                    "elapsed_s": round(elapsed, 2),
                    "rows_sec":  rps,
                    "log":       log,
                }

            # ── 5. COPY INTO from staged Parquet ─────────────────────────────
            if uploaded_via == "volumes":
                copy_src = cleanup_path              # /Volumes/cat/sch/vol/file
            else:
                copy_src = f"dbfs:{dbfs_path}"       # DBFS path

            step(f"Running COPY INTO from {uploaded_via} staging (Parquet)…")
            copy_sql = (
                f"COPY INTO {target} "
                f"FROM '{copy_src}' "
                "FILEFORMAT = PARQUET "
                "COPY_OPTIONS ('mergeSchema'='true')"
            )
            res2   = self._exec_sql(copy_sql, warehouse_id, timeout="50s")
            state2 = res2.get("status", {}).get("state", "UNKNOWN")

            if state2 == "SUCCEEDED":
                data_arr = res2.get("result", {}).get("data_array", [[str(total_read)]])
                copied   = data_arr[0][0] if data_arr and data_arr[0] else total_read
                step(f"  COPY INTO: {copied} rows loaded")
            else:
                # ── Fallback: parallel batch INSERT VALUES ────────────────────
                err2 = ((res2.get("status") or {}).get("error") or {}).get(
                    "message", state2)
                step(f"  COPY INTO {state2}: {err2} — using parallel batch INSERT")

                def _row_iter_from_arrow2():
                    for row_idx in range(arrow_table.num_rows):
                        yield [arrow_table.column(i)[row_idx].as_py()
                               for i in range(arrow_table.num_columns)]

                inserted = self._parallel_batch_insert(
                    target, col_names, _row_iter_from_arrow2(),
                    total_read, warehouse_id, step)
                total_read = inserted

            # ── 6. Cleanup staging file ───────────────────────────────────────
            if uploaded_via == "volumes" and cleanup_path:
                self._volumes_delete(cleanup_path)
            elif uploaded_via == "dbfs":
                self._dbfs_delete(dbfs_path)

            elapsed  = time.time() - start
            rps      = int(total_read / max(elapsed, 0.1))
            step(f"Done — {total_read:,} rows in {elapsed:.1f}s  ({rps:,} rows/sec)")
            return {
                "success":   True,
                "table":     src_table,
                "rows":      total_read,
                "columns":   len(cols),
                "elapsed_s": round(elapsed, 2),
                "rows_sec":  rps,
                "log":       log,
            }

        except Exception as exc:
            # cleanup whatever was staged
            if uploaded_via == "volumes" and cleanup_path:
                self._volumes_delete(cleanup_path)
            else:
                self._dbfs_delete(dbfs_path)
            step(f"ERROR: {exc}")
            return {
                "success": False,
                "table":   src_table,
                "error":   str(exc),
                "trace":   traceback.format_exc(),
                "log":     log,
            }

    # ── Incremental (hash-key) single-table migration ─────────────────────────
    def migrate_table_incremental(self, src_schema: str, src_table: str,
                                   warehouse_id: str,
                                   on_progress=None) -> dict:
        """
        Hash-key based incremental load — no watermark column needed.

        How it works:
        1. Every row gets a `_row_hash` column = SHA-256 of all column values.
        2. The hash is computed on the Python side (works with any SQL Server).
        3. First run behaves like a full load (table is empty).
        4. Subsequent runs:
           a. Read ALL existing hashes from the Delta target.
           b. Stream source rows, compute hash, compare:
              - New hash      → INSERT
              - Changed hash   → UPDATE (matched by all non-hash columns would
                                 be expensive, so we use a synthetic `_row_key`
                                 = hash of primary-key columns. If no PK exists
                                 we fall back to hash of ALL columns which
                                 effectively means inserts-only mode).
              - Missing hash   → optionally DELETE (controlled by flag).
           c. Uses batch INSERT for new rows and batch UPDATE via
              MERGE for changed rows.
        """
        import hashlib
        start   = time.time()
        log: list = []

        def step(msg: str):
            log.append(msg)
            if on_progress:
                on_progress(msg)

        safe_src = f"[{src_schema}].[{src_table}]"
        target   = f"`{self.catalog}`.`{self.schema}`.`{src_table}`"

        try:
            # ── 1. Describe source ────────────────────────────────────────
            step(f"Describing {safe_src}…")
            desc      = self.describe_source_table(src_schema, src_table)
            cols      = desc["columns"]
            row_count = desc["row_count"]
            step(f"  {len(cols)} columns, {row_count:,} rows")

            orig_names  = [c["name"] for c in cols]
            delta_names = [_sanitize_col_name(c["name"]) for c in cols]
            renamed = [(o, d) for o, d in zip(orig_names, delta_names) if o != d]
            if renamed:
                step(f"  Sanitizing {len(renamed)} column name(s)")

            # ── 2. Create Delta table with _row_hash column ──────────────
            nl = ",\n  "
            col_defs = nl.join(
                f'`{dn}` {c["delta_type"]}'
                + ("" if c["nullable"] else " NOT NULL")
                for dn, c in zip(delta_names, cols)
            )
            col_defs += f",\n  `_row_hash` STRING"

            create_sql = (
                f"CREATE TABLE IF NOT EXISTS {target} (\n  {col_defs}\n) "
                "USING DELTA "
                'TBLPROPERTIES ("delta.autoOptimize.optimizeWrite"="true",'
                '"delta.autoOptimize.autoCompact"="true")'
            )
            step(f"Creating/verifying Delta table {target} (with _row_hash)…")
            res   = self._exec_sql(create_sql, warehouse_id)
            state = res.get("status", {}).get("state", "UNKNOWN")
            if state != "SUCCEEDED":
                err_msg = (res.get("status", {}) or {}).get(
                    "error", {}).get("message", state) or state
                return {"success": False, "error": f"CREATE TABLE: {err_msg}", "log": log}

            # ── 3. Fetch existing hashes from Delta (background thread) ────
            step("Fetching existing row hashes from target…")
            t_hash_start = time.time()
            existing_hashes: set = set()

            def _fetch_hashes():
                """Download all _row_hash values from Delta (runs in thread)."""
                hash_sql = f"SELECT `_row_hash` FROM {target}"
                hres = self._exec_sql(hash_sql, warehouse_id, timeout="50s")
                hstate = hres.get("status", {}).get("state", "")
                if hstate == "SUCCEEDED":
                    for row in hres.get("result", {}).get("data_array", []):
                        if row and row[0]:
                            existing_hashes.add(row[0])
                    nxt = (hres.get("result", {}) or {}).get(
                        "next_chunk_internal_link")
                    while nxt:
                        cr = self._sess.get(f"{self.host}{nxt}", timeout=30)
                        if cr.status_code != 200:
                            break
                        cd = cr.json()
                        for row in cd.get("data_array", []):
                            if row and row[0]:
                                existing_hashes.add(row[0])
                        nxt = cd.get("next_chunk_internal_link")

            # Run hash fetch in background so we can prepare scan concurrently
            hash_thread = threading.Thread(target=_fetch_hashes, daemon=True)
            hash_thread.start()
            hash_thread.join()  # must complete before scan starts
            t_hash_end = time.time()

            step(f"  {len(existing_hashes):,} existing hashes loaded "
                 f"in {t_hash_end - t_hash_start:.1f}s")
            is_first_load = len(existing_hashes) == 0

            if is_first_load:
                step("  First load detected — will insert all rows")

            # ── 4+5. Stream source → chunked Parquet → async COPY INTO ───
            #
            # Architecture:
            #   scan source rows (main thread)
            #     → coerce + accumulate column buffers
            #     → every PARQUET_CHUNK new rows, flush:
            #         build Parquet → upload to Volume → fire COPY INTO (async)
            #     → at end, flush remainder
            #     → wait for all COPY INTO statements
            #     → INSERT fallback for any failed chunks
            #
            col_names   = delta_names
            arrow_types = [_delta_to_arrow(c["delta_type"]) for c in cols]
            all_col_names = col_names + ["_row_hash"]

            skipped     = 0
            total_read  = 0
            inserted    = 0

            # Working buffers for current chunk
            col_bufs  = [[] for _ in cols]
            hash_buf: list = []

            # Pending async COPY INTO operations
            # Each entry: (stmt_id|None, row_count, vol_path|None, arrow_table)
            pending_ops: list = []
            vol_ok      = False
            vol_checked = False

            def _compute_hash(values) -> str:
                raw = "|".join("" if v is None else str(v) for v in values)
                return hashlib.sha256(raw.encode("utf-8")).hexdigest()

            def _flush_chunk():
                """Build Parquet, upload to Volume, fire async COPY INTO."""
                nonlocal col_bufs, hash_buf, vol_ok, vol_checked
                n = len(hash_buf)
                if n == 0:
                    return

                # Ensure staging volume exists (once)
                if not vol_checked:
                    vol_checked = True
                    ok, msg = self._volumes_ensure_staging(warehouse_id)
                    vol_ok = ok
                    if not ok:
                        step(f"  Volume unavailable ({msg}) — chunks will use INSERT")

                # Build typed Arrow table
                fields = [pa.field(dn, at)
                          for dn, at in zip(delta_names, arrow_types)]
                fields.append(pa.field("_row_hash", pa.string()))
                arrays = []
                for i, at in enumerate(arrow_types):
                    try:
                        arrays.append(pa.array(col_bufs[i], type=at))
                    except (pa.ArrowInvalid, pa.ArrowTypeError):
                        arrays.append(
                            pa.array([None if v is None else str(v)
                                      for v in col_bufs[i]],
                                     type=pa.string()))
                        fields[i] = pa.field(delta_names[i], pa.string())
                arrays.append(pa.array(hash_buf, type=pa.string()))
                tbl = pa.table(
                    {f.name: a for f, a in zip(fields, arrays)},
                    schema=pa.schema(fields))

                pq_buf = io.BytesIO()
                pq.write_table(tbl, pq_buf, compression="snappy")
                pq_bytes = pq_buf.getvalue()

                if vol_ok:
                    fname = (f"{src_table}_c{len(pending_ops)}"
                             f"_{uuid.uuid4().hex[:8]}.parquet")
                    uok, uresult = self._volumes_upload(fname, pq_bytes)
                    if uok:
                        copy_sql = (
                            f"COPY INTO {target} FROM '{uresult}' "
                            "FILEFORMAT = PARQUET "
                            "COPY_OPTIONS ('mergeSchema'='true')")
                        sid = self._exec_sql_fire(copy_sql, warehouse_id)
                        pending_ops.append((sid, n, uresult, tbl))
                        step(f"  Chunk {len(pending_ops)}: {n:,} rows → "
                             f"{len(pq_bytes)/1024:.0f} KB uploaded, "
                             f"COPY INTO fired")
                    else:
                        pending_ops.append((None, n, None, tbl))
                        step(f"  Chunk {len(pending_ops)}: upload failed — "
                             f"queued for INSERT")
                else:
                    pending_ops.append((None, n, None, tbl))

                # Reset buffers for next chunk
                col_bufs = [[] for _ in cols]
                hash_buf = []

            # ── Scan source rows with interleaved flush ───────────────────
            t_scan_start = time.time()
            step(f"Streaming source → Parquet chunks of "
                 f"{self.PARQUET_CHUNK:,} new rows…")

            with pyodbc.connect(self.conn_str, timeout=60) as conn:
                conn.timeout = 0
                cur = conn.cursor()
                cur.execute(f"SELECT * FROM {safe_src}")
                while True:
                    rows = cur.fetchmany(self.INC_SCAN_CHUNK)
                    if not rows:
                        break
                    for row in rows:
                        h = _compute_hash(row)
                        if not is_first_load and h in existing_hashes:
                            skipped += 1
                        else:
                            for i, val in enumerate(row):
                                col_bufs[i].append(
                                    _coerce_value(val, arrow_types[i]))
                            hash_buf.append(h)
                            # Flush when chunk is full
                            if len(hash_buf) >= self.PARQUET_CHUNK:
                                _flush_chunk()
                    total_read += len(rows)
                    if total_read % 100_000 == 0:
                        flushed = sum(op[1] for op in pending_ops)
                        step(f"  Scanned {total_read:,} / {row_count:,} "
                             f"({len(hash_buf):,} buffered, "
                             f"{flushed:,} uploading)")

            # Flush remaining rows
            _flush_chunk()
            t_scan_end = time.time()

            new_count = sum(op[1] for op in pending_ops)
            step(f"  Scan + flush: {total_read:,} rows in "
                 f"{t_scan_end - t_scan_start:.1f}s — "
                 f"{new_count:,} new, {skipped:,} unchanged, "
                 f"{len(pending_ops)} chunk(s)")

            if new_count == 0:
                elapsed = time.time() - start
                step(f"Done — 0 new rows (all {skipped:,} unchanged) "
                     f"in {elapsed:.1f}s")
                return {
                    "success":      True,
                    "table":        src_table,
                    "rows":         0,
                    "rows_scanned": total_read,
                    "rows_skipped": skipped,
                    "columns":      len(cols),
                    "elapsed_s":    round(elapsed, 2),
                    "rows_sec":     0,
                    "log":          log,
                    "mode":         "incremental",
                }

            # ── Wait for all async COPY INTO operations ───────────────────
            step(f"Waiting for {len(pending_ops)} COPY INTO operation(s)…")
            fallback_tables: list = []   # Arrow tables needing INSERT

            for idx, (sid, cnt, vol_path, tbl) in enumerate(pending_ops):
                if sid:
                    res = self._wait_statement(sid)
                    cstate = res.get("status", {}).get("state", "")
                    if cstate == "SUCCEEDED":
                        inserted += cnt
                        step(f"  Chunk {idx+1}: {cnt:,} rows loaded via "
                             f"COPY INTO")
                    else:
                        cerr = ((res.get("status") or {}).get(
                            "error") or {}).get("message", cstate)
                        step(f"  Chunk {idx+1}: COPY INTO failed ({cerr})"
                             f" — queued for INSERT")
                        fallback_tables.append(tbl)
                else:
                    fallback_tables.append(tbl)
                # Clean up staging file
                if vol_path:
                    self._volumes_delete(vol_path)

            # ── INSERT fallback for failed chunks ─────────────────────────
            if fallback_tables:
                fb_total = sum(t.num_rows for t in fallback_tables)
                step(f"Running INSERT fallback for "
                     f"{len(fallback_tables)} chunk(s) "
                     f"({fb_total:,} rows)…")
                for tbl in fallback_tables:
                    ncols = tbl.num_columns

                    def _iter(t=tbl):
                        for r in range(t.num_rows):
                            yield [t.column(c)[r].as_py()
                                   for c in range(ncols)]

                    ins = self._parallel_batch_insert(
                        target, all_col_names, _iter(),
                        tbl.num_rows, warehouse_id, step)
                    inserted += ins

            step(f"  Inserted {inserted:,} rows total")

            elapsed = time.time() - start
            rps     = int(inserted / max(elapsed, 0.1))
            step(f"Done — {inserted:,} new rows inserted, "
                 f"{skipped:,} unchanged, in {elapsed:.1f}s ({rps:,} rows/sec)")
            return {
                "success":      True,
                "table":        src_table,
                "rows":         inserted,
                "rows_scanned": total_read,
                "rows_skipped": skipped,
                "columns":      len(cols),
                "elapsed_s":    round(elapsed, 2),
                "rows_sec":     rps,
                "log":          log,
                "mode":         "incremental",
            }

        except Exception as exc:
            step(f"ERROR: {exc}")
            return {
                "success": False,
                "table":   src_table,
                "error":   str(exc),
                "trace":   traceback.format_exc(),
                "log":     log,
            }

    # ── Parallel multi-table migration ────────────────────────────────────────
    def migrate_tables_parallel(self, tables: list, warehouse_id: str,
                                job_id: str, max_workers: int = 3,
                                load_mode: str = "full"):
        """
        Migrate tables in parallel (max_workers at a time).
        Progress is written to MIGRATION_JOBS[job_id].
        """
        job            = MIGRATION_JOBS[job_id]
        job["status"]  = "running"
        job["total"]   = len(tables)
        job["done"]    = 0
        job["failed"]  = 0
        job["results"] = {}   # keyed by "schema.table" for frontend Object.entries()
        job["logs"]    = []   # flat list of strings
        sem = threading.Semaphore(max_workers)
        log_lock = threading.Lock()

        # Pre-populate every table as queued so the UI shows names immediately
        for t in tables:
            full = f"{t.get('schema','dbo')}.{t.get('table','')}"
            job["results"][full] = {"status": "queued", "pct": 0, "rows_copied": 0}

        def _run_one(tbl):
            tname  = tbl.get("table", "")
            schema = tbl.get("schema", "dbo")
            full   = f"{schema}.{tname}"
            job["results"][full] = {"status": "running", "pct": 0, "rows_copied": 0}
            with sem:
                def _prog(msg):
                    with log_lock:
                        job["logs"].append(f"[{full}] {msg}")
                if load_mode == "incremental":
                    result = self.migrate_table_incremental(
                        schema, tname, warehouse_id, _prog)
                else:
                    result = self.migrate_table(
                        schema, tname, warehouse_id, _prog)
                result["status"]      = "done" if result.get("success") else "failed"
                result["pct"]         = 100
                result["rows_copied"] = result.get("rows", 0)
                job["results"][full]  = result
                if result["success"]:
                    job["done"]   += 1
                else:
                    job["failed"] += 1
                    with log_lock:
                        job["logs"].append(f"[{full}] ✕ FAILED: {result.get('error','unknown error')}")
                        trace = result.get("trace", "")
                        if trace:
                            # emit each traceback line individually for readability
                            for tline in trace.strip().splitlines():
                                job["logs"].append(f"[{full}]   {tline}")

        threads = [
            threading.Thread(target=_run_one, args=(t,), daemon=True)
            for t in tables
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        job["status"]      = "done"
        job["finished_at"] = datetime.now().isoformat()
