"""Chunked, parallel, resumable Extract for large database tables.

Runs the generated notebook's chunking code against a simulated source and an
in-memory dbutils.fs, so planning, commit-on-complete, resume and row-count
verification are exercised exactly as generated.
"""

import json
import os
import re
import sys
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metadata_notebooks import generate_metadata_notebooks

LANDING = "/Volumes/dbx_volumes/sales/landing"
DEST = f"{LANDING}/CATALOG_SALES"


def _chunk_block():
    code = next(n["code"] for n in generate_metadata_notebooks("m", "s", "/l", "/Shared/x", "dlt")["notebooks"]
                if n["name"] == "01_Meta_Extract")
    start = code.index("import hashlib\nfrom concurrent.futures")
    end = code.index("# Read from source", start)
    return code[start:end]


class FakeFS:
    """Directory-aware in-memory dbutils.fs."""

    def __init__(self):
        self.files = {}  # path -> content (dirs implied by prefixes)

    def put(self, path, content, overwrite=False):
        self.files[path] = content

    def head(self, path, max_bytes=65536):
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    def rm(self, path, recurse=False):
        for k in [k for k in self.files if k == path or k.startswith(path + "/")]:
            del self.files[k]

    def mkdirs(self, path):
        pass

    def ls(self, path):
        names = {k[len(path) + 1:].split("/")[0] + "/" for k in self.files if k.startswith(path + "/")}
        if not names:
            raise FileNotFoundError(path)
        return [types.SimpleNamespace(name=n) for n in sorted(names)]

    def mv(self, src, dst, recurse=False):
        for k in [k for k in self.files if k.startswith(src + "/")]:
            self.files[dst + k[len(src):]] = self.files.pop(k)


class FakeSource:
    """Answers the generated SQL like a table of `rows_per_value` rows per key value."""

    def __init__(self, mn, mx, rows_per_value, nulls=0, fail_on=()):
        self.mn, self.mx, self.rpv, self.nulls = mn, mx, rows_per_value, nulls
        self.fail_on = set(fail_on)
        self.selects = []

    def total(self):
        return (self.mx - self.mn + 1) * self.rpv + self.nulls

    def rows_for(self, where):
        if "IS NULL" in where:
            return self.nulls
        lo, hi = map(int, re.search(r'>= (-?\d+) AND .* < (-?\d+)', where).groups())
        lo, hi = max(lo, self.mn), min(hi, self.mx + 1)
        return max(0, hi - lo) * self.rpv

    def read(self, sql_query=None, table=None):
        q = sql_query or ""
        if "INFORMATION_SCHEMA.COLUMNS" in q:
            rows = [("CS_SOLD_DATE_SK",), ("CS_ITEM_SK",), ("CS_QUANTITY",)]
        elif q.startswith("SELECT MIN("):
            rows = [(self.mn, self.mx, self.total() - self.nulls)]
        elif q.startswith("SELECT COUNT(1)"):
            rows = [(self.total(),)]
        elif q.startswith("SELECT * FROM"):
            where = q.split(" WHERE ", 1)[1]
            self.selects.append(where)
            return FakeDF(self, where)
        else:
            raise AssertionError(q)
        return mock.MagicMock(collect=mock.MagicMock(return_value=rows))


class FakeDF:
    def __init__(self, src, where):
        self.src, self.where = src, where

    def withColumn(self, *_):
        return self

    @property
    def write(self):
        df = self

        class W:
            def mode(self, _):
                return self

            def parquet(self, path):
                if any(tag in df.where for tag in df.src.fail_on):
                    raise RuntimeError("Snowflake query failed")
                fs.put(f"{path}/part-0.parquet", str(df.src.rows_for(df.where)))
        return W()


fs = None


class TestChunkedExtract(unittest.TestCase):
    def _run(self, src, cfg=None, wm_col="", fs_obj=None, run_ts="20261013_120000", extra_rows=0):
        global fs
        fs = fs_obj or FakeFS()

        def landed_count():
            return sum(int(v) for k, v in fs.files.items() if k.startswith(DEST + "/") and k.endswith(".parquet")) + extra_rows

        spark = mock.MagicMock()
        spark.read.option.return_value.parquet.return_value.count.side_effect = landed_count
        g = {"source_config": cfg or {}, "LANDING_PATH": LANDING, "TABLE_NAME": "CATALOG_SALES",
             "TABLE_SCHEMA": "TPCDS_SF10TCL", "FULL_TABLE": "TPCDS_SF10TCL.CATALOG_SALES", "IS_FILE_SOURCE": False,
             "use_incremental": False, "watermark": None, "WM_COL": wm_col, "_qcol": lambda c: f'"{c}"',
             "_sql_esc": lambda v: str(v), "_CONNECTOR": "snowflake",
             "_qualified_table": '"TPCDS_SF10TCL"."CATALOG_SALES"', "run_ts": run_ts, "RUN_ID": "r1",
             "JOB_ID": "j1", "SERVER": "acct", "DATABASE": "db", "F": mock.MagicMock(), "landing_dest": DEST,
             "_cnt_sql": 'SELECT COUNT(1) AS cnt FROM "TPCDS_SF10TCL"."CATALOG_SALES"',
             "_read_source": src.read, "dbutils": types.SimpleNamespace(fs=fs), "spark": spark, "json": json}
        exec(compile(_chunk_block(), "chunks", "exec"), g)
        return g, fs

    def test_plan_splits_on_key_column_and_covers_nulls(self):
        src = FakeSource(2450815, 2452654, 7_800_000, nulls=5_000_000)  # ~14.4B rows
        g, _ = self._run(src)
        plan = g["_plan_chunks"](src.total())
        self.assertEqual(plan["column"], "CS_SOLD_DATE_SK")
        self.assertEqual(len(plan["chunks"]), 144 + 1)
        self.assertEqual(plan["chunks"][-1][0], "chunk_nulls")
        self.assertIn('"CS_SOLD_DATE_SK" >= 2450815 AND', plan["chunks"][0][1])
        self.assertIn("< 2452655", plan["chunks"][-2][1])  # last range includes MAX

    def test_split_column_setting_and_watermark_preference(self):
        src = FakeSource(1, 1000, 1_000_000)
        g, _ = self._run(src, cfg={"split_column": "CS_ITEM_SK"})
        self.assertEqual(g["_plan_chunks"](src.total())["column"], "CS_ITEM_SK")
        g, _ = self._run(src, wm_col="CS_QUANTITY")
        self.assertEqual(g["_plan_chunks"](src.total())["column"], "CS_QUANTITY")

    def test_full_run_commits_every_chunk_and_verifies_count(self):
        src = FakeSource(1, 1000, 1_000_000, nulls=10)  # 1B rows -> 10 chunks + nulls
        g, f = self._run(src)
        plan = g["_plan_chunks"](src.total())
        landed = g["_extract_chunked"](plan, src.total())
        self.assertEqual(landed, src.total())
        chunk_dirs = {k.split("/")[-2] for k in f.files if k.startswith(DEST + "/")}
        self.assertEqual(len(chunk_dirs), len(plan["chunks"]))
        self.assertFalse([k for k in f.files if "/_staging/CATALOG_SALES/chunk_" in k])  # all moved
        self.assertEqual(json.loads(f.files[f"{LANDING}/_staging/CATALOG_SALES/_plan.json"])["status"], "complete")

    def test_failure_keeps_done_chunks_and_rerun_resumes(self):
        src = FakeSource(1, 1000, 1_000_000, fail_on=("< 301",))
        g, f = self._run(src)
        plan = g["_plan_chunks"](src.total())
        with self.assertRaises(RuntimeError) as ctx:
            g["_extract_chunked"](plan, src.total())
        self.assertIn("next run resumes", str(ctx.exception))
        self.assertEqual(len({k.split("/")[-2] for k in f.files if k.startswith(DEST + "/")}), 9)

        src.fail_on, src.selects = set(), []
        g2, f = self._run(src, fs_obj=f, run_ts="20261013_140000")
        landed = g2["_extract_chunked"](g2["_plan_chunks"](src.total()), src.total())
        self.assertEqual(len(src.selects), 1)  # only the failed chunk re-read
        self.assertEqual(landed, src.total())
        self.assertEqual(g2["run_ts"], "20261013_120000")  # same batch id as the first attempt

    def test_count_mismatch_fails_and_forces_fresh_restart(self):
        src = FakeSource(1, 1000, 1_000_000)
        g, f = self._run(src, extra_rows=5)
        with self.assertRaises(RuntimeError) as ctx:
            g["_extract_chunked"](g["_plan_chunks"](src.total()), src.total())
        self.assertIn("Row count mismatch", str(ctx.exception))
        self.assertEqual(json.loads(f.files[f"{LANDING}/_staging/CATALOG_SALES/_plan.json"])["status"], "invalid")
        src.selects = []
        g2, f = self._run(src, fs_obj=f)
        g2["_extract_chunked"](g2["_plan_chunks"](src.total()), src.total())
        self.assertEqual(len(src.selects), 10)  # everything re-extracted

    def test_small_range_is_not_chunked(self):
        src = FakeSource(5, 5, 300_000_000)
        g, _ = self._run(src)
        self.assertIsNone(g["_plan_chunks"](src.total()))


class TestNotebookWiring(unittest.TestCase):
    def test_extract_uses_chunks_only_for_large_database_reads(self):
        code = next(n["code"] for n in generate_metadata_notebooks("m", "s", "/l", "/Shared/x", "dlt")["notebooks"]
                    if n["name"] == "01_Meta_Extract")
        self.assertIn("if not IS_FILE_SOURCE and _est_count and _est_count > _CHUNK_ROWS:", code)
        self.assertIn("row_count = _extract_chunked(_plan, _est_count)", code)
        self.assertNotIn("Pre-computed watermark", code)  # dead extra source pass removed

    def test_bronze_reads_chunk_subfolders(self):
        nbs = {n["name"]: n["code"] for n in generate_metadata_notebooks("m", "s", "/l", "/Shared/x", "standard")["notebooks"]}
        self.assertIn('spark.read.option("recursiveFileLookup", "true").parquet(landing_src)', nbs["02_Meta_Bronze"])
        sdp = next(n["code"] for n in generate_metadata_notebooks("m", "s", "/l", "/Shared/x", "dlt")["notebooks"]
                   if n["name"] == "02_Meta_SDP_Pipeline")
        self.assertIn('option("recursiveFileLookup", "true").load(_landing)', sdp)


if __name__ == "__main__":
    unittest.main()
