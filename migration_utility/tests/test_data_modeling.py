"""
test_data_modeling.py — Comprehensive tests for the AI Data Modeling module.

Run:   python -m pytest test_data_modeling.py -v
"""

import copy
import json
import math
import sys
import os
import unittest
from unittest.mock import MagicMock, patch

# Ensure module is importable
sys.path.insert(0, os.path.dirname(__file__))
import data_modeling as dm


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures / helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fact_table(name="sales_transactions", extra_cols=None):
    """Return a prototypical FACT table metadata dict."""
    cols = [
        {"name": "transaction_id", "data_type": "BIGINT", "is_nullable": False, "is_pk": True},
        {"name": "customer_id", "data_type": "INT", "is_nullable": False},
        {"name": "product_id", "data_type": "INT", "is_nullable": False},
        {"name": "store_id", "data_type": "INT", "is_nullable": False},
        {"name": "order_date", "data_type": "DATE", "is_nullable": False},
        {"name": "quantity", "data_type": "INT", "is_nullable": True},
        {"name": "total_amount", "data_type": "DECIMAL(18,2)", "is_nullable": True},
        {"name": "discount", "data_type": "FLOAT", "is_nullable": True},
    ]
    if extra_cols:
        cols.extend(extra_cols)
    return {"table_name": name, "columns": cols}


def _dim_table(name="customer", extra_cols=None):
    """Return a prototypical DIMENSION table metadata dict."""
    cols = [
        {"name": "customer_id", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "first_name", "data_type": "NVARCHAR(100)", "is_nullable": True},
        {"name": "last_name", "data_type": "NVARCHAR(100)", "is_nullable": True},
        {"name": "email", "data_type": "VARCHAR(255)", "is_nullable": True},
    ]
    if extra_cols:
        cols.extend(extra_cols)
    return {"table_name": name, "columns": cols}


def _sample_tables_meta():
    """Return a realistic set of tables for full pipeline tests."""
    return [
        _fact_table("sales_transactions"),
        _dim_table("customer"),
        _dim_table("product", extra_cols=[
            {"name": "product_name", "data_type": "NVARCHAR(200)", "is_nullable": True},
            {"name": "category_id", "data_type": "INT", "is_nullable": True},
        ]),
        _dim_table("store", extra_cols=[
            {"name": "store_name", "data_type": "VARCHAR(100)", "is_nullable": True},
            {"name": "region_id", "data_type": "INT", "is_nullable": True},
        ]),
    ]


def _make_model():
    """Helper: build a classified model from sample tables."""
    return dm.classify_tables(_sample_tables_meta())


# ─────────────────────────────────────────────────────────────────────────────
# 1. _score_table tests
# ─────────────────────────────────────────────────────────────────────────────

class TestScoreTable(unittest.TestCase):

    def test_fact_keyword_positive_score(self):
        """Table name with fact keywords scores positive."""
        for name in ("order_details", "sales", "invoice_line_items", "payment_log", "fact_events"):
            cols = [{"name": "id", "data_type": "INT"}, {"name": "amount", "data_type": "DECIMAL(10,2)"}]
            score = dm._score_table(name, cols)
            self.assertGreaterEqual(score, 1, f"{name} should have positive score, got {score}")

    def test_dim_keyword_negative_score(self):
        """Table name with dim keywords scores negative."""
        for name in ("customer", "dim_product", "department", "region_lookup", "category"):
            cols = [{"name": "id", "data_type": "INT"}, {"name": "name", "data_type": "VARCHAR(100)"}]
            score = dm._score_table(name, cols)
            self.assertLess(score, 0, f"{name} should have negative score, got {score}")

    def test_many_measures_boost_fact(self):
        """Tables with >30% numeric columns get a +2 boost."""
        cols = [
            {"name": "id", "data_type": "INT"},
            {"name": "amount", "data_type": "DECIMAL(10,2)"},
            {"name": "qty", "data_type": "INT"},
            {"name": "price", "data_type": "FLOAT"},
            {"name": "note", "data_type": "VARCHAR(100)"},
        ]
        score = dm._score_table("generic_data", cols)
        # 3 out of 5 columns = 60% measures → +2 boost, no keyword match
        self.assertGreaterEqual(score, 2)

    def test_many_fk_cols_boost_fact(self):
        """Tables with ≥3 FK-pattern columns get a +2 boost."""
        cols = [
            {"name": "id", "data_type": "INT"},
            {"name": "customer_id", "data_type": "INT"},
            {"name": "product_id", "data_type": "INT"},
            {"name": "store_id", "data_type": "INT"},
        ]
        score = dm._score_table("generic_data", cols)
        self.assertGreaterEqual(score, 2)

    def test_few_columns_low_measures_favors_dim(self):
        """Small table with few numeric columns gets -2 score."""
        cols = [
            {"name": "code", "data_type": "VARCHAR(10)"},
            {"name": "name", "data_type": "VARCHAR(100)"},
            {"name": "label", "data_type": "VARCHAR(50)"},
        ]
        score = dm._score_table("generic_data", cols)
        # 0 measures / 3 cols = 0%, ≤5 cols → -2
        self.assertLessEqual(score, -1)

    def test_neutral_table_name(self):
        """Table with no matching keywords scored only by column analysis."""
        cols = [{"name": "id", "data_type": "INT"}, {"name": "value", "data_type": "VARCHAR(100)"}]
        score = dm._score_table("data_table", cols)
        # No keyword match, 1/2 measure = 50% → +2, ≤5 cols & <20%? boundary case
        self.assertIsInstance(score, int)


# ─────────────────────────────────────────────────────────────────────────────
# 2. classify_tables tests
# ─────────────────────────────────────────────────────────────────────────────

class TestClassifyTables(unittest.TestCase):

    def test_basic_classification(self):
        model = dm.classify_tables(_sample_tables_meta())
        fact_names = {f["table_name"] for f in model["facts"]}
        dim_names = {d["table_name"] for d in model["dimensions"]}

        self.assertIn("sales_transactions", fact_names)
        self.assertIn("customer", dim_names)
        self.assertIn("product", dim_names)
        self.assertIn("store", dim_names)

    def test_roles_assigned(self):
        model = dm.classify_tables(_sample_tables_meta())
        for f in model["facts"]:
            self.assertEqual(f["role"], "fact")
        for d in model["dimensions"]:
            self.assertEqual(d["role"], "dimension")

    def test_relationships_detected(self):
        """FK-pattern columns should generate relationships."""
        model = dm.classify_tables(_sample_tables_meta())
        rel_targets = {r["to"] for r in model["relationships"]}
        # sales_transactions has customer_id, product_id, store_id
        self.assertTrue(rel_targets & {"customer", "product", "store"})

    def test_star_schema_detected(self):
        """No dim-to-dim relationships → star schema."""
        tables = [
            _fact_table(),
            {"table_name": "customer", "columns": [
                {"name": "id", "data_type": "INT", "is_nullable": False, "is_pk": True},
                {"name": "name", "data_type": "VARCHAR(100)", "is_nullable": True},
            ]},
            {"table_name": "product", "columns": [
                {"name": "id", "data_type": "INT", "is_nullable": False, "is_pk": True},
                {"name": "title", "data_type": "VARCHAR(200)", "is_nullable": True},
            ]},
        ]
        model = dm.classify_tables(tables)
        self.assertEqual(model["schema_type"], "star")

    def test_snowflake_schema_detected(self):
        """Dim-to-dim relationships → snowflake schema."""
        tables = [
            _fact_table(),
            _dim_table("customer"),
            # product has a category_id FK → category = dim referencing dim
            _dim_table("product", extra_cols=[
                {"name": "category_id", "data_type": "INT", "is_nullable": True},
            ]),
            _dim_table("category"),
        ]
        model = dm.classify_tables(tables)
        self.assertEqual(model["schema_type"], "snowflake")

    def test_empty_input(self):
        model = dm.classify_tables([])
        self.assertEqual(model["facts"], [])
        self.assertEqual(model["dimensions"], [])
        self.assertEqual(model["relationships"], [])
        self.assertEqual(model["schema_type"], "star")

    def test_single_table(self):
        model = dm.classify_tables([_fact_table()])
        self.assertEqual(len(model["facts"]), 1)
        self.assertEqual(len(model["dimensions"]), 0)

    def test_all_dims(self):
        tables = [_dim_table("customer"), _dim_table("department"), _dim_table("region")]
        model = dm.classify_tables(tables)
        self.assertEqual(len(model["facts"]), 0)
        self.assertGreaterEqual(len(model["dimensions"]), 2)

    def test_fk_table_explicit_reference(self):
        """Explicit fk_table field on a column should create a relationship."""
        tables = [{
            "table_name": "order_details",
            "columns": [
                {"name": "id", "data_type": "INT", "is_pk": True},
                {"name": "cust_ref", "data_type": "INT", "fk_table": "customer"},
                {"name": "amount", "data_type": "DECIMAL(10,2)"},
            ],
        }, _dim_table("customer")]
        model = dm.classify_tables(tables)
        from_tables = [r["from"] for r in model["relationships"]]
        self.assertIn("order_details", from_tables)


# ─────────────────────────────────────────────────────────────────────────────
# 3. generate_er_json tests
# ─────────────────────────────────────────────────────────────────────────────

class TestGenerateERJson(unittest.TestCase):

    def test_output_structure(self):
        model = _make_model()
        er = dm.generate_er_json(model)
        self.assertIn("nodes", er)
        self.assertIn("edges", er)
        self.assertIn("schema_type", er)

    def test_node_count_matches_tables(self):
        model = _make_model()
        er = dm.generate_er_json(model)
        expected = len(model["facts"]) + len(model["dimensions"])
        self.assertEqual(len(er["nodes"]), expected)

    def test_edge_count_matches_relationships(self):
        model = _make_model()
        er = dm.generate_er_json(model)
        self.assertEqual(len(er["edges"]), len(model["relationships"]))

    def test_node_has_required_fields(self):
        er = dm.generate_er_json(_make_model())
        for n in er["nodes"]:
            self.assertIn("id", n)
            self.assertIn("label", n)
            self.assertIn("type", n)
            self.assertIn("x", n)
            self.assertIn("y", n)
            self.assertIn("columns", n)
            self.assertIn(n["type"], ("fact", "dimension"))

    def test_edge_has_required_fields(self):
        er = dm.generate_er_json(_make_model())
        for e in er["edges"]:
            self.assertIn("from", e)
            self.assertIn("to", e)
            self.assertIn("label", e)

    def test_fact_nodes_positioned_center(self):
        er = dm.generate_er_json(_make_model())
        fact_nodes = [n for n in er["nodes"] if n["type"] == "fact"]
        for fn in fact_nodes:
            # Fact starts at y=300 per the code
            self.assertEqual(fn["y"], 300)

    def test_single_dimension_no_crash(self):
        """Single dimension should not divide by zero in angle calc."""
        model = {"facts": [], "dimensions": [
            {"table_name": "dim_a", "columns": [], "role": "dimension", "score": -3}
        ], "relationships": [], "schema_type": "star"}
        er = dm.generate_er_json(model)
        self.assertEqual(len(er["nodes"]), 1)


# ─────────────────────────────────────────────────────────────────────────────
# 4. generate_ddl tests
# ─────────────────────────────────────────────────────────────────────────────

class TestGenerateDDL(unittest.TestCase):

    def setUp(self):
        self.model = _make_model()
        self.ddl = dm.generate_ddl(self.model, "bronze", "hr")

    def test_ddl_contains_catalog_and_schema(self):
        self.assertIn("USE CATALOG bronze;", self.ddl)
        self.assertIn("USE SCHEMA hr;", self.ddl)

    def test_ddl_contains_create_table(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS", self.ddl)

    def test_ddl_uses_delta(self):
        self.assertIn("USING DELTA", self.ddl)

    def test_ddl_dimensions_before_facts(self):
        """Dimensions should appear before facts in DDL to satisfy FK ordering."""
        dim_pos = self.ddl.find("DIMENSION:")
        fact_pos = self.ddl.find("FACT:")
        if dim_pos != -1 and fact_pos != -1:
            self.assertLess(dim_pos, fact_pos)

    def test_ddl_schema_type_header(self):
        self.assertIn("Schema Type:", self.ddl)
        self.assertIn(self.model["schema_type"].upper(), self.ddl)

    def test_ddl_fk_constraints(self):
        if self.model["relationships"]:
            self.assertIn("ALTER TABLE", self.ddl)
            self.assertIn("ADD CONSTRAINT", self.ddl)
            self.assertIn("FOREIGN KEY", self.ddl)

    def test_ddl_custom_catalog_schema(self):
        ddl = dm.generate_ddl(self.model, "gold", "analytics")
        self.assertIn("USE CATALOG gold;", ddl)
        self.assertIn("USE SCHEMA analytics;", ddl)

    def test_ddl_not_null_for_non_nullable(self):
        """Non-nullable columns should have NOT NULL in DDL."""
        self.assertIn("NOT NULL", self.ddl)

    def test_ddl_with_empty_model(self):
        model = {"facts": [], "dimensions": [], "relationships": [], "schema_type": "star"}
        ddl = dm.generate_ddl(model, "main", "default")
        self.assertIn("USE CATALOG main;", ddl)
        # No CREATE TABLE expected
        self.assertNotIn("CREATE TABLE", ddl)


# ─────────────────────────────────────────────────────────────────────────────
# 5. _map_to_databricks_type tests
# ─────────────────────────────────────────────────────────────────────────────

class TestMapToDatabricksType(unittest.TestCase):

    def test_string_types(self):
        for t in ("VARCHAR", "NVARCHAR(100)", "CHAR(10)", "NCHAR(5)", "TEXT", "NTEXT"):
            self.assertEqual(dm._map_to_databricks_type(t), "STRING")

    def test_int_types(self):
        self.assertEqual(dm._map_to_databricks_type("INT"), "INT")
        self.assertEqual(dm._map_to_databricks_type("INTEGER"), "INT")
        self.assertEqual(dm._map_to_databricks_type("BIGINT"), "BIGINT")
        self.assertEqual(dm._map_to_databricks_type("SMALLINT"), "SMALLINT")
        self.assertEqual(dm._map_to_databricks_type("TINYINT"), "TINYINT")

    def test_float_types(self):
        self.assertEqual(dm._map_to_databricks_type("FLOAT"), "FLOAT")
        self.assertEqual(dm._map_to_databricks_type("REAL"), "FLOAT")
        self.assertEqual(dm._map_to_databricks_type("DOUBLE"), "DOUBLE")

    def test_decimal_preserves_precision(self):
        self.assertEqual(dm._map_to_databricks_type("DECIMAL(18,2)"), "DECIMAL(18,2)")

    def test_numeric_maps_to_decimal(self):
        result = dm._map_to_databricks_type("NUMERIC(10,4)")
        self.assertIn("DECIMAL", result)

    def test_money_types(self):
        self.assertEqual(dm._map_to_databricks_type("MONEY"), "DECIMAL(19,4)")
        self.assertEqual(dm._map_to_databricks_type("SMALLMONEY"), "DECIMAL(10,4)")

    def test_datetime_types(self):
        self.assertEqual(dm._map_to_databricks_type("DATE"), "DATE")
        for t in ("DATETIME", "DATETIME2", "SMALLDATETIME", "TIMESTAMP", "DATETIMEOFFSET"):
            self.assertEqual(dm._map_to_databricks_type(t), "TIMESTAMP")

    def test_boolean_types(self):
        self.assertEqual(dm._map_to_databricks_type("BIT"), "BOOLEAN")

    def test_binary_types(self):
        for t in ("BINARY", "VARBINARY", "IMAGE"):
            self.assertEqual(dm._map_to_databricks_type(t), "BINARY")

    def test_unknown_defaults_to_string(self):
        self.assertEqual(dm._map_to_databricks_type("XML"), "STRING")
        self.assertEqual(dm._map_to_databricks_type("CUSTOM_TYPE"), "STRING")

    def test_uniqueidentifier(self):
        self.assertEqual(dm._map_to_databricks_type("UNIQUEIDENTIFIER"), "STRING")


# ─────────────────────────────────────────────────────────────────────────────
# 6. apply_manual_edits tests
# ─────────────────────────────────────────────────────────────────────────────

class TestApplyManualEdits(unittest.TestCase):

    def test_role_change_fact_to_dim(self):
        model = _make_model()
        fact_name = model["facts"][0]["table_name"]
        edits = {"role_changes": [{"table_name": fact_name, "new_role": "dimension"}]}
        updated = dm.apply_manual_edits(model, edits)
        dim_names = {d["table_name"] for d in updated["dimensions"]}
        fact_names = {f["table_name"] for f in updated["facts"]}
        self.assertIn(fact_name, dim_names)
        self.assertNotIn(fact_name, fact_names)

    def test_role_change_dim_to_fact(self):
        model = _make_model()
        dim_name = model["dimensions"][0]["table_name"]
        edits = {"role_changes": [{"table_name": dim_name, "new_role": "fact"}]}
        updated = dm.apply_manual_edits(model, edits)
        fact_names = {f["table_name"] for f in updated["facts"]}
        self.assertIn(dim_name, fact_names)

    def test_add_relationship(self):
        model = _make_model()
        before = len(model["relationships"])
        edits = {"relationship_adds": [{"from": "customer", "to": "store", "type": "many-to-one"}]}
        updated = dm.apply_manual_edits(model, edits)
        self.assertEqual(len(updated["relationships"]), before + 1)

    def test_add_duplicate_relationship_ignored(self):
        model = _make_model()
        if model["relationships"]:
            existing = model["relationships"][0]
            before = len(model["relationships"])
            edits = {"relationship_adds": [{"from": existing["from"], "to": existing["to"]}]}
            updated = dm.apply_manual_edits(model, edits)
            self.assertEqual(len(updated["relationships"]), before)

    def test_remove_relationship(self):
        model = _make_model()
        if model["relationships"]:
            to_remove = model["relationships"][0]
            before = len(model["relationships"])
            edits = {"relationship_removes": [{"from": to_remove["from"], "to": to_remove["to"]}]}
            updated = dm.apply_manual_edits(model, edits)
            self.assertEqual(len(updated["relationships"]), before - 1)

    def test_column_rename(self):
        model = _make_model()
        tbl = (model["facts"] + model["dimensions"])[0]
        col = tbl["columns"][0]["name"]
        edits = {"column_edits": [{"table_name": tbl["table_name"], "col_name": col, "changes": {"new_name": "renamed_col"}}]}
        dm.apply_manual_edits(model, edits)
        self.assertEqual(tbl["columns"][0]["name"], "renamed_col")

    def test_column_retype(self):
        model = _make_model()
        tbl = (model["facts"] + model["dimensions"])[0]
        col = tbl["columns"][0]["name"]
        edits = {"column_edits": [{"table_name": tbl["table_name"], "col_name": col, "changes": {"data_type": "STRING"}}]}
        dm.apply_manual_edits(model, edits)
        self.assertEqual(tbl["columns"][0]["data_type"], "STRING")

    def test_schema_type_recalculated_after_edits(self):
        """After adding dim-to-dim relationship, schema should become snowflake."""
        model = _make_model()
        dim_names = [d["table_name"] for d in model["dimensions"]]
        if len(dim_names) >= 2:
            edits = {"relationship_adds": [{"from": dim_names[0], "to": dim_names[1]}]}
            updated = dm.apply_manual_edits(model, edits)
            self.assertEqual(updated["schema_type"], "snowflake")

    def test_empty_edits(self):
        model = _make_model()
        original = copy.deepcopy(model)
        dm.apply_manual_edits(model, {})
        self.assertEqual(len(model["facts"]), len(original["facts"]))
        self.assertEqual(len(model["dimensions"]), len(original["dimensions"]))


# ─────────────────────────────────────────────────────────────────────────────
# 7. _guess_fk_column tests
# ─────────────────────────────────────────────────────────────────────────────

class TestGuessFKColumn(unittest.TestCase):

    def test_standard_id_suffix(self):
        model = _make_model()
        result = dm._guess_fk_column("sales_transactions", "customer", model)
        self.assertEqual(result, "customer_id")

    def test_explicit_fk_table(self):
        model = {"facts": [{"table_name": "orders", "columns": [
            {"name": "cust_ref", "data_type": "INT", "fk_table": "customer"},
        ]}], "dimensions": [_dim_table("customer")], "relationships": [], "schema_type": "star"}
        result = dm._guess_fk_column("orders", "customer", model)
        self.assertEqual(result, "cust_ref")

    def test_no_matching_column_returns_none(self):
        model = {"facts": [{"table_name": "orders", "columns": [
            {"name": "id", "data_type": "INT"},
        ]}], "dimensions": [], "relationships": [], "schema_type": "star"}
        result = dm._guess_fk_column("orders", "nonexistent_dim", model)
        self.assertIsNone(result)


# ─────────────────────────────────────────────────────────────────────────────
# 8. _esc sanitization tests
# ─────────────────────────────────────────────────────────────────────────────

class TestEsc(unittest.TestCase):

    def test_normal_string(self):
        self.assertEqual(dm._esc("hello"), "hello")

    def test_single_quotes_escaped(self):
        self.assertEqual(dm._esc("O'Brien"), "O''Brien")

    def test_semicolons_removed(self):
        self.assertNotIn(";", dm._esc("DROP TABLE; --"))

    def test_backslashes_escaped(self):
        self.assertIn("\\\\", dm._esc("a\\b"))


# ─────────────────────────────────────────────────────────────────────────────
# 9. fetch_table_metadata tests (mocked executor)
# ─────────────────────────────────────────────────────────────────────────────

class TestFetchTableMetadata(unittest.TestCase):

    def _mock_executor(self, rows):
        executor = MagicMock()
        executor.execute_custom_sql.return_value = {
            "success": True,
            "rows": rows,
            "columns": ["column_name", "full_data_type", "is_nullable"],
            "row_count": len(rows),
        }
        return executor

    def test_returns_columns_from_rows(self):
        rows = [
            ["employee_id", "INT", "NO"],
            ["name", "VARCHAR(100)", "YES"],
            ["salary", "DECIMAL(18,2)", "YES"],
        ]
        executor = self._mock_executor(rows)
        result = dm.fetch_table_metadata(executor, "bronze", "hr", ["employees"], "wh-123")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["table_name"], "employees")
        self.assertEqual(len(result[0]["columns"]), 3)
        self.assertEqual(result[0]["columns"][0]["name"], "employee_id")
        self.assertTrue(result[0]["columns"][0]["is_pk"])   # ends with _id
        self.assertFalse(result[0]["columns"][0]["is_nullable"])  # NO

    def test_nullable_mapping(self):
        rows = [["col1", "INT", "YES"], ["col2", "INT", "NO"]]
        executor = self._mock_executor(rows)
        result = dm.fetch_table_metadata(executor, "cat", "sch", ["t1"], "wh")
        self.assertTrue(result[0]["columns"][0]["is_nullable"])
        self.assertFalse(result[0]["columns"][1]["is_nullable"])

    def test_fallback_on_empty_response(self):
        executor = MagicMock()
        executor.execute_custom_sql.return_value = {"success": True, "rows": []}
        result = dm.fetch_table_metadata(executor, "c", "s", ["t"], "wh")
        self.assertEqual(len(result[0]["columns"]), 1)
        self.assertEqual(result[0]["columns"][0]["name"], "id")

    def test_fallback_on_exception(self):
        executor = MagicMock()
        executor.execute_custom_sql.side_effect = Exception("Timeout")
        result = dm.fetch_table_metadata(executor, "c", "s", ["t"], "wh")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["columns"][0]["name"], "id")

    def test_multiple_tables(self):
        rows = [["col_a", "STRING", "YES"]]
        executor = self._mock_executor(rows)
        result = dm.fetch_table_metadata(executor, "c", "s", ["t1", "t2", "t3"], "wh")
        self.assertEqual(len(result), 3)
        self.assertEqual(executor.execute_custom_sql.call_count, 3)

    def test_warehouse_id_passed_to_executor(self):
        executor = self._mock_executor([["id", "INT", "NO"]])
        dm.fetch_table_metadata(executor, "c", "s", ["t"], "my-wh-id")
        executor.execute_custom_sql.assert_called_once()
        args = executor.execute_custom_sql.call_args
        self.assertEqual(args[0][1], "my-wh-id")

    def test_warehouse_id_fallback_from_executor(self):
        executor = MagicMock()
        executor.warehouse_id = "fallback-wh"
        executor.execute_custom_sql.return_value = {"success": True, "rows": [["id", "INT", "NO"]]}
        dm.fetch_table_metadata(executor, "c", "s", ["t"], None)
        args = executor.execute_custom_sql.call_args
        self.assertEqual(args[0][1], "fallback-wh")


# ─────────────────────────────────────────────────────────────────────────────
# 10. list_available_tables tests (mocked executor)
# ─────────────────────────────────────────────────────────────────────────────

class TestListAvailableTables(unittest.TestCase):

    def test_returns_table_names(self):
        executor = MagicMock()
        executor.execute_custom_sql.return_value = {
            "success": True,
            "rows": [["employees"], ["departments"], ["jobs"]],
        }
        result = dm.list_available_tables(executor, "bronze", "hr", "wh-123")
        self.assertEqual(result, ["employees", "departments", "jobs"])

    def test_empty_on_failure(self):
        executor = MagicMock()
        executor.execute_custom_sql.return_value = {"success": False, "rows": []}
        result = dm.list_available_tables(executor, "c", "s", "wh")
        self.assertEqual(result, [])

    def test_empty_on_exception(self):
        executor = MagicMock()
        executor.execute_custom_sql.side_effect = Exception("Boom")
        result = dm.list_available_tables(executor, "c", "s", "wh")
        self.assertEqual(result, [])

    def test_sql_uses_catalog_schema(self):
        executor = MagicMock()
        executor.execute_custom_sql.return_value = {"success": True, "rows": []}
        dm.list_available_tables(executor, "my_cat", "my_sch", "wh")
        sql_arg = executor.execute_custom_sql.call_args[0][0]
        self.assertIn("my_cat.information_schema.tables", sql_arg)
        self.assertIn("my_sch", sql_arg)


# ─────────────────────────────────────────────────────────────────────────────
# 11. _move_table_role tests
# ─────────────────────────────────────────────────────────────────────────────

class TestMoveTableRole(unittest.TestCase):

    def test_move_fact_to_dimension(self):
        model = _make_model()
        fact = model["facts"][0]["table_name"]
        dm._move_table_role(model, fact, "dimension")
        dim_names = {d["table_name"] for d in model["dimensions"]}
        self.assertIn(fact, dim_names)

    def test_move_dimension_to_fact(self):
        model = _make_model()
        dim = model["dimensions"][0]["table_name"]
        dm._move_table_role(model, dim, "fact")
        fact_names = {f["table_name"] for f in model["facts"]}
        self.assertIn(dim, fact_names)

    def test_move_nonexistent_table_no_crash(self):
        model = _make_model()
        before_facts = len(model["facts"])
        before_dims = len(model["dimensions"])
        dm._move_table_role(model, "nonexistent_table", "fact")
        self.assertEqual(len(model["facts"]), before_facts)
        self.assertEqual(len(model["dimensions"]), before_dims)


# ─────────────────────────────────────────────────────────────────────────────
# 12. _edit_column tests
# ─────────────────────────────────────────────────────────────────────────────

class TestEditColumn(unittest.TestCase):

    def test_rename_column(self):
        model = _make_model()
        tbl = model["facts"][0]
        col = tbl["columns"][0]["name"]
        dm._edit_column(model, tbl["table_name"], col, {"new_name": "new_col_name"})
        self.assertEqual(tbl["columns"][0]["name"], "new_col_name")

    def test_change_data_type(self):
        model = _make_model()
        tbl = model["facts"][0]
        col = tbl["columns"][0]["name"]
        dm._edit_column(model, tbl["table_name"], col, {"data_type": "STRING"})
        self.assertEqual(tbl["columns"][0]["data_type"], "STRING")

    def test_change_nullable(self):
        model = _make_model()
        tbl = model["facts"][0]
        col = tbl["columns"][0]["name"]
        dm._edit_column(model, tbl["table_name"], col, {"is_nullable": True})
        self.assertTrue(tbl["columns"][0]["is_nullable"])

    def test_change_pk(self):
        model = _make_model()
        tbl = model["facts"][0]
        col = tbl["columns"][1]["name"]
        dm._edit_column(model, tbl["table_name"], col, {"is_pk": True})
        self.assertTrue(tbl["columns"][1]["is_pk"])

    def test_edit_nonexistent_column_no_crash(self):
        model = _make_model()
        tbl = model["facts"][0]
        dm._edit_column(model, tbl["table_name"], "nonexistent_col", {"new_name": "x"})
        # Should not crash


# ─────────────────────────────────────────────────────────────────────────────
# 13. End-to-end pipeline test
# ─────────────────────────────────────────────────────────────────────────────

class TestEndToEndPipeline(unittest.TestCase):

    def test_full_pipeline(self):
        """Classify → ER JSON → DDL → Edit → Regenerate."""
        # Step 1: Classify
        model = dm.classify_tables(_sample_tables_meta())
        self.assertGreater(len(model["facts"]) + len(model["dimensions"]), 0)

        # Step 2: ER JSON
        er = dm.generate_er_json(model)
        self.assertEqual(len(er["nodes"]), len(model["facts"]) + len(model["dimensions"]))

        # Step 3: DDL
        ddl = dm.generate_ddl(model, "bronze", "hr")
        self.assertIn("CREATE TABLE", ddl)
        self.assertIn("USING DELTA", ddl)

        # Step 4: Manual edit — promote a dimension to fact
        if model["dimensions"]:
            dim_name = model["dimensions"][0]["table_name"]
            edits = {"role_changes": [{"table_name": dim_name, "new_role": "fact"}]}
            model = dm.apply_manual_edits(model, edits)
            fact_names = {f["table_name"] for f in model["facts"]}
            self.assertIn(dim_name, fact_names)

        # Step 5: Re-generate ER + DDL after edits
        er2 = dm.generate_er_json(model)
        ddl2 = dm.generate_ddl(model, "bronze", "hr")
        self.assertEqual(len(er2["nodes"]), len(model["facts"]) + len(model["dimensions"]))
        self.assertIn("CREATE TABLE", ddl2)

    def test_add_and_remove_relationship_cycle(self):
        model = _make_model()
        dims = [d["table_name"] for d in model["dimensions"]]
        if len(dims) >= 2:
            # Add
            model = dm.apply_manual_edits(model, {
                "relationship_adds": [{"from": dims[0], "to": dims[1]}]
            })
            found = any(r["from"] == dims[0] and r["to"] == dims[1] for r in model["relationships"])
            self.assertTrue(found)
            self.assertEqual(model["schema_type"], "snowflake")

            # Remove
            model = dm.apply_manual_edits(model, {
                "relationship_removes": [{"from": dims[0], "to": dims[1]}]
            })
            found = any(r["from"] == dims[0] and r["to"] == dims[1] for r in model["relationships"])
            self.assertFalse(found)


# ─────────────────────────────────────────────────────────────────────────────
# 14. Flask API route tests
# ─────────────────────────────────────────────────────────────────────────────

class TestFlaskRoutes(unittest.TestCase):
    """Test the Flask API routes for data modeling."""

    @classmethod
    def setUpClass(cls):
        """Import and prepare the Flask test client."""
        import app as flask_app
        cls.flask_app = flask_app
        cls.app = flask_app.app
        cls.app.config["TESTING"] = True
        cls.client = cls.app.test_client()

    def test_catalogs_schemas_endpoint(self):
        resp = self.client.get("/api/datamodel/catalogs-schemas")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["success"])
        self.assertIn("catalog_schemas", data)
        self.assertIsInstance(data["catalog_schemas"], list)

    @patch("data_modeling.list_available_tables", return_value=["employees", "departments"])
    @patch("app._dm_get_warehouse", return_value="wh-123")
    def test_tables_endpoint(self, mock_wh, mock_list):
        resp = self.client.post(
            "/api/datamodel/tables",
            json={"catalog": "bronze", "schema": "hr"},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["success"])
        self.assertEqual(data["tables"], ["employees", "departments"])

    def test_tables_endpoint_missing_params(self):
        resp = self.client.post(
            "/api/datamodel/tables",
            json={"catalog": "", "schema": ""},
        )
        data = resp.get_json()
        self.assertFalse(data["success"])
        self.assertIn("required", data["error"].lower())

    @patch("data_modeling.fetch_table_metadata")
    @patch("app._dm_get_warehouse", return_value="wh-123")
    def test_generate_endpoint(self, mock_wh, mock_fetch):
        mock_fetch.return_value = _sample_tables_meta()
        resp = self.client.post(
            "/api/datamodel/generate",
            json={"catalog": "bronze", "schema": "hr", "tables": ["sales_transactions", "customer"]},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["success"])
        self.assertIn("model_id", data)
        self.assertIn("er_json", data)
        self.assertIn("ddl", data)
        self.assertIn("schema_type", data)

    def test_generate_endpoint_no_tables(self):
        resp = self.client.post(
            "/api/datamodel/generate",
            json={"catalog": "bronze", "schema": "hr", "tables": []},
        )
        data = resp.get_json()
        self.assertFalse(data["success"])

    @patch("data_modeling.fetch_table_metadata")
    @patch("app._dm_get_warehouse", return_value="wh-123")
    def test_edit_endpoint(self, mock_wh, mock_fetch):
        mock_fetch.return_value = _sample_tables_meta()
        # First generate
        gen_resp = self.client.post(
            "/api/datamodel/generate",
            json={"catalog": "bronze", "schema": "hr", "tables": ["sales_transactions", "customer"]},
        )
        model_id = gen_resp.get_json()["model_id"]

        # Now edit
        resp = self.client.post(
            "/api/datamodel/edit",
            json={
                "model_id": model_id,
                "catalog": "bronze",
                "schema": "hr",
                "edits": {
                    "role_changes": [{"table_name": "customer", "new_role": "fact"}],
                },
            },
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["success"])
        fact_names = [f["table_name"] for f in data["facts"]]
        self.assertIn("customer", fact_names)

    def test_edit_endpoint_missing_model(self):
        resp = self.client.post(
            "/api/datamodel/edit",
            json={"model_id": "nonexistent", "edits": {}},
        )
        data = resp.get_json()
        self.assertFalse(data["success"])

    @patch("data_modeling.fetch_table_metadata")
    @patch("app._dm_get_warehouse", return_value="wh-123")
    def test_ddl_endpoint(self, mock_wh, mock_fetch):
        mock_fetch.return_value = _sample_tables_meta()
        # Generate first
        gen_resp = self.client.post(
            "/api/datamodel/generate",
            json={"catalog": "bronze", "schema": "hr", "tables": ["sales_transactions", "customer"]},
        )
        model_id = gen_resp.get_json()["model_id"]

        resp = self.client.post(
            "/api/datamodel/ddl",
            json={"model_id": model_id, "catalog": "bronze", "schema": "hr"},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["success"])
        self.assertIn("ddl", data)
        self.assertIn("CREATE TABLE", data["ddl"])


if __name__ == "__main__":
    unittest.main()
