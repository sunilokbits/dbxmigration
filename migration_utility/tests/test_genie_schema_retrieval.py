"""Offline regression tests for the real catalog-discovery route definitions.

AST loading avoids app/Genie imports and startup hooks. Settings, the resolver,
SDK, HTTP and clock are stubs; sockets are blocked. No warehouse, business rows,
doc_qa index, secret scope, or cloud service is contacted.
"""
import ast
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import socket
import threading
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from flask import Flask


ROUTE_PATH = Path(__file__).resolve().parents[1] / "routes" / "catalog_discovery.py"


def load_route():
    tree = ast.parse(ROUTE_PATH.read_text(encoding="utf-8"), filename=str(ROUTE_PATH))
    nodes = [node for node in tree.body if not (
        isinstance(node, ast.ImportFrom) and node.module == "routes.auth")]
    namespace = {"__name__": __name__, "login_required": lambda function: function}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(ROUTE_PATH), "exec"), namespace)
    return namespace


class Clock:
    def __init__(self):
        self.now = 100.0

    def monotonic(self):
        return self.now

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def response(payload=None, status=200):
    result = Mock(status_code=status)
    result.json.return_value = payload or {}
    if status >= 400:
        result.raise_for_status.side_effect = RuntimeError(f"HTTP {status}")
    return result


def table(name="jobs", catalog="meta", schema="control", dtype="BIGINT"):
    return {"catalog": catalog, "schema": schema, "table": name, "type": "MANAGED",
            "columns": [{"name": "job_id", "type": dtype, "comment": "metadata only"}]}


class SchemaRetrievalTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.dict("os.environ", {}, clear=True))
        self.enterContext(patch.object(socket, "create_connection",
                                      side_effect=AssertionError("Live network forbidden")))
        self.enterContext(patch.object(socket.socket, "connect",
                                      side_effect=AssertionError("Live network forbidden")))
        self.cfg = {"databricks_host": "configured.invalid/", "databricks_sql_warehouse_id": "wh-current",
                    "databricks_token": "fake-test-token"}
        self.pairs = {"metadata": ("meta", "control"), "app": ("meta", "app_owned")}
        self.config = ModuleType("config_cache")
        self.config.get_config = Mock(side_effect=lambda: dict(self.cfg))
        self.genie = ModuleType("routes.genie")
        self.genie.resolve_configured_catalogs = Mock(side_effect=lambda: dict(self.pairs))
        self.http = ModuleType("requests")
        self.http.post = Mock(side_effect=AssertionError("Unexpected HTTP POST"))
        self.http.get = Mock(side_effect=AssertionError("Unexpected HTTP GET"))
        self.sdk = ModuleType("databricks.sdk")
        self.sdk.WorkspaceClient = Mock()
        self.sdk.WorkspaceClient.return_value.config.authenticate.return_value = {
            "Authorization": "Bearer fake-oauth-token"}
        self.enterContext(patch.dict("sys.modules", {
            "config_cache": self.config, "routes": ModuleType("routes"), "routes.genie": self.genie,
            "databricks": ModuleType("databricks"), "databricks.sdk": self.sdk, "requests": self.http,
        }))
        self.r = load_route()
        self.clock = Clock()
        self.r["time"] = self.clock
        self.ensure_fresh = self.r["_ensure_cache_fresh"]
        # Retrieval tests must not spawn any discovery thread implicitly.
        self.r["_ensure_cache_fresh"] = Mock(side_effect=self.r["_sync_scope"])
        self.r["_sync_scope"]()
        self.app = Flask(__name__)
        self.app.register_blueprint(self.r["catalog_discovery_bp"])
        self.client = self.app.test_client()

    def seed(self, tables=None):
        self.r["_sync_scope"]()
        tables = tables if tables is not None else [table(f"jobs_{i:03d}") for i in range(80)]
        self.r["_cache"].update(tables=tables, catalogs=[{"name": "meta"}],
                                 schemas=[{"catalog": "meta", "schema": "control"}],
                                 last_refreshed=datetime.now(timezone.utc).isoformat())

    def embeddings(self, url, **kwargs):
        self.assertIn("/serving-endpoints/", url)
        return response({"data": [{"index": i, "embedding": [1.0, float(i + 1)]}
                                  for i, _ in enumerate(kwargs["json"]["input"])]})

    def enable_embeddings(self):
        self.http.post.side_effect = self.embeddings

    def success(self, rows=None, **result):
        return response({"statement_id": "statement-one", "status": {"state": "SUCCEEDED"},
                         "manifest": {"schema": {"columns": [{"name": "id", "type_name": "INT"}]}},
                         "result": {"data_array": rows or [["1"]], **result}})

    def test_sql_async_wait_and_settings_override_environment(self):
        with patch.dict("os.environ", {"DATABRICKS_HOST": "old.invalid", "DATABRICKS_SQL_WAREHOUSE_ID": "old"}):
            self.http.post.side_effect = None
            self.http.post.return_value = self.success()
            result = self.r["_execute_sql"]("SHOW CATALOGS")
        self.assertIsNone(result["error"])
        call = self.http.post.call_args
        self.assertEqual(call.args[0], "https://configured.invalid/api/2.0/sql/statements")
        self.assertEqual(call.kwargs["json"]["wait_timeout"], "0s")
        self.assertEqual(call.kwargs["json"]["warehouse_id"], "wh-current")
        self.assertLessEqual(call.kwargs["timeout"], 10)
        self.assertFalse(call.kwargs["allow_redirects"])
        self.assertEqual(result["column_types"], ["INT"])

    def test_sql_endpoint_resolves_new_runtime_warehouse(self):
        self.http.post.side_effect = None
        self.http.post.return_value = self.success()
        self.cfg.update(databricks_host="new.invalid", databricks_sql_warehouse_id="new-wh")
        result = self.client.post("/api/v1/sql/execute", json={"sql": "SHOW CATALOGS"})
        self.assertEqual(result.status_code, 200)
        self.assertTrue(self.http.post.call_args.args[0].startswith("https://new.invalid/"))
        self.assertEqual(self.http.post.call_args.kwargs["json"]["warehouse_id"], "new-wh")
        self.r["_execute_sql"]("SHOW CATALOGS", warehouse_id="explicit")
        self.assertEqual(self.http.post.call_args.kwargs["json"]["warehouse_id"], "explicit")

    def test_environment_fallback_and_recursion_guard(self):
        calls = []

        def reentrant_loader():
            calls.append(self.r["_runtime_config"]())
            return dict(self.cfg)

        self.config.get_config.side_effect = reentrant_loader
        with patch.dict("os.environ", {"DATABRICKS_HOST": "bootstrap.invalid", "DATABRICKS_SQL_WAREHOUSE_ID": "bootstrap"}):
            resolved = self.r["_runtime_config"]()
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["warehouse"], "bootstrap")
        self.assertEqual(resolved["warehouse"], "wh-current")
        self.assertFalse(self.r["_runtime_local"].reading_config)
        self.http.post.assert_not_called()

    def test_sdk_dict_and_callable_auth_never_become_bearer_dict(self):
        self.cfg.pop("databricks_token")
        for value in ({"Authorization": "Bearer fake-oauth-token"},
                      lambda: {"Authorization": "Bearer fake-oauth-token"},
                      lambda request: {"authorization": "Bearer fake-oauth-token"}):
            with self.subTest(value=value):
                self.sdk.WorkspaceClient.return_value.config.authenticate.return_value = value
                self.assertEqual(self.r["_headers"]()["Authorization"], "Bearer fake-oauth-token")
        self.assertEqual(self.sdk.WorkspaceClient.call_args.kwargs["host"], "https://configured.invalid")

    def test_auth_failure_and_missing_connection_make_no_http_call(self):
        self.cfg.pop("databricks_token")
        self.sdk.WorkspaceClient.return_value.config.authenticate.return_value = {"wrong": "header"}
        self.assertIn("bearer token", self.r["_execute_sql"]("SHOW CATALOGS")["error"])
        self.cfg.clear()
        self.assertIn("configured", self.r["_execute_sql"]("SHOW CATALOGS")["error"])
        self.http.post.assert_not_called()

    def test_sql_http_errors_are_prompt_even_with_non_json_body(self):
        for code in (400, 401, 403, 429, 500):
            with self.subTest(code=code):
                result = response(status=code)
                result.json.side_effect = ValueError("HTML body, not JSON")
                self.http.post.side_effect = None
                self.http.post.return_value = result
                self.assertIn(str(code), self.r["_execute_sql"]("SHOW CATALOGS")["error"])
                result.json.assert_not_called()
        self.http.get.assert_not_called()
        self.assertEqual(self.clock.now, 100.0)

    def test_sql_polling_uses_total_deadline_and_remaining_http_budget(self):
        pending = response({"statement_id": "one", "status": {"state": "RUNNING"}})
        self.http.post.side_effect = None
        self.http.post.return_value = pending
        self.http.get.side_effect = None
        self.http.get.return_value = pending
        result = self.r["_execute_sql"]("SHOW CATALOGS", timeout=1.2)
        self.assertIn("deadline exceeded", result["error"])
        self.assertAlmostEqual(self.clock.now, 101.2)
        self.assertEqual(self.http.get.call_count, 2)
        self.assertLessEqual(self.http.get.call_args.kwargs["timeout"], 0.21)

    def test_sql_poll_http_failure_stops_immediately(self):
        self.http.post.side_effect = None
        self.http.post.return_value = response({"statement_id": "one", "status": {"state": "PENDING"}})
        self.http.get.side_effect = None
        self.http.get.return_value = response(status=403)
        self.assertIn("403", self.r["_execute_sql"]("SHOW CATALOGS")["error"])
        self.assertEqual(self.http.get.call_count, 1)

    def test_sql_pending_then_success_and_invalid_deadlines(self):
        self.http.post.side_effect = None
        self.http.post.return_value = response({"statement_id": "one", "status": {"state": "PENDING"}})
        self.http.get.side_effect = None
        self.http.get.return_value = self.success()
        self.assertIsNone(self.r["_execute_sql"]("SHOW CATALOGS")["error"])
        count = self.http.post.call_count
        for timeout in (0, -1, float("nan"), float("inf")):
            with self.subTest(timeout=timeout):
                self.assertIn("positive and finite", self.r["_execute_sql"]("SHOW CATALOGS", timeout=timeout)["error"])
        self.assertEqual(self.http.post.call_count, count)

    def test_sql_terminal_states_and_missing_statement_id(self):
        for state in ("FAILED", "CANCELED", "CLOSED", "UNKNOWN", "PENDING"):
            with self.subTest(state=state):
                self.http.post.side_effect = None
                self.http.post.return_value = response({"status": {"state": state}})
                self.assertTrue(self.r["_execute_sql"]("SHOW CATALOGS")["error"])
        self.http.get.assert_not_called()

    def test_sql_pagination_is_bounded_and_uses_internal_chunk_urls(self):
        self.http.post.side_effect = None
        self.http.post.return_value = self.success([["1"]], next_chunk_index=1,
                                                   next_chunk_internal_link="https://untrusted.invalid")
        self.http.get.side_effect = None
        self.http.get.return_value = response({"data_array": [["2"], ["3"]], "next_chunk_index": 2})
        result = self.r["_execute_sql"]("SHOW CATALOGS", max_rows=2)
        self.assertEqual(result["data"], [["1"], ["2"]])
        self.assertTrue(result["truncated"])
        self.assertEqual(self.http.get.call_count, 1)
        self.assertEqual(self.http.get.call_args.args[0],
                         "https://configured.invalid/api/2.0/sql/statements/statement-one/result/chunks/1")

    def test_sql_manifest_truncation_and_malformed_json(self):
        success = self.success()
        success.json.return_value["manifest"]["truncated"] = True
        self.http.post.side_effect = None
        self.http.post.return_value = success
        self.assertTrue(self.r["_execute_sql"]("SHOW CATALOGS")["truncated"])
        success.json.side_effect = ValueError("malformed JSON")
        self.assertIn("malformed JSON", self.r["_execute_sql"]("SHOW CATALOGS")["error"])

    def test_strict_pairs_include_app_and_never_expand_on_false_flag(self):
        self.seed([table(), table("audit", schema="app_owned"), table("private", schema="other"),
                   table("outside", catalog="unconfigured")])
        for configured_only in (True, False):
            text = self.r["get_relevant_schema_context"](configured_only=configured_only)
            self.assertIn("meta.control.jobs", text)
            self.assertIn("meta.app_owned.audit", text)
            self.assertNotIn("meta.other", text)
            self.assertNotIn("unconfigured", text)
            self.assertIn("job_id BIGINT", text)
        self.http.post.assert_not_called()

    def test_empty_no_match_or_failed_resolver_never_falls_back(self):
        for pairs in ({}, {"metadata": ("new", "scope")}, {"metadata": ("meta", "")}):
            with self.subTest(pairs=pairs):
                self.pairs = pairs
                self.seed([table()])
                text = self.r["get_relevant_schema_context"]("jobs")
                self.assertIn("No matching tables", text)
                self.assertNotIn("meta.control.jobs", text)
        self.genie.resolve_configured_catalogs.side_effect = RuntimeError("unavailable")
        self.assertIn("No matching tables", self.r["get_schema_context"]())
        self.http.post.assert_not_called()

    def test_one_batch_for_question_and_bounded_preselection_then_cache_hit(self):
        self.seed()
        self.enable_embeddings()
        first = self.r["get_relevant_schema_context"]("jobs", top_n=6)
        self.assertEqual(self.http.post.call_count, 1)
        call = self.http.post.call_args
        self.assertEqual(len(call.kwargs["json"]["input"]), 41)
        self.assertLessEqual(call.kwargs["timeout"], 5)
        self.assertEqual(self.r["_cache"]["retrieval"]["scoped_tables"], 80)
        self.assertEqual(self.r["_cache"]["retrieval"]["returned_tables"], 6)
        self.assertEqual(self.r["get_relevant_schema_context"]("jobs", top_n=6), first)
        self.assertEqual(self.http.post.call_count, 1)
        stats = self.r["_cache"]["retrieval"]
        self.assertEqual(stats["vector_cache_hits"], 41)
        self.assertEqual(stats["embedding_requests"], 0)
        self.assertEqual(stats["ranking"], "vector")

    def test_lexical_preselection_finds_snake_case_and_column_tokens(self):
        self.seed([table(f"unrelated_{i:03d}") for i in range(80)] + [table("failed_runs", dtype="TIMESTAMP")])
        self.http.post.side_effect = RuntimeError("endpoint unavailable")
        text = self.r["get_relevant_schema_context"]("failed runs timestamp", top_n=1)
        self.assertIn("failed_runs", text)
        self.assertNotIn("unrelated_", text)
        self.assertEqual(self.r["_cache"]["retrieval"]["ranking"], "lexical")

    def test_failed_endpoint_has_cooldown_and_recovery(self):
        self.seed()
        self.http.post.side_effect = RuntimeError("HTTP 403")
        self.r["get_relevant_schema_context"]("jobs", top_n=2)
        self.r["get_relevant_schema_context"]("another question", top_n=2)
        self.assertEqual(self.http.post.call_count, 1)
        self.assertEqual(self.r["_cache"]["retrieval"]["fallback_reason"], "embedding_cooldown")
        self.assertEqual(len(self.r["_embedding_cache"]), 0)
        self.clock.now += 61
        self.enable_embeddings()
        self.r["get_relevant_schema_context"]("jobs", top_n=2)
        self.assertEqual(self.http.post.call_count, 2)
        self.assertEqual(self.r["_cache"]["retrieval"]["ranking"], "vector")

    def test_embedding_response_indices_are_respected(self):
        self.http.post.side_effect = lambda *args, **kwargs: response({"data": [
            {"index": 1, "embedding": [0.0, 1.0]}, {"index": 0, "embedding": [1.0, 0.0]}]})
        vectors = self.r["_embed_texts"](["one", "two"])
        self.assertEqual(vectors, [[1.0, 0.0], [0.0, 1.0]])

    def test_bad_vectors_fail_closed_without_caching(self):
        for data in ([], [{"embedding": []}], [{"embedding": [float("nan")]}],
                     [{"embedding": [0.0]}], [{"embedding": ["not-a-number"]}],
                     [{"index": 3, "embedding": [1.0]}]):
            with self.subTest(data=data):
                self.r["_embedding_circuits"].clear()
                self.http.post.side_effect = None
                self.http.post.return_value = response({"data": data})
                self.assertIsNone(self.r["_embed_texts"](["one"]))

    def test_embedding_five_second_deadline_discards_late_results(self):
        self.seed()

        def late_endpoint(url, **kwargs):
            self.assertLessEqual(kwargs["timeout"], 5)
            self.clock.now += 6
            return self.embeddings(url, **kwargs)

        self.http.post.side_effect = late_endpoint
        self.r["get_relevant_schema_context"]("jobs", top_n=2)
        self.assertEqual(self.r["_cache"]["retrieval"]["ranking"], "lexical")
        self.assertEqual(self.r["_cache"]["retrieval"]["fallback_reason"], "embedding_failed")
        self.assertEqual(len(self.r["_embedding_cache"]), 0)

    def test_embedding_circuit_state_is_bounded_and_host_isolated(self):
        self.r["_CIRCUIT_LIMIT"] = 3
        self.http.post.side_effect = RuntimeError("Unavailable")
        for i in range(10):
            config = {"host": f"https://workspace-{i}.invalid", "token": "fake", "warehouse": "wh"}
            self.r["_embed_texts"](["question"], config=config, scope=(("cat", "schema"),))
        self.assertEqual(len(self.r["_embedding_circuits"]), 3)
        self.assertEqual(self.http.post.call_count, 10)

    def test_concurrent_cache_hits_are_thread_safe(self):
        self.seed()
        self.enable_embeddings()
        expected = self.r["get_relevant_schema_context"]("jobs", top_n=2)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.r["get_relevant_schema_context"]("jobs", top_n=2), range(32)))
        self.assertTrue(all(text == expected for text in results))
        self.assertEqual(self.http.post.call_count, 1)
        self.assertEqual(len(self.r["_embedding_cache"]), 41)

    def test_only_missing_texts_reembedded_including_type_changes(self):
        self.seed([table(f"jobs_{i}") for i in range(5)])
        self.enable_embeddings()
        self.r["get_relevant_schema_context"]("jobs", top_n=2)
        self.r["_cache"]["tables"][0]["columns"][0]["type"] = "STRING"
        self.r["get_relevant_schema_context"]("jobs", top_n=2)
        self.assertEqual(len(self.http.post.call_args.kwargs["json"]["input"]), 1)
        self.assertIn("STRING", self.http.post.call_args.kwargs["json"]["input"][0])
        self.r["get_relevant_schema_context"]("Jobs?", top_n=2)
        self.assertEqual(self.http.post.call_args.kwargs["json"]["input"], ["Jobs?"])

    def test_cache_is_bounded_and_expires_table_and_question_vectors(self):
        self.seed([table(f"jobs_{i}") for i in range(5)])
        self.enable_embeddings()
        self.r["_VECTOR_CACHE_LIMIT"] = 8
        for i in range(10):
            self.r["get_relevant_schema_context"](f"jobs question {i}", top_n=2)
        self.assertLessEqual(len(self.r["_embedding_cache"]), 8)
        self.clock.now += 301
        self.r["get_relevant_schema_context"]("jobs question 9", top_n=2)
        self.assertEqual(len(self.http.post.call_args.kwargs["json"]["input"]), 5)
        self.clock.now += 601
        self.r["get_relevant_schema_context"]("jobs question 9", top_n=2)
        self.assertEqual(len(self.http.post.call_args.kwargs["json"]["input"]), 6)

    def test_model_host_and_scope_changes_cannot_reuse_old_vectors(self):
        self.seed()
        self.enable_embeddings()
        self.r["get_relevant_schema_context"]("jobs", top_n=2)
        self.r["_EMBED_ENDPOINT"] = "different-model"
        self.r["get_relevant_schema_context"]("jobs", top_n=2)
        self.assertEqual(len(self.http.post.call_args.kwargs["json"]["input"]), 41)
        self.cfg["databricks_host"] = "other.invalid"
        self.assertIn("No matching tables", self.r["get_relevant_schema_context"]("jobs"))
        self.assertEqual(len(self.r["_embedding_cache"]), 0)
        self.seed()
        self.r["get_relevant_schema_context"]("jobs", top_n=2)
        self.assertIn("https://other.invalid/", self.http.post.call_args.args[0])
        self.pairs = {"metadata": ("other", "scope")}
        self.assertIn("No matching tables", self.r["get_relevant_schema_context"]("jobs"))
        self.assertEqual(self.r["_cache"]["catalogs"], [])

    def test_settings_change_during_embedding_discards_prompt_and_vectors(self):
        self.seed()

        def change_settings(url, **kwargs):
            self.pairs = {"metadata": ("new", "scope")}
            return self.embeddings(url, **kwargs)

        self.http.post.side_effect = change_settings
        text = self.r["get_relevant_schema_context"]("jobs", top_n=2)
        self.assertIn("Settings changed", text)
        self.assertNotIn("meta.control", text)
        self.assertEqual(len(self.r["_embedding_cache"]), 0)

    def test_concurrent_embedding_misses_do_not_queue_or_duplicate_requests(self):
        self.seed()
        entered, release = threading.Event(), threading.Event()

        def slow_endpoint(url, **kwargs):
            entered.set()
            if not release.wait(3):
                raise AssertionError("Test embedding release timed out")
            return self.embeddings(url, **kwargs)

        self.http.post.side_effect = slow_endpoint
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.r["get_relevant_schema_context"], "jobs", 2)
            try:
                self.assertTrue(entered.wait(3))
                second = pool.submit(self.r["get_relevant_schema_context"], "jobs", 2)
                self.assertIn("meta.control", second.result(timeout=3))
                self.assertEqual(self.http.post.call_count, 1)
                self.assertEqual(self.r["_cache"]["retrieval"]["fallback_reason"], "embedding_busy")
            finally:
                release.set()
            self.assertIn("meta.control", first.result(timeout=3))

    def test_chunks_and_top_n_are_bounded(self):
        wide = table()
        wide["columns"] = [{"name": "x" * 100, "type": "DECIMAL(30, 6)"} for _ in range(500)]
        chunk = self.r["_table_blurb"](wide)
        self.assertLessEqual(len(chunk), self.r["_MAX_CHUNK_CHARS"])
        self.assertIn("DECIMAL(30, 6)", chunk)
        self.assertIn("omitted", chunk)
        self.seed()
        self.r["get_relevant_schema_context"]("", top_n=5000)
        self.assertEqual(self.r["_cache"]["retrieval"]["returned_tables"], 40)
        self.r["get_relevant_schema_context"]("jobs", top_n=-1)
        self.assertEqual(self.r["_cache"]["retrieval"]["returned_tables"], 0)
        self.http.post.assert_not_called()

    def test_discovery_bulk_columns_and_default_scope_no_business_queries(self):
        self.pairs = {"metadata": ("meta", "control")}
        calls = []

        def metadata_query(sql, **kwargs):
            calls.append((sql, kwargs))
            if "information_schema.tables" in sql:
                return {"error": None, "data": [[f"jobs_{i}", "MANAGED"] for i in range(100)]}
            self.assertIn("information_schema.columns", sql)
            return {"error": None, "truncated": True,
                    "data": [[f"jobs_{i}", "id", "BIGINT", ""] for i in range(100)]}

        self.r["_execute_sql"] = Mock(side_effect=metadata_query)
        self.r["_discover_columns"] = Mock(side_effect=AssertionError("Per-table column query forbidden"))
        self.r["_discover_catalogs"] = Mock(side_effect=AssertionError("Workspace listing forbidden"))
        self.r["_full_discovery"](include_columns=True)
        self.assertEqual(len(calls), 2)
        self.assertEqual(len(self.r["_cache"]["tables"]), 100)
        self.assertEqual(self.r["_cache"]["tables"][0]["columns"][0]["type"], "BIGINT")
        self.assertTrue(self.r["_cache"]["columns_truncated"])
        self.assertEqual(calls[1][1]["max_rows"], 20000)
        self.assertIsNone(self.r["_cache"]["error"])

    def test_discovery_empty_or_explicit_filter_cannot_expand_scope(self):
        self.r["_discover_tables"] = Mock(return_value=[])
        for pairs, catalog_filter in (({}, None), (self.pairs, []), (self.pairs, ["outside"])):
            self.pairs = pairs
            self.r["_full_discovery"](catalogs_filter=catalog_filter)
            self.r["_discover_tables"].assert_not_called()
            self.assertEqual(self.r["_cache"]["tables"], [])

    def test_settings_change_during_scan_cannot_publish_old_catalog(self):
        def changed(catalog, schema):
            self.pairs = {"metadata": ("new", "scope")}
            return [table()]

        self.r["_discover_tables"] = Mock(side_effect=changed)
        self.r["_full_discovery"]()
        self.assertEqual(self.r["_cache"]["tables"], [])
        self.assertEqual(self.r["_cache"]["catalogs"], [])
        self.assertIsNone(self.r["_cache"]["last_refreshed"])
        self.assertFalse(self.r["_cache"]["refresh_in_progress"])

    def test_observed_a_b_a_scope_change_invalidates_scan_generation(self):
        initial = dict(self.pairs)

        def changed(catalog, schema):
            self.pairs = {"metadata": ("new", "scope")}
            self.r["_sync_scope"]()
            self.pairs = initial
            self.r["_sync_scope"]()
            return [table()]

        self.r["_discover_tables"] = Mock(side_effect=changed)
        self.r["_full_discovery"]()
        self.assertEqual(self.r["_cache"]["tables"], [])

    def test_old_scan_cannot_clear_or_overwrite_new_scan(self):
        entered, release = threading.Event(), threading.Event()
        self.pairs = {"metadata": ("old", "control")}

        def discover(catalog, schema):
            if catalog == "old":
                entered.set()
                if not release.wait(3):
                    raise AssertionError("Test scan release timed out")
            return [table(catalog=catalog, schema=schema)]

        self.r["_discover_tables"] = Mock(side_effect=discover)
        with ThreadPoolExecutor(max_workers=1) as pool:
            old = pool.submit(self.r["_full_discovery"])
            try:
                self.assertTrue(entered.wait(3))
                self.pairs = {"metadata": ("new", "scope")}
                self.r["_full_discovery"]()
            finally:
                release.set()
            old.result(timeout=3)
        self.assertEqual(self.r["_cache"]["tables"][0]["catalog"], "new")
        self.assertFalse(self.r["_cache"]["refresh_in_progress"])

    def test_scan_connection_is_pinned_then_settings_change_discards_it(self):
        self.pairs = {"metadata": ("meta", "control")}
        calls = []

        def post(url, **kwargs):
            calls.append(url)
            self.cfg["databricks_host"] = "changed.invalid"
            sql = kwargs["json"]["statement"]
            rows = [["jobs", "MANAGED"]] if "information_schema.tables" in sql else [["jobs", "id", "INT", ""]]
            return self.success(rows)

        self.http.post.side_effect = post
        self.r["_full_discovery"](include_columns=True)
        self.assertTrue(all(url.startswith("https://configured.invalid/") for url in calls))
        self.assertEqual(self.r["_cache"]["tables"], [])
        self.assertIsNone(self.r["_runtime_local"].scan_config)

    def test_discovery_http_error_is_not_reported_as_successful_empty_scan(self):
        self.http.post.side_effect = None
        self.http.post.return_value = response(status=403)
        self.r["_full_discovery"](include_columns=True)
        self.assertIn("403", self.r["_cache"]["error"])
        self.assertIsNone(self.r["_cache"]["last_refreshed"])
        self.assertEqual(self.http.post.call_count, 1)
        self.assertFalse(self.r["_cache"]["refresh_in_progress"])

    def test_metadata_names_are_escaped(self):
        self.r["_execute_sql"] = Mock(return_value={"error": None, "data": []})
        self.r["_discover_tables"]("cat`alog", "o'hare")
        sql = self.r["_execute_sql"].call_args.args[0]
        self.assertIn("`cat``alog`", sql)
        self.assertIn("'o''hare'", sql)
        self.r["_discover_columns"]("cat", "sch", "t'able")
        self.assertIn("'t''able'", self.r["_execute_sql"].call_args.args[0])

    def test_status_reports_actual_retrieval_counts_and_invalidates_old_scope(self):
        self.seed()
        self.enable_embeddings()
        self.r["get_relevant_schema_context"]("jobs", top_n=3)
        status = self.client.get("/api/v1/catalog/status").json
        self.assertEqual(status["retrieval"]["scoped_tables"], 80)
        self.assertEqual(status["retrieval"]["preselected_tables"], 40)
        self.assertEqual(status["retrieval"]["returned_tables"], 3)
        self.assertEqual(status["retrieval"]["embedding_requests"], 1)
        self.assertEqual(status["vector_cache_entries"], 41)
        old_generation = status["generation"]
        self.pairs = {"app": ("new", "app")}
        status = self.client.get("/api/v1/catalog/status").json
        self.assertGreater(status["generation"], old_generation)
        self.assertEqual(status["retrieval"], {})
        self.assertEqual(self.client.get("/api/v1/catalog/list").json["catalogs"], [])
        self.assertEqual(self.client.get("/api/v1/catalog/tables").json["tables"], [])

    def test_details_cannot_return_old_cached_namespace(self):
        self.seed([table()])
        self.pairs = {"app": ("new", "app")}
        result = self.client.get("/api/v1/catalog/table-details?table=meta.control.jobs")
        self.assertEqual(result.status_code, 403)
        self.http.post.assert_not_called()

    def test_refresh_detects_scope_change_before_in_progress_early_return(self):
        self.seed()
        self.r["_cache"]["refresh_in_progress"] = True
        self.pairs = {"metadata": ("new", "scope")}
        thread = Mock()
        real_threading = self.r["threading"]
        self.r["threading"] = SimpleNamespace(Thread=Mock(return_value=thread))
        try:
            self.ensure_fresh()
        finally:
            self.r["threading"] = real_threading
        thread.start.assert_called_once()
        self.assertEqual(self.r["_cache"]["tables"], [])


if __name__ == "__main__":
    unittest.main()