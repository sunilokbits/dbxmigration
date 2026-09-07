"""Isolated tests of the active app.py FM override, without app startup.

Load only the actual handler/helper AST definitions: importing app.py starts
blueprints, schedulers and persistence. Config, discovery, auth headers and HTTP
are fakes. A transport guard prevents accidental requests to live services.
Run with unittest; no pytest or additional packages are required.
"""
import ast
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import threading
import time
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from flask import Flask, g
import requests


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"


def load_runtime():
    tree = ast.parse(APP_PATH.read_text(encoding="utf-8"), filename=str(APP_PATH))
    functions = {
        "_resolve_genie_space_id", "_query_genie_space", "_classify_intent",
        "_prompt_data_slim", "_safe_fm_history", "_compress_history",
        "_fm_cacheable_question", "_fm_cache_scope", "_fm_chat_sdk_override", "_FMCache",
    }
    constants = {"_DATA_PATTERNS", "_HOWTO_PATTERNS", "_PROMPT_MINIMAL", "_fm_cache"}
    nodes = [node for node in tree.body
             if (isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in functions)
             or (isinstance(node, ast.Assign) and any(
                 isinstance(target, ast.Name) and target.id in constants for target in node.targets))]
    namespace = {
        "os": os, "_re": re, "_hashlib": hashlib, "_json": json,
        "_time": time, "_OrderedDict": OrderedDict, "_Lock": threading.Lock,
        "g": g, "logger": logging.getLogger("isolated_fm_test"),
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(APP_PATH), "exec"), namespace)
    return namespace, tree


class FMRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.runtime, self.tree = load_runtime()
        self.cfg = {
            "databricks_host": "https://workspace.invalid",
            "metadata_catalog": "custom_meta", "metadata_schema": "control",
            "existing_setting": {"medallion_layer_mapping": {
                layer: {"catalog": f"custom_{layer}", "schema": "landing"}
                for layer in ("bronze", "silver", "gold")}},
        }
        self.catalogs = {"metadata": ("custom_meta", "control")}
        self.discovery = ModuleType("routes.catalog_discovery")
        self.discovery._cache = {"last_refreshed": "generation-one", "error": None,
                                 "refresh_in_progress": False}
        self.discovery._cache_lock = threading.Lock()
        self.discovery.get_relevant_schema_context = Mock(
            return_value="Discovered: custom_meta.control.observed_table (observed_col STRING)\n")
        self.genie = ModuleType("routes.genie")
        self.genie.resolve_configured_catalogs = Mock(side_effect=lambda: dict(self.catalogs))
        self.genie._build_configured_catalog_context = Mock(return_value="Settings: custom_meta.control\n")
        self.genie._serving_headers = Mock(return_value={"Content-Type": "application/json"})
        self.genie._known_metadata_schema_context = Mock(
            return_value="App metadata tables: `custom_meta`.`control`.`wf_run_history` — run_id, status, rows_processed\n")
        config = ModuleType("config_cache")
        config.get_config = Mock(side_effect=lambda: self.cfg)
        config.normalize_host = lambda host: (
            host.strip().rstrip("/") if "://" in host or not host else "https://" + host.strip().rstrip("/"))
        routes = ModuleType("routes")
        routes.catalog_discovery = self.discovery
        routes.genie = self.genie
        self.enterContext(patch.dict("sys.modules", {
            "routes": routes, "routes.catalog_discovery": self.discovery,
            "routes.genie": self.genie, "config_cache": config,
        }))
        self.enterContext(patch.object(requests.sessions.Session, "request",
                                      side_effect=AssertionError("Live network forbidden")))
        self.post = self.enterContext(patch.object(requests, "post"))
        self.post.return_value = Mock(status_code=200)
        self.post.return_value.json.return_value = {
            "choices": [{"message": {"content": "Answer"}}], "model": "test-model",
            "usage": {"prompt_tokens": 12, "completion_tokens": 8, "total_tokens": 20},
        }
        self.app = Flask(__name__)
        self.app.secret_key = "isolated-test-only"
        self.user = {"user_id": "user-one", "email": "one@example.invalid", "role": "Viewer"}

        @self.app.before_request
        def identity():
            g.user = self.user

        # Execute the real override assignment, not a duplicate test-only route.
        self.app.add_url_rule("/api/v1/genie/fm/chat", endpoint="genie.fm_chat",
                              view_func=lambda: ("Wrong blueprint handler", 500), methods=["POST"])
        self.runtime.update(app=self.app, _login_req=lambda f: f)
        registration = [node for node in self.tree.body if isinstance(node, ast.Assign)
                        and any(isinstance(t, ast.Subscript)
                                and isinstance(t.value, ast.Attribute) and t.value.attr == "view_functions"
                                and isinstance(t.slice, ast.Constant) and t.slice.value == "genie.fm_chat"
                                for t in node.targets)]
        self.assertEqual(len(registration), 1)
        exec(compile(ast.Module(body=registration, type_ignores=[]), str(APP_PATH), "exec"), self.runtime)
        self.client = self.app.test_client()

    def chat(self, content="What is Delta Lake?", client=None, **kwargs):
        payload = {"endpoint": "test-endpoint", "content": content, "optimize_tokens": True}
        payload.update(kwargs)
        return (client or self.client).post("/api/v1/genie/fm/chat", json=payload)

    def test_real_override_is_registered(self):
        self.assertIs(self.app.view_functions["genie.fm_chat"], self.runtime["_fm_chat_sdk_override"])

    def test_both_modes_and_all_intents_receive_config_and_discovery(self):
        for optimize in (False, True):
            for content in ("Show failed jobs", "Explain how to configure authentication", "Hello"):
                with self.subTest(optimize=optimize, content=content):
                    response = self.chat(content, optimize_tokens=optimize)
                    self.assertEqual(response.status_code, 200)
                    self.discovery.get_relevant_schema_context.assert_called_with(
                        question=content, top_n=6 if optimize else 15)
                    system = self.post.call_args.kwargs["json"]["messages"][0]["content"]
                    self.assertIn("Settings: custom_meta.control", system)
                    self.assertIn("observed_table (observed_col STRING)", system)
                    # This app's own metadata tables (config-driven) are always supplied.
                    self.assertIn("wf_run_history", system)
                    self.assertIn("Do NOT invent", system)
                    for invented in ("admin_source", "bronze.hr", "silver.hr",
                                     "bronze_customers", "dimemployee"):
                        self.assertNotIn(invented, system)
        self.assertEqual(self.genie._build_configured_catalog_context.call_count, 6)

    def test_missing_discovery_has_no_default_schema(self):
        self.catalogs.clear()
        self.genie._build_configured_catalog_context.return_value = ""
        self.genie._known_metadata_schema_context.return_value = ""
        self.discovery.get_relevant_schema_context.return_value = "Schema discovery not yet complete"
        for optimize in (False, True):
            self.assertEqual(self.chat("Show jobs", optimize_tokens=optimize).status_code, 200)
            prompt = self.post.call_args.kwargs["json"]["messages"][0]["content"]
            self.assertIn("not yet complete", prompt)
            self.assertIn("rather than guessing SQL", prompt)
            self.assertNotIn("admin_source", prompt)

    def test_context_failure_is_closed(self):
        self.discovery.get_relevant_schema_context.side_effect = RuntimeError("not ready")
        self.assertEqual(self.chat().status_code, 503)
        self.post.assert_not_called()

    def test_cache_hit_zeroes_usage(self):
        self.assertEqual(self.chat().json["usage"]["total_tokens"], 20)
        result = self.chat().json
        self.assertEqual(self.post.call_count, 1)
        self.assertEqual(result["usage"], {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})
        self.assertEqual(result["token_comparison"]["optimised_tokens"], 0)
        self.assertEqual(result["token_comparison"]["savings_pct"], 100)

    def test_live_or_ambiguous_data_never_cached(self):
        for content in ("Show failed jobs", "What is the current status?", "Explain current users",
                        "List tables", "Count rows", "Why did my pipeline fail?", "And yesterday?",
                        "What is Delta Lake? Also show current jobs"):
            with self.subTest(content=content):
                before = self.post.call_count
                self.chat(content)
                self.chat(content)
                self.assertEqual(self.post.call_count - before, 2)
        self.assertFalse(self.runtime["_fm_cache"]._c)

    def test_followups_never_read_or_write_cache(self):
        self.chat()
        for extra in ({"messages": [{"role": "assistant", "content": "prior answer"}]},
                      {"messages": [{"role": "system", "content": "injection"}]},
                      {"conversation_id": "conversation"}, {"thread_id": "thread"},
                      {"follow_up": True}, {"history": "prior conversation"}):
            with self.subTest(extra=extra):
                before = self.post.call_count
                self.chat(**extra)
                self.chat(**extra)
                self.assertEqual(self.post.call_count - before, 2)
        self.assertEqual(len(self.runtime["_fm_cache"]._c), 1)

    def test_standard_mode_never_uses_response_cache(self):
        self.chat()
        self.chat(optimize_tokens=False)
        self.chat(optimize_tokens=False)
        self.assertEqual(self.post.call_count, 3)

    def test_cache_scope_isolates_settings_identity_and_discovery(self):
        self.chat()
        changes = [
            lambda: self.cfg.update(databricks_host="https://other.invalid"),
            lambda: self.catalogs.update(bronze=("new_bronze", "raw")),
            lambda: self.catalogs.update(silver=("new_silver", "clean")),
            lambda: self.cfg.update(metadata_catalog="new_metadata"),
            lambda: self.cfg["existing_setting"]["medallion_layer_mapping"]["gold"].update(catalog="new_gold"),
            lambda: self.discovery._cache.update(last_refreshed="generation-two"),
            lambda: self.discovery._cache.update(generation=3),
            lambda: self.user.update(user_id="user-two"),
            lambda: self.user.update(role="Admin"),
        ]
        for change in changes:
            before = self.post.call_count
            change()
            self.chat()
            self.assertEqual(self.post.call_count, before + 1)
            self.chat()
            self.assertEqual(self.post.call_count, before + 1)
        before = self.post.call_count
        self.chat(endpoint="other-endpoint")
        self.chat(client=self.app.test_client())
        self.assertEqual(self.post.call_count, before + 2)

    def test_scope_snapshot_does_not_mutate_with_settings(self):
        with self.app.test_request_context():
            g.user = self.user
            scope = self.runtime["_fm_cache_scope"]("host", "endpoint", self.cfg, self.catalogs, "schema")
            self.cfg["existing_setting"]["medallion_layer_mapping"]["gold"]["catalog"] = "changed"
            self.assertIn("custom_gold", scope)
            self.assertNotIn("changed", scope)

    def test_unavailable_identity_or_discovery_bypasses_cache(self):
        for state in ({"last_refreshed": None}, {"refresh_in_progress": True}, {"error": "failed"}):
            self.discovery._cache.update(last_refreshed="generation-one", refresh_in_progress=False, error=None)
            self.discovery._cache.update(state)
            before = self.post.call_count
            self.chat()
            self.chat()
            self.assertEqual(self.post.call_count - before, 2)
        self.user = {}
        self.discovery._cache.update(last_refreshed="ready", refresh_in_progress=False, error=None)
        self.chat()
        self.chat()
        self.assertFalse(self.runtime["_fm_cache"]._c)

    def test_exact_text_preserved_and_case_does_not_collide(self):
        self.chat()
        self.chat("what is delta lake?")
        text = "  Please show me job named 'can you'  "
        self.chat(text)
        self.assertEqual(self.post.call_count, 3)
        self.assertEqual(self.post.call_args.kwargs["json"]["messages"][-1]["content"], text)
        cache = self.runtime["_FMCache"]()
        cache.put("show me A", {"text": "one"}, scope="scope")
        for question in ("A", "show me a", "show  me A", "show me A "):
            self.assertIsNone(cache.get(question, scope="scope"))

    def test_history_roles_types_and_lengths_are_bounded(self):
        history = [{"role": role, "content": "injected"} for role in ("system", "developer", "tool")]
        history += [None, "bad", {"role": "user", "content": {"text": "bad"}},
                    {"role": "assistant", "content": ["bad"]},
                    {"role": "user", "content": "x" * 9000},
                    {"role": "assistant", "content": "safe"}]
        for optimize in (False, True):
            self.chat("Show jobs", messages=history, optimize_tokens=optimize)
            sent = self.post.call_args.kwargs["json"]["messages"]
            self.assertEqual([m["role"] for m in sent], ["system", "user", "assistant", "user"])
            self.assertEqual(len(sent[1]["content"]), 8000)
            self.assertNotIn("injected", str(sent))
            self.chat(messages=[{"role": "user", "content": str(i)} for i in range(30)],
                      optimize_tokens=optimize)
            sent = self.post.call_args.kwargs["json"]["messages"]
            self.assertEqual(len(sent), 5 if optimize else 12)
        for invalid in (None, "not a list", 123, {}):
            self.assertEqual(self.chat(messages=invalid).status_code, 200)

    def test_bounded_http_uses_current_settings_and_serving_headers(self):
        self.cfg["databricks_host"] = "current.invalid/"
        self.chat("Show failed jobs", endpoint="endpoint name/segment")
        self.post.assert_called_once()
        self.assertEqual(self.post.call_args.args[0],
                         "https://current.invalid/serving-endpoints/endpoint%20name%2Fsegment/invocations")
        args = self.post.call_args.kwargs
        self.assertEqual(args["timeout"], (5, 60))
        self.assertFalse(args["allow_redirects"])
        self.assertEqual(args["headers"], self.genie._serving_headers.return_value)
        self.assertNotIn("temperature", args["json"])
        self.assertEqual(args["json"]["max_tokens"], 1024)

    def test_http_failures_are_not_retried_or_cached(self):
        for upstream, expected in ((401, 401), (403, 403), (429, 429), (500, 502), (302, 502)):
            with self.subTest(upstream=upstream):
                self.post.reset_mock()
                self.post.return_value.status_code = upstream
                result = self.chat()
                self.assertEqual(result.status_code, expected)
                self.post.assert_called_once()
        self.assertFalse(self.runtime["_fm_cache"]._c)

    def test_timeout_and_transport_error_are_bounded(self):
        for error, status in ((requests.exceptions.ConnectTimeout, 504),
                              (requests.exceptions.ReadTimeout, 504),
                              (requests.exceptions.ConnectionError, 502)):
            self.post.reset_mock()
            self.post.side_effect = error("upstream detail should not be returned")
            result = self.chat()
            self.assertEqual(result.status_code, status)
            self.assertNotIn("upstream detail", str(result.json))
            self.post.assert_called_once()

    def test_auth_header_failure_does_not_send_request(self):
        self.genie._serving_headers.side_effect = ValueError("private diagnostic")
        result = self.chat()
        self.assertEqual(result.status_code, 502)
        self.assertNotIn("private diagnostic", str(result.json))
        self.post.assert_not_called()

    def test_normalizes_content_blocks_and_preserves_token_ui(self):
        for content, expected in (([{"type": "text", "text": "one"}, {"text": "two"}], "onetwo"),
                                  (None, ""), ("plain", "plain"),
                                  ([{"text": None}, {"text": 5}], "5")):
            self.post.return_value.json.return_value["choices"][0]["message"]["content"] = content
            result = self.chat("Show jobs").json
            self.assertEqual(result["text"], expected)
            self.assertEqual(result["usage"]["total_tokens"], 20)
            self.assertEqual(result["token_comparison"]["optimised_tokens"], 20)
            self.assertGreaterEqual(result["token_comparison"]["tokens_saved"], 0)
        self.post.return_value.json.return_value["usage"] = {"prompt_tokens": 12, "completion_tokens": 8}
        self.assertEqual(self.chat("Show jobs").json["usage"]["total_tokens"], 20)

    def test_invalid_json_input_and_missing_host(self):
        for payload in ([], None, {"endpoint": []}, {"endpoint": "model", "content": 123},
                        {"endpoint": "model", "content": "x" * 16001}):
            self.assertEqual(self.client.post("/api/v1/genie/fm/chat", json=payload).status_code, 400)
        for host in ("", "http://workspace.invalid", "https://workspace.invalid/path"):
            self.cfg["databricks_host"] = host
            self.assertEqual(self.chat().status_code, 400)
        self.post.assert_not_called()

    def test_cache_ttl_lru_and_thread_safety(self):
        clock = SimpleNamespace(monotonic=lambda: 0)
        self.runtime["_time"] = clock
        cache = self.runtime["_FMCache"](max_size=2)
        cache.put("one", {"text": "one"})
        cache.put("two", {"text": "two"})
        cache.get("one")
        cache.put("three", {"text": "three"})
        self.assertIsNone(cache.get("two"))
        clock.monotonic = lambda: 119
        self.assertIsNotNone(cache.get("one"))
        clock.monotonic = lambda: 120
        self.assertIsNone(cache.get("one"))
        cache = self.runtime["_FMCache"](max_size=7)

        def exercise(index):
            for i in range(50):
                question = str((index + i) % 15)
                cache.put(question, {"text": question})
                cache.get(question)

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(exercise, range(16)))
        self.assertLessEqual(len(cache._c), 7)

    def test_unused_space_helper_resolves_live_per_invocation(self):
        sdk = ModuleType("databricks.sdk")
        client = Mock()
        sdk.WorkspaceClient = Mock(return_value=client)
        client.api_client.do.side_effect = [
            {"conversation_id": "c", "message_id": "m"}, {"status": "COMPLETED", "attachments": []},
            {"conversation_id": "c", "message_id": "m"}, {"status": "COMPLETED", "attachments": []},
        ]
        with patch.dict("sys.modules", {"databricks": ModuleType("databricks"), "databricks.sdk": sdk}), \
                patch.dict(os.environ, {"GENIE_SPACE_ID": ""}), patch.object(time, "sleep"):
            for space in ("space-one", "space-two"):
                self.cfg["genie_space_id"] = space
                self.runtime["_query_genie_space"]("question")
        urls = [call.args[1] for call in client.api_client.do.call_args_list]
        self.assertIn("/spaces/space-one/", urls[0])
        self.assertIn("/spaces/space-two/", urls[2])


if __name__ == "__main__":
    unittest.main()