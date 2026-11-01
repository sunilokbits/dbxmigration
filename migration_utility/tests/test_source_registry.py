"""Enterprise source catalogue (Project Bedrock Phase I, 17 systems).

Built-in source types must behave exactly as before; every new system has its
own secret, maps onto a connector, and never falls through to SQL Server.
"""

import json
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import source_registry as reg

BUILTINS = ("azuresql", "sqlserver", "synapse", "snowflake", "redshift", "sharepoint", "api")


class TestCatalogue(unittest.TestCase):
    def test_all_17_rfp_systems_present_once(self):
        nos = [v["rfp"]["no"] for v in reg.ENTERPRISE.values()] + [reg.SHAREPOINT_RFP["no"]]
        self.assertEqual(sorted(nos), list(range(1, 18)))
        labels = {v["label"] for v in reg.ENTERPRISE.values()}
        for name in ("SAP ECC", "ServiceNow", "MS Dynamics CRM", "SuccessFactors", "MS Defender for Endpoint",
                     "Arkieva (via NDP)", "Project44", "Outpost24", "Onapsis", "EPI Historian", "SAP Ariba",
                     "Anaplan", "SAP TMS", "Price Navigator", "SAP EHS", "Adobe"):
            self.assertIn(name, labels)

    def test_each_of_the_17_has_its_own_secret(self):
        keys = [reg.secret_key(k) for k in reg.ENTERPRISE] + [reg.secret_key("sharepoint")]
        self.assertEqual(len(keys), 17)
        self.assertEqual(len(set(keys)), 17)
        self.assertEqual(reg.secret_key("sap_ecc"), "source-sap-ecc-password")
        self.assertEqual(reg.secret_key("sharepoint"), "source-sharepoint-password")

    def test_builtin_secret_keys_unchanged(self):
        self.assertEqual(reg.secret_key("sqlserver"), "source-sql-password")
        self.assertEqual(reg.secret_key("snowflake"), "source-snowflake-password")
        self.assertEqual(reg.secret_key("not_a_source"), "source-sql-password")


class TestConnectorMapping(unittest.TestCase):
    def test_builtins_are_their_own_connector(self):
        for t in BUILTINS:
            self.assertEqual(reg.connector_type({"source_type": t}), t)
        self.assertEqual(reg.connector_type({}), "sqlserver")

    def test_unknown_never_falls_through_to_sql_server(self):
        self.assertEqual(reg.connector_type({"source_type": "oracle"}), "unknown")

    def test_enterprise_methods_map_to_connectors(self):
        cases = {"sap_ecc": "adls", "sap_tms": "adls", "sap_ehs": "adls", "servicenow": "lakeflow",
                 "ms_dynamics_crm": "lakeflow", "successfactors": "api", "epi_historian": "api",
                 "arkieva": "sqlserver", "price_navigator": "adls", "adobe": "adls"}
        for st, conn in cases.items():
            self.assertEqual(reg.connector_type(st), conn, st)

    def test_method_and_engine_choices(self):
        self.assertEqual(reg.connector_type({"source_type": "servicenow", "ingestion_method": "rest_api"}), "api")
        sn = next(s for s in reg.client_payload()["sources"] if s["key"] == "servicenow")
        self.assertEqual(sn["methods"], ["lakeflow_connect", "rest_api"])
        self.assertTrue(sn["rest_objects"])
        self.assertEqual(reg.connector_type({"source_type": "adobe", "ingestion_method": "rest_api"}), "api")
        self.assertEqual(reg.connector_type({"source_type": "adobe", "ingestion_method": "bogus"}), "adls")
        self.assertEqual(reg.connector_type({"source_type": "arkieva", "engine": "azuresql"}), "azuresql")
        self.assertEqual(reg.connector_type({"source_type": "arkieva", "engine": "oracle"}), "sqlserver")

    def test_readiness(self):
        self.assertTrue(reg.can_extract({"source_type": "sqlserver"})[0])
        self.assertTrue(reg.can_extract({"source_type": "api"})[0])  # built-in behaviour unchanged
        self.assertTrue(reg.can_extract({"source_type": "sap_ecc"})[0])
        self.assertTrue(reg.can_extract({"source_type": "arkieva"})[0])
        # Lakeflow Connect: Extract reads the Lakeflow-ingested Delta table
        self.assertTrue(reg.can_extract({"source_type": "servicenow"})[0])
        self.assertEqual(reg.readiness("servicenow"), "ready")
        # rest_api / streaming are extract-capable now (API → landing → Bronze/Silver)
        self.assertTrue(reg.can_extract({"source_type": "successfactors"})[0])
        self.assertEqual(reg.readiness("epi_historian"), "ready")
        self.assertEqual(reg.readiness("successfactors"), "ready")
        self.assertEqual(reg.readiness("price_navigator", "rest_api"), "ready")

    def test_client_payload_is_json_and_secret_free(self):
        payload = reg.client_payload()
        text = json.dumps(payload)
        self.assertEqual(len(payload["sources"]), len(BUILTINS) + len(reg.ENTERPRISE))
        self.assertNotIn("password\":", text)
        sp = next(s for s in payload["sources"] if s["key"] == "sharepoint")
        self.assertEqual(sp["rfp"]["no"], 3)


class TestSecretsUseRegistry(unittest.TestCase):
    def test_get_and_set_use_per_source_keys(self):
        import secrets_helper
        with mock.patch.object(secrets_helper, "get_secret", return_value="x") as g:
            secrets_helper.get_source_password("project44")
            g.assert_called_with("source-project44-password")
            secrets_helper.get_source_password("snowflake")
            g.assert_called_with("source-snowflake-password")
        with mock.patch.object(secrets_helper, "set_secret", return_value=True) as st:
            secrets_helper.set_source_password("v", source_type="sap_ariba")
            st.assert_called_with("source-sap-ariba-password", "v")


class TestEnterpriseSources(unittest.TestCase):
    def test_adls_lists_object_folders(self):
        import enterprise_sources as es
        rows = [{"name": "MARA/", "path": "x"}, {"name": "VBAK/"}, {"name": "_SUCCESS"}]
        with mock.patch("dbsql_client.execute_query", return_value=rows) as q:
            r = es.load_objects("adls", {"source_type": "sap_ecc", "server": "abfss://c@a.dfs.core.windows.net/snp/ecc/",
                                         "file_format": "parquet"})
            q.assert_called_once_with("LIST 'abfss://c@a.dfs.core.windows.net/snp/ecc'")
            t = es.list_tables("adls", {"source_type": "sap_ecc", "server": "/Volumes/c/s/v"})
        self.assertEqual([o["name"] for o in r["grouped"]["view"]], ["MARA", "VBAK"])
        self.assertTrue(t["success"])
        self.assertEqual([x["full_name"] for x in t["tables"]], ["sap_ecc.MARA", "sap_ecc.VBAK"])

    def test_adls_rejects_unsafe_paths(self):
        import enterprise_sources as es
        for bad in ("", "dev/landing", "abfss://x'; DROP TABLE y; --"):
            self.assertFalse(es.test_connection("adls", {"server": bad})["success"])

    def test_lakeflow_checks_uc_connection_and_parses_objects(self):
        import enterprise_sources as es
        ws = mock.MagicMock()
        ws.connections.get.return_value = mock.MagicMock(connection_type=mock.MagicMock(value="SERVICENOW"))
        with mock.patch("databricks.sdk.WorkspaceClient", return_value=ws):
            r = es.test_connection("lakeflow", {"server": "servicenow_prod"})
        self.assertTrue(r["success"])
        self.assertIn("SERVICENOW", r["server_version"])
        r = es.load_objects("lakeflow", {"source_type": "servicenow", "server": "c", "objects": "incident, change_request"})
        self.assertEqual([o["name"] for o in r["grouped"]["view"]], ["incident", "change_request"])
        self.assertFalse(es.test_connection("lakeflow", {"server": "bad name!"})["success"])


class _Resp:
    def __init__(self, doc, status=200):
        self._doc, self.status_code, self.ok = doc, status, status < 400
        self.text = json.dumps(doc)

    def json(self):
        return self._doc

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError(f"HTTP {self.status_code}")


class TestSharePointRest(unittest.TestCase):
    SITE = "https://contoso.sharepoint.com/sites/Finance"

    def test_profile_by_auth_and_graph_site_base(self):
        none = reg.rest_profile("sharepoint", "none")
        oa = reg.rest_profile("sharepoint", "oauth2")
        self.assertEqual(none["table_path"], "/{table}")
        self.assertNotIn("by_auth", none)
        self.assertEqual((oa["table_path"], oa["record_key"]), ("/lists/{table}/items", "fields"))
        self.assertEqual(reg.rest_api_base(self.SITE, oa),
                         "https://graph.microsoft.com/v1.0/sites/contoso.sharepoint.com:/sites/Finance:")
        self.assertEqual(reg.rest_api_base("https://contoso.sharepoint.com/", oa),
                         "https://graph.microsoft.com/v1.0/sites/contoso.sharepoint.com")
        self.assertEqual(reg.rest_api_base(self.SITE + "/", none), self.SITE)
        self.assertEqual(reg.rest_oauth_secret_key("sharepoint"), "source-sharepoint-password")
        self.assertIsNone(reg.oauth_client_secret_key("sharepoint"))
        sp = next(s for s in reg.client_payload()["sources"] if s["key"] == "sharepoint")
        self.assertTrue(sp["rest_objects"] and sp["rest_tenant"] and sp["builtin"])
        self.assertEqual(reg.connector_type("sharepoint"), "sharepoint")

    def test_no_auth_odata_v3_stats(self):
        import enterprise_sources as es
        sess = mock.MagicMock()
        sess.headers = {}
        sess.get.return_value = _Resp({"odata.count": "91", "value": [{"A": 1, "B": 2, "C": 3}]})
        with mock.patch.object(es, "_retry_session", return_value=sess):
            t = es.rest_profile_tables({"source_type": "sharepoint", "api_auth_type": "none",
                                        "server": "https://services.odata.org/V3/Northwind/Northwind.svc",
                                        "objects": "Customers"})
        self.assertEqual([(x["full_name"], x["col_count"], x["row_estimate"]) for x in t["tables"]],
                         [("sharepoint.Customers", 3, 91)])
        self.assertEqual(sess.get.call_args[1]["params"], {"$top": 1, "$inlinecount": "allpages"})
        self.assertEqual(sess.headers["Accept"], "application/json;odata=nometadata")

    def test_graph_oauth_stats_flatten_fields_and_count_by_paging(self):
        import enterprise_sources as es
        sess = mock.MagicMock()
        sess.headers = {}
        sess.post.return_value = _Resp({"access_token": "tok"})
        g = "https://graph.microsoft.com/v1.0/sites/contoso.sharepoint.com:/sites/Finance:"
        sess.get.side_effect = [
            _Resp({"value": [{"id": "1", "fields": {"@odata.etag": "e", "Title": "a", "Amount": 1}}]}),
            _Resp({"value": [{"id": "1"}, {"id": "2"}], "@odata.nextLink": g + "/next"}),
            _Resp({"value": [{"id": "3"}]}),
        ]
        with mock.patch.object(es, "_retry_session", return_value=sess), \
             mock.patch.object(es, "_saved_source", return_value={}):
            t = es.rest_profile_tables({"source_type": "sharepoint", "api_auth_type": "oauth2", "server": self.SITE,
                                        "username": "cid", "password": "sec", "tenant_id": "tid",
                                        "objects": "Invoices"})
        self.assertTrue(t["success"], t)
        self.assertEqual((t["tables"][0]["col_count"], t["tables"][0]["row_estimate"]), (2, 3))
        self.assertEqual(sess.post.call_args[1]["data"]["scope"], "https://graph.microsoft.com/.default")
        self.assertEqual(sess.get.call_args_list[0][0][0], g + "/lists/Invoices/items")
        self.assertEqual(sess.get.call_args_list[2][0][0], g + "/next")

    def test_graph_lists_are_discovered_when_no_objects_given(self):
        import enterprise_sources as es
        sess = mock.MagicMock()
        sess.headers = {}
        sess.post.return_value = _Resp({"access_token": "tok"})
        sess.get.return_value = _Resp({"value": [{"displayName": "Invoices", "list": {"hidden": False}},
                                                 {"displayName": "Secret", "list": {"hidden": True}}]})
        with mock.patch.object(es, "_retry_session", return_value=sess), \
             mock.patch.object(es, "_saved_source", return_value={}):
            r = es.rest_profile_objects({"source_type": "sharepoint", "api_auth_type": "oauth2", "server": self.SITE,
                                         "username": "cid", "password": "sec", "tenant_id": "tid"})
        self.assertEqual([o["name"] for o in r["grouped"]["view"]], ["Invoices"])

    def _create(self, **src):
        import workflow_manager as wfm
        with mock.patch.object(wfm, "_sync_pipeline_to_dbr", return_value=True), \
             mock.patch.object(wfm, "_sync_job_to_dbr", return_value=True):
            r = wfm.create_pipeline_for_table(
                "sharepoint", "Invoices", source_config={"source_type": "sharepoint", "server": self.SITE, **src},
                target_config={"bronze_catalog": "b", "silver_catalog": "s", "target_schema": "x"},
                pipeline_mode="dlt")
        self.addCleanup(wfm.PIPELINE_GROUPS.pop, r.get("group_id"), None)
        return r

    def test_pipeline_oauth_uses_graph_base_and_sharepoint_secret(self):
        r = self._create(username="cid", tenant_id="tid")  # older configs: no auth type -> OAuth
        self.assertTrue(r["success"], r)
        sc = r["group"]["source_config"]
        self.assertEqual((sc["connector"], sc["api_auth_type"], sc["oauth_tenant_id"]), ("api", "oauth2", "tid"))
        self.assertTrue(sc["api_base"].startswith("https://graph.microsoft.com/v1.0/sites/"))
        self.assertEqual(sc["oauth_client_secret_key"], "source-sharepoint-password")

    def test_pipeline_no_auth_reads_the_url_directly(self):
        r = self._create(api_auth_type="none")
        self.assertTrue(r["success"], r)
        sc = r["group"]["source_config"]
        self.assertEqual((sc["connector"], sc["api_base"]), ("api", self.SITE))
        self.assertEqual(sc["api_profile"]["table_path"], "/{table}")


class TestRestProfiles(unittest.TestCase):
    BASE = "https://services.odata.org/V4/Northwind/Northwind.svc"

    def test_dynamics_has_rest_profile_and_tenant_flag(self):
        self.assertEqual(reg.connector_type({"source_type": "ms_dynamics_crm", "ingestion_method": "rest_api"}), "api")
        d = next(s for s in reg.client_payload()["sources"] if s["key"] == "ms_dynamics_crm")
        self.assertEqual(d["methods"], ["lakeflow_connect", "rest_api"])
        self.assertTrue(d["rest_objects"] and d["rest_tenant"])
        sn = next(s for s in reg.client_payload()["sources"] if s["key"] == "servicenow")
        self.assertFalse(sn["rest_tenant"])
        self.assertEqual(reg.oauth_client_secret_key("ms_dynamics_crm"), "source-ms-dynamics-crm-client-secret")

    def test_odata_no_auth_stats_use_odata_count(self):
        import enterprise_sources as es
        sess = mock.MagicMock()
        sess.headers = {}
        sess.get.return_value = _Resp({"@odata.count": 91, "value": [{"@odata.etag": "x", "A": 1, "B": 2}]})
        with mock.patch.object(es, "_retry_session", return_value=sess):
            t = es.rest_profile_tables({"source_type": "ms_dynamics_crm", "server": self.BASE,
                                        "api_auth_type": "none", "objects": "Customers, Orders"})
        self.assertTrue(t["success"])
        self.assertEqual([(x["full_name"], x["col_count"], x["row_estimate"]) for x in t["tables"]],
                         [("ms_dynamics_crm.Customers", 2, 91), ("ms_dynamics_crm.Orders", 2, 91)])
        self.assertEqual(sess.get.call_args[0][0], f"{self.BASE}/Orders")
        self.assertEqual(sess.get.call_args[1]["params"], {"$top": 1, "$count": "true"})
        sess.post.assert_not_called()

    def test_dynamics_oauth_uses_entra_token_url_and_origin_scope(self):
        import enterprise_sources as es
        sess = mock.MagicMock()
        sess.headers = {}
        sess.post.return_value = _Resp({"access_token": "tok"})
        sess.get.return_value = _Resp({"@odata.count": 5, "value": [{"name": "a"}]})
        with mock.patch.object(es, "_retry_session", return_value=sess), \
             mock.patch.object(es, "_saved_source", return_value={}):
            r = es.rest_profile_test({"source_type": "ms_dynamics_crm", "api_auth_type": "oauth2",
                                      "server": "https://org.crm.dynamics.com/api/data/v9.2",
                                      "username": "cid", "password": "sec", "tenant_id": "tid",
                                      "objects": "accounts"})
        self.assertTrue(r["success"], r)
        self.assertEqual(sess.post.call_args[0][0], "https://login.microsoftonline.com/tid/oauth2/v2.0/token")
        self.assertEqual(sess.post.call_args[1]["data"]["scope"], "https://org.crm.dynamics.com/.default")
        self.assertEqual(sess.headers["Authorization"], "Bearer tok")
        self.assertIn("accounts: 5 rows", r["server_version"])

    def test_dynamics_oauth_without_tenant_and_servicenow_without_oauth_fail(self):
        import enterprise_sources as es
        with mock.patch.object(es, "_saved_source", return_value={}):
            r = es.rest_profile_test({"source_type": "ms_dynamics_crm", "api_auth_type": "oauth2",
                                      "server": self.BASE, "username": "c", "password": "s"})
        self.assertIn("Tenant ID", r["error"])
        r = es.rest_profile_test({"source_type": "servicenow", "api_auth_type": "none",
                                  "server": "https://x.service-now.com"})
        self.assertFalse(r["success"])
        self.assertIn("OAuth", r["error"])
        r = es.rest_profile_test({"source_type": "ms_dynamics_crm", "api_auth_type": "none",
                                  "server": "http://insecure"})
        self.assertFalse(r["success"])


class TestRoutes(unittest.TestCase):
    def setUp(self):
        from flask import Flask
        from routes.source import source_bp
        from routes.workflow import workflow_bp
        app = Flask(__name__)
        app.secret_key = "t"
        app.register_blueprint(source_bp)
        app.register_blueprint(workflow_bp)
        self.client = app.test_client()
        p = mock.patch("routes.auth.get_current_user",
                       return_value={"email": "a@b.c", "role": "Admin", "display_name": "A"})
        p.start()
        self.addCleanup(p.stop)
        for target in ("routes.source.get_source_password", "routes.workflow.get_source_password"):
            q = mock.patch(target, return_value="")
            q.start()
            self.addCleanup(q.stop)

    def test_registry_script(self):
        r = self.client.get("/api/v1/sources/registry.js")
        self.assertEqual(r.status_code, 200)
        body = r.get_data(as_text=True)
        self.assertTrue(body.startswith("window.SOURCE_REGISTRY = "))
        payload = json.loads(body[len("window.SOURCE_REGISTRY = "):-1])
        self.assertEqual(sum(1 for s in payload["sources"] if s["rfp"]), 17)

    def test_enterprise_sources_route_to_their_connector(self):
        with mock.patch("enterprise_sources.test_connection", return_value={"success": True}) as t:
            r = self.client.post("/api/v1/source/test-connection",
                                 json={"source_type": "sap_ecc", "server": "/Volumes/c/s/v"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(t.call_args[0][0], "adls")
        with mock.patch("enterprise_sources.list_tables", return_value={"success": True, "tables": [], "total": 0}) as t:
            self.client.post("/api/v1/workflow/list-tables", json={"source_type": "servicenow", "server": "c"})
        self.assertEqual(t.call_args[0][0], "lakeflow")

    def test_unknown_type_is_rejected_not_sent_to_sql_server(self):
        with mock.patch("sql_pool.get_connection") as gc:
            r = self.client.post("/api/v1/source/test-connection",
                                 json={"source_type": "oracle", "server": "h", "database": "d", "username": "u"})
        self.assertEqual(r.status_code, 400)
        gc.assert_not_called()

    def test_api_based_enterprise_source_uses_rest_client(self):
        with mock.patch("api_source_client.test_connection", return_value={"success": True}) as t:
            self.client.post("/api/v1/source/test-connection",
                             json={"source_type": "project44", "server": "https://api.project44.com",
                                   "api_auth_type": "none"})
        t.assert_called_once()

    def test_rest_profile_sources_route_to_profile_client(self):
        with mock.patch("enterprise_sources.rest_profile_test", return_value={"success": True}) as t:
            self.client.post("/api/v1/source/test-connection",
                             json={"source_type": "ms_dynamics_crm", "ingestion_method": "rest_api",
                                   "server": "https://o.crm.dynamics.com/api/data/v9.2", "api_auth_type": "none"})
        t.assert_called_once()
        with mock.patch("enterprise_sources.rest_profile_test", return_value={"success": True}) as t:
            self.client.post("/api/v1/source/test-connection",
                             json={"source_type": "sharepoint", "server": "https://c.sharepoint.com/sites/x",
                                   "api_auth_type": "none"})
        t.assert_called_once()
        with mock.patch("enterprise_sources.rest_profile_tables",
                        return_value={"success": True, "tables": [], "total": 0}) as t:
            self.client.post("/api/v1/workflow/list-tables",
                             json={"source_type": "servicenow", "ingestion_method": "rest_api",
                                   "server": "https://x.service-now.com"})
        t.assert_called_once()


class TestPipelineCreation(unittest.TestCase):
    def test_lakeflow_without_ingestion_pipeline_is_blocked(self):
        import workflow_manager as wfm
        with mock.patch("enterprise_sources.lakeflow_destination", return_value=""):
            r = wfm.create_pipeline_for_table("servicenow", "incident",
                                              source_config={"source_type": "servicenow", "server": "sn"})
        self.assertFalse(r["success"])
        self.assertIn("Lakeflow Connect ingestion pipeline", r["error"])

    def test_lakeflow_source_is_stamped_with_destination_table(self):
        import workflow_manager as wfm
        with mock.patch("enterprise_sources.lakeflow_destination", return_value="lf.sn.incident"), \
             mock.patch.object(wfm, "_sync_pipeline_to_dbr", return_value=True), \
             mock.patch.object(wfm, "_sync_job_to_dbr", return_value=True):
            r = wfm.create_pipeline_for_table(
                "servicenow", "incident", source_config={"source_type": "servicenow", "server": "sn"},
                target_config={"bronze_catalog": "b", "silver_catalog": "s", "target_schema": "x"},
                pipeline_mode="dlt")
        self.addCleanup(wfm.PIPELINE_GROUPS.pop, r.get("group_id"), None)
        self.assertTrue(r["success"])
        sc = r["group"]["source_config"]
        self.assertEqual((sc["connector"], sc["lakeflow_table"]), ("lakeflow", "lf.sn.incident"))

    def test_servicenow_rest_is_stamped_with_profile_and_secret_keys(self):
        import workflow_manager as wfm
        with mock.patch.object(wfm, "_sync_pipeline_to_dbr", return_value=True), \
             mock.patch.object(wfm, "_sync_job_to_dbr", return_value=True):
            r = wfm.create_pipeline_for_table(
                "servicenow", "incident",
                source_config={"source_type": "servicenow", "ingestion_method": "rest_api",
                               "server": "https://x.service-now.com", "api_auth_type": "oauth2",
                               "username": "my-client-id"},
                target_config={"bronze_catalog": "b", "silver_catalog": "s", "target_schema": "x"},
                pipeline_mode="dlt")
        self.addCleanup(wfm.PIPELINE_GROUPS.pop, r.get("group_id"), None)
        self.assertTrue(r["success"])
        sc = r["group"]["source_config"]
        self.assertEqual(sc["connector"], "api")
        self.assertEqual(sc["api_profile"]["table_path"], "/api/now/table/{table}")
        self.assertEqual(sc["oauth_client_secret_key"], "source-servicenow-client-secret")
        self.assertEqual(sc["oauth_client_id"], "my-client-id")
        self.assertNotIn("oauth_client_id_key", sc)
        self.assertNotIn("client_secret", json.dumps(sc).replace("oauth_client_secret_key", ""))

    def _dyn(self, **src):
        import workflow_manager as wfm
        with mock.patch.object(wfm, "_sync_pipeline_to_dbr", return_value=True), \
             mock.patch.object(wfm, "_sync_job_to_dbr", return_value=True):
            r = wfm.create_pipeline_for_table(
                "ms_dynamics_crm", "accounts",
                source_config={"source_type": "ms_dynamics_crm", "ingestion_method": "rest_api",
                               "server": "https://org.crm.dynamics.com/api/data/v9.2", **src},
                target_config={"bronze_catalog": "b", "silver_catalog": "s", "target_schema": "x"},
                pipeline_mode="dlt")
        self.addCleanup(wfm.PIPELINE_GROUPS.pop, r.get("group_id"), None)
        return r

    def test_dynamics_rest_oauth_needs_tenant_and_stamps_it(self):
        r = self._dyn(api_auth_type="oauth2", username="cid")
        self.assertFalse(r["success"])
        self.assertIn("Tenant ID", r["error"])
        r = self._dyn(api_auth_type="oauth2", username="cid", tenant_id="tid")
        self.assertTrue(r["success"])
        sc = r["group"]["source_config"]
        self.assertEqual((sc["connector"], sc["oauth_client_id"], sc["oauth_tenant_id"]), ("api", "cid", "tid"))
        self.assertEqual(sc["api_profile"]["paging"], "next_link")
        self.assertEqual(sc["oauth_client_secret_key"], "source-ms-dynamics-crm-client-secret")

    def test_dynamics_rest_no_auth_needs_no_credentials(self):
        r = self._dyn(api_auth_type="none")
        self.assertTrue(r["success"])
        self.assertNotIn("oauth_client_id", r["group"]["source_config"])

    def test_lakeflow_target_colliding_with_ingested_schema_is_blocked(self):
        import workflow_manager as wfm
        with mock.patch("enterprise_sources.lakeflow_destination", return_value="b.sn.incident"):
            r = wfm.create_pipeline_for_table(
                "servicenow", "incident", source_config={"source_type": "servicenow", "server": "sn"},
                target_config={"bronze_catalog": "b", "silver_catalog": "s", "target_schema": "sn"},
                pipeline_mode="dlt")
        self.assertFalse(r["success"])
        self.assertIn("Default Schema", r["error"])

    def test_ready_source_is_stamped_with_connector(self):
        import workflow_manager as wfm
        with mock.patch.object(wfm, "_sync_pipeline_to_dbr", return_value=True), \
             mock.patch.object(wfm, "_sync_job_to_dbr", return_value=True):
            r = wfm.create_pipeline_for_table(
                "sap_ecc", "MARA", source_config={"source_type": "sap_ecc", "server": "/Volumes/c/s/v"},
                target_config={"bronze_catalog": "b", "silver_catalog": "s", "target_schema": "sap"}, pipeline_mode="dlt")
        self.addCleanup(wfm.PIPELINE_GROUPS.pop, r.get("group_id"), None)
        self.assertTrue(r["success"])
        sc = r["group"]["source_config"]
        self.assertEqual((sc["connector"], sc["ingestion_method"]), ("adls", "adls_landing"))
        self.assertTrue(all(j["job_name"] for j in r["jobs"]))


class TestNotebooks(unittest.TestCase):
    def _common(self):
        from metadata_notebooks import _gen_common_functions
        spark = mock.MagicMock()
        g = {"spark": spark, "dbutils": mock.MagicMock(), "__name__": "nb"}
        exec(compile(_gen_common_functions("ts"), "common", "exec"), g)
        return g, spark

    def test_source_connection_refuses_non_database_connectors(self):
        g, _ = self._common()
        for cfg in ({"source_type": "sap_ecc", "connector": "adls"}, {"source_type": "api"},
                    {"source_type": "sharepoint"}):
            with self.assertRaises(ValueError):
                g["_source_connection"](cfg, "pw")
        fmt, *_ = g["_source_connection"]({"source_type": "arkieva", "connector": "sqlserver", "server": "h"}, "pw")
        self.assertEqual(fmt, "sqlserver")
        fmt, *_ = g["_source_connection"]({"source_type": "snowflake", "account": "a"}, "pw")
        self.assertEqual(fmt, "snowflake")

    def test_read_files_applies_format_options(self):
        g, spark = self._common()
        g["_read_files_with"]("/Volumes/c/s/v/MARA", "csv")
        spark.read.format.assert_called_with("csv")
        reader = spark.read.format.return_value
        reader.option.assert_any_call("header", "true")

    def test_extract_and_recon_have_file_branches(self):
        from metadata_notebooks import generate_metadata_notebooks
        nbs = {n["name"]: n["code"] for n in generate_metadata_notebooks("m", "s", "/l", "/Shared/x", "dlt")["notebooks"]}
        ext = nbs["01_Meta_Extract"]
        self.assertIn('IS_FILE_SOURCE = (_CONNECTOR == "adls")', ext)
        self.assertIn("df = _read_files_with(_SRC_OBJECT_PATH, _FILE_FORMAT)", ext)
        rec = nbs["04_Meta_Reconciliation"]
        self.assertIn('"status": "SKIPPED"', rec)
        self.assertIn("isinstance(f.dataType, NumericType)", rec)


if __name__ == "__main__":
    unittest.main()
