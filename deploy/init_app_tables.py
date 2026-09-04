#!/usr/bin/env python3
"""Bootstrap this app's own Delta tables (app_config, user_roles, audit_log,
job_schedules, migration_jobs, dm_models) using the DEPLOY identity's
credentials, not the app's own runtime service principal.

Why this needs to exist: one_click_deploy.py already does this for client
deploys using the client's own PAT (see init_app_tables() there), but dev/
staging never ran anything equivalent -- they relied entirely on the app's
runtime SP self-healing via dbsql_client.ensure_tables() the first time a
route touched these tables, and that self-heal has the exact same
CREATE CATALOG issue described below.

Verified against the live dev workspace before this script existed:
`CREATE CATALOG IF NOT EXISTS admin_source` fails with
[INVALID_STATE] "Metastore storage root URL does not exist. Default
Storage is enabled in your account..." EVEN THOUGH admin_source already
existed there -- this metastore's default-storage config makes bare
`CREATE CATALOG IF NOT EXISTS <name>` (no MANAGED LOCATION) unreliable
regardless of whether the catalog is already there, and that's exactly
what cascaded into staging's "TABLE_OR_VIEW_NOT_FOUND" no matter how many
times a save was retried -- the catalog itself never got created there.

Fix: check catalog existence via the SDK first (Catalogs.get, a clean
lookup with no storage-root side effects) and only attempt creation when
it's genuinely missing, instead of relying on CREATE CATALOG IF NOT
EXISTS's SQL-level short-circuiting. Schema/table DDL doesn't have this
issue (confirmed working) and still runs via the Statement Execution API.
Idempotent and non-blocking throughout -- a permission/storage gap here
is a metastore-admin problem to fix, not something to block the deploy on.

Also verifies (and, if needed, repairs) the databricks-token Databricks
Secret the app's own runtime queries actually use -- see the comment
above the verify/repair block below for why a stale value there produces
the exact same symptom as the tables never having been created at all.

And grants the app's own Service Principal direct Unity Catalog access to
this catalog/schema (see the comment above that block) -- so a future
empty/stale/unreadable databricks-token secret degrades to a warning
instead of reproducing this entire "TABLE_OR_VIEW_NOT_FOUND on every save"
incident again.
"""
import os
import re
import sys
from pathlib import Path

wh_id = os.environ.get("SQL_WAREHOUSE_ID", "")

if not wh_id:
    print("No SQL_WAREHOUSE_ID set — skipping app table bootstrap")
    sys.exit(0)


def _read_app_yml_env(name, default):
    try:
        with open("app.yml", encoding="utf-8") as f:
            text = f.read()
    except OSError:
        return default
    m = re.search(rf'-\s*name:\s*{re.escape(name)}\s*\n\s*value:\s*"([^"]*)"', text)
    return m.group(1) if m and m.group(1) else default


catalog = _read_app_yml_env("DATABRICKS_CATALOG", "admin_source")
schema = _read_app_yml_env("DATABRICKS_SCHEMA", "migration_app")

sql_path = Path("src/sql/init_app_tables.sql")
if not sql_path.is_file():
    print(f"WARN: {sql_path} not found — skipping app table bootstrap")
    sys.exit(0)

raw_sql = sql_path.read_text(encoding="utf-8")
raw_sql = raw_sql.replace("${catalog}", catalog).replace("${schema}", schema)
# Drop full-line-comment-only chunks and the CREATE CATALOG statement (handled
# separately below via the SDK) before splitting on ";" -- CREATE SCHEMA and
# every CREATE TABLE run as-is through the Statement Execution API.
statements = []
for chunk in raw_sql.split(";"):
    stmt = chunk.strip()
    if not stmt:
        continue
    non_comment_lines = [l for l in stmt.splitlines() if l.strip() and not l.strip().startswith("--")]
    if not non_comment_lines:
        continue
    if non_comment_lines[0].upper().startswith("CREATE CATALOG"):
        continue  # handled via the SDK existence check below
    statements.append(stmt)

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()

    # Print both a plain line (for anyone reading the raw log) and a GitHub
    # Actions workflow-command-prefixed line, which turns into a check-run
    # annotation retrievable via the public
    # /repos/{owner}/{repo}/check-runs/{job_id}/annotations API without
    # needing the elevated auth "download job logs" requires.
    def _report(level, msg):
        print(msg)
        print(f"::{level}::{msg}")

    try:
        w.catalogs.get(catalog)
        print(f"Catalog '{catalog}' already exists — skipping creation")
    except Exception:
        try:
            w.catalogs.create(name=catalog)
            print(f"Created catalog '{catalog}'")
        except Exception as exc1:
            # This metastore has no default storage root, so a bare CREATE
            # CATALOG (no explicit location) fails here too. Instead of
            # giving up, piggyback on any external location that already
            # exists in this workspace (e.g. the bronze/silver root the
            # Infra Setup tool created) -- Unity Catalog only requires the
            # storage_root to fall under a registered external location's
            # URL, not that it have its own dedicated one.
            created = False
            try:
                locations = list(w.external_locations.list())
            except Exception:
                locations = []
            for loc in locations:
                try:
                    w.catalogs.create(name=catalog, storage_root=f"{loc.url.rstrip('/')}/{catalog}")
                    print(f"Created catalog '{catalog}' under existing external location '{loc.name}'")
                    created = True
                    break
                except Exception:
                    continue
            if not created:
                print(
                    f"WARN: could not create catalog '{catalog}' (non-blocking): {exc1}\n"
                    f"  No usable external location found in this workspace either "
                    f"({len(locations)} checked). A metastore admin needs to create the "
                    f"catalog manually with an explicit managed location, e.g.: "
                    f"CREATE CATALOG {catalog} MANAGED LOCATION '<abfss-path>'"
                )

    ok_count = 0
    for stmt in statements:
        label = stmt.splitlines()[0][:80]
        try:
            resp = w.statement_execution.execute_statement(
                warehouse_id=wh_id, statement=stmt, wait_timeout="30s",
            )
            state = resp.status.state.value if resp.status and resp.status.state else "UNKNOWN"
            if state == "SUCCEEDED":
                print(f"OK: {label}")
                ok_count += 1
            else:
                err = resp.status.error.message if resp.status and resp.status.error else state
                print(f"WARN: statement failed (non-blocking): {label} -> {err}")
        except Exception as exc:
            print(f"WARN: statement failed (non-blocking): {label} -> {exc}")
    print(f"Bootstrapped {ok_count}/{len(statements)} schema/table statement(s) in {catalog}.{schema}")

    # Grant the Databricks App's own Service Principal durable Unity Catalog
    # access to this catalog/schema, using the deploy identity's admin
    # rights (the same rights that just created the catalog/tables above).
    #
    # Without this grant, the ONLY thing that makes runtime queries work is
    # the "databricks-token" secret happening to hold a PAT belonging to an
    # identity that already has UC access (see the repair block below). If
    # that secret is ever empty, a stale/placeholder value, or simply
    # unreadable by the app (secret-scope ACL gap), dbsql_client.get_connection()
    # silently falls back to the app's own M2M OAuth SP -- an identity UC has
    # never granted anything to -- and every query returns
    # TABLE_OR_VIEW_NOT_FOUND, indistinguishable from the table not existing.
    # That's exactly the "Saved locally only" error users hit on Settings ->
    # Save Config, and why the config only ever lands in the ephemeral local
    # deployconfig.json fallback that SNAPSHOT deploys wipe on every release.
    #
    # Granting the SP directly here closes that gap for good, independent of
    # the secret's health -- the app now has two independent, valid paths to
    # its own tables instead of one fragile one. Idempotent (GRANT is safe to
    # re-run) and non-blocking (a permission gap here is worth surfacing, not
    # failing the deploy over).
    app_name = _read_app_yml_env("DATABRICKS_APP_NAME", "dbxmigration")
    try:
        apps = list(w.apps.list())
        app = next((a for a in apps if a.name == app_name), None)
        sp_id = app.service_principal_client_id if app else ""
        if not sp_id:
            _report("warning", f"Could not resolve service principal for app '{app_name}' -- skipping SP grant")
        else:
            for grant_sql in (
                f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{sp_id}`",
                f"GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA `{catalog}`.`{schema}` TO `{sp_id}`",
            ):
                try:
                    resp = w.statement_execution.execute_statement(
                        warehouse_id=wh_id, statement=grant_sql, wait_timeout="30s",
                    )
                    state = resp.status.state.value if resp.status and resp.status.state else "UNKNOWN"
                    if state == "SUCCEEDED":
                        print(f"OK: {grant_sql}")
                    else:
                        err = resp.status.error.message if resp.status and resp.status.error else state
                        _report("warning", f"Grant failed (non-blocking): {grant_sql} -> {err}")
                except Exception as exc:
                    _report("warning", f"Grant failed (non-blocking): {grant_sql} -> {exc}")
    except Exception as exc:
        _report("warning", f"Could not grant app SP catalog/schema access (non-blocking): {exc}")

    # The app's own runtime queries never use this script's/CI's deploy PAT
    # directly -- they use whatever PAT is stored in the databricks-token
    # Databricks Secret (deliberately never clobbered by the CI "Scaffold
    # secret scope keys" step, to protect a value a client may have set via
    # Settings > Secret Vault). Unity Catalog returns TABLE_OR_VIEW_NOT_FOUND
    # -- not PERMISSION_DENIED -- for an object the querying principal can't
    # see, which looks identical to "doesn't exist" in the app's own error
    # message. That made every earlier catalog-existence fix look like it
    # didn't work on a workspace whose stored secret is a stale/different/
    # under-privileged token from an earlier deploy. Verify it can actually
    # see app_config using the exact same kind of call the app makes at
    # runtime, and only replace it with this (just-proven-working) deploy
    # identity's PAT when it can't.
    try:
        secret_scope = _read_app_yml_env("DATABRICKS_SECRET_SCOPE", "migration-studio")
        needs_repair = True
        try:
            import base64 as _b64
            stored_secret = w.secrets.get_secret(scope=secret_scope, key="databricks-token")
            stored_token = _b64.b64decode(stored_secret.value).decode("utf-8") if stored_secret.value else ""
        except Exception as exc:
            stored_token = ""
            _report("warning", f"Could not read databricks-token secret to test it ({exc}) — will reseed it")

        if stored_token and stored_token.strip().upper() != "REPLACE_ME":
            try:
                test_w = WorkspaceClient(host=w.config.host, token=stored_token, auth_type="pat")
                test_w.statement_execution.execute_statement(
                    warehouse_id=wh_id,
                    statement=f"SELECT 1 FROM `{catalog}`.`{schema}`.app_config LIMIT 1",
                    wait_timeout="30s",
                )
                needs_repair = False
                _report("notice", "databricks-token secret can already see app_config — no repair needed")
            except Exception as exc:
                _report("warning", f"databricks-token secret cannot see app_config ({exc}) — repairing")
        else:
            _report("notice", "databricks-token secret is empty/placeholder — seeding it")

        if needs_repair:
            deploy_token = os.environ.get("DATABRICKS_TOKEN", "")
            if deploy_token:
                w.secrets.put_secret(scope=secret_scope, key="databricks-token", string_value=deploy_token)
                _report("notice", "Repaired databricks-token secret with the deploy identity's PAT")
            else:
                _report("warning", "no DATABRICKS_TOKEN in this job's env to repair with")
    except Exception as exc:
        _report("warning", f"could not verify/repair databricks-token secret (non-blocking): {exc}")
except Exception as exc:
    print(f"WARN: could not bootstrap app tables in {catalog}.{schema} (non-blocking): {exc}")
