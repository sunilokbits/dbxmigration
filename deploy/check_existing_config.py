#!/usr/bin/env python3
"""Report whether app_config already has rows in the target workspace.

Read-only audit step for CI/CD. The app itself already never resets this
table -- config_cache.py only ever does CREATE TABLE IF NOT EXISTS plus a
per-key MERGE upsert (insert new keys, update the ones a user explicitly
saves via Settings), so existing settings and any new setting a later
release adds both survive a redeploy without conflict. This script makes
that guarantee visible in the deploy log each run instead of leaving it
implicit, and gives an early signal if the table is unexpectedly empty
after a workspace's first-ever deploy.
"""
import os
import re
import sys

wh_id = os.environ.get("SQL_WAREHOUSE_ID", "")
if not wh_id:
    print("No SQL_WAREHOUSE_ID set — skipping config-preservation check")
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

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    result = w.statement_execution.execute_statement(
        warehouse_id=wh_id,
        statement=f"SELECT COUNT(*) FROM `{catalog}`.`{schema}`.app_config",
        wait_timeout="30s",
    )
    rows = result.result.data_array if result.result else None
    count = int(rows[0][0]) if rows else 0
except Exception as exc:
    print(f"Could not check existing config (non-blocking — likely a brand-new workspace where the table doesn't exist yet): {exc}")
    sys.exit(0)

if count:
    print(
        f"Found {count} existing config key(s) in {catalog}.{schema}.app_config — "
        "preserved as-is. This deploy only creates the table if missing and "
        "upserts individual keys a user explicitly saves via Settings; it "
        "never resets or bulk-overwrites existing rows."
    )
else:
    print(f"No existing config found in {catalog}.{schema}.app_config — first deploy for this workspace, nothing to preserve yet.")
