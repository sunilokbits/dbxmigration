#!/usr/bin/env python3
"""Render Genie Space instructions/description templates with the catalogs
actually configured in this deployment's Settings (Metadata Catalog, and
each medallion layer's target catalog), instead of shipping the fixed
admin_source/bronze.hr/... names this app happened to be tested with.

Placeholders in deploy/genie_space_{instructions,description}.txt:
  {META_CAT}.{META_SCH}  -- Metadata Catalog/Schema (wf_* + app tables)
  {BRONZE} {SILVER}      -- medallion layer target catalog.schema
  {RECON} {LOG}          -- reconciliation / loggingdetails catalog.schema

Best-effort: on any failure this falls back to the static defaults
(same as the app itself does before Settings has ever been configured)
rather than blocking the deploy -- the Genie Space update step this feeds
is already non-blocking in cicd.yml.
"""
import json
import os
import re

wh_id = os.environ.get("SQL_WAREHOUSE_ID", "")


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

placeholders = {
    "META_CAT": catalog,
    "META_SCH": schema,
    "BRONZE": "bronze.hr",
    "SILVER": "silver.hr",
    "RECON": "reconciliation.hr",
    "LOG": "loggingdetails.hr",
}

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    if not wh_id:
        # SQL_WAREHOUSE_ID isn't resolved yet at this point in every job
        # (e.g. the staging job runs this before its own "Resolve SQL
        # Warehouse ID" step) -- discover one directly instead of skipping.
        warehouses = list(w.warehouses.list())
        running = next((wh for wh in warehouses if "RUNNING" in str(getattr(wh.state, "value", wh.state)).upper()), None)
        chosen = running or (warehouses[0] if warehouses else None)
        wh_id = chosen.id if chosen else ""
except Exception as exc:
    print(f"Could not resolve a SQL warehouse for Genie template rendering (using defaults): {exc}")

if wh_id:
    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=wh_id,
            statement=f"SELECT config_key, config_value FROM `{catalog}`.`{schema}`.app_config",
            wait_timeout="30s",
        )
        rows = result.result.data_array if result.result else []
        cfg = {}
        for row in rows:
            key, value = row[0], row[1]
            try:
                cfg[key] = json.loads(value)
            except (TypeError, ValueError):
                cfg[key] = value

        if cfg.get("metadata_catalog"):
            placeholders["META_CAT"] = cfg["metadata_catalog"]
        if cfg.get("metadata_schema"):
            placeholders["META_SCH"] = cfg["metadata_schema"]

        mapping = (cfg.get("existing_setting") or {}).get("medallion_layer_mapping") or {}
        for layer_key, ph in (("bronze", "BRONZE"), ("silver", "SILVER"),
                               ("reconciliation", "RECON"), ("loggingdetails", "LOG")):
            layer = mapping.get(layer_key) or {}
            if layer.get("catalog") and layer.get("schema"):
                placeholders[ph] = f"{layer['catalog']}.{layer['schema']}"

        # reports.py/workflow.py's Reconciliation Report and Audit execution-log
        # routes actually read these from top-level "reconciliation"/"logging"
        # config keys, not medallion_layer_mapping -- prefer those since they're
        # the ones a USE CATALOG error actually refers to.
        recon_top = cfg.get("reconciliation") or {}
        if recon_top.get("catalog") and recon_top.get("schema"):
            placeholders["RECON"] = f"{recon_top['catalog']}.{recon_top['schema']}"
        log_top = cfg.get("logging") or {}
        if log_top.get("catalog") and log_top.get("schema"):
            placeholders["LOG"] = f"{log_top['catalog']}.{log_top['schema']}"
    except Exception as exc:
        print(f"Could not read app_config for Genie template rendering (using defaults): {exc}")
else:
    print("No SQL_WAREHOUSE_ID set -- rendering Genie templates with defaults")

out_dir = "/tmp"
for name in ("genie_space_instructions.txt", "genie_space_description.txt"):
    src = os.path.join("deploy", name)
    if not os.path.isfile(src):
        continue
    with open(src, encoding="utf-8") as f:
        text = f.read()
    for key, val in placeholders.items():
        text = text.replace("{" + key + "}", val)
    dest = os.path.join(out_dir, name)
    with open(dest, "w", encoding="utf-8") as f:
        f.write(text)
    print(dest)
