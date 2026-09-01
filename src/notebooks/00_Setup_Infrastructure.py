# Databricks notebook source
# MAGIC %md
# MAGIC # Migration Studio — Environment-Aware Infrastructure Setup
# MAGIC
# MAGIC Run this notebook to provision the Azure + Unity Catalog infrastructure this
# MAGIC app needs, for a chosen **environment (dev / staging / prod)**:
# MAGIC
# MAGIC 1. Azure Storage Account (ADLS Gen2) + Container + landing folders
# MAGIC 2. Databricks Access Connector + RBAC role assignment (connector → storage)
# MAGIC 3. Unity Catalog Storage Credential
# MAGIC 4. Unity Catalog External Locations
# MAGIC 5. Unity Catalog Catalogs + Schemas (Bronze / Silver — your own data; optionally
# MAGIC    Admin/Reconciliation/Logging — this app's own metadata, usually left as shared defaults)
# MAGIC 6. Landing-zone external Volume
# MAGIC
# MAGIC **Instructions:**
# MAGIC 1. Attach this notebook to a cluster in the **target** Databricks workspace for
# MAGIC    the chosen environment, and run Cell 1 to reveal the widgets.
# MAGIC 2. Fill in the widgets (resource names are auto-suffixed per environment —
# MAGIC    see Cell 3 for the exact naming rule).
# MAGIC 3. Run **All Cells** — every step is idempotent (safe to re-run).
# MAGIC 4. Copy the printed summary's catalog/storage names into
# MAGIC    `deploy/clients/<name>.json` for that environment's one-click deploy.
# MAGIC
# MAGIC This notebook is self-contained (no dependency on the `migration_utility`
# MAGIC Python package) so it can run in any Databricks workspace on its own.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration Widgets

# COMMAND ----------

dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")

dbutils.widgets.text("subscription_id", "", "Azure Subscription ID")
dbutils.widgets.text("resource_group", "", "Azure Resource Group")
dbutils.widgets.text("region", "centralindia", "Azure Region")

dbutils.widgets.text("storage_account_base", "", "Storage Account Name (base, no suffix)")
dbutils.widgets.text("container", "datalake", "Container Name")

dbutils.widgets.text("access_connector_base", "", "Access Connector Name (base, no suffix)")
dbutils.widgets.text("storage_credential_base", "", "Storage Credential Name (base, no suffix)")
dbutils.widgets.dropdown("role_assignment", "Storage Blob Data Owner", [
    "Storage Blob Data Owner", "Storage Blob Data Contributor",
    "Storage Blob Data Reader", "User Access Administrator", "Contributor",
], "Access Connector RBAC Role")

dbutils.widgets.text("catalog_bronze_base", "bronze", "Bronze Catalog (base, client-owned)")
dbutils.widgets.text("catalog_silver_base", "silver", "Silver Catalog (base, client-owned)")
dbutils.widgets.text("catalog_volumes_base", "dev_volumes", "Volumes Catalog (base, client-owned)")
dbutils.widgets.text("default_schema", "hr", "Default Schema")

dbutils.widgets.dropdown("include_metadata_catalogs", "false", ["false", "true"],
                          "Also create Admin/Reconciliation/Logging catalogs")
dbutils.widgets.text("catalog_admin", "admin_source", "Admin/Metadata Catalog (app-owned)")
dbutils.widgets.text("catalog_recon", "reconciliation", "Reconciliation Catalog (app-owned)")
dbutils.widgets.text("catalog_logging", "loggingdetails", "Logging Catalog (app-owned)")

# Optional — only needed if this cluster's identity can't reach Azure ARM directly
# (e.g. running against a different tenant than the workspace's own).
dbutils.widgets.text("azure_tenant_id", "", "Azure Tenant ID (optional — SP auth)")
dbutils.widgets.text("azure_client_id", "", "Azure Client ID (optional — SP auth)")
dbutils.widgets.text("azure_client_secret", "", "Azure Client Secret (optional — SP auth)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Install Azure SDK Packages

# COMMAND ----------

# MAGIC %pip install -q azure-identity azure-mgmt-storage azure-storage-file-datalake azure-mgmt-databricks azure-mgmt-authorization
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Resolve Environment-Suffixed Resource Names
# MAGIC
# MAGIC Mirrors the existing `databricks.yml` bundle-variable convention
# MAGIC (`bronze_staging`/`bronze_prod`, `sqltodatabrciksmigprod`):
# MAGIC - **dev** → no suffix
# MAGIC - **staging** → `_staging` for catalogs/connector, `stg` for storage account
# MAGIC   (Azure storage account names allow only lowercase letters/digits)
# MAGIC - **prod** → `_prod` for catalogs/connector, `prod` for storage account

# COMMAND ----------

ENV = dbutils.widgets.get("environment")

_CATALOG_SUFFIX = {"dev": "", "staging": "_staging", "prod": "_prod"}[ENV]
_STORAGE_SUFFIX = {"dev": "", "staging": "stg", "prod": "prod"}[ENV]

storage_account = (dbutils.widgets.get("storage_account_base") + _STORAGE_SUFFIX).lower()
access_connector = dbutils.widgets.get("access_connector_base") + _CATALOG_SUFFIX
storage_credential_name = dbutils.widgets.get("storage_credential_base") + _CATALOG_SUFFIX
container = dbutils.widgets.get("container")

catalog_bronze = dbutils.widgets.get("catalog_bronze_base") + _CATALOG_SUFFIX
catalog_silver = dbutils.widgets.get("catalog_silver_base") + _CATALOG_SUFFIX
catalog_volumes = dbutils.widgets.get("catalog_volumes_base") + _CATALOG_SUFFIX
default_schema = dbutils.widgets.get("default_schema")

include_metadata = dbutils.widgets.get("include_metadata_catalogs") == "true"
catalog_admin = dbutils.widgets.get("catalog_admin")
catalog_recon = dbutils.widgets.get("catalog_recon")
catalog_logging = dbutils.widgets.get("catalog_logging")

subscription_id = dbutils.widgets.get("subscription_id")
resource_group = dbutils.widgets.get("resource_group")
region = dbutils.widgets.get("region")
role_assignment = dbutils.widgets.get("role_assignment")

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
landing_folder = f"{ENV}/landing"
bronze_folder = f"{ENV}/uc-managed/bronze"
silver_folder = f"{ENV}/uc-managed/silver"

print(f"Environment          : {ENV}")
print(f"Storage Account       : {storage_account}")
print(f"Container             : {container}")
print(f"Access Connector      : {access_connector}")
print(f"Storage Credential    : {storage_credential_name}")
print(f"Bronze Catalog        : {catalog_bronze}  → {base_path}/{bronze_folder}")
print(f"Silver Catalog        : {catalog_silver}  → {base_path}/{silver_folder}")
print(f"Volumes Catalog       : {catalog_volumes}")
print(f"Landing Path          : {base_path}/{landing_folder}")
print(f"Metadata catalogs?    : {include_metadata} (admin={catalog_admin}, recon={catalog_recon}, logging={catalog_logging})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Helpers — Azure Credential & Databricks REST API

# COMMAND ----------

import json
import time
import uuid
from datetime import datetime


def _ts():
    return datetime.now().strftime("%H:%M:%S")


def _log(msg, level="INFO"):
    print(f"[{_ts()}] [{level}] {msg}")


def get_azure_credential():
    """Service Principal if widgets are filled, otherwise DefaultAzureCredential
    (picks up the notebook's managed identity / attached auth)."""
    tenant = dbutils.widgets.get("azure_tenant_id")
    client = dbutils.widgets.get("azure_client_id")
    secret = dbutils.widgets.get("azure_client_secret")
    if tenant and client and secret:
        from azure.identity import ClientSecretCredential
        cred = ClientSecretCredential(tenant_id=tenant, client_id=client, client_secret=secret)
        cred.get_token("https://management.azure.com/.default")
        _log("Authenticated to Azure via Service Principal.")
        return cred
    from azure.identity import DefaultAzureCredential
    cred = DefaultAzureCredential()
    cred.get_token("https://management.azure.com/.default")
    _log("Authenticated to Azure via DefaultAzureCredential.")
    return cred


def get_workspace_host_and_token():
    """Use the notebook's own execution context — no PAT widget required."""
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    host = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
    token = ctx.apiToken().get()
    return host, token


_HOST, _TOKEN = get_workspace_host_and_token()


def _databricks_api(method, path, payload=None):
    import requests
    url = f"{_HOST}{path}"
    headers = {"Authorization": f"Bearer {_TOKEN}", "Content-Type": "application/json"}
    r = requests.request(method, url, headers=headers, json=payload, timeout=30)
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text}
    return (200 <= r.status_code < 300), body

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Step 1 — Storage Account + Container + Folders

# COMMAND ----------

def create_storage():
    _log("═══ Step 1: Storage Account + Container + Folders ═══")
    from azure.mgmt.storage import StorageManagementClient
    from azure.mgmt.storage.models import StorageAccountCreateParameters, Sku, Kind
    from azure.storage.filedatalake import DataLakeServiceClient

    credential = get_azure_credential()
    storage_client = StorageManagementClient(credential, subscription_id)

    try:
        storage_client.storage_accounts.get_properties(resource_group, storage_account)
        _log(f"Storage account '{storage_account}' already exists — OK.")
    except Exception:
        _log(f"Creating storage account '{storage_account}' in '{region}'…")
        params = StorageAccountCreateParameters(
            sku=Sku(name="Standard_LRS"), kind=Kind.STORAGE_V2,
            location=region, is_hns_enabled=True,
        )
        poller = storage_client.storage_accounts.begin_create(resource_group, storage_account, params)
        poller.result()
        _log(f"Storage account '{storage_account}' created.")

    account_url = f"https://{storage_account}.dfs.core.windows.net"
    datalake_client = DataLakeServiceClient(account_url=account_url, credential=credential)
    try:
        fs_client = datalake_client.create_file_system(container)
        _log(f"Container '{container}' created.")
    except Exception as e:
        if "already exists" in str(e).lower() or "ContainerAlreadyExists" in str(e):
            _log(f"Container '{container}' already exists — OK.")
            fs_client = datalake_client.get_file_system_client(container)
        else:
            raise

    for folder in (landing_folder, bronze_folder, silver_folder):
        try:
            fs_client.create_directory(folder)
            _log(f"Folder '{folder}' created.")
        except Exception as e:
            if "already exists" in str(e).lower() or "PathAlreadyExists" in str(e):
                _log(f"Folder '{folder}' already exists — OK.")
            else:
                _log(f"Failed to create folder '{folder}': {e}", "ERROR")


create_storage()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Step 2 — Access Connector + RBAC Role Assignment

# COMMAND ----------

def create_access_connector():
    _log("═══ Step 2: Access Connector + Role Assignment ═══")
    from azure.mgmt.databricks import AzureDatabricksManagementClient
    from azure.mgmt.authorization import AuthorizationManagementClient

    credential = get_azure_credential()
    dbr_client = AzureDatabricksManagementClient(credential, subscription_id)

    connector_body = {"location": region, "identity": {"type": "SystemAssigned"}}
    try:
        poller = dbr_client.access_connectors.begin_create_or_update(
            resource_group, access_connector, connector_body)
        ac_result = poller.result()
        _log(f"Access Connector '{access_connector}' created/updated.")
    except Exception:
        ac_result = dbr_client.access_connectors.get(resource_group, access_connector)
        _log(f"Access Connector '{access_connector}' already exists — using it.")

    connector_id = ac_result.id
    principal_id = ac_result.identity.principal_id if ac_result.identity else None
    _log(f"Access Connector ID: {connector_id}")

    if principal_id:
        storage_scope = (
            f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
            f"/providers/Microsoft.Storage/storageAccounts/{storage_account}"
        )
        auth_client = AuthorizationManagementClient(credential, subscription_id)
        role_defs = list(auth_client.role_definitions.list(
            storage_scope, filter=f"roleName eq '{role_assignment}'"))
        if role_defs:
            role_def_id = role_defs[0].id
            assignment_name = str(uuid.uuid5(
                uuid.NAMESPACE_URL, f"{principal_id}:{role_def_id}:{storage_scope}"))
            try:
                auth_client.role_assignments.create(storage_scope, assignment_name, {
                    "role_definition_id": role_def_id,
                    "principal_id": principal_id,
                    "principal_type": "ServicePrincipal",
                })
                _log(f"Role '{role_assignment}' assigned to Access Connector on storage account.")
            except Exception as e:
                if "already exists" in str(e).lower() or "RoleAssignmentExists" in str(e):
                    _log("Role assignment already exists — OK.")
                else:
                    _log(f"Role assignment warning (may need manual RBAC): {e}", "WARN")
        else:
            _log(f"Role definition '{role_assignment}' not found!", "ERROR")
    else:
        _log("No principalId on connector — role assignment skipped.", "WARN")

    return connector_id


_connector_id = create_access_connector()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Step 3 — Register Storage Credential in Unity Catalog

# COMMAND ----------

def create_storage_credential(connector_id):
    _log("═══ Step 3: Register Storage Credential ═══")
    payload = {
        "name": storage_credential_name,
        "azure_managed_identity": {"access_connector_id": connector_id},
        "skip_validation": True,
        "comment": f"Auto-created from Access Connector {access_connector} ({ENV})",
    }
    ok, body = _databricks_api("POST", "/api/2.1/unity-catalog/storage-credentials", payload)
    if ok:
        _log(f"Storage credential '{storage_credential_name}' registered.")
    elif "already exists" in json.dumps(body).lower():
        _log(f"Storage credential '{storage_credential_name}' already exists — updating connector.")
        _databricks_api("PATCH", f"/api/2.1/unity-catalog/storage-credentials/{storage_credential_name}",
                         {"azure_managed_identity": {"access_connector_id": connector_id},
                          "skip_validation": True, "force": True})
    else:
        raise RuntimeError(f"Storage credential creation failed: {body}")

    _log("Validating storage credential (RBAC propagation may take a few minutes)…")
    for attempt in range(1, 4):
        vok, vbody = _databricks_api(
            "PATCH", f"/api/2.1/unity-catalog/storage-credentials/{storage_credential_name}",
            {"azure_managed_identity": {"access_connector_id": connector_id},
             "skip_validation": False, "force": True})
        if vok:
            _log(f"Storage credential validated (attempt {attempt}).")
            break
        _log(f"Validation attempt {attempt}/3 failed — RBAC may still be propagating, waiting 15s…", "INFO")
        if attempt < 3:
            time.sleep(15)
    else:
        _log("Validation deferred — will succeed once RBAC propagation completes.", "WARN")

    return storage_credential_name


_cred_name = create_storage_credential(_connector_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Step 4 — External Locations

# COMMAND ----------

def create_external_locations(credential_name):
    _log("═══ Step 4: External Locations ═══")
    locations = {
        f"{ENV}-landing-loc": f"{base_path}/{landing_folder}",
        f"{ENV}-bronze-loc": f"{base_path}/{bronze_folder}",
        f"{ENV}-silver-loc": f"{base_path}/{silver_folder}",
    }
    for loc_name, url in locations.items():
        _log(f"Creating external location '{loc_name}' → {url}")
        payload = {"name": loc_name, "url": url, "credential_name": credential_name, "skip_validation": True}
        ok, body = _databricks_api("POST", "/api/2.1/unity-catalog/external-locations", payload)
        if ok:
            _log(f"External location '{loc_name}' created.")
        elif "already exists" in json.dumps(body).lower():
            _log(f"External location '{loc_name}' already exists — updating.")
            _databricks_api("PATCH", f"/api/2.1/unity-catalog/external-locations/{loc_name}",
                             {"credential_name": credential_name, "skip_validation": True})
        elif "overlap" in json.dumps(body).lower():
            _log(f"External location '{loc_name}' overlaps an existing one — skipping.")
        else:
            _log(f"Failed to create external location '{loc_name}': {body}", "ERROR")


create_external_locations(_cred_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Step 5 — Catalogs + Schemas
# MAGIC
# MAGIC Bronze/Silver are client-owned (this environment's own data). Admin/
# MAGIC Reconciliation/Logging are this app's own metadata catalogs — only
# MAGIC created here if you opted in via the widget; normally they're
# MAGIC provisioned once via the existing bundle/CI-CD path instead.

# COMMAND ----------

def _create_catalog_and_schema(catalog_name, storage_root, schema_name, comment):
    payload = {"name": catalog_name, "comment": comment}
    if storage_root:
        payload["storage_root"] = storage_root
    ok, body = _databricks_api("POST", "/api/2.1/unity-catalog/catalogs", payload)
    if ok:
        _log(f"Catalog '{catalog_name}' created.")
    elif "already exists" in json.dumps(body).lower():
        _log(f"Catalog '{catalog_name}' already exists — OK.")
    else:
        _log(f"Failed to create catalog '{catalog_name}': {body}", "ERROR")
        return

    ok2, body2 = _databricks_api("POST", "/api/2.1/unity-catalog/schemas",
                                  {"name": schema_name, "catalog_name": catalog_name, "comment": comment})
    if ok2:
        _log(f"  Schema '{catalog_name}.{schema_name}' created.")
    elif "already exists" in json.dumps(body2).lower():
        _log(f"  Schema '{catalog_name}.{schema_name}' already exists — OK.")
    else:
        _log(f"  Failed to create schema '{catalog_name}.{schema_name}': {body2}", "ERROR")


def create_catalogs():
    _log("═══ Step 5: Catalogs + Schemas ═══")
    _create_catalog_and_schema(catalog_bronze, f"{base_path}/{bronze_folder}",
                                default_schema, f"Bronze layer — {ENV}")
    _create_catalog_and_schema(catalog_silver, f"{base_path}/{silver_folder}",
                                default_schema, f"Silver layer — {ENV}")
    # Volumes catalog (no dedicated storage_root — schema/volume path set in Step 6)
    _create_catalog_and_schema(catalog_volumes, "", default_schema, f"Volumes — {ENV}")

    if include_metadata:
        _log("Also creating Admin/Reconciliation/Logging catalogs (opted in)…")
        _create_catalog_and_schema(catalog_admin, "", "configtables", "App metadata — configtables")
        _create_catalog_and_schema(catalog_recon, "", default_schema, "Reconciliation results")
        _create_catalog_and_schema(catalog_logging, "", default_schema, "Execution logging")
    else:
        _log("Skipping Admin/Reconciliation/Logging catalogs (app metadata — not opted in).")


create_catalogs()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Step 6 — Landing-Zone Volume

# COMMAND ----------

def create_volume():
    _log("═══ Step 6: Volume ═══")
    vol_path = f"{base_path}/{landing_folder}"
    payload = {
        "name": "landing", "catalog_name": catalog_volumes, "schema_name": default_schema,
        "volume_type": "EXTERNAL", "storage_location": vol_path,
        "comment": f"Landing zone external volume — {ENV}",
    }
    for attempt in range(1, 4):
        ok, body = _databricks_api("POST", "/api/2.1/unity-catalog/volumes", payload)
        if ok:
            _log(f"Volume 'landing' created → {vol_path}")
            return
        if "already exists" in json.dumps(body).lower():
            _log("Volume 'landing' already exists — OK.")
            return
        if any(k in json.dumps(body).lower() for k in ("cloud_storage", "access", "abfs")):
            if attempt < 3:
                _log(f"Volume creation attempt {attempt}/3 failed (RBAC still propagating) — waiting 20s…", "WARN")
                time.sleep(20)
                continue
        _log(f"Failed to create volume: {body}", "ERROR")
        return


create_volume()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Summary — Copy Into `deploy/clients/<name>.json`

# COMMAND ----------

print("=" * 70)
print(f"Infrastructure setup complete for environment: {ENV}")
print("=" * 70)
print(json.dumps({
    "storage_account": storage_account,
    "container": container,
    "access_connector": access_connector,
    "storage_credential_name": storage_credential_name,
    "catalog_bronze": catalog_bronze,
    "catalog_silver": catalog_silver,
    "catalog_volumes": catalog_volumes,
    "default_schema": default_schema,
    "landing_path": f"{base_path}/{landing_folder}",
    **({"catalog_admin": catalog_admin, "catalog_recon": catalog_recon,
        "catalog_logging": catalog_logging} if include_metadata else {}),
}, indent=2))
