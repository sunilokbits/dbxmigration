"""
AutoInfraCreation — Automated Unity Catalog Infrastructure Setup
Creates Azure Storage, Access Connector, External Locations, Volume, and Catalogs
for an external Unity Catalog on Azure Databricks.

Uses Azure Python SDK (no Azure CLI dependency).

Usage:
    python AutoInfraCreation.py                 # interactive — prompts for credentials
    python AutoInfraCreation.py --auto          # uses env-vars for all credentials
"""

import os
import sys
import json
import time
import uuid
import argparse
from datetime import datetime


# ═══════════════════════════════════════════════════════════════════════════════
#  Configuration — loaded at runtime from deployconfig.json via Settings page.
#  These are minimal fallbacks only used when running the script directly.
# ═══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # Azure
    "subscription_id"    : "",
    "region"             : "centralindia",
    "resource_group"     : "",

    # Storage Account
    "storage_account"    : "",
    "container"          : "datalake",
    "folders"            : [],

    # Access Connector
    "access_connector"   : "",

    # Databricks workspace
    "databricks_host"    : os.getenv("DATABRICKS_HOST", ""),
    "databricks_token"   : os.getenv("DATABRICKS_TOKEN", ""),

    # External Locations
    "external_locations" : {},

    # Volume
    "volume_name"        : "landing",
    "volume_catalog"     : "",
    "volume_schema"      : "default",
    "volume_path"        : "",

    # Catalogs  →  catalog_name : {location, schemas}
    "catalogs"           : {},
}


# ═══════════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _ts():
    return datetime.now().strftime("%H:%M:%S")


def _import_attr(module_path, attr, pip_name, fallback_modules=()):
    """Return `attr` from `module_path`, self-healing across environments.

    `azure.*` are PEP 420 namespace packages: when a distribution is missing
    or only partially installed, `import azure.mgmt.resource` can still
    succeed (resolving to an empty namespace dir) while the client class is
    absent — surfacing as "module has no attribute 'ResourceManagementClient'"
    rather than ImportError. So an AttributeError must trigger the same
    reinstall path as a failed import, and we also try the class's real
    defining submodule (e.g. azure.mgmt.resource.resources) as a fallback.
    """
    import importlib

    def _lookup():
        for path in (module_path,) + tuple(fallback_modules):
            try:
                mod = importlib.import_module(path)
            except ImportError:
                continue
            found = getattr(mod, attr, None)
            if found is not None:
                return found
        return None

    found = _lookup()
    if found is not None:
        return found

    _log(f"'{attr}' unavailable from {pip_name} — (re)installing…", "WARN")
    import subprocess
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--quiet", "--upgrade",
         "--force-reinstall", "--no-cache-dir", pip_name]
    )
    importlib.invalidate_caches()
    for path in (module_path,) + tuple(fallback_modules):
        if path in sys.modules:
            del sys.modules[path]

    found = _lookup()
    if found is None:
        raise ImportError(
            f"Could not resolve '{attr}' from '{module_path}' even after "
            f"reinstalling '{pip_name}'. The Python environment may have a "
            f"conflicting/partial 'azure' namespace package installation."
        )
    return found


def _log(msg, level="INFO"):
    print(f"[{_ts()}] [{level}] {msg}")


# Cache credential so the browser prompt only appears once per session
_CACHED_CREDENTIAL = None
_USER_CREDENTIAL = None   # injected from settings.py (device code auth)

def set_user_credential(cred):
    """Allow settings.py to inject the device-code / admin credential."""
    global _USER_CREDENTIAL
    _USER_CREDENTIAL = cred

def _get_azure_credential(cfg=None):
    """Return an Azure credential.

    If cfg contains azure_tenant_id, azure_client_id, azure_client_secret,
    uses ClientSecretCredential (Service Principal).
    Otherwise tries DefaultAzureCredential (env-vars, managed-identity,
    VS Code, Azure CLI).  Falls back to InteractiveBrowserCredential.
    """
    global _CACHED_CREDENTIAL

    # Priority 1: User credential injected from device code auth (admin permissions)
    if _USER_CREDENTIAL is not None:
        try:
            _USER_CREDENTIAL.get_token("https://management.azure.com/.default")
            _log("Authenticated via user device-code credential (admin).")
            return _USER_CREDENTIAL
        except Exception:
            pass  # token expired, fall through

    # Priority 2: SP credentials from config
    if cfg:
        sp_tenant = (cfg.get("azure_tenant_id") or "").strip()
        sp_client = (cfg.get("azure_client_id") or "").strip()
        sp_secret = (cfg.get("azure_client_secret") or "").strip()
        if sp_tenant and sp_client and sp_secret:
            from azure.identity import ClientSecretCredential
            cred = ClientSecretCredential(
                tenant_id=sp_tenant, client_id=sp_client, client_secret=sp_secret
            )
            cred.get_token("https://management.azure.com/.default")
            _log("Authenticated via Service Principal.")
            _CACHED_CREDENTIAL = cred
            return cred
        if sp_tenant or sp_client or sp_secret:
            missing = [
                name for name, value in (
                    ("Tenant ID", sp_tenant),
                    ("Client ID", sp_client),
                    ("Client Secret", sp_secret),
                ) if not value
            ]
            _log(
                "Service Principal partially configured — missing: "
                + ", ".join(missing)
                + ". Fill these under Settings → Azure Service Principal, then Save Config.",
                "WARN",
            )

    if _CACHED_CREDENTIAL is not None:
        return _CACHED_CREDENTIAL

    from azure.identity import DefaultAzureCredential

    try:
        cred = DefaultAzureCredential()
        # Force a token request to verify it works
        cred.get_token("https://management.azure.com/.default")
        _log("Authenticated via DefaultAzureCredential.")
        _CACHED_CREDENTIAL = cred
        return cred
    except Exception as e:
        # This code always runs inside a Flask request (local dev server or the
        # deployed Databricks App) — there is no interactive browser session to
        # complete an OAuth redirect back to, so InteractiveBrowserCredential
        # would previously fail with an opaque "Failed to open a browser" OS
        # error. Fail fast with an actionable message instead.
        raise RuntimeError(
            "No Azure credentials available (DefaultAzureCredential failed: "
            f"{e}). Fill in Tenant ID, Client ID and Client Secret under "
            "Settings \u2192 Azure Service Principal, or run 'az login' on the "
            "machine hosting this app."
        ) from e


def _lookup_credential_connector_id(cfg, cred_name):
    """Return the Access Connector ID already bound to an existing UC storage
    credential, or "" if there isn't one.

    Authoritative (and PAT-only): reuses whatever connector the credential was
    actually created with, instead of guessing a resource ID from config that
    may name a connector which doesn't exist.
    """
    if not cred_name:
        return ""
    ok, body = _databricks_api(
        "GET", f"/api/2.1/unity-catalog/storage-credentials/{cred_name}", cfg)
    if not ok or not isinstance(body, dict):
        return ""
    return (body.get("azure_managed_identity") or {}).get("access_connector_id", "")


def _azure_credentials_available(cfg):
    """True if an Azure Resource Manager credential can be obtained."""
    try:
        _get_azure_credential(cfg)
        return True
    except Exception as e:
        _log(f"Azure credentials unavailable — Azure steps will be skipped. ({e})", "WARN")
        return False


def _derive_connector_id(cfg):
    """Build the Access Connector's ARM resource ID from config alone.

    Lets the Unity Catalog steps (3-6) run with only a Databricks PAT when the
    connector was pre-created outside this app — a PAT authenticates to the
    Databricks API, never to Azure Resource Manager, so Steps 0-2 can't run
    without Azure credentials.
    """
    sub = (cfg.get("subscription_id") or "").strip()
    rg = (cfg.get("resource_group") or "").strip()
    ac = (cfg.get("access_connector") or "").strip()
    if not (sub and rg and ac):
        return ""
    return (f"/subscriptions/{sub}/resourceGroups/{rg}"
            f"/providers/Microsoft.Databricks/accessConnectors/{ac}")


def _databricks_api(method, path, cfg, payload=None):
    """Call Databricks REST API. Returns (success:bool, data:dict)."""
    import requests

    url = f"{cfg['databricks_host'].rstrip('/')}{path}"
    headers = {
        "Authorization": f"Bearer {cfg['databricks_token']}",
        "Content-Type":  "application/json",
    }
    resp = requests.request(method, url, headers=headers, json=payload, timeout=60)
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text[:500]}

    ok = 200 <= resp.status_code < 300
    if not ok:
        # "already exists" is expected for idempotent re-runs — log at DEBUG
        err_code = body.get("error_code", "") if isinstance(body, dict) else ""
        level = "DEBUG" if "ALREADY_EXISTS" in err_code else "ERROR"
        _log(f"Databricks API {resp.status_code}: {json.dumps(body)[:300]}", level)
    return ok, body


# ═══════════════════════════════════════════════════════════════════════════════
#  Step 0 — Verify Azure Credentials  (Python SDK — no CLI needed)
# ═══════════════════════════════════════════════════════════════════════════════

def set_subscription(cfg):
    """Verify Azure credentials and subscription access via Python SDK."""
    ResourceManagementClient = _import_attr(
        "azure.mgmt.resource", "ResourceManagementClient", "azure-mgmt-resource",
        fallback_modules=("azure.mgmt.resource.resources",),
    )

    sub = cfg["subscription_id"]
    _log(f"Authenticating to Azure (subscription: {sub})…")
    credential = _get_azure_credential(cfg)
    # Verify we can access the subscription by listing resource groups
    rm_client = ResourceManagementClient(credential, sub)
    rg_name = cfg["resource_group"]
    try:
        rg = rm_client.resource_groups.get(rg_name)
        _log(f"Verified: Resource Group '{rg_name}' exists in '{rg.location}'")
    except Exception as e:
        if "not found" in str(e).lower() or "could not be found" in str(e).lower():
            _log(f"Resource Group '{rg_name}' not found — creating in '{cfg['region']}'…")
            rm_client.resource_groups.create_or_update(rg_name, {"location": cfg["region"]})
            _log(f"Resource Group '{rg_name}' created.")
        else:
            raise
    _log("Azure authentication successful.")


# ═══════════════════════════════════════════════════════════════════════════════
#  Step 1 — Create Storage Account + Container + Folders  (Python SDK)
# ═══════════════════════════════════════════════════════════════════════════════

def create_storage(cfg):
    _log("═══ Step 1: Storage Account + Container + Folders ═══")

    StorageManagementClient = _import_attr(
        "azure.mgmt.storage", "StorageManagementClient", "azure-mgmt-storage")
    StorageAccountCreateParameters = _import_attr(
        "azure.mgmt.storage.models", "StorageAccountCreateParameters", "azure-mgmt-storage")
    Sku = _import_attr("azure.mgmt.storage.models", "Sku", "azure-mgmt-storage")
    Kind = _import_attr("azure.mgmt.storage.models", "Kind", "azure-mgmt-storage")
    DataLakeServiceClient = _import_attr(
        "azure.storage.filedatalake", "DataLakeServiceClient", "azure-storage-file-datalake")

    sub  = cfg["subscription_id"]
    rg   = cfg["resource_group"]
    sa   = cfg["storage_account"]
    loc  = cfg["region"]
    ctr  = cfg["container"]
    credential = _get_azure_credential(cfg)

    # 1a — Storage account (HNS enabled for ADLS Gen2)
    _log(f"Creating storage account '{sa}' in '{loc}'…")
    storage_client = StorageManagementClient(credential, sub)
    try:
        existing = storage_client.storage_accounts.get_properties(rg, sa)
        _log(f"Storage account '{sa}' already exists — OK.", "INFO")
    except Exception:
        # Create the account
        params = StorageAccountCreateParameters(
            sku=Sku(name="Standard_LRS"),
            kind=Kind.STORAGE_V2,
            location=loc,
            is_hns_enabled=True,  # hierarchical namespace → ADLS Gen2
        )
        poller = storage_client.storage_accounts.begin_create(rg, sa, params)
        poller.result()  # wait for completion
        _log(f"Storage account '{sa}' created.")

    # Verify it exists
    try:
        sa_info = storage_client.storage_accounts.get_properties(rg, sa)
        _log(f"Storage account '{sa}' verified (id: {sa_info.id[:80]}…)")
    except Exception as e:
        raise RuntimeError(
            f"Storage account '{sa}' not found after create. "
            f"Check RG '{rg}' exists and you have permissions. Error: {e}"
        )

    # 1b & 1c — Container + Folders using DataLake SDK
    account_url = f"https://{sa}.dfs.core.windows.net"
    datalake_client = DataLakeServiceClient(account_url=account_url, credential=credential)

    _log(f"Creating container (filesystem) '{ctr}'…")
    try:
        fs_client = datalake_client.create_file_system(ctr)
        _log(f"Container '{ctr}' created.")
    except Exception as e:
        if "already exists" in str(e).lower() or "ContainerAlreadyExists" in str(e):
            _log(f"Container '{ctr}' already exists — OK.", "INFO")
            fs_client = datalake_client.get_file_system_client(ctr)
        else:
            raise

    # 1c — Folders (directories in ADLS)
    for folder in cfg.get("folders", []):
        _log(f"Creating folder '{folder}'…")
        try:
            dir_client = fs_client.create_directory(folder)
            _log(f"  Folder '{folder}' created.")
        except Exception as e:
            if "already exists" in str(e).lower() or "PathAlreadyExists" in str(e):
                _log(f"  Folder '{folder}' already exists — OK.", "INFO")
            else:
                _log(f"  Failed to create folder '{folder}': {e}", "ERROR")
    _log("All folders created.")


# ═══════════════════════════════════════════════════════════════════════════════
#  Step 2 — Create Access Connector + Role Assignment  (Python SDK)
# ═══════════════════════════════════════════════════════════════════════════════

def create_access_connector(cfg):
    _log("═══ Step 2: Access Connector + Role Assignment ═══")

    AzureDatabricksManagementClient = _import_attr(
        "azure.mgmt.databricks", "AzureDatabricksManagementClient", "azure-mgmt-databricks")
    AuthorizationManagementClient = _import_attr(
        "azure.mgmt.authorization", "AuthorizationManagementClient", "azure-mgmt-authorization")

    sub  = cfg["subscription_id"]
    rg   = cfg["resource_group"]
    loc  = cfg["region"]
    ac   = cfg["access_connector"]
    sa   = cfg["storage_account"]
    credential = _get_azure_credential(cfg)

    # 2a — Create Access Connector via azure-mgmt-databricks
    _log(f"Creating Access Connector '{ac}'…")
    dbr_client = AzureDatabricksManagementClient(credential, sub)
    connector_body = {
        "location": loc,
        "identity": {"type": "SystemAssigned"},
    }
    try:
        poller = dbr_client.access_connectors.begin_create_or_update(rg, ac, connector_body)
        ac_result = poller.result()
        _log(f"Access Connector '{ac}' created/updated.")
    except Exception as e:
        # Try to fetch it if it already exists
        try:
            ac_result = dbr_client.access_connectors.get(rg, ac)
            _log(f"Access Connector '{ac}' already exists — using it.", "INFO")
        except Exception as e2:
            raise RuntimeError(
                f"Access Connector '{ac}' not found in RG '{rg}'. Error: {e2}"
            )

    connector_id = ac_result.id
    principal_id = (ac_result.identity.principal_id
                    if ac_result.identity else None)

    if not connector_id:
        raise RuntimeError(
            f"Access Connector '{ac}' not found in RG '{rg}'. "
            f"Verify the resource group exists."
        )

    _log(f"Access Connector ID: {connector_id}")

    if principal_id:
        _log(f"Access Connector principal ID: {principal_id}")
        # 2c — Access Connector MUST have a storage-data role for Unity Catalog.
        #      The user's chosen role (e.g. "User Access Administrator") is for
        #      the App Service identity, not the Access Connector.
        storage_scope = (
            f"/subscriptions/{sub}/resourceGroups/{rg}"
            f"/providers/Microsoft.Storage/storageAccounts/{sa}"
        )
        ac_role = "Storage Blob Data Owner"
        _log(f"Assigning '{ac_role}' role to Access Connector on storage account…")

        auth_client = AuthorizationManagementClient(credential, sub)

        # Find the role definition ID
        role_defs = list(auth_client.role_definitions.list(
            storage_scope,
            filter=f"roleName eq '{ac_role}'"
        ))
        if not role_defs:
            _log(f"Role definition '{ac_role}' not found!", "ERROR")
        else:
            role_def_id = role_defs[0].id
            assignment_name = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{principal_id}:{role_def_id}:{storage_scope}"))
            try:
                auth_client.role_assignments.create(
                    storage_scope,
                    assignment_name,
                    {
                        "role_definition_id": role_def_id,
                        "principal_id": principal_id,
                        "principal_type": "ServicePrincipal",
                    },
                )
                _log("Role assignment complete.")
            except Exception as e:
                if "already exists" in str(e).lower() or "RoleAssignmentExists" in str(e):
                    _log("Role assignment already exists — OK.", "INFO")
                else:
                    _log(f"Role assignment warning: {e}", "WARN")
                    _log(f"ACTION REQUIRED: Manually assign '{ac_role}' role to the Access Connector's managed identity.", "WARN")
                    _log(f"  Principal ID : {principal_id}", "WARN")
                    _log(f"  Storage Acct : {sa}", "WARN")
                    _log(f"  Go to: Azure Portal → Storage Account '{sa}' → Access Control (IAM) → Add role assignment", "WARN")
                    _log("  External Locations will be created with skip_validation=true and can be validated later.", "WARN")
    else:
        _log("No principalId found — role assignment skipped (connector may not have SystemAssigned identity)", "WARN")

    return connector_id


# ═══════════════════════════════════════════════════════════════════════════════
#  Step 3 — Register Storage Credential in Unity Catalog
# ═══════════════════════════════════════════════════════════════════════════════

def create_storage_credential(cfg, connector_id):
    """Register the Azure Access Connector as a Storage Credential in Unity Catalog."""
    _log("═══ Step 3: Register Storage Credential ═══")

    cred_name = cfg.get("storage_credential_name") or cfg["access_connector"]

    # Re-binding an existing credential to a different connector needs
    # Contributor on that connector (PERMISSION_DENIED otherwise). Without an
    # Azure credential the connector ID is only inferred, so keep whatever
    # binding the credential already has; in "create" mode the Service
    # Principal owns the connector it just made, so updating is safe.
    existing_connector_id = _lookup_credential_connector_id(cfg, cred_name)
    if existing_connector_id and (cfg.get("infra_mode") or "create").strip().lower() != "create":
        if connector_id and existing_connector_id != connector_id:
            _log(f"Storage credential '{cred_name}' is already bound to a different "
                 f"Access Connector — keeping the existing binding.", "INFO")
            _log(f"  existing : {existing_connector_id}", "INFO")
            _log(f"  configured: {connector_id}", "INFO")
        _log(f"Storage credential '{cred_name}' already exists — reusing it "
             f"(connector: {existing_connector_id.rsplit('/', 1)[-1]}).")
        return cred_name

    if not connector_id:
        _log("No connector_id provided — cannot create storage credential.", "ERROR")
        raise RuntimeError("Missing connector_id for storage credential")

    _log(f"Registering storage credential '{cred_name}' with connector: {connector_id}")

    # Always create with skip_validation=true because Azure RBAC propagation
    # takes 5-10 minutes after role assignment in Step 2.  Validation is
    # attempted separately with retries below.
    payload = {
        "name": cred_name,
        "azure_managed_identity": {
            "access_connector_id": connector_id,
        },
        "skip_validation": True,
        "comment": f"Auto-created from Access Connector {cfg['access_connector']}",
    }
    ok, body = _databricks_api(
        "POST",
        "/api/2.1/unity-catalog/storage-credentials",
        cfg,
        payload,
    )
    if ok:
        _log(f"Storage credential '{cred_name}' registered (validation deferred).")
    elif "already exists" in json.dumps(body).lower():
        _log(f"Storage credential '{cred_name}' already exists — updating connector ID.", "INFO")
        update_payload = {
            "azure_managed_identity": {
                "access_connector_id": connector_id,
            },
            "skip_validation": True,
            "force": True,
        }
        ok2, body2 = _databricks_api(
            "PATCH",
            f"/api/2.1/unity-catalog/storage-credentials/{cred_name}",
            cfg,
            update_payload,
        )
        if ok2:
            _log(f"Storage credential '{cred_name}' updated.")
        else:
            _log(f"Storage credential update note: {json.dumps(body2)[:200]}", "INFO")
    else:
        _log(f"Failed to create storage credential: {body}", "ERROR")
        raise RuntimeError(f"Storage credential creation failed: {body}")

    # ── Attempt validation with retries (RBAC may still be propagating) ──
    _log("Validating storage credential (RBAC propagation may take a few minutes)…")
    validated = False
    for attempt in range(1, 4):
        validate_payload = {
            "azure_managed_identity": {
                "access_connector_id": connector_id,
            },
            "skip_validation": False,
            "force": True,
        }
        vok, vbody = _databricks_api(
            "PATCH",
            f"/api/2.1/unity-catalog/storage-credentials/{cred_name}",
            cfg,
            validate_payload,
        )
        if vok:
            _log(f"Storage credential '{cred_name}' validated successfully (attempt {attempt}).")
            validated = True
            break
        else:
            err_text = json.dumps(vbody).lower() if vbody else ""
            if "cloud_storage" in err_text or "access" in err_text or "abfs" in err_text:
                _log(f"Validation attempt {attempt}/3 failed (RBAC still propagating) — waiting 15s…", "INFO")
                if attempt < 3:
                    time.sleep(15)
            else:
                _log(f"Validation attempt {attempt}/3 non-storage error: {json.dumps(vbody)[:200]}", "INFO")
                break

    if not validated:
        _log("Storage credential created but validation deferred — RBAC may still be propagating.", "WARN")
        _log("MetadataFlow will re-validate automatically when you create tables.", "WARN")

    return cred_name


# ═══════════════════════════════════════════════════════════════════════════════
#  Step 4 — Create External Locations  (Databricks Unity Catalog API)
# ═══════════════════════════════════════════════════════════════════════════════

def create_external_locations(cfg, credential_name):
    _log("═══ Step 4: External Locations ═══")

    expected_sa = cfg.get("storage_account", "").lower()

    # ── De-duplicate: drop external locations whose URL is identical to or
    #    overlaps with (is a parent/child of) an already-registered URL.
    #    Unity Catalog forbids overlapping external-location paths.
    registered_urls = []  # URLs successfully created so far

    def _overlaps(new_url, existing_url):
        """Return True if *new_url* overlaps *existing_url* (parent, child, or equal)."""
        a = new_url.rstrip("/") + "/"
        b = existing_url.rstrip("/") + "/"
        return a.startswith(b) or b.startswith(a)

    for loc_name, url in cfg["external_locations"].items():
        # Auto-fix: if URL references a different storage account, correct it
        if expected_sa and url and expected_sa not in url.lower():
            import re
            old_url = url
            url = re.sub(r'@[^.]+\.dfs\.core\.windows\.net', f'@{expected_sa}.dfs.core.windows.net', url)
            _log(f"Auto-corrected external location URL: {old_url} → {url}", "WARN")

        # Skip if this URL overlaps with one we already registered
        overlap = next((u for u in registered_urls if _overlaps(url, u)), None)
        if overlap:
            _log(f"Skipping external location '{loc_name}': URL '{url}' overlaps with already-registered '{overlap}'.", "INFO")
            continue

        _log(f"Creating external location '{loc_name}' → {url}")
        payload = {
            "name"            : loc_name,
            "url"             : url,
            "credential_name" : credential_name,
            "skip_validation" : True,       # always skip — validate separately
        }
        ok, body = _databricks_api(
            "POST",
            "/api/2.1/unity-catalog/external-locations",
            cfg,
            payload,
        )
        if ok:
            _log(f"External location '{loc_name}' created.")
            registered_urls.append(url)
        elif "already exists" in json.dumps(body).lower():
            _log(f"External location '{loc_name}' already exists — updating.", "INFO")
            upd = {"credential_name": credential_name, "skip_validation": True}
            _databricks_api("PATCH",
                            f"/api/2.1/unity-catalog/external-locations/{loc_name}",
                            cfg, upd)
            registered_urls.append(url)
        elif "location_overlap" in json.dumps(body).lower() or "overlaps" in json.dumps(body).lower():
            _log(f"External location '{loc_name}' overlaps with an existing location — skipping (covered by parent).", "INFO")
            continue
        else:
            _log(f"Failed to create external location '{loc_name}': {body}", "ERROR")
            continue

        # Attempt validation (non-blocking — don't fail the whole step)
        _log(f"Validating external location '{loc_name}'…")
        vok, vbody = _databricks_api(
            "POST",
            "/api/2.1/unity-catalog/validate-storage-credentials",
            cfg,
            {"storage_credential_name": credential_name, "url": url},
        )
        if vok:
            results = vbody.get("results", []) if isinstance(vbody, dict) else []
            failed = [r for r in results if r.get("result") == "FAIL"]
            if failed:
                _log(f"External location '{loc_name}' validation partial: {len(failed)} checks failed (RBAC may still be propagating).", "WARN")
            else:
                _log(f"External location '{loc_name}' validated OK.")
        else:
            _log(f"External location '{loc_name}' validation deferred (RBAC still propagating).", "INFO")


# ═══════════════════════════════════════════════════════════════════════════════
#  Step 5 — Create Catalogs  (Databricks Unity Catalog API)
# ═══════════════════════════════════════════════════════════════════════════════

def create_catalogs(cfg):
    _log("═══ Step 5: Catalogs ═══")

    expected_sa = cfg.get("storage_account", "").lower()
    ctr = cfg.get("container", "datalake")

    for catalog_name, cat_cfg in cfg["catalogs"].items():
        # Support both old format (string) and new format ({location, schemas})
        if isinstance(cat_cfg, str):
            storage_root = cat_cfg
            schemas = ["default"]
        else:
            storage_root = cat_cfg.get("location", "")
            schemas = cat_cfg.get("schemas", ["default"]) or ["default"]

        # Auto-fix: if the catalog URL references a different storage account
        # than what's configured, rebuild the URL with the correct account.
        if expected_sa and storage_root and expected_sa not in storage_root.lower():
            import re
            old_url = storage_root
            storage_root = re.sub(
                r'@[^.]+\.dfs\.core\.windows\.net',
                f'@{expected_sa}.dfs.core.windows.net',
                storage_root,
            )
            _log(f"Auto-corrected catalog URL: {old_url} → {storage_root}", "WARN")
            storage_root = cat_cfg.get("location", "")
            schemas = cat_cfg.get("schemas", ["default"]) or ["default"]

        _log(f"Creating catalog '{catalog_name}' → {storage_root}")

        # Use storage_root (top-level field) — required when metastore has no default root
        payload = {
            "name"         : catalog_name,
            "comment"      : f"Auto-created catalog for {catalog_name}",
            "storage_root" : storage_root,
        }
        ok, body = _databricks_api(
            "POST",
            "/api/2.1/unity-catalog/catalogs",
            cfg,
            payload,
        )
        if ok:
            _log(f"Catalog '{catalog_name}' created.")
        elif "already exists" in json.dumps(body).lower():
            # Verify that the existing catalog points to the correct storage
            _log(f"Catalog '{catalog_name}' already exists — verifying storage_root…", "INFO")
            gok, gdata = _databricks_api("GET", f"/api/2.1/unity-catalog/catalogs/{catalog_name}", cfg)
            if gok:
                existing_root = (gdata.get("storage_root") or "").lower()
                expected_sa = cfg.get("storage_account", "").lower()
                if expected_sa and existing_root and expected_sa not in existing_root:
                    _log(f"Catalog '{catalog_name}' points to wrong storage! Existing: {existing_root}", "WARN")
                    _log(f"Expected storage account: {expected_sa} — deleting and recreating catalog…", "WARN")
                    _databricks_api("DELETE", f"/api/2.1/unity-catalog/catalogs/{catalog_name}?force=true", cfg)
                    ok2, body2 = _databricks_api("POST", "/api/2.1/unity-catalog/catalogs", cfg, payload)
                    if ok2:
                        _log(f"Catalog '{catalog_name}' recreated with correct storage_root.")
                    else:
                        _log(f"Failed to recreate catalog '{catalog_name}': {body2}", "ERROR")
                        continue
                else:
                    _log(f"Catalog '{catalog_name}' storage_root OK.")
        else:
            _log(f"Failed to create catalog '{catalog_name}': {body}", "ERROR")
            continue

        # Create all specified schemas
        for schema_name in schemas:
            schema_payload = {
                "name"        : schema_name,
                "catalog_name": catalog_name,
                "comment"     : f"Schema {schema_name}",
            }
            ok2, body2 = _databricks_api(
                "POST",
                "/api/2.1/unity-catalog/schemas",
                cfg,
                schema_payload,
            )
            if ok2:
                _log(f"  Schema '{catalog_name}.{schema_name}' created.")
            elif "already exists" in json.dumps(body2).lower():
                _log(f"  Schema '{catalog_name}.{schema_name}' already exists — OK.", "INFO")
            else:
                _log(f"  Failed to create schema '{catalog_name}.{schema_name}': {body2}", "ERROR")

    # ── Create Reconciliation catalog if configured ──
    recon_cfg = cfg.get("reconciliation", {})
    if recon_cfg and recon_cfg.get("catalog"):
        r_cat = recon_cfg["catalog"]
        r_sch = recon_cfg.get("schema", "hr")
        r_loc = recon_cfg.get("location", "")
        # Auto-fix storage account mismatch
        if expected_sa and r_loc and expected_sa not in r_loc.lower():
            import re
            r_loc = re.sub(r'@[^.]+\.dfs\.core\.windows\.net', f'@{expected_sa}.dfs.core.windows.net', r_loc)
            _log(f"Auto-corrected reconciliation catalog URL to use '{expected_sa}'", "WARN")
        _log(f"Creating reconciliation catalog '{r_cat}' → {r_loc}")
        payload = {"name": r_cat, "comment": "Reconciliation results catalog"}
        if r_loc:
            payload["storage_root"] = r_loc
        ok, body = _databricks_api("POST", "/api/2.1/unity-catalog/catalogs", cfg, payload)
        if ok:
            _log(f"Catalog '{r_cat}' created.")
        elif "already exists" in json.dumps(body).lower():
            _log(f"Catalog '{r_cat}' already exists — OK.", "INFO")
        else:
            _log(f"Failed to create reconciliation catalog: {body}", "ERROR")
        # Create schema
        if ok or "already exists" in json.dumps(body).lower():
            ok2, body2 = _databricks_api("POST", "/api/2.1/unity-catalog/schemas", cfg,
                {"name": r_sch, "catalog_name": r_cat, "comment": f"Reconciliation schema {r_sch}"})
            if ok2:
                _log(f"  Schema '{r_cat}.{r_sch}' created.")
            elif "already exists" in json.dumps(body2).lower():
                _log(f"  Schema '{r_cat}.{r_sch}' already exists — OK.", "INFO")
            else:
                _log(f"  Failed to create schema '{r_cat}.{r_sch}': {body2}", "ERROR")

    # ── Create Logging catalog if configured ──
    log_cfg = cfg.get("logging", {})
    if log_cfg and log_cfg.get("catalog"):
        l_cat = log_cfg["catalog"]
        l_sch = log_cfg.get("schema", "hr")
        l_loc = log_cfg.get("location", "")
        # Auto-fix storage account mismatch
        if expected_sa and l_loc and expected_sa not in l_loc.lower():
            import re
            l_loc = re.sub(r'@[^.]+\.dfs\.core\.windows\.net', f'@{expected_sa}.dfs.core.windows.net', l_loc)
            _log(f"Auto-corrected logging catalog URL to use '{expected_sa}'", "WARN")
        _log(f"Creating logging catalog '{l_cat}' → {l_loc}")
        payload = {"name": l_cat, "comment": "Execution logging catalog"}
        if l_loc:
            payload["storage_root"] = l_loc
        ok, body = _databricks_api("POST", "/api/2.1/unity-catalog/catalogs", cfg, payload)
        if ok:
            _log(f"Catalog '{l_cat}' created.")
        elif "already exists" in json.dumps(body).lower():
            _log(f"Catalog '{l_cat}' already exists — OK.", "INFO")
        else:
            _log(f"Failed to create logging catalog: {body}", "ERROR")
        # Create schema
        if ok or "already exists" in json.dumps(body).lower():
            ok2, body2 = _databricks_api("POST", "/api/2.1/unity-catalog/schemas", cfg,
                {"name": l_sch, "catalog_name": l_cat, "comment": f"Logging schema {l_sch}"})
            if ok2:
                _log(f"  Schema '{l_cat}.{l_sch}' created.")
            elif "already exists" in json.dumps(body2).lower():
                _log(f"  Schema '{l_cat}.{l_sch}' already exists — OK.", "INFO")
            else:
                _log(f"  Failed to create schema '{l_cat}.{l_sch}': {body2}", "ERROR")


# ═══════════════════════════════════════════════════════════════════════════════
#  Step 5 — Create Volume  (Databricks Unity Catalog API)
# ═══════════════════════════════════════════════════════════════════════════════

def create_volume(cfg):
    _log("═══ Step 6: Volume ═══")

    vol_name  = cfg["volume_name"]
    catalog   = cfg["volume_catalog"]
    schema    = cfg["volume_schema"]
    vol_path  = cfg["volume_path"]

    # Auto-fix: if volume path references a different storage account, correct it
    expected_sa = cfg.get("storage_account", "").lower()
    if expected_sa and vol_path and expected_sa not in vol_path.lower():
        import re
        old_path = vol_path
        vol_path = re.sub(r'@[^.]+\.dfs\.core\.windows\.net', f'@{expected_sa}.dfs.core.windows.net', vol_path)
        _log(f"Auto-corrected volume path: {old_path} → {vol_path}", "WARN")

    # Auto-create the schema if it doesn't exist
    _log(f"Ensuring schema '{catalog}.{schema}' exists…")
    schema_payload = {
        "name"        : schema,
        "catalog_name": catalog,
        "comment"     : f"Schema {schema}",
    }
    sok, sbody = _databricks_api(
        "POST",
        "/api/2.1/unity-catalog/schemas",
        cfg,
        schema_payload,
    )
    if sok:
        _log(f"Schema '{catalog}.{schema}' created.")
    elif "already exists" in json.dumps(sbody).lower():
        _log(f"Schema '{catalog}.{schema}' already exists — OK.", "INFO")
    else:
        _log(f"Failed to create schema '{catalog}.{schema}': {sbody}", "ERROR")
        _log("Volume creation may fail if the schema doesn't exist.", "WARN")

    _log(f"Creating volume '{catalog}.{schema}.{vol_name}' → {vol_path}")
    payload = {
        "name"            : vol_name,
        "catalog_name"    : catalog,
        "schema_name"     : schema,
        "volume_type"     : "EXTERNAL",
        "storage_location": vol_path,
        "comment"         : "Landing zone external volume",
    }

    # Retry up to 3 times — RBAC role assignments can take a few minutes to
    # propagate, causing UC_CLOUD_STORAGE_ACCESS_FAILURE on the first attempt.
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        ok, body = _databricks_api(
            "POST",
            "/api/2.1/unity-catalog/volumes",
            cfg,
            payload,
        )
        if ok:
            _log(f"Volume '{vol_name}' created.")
            break
        elif "already exists" in json.dumps(body).lower():
            _log(f"Volume '{vol_name}' already exists — skipping.", "INFO")
            break
        elif ("cloud_storage" in json.dumps(body).lower()
              or "access" in json.dumps(body).lower()
              or "abfs" in json.dumps(body).lower()):
            if attempt < max_attempts:
                _log(f"Volume creation attempt {attempt}/{max_attempts} failed "
                     f"(RBAC still propagating) — waiting 20s…", "WARN")
                time.sleep(20)
            else:
                _log(f"Failed to create volume after {max_attempts} attempts "
                     f"(storage access denied — check RBAC): {body}", "ERROR")
        else:
            _log(f"Failed to create volume: {body}", "ERROR")
            break


# ═══════════════════════════════════════════════════════════════════════════════
#  Orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

def run_all(cfg):
    """Execute every infra step in order."""
    _log("╔════════════════════════════════════════════════════════════════╗")
    _log("║  Unity Catalog Infrastructure — Automated Setup              ║")
    _log("╚════════════════════════════════════════════════════════════════╝")
    _log(f"Subscription : {cfg['subscription_id']}")
    _log(f"Region       : {cfg['region']}")
    _log(f"Storage Acct : {cfg['storage_account']}")
    _log("")

    # Step 0 — Verify Azure credentials
    set_subscription(cfg)

    # Step 1 — Azure Storage
    create_storage(cfg)

    # Step 2 — Access Connector + RBAC
    connector_id = create_access_connector(cfg)

    # Step 3-6 — Databricks Unity Catalog (requires Databricks credentials)
    if cfg.get("databricks_host") and cfg.get("databricks_token"):
        # Step 3 — Register Storage Credential
        cred_name = create_storage_credential(cfg, connector_id)
        # Step 4 — External Locations
        create_external_locations(cfg, cred_name)
        # Step 5 — Catalogs
        create_catalogs(cfg)
        # Step 6 — Volume
        create_volume(cfg)
    else:
        _log("DATABRICKS_HOST / DATABRICKS_TOKEN not set — skipping Databricks API steps (3-6).", "WARN")
        _log("Set env-vars and re-run, or create these objects manually in the Databricks UI.")

    _log("")
    _log("╔════════════════════════════════════════════════════════════════╗")
    _log("║  Infrastructure setup complete                               ║")
    _log("╚════════════════════════════════════════════════════════════════╝")


# ═══════════════════════════════════════════════════════════════════════════════
#  API-Callable Orchestrator  (step-by-step results, no sys.exit)
# ═══════════════════════════════════════════════════════════════════════════════

def run_all_api(cfg):
    """Execute every infra step and return structured results with logs.

    Returns dict:
        success : bool
        steps   : [{step, name, status, message, logs}]
        summary : str
    """
    import io, contextlib

    steps = []

    def _run_step(step_num, name, fn, *args, **kwargs):
        """Run a single step, capturing stdout/stderr and exceptions."""
        buf = io.StringIO()
        entry = {"step": step_num, "name": name, "status": "running", "message": "", "logs": ""}
        try:
            with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
                result = fn(*args, **kwargs)
            entry["status"] = "success"
            entry["message"] = f"{name} completed successfully"
            entry["logs"] = buf.getvalue()
            return result
        except Exception as e:
            entry["status"] = "error"
            entry["message"] = str(e)[:500]
            entry["logs"] = buf.getvalue()
            return None
        finally:
            steps.append(entry)

    # Step 0 — Verify Azure credentials
    _run_step(0, "Set Azure Subscription", set_subscription, cfg)

    # Step 1 — Storage
    _run_step(1, "Create Storage Account + Container + Folders", create_storage, cfg)

    # Step 2 — Access Connector
    connector_id = _run_step(2, "Create Access Connector + RBAC", create_access_connector, cfg)

    # Steps 3-6 require Databricks credentials
    if cfg.get("databricks_host") and cfg.get("databricks_token"):
        # Steps 3 & 4 need connector_id from Step 2
        if not connector_id:
            skip_msg = ("Access Connector ID not available (Step 2 failed). "
                        "Cannot create Storage Credential or External Locations. "
                        "Fix Step 2 errors and retry.")
            for skip_step, skip_name in [
                (3, "Register Storage Credential"),
                (4, "Create External Locations"),
            ]:
                steps.append({"step": skip_step, "name": skip_name,
                              "status": "skipped", "message": skip_msg, "logs": ""})
        else:
            cred_name = _run_step(3, "Register Storage Credential in Unity Catalog",
                                  create_storage_credential, cfg, connector_id)
            if cred_name:
                _run_step(4, "Create External Locations",
                          create_external_locations, cfg, cred_name)
            else:
                steps.append({"step": 4, "name": "Create External Locations",
                              "status": "skipped",
                              "message": "Storage Credential not available (Step 3 failed)",
                              "logs": ""})

        # Steps 5 & 6 use Databricks API directly — no connector_id needed
        _run_step(5, "Create Unity Catalogs", create_catalogs, cfg)
        _run_step(6, "Create Volume", create_volume, cfg)
    else:
        steps.append({"step": 3, "name": "Databricks API Steps (3-6)",
                      "status": "skipped",
                      "message": "No databricks_host/databricks_token — skipped Storage Credential, External Locations, Catalogs & Volume",
                      "logs": ""})

    failed = [s for s in steps if s["status"] == "error"]
    return {
        "success": len(failed) == 0,
        "steps":   steps,
        "summary": f"{len(steps)} steps executed, {len(failed)} failed" if failed
                   else f"All {len(steps)} steps completed successfully",
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  Streaming Orchestrator  (yields JSON events per step — for SSE)
# ═══════════════════════════════════════════════════════════════════════════════

def run_all_streaming(cfg):
    """Generator: yields one JSON-serialisable dict per step as it completes.

    Each yielded dict has:
        event : "step" | "done"
        step  : int
        name  : str
        status: "running" | "success" | "error" | "skipped"
        message: str
        logs  : str
    Final yield has event="done" with summary info.
    """
    import io, contextlib

    all_steps = []

    def _do_step(step_num, name, fn, *args, **kwargs):
        buf = io.StringIO()
        entry = {"event": "step", "step": step_num, "name": name,
                 "status": "success", "message": "", "logs": ""}
        result = None
        try:
            with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
                result = fn(*args, **kwargs)
            entry["status"] = "success"
            entry["message"] = f"{name} completed successfully"
        except Exception as e:
            entry["status"] = "error"
            entry["message"] = str(e)[:500]
            result = None
        entry["logs"] = buf.getvalue()
        all_steps.append(entry)
        return entry, result

    infra_mode = (cfg.get("infra_mode") or "create").strip().lower()
    azure_ready = _azure_credentials_available(cfg)

    if infra_mode == "create" and not azure_ready:
        # "Create new infrastructure" was explicitly chosen, so silently
        # skipping the Azure steps would leave nothing to build against.
        err_msg = (
            "Cannot create Azure resources — no Azure credentials. Fill Tenant ID / "
            "Client ID / Client Secret under Settings → Azure Service Principal (it "
            "needs Contributor + User Access Administrator on the resource group), or "
            "switch the deploy mode to 'Use existing infrastructure'."
        )
        err_entry = {"event": "step", "step": 0, "name": "Set Azure Subscription",
                     "status": "error", "message": err_msg, "logs": ""}
        all_steps.append(err_entry)
        yield err_entry
        yield {"event": "done", "success": False, "steps": all_steps,
               "summary": "Azure Service Principal required to create new infrastructure"}
        return

    if not azure_ready:
        # A Databricks PAT authenticates to the Databricks API only — never to
        # Azure Resource Manager. Rather than failing Steps 0-2 three times with
        # the same opaque credential-chain dump, skip them with one actionable
        # message and let the PAT-only Unity Catalog steps proceed against
        # pre-existing Azure resources.
        skip_msg = (
            "Skipped — no Azure credentials. A Databricks PAT cannot create Azure "
            "resources. Either fill Tenant ID / Client ID / Client Secret under "
            "Settings → Azure Service Principal, or pre-create the Storage Account "
            "and Access Connector (e.g. via the src/notebooks/00_Setup_Infrastructure.py "
            "notebook or the Azure portal) — Steps 3-6 below will then use them."
        )
        for skip_step, skip_name in (
            (0, "Set Azure Subscription"),
            (1, "Create Storage Account + Container + Folders"),
            (2, "Create Access Connector + RBAC"),
        ):
            skip_entry = {"event": "step", "step": skip_step, "name": skip_name,
                          "status": "skipped", "message": skip_msg, "logs": ""}
            all_steps.append(skip_entry)
            yield skip_entry
        connector_id = None
    else:
        # Step 0 — Verify Azure credentials via SDK
        yield {"event": "step", "step": 0, "name": "Set Azure Subscription",
               "status": "running", "message": "Authenticating via Azure SDK…", "logs": ""}
        entry, _ = _do_step(0, "Set Azure Subscription", set_subscription, cfg)
        yield entry

        # Step 1 — Storage
        yield {"event": "step", "step": 1, "name": "Create Storage Account + Container + Folders",
               "status": "running", "message": "Creating storage…", "logs": ""}
        entry, _ = _do_step(1, "Create Storage Account + Container + Folders", create_storage, cfg)
        yield entry

        # Step 2 — Access Connector
        yield {"event": "step", "step": 2, "name": "Create Access Connector + RBAC",
               "status": "running", "message": "Creating access connector…", "logs": ""}
        entry, connector_id = _do_step(2, "Create Access Connector + RBAC", create_access_connector, cfg)
        yield entry

    # Steps 3-6 require Databricks credentials
    if cfg.get("databricks_host") and cfg.get("databricks_token"):
        # Steps 3 & 4 need the Access Connector's ARM resource ID. Normally
        # Step 2 returns it, but that step needs Azure (ARM) credentials which
        # a Databricks PAT cannot provide. When the connector already exists
        # (e.g. created by hand in the Azure portal), its resource ID is fully
        # determined by subscription/resource group/name — so derive it and let
        # the PAT-only Unity Catalog steps continue.
        if not connector_id:
            cred_name = cfg.get("storage_credential_name") or cfg.get("access_connector")
            connector_id = _lookup_credential_connector_id(cfg, cred_name)
            source = "existing storage credential"
            if not connector_id:
                connector_id = _derive_connector_id(cfg)
                source = "config (Subscription / Resource Group / Access Connector name)"
            if connector_id:
                derived_entry = {
                    "event": "step", "step": 2,
                    "name": "Create Access Connector + RBAC",
                    "status": "skipped",
                    "message": (f"Azure step skipped — Access Connector resolved from {source}: "
                                f"{connector_id.rsplit('/', 1)[-1]}"),
                    "logs": f"[resolved from {source}] Access Connector ID: {connector_id}\n",
                }
                all_steps.append(derived_entry)
                yield derived_entry

        if not connector_id:
            skip_msg = ("Access Connector ID not available (Step 2 failed). "
                        "Cannot create Storage Credential or External Locations.")
            for skip_step, skip_name in [
                (3, "Register Storage Credential"), (4, "Create External Locations"),
            ]:
                skip_entry = {"event": "step", "step": skip_step, "name": skip_name,
                              "status": "skipped", "message": skip_msg, "logs": ""}
                all_steps.append(skip_entry)
                yield skip_entry
        else:
            # Step 3
            yield {"event": "step", "step": 3, "name": "Register Storage Credential",
                   "status": "running", "message": "Registering credential…", "logs": ""}
            entry, cred_name = _do_step(3, "Register Storage Credential",
                                        create_storage_credential, cfg, connector_id)
            yield entry

            # Step 4
            if cred_name:
                yield {"event": "step", "step": 4, "name": "Create External Locations",
                       "status": "running", "message": "Creating external locations…", "logs": ""}
                entry, _ = _do_step(4, "Create External Locations",
                                    create_external_locations, cfg, cred_name)
                yield entry
            else:
                skip_entry = {"event": "step", "step": 4, "name": "Create External Locations",
                              "status": "skipped",
                              "message": "Storage Credential not available (Step 3 failed)", "logs": ""}
                all_steps.append(skip_entry)
                yield skip_entry

        # Steps 5 & 6 use Databricks REST API directly — no connector_id needed
        yield {"event": "step", "step": 5, "name": "Create Unity Catalogs",
               "status": "running", "message": "Creating catalogs…", "logs": ""}
        entry, _ = _do_step(5, "Create Unity Catalogs", create_catalogs, cfg)
        yield entry

        yield {"event": "step", "step": 6, "name": "Create Volume",
               "status": "running", "message": "Creating volume…", "logs": ""}
        entry, _ = _do_step(6, "Create Volume", create_volume, cfg)
        yield entry
    else:
        skip_entry = {"event": "step", "step": 3, "name": "Databricks API Steps (3-6)",
                      "status": "skipped",
                      "message": "No databricks_host/databricks_token — skipped", "logs": ""}
        all_steps.append(skip_entry)
        yield skip_entry

    failed = [s for s in all_steps if s["status"] == "error"]
    yield {
        "event": "done",
        "success": len(failed) == 0,
        "steps": all_steps,
        "summary": f"{len(all_steps)} steps executed, {len(failed)} failed" if failed
                   else f"All {len(all_steps)} steps completed successfully",
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  CLI Entry Point
# ═══════════════════════════════════════════════════════════════════════════════

def _prompt_credentials(cfg):
    """Prompt for Databricks host/token if they are not already set."""
    if not cfg["databricks_host"]:
        cfg["databricks_host"] = input("Enter Databricks workspace URL (e.g. https://adb-xxx.azuredatabricks.net): ").strip()
    if not cfg["databricks_token"]:
        cfg["databricks_token"] = input("Enter Databricks PAT: ").strip()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unity Catalog Infra Setup")
    parser.add_argument("--auto", action="store_true", help="Skip prompts — use env-vars only")
    args = parser.parse_args()

    cfg = dict(CONFIG)

    if not args.auto:
        _prompt_credentials(cfg)

    run_all(cfg)
