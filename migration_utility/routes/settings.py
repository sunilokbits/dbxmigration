"""Settings API — Catalog/Schema listing and access validation for ExistingSetting."""
from functools import wraps

from flask import Blueprint, jsonify, request, session
from .auth import login_required
from config_cache import get_config, get_databricks_token
from log_config import get_logger

logger = get_logger(__name__)
settings_bp = Blueprint("settings", __name__, url_prefix="/api/v1")


def _admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get("role") != "Admin":
            return jsonify({"success": False, "error": "Admin access required"}), 403
        return f(*args, **kwargs)
    return decorated


# The only secret keys this app ever reads (see secrets_helper.py:
# _SOURCE_PASSWORD_SECRET_KEYS, get_databricks_token, get_devops_token).
# A whitelist so this endpoint can never be used to write an arbitrary
# key into the secret scope.
ALLOWED_SECRET_KEYS = {
    "databricks-token": "Databricks PAT used for API/SQL calls",
    "devops-pat": "Azure DevOps PAT",
    "source-sql-password": "SQL Server source password",
    "source-azuresql-password": "Azure SQL source password",
    "source-snowflake-password": "Snowflake source password",
    "source-bigquery-password": "BigQuery source password",
    "source-redshift-password": "Redshift source password",
    "source-synapse-password": "Synapse source password",
    "source-sharepoint-password": "SharePoint source password",
    "source-api-password": "Generic API source password",
}


@settings_bp.route("/deploy-config", methods=["GET"])
@login_required
def get_deploy_config():
    """Return the config for Settings page pre-fill, with secrets masked.

    The returned password/token fields are never the real value -- only
    MASKED_VALUE when one is actually set. This matters because the
    frontend pre-fills form fields directly from this response
    (G('srcPass').value = src.password), and routes/source.py trusts
    whatever comes back in that field over the real stored secret unless
    it looks masked or empty. Returning the raw value here previously
    meant re-submitting an unchanged field would silently re-use a stale
    plaintext value instead of falling back to the governed secret.
    """
    import copy
    from secrets_helper import MASKED_VALUE

    try:
        cfg = copy.deepcopy(get_config())
        if cfg.get("source", {}).get("password"):
            cfg["source"]["password"] = MASKED_VALUE
        if cfg.get("databricks_token"):
            cfg["databricks_token"] = MASKED_VALUE
        if cfg.get("devops_pat"):
            cfg["devops_pat"] = MASKED_VALUE
        return jsonify({"success": True, "config": cfg})
    except Exception as e:
        logger.error("Failed to load config: %s", e)
        return jsonify({"success": False, "error": str(e)})


@settings_bp.route("/deploy-config", methods=["POST"])
@login_required
def save_deploy_config():
    """Save config from Settings page.

    The frontend round-trips whatever get_deploy_config() returned, so any
    password/token field the user didn't retype still contains MASKED_VALUE.
    Persisting that literal string would clobber the real value already in
    Delta -- restore the previous cached value for any field that still
    looks masked instead of overwriting it.

    Conversely, when a field DOES contain a real, freshly-typed value, write
    it to the governed Databricks secret scope (set_source_password /
    set_databricks_token / set_devops_token were previously defined but never
    called from anywhere -- meaning nothing typed into this page ever reached
    the secret store, only the plaintext Delta config). Once stored as a
    secret, drop the plaintext copy from what gets saved to Delta so it isn't
    kept in two places.
    """
    try:
        from config_cache import save_config, get_config
        from secrets_helper import is_masked, set_source_password, set_databricks_token, set_devops_token
        data = request.get_json(force=True)

        existing = get_config()
        existing_source = existing.get("source", {}) if isinstance(existing.get("source"), dict) else {}

        if isinstance(data.get("source"), dict):
            pw = data["source"].get("password", "")
            if is_masked(pw):
                data["source"]["password"] = existing_source.get("password", "")
            elif pw:
                source_type = data["source"].get("source_type", "sqlserver")
                if set_source_password(pw, source_type=source_type):
                    data["source"]["password"] = ""

        tok = data.get("databricks_token", "")
        if is_masked(tok):
            data["databricks_token"] = existing.get("databricks_token", "")
        elif tok:
            if set_databricks_token(tok):
                data["databricks_token"] = ""

        pat = data.get("devops_pat", "")
        if is_masked(pat):
            data["devops_pat"] = existing.get("devops_pat", "")
        elif pat:
            if set_devops_token(pat):
                data["devops_pat"] = ""

        save_config(data)
        return jsonify({"success": True})
    except Exception as e:
        logger.error("Failed to save config: %s", e)
        return jsonify({"success": False, "error": str(e)})





@settings_bp.route("/test-databricks", methods=["POST"])
@login_required
def test_databricks_conn():
    """Test Databricks workspace connectivity."""
    try:
        import requests as req
        body = request.get_json(force=True)
        host = (body.get("databricks_host") or "").rstrip("/")
        token = body.get("databricks_token") or ""
        # If token is masked, use the real one from secrets
        from secrets_helper import is_masked
        if not token or is_masked(token):
            token = get_databricks_token()
        if not host or not token:
            return jsonify({"success": False, "error": "Host and token required"})
        # Test by calling clusters/list (lightweight)
        resp = req.get(
            f"{host}/api/2.0/clusters/list",
            headers={"Authorization": f"Bearer {token}"},
            params={"page_size": 1},
            timeout=15
        )
        if resp.status_code == 200:
            data = resp.json()
            cluster_count = len(data.get("clusters", []))
            return jsonify({"success": True, "message": f"Connected. {cluster_count} cluster(s) found.",
                           "host": host, "clusters": cluster_count})
        elif resp.status_code == 401:
            return jsonify({"success": False, "error": "Authentication failed (401). Check PAT token."})
        elif resp.status_code == 403:
            return jsonify({"success": False, "error": "Forbidden (403). Token may lack permissions."})
        else:
            return jsonify({"success": False, "error": f"HTTP {resp.status_code}: {resp.text[:200]}"})
    except req.exceptions.Timeout:
        return jsonify({"success": False, "error": "Connection timed out. Check host URL."})
    except req.exceptions.ConnectionError as e:
        return jsonify({"success": False, "error": f"Connection error: {str(e)[:100]}"})
    except Exception as e:
        logger.error("Test Databricks connection failed: %s", e)
        return jsonify({"success": False, "error": str(e)[:200]})


@settings_bp.route("/test-storage-credential", methods=["POST"])
@login_required
def test_storage_credential():
    """Look up a Unity Catalog storage credential and validate it can reach ADLS."""
    import json as _json
    try:
        import requests as req
        body = request.get_json(force=True)
        host = (body.get("databricks_host") or "").rstrip("/")
        token = body.get("databricks_token") or ""
        cred_name = (body.get("storage_credential_name") or "").strip()
        test_url = (body.get("test_url") or "").strip()

        from secrets_helper import is_masked
        if not token or is_masked(token):
            token = get_databricks_token()
        cfg = get_config()
        if not host:
            host = (cfg.get("databricks_host") or "").rstrip("/")
        if not host or not token:
            return jsonify({"success": False, "error": "Databricks host and token are required"})
        if not cred_name:
            return jsonify({"success": False, "error": "Storage Credential Name (or Access Connector Name) is required"})

        headers = {"Authorization": f"Bearer {token}"}

        # 1) Does the credential exist?
        get_resp = req.get(
            f"{host}/api/2.1/unity-catalog/storage-credentials/{cred_name}",
            headers=headers, timeout=20,
        )
        if get_resp.status_code == 404:
            return jsonify({"success": False, "error": f"Storage credential '{cred_name}' not found in Unity Catalog."})
        if get_resp.status_code != 200:
            return jsonify({"success": False, "error": f"Failed to look up credential (HTTP {get_resp.status_code}): {get_resp.text[:200]}"})
        cred = get_resp.json()
        credential_id = cred.get("id", "")
        owner = cred.get("owner", "")
        access_connector_id = (cred.get("azure_managed_identity") or {}).get("access_connector_id", "")

        # 2) Does an external location already cover the test URL?
        external_location = ""
        if test_url:
            try:
                loc_resp = req.get(f"{host}/api/2.1/unity-catalog/external-locations", headers=headers, timeout=20)
                if loc_resp.status_code == 200:
                    for loc in loc_resp.json().get("external_locations", []):
                        loc_url = (loc.get("url") or "").rstrip("/")
                        if loc_url and test_url.rstrip("/").startswith(loc_url):
                            external_location = loc.get("name", "")
                            break
            except Exception:
                pass  # non-fatal — external location coverage is informational only

        # 3) Validate the credential can actually reach storage (same technique
        #    AutoInfraCreation.create_storage_credential uses: a force PATCH with
        #    skip_validation=False triggers Databricks to live-test the connector).
        if not access_connector_id:
            return jsonify({
                "success": False,
                "error": "Credential has no Azure managed identity / access connector attached.",
                "credential_id": credential_id, "owner": owner,
            })
        validate_resp = req.patch(
            f"{host}/api/2.1/unity-catalog/storage-credentials/{cred_name}",
            headers=headers, timeout=45,
            json={
                "azure_managed_identity": {"access_connector_id": access_connector_id},
                "skip_validation": False,
                "force": True,
            },
        )
        if validate_resp.status_code == 200:
            return jsonify({
                "success": True,
                "message": "Credential can access storage",
                "credential_name": cred_name, "credential_id": credential_id, "owner": owner,
                "access_connector_id": access_connector_id, "external_location": external_location,
                "validation": {"passed": True, "overlap": bool(external_location), "url": test_url},
            })
        try:
            verr = validate_resp.json()
        except Exception:
            verr = {"message": validate_resp.text[:300]}
        err_msg = verr.get("message") or _json.dumps(verr)[:300]
        return jsonify({
            "success": False,
            "error": "Storage access validation failed — RBAC may still be propagating (can take 5-10 min after role assignment).",
            "detail": err_msg[:300],
            "credential_id": credential_id, "owner": owner, "access_connector_id": access_connector_id,
        })
    except req.exceptions.Timeout:
        return jsonify({"success": False, "error": "Connection timed out. Check host URL."})
    except req.exceptions.ConnectionError as e:
        return jsonify({"success": False, "error": f"Connection error: {str(e)[:100]}"})
    except Exception as e:
        logger.error("Test storage credential failed: %s", e, exc_info=True)
        return jsonify({"success": False, "error": str(e)[:200]})


@settings_bp.route("/apply-rbac", methods=["POST"])
@login_required
def apply_rbac():
    """Assign an Azure RBAC role to the configured Azure Service Principal on the storage account.

    NOTE: this app runs as a Databricks App (see app.yml), not an Azure App
    Service — there is no separate "App Service managed identity" resource to
    target. The only real, resolvable identity available here is the Azure
    Service Principal already configured above (Tenant ID / Client ID /
    Client Secret), so that is what this endpoint grants the role to.
    """
    role_name = None
    client_id = None
    try:
        body = request.get_json(force=True)
        role_name = (body.get("role_name") or "Storage Blob Data Owner").strip()
        cfg = get_config()
        sub = cfg.get("subscription_id", "")
        rg = cfg.get("resource_group", "")
        sa = cfg.get("storage_account", "")
        tenant_id = cfg.get("azure_tenant_id", "")
        client_id = cfg.get("azure_client_id", "")
        client_secret = cfg.get("azure_client_secret", "")

        missing = [label for label, val in [
            ("Subscription ID", sub), ("Resource Group", rg), ("Storage Account Name", sa),
            ("Tenant ID", tenant_id), ("Client ID", client_id), ("Client Secret", client_secret),
        ] if not val]
        if missing:
            return jsonify({"success": False, "error": "Missing required fields: " + ", ".join(missing)})

        import requests as req
        from azure.identity import ClientSecretCredential
        credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

        # 1) Resolve the Service Principal's AAD object ID via Microsoft Graph —
        #    Azure RBAC role assignments need the object ID, not the app/client ID.
        graph_token = credential.get_token("https://graph.microsoft.com/.default").token
        sp_resp = req.get(
            "https://graph.microsoft.com/v1.0/servicePrincipals",
            headers={"Authorization": f"Bearer {graph_token}"},
            params={"$filter": f"appId eq '{client_id}'"},
            timeout=20,
        )
        if sp_resp.status_code != 200:
            return jsonify({
                "success": False,
                "error": f"Could not look up the Service Principal in Microsoft Graph (HTTP {sp_resp.status_code}). "
                         "It may lack Graph read permissions.",
                "detail": sp_resp.text[:300],
                "cli_command": f"az ad sp show --id {client_id} --query id -o tsv",
            })
        sp_values = sp_resp.json().get("value", [])
        if not sp_values:
            return jsonify({"success": False, "error": f"No Service Principal found for Client ID '{client_id}'."})
        principal_id = sp_values[0]["id"]

        # 2) Resolve the built-in role definition for the requested role name.
        from azure.mgmt.authorization import AuthorizationManagementClient
        import uuid
        storage_scope = (
            f"/subscriptions/{sub}/resourceGroups/{rg}"
            f"/providers/Microsoft.Storage/storageAccounts/{sa}"
        )
        auth_client = AuthorizationManagementClient(credential, sub)
        role_defs = list(auth_client.role_definitions.list(storage_scope, filter=f"roleName eq '{role_name}'"))
        if not role_defs:
            return jsonify({"success": False, "error": f"Role definition '{role_name}' not found."})
        role_def_id = role_defs[0].id

        # 3) Create the role assignment (idempotent — treat "already exists" as success).
        assignment_name = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{principal_id}:{role_def_id}:{storage_scope}"))
        try:
            auth_client.role_assignments.create(
                storage_scope, assignment_name,
                {"role_definition_id": role_def_id, "principal_id": principal_id, "principal_type": "ServicePrincipal"},
            )
        except Exception as e:
            if "alreadyexists" not in str(e).lower().replace(" ", ""):
                raise

        return jsonify({
            "success": True,
            "message": f'"{role_name}" assigned to Service Principal ({client_id}) on storage account "{sa}".',
        })
    except Exception as e:
        logger.error("Apply RBAC failed: %s", e, exc_info=True)
        err = str(e)
        resp = {"success": False, "error": err[:300]}
        if "authorizationfailed" in err.lower().replace(" ", "") or "does not have authorization" in err.lower():
            resp["cli_command"] = (
                f"az role assignment create --assignee {client_id or '<client-id>'} "
                f"--role \"{role_name or '<role>'}\" --scope <storage-account-resource-id>"
            )
        return jsonify(resp)


@settings_bp.route("/settings/catalogs", methods=["GET"])
@login_required
def list_catalogs():
    """List catalogs accessible to the current user/SP via SQL."""
    cfg = get_config()
    dbx_host = cfg.get("databricks_host", "").rstrip("/")
    dbx_token = get_databricks_token()
    if not dbx_host or not dbx_token:
        return jsonify({"success": False, "catalogs": [], "error": "Databricks not configured."})
    try:
        from unity_catalog_executor import UnityCatalogExecutor
        meta_cat = cfg.get("metadata_catalog", "admin_source") or "admin_source"
        meta_sch = cfg.get("metadata_schema", "configtables") or "configtables"
        uc = UnityCatalogExecutor(dbx_host, dbx_token, meta_cat, meta_sch)
        wh_resp = uc.list_warehouses()
        warehouses = wh_resp.get("warehouses", [])
        wh_id = next((w["id"] for w in warehouses if w.get("state") == "RUNNING"), None)
        if not wh_id and warehouses:
            wh_id = warehouses[0].get("id")
        if not wh_id:
            logger.error("No SQL Warehouse found for catalog listing")
            return jsonify({"success": False, "catalogs": [], "error": "No SQL Warehouse available."})
        logger.info("Listing catalogs via warehouse %s", wh_id)
        result = uc._execute_statement("SHOW CATALOGS", wh_id, wait_timeout="30s")
        if result.get("error"):
            logger.error("SHOW CATALOGS failed: %s", result["error"])
            return jsonify({"success": False, "catalogs": [], "error": result["error"]})
        status = result.get("status", {}).get("state", "")
        if status not in ("SUCCEEDED", "CLOSED", ""):
            logger.error("SHOW CATALOGS state: %s", status)
            return jsonify({"success": False, "catalogs": [], "error": f"Query state: {status}"})
        data_array = result.get("result", {}).get("data_array", [])
        catalogs = [row[0] for row in data_array if row and row[0]]
        catalogs = [c for c in catalogs if c not in ("system", "__databricks_internal")]
        logger.info("Catalogs listed: %d found", len(catalogs))
        return jsonify({"success": True, "catalogs": sorted(catalogs)})
    except Exception as e:
        logger.error("Failed to list catalogs: %s", e, exc_info=True)
        return jsonify({"success": False, "catalogs": [], "error": str(e)[:200]})


@settings_bp.route("/settings/schemas", methods=["GET"])
@login_required
def list_schemas():
    """List schemas in a catalog via SQL."""
    catalog = request.args.get("catalog", "")
    if not catalog:
        return jsonify({"success": False, "schemas": [], "error": "catalog parameter required."})
    cfg = get_config()
    dbx_host = cfg.get("databricks_host", "").rstrip("/")
    dbx_token = get_databricks_token()
    if not dbx_host or not dbx_token:
        return jsonify({"success": False, "schemas": [], "error": "Databricks not configured."})
    try:
        from unity_catalog_executor import UnityCatalogExecutor
        uc = UnityCatalogExecutor(dbx_host, dbx_token, catalog, "default")
        wh_resp = uc.list_warehouses()
        warehouses = wh_resp.get("warehouses", [])
        wh_id = next((w["id"] for w in warehouses if w.get("state") == "RUNNING"), None)
        if not wh_id and warehouses:
            wh_id = warehouses[0].get("id")
        if not wh_id:
            return jsonify({"success": False, "schemas": [], "error": "No SQL Warehouse available."})
        result = uc._execute_statement(f"SHOW SCHEMAS IN `{catalog}`", wh_id, wait_timeout="30s")
        if result.get("error"):
            return jsonify({"success": False, "schemas": [], "error": result["error"]})
        data_array = result.get("result", {}).get("data_array", [])
        schemas = [row[0] for row in data_array if row and row[0]]
        schemas = [s for s in schemas if s not in ("information_schema",)]
        return jsonify({"success": True, "schemas": sorted(schemas)})
    except Exception as e:
        logger.error("Failed to list schemas for %s: %s", catalog, e)
        return jsonify({"success": False, "schemas": [], "error": str(e)})


@settings_bp.route("/settings/validate-access", methods=["POST"])
@login_required
def validate_access():
    """Validate access for a medallion layer configuration."""
    body = request.get_json(force=True)
    layer = body.get("layer", "")
    catalog = body.get("catalog", "")
    schema = body.get("schema", "")
    storage_account = body.get("storage_account", "")
    container = body.get("container", "")
    base_path = body.get("base_path", "")

    if not catalog or not schema or not storage_account or not container:
        return jsonify({"success": True, "valid": False, "error": "Missing required fields"})

    cfg = get_config()
    dbx_host = cfg.get("databricks_host", "").rstrip("/")
    dbx_token = get_databricks_token()
    if not dbx_host or not dbx_token:
        return jsonify({"success": True, "valid": False, "error": "Databricks not configured."})

    errors = []
    import requests as req

    # 1. Validate catalog access
    try:
        resp = req.get(
            f"{dbx_host}/api/2.1/unity-catalog/catalogs/{catalog}",
            headers={"Authorization": f"Bearer {dbx_token}"},
            timeout=15
        )
        if resp.status_code != 200:
            errors.append(f"Catalog '{catalog}' not accessible (HTTP {resp.status_code})")
    except Exception as e:
        errors.append(f"Catalog check failed: {e}")

    # 2. Validate schema access
    try:
        resp = req.get(
            f"{dbx_host}/api/2.1/unity-catalog/schemas/{catalog}.{schema}",
            headers={"Authorization": f"Bearer {dbx_token}"},
            timeout=15
        )
        if resp.status_code != 200:
            errors.append(f"Schema '{catalog}.{schema}' not accessible (HTTP {resp.status_code})")
    except Exception as e:
        errors.append(f"Schema check failed: {e}")

    # 3. Validate storage path (construct ABFSS path and check via DBFS/Files API)
    abfss_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{base_path}" if base_path else f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    try:
        # Use SQL Statement API to test path access
        from unity_catalog_executor import UnityCatalogExecutor
        uc = UnityCatalogExecutor(dbx_host, dbx_token, catalog, schema)
        wh_resp = uc.list_warehouses()
        warehouses = wh_resp.get("warehouses", [])
        wh_id = next((w["id"] for w in warehouses if w.get("state") == "RUNNING"), None)
        if not wh_id and warehouses:
            wh_id = warehouses[0].get("id")
        if wh_id:
            test_sql = f"LIST \'{abfss_path}\'"
            result = uc._execute_statement(test_sql, wh_id, wait_timeout="15s")
            status_state = result.get("status", {}).get("state", "")
            if status_state in ("FAILED",):
                err_msg = result.get("status", {}).get("error", {}).get("message", "")
                if "ACCESS_DENIED" in err_msg.upper() or "FORBIDDEN" in err_msg.upper():
                    errors.append(f"Storage path access denied: {abfss_path}")
                # If it's just "path not found" that's OK — it will be created
        else:
            # No warehouse available — skip storage validation, just validate catalog/schema
            pass
    except Exception as e:
        # Storage validation is best-effort; catalog/schema validation is critical
        logger.warning("Storage path validation skipped: %s", e)

    if errors:
        return jsonify({"success": True, "valid": False, "error": "; ".join(errors)})

    return jsonify({"success": True, "valid": True, "layer": layer, "message": f"All checks passed for {layer}"})


@settings_bp.route("/settings/secrets", methods=["GET"])
@login_required
@_admin_required
def list_secret_status():
    """Return whether each known secret key is configured — never the value itself."""
    from secrets_helper import _get_ws_client, is_masked, _SECRET_SCOPE

    ws = _get_ws_client()
    existing = {}
    if ws is not None:
        try:
            for meta in ws.secrets.list_secrets(scope=_SECRET_SCOPE):
                existing[meta.key] = meta.last_updated_timestamp
        except Exception as e:
            logger.warning("Could not list secrets in scope %s: %s", _SECRET_SCOPE, e)

    from secrets_helper import get_secret
    keys = []
    for key, description in ALLOWED_SECRET_KEYS.items():
        current = get_secret(key) if key in existing else ""
        keys.append({
            "key": key,
            "description": description,
            "configured": key in existing and current and not is_masked(current),
            "last_updated": existing.get(key),
        })
    return jsonify({"success": True, "scope": _SECRET_SCOPE, "keys": keys})


@settings_bp.route("/settings/secrets", methods=["POST"])
@login_required
@_admin_required
def update_secret():
    """Update one secret's value. Admin-only, audited, whitelisted keys only."""
    from secrets_helper import set_secret, is_masked
    from audit import log_action

    data = request.get_json(force=True) or {}
    key = data.get("key", "")
    value = data.get("value", "")

    if key not in ALLOWED_SECRET_KEYS:
        return jsonify({"success": False, "error": f"Unknown secret key: {key}"}), 400
    if not value or is_masked(value):
        return jsonify({"success": False, "error": "A real, non-empty value is required"}), 400

    ok = set_secret(key, value)
    # Never log the secret value itself — only that this key was changed and by whom.
    log_action("update_secret", resource_type="secret", resource_id=key,
               details={"description": ALLOWED_SECRET_KEYS[key]})

    if not ok:
        return jsonify({"success": False, "error": f"Failed to store secret '{key}'"}), 500
    return jsonify({"success": True, "message": f"'{key}' updated"})
