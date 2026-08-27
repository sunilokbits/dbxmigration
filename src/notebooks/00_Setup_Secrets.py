# Databricks notebook source
# MAGIC %md
# MAGIC # Migration Studio — Secret & Scope Setup
# MAGIC
# MAGIC Run this notebook **once** to create the Databricks secret scope and populate
# MAGIC all secrets required by the **dbxmigrator** app.
# MAGIC
# MAGIC **Instructions:**
# MAGIC 1. Fill in the widgets at the top (they appear after running Cell 1)
# MAGIC 2. Run **All Cells** — the notebook is idempotent (safe to re-run)
# MAGIC 3. After completion, the app picks up secrets automatically — no redeploy needed

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration Widgets

# COMMAND ----------

dbutils.widgets.text("scope_name", "migration-studio", "Secret Scope Name")
dbutils.widgets.text("sql_password", "", "Source SQL Server Password")
dbutils.widgets.text("databricks_token", "", "Databricks PAT Token")
dbutils.widgets.text("devops_pat", "", "Azure DevOps PAT Token")
dbutils.widgets.text("flask_secret", "", "Flask Secret Key (leave blank to auto-generate)")
dbutils.widgets.text("app_service_principal", "app-3h9355 dbxmigrator", "App Service Principal Name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Widget Values

# COMMAND ----------

scope_name = dbutils.widgets.get("scope_name")
sql_password = dbutils.widgets.get("sql_password")
databricks_token = dbutils.widgets.get("databricks_token")
devops_pat = dbutils.widgets.get("devops_pat")
flask_secret = dbutils.widgets.get("flask_secret")
app_sp_name = dbutils.widgets.get("app_service_principal")

if not flask_secret:
    import uuid
    flask_secret = uuid.uuid4().hex + uuid.uuid4().hex
    print(f"Auto-generated Flask secret key ({len(flask_secret)} chars)")

print(f"Scope        : {scope_name}")
print(f"SQL Password : {'*' * len(sql_password) if sql_password else '(empty — will skip)'}")
print(f"DBX Token    : {'*' * len(databricks_token) if databricks_token else '(empty — will skip)'}")
print(f"DevOps PAT   : {'*' * len(devops_pat) if devops_pat else '(empty — will skip)'}")
print(f"Flask Secret : {'*' * 8} ({len(flask_secret)} chars)")
print(f"App SP       : {app_sp_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Secret Scope

# COMMAND ----------

try:
    dbutils.secrets.createScope(scope_name)
    print(f"✅ Created secret scope: {scope_name}")
except Exception as e:
    if "SCOPE_ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
        print(f"✅ Secret scope already exists: {scope_name}")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Store Secrets

# COMMAND ----------

secrets_to_store = {
    "source-sql-password": sql_password,
    "databricks-token": databricks_token,
    "devops-pat": devops_pat,
    "flask-secret-key": flask_secret,
}

stored = 0
skipped = 0
for key, value in secrets_to_store.items():
    if not value:
        print(f"⏭️  Skipped: {key} (empty value)")
        skipped += 1
        continue
    try:
        dbutils.secrets.put(scope_name, key, value)
        print(f"✅ Stored: {key}")
        stored += 1
    except Exception as e:
        print(f"❌ Failed to store {key}: {e}")

print(f"\nDone: {stored} stored, {skipped} skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Grant App Service Principal Access

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

try:
    sp_list = list(w.service_principals.list(filter=f'displayName eq "{app_sp_name}"'))
    if sp_list:
        sp = sp_list[0]
        print(f"✅ Found service principal: {sp.display_name} (ID: {sp.id})")

        try:
            w.secrets.put_acl(scope=scope_name, principal=sp.application_id, permission="MANAGE")
            print(f"✅ Granted MANAGE permission on '{scope_name}' to {sp.display_name}")
        except Exception as e:
            if "MANAGE" in str(e) or "already" in str(e).lower():
                print(f"✅ ACL already set for {sp.display_name}")
            else:
                print(f"⚠️  Could not set ACL (non-fatal): {e}")
                print("   The app may still work if scope was created with 'All Users' manage principal")
    else:
        print(f"⚠️  Service principal '{app_sp_name}' not found")
        print("   The app should still work if scope was created with 'All Users' manage principal")
except Exception as e:
    print(f"⚠️  Could not configure ACL (non-fatal): {e}")
    print("   The app should still work if scope uses the default manage principal")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify Secrets

# COMMAND ----------

print(f"Secrets in scope '{scope_name}':")
print("-" * 40)
for secret in dbutils.secrets.list(scope_name):
    print(f"  🔑 {secret.key}")

print()

for key in ["source-sql-password", "databricks-token", "devops-pat", "flask-secret-key"]:
    try:
        val = dbutils.secrets.get(scope_name, key)
        status = "✅ readable" if val else "⚠️ empty"
    except Exception:
        status = "❌ not found"
    print(f"  {key}: {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup Complete
# MAGIC
# MAGIC The **dbxmigrator** app will automatically pick up these secrets on the next request.
# MAGIC
# MAGIC **No redeployment needed** — just refresh the app at:
# MAGIC `https://dbxmigrator-7405613585789005.5.azure.databricksapps.com`
# MAGIC
# MAGIC ### To update a secret later:
# MAGIC ```python
# MAGIC dbutils.secrets.put("migration-studio", "source-sql-password", "new-password-here")
# MAGIC ```
# MAGIC
# MAGIC ### To delete the scope (removes all secrets):
# MAGIC ```python
# MAGIC dbutils.secrets.deleteScope("migration-studio")
# MAGIC ```
