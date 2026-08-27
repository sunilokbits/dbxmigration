# One-Click Deploy — DBX Migration Studio

Deploy the full app (Databricks App + Jobs + Unity Catalog infra + secrets +
Genie Space + app tables) into **any** client's Databricks workspace with one
command, from any machine (Windows/macOS/Linux/CI) — no pre-installed
Databricks CLI or profile required.

## 1. Create a client config

```bash
cp deploy/client.template.json deploy/clients/<client_name>.json
```

Fill in the non-secret fields (workspace host, catalogs, storage account,
Azure subscription/RG, etc). **Never put passwords or tokens in this file** —
`deploy/clients/*.json` is git-ignored, but secrets still belong in
environment variables only.

## 2. Set secrets as environment variables

| Env var | Required | Purpose |
|---|---|---|
| `DATABRICKS_TOKEN` | Yes | PAT for the **target** client workspace |
| `DBX_SOURCE_PASSWORD` | If source DB configured | Source database password |
| `DBX_DEVOPS_PAT` | No | Azure DevOps PAT |
| `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` / `AZURE_TENANT_ID` | No | Service Principal for Azure infra (omit to use browser/CLI login) |

If a required var is missing and you're running interactively, the script
prompts for it (masked input) instead of failing.

## 3. Run it

macOS/Linux:
```bash
./deploy/deploy.sh deploy/clients/acme.json
```

Windows:
```powershell
.\deploy\deploy.ps1 deploy\clients\acme.json
```

Both launchers create an isolated `.deploy_venv`, install `requirements.txt`,
then run `deploy/one_click_deploy.py`. Useful flags (pass after the config path):

- `--skip-infra` — skip Azure Storage/Access Connector/Unity Catalog creation (use when infra already exists)
- `--skip-genie` — skip Genie Space resolution
- `--yes` — fully non-interactive (for CI); fails fast instead of prompting

## What it does, in order

1. **Pre-flight** — Python/package checks, installs the Databricks CLI if missing (asks first), verifies the token can authenticate to the target workspace, verifies Azure credentials.
2. **Genie Space** — reuses `genie_space_id` from the config if set; otherwise attempts to auto-create one via the Genie API, and falls back to a guided manual step (create in the UI, paste the ID) if that API isn't available in the target workspace/region.
3. **Infrastructure** — Azure Storage, Access Connector + RBAC, Unity Catalog storage credential, external locations, catalogs/schemas, and the landing volume (via the existing `migration_utility/AutoInfraCreation.py`, fully idempotent).
4. **Secrets** — creates/reuses the `migration-studio` secret scope and stores the source DB password, DevOps PAT, Databricks token, and a freshly generated per-client Flask session secret.
5. **Bundle deploy** — `databricks bundle deploy` against a generic, host-agnostic `client` target (see `databricks.yml`), passing every client-specific value as `--var`, then starts the App.
6. **App tables** — runs `src/sql/init_app_tables.sql` against the SQL warehouse (`CREATE TABLE IF NOT EXISTS`, safe to re-run).
7. **Validation** — re-checks catalog/schema/secret-scope/warehouse/App status/Genie Space reachability and writes a pass/fail report to `deploy/reports/<client>-<timestamp>.json`. The script exits non-zero if any required step failed.

## Re-running / idempotency

Every step is safe to re-run: Azure/UC resources are created only if missing,
secrets are overwritten with `put_secret`, and `bundle deploy` reconciles the
declared state. If a run fails partway, just fix the reported issue and run
the exact same command again.
