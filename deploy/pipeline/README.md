# CI/CD setup — Azure DevOps (minimum-effort version)

This project deploys via [`azure-pipelines.yml`](../../azure-pipelines.yml), a
4-stage pipeline: **Validate → Dev → Staging → Client production workspaces**.
Nothing deploys on a PR; `main` auto-promotes through dev and staging; a
tagged `release/*` push requires one manual approval, then fans out to every
client workspace in parallel.

**Auth model:** a Databricks PAT per workspace (dev, staging, one per client)
— that's it. No Key Vault, no service connections, no separate secrets
system. That PAT is the only thing that has to exist before the pipeline can
reach a workspace for the first time; everything else — Unity Catalog infra,
the Databricks secret scope + every secret in it, the Genie Space, and the
Databricks App itself — is created automatically by
`deploy/one_click_deploy.py`, which the `DeployClients` stage already calls.

Only production has an approval gate, so only one Azure DevOps Environment is
needed, total — dev/staging deploy straight through.

## Total setup: 1 environment + a variable group per workspace

## 1. Create the pipeline

**Pipelines → New pipeline → Azure Repos Git → DBXMigrationapp → Existing
YAML file → `/azure-pipelines.yml`**. Save — don't run yet, step 2 below
needs to exist first or the pipeline fails with "variable group not found".

## 2. Variable groups (Pipelines → Library → + Variable group)

One group per target, plain Azure DevOps variables (no Key Vault link needed)
— mark the ones below as **secret** using the lock icon next to each value:

**`kv-dev`** and **`kv-staging`**
| Variable | Secret? | Value |
|---|---|---|
| `DATABRICKS_HOST` | no | that workspace's URL |
| `DATABRICKS_TOKEN` | **yes** | PAT for that workspace |

**`kv-<clientName>`** — one per onboarded client, name must match an entry in
the `clients` parameter in `azure-pipelines.yml`:
| Variable | Secret? | Value |
|---|---|---|
| `DATABRICKS_HOST` | no | client workspace URL |
| `DATABRICKS_TOKEN` | **yes** | PAT for that client's workspace |
| `CLIENT_CONFIG_JSON` | no | full contents of `deploy/client.template.json`, filled in with that client's non-secret values (catalogs, storage account, Azure sub/RG, etc.) |
| `DBX_SOURCE_PASSWORD` | **yes** | source DB password — omit the variable entirely if no source DB is configured for this client |
| `DBX_DEVOPS_PAT` | **yes**, optional | only if the app itself needs to call Azure DevOps |
| `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` / `AZURE_TENANT_ID` | **yes**, optional | only if you want Azure storage/Unity Catalog infra auto-provisioned for this client; omit all three to skip infra creation (`--skip-infra`) |

## 3. One Environment (Pipelines → Environments → New environment)

Create **`clients-prod`** → add an **Approvals** check with your release
approver(s). This is the only environment needed — it's what every client
job in `DeployClients` waits on.

## 4. List your clients in the pipeline

Edit the `clients` parameter at the top of `azure-pipelines.yml`:
```yaml
parameters:
  - name: clients
    default:
      - insight
```
**Important:** Azure DevOps checks that every `kv-<name>` group referenced here
exists *before the pipeline runs at all* — even for the `DeployClients` stage,
which normally only runs on a `release/*` tag. If you list a client whose
`kv-<name>` group doesn't exist yet (or was deleted), the **entire pipeline
fails immediately**, including `Validate` and `DeployDev`. Keep this list
empty (`default: []`) until each client's variable group actually exists.

## 5. Ship it

- Merge a PR to `main` → deploys to `dev`, then `staging`
- Push a tag like `release/1.0.0` → waits for approval on `clients-prod`,
  then deploys to every listed client in parallel — infra, secrets, Genie
  Space, and the Databricks App all created/updated automatically

## 6. Onboard a new client later (2 steps, no pipeline redesign)

1. Generate one Databricks PAT for that client's workspace → create its
   `kv-<clientName>` variable group (step 2's table).
2. Add `<clientName>` to the `clients` list in `azure-pipelines.yml`, push a
   `release/*` tag.

## 7. Rollback

Re-run the pipeline against the previous `release/*` tag, or `git revert` and
re-tag. `databricks bundle deploy` reconciles state, so redeploying an older
revision is safe.
