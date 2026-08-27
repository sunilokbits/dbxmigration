# CI/CD setup — Azure DevOps

This project deploys via [`azure-pipelines.yml`](../../azure-pipelines.yml), a
4-stage pipeline: **Validate → Dev → Staging → Client production workspaces**.
It follows the same shape as the architecture guide: nothing deploys on a PR,
`main` auto-promotes through dev and staging, and a tagged `release/*` push
requires a manual approval before touching any client's production workspace.

One-time setup, in this order:

## 1. Create the Azure DevOps project & push the code

Already done if you're reading this from the repo. Repo:
`https://dev.azure.com/EMEA-SalesOps/AI Accelerator/_git/DBXMigrationapp`

## 2. Branch policies (Project Settings → Repos → Branches → `main`)

- Require a pull request before merging to `main`.
- Require at least 1 reviewer.
- Require the `Validate` pipeline stage to pass before merge.

## 3. Environments (Pipelines → Environments)

Create three:

| Environment | Purpose | Approval check |
|---|---|---|
| `dev` | auto-deploy target | none |
| `staging` | auto-deploy target | none |
| `clients-prod` | shared gate for **every** client production deploy | **Approvals** check — add the release approver(s) |

The `clients-prod` environment is what the manual-approval gate in the guide
maps to: every client job in the `DeployClients` stage runs against this one
environment, so a single approval check protects all of them.

## 4. Key Vault + variable groups (one per target)

Each stage pulls a variable group named `kv-<target>`:

- `kv-dev`, `kv-staging` — for the two shared internal workspaces.
- `kv-<clientName>` — one per onboarded client (must match an entry in the
  `clients` parameter list at the top of `azure-pipelines.yml`).

For each, create (or reuse) an Azure Key Vault, then in **Pipelines → Library
→ + Variable group**:

1. Name it `kv-<target>`.
2. Toggle **Link secrets from an Azure key vault as variables**.
3. Pick the subscription (via an ARM service connection using **workload
   identity federation** — no stored client secret) and the vault.
4. Authorize these secret names to be pulled in as variables:

   | Secret name | Used by | Contents |
   |---|---|---|
   | `DATABRICKS-HOST` | dev, staging | workspace URL |
   | `DATABRICKS-CLIENT-ID` / `DATABRICKS-CLIENT-SECRET` | dev, staging | Databricks OAuth M2M service-principal creds (per-target, least-privilege) |
   | `DATABRICKS-TOKEN` | staging (integration tests), every client | PAT scoped to that one workspace, used only where the bundle/CLI needs it |
   | `CLIENT-CONFIG-JSON` | every client | the full contents of that client's `deploy/client.template.json`, filled in (non-secret fields only) |
   | `DBX-SOURCE-PASSWORD` | every client (if source DB configured) | source database password |
   | `DBX-DEVOPS-PAT` | every client (optional) | Azure DevOps PAT used by the app itself, if configured |
   | `AZURE-CLIENT-ID` / `AZURE-CLIENT-SECRET` / `AZURE-TENANT-ID` | every client (optional) | SP used by `AutoInfraCreation.py` to provision that client's Azure/UC infra |

   Azure DevOps maps `SECRET-NAME` → `$(SECRET_NAME)` automatically, which is
   why the pipeline references `$(CLIENT_CONFIG_JSON)`, `$(DATABRICKS_TOKEN)`, etc.

## 5. Onboard a new client

1. Create the client's Key Vault (or a new secret set in a shared vault).
2. Populate the secrets from the table above — `CLIENT-CONFIG-JSON` is just
   `deploy/client.template.json` with the client's real (non-secret) values.
3. Create variable group `kv-<clientName>` linked to it.
4. Create an Azure DevOps service connection for that client's subscription
   using workload identity federation (Project Settings → Service connections
   → New → Azure Resource Manager → Workload identity federation).
5. Add `<clientName>` to the `clients` parameter list in `azure-pipelines.yml`.
6. Push a `release/*` tag — after `staging` succeeds, the `clients-prod`
   approval gate fires once and every client (including the new one) deploys.

## 6. Rollback

Same as any other target: re-run the pipeline against the previous `release/*`
tag, or `git revert` and re-tag. `databricks bundle deploy` reconciles state,
so redeploying an older revision is safe.
