# Production Notes

This project is designed so the same code can run locally in Docker and later move to a cloud host with configuration changes instead of a rewrite.

## Recommended shape

- `app`: Streamlit UI container
- `sync`: background worker container
- `db`: PostgreSQL in local development (managed service in production)

## Authentication

Supported modes:

- `AUTH_MODE=local` for local development only
- `AUTH_MODE=app_service` for Azure App Service / Easy Auth in production
- `AUTH_MODE=disabled` only for intentionally open internal environments (not recommended)

### Production guard

Setting `ENVIRONMENT=production` enforces the following at startup:

| Configuration | Allowed in production? |
|---|---|
| `AUTH_MODE=local` | No -- raises `RuntimeError` unless `ALLOW_LOCAL_AUTH_IN_PRODUCTION=1` |
| `AUTH_MODE=disabled` | No -- raises `RuntimeError` unless `ALLOW_DISABLED_AUTH_IN_PRODUCTION=1` |
| `AUTH_ALLOW_UNVERIFIED_APP_SERVICE_PROXY=1` | No -- raises `RuntimeError` unconditionally |

Never set the override flags in a real deployment.  They exist only to document why the guard is there.

### Fail-closed database behavior

When `AUTH_USE_DATABASE_ROLES=true` and the database becomes unreachable:

- The app denies access to every request until the database recovers.
- No fallback to token claims or default roles occurs.
- A `ERROR` log entry is written for each denied request.

This is intentional.  A database outage must not silently degrade to
unauthenticated access on an executive dashboard.

## Azure deployment

See `azure/container-apps.bicep` for the full Bicep template.  The minimal
steps for a first deployment:

### 1. Prerequisites

```bash
az login
az acr login --name YOURREGISTRY
cp azure/parameters.example.json azure/parameters.json
# Edit azure/parameters.json with your values
```

### 2. Create supporting resources (once)

```bash
# Resource group
az group create --name stm-prod --location canadacentral

# Azure Database for PostgreSQL Flexible Server
az postgres flexible-server create \
  --resource-group stm-prod \
  --name stm-db \
  --sku-name Standard_B1ms \
  --tier Burstable \
  --storage-size 32 \
  --version 16

# Azure Container Registry
az acr create --resource-group stm-prod --name YOURREGISTRY --sku Basic

# Key Vault for secrets
az keyvault create --resource-group stm-prod --name stm-vault
az keyvault secret set --vault-name stm-vault --name stm-database-url \
  --value "postgresql://USER:PASS@stm-db.postgres.database.azure.com:5432/strategic_tasks"
az keyvault secret set --vault-name stm-vault --name stm-proxy-secret \
  --value "$(openssl rand -hex 32)"
```

### 3. Deploy

```bash
./azure/deploy.sh \
  --resource-group stm-prod \
  --registry YOURREGISTRY.azurecr.io \
  --env-name stm-prod
```

### 4. Enable Easy Auth

In the Azure Portal, open the app Container App, go to **Authentication**, add
Microsoft identity provider.  Set the client ID and secret of your app
registration.

Set these application settings on the container app:

```
AUTH_MODE=app_service
ENVIRONMENT=production
APP_TRUSTED_PROXY_SECRET=<same value stored in Key Vault as stm-proxy-secret>
AUTH_ALLOWED_TENANT_IDS=<your Azure AD tenant ID>
AUTH_USE_DATABASE_ROLES=true
```

### 5. Initialize the database schema

Run once after the first deployment:

```bash
az containerapp exec \
  --name stm-prod-sync \
  --resource-group stm-prod \
  --command "python -c \"from src.infrastructure.task_store import load_tasks_from_database; load_tasks_from_database('')\" "
```

Or set `DB_BOOTSTRAP_SCHEMA=true` on the sync container for the first run only, then set it back to `false`.

## Graph / OneDrive access

The sync worker uses Microsoft Graph as a source adapter.

Typical production setup:

- one app registration for website login (handled by Easy Auth)
- one app registration for Graph access (used by the sync worker)

The sync worker needs:

- `GRAPH_TENANT_ID`
- `GRAPH_CLIENT_ID`
- `GRAPH_CLIENT_SECRET`

Store all three in Key Vault and reference them in `parameters.json`.

## Data source security

In production (`ENVIRONMENT=production`):

- Generic HTTP API sources (`api` kind) are **blocked by default**.
- Allowed sources: `postgres`, `graph` (SharePoint/OneDrive), local files inside `TASK_SOURCE_ROOT`.
- To re-enable API sources explicitly: `TASK_SOURCE_ALLOWED_KINDS=csv,json,excel,graph,postgres,api`

## Secrets

Do not commit real secrets.

Store secrets in:

- Azure Key Vault referenced from `parameters.json`
- Azure App Service / Container App application settings (for values not in Key Vault)

At minimum, keep these out of git:

- `POSTGRES_PASSWORD` / database URL
- `GRAPH_CLIENT_SECRET`
- `APP_TRUSTED_PROXY_SECRET`

## Warehouse behavior

The sync process is snapshot-based:

- stage incoming rows
- resolve the current snapshot
- merge into `warehouse.tasks_current`
- append changes to `warehouse.task_history`

## Operational notes

- `TASK_SOURCE_ROOT` limits local file-source expansion
- `TASK_CSV_CHUNK_ROWS` can reduce peak memory during large CSV loads
- `TEST_DATABASE_URL` enables the Postgres integration test path
- `DB_BOOTSTRAP_SCHEMA=true` auto-creates schema on first run; set to `false` in production after initialization
