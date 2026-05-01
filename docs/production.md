# Production Notes

This project is designed so the same code can run locally in Docker and later move to a cloud host with configuration changes instead of a rewrite.

## Recommended shape

- `app`: Streamlit UI container
- `sync`: background worker container
- `db`: PostgreSQL in local development

Typical production split:

- web app on Azure App Service
- PostgreSQL on a managed database service
- sync worker on a scheduled container or job runner

## Authentication

Supported modes:

- `AUTH_MODE=local` for local development
- `AUTH_MODE=app_service` for Azure App Service authentication
- `AUTH_MODE=disabled` only for intentionally open internal environments

In production, Azure should handle sign-in. The app then reads the user identity from App Service headers and applies app-side authorization rules.

## Graph / OneDrive access

The sync worker uses Microsoft Graph as a source adapter.

Typical production setup:

- one app registration for website login, handled by Azure/App Service
- one app registration for Graph access, used by the sync worker

The sync worker needs:

- `GRAPH_TENANT_ID`
- `GRAPH_CLIENT_ID`
- `GRAPH_CLIENT_SECRET`

## Secrets

Do not commit real secrets.

Store secrets in:

- Azure App Service application settings
- a managed secret store such as Key Vault
- or another environment-level secret manager

At minimum, keep these out of git:

- `POSTGRES_PASSWORD`
- `GRAPH_CLIENT_SECRET`
- any real production database URL

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
