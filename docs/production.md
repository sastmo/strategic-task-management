# Production Notes

Strategic Alignment Board is designed so the same code can run locally in Docker and later move to Azure with configuration changes instead of a rewrite.

The production shape is intentionally simple:

- `app`: Streamlit dashboard container
- `sync`: background ingestion worker container
- `db`: managed PostgreSQL database
- `auth`: existing Microsoft/Azure identity layer
- `secrets`: Azure Key Vault or application settings

## Recommended Production Shape

| Component | Role |
|---|---|
| Streamlit app | Serves the executive alignment board |
| Sync worker | Reads configured sources and writes warehouse snapshots |
| PostgreSQL | Stores current task state, history, sync runs, auth roles, and audit events |
| Microsoft Graph / SharePoint | Preferred production source for spreadsheet-based workflows |
| Azure auth | Provides organization sign-in and identity headers |
| Key Vault | Stores database URLs, proxy secrets, and Graph secrets |

## Production Configuration

Set production mode explicitly:

```env
ENVIRONMENT=production
TASKS_SOURCE=postgresql://...
DATABASE_URL=postgresql://...
AUTH_MODE=app_service
AUTH_REQUIRED=true
AUTH_REQUIRE_EXPLICIT_ACCESS=true
AUTH_DEFAULT_ROLE=
AUTH_USE_DATABASE_ROLES=true
AUTH_AUDIT_TO_DATABASE=true
APP_TRUSTED_PROXY_SECRET=<stored outside git>
```

Optional Microsoft Graph settings for the sync worker:

```env
GRAPH_TENANT_ID=<tenant-id>
GRAPH_CLIENT_ID=<client-id>
GRAPH_CLIENT_SECRET=<stored outside git>
SYNC_SOURCE_CONFIG=<source-config>
```

## Production Guards

When `ENVIRONMENT=production`, the app rejects unsafe startup configurations.

| Configuration | Production behavior |
|---|---|
| `AUTH_MODE=local` | Rejected unless explicitly overridden |
| `AUTH_MODE=disabled` | Rejected unless explicitly overridden |
| `AUTH_ALLOW_UNVERIFIED_APP_SERVICE_PROXY=1` | Rejected |
| Missing `APP_TRUSTED_PROXY_SECRET` with `AUTH_MODE=app_service` | Rejected |
| Missing `DATABASE_URL` | Rejected |
| Non-PostgreSQL `TASKS_SOURCE` | Rejected |

Do not use override flags in a real deployment. They exist only to make local testing and guard behavior explicit.

## Authentication

Local development can use:

```env
AUTH_MODE=local
```

Production should use Azure/App Service style authentication:

1. Azure authenticates the user.
2. The app receives identity headers from the trusted auth layer.
3. `APP_TRUSTED_PROXY_SECRET` confirms the request passed through the expected proxy.
4. The app checks explicit access through database roles or configured groups.
5. Audit events can be written to PostgreSQL.

When `AUTH_USE_DATABASE_ROLES=true`, database lookup failures deny access. The app does not silently fall back to default roles or token claims.

## Azure Deployment

The repository includes:

```text
azure/container-apps.bicep
azure/deploy.sh
azure/parameters.example.json
```

Minimal deployment path:

1. Create or confirm the Azure resources: resource group, container registry, PostgreSQL, Key Vault, and app registration.
2. Copy `azure/parameters.example.json` to `azure/parameters.json`.
3. Fill in tenant IDs, group IDs, registry name, Key Vault references, and source configuration.
4. Store real secrets in Key Vault or Azure application settings.
5. Deploy with:

```bash
./azure/deploy.sh \
  --resource-group <resource-group> \
  --registry <registry>.azurecr.io \
  --env-name <environment-name>
```

6. Enable Microsoft authentication for the container app or fronting App Service.
7. Confirm the sync worker completes a successful ingestion run.

## Database Initialization

For the first deployment only, set:

```json
"bootstrapSchema": {
  "value": "true"
}
```

Deploy once, confirm the schema exists and the sync worker runs successfully, then set it back to:

```json
"bootstrapSchema": {
  "value": "false"
}
```

Runtime schema creation should not remain enabled after production initialization.

## Data Source Security

In production, preferred sources are:

- PostgreSQL for dashboard reads
- Microsoft Graph / SharePoint for controlled spreadsheet ingestion
- Local files only when restricted by `TASK_SOURCE_ROOT`

Generic HTTP API sources are blocked by default in production. Re-enable them only when there is a clear trust boundary and review process.

## Secrets

Never commit real secrets.

Keep these outside git:

- `DATABASE_URL`
- `POSTGRES_PASSWORD`
- `GRAPH_CLIENT_SECRET`
- `APP_TRUSTED_PROXY_SECRET`
- Azure parameter files containing real environment values

Use Azure Key Vault or Azure application settings for production values.

## Warehouse Behavior

The sync worker uses a snapshot pattern:

1. Read source data.
2. Normalize records.
3. Stage incoming rows.
4. Resolve the current snapshot.
5. Merge current records into `warehouse.tasks_current`.
6. Append changes to `warehouse.task_history`.
7. Record sync status in `ops.ingestion_runs`.

This gives the dashboard a stable current view while preserving enough history for auditing and freshness checks.

## Operational Notes

Useful settings:

| Setting | Purpose |
|---|---|
| `TASK_SOURCE_ROOT` | Restricts local file-source access |
| `TASK_CSV_CHUNK_ROWS` | Reduces peak memory during large CSV loads |
| `TEST_DATABASE_URL` | Enables PostgreSQL integration tests |
| `DB_BOOTSTRAP_SCHEMA` | Allows first-run schema initialization |

Set `DB_BOOTSTRAP_SCHEMA=false` after production initialization.
