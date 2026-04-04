# Strategic Task Management

## Run locally

### Mac/Linux
```bash
./run.sh
```

### Windows
```bat
run.bat
```

## Default local stack

```bash
docker compose up --build
```

## CI

GitHub Actions now runs these checks on pushes, pull requests, and manual runs:

```bash
python -m compileall src app.py
python -m unittest discover -s tests -v
docker compose config
```

## Architecture at a glance

- `app.py`: Streamlit entrypoint
- `src/domain/`: business rules and core models
- `src/application/`: orchestration for auth, sync, settings, and workflows
- `src/infrastructure/`: files, Graph, database, Azure, and external system integrations
- `src/presentation/`: dashboard rendering and auth-facing UI pieces
- `sql/`: warehouse, marts, and operational analytics SQL assets
- `tests/`: unit and behavior tests

## Compatibility modules

These root modules still exist to avoid breaking older imports:

- `src/loader.py`
- `src/schema.py`
- `src/sync_to_db.py`
- `src/warehouse.py`
- `src/auto_sync.py`
- `src/dashboard.py`

The main implementation now lives in the layered packages under `src/`.

## Source config examples

### Local files
```json
{
  "sources": [
    "/app/data/tasks.csv",
    {
      "source": "/app/data/planning.xlsx",
      "source_name": "planning_book",
      "all_sheets": true
    }
  ],
  "union_mode": "union"
}
```

### Microsoft Graph / SharePoint
```json
{
  "sources": [
    {
      "kind": "graph",
      "site_url": "https://contoso.sharepoint.com/sites/Strategy",
      "drive_name": "Shared Documents",
      "file_path": "/Plans/master.xlsx",
      "source_name": "sharepoint_master",
      "all_sheets": true
    }
  ],
  "union_mode": "union"
}
```

Set that config in `SYNC_SOURCE_CONFIG` or `TASKS_SOURCE`.
`site_url` should be the SharePoint site itself, and `file_path` should be relative to the selected document library root.

## Graph auth env vars

- `GRAPH_AUTH_MODE=client_secret`
- `GRAPH_TENANT_ID=...`
- `GRAPH_CLIENT_ID=...`
- `GRAPH_CLIENT_SECRET=...`

For Azure-managed identity later, switch `GRAPH_AUTH_MODE` to `managed_identity` or `default_azure_credential`.
