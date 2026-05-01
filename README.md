# Strategic Task Management

A dashboard that consolidates strategic tasks from multiple data sources and displays them as an interactive bubble map and per-owner card view.

![Dashboard overview](assets/0_dashboard-summary.png)

![Owner detail view](assets/1_dashboard-detail.png)

---

## Run locally

Copy `.env.example` to `.env`, then:

```bash
docker compose up --build
```

The app is available at `http://localhost:8501`.

For a direct Python run instead of Docker:

```bash
./run.sh
```

---

## Data sources

Set `TASKS_SOURCE` (for the web app) or `SYNC_SOURCE_CONFIG` (for the background sync service) to any supported source.

| Type | Example |
|---|---|
| CSV | `/app/data/tasks.csv` |
| Excel | `{"source": "plan.xlsx", "all_sheets": true}` |
| JSON | `/app/data/tasks.json` |
| REST API | `https://api.example.com/tasks` |
| SharePoint | see below |

### SharePoint / Microsoft Graph

```json
{
  "sources": [
    {
      "kind": "graph",
      "site_url": "https://contoso.sharepoint.com/sites/Strategy",
      "drive_name": "Shared Documents",
      "file_path": "/Plans/master.xlsx",
      "all_sheets": true
    }
  ]
}
```

Required environment variables:

```
GRAPH_AUTH_MODE=client_secret
GRAPH_TENANT_ID=...
GRAPH_CLIENT_ID=...
GRAPH_CLIENT_SECRET=...
```

### Large CSV handling

For large CSV sources, you can ask the loader to read the file in smaller batches:

```env
TASK_CSV_CHUNK_ROWS=5000
```

This keeps the sync model snapshot-based, but lowers peak memory during CSV ingestion by processing the file in smaller DataFrame chunks before the final merge.

---

## Authentication

| `AUTH_MODE` | When to use |
|---|---|
| `local` | Local development, no sign-in required |
| `app_service` | Azure App Service with AAD |
| `disabled` | Fully open, internal deployments |

---

## Project layout

```
app.py                  Streamlit entry point
src/domain/             Core models and business rules
src/application/        Auth, sync, and workflow orchestration
src/infrastructure/     Database, Graph API, file readers
src/presentation/       Dashboard rendering and auth UI
sql/                    Warehouse and analytics SQL views
tests/                  Unit and behaviour tests
data/                   Default local task files
assets/                 Screenshots and static files
```

---

## Tests

```bash
python3.12 -m unittest discover -s tests -v
python3.12 -m mypy
ruff check .
```

Optional integration test:

```bash
TEST_DATABASE_URL=postgresql://... python3.12 -m unittest tests.test_task_store_integration -v
```

---

## Production handoff

For Azure/App Service production:

- keep the same app code
- set `AUTH_MODE=app_service`
- store secrets in Azure configuration or a secret manager, not in git
- use one Graph app registration for the sync worker and Azure/App Service auth for website login
- keep the database as the live read layer for the app
- use `TASK_SOURCE_ROOT` when mounting local folders in dev or worker environments
