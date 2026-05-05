[![CI](https://github.com/sastmo/strategic-task-management/actions/workflows/ci.yml/badge.svg)](https://github.com/sastmo/strategic-task-management/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Executive%20Dashboard-ff4b4b.svg)](https://streamlit.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Warehouse-336791.svg)](https://www.postgresql.org/)
[![Azure](https://img.shields.io/badge/Azure-Container%20Apps-0078D4.svg)](https://azure.microsoft.com/en-us/products/container-apps)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED.svg)](https://www.docker.com/)
[![Security](https://img.shields.io/badge/Security-Fail--Closed-brightgreen.svg)]()
[![Product](https://img.shields.io/badge/Product-Executive%20Alignment-purple.svg)]()

# Strategic Alignment Board

This is an internal executive alignment board for senior leaders who need a fast read on whether teams are working on the priorities that protect current sales and create future sales opportunities.

It is built for organizations where work is scattered across spreadsheets, notes, Jira, Trello, Asana, and department-specific tools. The app does not replace those systems. It sits above them, normalizes the signals, and turns them into a simple matrix that makes ownership, progress, and misalignment visible.

The product is intentionally minimal and low-interaction. Its job is not to become another task-management tool; its job is to create a shared executive view that can be understood in minutes and used to drive accountability.

## Executive View

The dashboard starts with a compact summary view for quick senior review.

![Dashboard overview](assets/0_dashboard-summary.png)

The owner detail view adds enough context to support follow-up without turning the product into a planning tool.

![Owner detail view](assets/1_dashboard-detail.png)

## Product Needs And Design Decisions

| User reality | Product need | Design decision |
|---|---|---|
| Senior leaders are busy and need signal quickly | The UI must be simple, visual, and low-interaction | Executive matrix, owner cards, minimal navigation |
| Teams already use different tools | The app should not replace team workflows | Source adapters and normalized ingestion layer |
| Many organizations already depend on cloud services, and this repo uses Azure/Microsoft as the example path | Deployment should fit existing enterprise cloud and identity patterns | Microsoft Graph/SharePoint, Azure auth, Azure hosting |
| Leadership needs trusted, current information | Data should be snapshotted and auditable | PostgreSQL warehouse, sync runs, task history |
| Internal strategy data is sensitive | Access should fail closed | Explicit auth, database roles, proxy validation |
| Admins need a manageable deployment path | Setup should be containerized and cloud-ready | Docker, Azure Container Apps, environment-based config |

## How It Works

The app is designed as a visibility layer above existing team workflows.

1. Teams continue using their current planning tools or controlled source files.
2. A sync worker reads configured sources such as CSV, Excel, JSON, Microsoft Graph, or SharePoint.
3. Incoming rows are normalized into a common task model.
4. PostgreSQL stores the current task state, sync history, and change history.
5. The Streamlit dashboard reads from PostgreSQL and presents the executive alignment matrix.

In production, the dashboard should read from PostgreSQL. Source ingestion should happen through the sync worker, not directly through the UI.

## Architecture

The repository separates product logic, infrastructure, and presentation so the dashboard can stay simple while the ingestion and deployment paths remain production-aware.

```text
app.py                  Streamlit entry point
src/domain/             Task model and business rules
src/application/        Auth, settings, sync orchestration, workflow loading
src/infrastructure/     PostgreSQL, Microsoft Graph, source readers
src/presentation/       Dashboard HTML, CSS, JavaScript, auth UI
sql/                    Optional warehouse, mart, and ops SQL views
tests/                  Unit, behavior, security, and integration tests
azure/                  Azure Container Apps deployment template and script
data/                   Local sample source files
assets/                 README screenshots
```

Runtime services:

- `app`: Streamlit dashboard for the executive view.
- `sync`: background worker that ingests source data and writes warehouse snapshots.
- `postgres`: local Docker database for development. Use a managed PostgreSQL service in production.

<details>
<summary><strong>Run Locally</strong></summary>

Copy the example environment file and choose a local-only database password:

```bash
cp .env.example .env
```

Run the full local stack:

```bash
docker compose up --build
```

The dashboard will be available at:

```text
http://localhost:8501
```

For a direct Python run:

```bash
./run.sh
```

Direct Python mode is useful for UI iteration with sample data. Docker Compose is closer to the production shape because it includes PostgreSQL and the sync worker.

</details>

<details>
<summary><strong>Configuration</strong></summary>

The app is configured through environment variables so the same codebase can run locally, in Docker, and in production.

Important production settings include:

| Variable | Purpose |
|---|---|
| `ENVIRONMENT` | Set to `production` to enable production guards |
| `DATABASE_URL` | PostgreSQL warehouse/auth database URL |
| `TASKS_SOURCE` | Dashboard read source. Should be PostgreSQL in production |
| `SYNC_SOURCE_CONFIG` | Source configuration used by the sync worker |
| `AUTH_MODE` | `local`, `app_service`, or `disabled` |
| `AUTH_REQUIRE_EXPLICIT_ACCESS` | Prevents default access grants when true |
| `AUTH_USE_DATABASE_ROLES` | Uses database-backed app roles when true |
| `APP_TRUSTED_PROXY_SECRET` | Shared proxy secret for production App Service auth |
| `GRAPH_TENANT_ID` | Microsoft Graph tenant ID |
| `GRAPH_CLIENT_ID` | Microsoft Graph app/client ID |
| `GRAPH_CLIENT_SECRET` | Microsoft Graph client secret. Store outside git |
| `DB_BOOTSTRAP_SCHEMA` | Allows first-run schema initialization when true |

Detailed production configuration lives in [docs/production.md](docs/production.md).

</details>

<details>
<summary><strong>Authentication And Authorization</strong></summary>

Local development can use:

```env
AUTH_MODE=local
```

Production is designed for an Azure/Microsoft environment:

- Azure handles sign-in.
- The app reads trusted identity headers.
- A shared proxy secret confirms the request passed through the expected auth layer.
- Database roles or Azure group mapping control access.
- Audit events can be written to PostgreSQL.

The production posture is fail-closed. Unsafe local or disabled auth modes are rejected when `ENVIRONMENT=production`, and database role lookup failures deny access instead of silently granting fallback permissions.

</details>

<details>
<summary><strong>Azure Deployment</strong></summary>

The included Azure assets are intended to support a small internal deployment using existing enterprise infrastructure:

- Azure Container Apps or App Service style authentication
- Azure Database for PostgreSQL
- Azure Key Vault for secrets
- Microsoft Graph/SharePoint as the preferred spreadsheet source adapter
- HTTPS-only ingress

Deployment details are intentionally kept out of the main README. See [docs/production.md](docs/production.md) for the minimal production path.

</details>

<details>
<summary><strong>Security Posture</strong></summary>

Security decisions focus on the risks that matter for a small internal executive dashboard:

- Production auth fails closed for unsafe modes.
- Database-backed role lookup failures deny access when database roles are required.
- Generic HTTP API task sources are blocked by default in production.
- Local file sources are restricted by `TASK_SOURCE_ROOT`.
- Dashboard JSON is escaped before being embedded into HTML.
- Real secrets should live in Azure Key Vault or application settings, not in git.

</details>

<details>
<summary><strong>Testing</strong></summary>

Run the main checks:

```bash
ruff check .
mypy
python -m unittest discover -s tests -v
```

Run coverage the same way CI does:

```bash
pytest tests/ -v --tb=short --cov=src --cov-report=xml --cov-report=term-missing --cov-fail-under=70
```

Optional PostgreSQL integration tests:

```bash
TEST_DATABASE_URL=postgresql://... python -m unittest tests.test_task_store_integration -v
```

CI runs Ruff, mypy, source compilation, tests with coverage, coverage artifact upload, and Docker Compose config validation.

</details>

## Production Notes

For deployment, authentication, data source security, secrets, warehouse behavior, and operational settings, see [docs/production.md](docs/production.md).
