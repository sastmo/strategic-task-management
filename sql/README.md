# SQL Layer

This folder holds analyst-facing SQL assets for the warehouse.

## Purpose

- `warehouse/`: reusable base views on top of the live warehouse tables
- `marts/`: business-oriented analytics views
- `ops/`: operational monitoring views for sync health

## Suggested usage

Apply these files to PostgreSQL with `psql` or your migration tool after the core warehouse tables exist.

Example:

```bash
psql "$DATABASE_URL" -f sql/warehouse/task_portfolio_base.sql
psql "$DATABASE_URL" -f sql/marts/marketing_portfolio_kpis.sql
psql "$DATABASE_URL" -f sql/marts/owner_execution_scorecard.sql
psql "$DATABASE_URL" -f sql/ops/sync_run_health.sql
```
