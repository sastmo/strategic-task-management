CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.task_portfolio_base AS
SELECT
    current_tasks.record_id,
    current_tasks.business_key,
    current_tasks.source_task_id,
    current_tasks.source_name,
    current_tasks.source_kind,
    current_tasks.owner,
    current_tasks.name,
    current_tasks.current_impact,
    current_tasks.future_impact,
    current_tasks.progress,
    current_tasks.done,
    current_tasks.paused,
    CASE
        WHEN current_tasks.done OR current_tasks.progress >= 100 THEN 'done'
        WHEN current_tasks.paused THEN 'paused'
        ELSE 'active'
    END AS status,
    current_tasks.future_impact - current_tasks.current_impact AS future_gap,
    GREATEST(current_tasks.future_impact - current_tasks.progress, 0) AS execution_gap,
    CASE
        WHEN current_tasks.future_impact >= 80 AND current_tasks.progress < 60 THEN 'growth_bet'
        WHEN current_tasks.current_impact >= 75 AND current_tasks.progress < 60 THEN 'revenue_protect'
        WHEN current_tasks.progress >= 100 THEN 'landed'
        ELSE 'managed'
    END AS portfolio_bucket,
    current_tasks.first_seen_at,
    current_tasks.last_seen_at,
    current_tasks.updated_at
FROM warehouse.tasks_current AS current_tasks
WHERE current_tasks.is_deleted = FALSE;
