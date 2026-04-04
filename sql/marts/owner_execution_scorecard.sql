CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.owner_execution_scorecard AS
SELECT
    portfolio.owner,
    COUNT(*) AS task_count,
    COUNT(*) FILTER (WHERE portfolio.status = 'active') AS active_task_count,
    COUNT(*) FILTER (WHERE portfolio.status = 'paused') AS paused_task_count,
    COUNT(*) FILTER (WHERE portfolio.status = 'done') AS done_task_count,
    ROUND(AVG(portfolio.progress)::numeric, 1) AS avg_progress_pct,
    ROUND(AVG(portfolio.current_impact)::numeric, 1) AS avg_current_impact,
    ROUND(AVG(portfolio.future_impact)::numeric, 1) AS avg_future_impact,
    ROUND(AVG(portfolio.execution_gap)::numeric, 1) AS avg_execution_gap,
    MAX(portfolio.future_impact) AS max_future_impact,
    COUNT(*) FILTER (
        WHERE portfolio.status = 'active'
          AND portfolio.future_impact >= 80
          AND portfolio.progress < 50
    ) AS high_priority_open_count
FROM analytics.task_portfolio_base AS portfolio
GROUP BY portfolio.owner
ORDER BY avg_future_impact DESC, avg_progress_pct DESC, task_count DESC;
