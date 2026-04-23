CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.marketing_portfolio_kpis AS
SELECT
    portfolio.source_name,
    COUNT(*) AS task_count,
    COUNT(*) FILTER (WHERE portfolio.status = 'active') AS active_task_count,
    COUNT(*) FILTER (WHERE portfolio.status = 'paused') AS paused_task_count,
    COUNT(*) FILTER (WHERE portfolio.status = 'done') AS done_task_count,
    ROUND(AVG(portfolio.progress)::numeric, 1) AS avg_progress_pct,
    ROUND(AVG(portfolio.current_impact)::numeric, 1) AS avg_current_impact,
    ROUND(AVG(portfolio.future_impact)::numeric, 1) AS avg_future_impact,
    ROUND(AVG(portfolio.future_gap)::numeric, 1) AS avg_future_gap,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE portfolio.status = 'done') / NULLIF(COUNT(*), 0),
        1
    ) AS completion_rate_pct,
    COUNT(*) FILTER (
        WHERE portfolio.portfolio_bucket = 'growth_bet'
    ) AS growth_bet_count,
    COUNT(*) FILTER (
        WHERE portfolio.portfolio_bucket = 'revenue_protect'
    ) AS revenue_protect_count
FROM analytics.task_portfolio_base AS portfolio
GROUP BY portfolio.source_name
ORDER BY avg_future_impact DESC, avg_progress_pct DESC;
