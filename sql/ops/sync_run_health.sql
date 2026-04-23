CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.sync_run_health AS
SELECT
    runs.run_id,
    runs.pipeline_name,
    runs.status,
    runs.union_mode,
    runs.source_count,
    runs.frame_count,
    runs.staged_row_count,
    runs.current_row_count,
    runs.inserted_count,
    runs.updated_count,
    runs.deleted_count,
    runs.unchanged_count,
    runs.started_at,
    runs.finished_at,
    EXTRACT(EPOCH FROM (runs.finished_at - runs.started_at))::numeric(12, 2) AS duration_seconds,
    CASE
        WHEN runs.status = 'success' THEN FALSE
        ELSE TRUE
    END AS needs_attention,
    runs.error_message
FROM ops.ingestion_runs AS runs
ORDER BY runs.run_id DESC;
