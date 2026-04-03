from __future__ import annotations

import json
from typing import Any

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from src.domain.tasks import Task
from src.infrastructure.user_repository import AUTH_SCHEMA_STATEMENTS


class TaskWarehouseStore:
    def __init__(self, connection: psycopg.Connection) -> None:
        self.connection = connection

    def ensure_database_objects(self) -> None:
        statements = [
            "CREATE SCHEMA IF NOT EXISTS ops",
            "CREATE SCHEMA IF NOT EXISTS staging",
            "CREATE SCHEMA IF NOT EXISTS warehouse",
            """
            CREATE TABLE IF NOT EXISTS ops.ingestion_runs (
                run_id BIGSERIAL PRIMARY KEY,
                pipeline_name TEXT NOT NULL DEFAULT 'task_sync',
                union_mode TEXT NOT NULL DEFAULT 'union',
                source_config JSONB NOT NULL DEFAULT '{}'::jsonb,
                status TEXT NOT NULL DEFAULT 'running',
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                finished_at TIMESTAMPTZ,
                source_count INTEGER NOT NULL DEFAULT 0,
                frame_count INTEGER NOT NULL DEFAULT 0,
                staged_row_count INTEGER NOT NULL DEFAULT 0,
                current_row_count INTEGER NOT NULL DEFAULT 0,
                inserted_count INTEGER NOT NULL DEFAULT 0,
                updated_count INTEGER NOT NULL DEFAULT 0,
                deleted_count INTEGER NOT NULL DEFAULT 0,
                unchanged_count INTEGER NOT NULL DEFAULT 0,
                error_message TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS staging.task_records (
                run_id BIGINT NOT NULL REFERENCES ops.ingestion_runs(run_id) ON DELETE CASCADE,
                business_key TEXT NOT NULL,
                source_task_id TEXT NOT NULL,
                name TEXT NOT NULL,
                owner TEXT NOT NULL,
                current_impact INTEGER NOT NULL,
                future_impact INTEGER NOT NULL,
                progress INTEGER NOT NULL,
                done BOOLEAN NOT NULL,
                paused BOOLEAN NOT NULL,
                record_hash TEXT NOT NULL,
                source_name TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                source_sheet TEXT NOT NULL DEFAULT '',
                source_path TEXT NOT NULL DEFAULT '',
                source_priority INTEGER NOT NULL DEFAULT 100,
                source_order INTEGER NOT NULL DEFAULT 0,
                source_row_number INTEGER NOT NULL DEFAULT 0,
                loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS staging.task_snapshots (
                run_id BIGINT NOT NULL REFERENCES ops.ingestion_runs(run_id) ON DELETE CASCADE,
                record_id TEXT NOT NULL,
                business_key TEXT NOT NULL,
                source_task_id TEXT NOT NULL,
                name TEXT NOT NULL,
                owner TEXT NOT NULL,
                current_impact INTEGER NOT NULL,
                future_impact INTEGER NOT NULL,
                progress INTEGER NOT NULL,
                done BOOLEAN NOT NULL,
                paused BOOLEAN NOT NULL,
                record_hash TEXT NOT NULL,
                source_name TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                source_sheet TEXT NOT NULL DEFAULT '',
                source_path TEXT NOT NULL DEFAULT '',
                source_priority INTEGER NOT NULL DEFAULT 100,
                source_order INTEGER NOT NULL DEFAULT 0,
                source_row_number INTEGER NOT NULL DEFAULT 0,
                loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (run_id, record_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS warehouse.tasks_current (
                record_id TEXT PRIMARY KEY,
                business_key TEXT NOT NULL,
                source_task_id TEXT NOT NULL,
                name TEXT NOT NULL,
                owner TEXT NOT NULL,
                current_impact INTEGER NOT NULL,
                future_impact INTEGER NOT NULL,
                progress INTEGER NOT NULL,
                done BOOLEAN NOT NULL,
                paused BOOLEAN NOT NULL,
                record_hash TEXT NOT NULL,
                source_name TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                source_sheet TEXT NOT NULL DEFAULT '',
                source_path TEXT NOT NULL DEFAULT '',
                source_priority INTEGER NOT NULL DEFAULT 100,
                source_order INTEGER NOT NULL DEFAULT 0,
                source_row_number INTEGER NOT NULL DEFAULT 0,
                is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
                first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                created_run_id BIGINT REFERENCES ops.ingestion_runs(run_id),
                last_run_id BIGINT REFERENCES ops.ingestion_runs(run_id),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS warehouse.task_history (
                history_id BIGSERIAL PRIMARY KEY,
                run_id BIGINT NOT NULL REFERENCES ops.ingestion_runs(run_id),
                record_id TEXT NOT NULL,
                business_key TEXT NOT NULL,
                change_type TEXT NOT NULL,
                source_task_id TEXT NOT NULL,
                name TEXT NOT NULL,
                owner TEXT NOT NULL,
                current_impact INTEGER NOT NULL,
                future_impact INTEGER NOT NULL,
                progress INTEGER NOT NULL,
                done BOOLEAN NOT NULL,
                paused BOOLEAN NOT NULL,
                record_hash TEXT NOT NULL,
                source_name TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                source_sheet TEXT NOT NULL DEFAULT '',
                source_path TEXT NOT NULL DEFAULT '',
                changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_ingestion_runs_status
            ON ops.ingestion_runs (status, started_at DESC)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_task_records_run
            ON staging.task_records (run_id, business_key)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_task_snapshots_run
            ON staging.task_snapshots (run_id, business_key)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_tasks_current_live
            ON warehouse.tasks_current (source_name, owner)
            WHERE is_deleted = FALSE
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_tasks_current_business_key
            ON warehouse.tasks_current (business_key)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_task_history_key
            ON warehouse.task_history (business_key, changed_at DESC)
            """,
            """
            CREATE OR REPLACE VIEW warehouse.latest_task_records AS
            WITH latest_run AS (
                SELECT MAX(run_id) AS run_id
                FROM ops.ingestion_runs
                WHERE status = 'success'
            )
            SELECT
                r.run_id,
                r.business_key,
                r.source_task_id,
                r.name,
                r.owner,
                r.current_impact,
                r.future_impact,
                r.progress,
                r.done,
                r.paused,
                r.record_hash,
                r.source_name,
                r.source_kind,
                r.source_sheet,
                r.source_path,
                r.source_priority,
                r.source_order,
                r.source_row_number,
                r.loaded_at
            FROM staging.task_records r
            JOIN latest_run lr ON lr.run_id = r.run_id
            """,
            """
            CREATE OR REPLACE VIEW warehouse.latest_task_snapshot AS
            WITH latest_run AS (
                SELECT MAX(run_id) AS run_id
                FROM ops.ingestion_runs
                WHERE status = 'success'
            )
            SELECT
                s.run_id,
                s.record_id,
                s.business_key,
                s.source_task_id,
                s.name,
                s.owner,
                s.current_impact,
                s.future_impact,
                s.progress,
                s.done,
                s.paused,
                s.record_hash,
                s.source_name,
                s.source_kind,
                s.source_sheet,
                s.source_path,
                s.source_priority,
                s.source_order,
                s.source_row_number,
                s.loaded_at
            FROM staging.task_snapshots s
            JOIN latest_run lr ON lr.run_id = s.run_id
            """,
        ]
        statements[3:3] = list(AUTH_SCHEMA_STATEMENTS)

        with self.connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)

    def log_event(
        self,
        *,
        event_type: str,
        payload: dict[str, Any],
        actor_type: str = "system",
        actor_id: str = "src.sync_to_db",
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO app.event_log (event_type, actor_type, actor_id, payload)
                VALUES (%s, %s, %s, %s::jsonb)
                """,
                (event_type, actor_type, actor_id, json.dumps(payload)),
            )

    def create_ingestion_run(
        self,
        *,
        source_config: dict[str, Any],
        union_mode: str,
        source_count: int,
        frame_count: int,
        staged_row_count: int,
        current_row_count: int,
    ) -> int:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                INSERT INTO ops.ingestion_runs (
                    union_mode,
                    source_config,
                    source_count,
                    frame_count,
                    staged_row_count,
                    current_row_count
                )
                VALUES (%s, %s::jsonb, %s, %s, %s, %s)
                RETURNING run_id
                """,
                (
                    union_mode,
                    json.dumps(source_config),
                    source_count,
                    frame_count,
                    staged_row_count,
                    current_row_count,
                ),
            )
            row = cursor.fetchone()

        if row is None:
            raise RuntimeError("Failed to create ingestion run record.")

        return int(row["run_id"])

    def finalize_ingestion_run(
        self,
        *,
        run_id: int,
        status: str,
        inserted_count: int = 0,
        updated_count: int = 0,
        deleted_count: int = 0,
        unchanged_count: int = 0,
        error_message: str | None = None,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ops.ingestion_runs
                SET
                    status = %s,
                    inserted_count = %s,
                    updated_count = %s,
                    deleted_count = %s,
                    unchanged_count = %s,
                    error_message = %s,
                    finished_at = NOW()
                WHERE run_id = %s
                """,
                (
                    status,
                    inserted_count,
                    updated_count,
                    deleted_count,
                    unchanged_count,
                    error_message,
                    run_id,
                ),
            )

    def stage_task_data(
        self,
        *,
        run_id: int,
        staged_frame: pd.DataFrame,
        current_frame: pd.DataFrame,
    ) -> None:
        if not staged_frame.empty:
            staged_rows = [
                (
                    run_id,
                    str(row["business_key"]),
                    str(row["source_task_id"]),
                    str(row["name"]),
                    str(row["owner"]),
                    int(row["current_impact"]),
                    int(row["future_impact"]),
                    int(row["progress"]),
                    bool(row["done"]),
                    bool(row["paused"]),
                    str(row["record_hash"]),
                    str(row["source_name"]),
                    str(row["source_kind"]),
                    str(row["source_sheet"]),
                    str(row["source_path"]),
                    int(row["source_priority"]),
                    int(row["source_order"]),
                    int(row["source_row_number"]),
                )
                for _, row in staged_frame.iterrows()
            ]

            with self.connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO staging.task_records (
                        run_id,
                        business_key,
                        source_task_id,
                        name,
                        owner,
                        current_impact,
                        future_impact,
                        progress,
                        done,
                        paused,
                        record_hash,
                        source_name,
                        source_kind,
                        source_sheet,
                        source_path,
                        source_priority,
                        source_order,
                        source_row_number
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    staged_rows,
                )

        if not current_frame.empty:
            snapshot_rows = [
                (
                    run_id,
                    str(row["record_id"]),
                    str(row["business_key"]),
                    str(row["source_task_id"]),
                    str(row["name"]),
                    str(row["owner"]),
                    int(row["current_impact"]),
                    int(row["future_impact"]),
                    int(row["progress"]),
                    bool(row["done"]),
                    bool(row["paused"]),
                    str(row["record_hash"]),
                    str(row["source_name"]),
                    str(row["source_kind"]),
                    str(row["source_sheet"]),
                    str(row["source_path"]),
                    int(row["source_priority"]),
                    int(row["source_order"]),
                    int(row["source_row_number"]),
                )
                for _, row in current_frame.iterrows()
            ]

            with self.connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO staging.task_snapshots (
                        run_id,
                        record_id,
                        business_key,
                        source_task_id,
                        name,
                        owner,
                        current_impact,
                        future_impact,
                        progress,
                        done,
                        paused,
                        record_hash,
                        source_name,
                        source_kind,
                        source_sheet,
                        source_path,
                        source_priority,
                        source_order,
                        source_row_number
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    snapshot_rows,
                )

    def load_current_tasks(self) -> list[Task]:
        storage = self._detect_task_storage()
        if storage == "missing":
            return []

        query = (
            """
            SELECT
                record_id AS id,
                name,
                owner,
                current_impact,
                future_impact,
                progress,
                done,
                paused
            FROM warehouse.tasks_current
            WHERE is_deleted = FALSE
            ORDER BY owner, name
            """
            if storage == "warehouse"
            else """
            SELECT
                id,
                name,
                owner,
                current_impact,
                future_impact,
                progress,
                done,
                paused
            FROM public.tasks
            ORDER BY owner, name
            """
        )

        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        return [
            Task(
                id=str(row["id"]),
                name=str(row["name"]),
                owner=str(row["owner"]),
                current_impact=int(row["current_impact"]),
                future_impact=int(row["future_impact"]),
                progress=int(row["progress"]),
                done=bool(row["done"]),
                paused=bool(row["paused"]),
            )
            for row in rows
        ]

    def merge_staged_data(self, *, run_id: int, source_names: list[str]) -> dict[str, int]:
        counts = self._calculate_snapshot_counts(run_id=run_id)
        self._write_change_history(run_id=run_id)
        self._upsert_current_snapshot(run_id=run_id)
        deleted_count = self._mark_deleted_records(run_id=run_id, source_names=source_names)
        return {**counts, "deleted_count": deleted_count}

    def prune_old_staging_data(self, *, keep_days: int = 30) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM staging.task_records
                WHERE run_id IN (
                    SELECT run_id
                    FROM ops.ingestion_runs
                    WHERE finished_at IS NOT NULL
                      AND finished_at < NOW() - (%s * INTERVAL '1 day')
                )
                """,
                (keep_days,),
            )
            cursor.execute(
                """
                DELETE FROM staging.task_snapshots
                WHERE run_id IN (
                    SELECT run_id
                    FROM ops.ingestion_runs
                    WHERE finished_at IS NOT NULL
                      AND finished_at < NOW() - (%s * INTERVAL '1 day')
                )
                """,
                (keep_days,),
            )

    def _detect_task_storage(self) -> str:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    to_regclass('warehouse.tasks_current') AS warehouse_tasks,
                    to_regclass('public.tasks') AS public_tasks
                """
            )
            row = cursor.fetchone()

        if row and row["warehouse_tasks"]:
            return "warehouse"
        if row and row["public_tasks"]:
            return "legacy"
        return "missing"

    def _calculate_snapshot_counts(self, *, run_id: int) -> dict[str, int]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE target.record_id IS NULL) AS inserted_count,
                    COUNT(*) FILTER (
                        WHERE target.record_id IS NOT NULL
                        AND (target.record_hash <> src.record_hash OR target.is_deleted = TRUE)
                    ) AS updated_count,
                    COUNT(*) FILTER (
                        WHERE target.record_id IS NOT NULL
                        AND target.record_hash = src.record_hash
                        AND target.is_deleted = FALSE
                    ) AS unchanged_count
                FROM staging.task_snapshots src
                LEFT JOIN warehouse.tasks_current target
                    ON target.record_id = src.record_id
                WHERE src.run_id = %s
                """,
                (run_id,),
            )
            row = cursor.fetchone()

        return {
            "inserted_count": int(row["inserted_count"] or 0),
            "updated_count": int(row["updated_count"] or 0),
            "unchanged_count": int(row["unchanged_count"] or 0),
        }

    def _write_change_history(self, *, run_id: int) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO warehouse.task_history (
                    run_id,
                    record_id,
                    business_key,
                    change_type,
                    source_task_id,
                    name,
                    owner,
                    current_impact,
                    future_impact,
                    progress,
                    done,
                    paused,
                    record_hash,
                    source_name,
                    source_kind,
                    source_sheet,
                    source_path
                )
                SELECT
                    src.run_id,
                    src.record_id,
                    src.business_key,
                    CASE
                        WHEN target.record_id IS NULL THEN 'inserted'
                        WHEN target.is_deleted = TRUE THEN 'reactivated'
                        ELSE 'updated'
                    END AS change_type,
                    src.source_task_id,
                    src.name,
                    src.owner,
                    src.current_impact,
                    src.future_impact,
                    src.progress,
                    src.done,
                    src.paused,
                    src.record_hash,
                    src.source_name,
                    src.source_kind,
                    src.source_sheet,
                    src.source_path
                FROM staging.task_snapshots src
                LEFT JOIN warehouse.tasks_current target
                    ON target.record_id = src.record_id
                WHERE src.run_id = %s
                  AND (
                      target.record_id IS NULL
                      OR target.record_hash <> src.record_hash
                      OR target.is_deleted = TRUE
                  )
                """,
                (run_id,),
            )

    def _upsert_current_snapshot(self, *, run_id: int) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO warehouse.tasks_current (
                    record_id,
                    business_key,
                    source_task_id,
                    name,
                    owner,
                    current_impact,
                    future_impact,
                    progress,
                    done,
                    paused,
                    record_hash,
                    source_name,
                    source_kind,
                    source_sheet,
                    source_path,
                    source_priority,
                    source_order,
                    source_row_number,
                    is_deleted,
                    first_seen_at,
                    last_seen_at,
                    created_run_id,
                    last_run_id,
                    updated_at
                )
                SELECT
                    record_id,
                    business_key,
                    source_task_id,
                    name,
                    owner,
                    current_impact,
                    future_impact,
                    progress,
                    done,
                    paused,
                    record_hash,
                    source_name,
                    source_kind,
                    source_sheet,
                    source_path,
                    source_priority,
                    source_order,
                    source_row_number,
                    FALSE,
                    NOW(),
                    NOW(),
                    %s,
                    %s,
                    NOW()
                FROM staging.task_snapshots
                WHERE run_id = %s
                ON CONFLICT (record_id) DO UPDATE SET
                    business_key = EXCLUDED.business_key,
                    source_task_id = EXCLUDED.source_task_id,
                    name = EXCLUDED.name,
                    owner = EXCLUDED.owner,
                    current_impact = EXCLUDED.current_impact,
                    future_impact = EXCLUDED.future_impact,
                    progress = EXCLUDED.progress,
                    done = EXCLUDED.done,
                    paused = EXCLUDED.paused,
                    record_hash = EXCLUDED.record_hash,
                    source_name = EXCLUDED.source_name,
                    source_kind = EXCLUDED.source_kind,
                    source_sheet = EXCLUDED.source_sheet,
                    source_path = EXCLUDED.source_path,
                    source_priority = EXCLUDED.source_priority,
                    source_order = EXCLUDED.source_order,
                    source_row_number = EXCLUDED.source_row_number,
                    is_deleted = FALSE,
                    last_seen_at = NOW(),
                    last_run_id = EXCLUDED.last_run_id,
                    updated_at = NOW()
                """,
                (run_id, run_id, run_id),
            )

    def _mark_deleted_records(self, *, run_id: int, source_names: list[str]) -> int:
        if not source_names:
            return 0

        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                UPDATE warehouse.tasks_current target
                SET
                    is_deleted = TRUE,
                    last_run_id = %s,
                    updated_at = NOW()
                WHERE target.is_deleted = FALSE
                  AND target.source_name = ANY(%s)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM staging.task_snapshots src
                      WHERE src.run_id = %s
                        AND src.record_id = target.record_id
                  )
                RETURNING
                    target.record_id,
                    target.business_key,
                    target.source_task_id,
                    target.name,
                    target.owner,
                    target.current_impact,
                    target.future_impact,
                    target.progress,
                    target.done,
                    target.paused,
                    target.record_hash,
                    target.source_name,
                    target.source_kind,
                    target.source_sheet,
                    target.source_path
                """,
                (run_id, source_names, run_id),
            )
            deleted_rows = cursor.fetchall()

        if not deleted_rows:
            return 0

        history_rows = [
            (
                run_id,
                str(row["record_id"]),
                str(row["business_key"]),
                str(row["source_task_id"]),
                str(row["name"]),
                str(row["owner"]),
                int(row["current_impact"]),
                int(row["future_impact"]),
                int(row["progress"]),
                bool(row["done"]),
                bool(row["paused"]),
                str(row["record_hash"]),
                str(row["source_name"]),
                str(row["source_kind"]),
                str(row["source_sheet"]),
                str(row["source_path"]),
            )
            for row in deleted_rows
        ]

        with self.connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO warehouse.task_history (
                    run_id,
                    record_id,
                    business_key,
                    change_type,
                    source_task_id,
                    name,
                    owner,
                    current_impact,
                    future_impact,
                    progress,
                    done,
                    paused,
                    record_hash,
                    source_name,
                    source_kind,
                    source_sheet,
                    source_path
                )
                VALUES (%s, %s, %s, 'deleted', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                history_rows,
            )

        return len(deleted_rows)


def load_tasks_from_database(database_url: str) -> list[Task]:
    with psycopg.connect(database_url) as connection:
        store = TaskWarehouseStore(connection)
        return store.load_current_tasks()
