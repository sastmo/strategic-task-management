from __future__ import annotations

from typing import Any

import pandas as pd

from src.infrastructure.task_store import TaskWarehouseStore, load_tasks_from_database


def ensure_database_objects(connection) -> None:
    TaskWarehouseStore(connection).ensure_database_objects()


def log_event(
    connection,
    *,
    event_type: str,
    payload: dict[str, Any],
    actor_type: str = "system",
    actor_id: str = "src.sync_to_db",
) -> None:
    TaskWarehouseStore(connection).log_event(
        event_type=event_type,
        payload=payload,
        actor_type=actor_type,
        actor_id=actor_id,
    )


def create_ingestion_run(
    connection,
    *,
    source_config: dict[str, Any],
    union_mode: str,
    source_count: int,
    frame_count: int,
    staged_row_count: int,
    current_row_count: int,
) -> int:
    return TaskWarehouseStore(connection).create_ingestion_run(
        source_config=source_config,
        union_mode=union_mode,
        source_count=source_count,
        frame_count=frame_count,
        staged_row_count=staged_row_count,
        current_row_count=current_row_count,
    )


def finalize_ingestion_run(
    connection,
    *,
    run_id: int,
    status: str,
    inserted_count: int = 0,
    updated_count: int = 0,
    deleted_count: int = 0,
    unchanged_count: int = 0,
    error_message: str | None = None,
) -> None:
    TaskWarehouseStore(connection).finalize_ingestion_run(
        run_id=run_id,
        status=status,
        inserted_count=inserted_count,
        updated_count=updated_count,
        deleted_count=deleted_count,
        unchanged_count=unchanged_count,
        error_message=error_message,
    )


def stage_task_data(
    connection,
    *,
    run_id: int,
    staged_frame: pd.DataFrame,
    current_frame: pd.DataFrame,
) -> None:
    TaskWarehouseStore(connection).stage_task_data(
        run_id=run_id,
        staged_frame=staged_frame,
        current_frame=current_frame,
    )


def merge_staged_data(
    connection,
    *,
    run_id: int,
    source_names: list[str],
) -> dict[str, int]:
    return TaskWarehouseStore(connection).merge_staged_data(
        run_id=run_id,
        source_names=source_names,
    )


def prune_old_staging_data(connection, *, keep_days: int = 30) -> None:
    TaskWarehouseStore(connection).prune_old_staging_data(keep_days=keep_days)


__all__ = [
    "TaskWarehouseStore",
    "create_ingestion_run",
    "ensure_database_objects",
    "finalize_ingestion_run",
    "load_tasks_from_database",
    "log_event",
    "merge_staged_data",
    "prune_old_staging_data",
    "stage_task_data",
]
