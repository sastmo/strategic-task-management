from __future__ import annotations

from dataclasses import asdict, dataclass
import logging
from typing import Any

from src.application.task_workflow import load_task_batch

_logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SyncSummary:
    run_id: int
    source_count: int
    frame_count: int
    staged_row_count: int
    current_row_count: int
    union_mode: str
    inserted_count: int
    updated_count: int
    deleted_count: int
    unchanged_count: int

    def as_dict(self) -> dict[str, int | str]:
        return asdict(self)

    def describe(self) -> str:
        return (
            "Task sync complete "
            f"(run_id={self.run_id}, sources={self.source_count}, frames={self.frame_count}, "
            f"staged={self.staged_row_count}, current={self.current_row_count}, "
            f"inserted={self.inserted_count}, updated={self.updated_count}, "
            f"deleted={self.deleted_count}, unchanged={self.unchanged_count})."
        )


def sync_to_database(source_input: Any, database_url: str) -> SyncSummary:
    import psycopg

    from src.infrastructure.task_store import TaskWarehouseStore

    batch = load_task_batch(source_input)
    source_names = sorted({source.source_name for source in batch.resolved_sources})
    source_config_payload = batch.source_config_payload()

    with psycopg.connect(database_url) as connection:
        store = TaskWarehouseStore(connection)
        store.ensure_database_objects()

        run_id = store.create_ingestion_run(
            source_config=source_config_payload,
            union_mode=batch.source_config.union_mode,
            source_count=batch.source_count,
            frame_count=batch.frame_count,
            staged_row_count=len(batch.staged_frame),
            current_row_count=len(batch.current_frame),
        )
        store.log_event(
            event_type="task_sync.started",
            payload={
                "run_id": run_id,
                "source_count": batch.source_count,
                "frame_count": batch.frame_count,
                "staged_row_count": len(batch.staged_frame),
                "current_row_count": len(batch.current_frame),
                "union_mode": batch.source_config.union_mode,
            },
        )
        connection.commit()

        try:
            store.stage_task_data(
                run_id=run_id,
                staged_frame=batch.staged_frame,
                current_frame=batch.current_frame,
            )
            merge_stats = store.merge_staged_data(
                run_id=run_id,
                source_names=source_names,
            )
            store.finalize_ingestion_run(
                run_id=run_id,
                status="success",
                inserted_count=merge_stats["inserted_count"],
                updated_count=merge_stats["updated_count"],
                deleted_count=merge_stats["deleted_count"],
                unchanged_count=merge_stats["unchanged_count"],
            )
            store.log_event(
                event_type="task_sync.completed",
                payload={
                    "run_id": run_id,
                    "union_mode": batch.source_config.union_mode,
                    **merge_stats,
                    "staged_row_count": len(batch.staged_frame),
                    "current_row_count": len(batch.current_frame),
                },
            )
            store.prune_old_staging_data()
            connection.commit()
        except Exception as exc:
            connection.rollback()
            store.finalize_ingestion_run(
                run_id=run_id,
                status="failed",
                error_message=str(exc),
            )
            store.log_event(
                event_type="task_sync.failed",
                payload={
                    "run_id": run_id,
                    "error": str(exc),
                    "union_mode": batch.source_config.union_mode,
                },
            )
            connection.commit()
            raise

    summary = SyncSummary(
        run_id=run_id,
        source_count=batch.source_count,
        frame_count=batch.frame_count,
        staged_row_count=len(batch.staged_frame),
        current_row_count=len(batch.current_frame),
        union_mode=batch.source_config.union_mode,
        inserted_count=merge_stats["inserted_count"],
        updated_count=merge_stats["updated_count"],
        deleted_count=merge_stats["deleted_count"],
        unchanged_count=merge_stats["unchanged_count"],
    )
    _logger.info(summary.describe())
    return summary
