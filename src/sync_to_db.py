from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import psycopg

from src.loader import load_task_batch
from src.warehouse import (
    create_ingestion_run,
    ensure_database_objects,
    finalize_ingestion_run,
    log_event,
    merge_staged_data,
    prune_old_staging_data,
    stage_task_data,
)


DEFAULT_SYNC_SOURCE: Any = os.getenv(
    "SYNC_SOURCE",
    str(Path(__file__).resolve().parent.parent / "data"),
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://stm_user:stm_password@db:5432/strategic_tasks",
)


def sync_to_db(source: Any, database_url: str) -> dict[str, int | str]:
    batch = load_task_batch(source)
    union_mode = str(batch.source_config["union_mode"])
    source_names = sorted(
        {
            str(spec["source_name"])
            for spec in batch.source_config["sources"]
            if str(spec.get("source_name", "")).strip()
        }
    )

    with psycopg.connect(database_url) as conn:
        ensure_database_objects(conn)

        run_id = create_ingestion_run(
            conn,
            source_config=batch.source_config,
            union_mode=union_mode,
            source_count=batch.source_count,
            frame_count=batch.frame_count,
            staged_row_count=len(batch.staged_frame),
            current_row_count=len(batch.current_frame),
        )
        log_event(
            conn,
            event_type="task_sync.started",
            payload={
                "run_id": run_id,
                "source_count": batch.source_count,
                "frame_count": batch.frame_count,
                "staged_row_count": len(batch.staged_frame),
                "current_row_count": len(batch.current_frame),
                "union_mode": union_mode,
            },
        )
        conn.commit()

        try:
            stage_task_data(
                conn,
                run_id=run_id,
                staged_frame=batch.staged_frame,
                current_frame=batch.current_frame,
            )
            merge_stats = merge_staged_data(
                conn,
                run_id=run_id,
                source_names=source_names,
            )
            finalize_ingestion_run(
                conn,
                run_id=run_id,
                status="success",
                inserted_count=merge_stats["inserted_count"],
                updated_count=merge_stats["updated_count"],
                deleted_count=merge_stats["deleted_count"],
                unchanged_count=merge_stats["unchanged_count"],
            )
            log_event(
                conn,
                event_type="task_sync.completed",
                payload={
                    "run_id": run_id,
                    "union_mode": union_mode,
                    **merge_stats,
                    "staged_row_count": len(batch.staged_frame),
                    "current_row_count": len(batch.current_frame),
                },
            )
            prune_old_staging_data(conn)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            finalize_ingestion_run(
                conn,
                run_id=run_id,
                status="failed",
                error_message=str(exc),
            )
            log_event(
                conn,
                event_type="task_sync.failed",
                payload={
                    "run_id": run_id,
                    "error": str(exc),
                    "union_mode": union_mode,
                },
            )
            conn.commit()
            raise

    summary = {
        "run_id": run_id,
        "source_count": batch.source_count,
        "frame_count": batch.frame_count,
        "staged_row_count": len(batch.staged_frame),
        "current_row_count": len(batch.current_frame),
        "union_mode": union_mode,
        **merge_stats,
    }
    print(
        "Task sync complete "
        f"(run_id={run_id}, sources={batch.source_count}, frames={batch.frame_count}, "
        f"staged={len(batch.staged_frame)}, current={len(batch.current_frame)}, "
        f"inserted={merge_stats['inserted_count']}, updated={merge_stats['updated_count']}, "
        f"deleted={merge_stats['deleted_count']}, unchanged={merge_stats['unchanged_count']})."
    )
    return summary


if __name__ == "__main__":
    sync_to_db(DEFAULT_SYNC_SOURCE, DATABASE_URL)
