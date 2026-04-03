from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg

from src.loader import read_source_spec_to_frames
from src.schema import validate_and_clean


DEFAULT_SYNC_SOURCE: Any = os.getenv(
    "SYNC_SOURCE",
    str(Path(__file__).resolve().parent.parent / "data" / "tasks.csv"),
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://stm_user:stm_password@db:5432/strategic_tasks",
)


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                owner TEXT NOT NULL,
                current_impact INTEGER NOT NULL,
                future_impact INTEGER NOT NULL,
                progress INTEGER NOT NULL,
                done BOOLEAN NOT NULL DEFAULT FALSE,
                paused BOOLEAN NOT NULL DEFAULT FALSE,
                source_name TEXT NOT NULL DEFAULT 'default',
                source_kind TEXT NOT NULL DEFAULT 'unknown',
                source_sheet TEXT NOT NULL DEFAULT '',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tasks_source
            ON tasks (source_name, source_sheet)
            """
        )

    conn.commit()


def get_frame_source_metadata(raw_df: pd.DataFrame, clean_df: pd.DataFrame) -> tuple[str, str, str]:
    if not clean_df.empty:
        return (
            str(clean_df["_source_name"].iloc[0]),
            str(clean_df["_source_kind"].iloc[0]),
            str(clean_df["_source_sheet"].iloc[0]),
        )

    if not raw_df.empty and "_source_name" in raw_df.columns:
        return (
            str(raw_df["_source_name"].iloc[0]),
            str(raw_df["_source_kind"].iloc[0]) if "_source_kind" in raw_df.columns else "unknown",
            str(raw_df["_source_sheet"].iloc[0]) if "_source_sheet" in raw_df.columns else "",
        )

    return ("default", "unknown", "")


def upsert_rows(
    conn: psycopg.Connection,
    clean_df: pd.DataFrame,
    source_name: str,
    source_kind: str,
    source_sheet: str,
) -> None:
    with conn.cursor() as cur:
        for _, row in clean_df.iterrows():
            cur.execute(
                """
                INSERT INTO tasks (
                    id,
                    name,
                    owner,
                    current_impact,
                    future_impact,
                    progress,
                    done,
                    paused,
                    source_name,
                    source_kind,
                    source_sheet,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    owner = EXCLUDED.owner,
                    current_impact = EXCLUDED.current_impact,
                    future_impact = EXCLUDED.future_impact,
                    progress = EXCLUDED.progress,
                    done = EXCLUDED.done,
                    paused = EXCLUDED.paused,
                    source_name = EXCLUDED.source_name,
                    source_kind = EXCLUDED.source_kind,
                    source_sheet = EXCLUDED.source_sheet,
                    updated_at = NOW()
                """,
                (
                    str(row["id"]),
                    str(row["name"]),
                    str(row["owner"]),
                    int(row["currentImpact"]),
                    int(row["futureImpact"]),
                    int(row["progress"]),
                    bool(row["done"]),
                    bool(row["paused"]),
                    source_name,
                    source_kind,
                    source_sheet,
                ),
            )


def delete_missing_rows_for_source(
    conn: psycopg.Connection,
    incoming_ids: list[str],
    source_name: str,
    source_sheet: str,
) -> None:
    with conn.cursor() as cur:
        if incoming_ids:
            cur.execute(
                """
                DELETE FROM tasks
                WHERE source_name = %s
                  AND source_sheet = %s
                  AND NOT (id = ANY(%s))
                """,
                (source_name, source_sheet, incoming_ids),
            )
        else:
            cur.execute(
                """
                DELETE FROM tasks
                WHERE source_name = %s
                  AND source_sheet = %s
                """,
                (source_name, source_sheet),
            )


def sync_to_db(source: Any, database_url: str) -> None:
    raw_frames = read_source_spec_to_frames(source)

    with psycopg.connect(database_url) as conn:
        ensure_schema(conn)

        total_rows = 0

        for raw_df in raw_frames:
            clean_df = validate_and_clean(raw_df)
            source_name, source_kind, source_sheet = get_frame_source_metadata(raw_df, clean_df)

            incoming_ids = [str(value) for value in clean_df["id"].tolist()]
            upsert_rows(conn, clean_df, source_name, source_kind, source_sheet)
            delete_missing_rows_for_source(conn, incoming_ids, source_name, source_sheet)

            total_rows += len(clean_df)

        conn.commit()

    print(f"Synced {total_rows} task(s) into PostgreSQL across {len(raw_frames)} source frame(s).")


if __name__ == "__main__":
    sync_to_db(DEFAULT_SYNC_SOURCE, DATABASE_URL)