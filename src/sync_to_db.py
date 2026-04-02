from __future__ import annotations

import os
from pathlib import Path

import psycopg

from src.loader import read_source_to_frame
from src.schema import validate_and_clean


DEFAULT_SYNC_SOURCE = os.getenv(
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
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    conn.commit()


def sync_to_db(source: str, database_url: str) -> None:
    df = validate_and_clean(read_source_to_frame(source))

    with psycopg.connect(database_url) as conn:
        ensure_schema(conn)

        with conn.cursor() as cur:
            for _, row in df.iterrows():
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
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        owner = EXCLUDED.owner,
                        current_impact = EXCLUDED.current_impact,
                        future_impact = EXCLUDED.future_impact,
                        progress = EXCLUDED.progress,
                        done = EXCLUDED.done,
                        paused = EXCLUDED.paused,
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
                    ),
                )

        conn.commit()

    print(f"Synced {len(df)} task(s) into PostgreSQL.")


if __name__ == "__main__":
    sync_to_db(DEFAULT_SYNC_SOURCE, DATABASE_URL)