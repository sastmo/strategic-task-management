from __future__ import annotations

"""Compatibility entrypoint for the task sync workflow."""

from pathlib import Path
from typing import Any

from src.application.settings import load_database_url, load_sync_source_input
from src.application.task_sync import SyncSummary, sync_to_database


DEFAULT_SYNC_SOURCE: Any = load_sync_source_input(
    str(Path(__file__).resolve().parent.parent / "data")
)
DATABASE_URL = load_database_url()


def sync_to_db(source: Any, database_url: str) -> dict[str, int | str]:
    summary = sync_to_database(source, database_url)
    return summary.as_dict()


if __name__ == "__main__":
    sync_to_database(DEFAULT_SYNC_SOURCE, DATABASE_URL)


__all__ = [
    "DATABASE_URL",
    "DEFAULT_SYNC_SOURCE",
    "SyncSummary",
    "sync_to_database",
    "sync_to_db",
]
