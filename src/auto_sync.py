from __future__ import annotations

from src.application.auto_sync import (
    AutoSyncMonitor,
    SourceSnapshot,
    build_source_snapshot,
    determine_sync_reason,
    run_auto_sync,
)


def main() -> None:
    run_auto_sync()


if __name__ == "__main__":
    main()


__all__ = [
    "AutoSyncMonitor",
    "SourceSnapshot",
    "build_source_snapshot",
    "determine_sync_reason",
    "main",
    "run_auto_sync",
]
