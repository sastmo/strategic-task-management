from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.application.settings import AutoSyncSettings, load_auto_sync_settings
from src.infrastructure.sources import (
    detect_source_kind,
    expand_source_specs,
    parse_source_config,
)

_logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SourceSnapshot:
    fingerprint: str
    source_count: int
    local_file_count: int
    volatile_source_count: int
    tracked_paths: list[str]
    details: dict[str, Any]


class AutoSyncMonitor:
    def __init__(
        self,
        settings: AutoSyncSettings,
        *,
        sync_function: Callable[[Any, str], Any] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        printer: Callable[[str], None] = print,
    ) -> None:
        self.settings = settings
        self.sync_function = sync_function or _default_sync_function
        self.monotonic = monotonic
        self.sleep = sleep
        self.printer = printer
        self.last_synced_fingerprint: str | None = None
        self.last_success_at: float | None = None
        self.last_attempt_at: float | None = None
        self.last_attempt_failed = False

    def run_forever(self) -> None:
        self.printer(
            "Auto sync started. "
            f"Target={describe_sync_target(self.settings.source_input)} "
            f"Poll={self.settings.poll_seconds}s Refresh={self.settings.refresh_seconds}s "
            f"Retry={self.settings.retry_seconds}s"
        )

        while True:
            now = self.monotonic()

            try:
                snapshot = build_source_snapshot(self.settings.source_input)
            except Exception as exc:
                self.printer(f"Auto sync source scan failed: {exc}")
                _logger.exception("Auto sync source scan failed")
                self.sleep(self.settings.retry_seconds)
                continue

            reason = determine_sync_reason(
                snapshot=snapshot,
                last_synced_fingerprint=self.last_synced_fingerprint,
                last_success_at=self.last_success_at,
                last_attempt_at=self.last_attempt_at,
                last_attempt_failed=self.last_attempt_failed,
                now=now,
                refresh_seconds=self.settings.refresh_seconds,
                retry_seconds=self.settings.retry_seconds,
            )

            if reason is None:
                if self.settings.verbose_idle:
                    self.printer(
                        "Auto sync idle. "
                        f"Sources={snapshot.source_count} Files={snapshot.local_file_count} "
                        f"Volatile={snapshot.volatile_source_count}"
                    )
                self.sleep(self.settings.poll_seconds)
                continue

            self.printer(
                "Auto sync triggered. "
                f"Reason={reason} Sources={snapshot.source_count} Files={snapshot.local_file_count} "
                f"Volatile={snapshot.volatile_source_count}"
            )

            self.last_attempt_at = now
            try:
                self.sync_function(self.settings.source_input, self.settings.database_url)
                self.last_synced_fingerprint = snapshot.fingerprint
                self.last_success_at = self.monotonic()
                self.last_attempt_failed = False
            except Exception as exc:
                from src.application.task_sync import SyncLockConflict

                if isinstance(exc, SyncLockConflict):
                    self.printer(f"Auto sync skipped: {exc}")
                    self.sleep(self.settings.poll_seconds)
                    continue
                self.last_attempt_failed = True
                self.printer(f"Auto sync failed: {exc}")
                _logger.exception("Auto sync failed")
                self.sleep(self.settings.retry_seconds)
                continue

            self.sleep(self.settings.poll_seconds)


def _default_sync_function(source_input: Any, database_url: str) -> Any:
    from src.application.task_sync import sync_to_database

    return sync_to_database(source_input, database_url)


def is_local_path(source_value: str) -> bool:
    return not source_value.startswith(
        ("http://", "https://", "postgres://", "postgresql://")
    )


def build_source_snapshot(source_input: Any) -> SourceSnapshot:
    source_config = parse_source_config(source_input)
    resolved_sources = expand_source_specs(list(source_config.sources))

    source_states: list[dict[str, Any]] = []
    tracked_paths: list[str] = []
    local_file_count = 0
    volatile_source_count = 0

    for source in resolved_sources:
        source_kind = detect_source_kind(source.source)
        state: dict[str, Any] = {
            "source": source.source,
            "source_name": source.source_name,
            "source_kind": source_kind,
            "source_priority": source.source_priority,
            "source_order": source.source_order,
            "sheet_name": source.sheet_name,
            "all_sheets": source.all_sheets,
        }

        if is_local_path(source.source):
            from pathlib import Path

            source_path = Path(source.source)
            state["exists"] = source_path.exists()
            if source_path.exists():
                stat = source_path.stat()
                state["size"] = stat.st_size
                state["mtime_ns"] = stat.st_mtime_ns
                local_file_count += 1
                tracked_paths.append(str(source_path))
            else:
                state["size"] = None
                state["mtime_ns"] = None
        else:
            volatile_source_count += 1
            state["volatile"] = True

        source_states.append(state)

    payload = {
        "requested_sources": source_config.sources,
        "union_mode": source_config.union_mode,
        "expanded_sources": source_states,
    }
    fingerprint = hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()

    return SourceSnapshot(
        fingerprint=fingerprint,
        source_count=len(resolved_sources),
        local_file_count=local_file_count,
        volatile_source_count=volatile_source_count,
        tracked_paths=tracked_paths,
        details=payload,
    )


def describe_sync_target(source_input: Any) -> str:
    text = str(source_input).strip()
    if len(text) <= 140:
        return text
    return f"{text[:137]}..."


def determine_sync_reason(
    *,
    snapshot: SourceSnapshot,
    last_synced_fingerprint: str | None,
    last_success_at: float | None,
    last_attempt_at: float | None,
    last_attempt_failed: bool,
    now: float,
    refresh_seconds: int,
    retry_seconds: int,
) -> str | None:
    if last_synced_fingerprint is None:
        return "startup"

    if snapshot.fingerprint != last_synced_fingerprint:
        return "source_change"

    if (
        last_attempt_failed
        and last_attempt_at is not None
        and (now - last_attempt_at) >= retry_seconds
    ):
        return "retry_after_failure"

    if last_success_at is not None and (now - last_success_at) >= refresh_seconds:
        return "scheduled_refresh"

    return None


def run_auto_sync(
    source_input: Any | None = None,
    database_url: str | None = None,
) -> None:
    from pathlib import Path

    default_source = str(Path(__file__).resolve().parents[2] / "data")
    settings = load_auto_sync_settings(default_source)

    if source_input is not None or database_url is not None:
        settings = AutoSyncSettings(
            source_input=str(source_input if source_input is not None else settings.source_input),
            database_url=str(database_url if database_url is not None else settings.database_url),
            poll_seconds=settings.poll_seconds,
            refresh_seconds=settings.refresh_seconds,
            retry_seconds=settings.retry_seconds,
            verbose_idle=settings.verbose_idle,
        )

    monitor = AutoSyncMonitor(settings)
    monitor.run_forever()


if __name__ == "__main__":
    run_auto_sync()
