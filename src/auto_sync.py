from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import time
import traceback
from typing import Any

from src.loader import detect_source_kind, expand_source_specs, parse_source_config
from src.sync_to_db import sync_to_db


def env_flag(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y", "on"}


SYNC_SOURCE_INPUT = os.getenv(
    "SYNC_SOURCE_CONFIG",
    os.getenv("SYNC_SOURCE", "/app/data"),
)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://stm_user:stm_password@db:5432/strategic_tasks",
)
SYNC_POLL_SECONDS = max(
    5,
    int(os.getenv("SYNC_POLL_SECONDS", os.getenv("SYNC_INTERVAL_SECONDS", "30"))),
)
SYNC_REFRESH_SECONDS = max(
    SYNC_POLL_SECONDS,
    int(os.getenv("SYNC_REFRESH_SECONDS", "1800")),
)
SYNC_RETRY_SECONDS = max(
    SYNC_POLL_SECONDS,
    int(os.getenv("SYNC_RETRY_SECONDS", "120")),
)
SYNC_VERBOSE_IDLE = env_flag("SYNC_VERBOSE_IDLE", "false")


@dataclass(slots=True)
class SourceSnapshot:
    fingerprint: str
    source_count: int
    local_file_count: int
    volatile_source_count: int
    tracked_paths: list[str]
    details: dict[str, Any]


def is_local_path(source_value: str) -> bool:
    return not source_value.startswith(
        ("http://", "https://", "postgres://", "postgresql://")
    )


def build_source_snapshot(source_input: Any) -> SourceSnapshot:
    config = parse_source_config(source_input)
    expanded_sources = expand_source_specs(list(config["sources"]))

    source_states: list[dict[str, Any]] = []
    tracked_paths: list[str] = []
    local_file_count = 0
    volatile_source_count = 0

    for spec in expanded_sources:
        source_value = str(spec["source"])
        source_kind = detect_source_kind(source_value)

        state: dict[str, Any] = {
            "source": source_value,
            "source_name": str(spec["source_name"]),
            "source_kind": source_kind,
            "source_priority": int(spec.get("source_priority", 100)),
            "source_order": int(spec.get("source_order", 0)),
            "sheet_name": spec.get("sheet_name"),
            "all_sheets": bool(spec.get("all_sheets", False)),
        }

        if is_local_path(source_value):
            source_path = Path(source_value)
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
        "requested_sources": config["sources"],
        "union_mode": config.get("union_mode", "union"),
        "expanded_sources": source_states,
    }
    fingerprint = hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()

    return SourceSnapshot(
        fingerprint=fingerprint,
        source_count=len(expanded_sources),
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
) -> str | None:
    if last_synced_fingerprint is None:
        return "startup"

    if snapshot.fingerprint != last_synced_fingerprint:
        return "source-change"

    if (
        last_attempt_failed
        and last_attempt_at is not None
        and (now - last_attempt_at) >= SYNC_RETRY_SECONDS
    ):
        return "retry-after-failure"

    if last_success_at is not None and (now - last_success_at) >= SYNC_REFRESH_SECONDS:
        return "scheduled-refresh"

    return None


def run_auto_sync(source_input: Any = SYNC_SOURCE_INPUT, database_url: str = DATABASE_URL) -> None:
    print(
        "Auto sync started. "
        f"Target={describe_sync_target(source_input)} "
        f"Poll={SYNC_POLL_SECONDS}s Refresh={SYNC_REFRESH_SECONDS}s Retry={SYNC_RETRY_SECONDS}s"
    )

    last_synced_fingerprint: str | None = None
    last_success_at: float | None = None
    last_attempt_at: float | None = None
    last_attempt_failed = False

    while True:
        now = time.monotonic()

        try:
            snapshot = build_source_snapshot(source_input)
        except Exception as exc:
            print(f"Auto sync source scan failed: {exc}")
            traceback.print_exc()
            time.sleep(SYNC_RETRY_SECONDS)
            continue

        reason = determine_sync_reason(
            snapshot=snapshot,
            last_synced_fingerprint=last_synced_fingerprint,
            last_success_at=last_success_at,
            last_attempt_at=last_attempt_at,
            last_attempt_failed=last_attempt_failed,
            now=now,
        )

        if reason is None:
            if SYNC_VERBOSE_IDLE:
                print(
                    "Auto sync idle. "
                    f"Sources={snapshot.source_count} Files={snapshot.local_file_count} "
                    f"Volatile={snapshot.volatile_source_count}"
                )
            time.sleep(SYNC_POLL_SECONDS)
            continue

        print(
            "Auto sync triggered. "
            f"Reason={reason} Sources={snapshot.source_count} Files={snapshot.local_file_count} "
            f"Volatile={snapshot.volatile_source_count}"
        )

        last_attempt_at = now
        try:
            sync_to_db(source_input, database_url)
            last_synced_fingerprint = snapshot.fingerprint
            last_success_at = time.monotonic()
            last_attempt_failed = False
        except Exception as exc:
            last_attempt_failed = True
            print(f"Auto sync failed: {exc}")
            traceback.print_exc()
            time.sleep(SYNC_RETRY_SECONDS)
            continue

        time.sleep(SYNC_POLL_SECONDS)


def main() -> None:
    run_auto_sync()


if __name__ == "__main__":
    main()
