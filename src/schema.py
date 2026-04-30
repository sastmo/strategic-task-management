"""Compatibility exports for task normalization and schema helpers.

The canonical frame logic now lives in src.domain and src.infrastructure.task_frames.
"""

from __future__ import annotations

from src.domain.tasks import (
    SUPPORTED_UNION_MODES,
    Task,
    build_business_key,
    is_done,
    is_paused,
    normalize_owner,
    normalize_status,
    normalize_union_mode,
    slugify,
    task_status,
    text_or_blank,
    to_bool,
    to_paused_bool,
)
from src.infrastructure.task_frames import (
    COLUMN_ALIASES,
    CURRENT_COLUMNS,
    REQUIRED_COLUMNS,
    SOURCE_METADATA_COLUMNS,
    STAGED_COLUMNS,
    empty_current_frame,
    empty_staged_frame,
    frame_to_tasks,
    normalize_task_frame,
    resolve_current_frame,
)

validate_and_clean = normalize_task_frame
resolve_current_tasks = resolve_current_frame

__all__ = [
    "COLUMN_ALIASES",
    "CURRENT_COLUMNS",
    "REQUIRED_COLUMNS",
    "SOURCE_METADATA_COLUMNS",
    "STAGED_COLUMNS",
    "SUPPORTED_UNION_MODES",
    "Task",
    "build_business_key",
    "empty_current_frame",
    "empty_staged_frame",
    "frame_to_tasks",
    "is_done",
    "is_paused",
    "normalize_owner",
    "normalize_status",
    "normalize_union_mode",
    "resolve_current_tasks",
    "slugify",
    "task_status",
    "text_or_blank",
    "to_bool",
    "to_paused_bool",
    "validate_and_clean",
]
