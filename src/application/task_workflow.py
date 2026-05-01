from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.domain.tasks import Task
from src.infrastructure.sources import (
    ResolvedSourceSpec,
    SourceList,
    SourceSpec,
    TaskSourceConfig,
    expand_source_specs,
    is_database_url,
    parse_source_config,
    read_source_spec_to_frames,
)
from src.infrastructure.task_frames import (
    empty_staged_frame,
    frame_to_tasks,
    normalize_task_frame,
    resolve_current_frame,
)


@dataclass(slots=True)
class LoadedTaskBatch:
    source_config: TaskSourceConfig
    resolved_sources: list[ResolvedSourceSpec]
    staged_frame: pd.DataFrame
    current_frame: pd.DataFrame
    frame_count: int
    source_count: int

    def source_config_payload(self) -> dict[str, Any]:
        return {
            "sources": [source.to_payload() for source in self.resolved_sources],
            "union_mode": self.source_config.union_mode,
        }


def load_task_batch(source_input: SourceSpec | SourceList | dict[str, Any]) -> LoadedTaskBatch:
    source_config = parse_source_config(source_input)
    resolved_sources = expand_source_specs(list(source_config.sources))

    normalized_frames: list[pd.DataFrame] = []
    frame_count = 0
    for source in resolved_sources:
        for frame in read_source_spec_to_frames(source):
            normalized_frames.append(normalize_task_frame(frame))
            frame_count += 1
    staged_frame = (
        pd.concat(normalized_frames, ignore_index=True)
        if normalized_frames
        else empty_staged_frame()
    )
    current_frame = resolve_current_frame(
        staged_frame,
        union_mode=source_config.union_mode,
    )

    return LoadedTaskBatch(
        source_config=source_config,
        resolved_sources=resolved_sources,
        staged_frame=staged_frame,
        current_frame=current_frame,
        frame_count=frame_count,
        source_count=len(resolved_sources),
    )


def read_sources_to_frame(sources: SourceSpec | SourceList) -> pd.DataFrame:
    return load_task_batch(sources).staged_frame.copy()


def read_source_to_frame(source: SourceSpec | SourceList) -> pd.DataFrame:
    return read_sources_to_frame(source)


def load_tasks_from_database(database_url: str) -> list[Task]:
    from src.infrastructure.task_store import load_tasks_from_database as _load_tasks

    return _load_tasks(database_url)


def load_tasks(source_input: SourceSpec | SourceList | dict[str, Any]) -> list[Task]:
    if is_database_url(source_input):
        return load_tasks_from_database(str(source_input))

    batch = load_task_batch(source_input)
    return frame_to_tasks(batch.current_frame)
