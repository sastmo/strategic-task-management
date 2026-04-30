"""Compatibility exports for the loader/workflow API.

The main implementation now lives under src.application and src.infrastructure.
"""

from __future__ import annotations

from src.application.task_workflow import (
    LoadedTaskBatch,
    load_task_batch,
    load_tasks,
    load_tasks_from_database,
    read_source_to_frame,
    read_sources_to_frame,
)
from src.infrastructure.sources import (
    ResolvedSourceSpec,
    SourceList,
    SourceSpec,
    TaskSourceConfig,
    detect_source_kind,
    expand_source_specs,
    is_database_url,
    parse_source_config,
    read_source_spec_to_frames,
)
from src.infrastructure.graph.client import (
    GraphAuthSettings,
    GraphDownloadedFile,
    GraphFileClient,
    load_graph_auth_settings,
    parse_site_url,
)
from src.infrastructure.task_frames import frame_to_tasks

load_tasks_from_db = load_tasks_from_database

__all__ = [
    "LoadedTaskBatch",
    "ResolvedSourceSpec",
    "SourceList",
    "SourceSpec",
    "TaskSourceConfig",
    "GraphAuthSettings",
    "GraphDownloadedFile",
    "GraphFileClient",
    "detect_source_kind",
    "expand_source_specs",
    "frame_to_tasks",
    "is_database_url",
    "load_task_batch",
    "load_graph_auth_settings",
    "load_tasks",
    "load_tasks_from_database",
    "load_tasks_from_db",
    "parse_source_config",
    "parse_site_url",
    "read_source_spec_to_frames",
    "read_source_to_frame",
    "read_sources_to_frame",
]
