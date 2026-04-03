from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

__all__ = [
    "COLUMN_ALIASES",
    "CURRENT_COLUMNS",
    "REQUIRED_COLUMNS",
    "ResolvedSourceSpec",
    "SOURCE_METADATA_COLUMNS",
    "STAGED_COLUMNS",
    "SourceList",
    "SourceSpec",
    "TaskSourceConfig",
    "UserAccessRepository",
    "TaskWarehouseStore",
    "build_app_service_login_url",
    "build_app_service_logout_url",
    "decode_client_principal",
    "detect_source_kind",
    "empty_current_frame",
    "empty_staged_frame",
    "expand_source_specs",
    "frame_to_tasks",
    "get_default_azure_credential",
    "has_azure_identity_support",
    "is_database_url",
    "load_tasks_from_database",
    "open_user_access_repository",
    "normalize_task_frame",
    "parse_source_config",
    "parse_app_service_user",
    "read_source_spec_to_frames",
    "resolve_current_frame",
]


if TYPE_CHECKING:
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
    from src.infrastructure.task_store import TaskWarehouseStore, load_tasks_from_database
    from src.infrastructure.auth.app_service import (
        build_app_service_login_url,
        build_app_service_logout_url,
        decode_client_principal,
        parse_app_service_user,
    )
    from src.infrastructure.azure.credentials import (
        get_default_azure_credential,
        has_azure_identity_support,
    )
    from src.infrastructure.user_repository import (
        UserAccessRepository,
        open_user_access_repository,
    )


def __getattr__(name: str) -> Any:
    export_map = {
        "ResolvedSourceSpec": ("src.infrastructure.sources", "ResolvedSourceSpec"),
        "SourceList": ("src.infrastructure.sources", "SourceList"),
        "SourceSpec": ("src.infrastructure.sources", "SourceSpec"),
        "TaskSourceConfig": ("src.infrastructure.sources", "TaskSourceConfig"),
        "detect_source_kind": ("src.infrastructure.sources", "detect_source_kind"),
        "expand_source_specs": ("src.infrastructure.sources", "expand_source_specs"),
        "is_database_url": ("src.infrastructure.sources", "is_database_url"),
        "parse_source_config": ("src.infrastructure.sources", "parse_source_config"),
        "read_source_spec_to_frames": ("src.infrastructure.sources", "read_source_spec_to_frames"),
        "COLUMN_ALIASES": ("src.infrastructure.task_frames", "COLUMN_ALIASES"),
        "CURRENT_COLUMNS": ("src.infrastructure.task_frames", "CURRENT_COLUMNS"),
        "REQUIRED_COLUMNS": ("src.infrastructure.task_frames", "REQUIRED_COLUMNS"),
        "SOURCE_METADATA_COLUMNS": ("src.infrastructure.task_frames", "SOURCE_METADATA_COLUMNS"),
        "STAGED_COLUMNS": ("src.infrastructure.task_frames", "STAGED_COLUMNS"),
        "empty_current_frame": ("src.infrastructure.task_frames", "empty_current_frame"),
        "empty_staged_frame": ("src.infrastructure.task_frames", "empty_staged_frame"),
        "frame_to_tasks": ("src.infrastructure.task_frames", "frame_to_tasks"),
        "normalize_task_frame": ("src.infrastructure.task_frames", "normalize_task_frame"),
        "resolve_current_frame": ("src.infrastructure.task_frames", "resolve_current_frame"),
        "build_app_service_login_url": ("src.infrastructure.auth.app_service", "build_app_service_login_url"),
        "build_app_service_logout_url": ("src.infrastructure.auth.app_service", "build_app_service_logout_url"),
        "decode_client_principal": ("src.infrastructure.auth.app_service", "decode_client_principal"),
        "parse_app_service_user": ("src.infrastructure.auth.app_service", "parse_app_service_user"),
        "get_default_azure_credential": ("src.infrastructure.azure.credentials", "get_default_azure_credential"),
        "has_azure_identity_support": ("src.infrastructure.azure.credentials", "has_azure_identity_support"),
        "UserAccessRepository": ("src.infrastructure.user_repository", "UserAccessRepository"),
        "open_user_access_repository": ("src.infrastructure.user_repository", "open_user_access_repository"),
        "TaskWarehouseStore": ("src.infrastructure.task_store", "TaskWarehouseStore"),
        "load_tasks_from_database": ("src.infrastructure.task_store", "load_tasks_from_database"),
    }

    if name not in export_map:
        raise AttributeError(f"module 'src.infrastructure' has no attribute {name!r}")

    module_name, attribute_name = export_map[name]
    module = import_module(module_name)
    return getattr(module, attribute_name)
