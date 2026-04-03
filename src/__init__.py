from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

__all__ = [
    "Task",
    "build_dashboard_html",
    "load_task_batch",
    "load_tasks",
    "resolve_request_authorization",
    "run_auto_sync",
    "sync_to_database",
    "sync_to_db",
    "validate_and_clean",
]

__version__ = "0.6.0"


if TYPE_CHECKING:
    from src.domain.tasks import Task
    from src.application.auth_service import resolve_request_authorization
    from src.presentation.dashboard import build_dashboard_html
    from src.application.auto_sync import run_auto_sync
    from src.application.task_sync import sync_to_database
    from src.application.task_workflow import load_task_batch, load_tasks
    from src.schema import validate_and_clean
    from src.sync_to_db import sync_to_db


def __getattr__(name: str) -> Any:
    export_map = {
        "Task": ("src.domain.tasks", "Task"),
        "build_dashboard_html": ("src.presentation.dashboard", "build_dashboard_html"),
        "load_task_batch": ("src.application.task_workflow", "load_task_batch"),
        "load_tasks": ("src.application.task_workflow", "load_tasks"),
        "resolve_request_authorization": ("src.application.auth_service", "resolve_request_authorization"),
        "run_auto_sync": ("src.application.auto_sync", "run_auto_sync"),
        "sync_to_database": ("src.application.task_sync", "sync_to_database"),
        "sync_to_db": ("src.sync_to_db", "sync_to_db"),
        "validate_and_clean": ("src.schema", "validate_and_clean"),
    }

    if name not in export_map:
        raise AttributeError(f"module 'src' has no attribute {name!r}")

    module_name, attribute_name = export_map[name]
    module = import_module(module_name)
    return getattr(module, attribute_name)
