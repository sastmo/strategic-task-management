from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

__all__ = [
    "Task",
    "build_dashboard_html",
    "load_task_batch",
    "load_tasks",
    "run_auto_sync",
    "sync_to_db",
    "validate_and_clean",
]

__version__ = "0.4.0"


if TYPE_CHECKING:
    from src.auto_sync import run_auto_sync
    from src.dashboard import build_dashboard_html
    from src.loader import load_task_batch, load_tasks
    from src.schema import Task, validate_and_clean
    from src.sync_to_db import sync_to_db


def __getattr__(name: str) -> Any:
    export_map = {
        "Task": ("src.schema", "Task"),
        "build_dashboard_html": ("src.dashboard", "build_dashboard_html"),
        "load_task_batch": ("src.loader", "load_task_batch"),
        "load_tasks": ("src.loader", "load_tasks"),
        "run_auto_sync": ("src.auto_sync", "run_auto_sync"),
        "sync_to_db": ("src.sync_to_db", "sync_to_db"),
        "validate_and_clean": ("src.schema", "validate_and_clean"),
    }

    if name not in export_map:
        raise AttributeError(f"module 'src' has no attribute {name!r}")

    module_name, attribute_name = export_map[name]
    module = import_module(module_name)
    return getattr(module, attribute_name)
