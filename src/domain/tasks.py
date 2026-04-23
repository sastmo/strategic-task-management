from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Final, Literal

UnionMode = Literal["union", "union_all"]
TaskStatus = Literal["active", "done", "paused"]

SUPPORTED_UNION_MODES: Final[tuple[UnionMode, ...]] = ("union", "union_all")


@dataclass(slots=True, frozen=True)
class Task:
    id: str
    name: str
    owner: str
    current_impact: int
    future_impact: int
    progress: int
    done: bool = False
    paused: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "progress", max(0, min(100, self.progress)))

    @property
    def currentImpact(self) -> int:
        return self.current_impact

    @property
    def futureImpact(self) -> int:
        return self.future_impact


def text_or_blank(value: object) -> str:
    if value is None:
        return ""

    try:
        if value != value:
            return ""
    except Exception:
        pass

    return str(value).strip()


def to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value

    return text_or_blank(value).lower() in {
        "true",
        "1",
        "yes",
        "y",
        "done",
        "complete",
        "completed",
    }


def to_paused_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value

    return text_or_blank(value).lower() in {
        "true",
        "1",
        "yes",
        "y",
        "paused",
        "pause",
        "on hold",
        "hold",
    }


def normalize_status(value: object) -> TaskStatus:
    text = text_or_blank(value).lower()
    if text in {"done", "complete", "completed", "finished"}:
        return "done"
    if text in {"paused", "pause", "on hold", "hold"}:
        return "paused"
    return "active"


def normalize_owner(owner: object) -> str:
    text = text_or_blank(owner)
    return text if text else "Unassigned"


def normalize_union_mode(value: object) -> UnionMode:
    mode = text_or_blank(value).lower() or "union"
    if mode not in SUPPORTED_UNION_MODES:
        supported = ", ".join(sorted(SUPPORTED_UNION_MODES))
        raise ValueError(f"Unsupported union mode: {value}. Expected one of: {supported}")
    return mode  # type: ignore[return-value]


def slugify(value: object, default: str = "unknown") -> str:
    text = text_or_blank(value).lower()
    cleaned = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return cleaned or default


def build_business_key(source_name: object, owner: object, source_task_id: object) -> str:
    return "::".join(
        (
            slugify(source_name, "source"),
            slugify(owner, "owner"),
            slugify(source_task_id, "task"),
        )
    )


def is_done(task: Task) -> bool:
    return task.done or task.progress >= 100


def is_paused(task: Task) -> bool:
    return (not is_done(task)) and task.paused


def task_status(task: Task) -> TaskStatus:
    if is_done(task):
        return "done"
    if is_paused(task):
        return "paused"
    return "active"
