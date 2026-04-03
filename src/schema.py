from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import pandas as pd

REQUIRED_COLUMNS: Final[list[str]] = [
    "id",
    "name",
    "owner",
    "currentImpact",
    "futureImpact",
    "progress",
    "done",
]

SOURCE_METADATA_COLUMNS: Final[list[str]] = [
    "_source_name",
    "_source_kind",
    "_source_sheet",
]

COLUMN_ALIASES: Final[dict[str, str]] = {
    "id": "id",
    "task id": "id",
    "task_id": "id",
    "name": "name",
    "task": "name",
    "task name": "name",
    "task_name": "name",
    "owner": "owner",
    "department": "owner",
    "team": "owner",
    "currentimpact": "currentImpact",
    "current impact": "currentImpact",
    "current_impact": "currentImpact",
    "futureimpact": "futureImpact",
    "future impact": "futureImpact",
    "future_impact": "futureImpact",
    "progress": "progress",
    "completion": "progress",
    "done": "done",
    "status_done": "done",
    "paused": "paused",
    "pause": "paused",
    "on hold": "paused",
    "on_hold": "paused",
    "hold": "paused",
    "status": "status",
    "task status": "status",
    "task_status": "status",
}


@dataclass(slots=True)
class Task:
    id: str
    name: str
    owner: str
    currentImpact: int
    futureImpact: int
    progress: int
    done: bool = False
    paused: bool = False


def to_bool(value) -> bool:
    return str(value).strip().lower() in {
        "true",
        "1",
        "yes",
        "y",
        "done",
        "completed",
    }


def to_paused_bool(value) -> bool:
    return str(value).strip().lower() in {
        "true",
        "1",
        "yes",
        "y",
        "paused",
        "pause",
        "on hold",
        "hold",
    }


def normalize_status(value) -> str:
    text = str(value).strip().lower()
    if text in {"done", "complete", "completed", "finished"}:
        return "done"
    if text in {"paused", "pause", "on hold", "hold"}:
        return "paused"
    return "active"


def normalize_owner(owner: str) -> str:
    owner = str(owner).strip()
    return owner if owner else "Unassigned"


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map: dict[str, str] = {}

    for col in df.columns:
        if str(col).startswith("_source_"):
            rename_map[col] = str(col)
            continue

        clean = str(col).strip().replace("-", " ").replace("_", " ").lower()
        rename_map[col] = COLUMN_ALIASES.get(
            clean,
            COLUMN_ALIASES.get(clean.replace(" ", ""), str(col).strip()),
        )

    return df.rename(columns=rename_map)


def fill_blank_ids(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    counter = 1
    new_ids: list[str] = []

    for value in df["id"].astype(str).str.strip():
        if value:
            new_ids.append(value)
        else:
            new_ids.append(f"task-{counter}")
            counter += 1

    df["id"] = new_ids
    return df


def deduplicate_ids(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if not df["id"].duplicated().any():
        return df

    seen: dict[str, int] = {}
    deduped: list[str] = []

    for value in df["id"]:
        count = seen.get(value, 0) + 1
        seen[value] = count
        deduped.append(value if count == 1 else f"{value}-{count}")

    df["id"] = deduped
    return df


def ensure_source_metadata(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "_source_name" not in df.columns:
        df["_source_name"] = "default"

    if "_source_kind" not in df.columns:
        df["_source_kind"] = "unknown"

    if "_source_sheet" not in df.columns:
        df["_source_sheet"] = ""

    df["_source_name"] = df["_source_name"].astype(str).fillna("default")
    df["_source_kind"] = df["_source_kind"].astype(str).fillna("unknown")
    df["_source_sheet"] = df["_source_sheet"].astype(str).fillna("")

    return df


def validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df).copy()
    df = ensure_source_metadata(df)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            f"Missing columns after normalization: {', '.join(missing)}"
        )

    if "paused" not in df.columns:
        df["paused"] = False

    if "status" not in df.columns:
        df["status"] = ""

    df["id"] = df["id"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df["owner"] = df["owner"].apply(normalize_owner)

    df["currentImpact"] = (
        pd.to_numeric(df["currentImpact"], errors="coerce")
        .fillna(0)
        .clip(0, 100)
        .round()
        .astype(int)
    )
    df["futureImpact"] = (
        pd.to_numeric(df["futureImpact"], errors="coerce")
        .fillna(0)
        .clip(0, 100)
        .round()
        .astype(int)
    )
    df["progress"] = (
        pd.to_numeric(df["progress"], errors="coerce")
        .fillna(0)
        .clip(0, 100)
        .round()
        .astype(int)
    )

    df["status"] = df["status"].apply(normalize_status)

    done_from_status = df["status"].eq("done")
    paused_from_status = df["status"].eq("paused")

    df["done"] = (
        df["done"].apply(to_bool)
        | (df["progress"] >= 100)
        | done_from_status
    )
    df["paused"] = (
        (~df["done"])
        & (df["paused"].apply(to_paused_bool) | paused_from_status)
    )

    df = df[df["name"].ne("")].copy()
    df = fill_blank_ids(df)
    df = deduplicate_ids(df)

    return df[REQUIRED_COLUMNS + ["paused"] + SOURCE_METADATA_COLUMNS]


def is_done(task: Task) -> bool:
    return task.done or task.progress >= 100


def is_paused(task: Task) -> bool:
    return (not is_done(task)) and task.paused


def task_status(task: Task) -> str:
    if is_done(task):
        return "done"
    if is_paused(task):
        return "paused"
    return "active"