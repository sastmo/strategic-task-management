from __future__ import annotations

import hashlib
import json
from typing import Final

import pandas as pd

from src.domain.tasks import (
    Task,
    build_business_key,
    normalize_owner,
    normalize_status,
    normalize_union_mode,
    slugify,
    text_or_blank,
    to_bool,
    to_paused_bool,
)

REQUIRED_COLUMNS: Final[list[str]] = [
    "name",
    "current_impact",
    "future_impact",
    "progress",
]

SOURCE_METADATA_COLUMNS: Final[list[str]] = [
    "source_name",
    "source_kind",
    "source_sheet",
    "source_path",
    "source_priority",
    "source_order",
    "source_row_number",
]

STAGED_COLUMNS: Final[list[str]] = [
    "business_key",
    "source_task_id",
    "name",
    "owner",
    "current_impact",
    "future_impact",
    "progress",
    "done",
    "paused",
    "record_hash",
    *SOURCE_METADATA_COLUMNS,
]

CURRENT_COLUMNS: Final[list[str]] = ["record_id", *STAGED_COLUMNS]

COLUMN_ALIASES: Final[dict[str, str]] = {
    "id": "id",
    "task id": "id",
    "task_id": "id",
    "task code": "id",
    "name": "name",
    "task": "name",
    "task name": "name",
    "task_name": "name",
    "title": "name",
    "owner": "owner",
    "department": "owner",
    "dept": "owner",
    "team": "owner",
    "group": "owner",
    "currentimpact": "current_impact",
    "current impact": "current_impact",
    "current_impact": "current_impact",
    "impact current": "current_impact",
    "futureimpact": "future_impact",
    "future impact": "future_impact",
    "future_impact": "future_impact",
    "impact future": "future_impact",
    "progress": "progress",
    "completion": "progress",
    "percent complete": "progress",
    "done": "done",
    "completed": "done",
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


def empty_staged_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=STAGED_COLUMNS)


def empty_current_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=CURRENT_COLUMNS)


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map: dict[object, str] = {}

    for column in df.columns:
        column_name = str(column)
        if column_name in SOURCE_METADATA_COLUMNS:
            rename_map[column] = column_name
            continue

        clean_name = column_name.strip().replace("-", " ").replace("_", " ").lower()
        rename_map[column] = COLUMN_ALIASES.get(
            clean_name,
            COLUMN_ALIASES.get(clean_name.replace(" ", ""), column_name.strip()),
        )

    return df.rename(columns=rename_map)


def ensure_source_metadata(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    defaults = {
        "source_name": "default",
        "source_kind": "unknown",
        "source_sheet": "",
        "source_path": "",
        "source_priority": 100,
        "source_order": 0,
        "source_row_number": 0,
    }

    for column, default_value in defaults.items():
        if column not in df.columns:
            df[column] = default_value

    for column in ("source_name", "source_kind", "source_sheet", "source_path"):
        df[column] = df[column].apply(text_or_blank)
        if column == "source_name":
            df[column] = df[column].replace("", "default")
        if column == "source_kind":
            df[column] = df[column].replace("", "unknown")

    for column, default_value in (
        ("source_priority", 100),
        ("source_order", 0),
        ("source_row_number", 0),
    ):
        df[column] = (
            pd.to_numeric(df[column], errors="coerce")
            .fillna(default_value)
            .round()
            .astype(int)
        )

    return df


def assign_source_task_ids(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    generated_counts: dict[str, int] = {}
    source_task_ids: list[str] = []

    for raw_id, task_name in zip(df["id"].tolist(), df["name"].tolist(), strict=False):
        source_task_id = text_or_blank(raw_id)
        if source_task_id:
            source_task_ids.append(source_task_id)
            continue

        base = slugify(task_name, "task")
        count = generated_counts.get(base, 0) + 1
        generated_counts[base] = count
        source_task_ids.append(base if count == 1 else f"{base}-{count}")

    df["source_task_id"] = source_task_ids
    return df


def build_record_hash(row: pd.Series) -> str:
    payload = {
        "business_key": str(row["business_key"]),
        "source_task_id": str(row["source_task_id"]),
        "name": str(row["name"]),
        "owner": str(row["owner"]),
        "current_impact": int(row["current_impact"]),
        "future_impact": int(row["future_impact"]),
        "progress": int(row["progress"]),
        "done": bool(row["done"]),
        "paused": bool(row["paused"]),
        "source_sheet": str(row["source_sheet"]),
        "source_path": str(row["source_path"]),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def normalize_task_frame(df: pd.DataFrame) -> pd.DataFrame:
    df = ensure_source_metadata(standardize_columns(df).copy())

    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Missing columns after normalization: {', '.join(missing)}")

    if "id" not in df.columns:
        df["id"] = ""

    if "owner" not in df.columns:
        df["owner"] = ""

    if "paused" not in df.columns:
        df["paused"] = False

    if "done" not in df.columns:
        df["done"] = False

    if "status" not in df.columns:
        df["status"] = ""

    df["id"] = df["id"].apply(text_or_blank)
    df["name"] = df["name"].apply(text_or_blank)
    df["owner"] = df["owner"].apply(normalize_owner)

    for column in ("current_impact", "future_impact", "progress"):
        df[column] = (
            pd.to_numeric(df[column], errors="coerce")
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
    if df.empty:
        return empty_staged_frame()

    df = assign_source_task_ids(df)
    df["business_key"] = df.apply(
        lambda row: build_business_key(
            row["source_name"],
            row["owner"],
            row["source_task_id"],
        ),
        axis=1,
    )
    df["record_hash"] = df.apply(build_record_hash, axis=1)

    return df[STAGED_COLUMNS]


def resolve_current_frame(df: pd.DataFrame, union_mode: object = "union") -> pd.DataFrame:
    if df.empty:
        return empty_current_frame()

    mode = normalize_union_mode(union_mode)
    current = df.copy().sort_values(
        by=[
            "source_priority",
            "source_order",
            "source_row_number",
            "business_key",
            "record_hash",
        ],
        ascending=[False, True, True, True, True],
        kind="stable",
    )

    if mode == "union":
        current = current.drop_duplicates(subset=["business_key"], keep="first").copy()
        current.insert(0, "record_id", current["business_key"])
        return current[CURRENT_COLUMNS]

    seen: dict[str, int] = {}
    record_ids: list[str] = []

    for business_key in current["business_key"].tolist():
        count = seen.get(business_key, 0) + 1
        seen[business_key] = count
        record_ids.append(business_key if count == 1 else f"{business_key}::dup{count}")

    current = current.copy()
    current.insert(0, "record_id", record_ids)
    return current[CURRENT_COLUMNS]


def frame_to_tasks(df: pd.DataFrame) -> list[Task]:
    record_id_column = "record_id" if "record_id" in df.columns else "id"

    return [
        Task(
            id=str(row[record_id_column]),
            name=str(row["name"]),
            owner=str(row["owner"]),
            current_impact=int(row["current_impact"]),
            future_impact=int(row["future_impact"]),
            progress=int(row["progress"]),
            done=bool(row["done"]),
            paused=bool(row["paused"]),
            completed_at=(
                row["completed_at"]
                if "completed_at" in df.columns and pd.notna(row["completed_at"])
                else None
            ),
        )
        for _, row in df.iterrows()
    ]
