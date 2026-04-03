from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
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
    "_source_path",
    "_source_priority",
    "_source_order",
    "_source_row_number",
]

STAGED_COLUMNS: Final[list[str]] = [
    "businessKey",
    "sourceTaskId",
    "name",
    "owner",
    "currentImpact",
    "futureImpact",
    "progress",
    "done",
    "paused",
    "recordHash",
    *SOURCE_METADATA_COLUMNS,
]

CURRENT_COLUMNS: Final[list[str]] = ["id", *STAGED_COLUMNS]

SUPPORTED_UNION_MODES: Final[set[str]] = {"union", "union_all"}

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
    "currentimpact": "currentImpact",
    "current impact": "currentImpact",
    "current_impact": "currentImpact",
    "impact current": "currentImpact",
    "futureimpact": "futureImpact",
    "future impact": "futureImpact",
    "future_impact": "futureImpact",
    "impact future": "futureImpact",
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


def empty_staged_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=STAGED_COLUMNS)


def empty_current_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=CURRENT_COLUMNS)


def text_or_blank(value: object) -> str:
    if pd.isna(value):
        return ""
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


def normalize_status(value: object) -> str:
    text = text_or_blank(value).lower()
    if text in {"done", "complete", "completed", "finished"}:
        return "done"
    if text in {"paused", "pause", "on hold", "hold"}:
        return "paused"
    return "active"


def normalize_owner(owner: object) -> str:
    text = text_or_blank(owner)
    return text if text else "Unassigned"


def normalize_union_mode(value: object) -> str:
    mode = text_or_blank(value).lower() or "union"
    if mode not in SUPPORTED_UNION_MODES:
        supported = ", ".join(sorted(SUPPORTED_UNION_MODES))
        raise ValueError(f"Unsupported union mode: {value}. Expected one of: {supported}")
    return mode


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


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map: dict[object, str] = {}

    for col in df.columns:
        column_name = str(col)
        if column_name.startswith("_source_"):
            rename_map[col] = column_name
            continue

        clean = column_name.strip().replace("-", " ").replace("_", " ").lower()
        rename_map[col] = COLUMN_ALIASES.get(
            clean,
            COLUMN_ALIASES.get(clean.replace(" ", ""), column_name.strip()),
        )

    return df.rename(columns=rename_map)


def ensure_source_metadata(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    defaults = {
        "_source_name": "default",
        "_source_kind": "unknown",
        "_source_sheet": "",
        "_source_path": "",
        "_source_priority": 100,
        "_source_order": 0,
        "_source_row_number": 0,
    }

    for column, default_value in defaults.items():
        if column not in df.columns:
            df[column] = default_value

    for column in ("_source_name", "_source_kind", "_source_sheet", "_source_path"):
        df[column] = df[column].apply(text_or_blank)
        if column == "_source_name":
            df[column] = df[column].replace("", "default")
        if column == "_source_kind":
            df[column] = df[column].replace("", "unknown")

    for column, default_value in (
        ("_source_priority", 100),
        ("_source_order", 0),
        ("_source_row_number", 0),
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

    df["sourceTaskId"] = source_task_ids
    return df


def build_record_hash(row: pd.Series) -> str:
    payload = {
        "businessKey": str(row["businessKey"]),
        "sourceTaskId": str(row["sourceTaskId"]),
        "name": str(row["name"]),
        "owner": str(row["owner"]),
        "currentImpact": int(row["currentImpact"]),
        "futureImpact": int(row["futureImpact"]),
        "progress": int(row["progress"]),
        "done": bool(row["done"]),
        "paused": bool(row["paused"]),
        "sourceSheet": str(row["_source_sheet"]),
        "sourcePath": str(row["_source_path"]),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df).copy()
    df = ensure_source_metadata(df)

    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Missing columns after normalization: {', '.join(missing)}")

    if "paused" not in df.columns:
        df["paused"] = False

    if "status" not in df.columns:
        df["status"] = ""

    df["id"] = df["id"].apply(text_or_blank)
    df["name"] = df["name"].apply(text_or_blank)
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
    if df.empty:
        return empty_staged_frame()

    df = assign_source_task_ids(df)
    df["businessKey"] = df.apply(
        lambda row: build_business_key(
            row["_source_name"],
            row["owner"],
            row["sourceTaskId"],
        ),
        axis=1,
    )
    df["recordHash"] = df.apply(build_record_hash, axis=1)

    return df[STAGED_COLUMNS]


def resolve_current_tasks(df: pd.DataFrame, union_mode: object = "union") -> pd.DataFrame:
    if df.empty:
        return empty_current_frame()

    mode = normalize_union_mode(union_mode)
    current = df.copy().sort_values(
        by=[
            "_source_priority",
            "_source_order",
            "_source_row_number",
            "businessKey",
            "recordHash",
        ],
        ascending=[False, True, True, True, True],
        kind="stable",
    )

    if mode == "union":
        current = current.drop_duplicates(subset=["businessKey"], keep="first").copy()
        current.insert(0, "id", current["businessKey"])
        return current[CURRENT_COLUMNS]

    seen: dict[str, int] = {}
    record_ids: list[str] = []

    for business_key in current["businessKey"].tolist():
        count = seen.get(business_key, 0) + 1
        seen[business_key] = count
        record_ids.append(
            business_key if count == 1 else f"{business_key}::dup{count}"
        )

    current = current.copy()
    current.insert(0, "id", record_ids)
    return current[CURRENT_COLUMNS]


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
