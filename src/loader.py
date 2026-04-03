from __future__ import annotations

from dataclasses import dataclass
import glob
import json
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
import requests
from psycopg.rows import dict_row

from src.schema import (
    Task,
    empty_staged_frame,
    normalize_union_mode,
    resolve_current_tasks,
    validate_and_clean,
)

SourceSpec = str | dict[str, Any]
SourceList = list[SourceSpec]

SUPPORTED_FILE_SUFFIXES = {".csv", ".json", ".xls", ".xlsx"}


@dataclass(slots=True)
class LoadedTaskBatch:
    source_config: dict[str, Any]
    staged_frame: pd.DataFrame
    current_frame: pd.DataFrame
    frame_count: int
    source_count: int


def detect_source_kind(source: str) -> str:
    source = str(source).strip()

    if source.startswith(("postgresql://", "postgres://")):
        return "postgres"

    if source.startswith(("http://", "https://")):
        return "api"

    suffix = Path(source).suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix in {".xlsx", ".xls"}:
        return "excel"
    if suffix == ".json":
        return "json"

    raise ValueError(f"Unsupported source type: {source}")


def extract_json_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        if all(isinstance(item, dict) for item in payload):
            return payload
        raise ValueError("JSON task payload list must contain objects.")

    if isinstance(payload, dict):
        for key in ("tasks", "data", "items", "results"):
            if isinstance(payload.get(key), list):
                return payload[key]

    raise ValueError(
        "JSON source must be a list of task objects or a dict containing tasks/data/items/results."
    )


def derive_source_name(source_value: str) -> str:
    source_value = str(source_value).strip()

    if source_value.startswith(("http://", "https://")):
        return source_value.rstrip("/").split("/")[-1] or "api_source"

    path = Path(source_value)
    return path.stem or path.name or "source"


def is_source_spec_dict(value: object) -> bool:
    return isinstance(value, dict) and any(
        key in value for key in ("source", "path", "url", "glob")
    )


def is_database_url(value: object) -> bool:
    return isinstance(value, str) and str(value).strip().startswith(
        ("postgresql://", "postgres://")
    )


def parse_inline_json(value: str) -> Any | None:
    text = str(value).strip()
    if not text or text[0] not in "[{":
        return None

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def parse_source_config(source_input: Any) -> dict[str, Any]:
    if is_source_spec_dict(source_input):
        return {"sources": [source_input], "union_mode": "union"}

    if isinstance(source_input, list):
        return {"sources": source_input, "union_mode": "union"}

    if isinstance(source_input, dict):
        sources = source_input.get("sources")
        if sources is None:
            raise ValueError("Source config dict must include a 'sources' key.")

        return {
            "sources": list(sources),
            "union_mode": source_input.get("union_mode", "union"),
        }

    if not isinstance(source_input, str):
        raise TypeError("Source config must be a string, list, or dict.")

    raw_value = source_input.strip()
    if not raw_value:
        raise ValueError("Source config cannot be empty.")

    parsed_json = parse_inline_json(raw_value)
    if parsed_json is not None:
        if isinstance(parsed_json, dict) and "sources" in parsed_json:
            return parse_source_config(parsed_json)
        if isinstance(parsed_json, list) and all(
            isinstance(item, str) or is_source_spec_dict(item)
            for item in parsed_json
        ):
            return {"sources": parsed_json, "union_mode": "union"}

    path = Path(raw_value)
    if path.suffix.lower() == ".json" and path.exists():
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict) and "sources" in payload:
            return parse_source_config(payload)

    if (
        "," in raw_value
        and not raw_value.startswith(("http://", "https://", "postgres://", "postgresql://"))
    ):
        sources = [item.strip() for item in raw_value.split(",") if item.strip()]
        return {"sources": sources, "union_mode": "union"}

    return {"sources": [raw_value], "union_mode": "union"}


def normalize_source_spec(source: SourceSpec, *, source_order: int | None = None) -> dict[str, Any]:
    if isinstance(source, str):
        spec: dict[str, Any] = {
            "source": source,
            "source_name": derive_source_name(source),
        }
    elif isinstance(source, dict):
        if "source" in source:
            source_value = source["source"]
        elif "path" in source:
            source_value = source["path"]
        elif "url" in source:
            source_value = source["url"]
        else:
            raise ValueError(
                "Source spec dict must include one of: source, path, url."
            )

        spec = dict(source)
        spec["source"] = str(source_value)
        spec.setdefault("source_name", derive_source_name(str(source_value)))
    else:
        raise TypeError("Source must be a string path/URL or a dict source spec.")

    spec["source"] = str(spec["source"]).strip()
    spec["source_name"] = str(spec["source_name"]).strip() or derive_source_name(spec["source"])
    spec["source_priority"] = int(spec.get("source_priority", 100))
    if source_order is not None:
        spec["source_order"] = int(source_order)
    else:
        spec["source_order"] = int(spec.get("source_order", 0))

    return spec


def expand_source_spec(source: SourceSpec, *, start_order: int) -> list[dict[str, Any]]:
    if isinstance(source, dict) and "glob" in source:
        pattern = str(source["glob"]).strip()
        matches = sorted(glob.glob(pattern, recursive=bool(source.get("recursive", False))))
        expanded: list[dict[str, Any]] = []

        for offset, match in enumerate(matches):
            spec = dict(source)
            spec.pop("glob", None)
            spec.pop("recursive", None)
            spec["source"] = match
            spec.setdefault("source_name", derive_source_name(match))
            expanded.append(normalize_source_spec(spec, source_order=start_order + offset))

        return expanded

    normalized = normalize_source_spec(source, source_order=start_order)
    source_path = Path(normalized["source"])
    explicit_source_name = isinstance(source, dict) and "source_name" in source

    if not normalized["source"].startswith(("http://", "https://")) and source_path.is_dir():
        matches = sorted(
            str(path)
            for path in source_path.iterdir()
            if path.is_file() and path.suffix.lower() in SUPPORTED_FILE_SUFFIXES
        )
        expanded = []
        for offset, match in enumerate(matches):
            spec = dict(normalized)
            spec["source"] = match
            spec["source_name"] = (
                normalized["source_name"]
                if explicit_source_name
                else derive_source_name(match)
            )
            spec["source_order"] = start_order + offset
            expanded.append(spec)
        return expanded

    return [normalized]


def expand_source_specs(sources: SourceList) -> list[dict[str, Any]]:
    expanded: list[dict[str, Any]] = []
    next_order = 1

    for source in sources:
        items = expand_source_spec(source, start_order=next_order)
        expanded.extend(items)
        next_order += len(items)

    return expanded


def add_source_metadata(
    df: pd.DataFrame,
    *,
    source_name: str,
    source_kind: str,
    source_path: str,
    source_priority: int,
    source_order: int,
    source_sheet: str | None = None,
) -> pd.DataFrame:
    df = df.copy()
    df["_source_name"] = source_name
    df["_source_kind"] = source_kind
    df["_source_sheet"] = source_sheet or ""
    df["_source_path"] = source_path
    df["_source_priority"] = int(source_priority)
    df["_source_order"] = int(source_order)
    df["_source_row_number"] = range(1, len(df) + 1)
    return df


def read_csv_source(source: str) -> pd.DataFrame:
    return pd.read_csv(source, dtype=object)


def read_json_source(source: str) -> pd.DataFrame:
    with open(source, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return pd.DataFrame(extract_json_records(payload))


def read_api_source(source: str) -> pd.DataFrame:
    response = requests.get(source, timeout=15)
    response.raise_for_status()
    return pd.DataFrame(extract_json_records(response.json()))


def read_excel_source(spec: dict[str, Any]) -> list[pd.DataFrame]:
    source = spec["source"]
    source_name = spec["source_name"]
    source_priority = int(spec["source_priority"])
    source_order = int(spec["source_order"])
    all_sheets = bool(spec.get("all_sheets", False))
    sheet_name = spec.get("sheet_name")

    frames: list[pd.DataFrame] = []

    if all_sheets:
        workbook = pd.read_excel(source, sheet_name=None, dtype=object)
        for sheet, df in workbook.items():
            frames.append(
                add_source_metadata(
                    df,
                    source_name=source_name,
                    source_kind="excel",
                    source_path=source,
                    source_priority=source_priority,
                    source_order=source_order,
                    source_sheet=str(sheet),
                )
            )
        return frames

    if isinstance(sheet_name, list):
        for sheet in sheet_name:
            df = pd.read_excel(source, sheet_name=sheet, dtype=object)
            frames.append(
                add_source_metadata(
                    df,
                    source_name=source_name,
                    source_kind="excel",
                    source_path=source,
                    source_priority=source_priority,
                    source_order=source_order,
                    source_sheet=str(sheet),
                )
            )
        return frames

    selected_sheet = sheet_name if sheet_name is not None else 0
    df = pd.read_excel(source, sheet_name=selected_sheet, dtype=object)
    frames.append(
        add_source_metadata(
            df,
            source_name=source_name,
            source_kind="excel",
            source_path=source,
            source_priority=source_priority,
            source_order=source_order,
            source_sheet=str(selected_sheet),
        )
    )
    return frames


def read_source_spec_to_frames(source: SourceSpec) -> list[pd.DataFrame]:
    spec = normalize_source_spec(source)
    source_value = spec["source"]
    source_name = spec["source_name"]
    source_priority = int(spec["source_priority"])
    source_order = int(spec["source_order"])
    kind = detect_source_kind(source_value)

    if kind == "postgres":
        raise ValueError(
            "PostgreSQL sources should be read through load_tasks_from_db(), not read_source_spec_to_frames()."
        )

    if kind == "csv":
        df = read_csv_source(source_value)
        return [
            add_source_metadata(
                df,
                source_name=source_name,
                source_kind="csv",
                source_path=source_value,
                source_priority=source_priority,
                source_order=source_order,
            )
        ]

    if kind == "json":
        df = read_json_source(source_value)
        return [
            add_source_metadata(
                df,
                source_name=source_name,
                source_kind="json",
                source_path=source_value,
                source_priority=source_priority,
                source_order=source_order,
            )
        ]

    if kind == "api":
        df = read_api_source(source_value)
        return [
            add_source_metadata(
                df,
                source_name=source_name,
                source_kind="api",
                source_path=source_value,
                source_priority=source_priority,
                source_order=source_order,
            )
        ]

    if kind == "excel":
        return read_excel_source(spec)

    raise ValueError(f"Unsupported source kind: {kind}")


def read_sources_to_frame(sources: SourceSpec | SourceList) -> pd.DataFrame:
    batch = load_task_batch(sources)
    return batch.staged_frame.copy()


def read_source_to_frame(source: SourceSpec | SourceList) -> pd.DataFrame:
    return read_sources_to_frame(source)


def frame_to_tasks(df: pd.DataFrame) -> list[Task]:
    return [
        Task(
            id=str(row["id"]),
            name=str(row["name"]),
            owner=str(row["owner"]),
            currentImpact=int(row["currentImpact"]),
            futureImpact=int(row["futureImpact"]),
            progress=int(row["progress"]),
            done=bool(row["done"]),
            paused=bool(row["paused"]),
        )
        for _, row in df.iterrows()
    ]


def detect_task_storage(conn: psycopg.Connection) -> str:
    query = """
        SELECT
            to_regclass('warehouse.tasks_current') AS warehouse_tasks,
            to_regclass('public.tasks') AS public_tasks
    """

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query)
        row = cur.fetchone()

    if row and row["warehouse_tasks"]:
        return "warehouse"
    if row and row["public_tasks"]:
        return "legacy"
    return "missing"


def load_tasks_from_db(database_url: str) -> list[Task]:
    warehouse_query = """
        SELECT
            record_id AS id,
            name,
            owner,
            current_impact AS "currentImpact",
            future_impact AS "futureImpact",
            progress,
            done,
            paused
        FROM warehouse.tasks_current
        WHERE is_deleted = FALSE
        ORDER BY owner, name
    """

    legacy_query = """
        SELECT
            id,
            name,
            owner,
            current_impact AS "currentImpact",
            future_impact AS "futureImpact",
            progress,
            done,
            paused
        FROM public.tasks
        ORDER BY owner, name
    """

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        storage = detect_task_storage(conn)
        if storage == "missing":
            return []
        query = warehouse_query if storage == "warehouse" else legacy_query

        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    if not rows:
        return []

    return [
        Task(
            id=str(row["id"]),
            name=str(row["name"]),
            owner=str(row["owner"]),
            currentImpact=int(row["currentImpact"]),
            futureImpact=int(row["futureImpact"]),
            progress=int(row["progress"]),
            done=bool(row["done"]),
            paused=bool(row["paused"]),
        )
        for row in rows
    ]


def load_task_batch(source_input: SourceSpec | SourceList | dict[str, Any]) -> LoadedTaskBatch:
    config = parse_source_config(source_input)
    union_mode = normalize_union_mode(config["union_mode"])
    expanded_sources = expand_source_specs(list(config["sources"]))

    raw_frames: list[pd.DataFrame] = []
    for source in expanded_sources:
        raw_frames.extend(read_source_spec_to_frames(source))

    cleaned_frames = [validate_and_clean(frame) for frame in raw_frames]

    if cleaned_frames:
        staged_frame = pd.concat(cleaned_frames, ignore_index=True)
    else:
        staged_frame = empty_staged_frame()

    current_frame = resolve_current_tasks(staged_frame, union_mode=union_mode)

    return LoadedTaskBatch(
        source_config={
            "sources": expanded_sources,
            "union_mode": union_mode,
        },
        staged_frame=staged_frame,
        current_frame=current_frame,
        frame_count=len(raw_frames),
        source_count=len(expanded_sources),
    )


def load_tasks(source: SourceSpec | SourceList | dict[str, Any]) -> list[Task]:
    if is_database_url(source):
        return load_tasks_from_db(str(source))

    batch = load_task_batch(source)
    return frame_to_tasks(batch.current_frame)
