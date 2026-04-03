from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
import requests
from psycopg.rows import dict_row

from src.schema import Task, validate_and_clean


SourceSpec = str | dict[str, Any]
SourceList = list[SourceSpec]


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


def extract_json_records(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return payload

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


def normalize_source_spec(source: SourceSpec) -> dict[str, Any]:
    if isinstance(source, str):
        return {
            "source": source,
            "source_name": derive_source_name(source),
        }

    if isinstance(source, dict):
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
        return spec

    raise TypeError("Source must be a string path/URL or a dict source spec.")


def add_source_metadata(
    df: pd.DataFrame,
    *,
    source_name: str,
    source_kind: str,
    source_sheet: str | None = None,
) -> pd.DataFrame:
    df = df.copy()
    df["_source_name"] = source_name
    df["_source_kind"] = source_kind
    df["_source_sheet"] = source_sheet or ""
    return df


def read_csv_source(source: str) -> pd.DataFrame:
    return pd.read_csv(source)


def read_json_source(source: str) -> pd.DataFrame:
    with open(source, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return pd.DataFrame(extract_json_records(payload))


def read_api_source(source: str) -> pd.DataFrame:
    response = requests.get(source, timeout=15)
    response.raise_for_status()
    return pd.DataFrame(extract_json_records(response.json()))


def read_excel_source(spec: dict[str, Any]) -> list[pd.DataFrame]:
    source = spec["source"]
    source_name = spec["source_name"]
    all_sheets = bool(spec.get("all_sheets", False))
    sheet_name = spec.get("sheet_name")

    frames: list[pd.DataFrame] = []

    if all_sheets:
        workbook = pd.read_excel(source, sheet_name=None)
        for sheet, df in workbook.items():
            frames.append(
                add_source_metadata(
                    df,
                    source_name=source_name,
                    source_kind="excel",
                    source_sheet=str(sheet),
                )
            )
        return frames

    if isinstance(sheet_name, list):
        for sheet in sheet_name:
            df = pd.read_excel(source, sheet_name=sheet)
            frames.append(
                add_source_metadata(
                    df,
                    source_name=source_name,
                    source_kind="excel",
                    source_sheet=str(sheet),
                )
            )
        return frames

    df = pd.read_excel(source, sheet_name=sheet_name if sheet_name is not None else 0)
    frames.append(
        add_source_metadata(
            df,
            source_name=source_name,
            source_kind="excel",
            source_sheet=str(sheet_name) if sheet_name is not None else "0",
        )
    )
    return frames


def read_source_spec_to_frames(source: SourceSpec) -> list[pd.DataFrame]:
    spec = normalize_source_spec(source)
    source_value = spec["source"]
    source_name = spec["source_name"]
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
            )
        ]

    if kind == "json":
        df = read_json_source(source_value)
        return [
            add_source_metadata(
                df,
                source_name=source_name,
                source_kind="json",
            )
        ]

    if kind == "api":
        df = read_api_source(source_value)
        return [
            add_source_metadata(
                df,
                source_name=source_name,
                source_kind="api",
            )
        ]

    if kind == "excel":
        return read_excel_source(spec)

    raise ValueError(f"Unsupported source kind: {kind}")


def read_sources_to_frame(sources: SourceSpec | SourceList) -> pd.DataFrame:
    if isinstance(sources, list):
        frames: list[pd.DataFrame] = []
        for source in sources:
            frames.extend(read_source_spec_to_frames(source))
    else:
        frames = read_source_spec_to_frames(sources)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


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


def load_tasks_from_db(database_url: str) -> list[Task]:
    query = """
        SELECT
            id,
            name,
            owner,
            current_impact AS "currentImpact",
            future_impact AS "futureImpact",
            progress,
            done,
            paused
        FROM tasks
        ORDER BY owner, name
    """

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
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


def load_tasks(source: SourceSpec | SourceList) -> list[Task]:
    if isinstance(source, str) and detect_source_kind(source) == "postgres":
        return load_tasks_from_db(source)

    df = validate_and_clean(read_source_to_frame(source))
    return frame_to_tasks(df)