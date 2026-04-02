from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
import requests
from psycopg.rows import dict_row

from src.schema import Task, validate_and_clean


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


def read_source_to_frame(source: str) -> pd.DataFrame:
    kind = detect_source_kind(source)

    if kind == "csv":
        return pd.read_csv(source)

    if kind == "excel":
        return pd.read_excel(source)

    if kind == "json":
        with open(source, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return pd.DataFrame(extract_json_records(payload))

    if kind == "api":
        response = requests.get(source, timeout=15)
        response.raise_for_status()
        return pd.DataFrame(extract_json_records(response.json()))

    raise ValueError(
        "PostgreSQL sources should be read through load_tasks(), not read_source_to_frame()."
    )


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


def load_tasks(source: str) -> list[Task]:
    kind = detect_source_kind(source)

    if kind == "postgres":
        return load_tasks_from_db(source)

    df = validate_and_clean(read_source_to_frame(source))
    return frame_to_tasks(df)