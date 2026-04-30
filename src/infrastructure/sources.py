from __future__ import annotations

from dataclasses import dataclass
import glob
from io import BytesIO
import json
import os
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import requests

from src.domain.tasks import normalize_union_mode
from src.infrastructure.graph.client import GraphFileClient

SourceSpec = str | dict[str, Any]
SourceList = list[SourceSpec]

SUPPORTED_FILE_SUFFIXES = {".csv", ".json", ".xls", ".xlsx"}
TASK_SOURCE_ROOT_ENV = "TASK_SOURCE_ROOT"


@dataclass(frozen=True, slots=True)
class TaskSourceConfig:
    sources: list[SourceSpec]
    union_mode: str = "union"


@dataclass(frozen=True, slots=True)
class ResolvedSourceSpec:
    source: str
    source_name: str
    source_priority: int = 100
    source_order: int = 0
    kind: str | None = None
    sheet_name: str | int | list[str | int] | None = None
    all_sheets: bool = False
    site_url: str | None = None
    drive_id: str | None = None
    drive_name: str | None = None
    file_path: str | None = None
    item_id: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "source": self.source,
            "source_name": self.source_name,
            "source_priority": self.source_priority,
            "source_order": self.source_order,
            "kind": self.kind,
            "sheet_name": self.sheet_name,
            "all_sheets": self.all_sheets,
        }
        optional_values = {
            "site_url": self.site_url,
            "drive_id": self.drive_id,
            "drive_name": self.drive_name,
            "file_path": self.file_path,
            "item_id": self.item_id,
        }
        payload.update({key: value for key, value in optional_values.items() if value})
        return payload


def detect_source_kind(source: str) -> str:
    source = str(source).strip()

    if source.startswith("graph://"):
        return "graph"

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

    if source_value.startswith("graph://"):
        return Path(source_value.rsplit("::", 1)[-1]).stem or "graph_source"

    if source_value.startswith(("http://", "https://")):
        return source_value.rstrip("/").split("/")[-1] or "api_source"

    path = Path(source_value)
    return path.stem or path.name or "source"


def is_graph_source_spec_dict(value: object) -> bool:
    if not isinstance(value, dict):
        return False

    kind = str(value.get("kind", value.get("source_kind", ""))).strip().lower()
    return kind == "graph" or (
        "site_url" in value and any(key in value for key in ("file_path", "item_id"))
    )


def is_source_spec_dict(value: object) -> bool:
    return isinstance(value, dict) and (
        is_graph_source_spec_dict(value)
        or any(key in value for key in ("source", "path", "url", "glob"))
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


def parse_source_config(source_input: Any) -> TaskSourceConfig:
    if is_source_spec_dict(source_input):
        return TaskSourceConfig(sources=[source_input])

    if isinstance(source_input, list):
        return TaskSourceConfig(sources=source_input)

    if isinstance(source_input, dict):
        sources = source_input.get("sources")
        if sources is None:
            raise ValueError("Source config dict must include a 'sources' key.")

        return TaskSourceConfig(
            sources=list(sources),
            union_mode=normalize_union_mode(source_input.get("union_mode", "union")),
        )

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
            return TaskSourceConfig(sources=parsed_json)

    path = Path(raw_value)
    if path.suffix.lower() == ".json" and path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and "sources" in payload:
            return parse_source_config(payload)

    if (
        "," in raw_value
        and not raw_value.startswith(("http://", "https://", "postgres://", "postgresql://"))
    ):
        sources = [item.strip() for item in raw_value.split(",") if item.strip()]
        return TaskSourceConfig(sources=sources)

    return TaskSourceConfig(sources=[raw_value])


def build_graph_source_value(
    *,
    site_url: str,
    file_path: str = "",
    item_id: str = "",
    drive_name: str = "",
    drive_id: str = "",
) -> str:
    site_marker = site_url.replace("https://", "").replace("http://", "").rstrip("/")
    drive_marker = drive_name or drive_id or "default-drive"
    item_marker = (file_path or item_id or "graph-item").lstrip("/")
    return f"graph://{site_marker}::{drive_marker}::{item_marker}"


def configured_source_root() -> Path | None:
    raw_value = os.getenv(TASK_SOURCE_ROOT_ENV, "").strip()
    if not raw_value:
        return None
    return Path(raw_value).expanduser().resolve()


def ensure_local_source_allowed(path: str | Path, *, source_root: Path | None = None) -> None:
    allowed_root = source_root or configured_source_root()
    if allowed_root is None:
        return

    resolved_path = Path(path).expanduser().resolve()
    try:
        resolved_path.relative_to(allowed_root)
    except ValueError as exc:
        raise ValueError(
            f"Local task sources must stay within {allowed_root}. "
            f"Received: {resolved_path}"
        ) from exc


def normalize_source_spec(
    source: SourceSpec,
    *,
    source_order: int | None = None,
) -> ResolvedSourceSpec:
    if is_graph_source_spec_dict(source):
        assert isinstance(source, dict)

        site_url = str(source.get("site_url", "")).strip()
        drive_id = str(source.get("drive_id", "")).strip()
        drive_name = str(source.get("drive_name", "")).strip()
        file_path = str(source.get("file_path", "")).strip()
        item_id = str(source.get("item_id", "")).strip()

        if not site_url:
            raise ValueError("Graph source specs must include a 'site_url'.")
        if not file_path and not item_id:
            raise ValueError("Graph source specs must include 'file_path' or 'item_id'.")

        synthetic_source = build_graph_source_value(
            site_url=site_url,
            file_path=file_path,
            item_id=item_id,
            drive_name=drive_name,
            drive_id=drive_id,
        )
        source_name = str(source.get("source_name", "")).strip() or derive_source_name(
            file_path or item_id or synthetic_source
        )

        return ResolvedSourceSpec(
            source=synthetic_source,
            source_name=source_name,
            source_priority=int(source.get("source_priority", 100)),
            source_order=int(source_order if source_order is not None else source.get("source_order", 0)),
            kind="graph",
            sheet_name=source.get("sheet_name"),
            all_sheets=bool(source.get("all_sheets", False)),
            site_url=site_url,
            drive_id=drive_id or None,
            drive_name=drive_name or None,
            file_path=file_path or None,
            item_id=item_id or None,
        )

    if isinstance(source, str):
        spec = {
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
            raise ValueError("Source spec dict must include one of: source, path, url.")

        spec = dict(source)
        spec["source"] = str(source_value)
        spec.setdefault("source_name", derive_source_name(str(source_value)))
    else:
        raise TypeError("Source must be a string path/URL or a dict source spec.")

    return ResolvedSourceSpec(
        source=str(spec["source"]).strip(),
        source_name=str(spec["source_name"]).strip() or derive_source_name(str(spec["source"])),
        source_priority=int(spec.get("source_priority", 100)),
        source_order=int(source_order if source_order is not None else spec.get("source_order", 0)),
        kind=str(spec.get("kind", spec.get("source_kind", ""))).strip().lower() or None,
        sheet_name=spec.get("sheet_name"),
        all_sheets=bool(spec.get("all_sheets", False)),
    )


def expand_source_spec(source: SourceSpec, *, start_order: int) -> list[ResolvedSourceSpec]:
    if isinstance(source, dict) and "glob" in source:
        pattern = str(source["glob"]).strip()
        matches = sorted(glob.glob(pattern, recursive=bool(source.get("recursive", False))))
        expanded: list[ResolvedSourceSpec] = []

        for offset, match in enumerate(matches):
            ensure_local_source_allowed(match)
            spec = dict(source)
            spec.pop("glob", None)
            spec.pop("recursive", None)
            spec["source"] = match
            spec.setdefault("source_name", derive_source_name(match))
            expanded.append(normalize_source_spec(spec, source_order=start_order + offset))

        return expanded

    normalized = normalize_source_spec(source, source_order=start_order)
    source_path = Path(normalized.source)
    explicit_source_name = isinstance(source, dict) and "source_name" in source

    if not normalized.source.startswith(("http://", "https://")) and source_path.is_dir():
        ensure_local_source_allowed(source_path)
        matches = sorted(
            str(path)
            for path in source_path.iterdir()
            if path.is_file() and path.suffix.lower() in SUPPORTED_FILE_SUFFIXES
        )

        for match in matches:
            ensure_local_source_allowed(match)

        return [
            ResolvedSourceSpec(
                source=match,
                source_name=normalized.source_name if explicit_source_name else derive_source_name(match),
                source_priority=normalized.source_priority,
                source_order=start_order + offset,
                sheet_name=normalized.sheet_name,
                all_sheets=normalized.all_sheets,
            )
            for offset, match in enumerate(matches)
        ]

    if normalized.kind != "graph" and not normalized.source.startswith(
        ("http://", "https://", "postgres://", "postgresql://")
    ):
        ensure_local_source_allowed(normalized.source)

    return [normalized]


def expand_source_specs(sources: SourceList) -> list[ResolvedSourceSpec]:
    expanded: list[ResolvedSourceSpec] = []
    next_order = 1

    for source in sources:
        items = expand_source_spec(source, start_order=next_order)
        expanded.extend(items)
        next_order += len(items)

    return expanded


def add_source_metadata(
    df: pd.DataFrame,
    *,
    source_spec: ResolvedSourceSpec,
    source_kind: str,
    source_sheet: str | None = None,
    source_path: str | None = None,
) -> pd.DataFrame:
    df = df.copy()
    df["source_name"] = source_spec.source_name
    df["source_kind"] = source_kind
    df["source_sheet"] = source_sheet or ""
    df["source_path"] = source_path or source_spec.source
    df["source_priority"] = int(source_spec.source_priority)
    df["source_order"] = int(source_spec.source_order)
    df["source_row_number"] = range(1, len(df) + 1)
    return df


def read_csv_source(source: str) -> pd.DataFrame:
    return pd.read_csv(source, dtype=object)


def read_json_source(source: str) -> pd.DataFrame:
    payload = json.loads(Path(source).read_text(encoding="utf-8"))
    return pd.DataFrame(extract_json_records(payload))


def read_api_source(source: str) -> pd.DataFrame:
    response = requests.get(source, timeout=15)
    response.raise_for_status()
    return pd.DataFrame(extract_json_records(response.json()))


def _read_excel_frames(
    load_sheet: Callable,
    load_all_sheets: Callable,
    source_spec: ResolvedSourceSpec,
    *,
    source_kind: str,
    source_path: str | None = None,
) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []

    if source_spec.all_sheets:
        for sheet, df in load_all_sheets().items():
            frames.append(
                add_source_metadata(
                    df,
                    source_spec=source_spec,
                    source_kind=source_kind,
                    source_sheet=str(sheet),
                    source_path=source_path,
                )
            )
        return frames

    if isinstance(source_spec.sheet_name, list):
        for sheet in source_spec.sheet_name:
            frames.append(
                add_source_metadata(
                    load_sheet(sheet),
                    source_spec=source_spec,
                    source_kind=source_kind,
                    source_sheet=str(sheet),
                    source_path=source_path,
                )
            )
        return frames

    selected_sheet = source_spec.sheet_name if source_spec.sheet_name is not None else 0
    frames.append(
        add_source_metadata(
            load_sheet(selected_sheet),
            source_spec=source_spec,
            source_kind=source_kind,
            source_sheet=str(selected_sheet),
            source_path=source_path,
        )
    )
    return frames


def read_excel_source(source_spec: ResolvedSourceSpec) -> list[pd.DataFrame]:
    return _read_excel_frames(
        load_sheet=lambda sheet: pd.read_excel(source_spec.source, sheet_name=sheet, dtype=object),
        load_all_sheets=lambda: pd.read_excel(source_spec.source, sheet_name=None, dtype=object),
        source_spec=source_spec,
        source_kind="excel",
    )


def infer_content_kind(name: str) -> str:
    suffix = Path(str(name)).suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix in {".xlsx", ".xls"}:
        return "excel"
    if suffix == ".json":
        return "json"
    raise ValueError(f"Unsupported Graph file type: {name}")


def read_csv_bytes_source(content: bytes) -> pd.DataFrame:
    return pd.read_csv(BytesIO(content), dtype=object)


def read_json_bytes_source(content: bytes) -> pd.DataFrame:
    payload = json.loads(content.decode("utf-8"))
    return pd.DataFrame(extract_json_records(payload))


def read_excel_bytes_source(
    source_spec: ResolvedSourceSpec,
    content: bytes,
    *,
    source_kind: str,
    source_path: str,
) -> list[pd.DataFrame]:
    return _read_excel_frames(
        load_sheet=lambda sheet: pd.read_excel(BytesIO(content), sheet_name=sheet, dtype=object),
        load_all_sheets=lambda: pd.read_excel(BytesIO(content), sheet_name=None, dtype=object),
        source_spec=source_spec,
        source_kind=source_kind,
        source_path=source_path,
    )


def read_graph_source(source_spec: ResolvedSourceSpec) -> list[pd.DataFrame]:
    client = GraphFileClient.from_env()
    download = client.download_file(
        site_url=source_spec.site_url or "",
        drive_id=source_spec.drive_id or "",
        drive_name=source_spec.drive_name or "",
        file_path=source_spec.file_path or "",
        item_id=source_spec.item_id or "",
    )
    content_kind = infer_content_kind(download.name)
    source_path = download.web_url or source_spec.source

    if content_kind == "csv":
        return [
            add_source_metadata(
                read_csv_bytes_source(download.content),
                source_spec=source_spec,
                source_kind="graph_csv",
                source_path=source_path,
            )
        ]

    if content_kind == "json":
        return [
            add_source_metadata(
                read_json_bytes_source(download.content),
                source_spec=source_spec,
                source_kind="graph_json",
                source_path=source_path,
            )
        ]

    if content_kind == "excel":
        return read_excel_bytes_source(
            source_spec,
            download.content,
            source_kind="graph_excel",
            source_path=source_path,
        )

    raise ValueError(f"Unsupported Graph file kind: {content_kind}")


def coerce_source_spec(source: ResolvedSourceSpec | SourceSpec) -> ResolvedSourceSpec:
    if isinstance(source, ResolvedSourceSpec):
        return source
    return normalize_source_spec(source)


def read_source_spec_to_frames(source: ResolvedSourceSpec | SourceSpec) -> list[pd.DataFrame]:
    source_spec = coerce_source_spec(source)
    kind = source_spec.kind or detect_source_kind(source_spec.source)

    if kind == "postgres":
        raise ValueError(
            "PostgreSQL sources should be read through load_tasks_from_database(), not read_source_spec_to_frames()."
        )

    if kind == "csv":
        return [
            add_source_metadata(
                read_csv_source(source_spec.source),
                source_spec=source_spec,
                source_kind="csv",
            )
        ]

    if kind == "json":
        return [
            add_source_metadata(
                read_json_source(source_spec.source),
                source_spec=source_spec,
                source_kind="json",
            )
        ]

    if kind == "api":
        return [
            add_source_metadata(
                read_api_source(source_spec.source),
                source_spec=source_spec,
                source_kind="api",
            )
        ]

    if kind == "excel":
        return read_excel_source(source_spec)

    if kind == "graph":
        return read_graph_source(source_spec)

    raise ValueError(f"Unsupported source kind: {kind}")
