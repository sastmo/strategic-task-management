from __future__ import annotations

from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Dict, List
import json

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components


st.set_page_config(
    page_title="Strategic Task Management",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# =========================
# Visual configuration
# =========================
BG = "#161a20"
PANEL = "#1d2229"
PANEL_2 = "#20262e"
BORDER = "#2b323c"
TEXT = "#ffffff"
TEXT_MUTED = "#a7b1bf"
TEXT_MUTED2 = "#8f99a8"
TEXT_MUTED3 = "#cfd6df"

DONE_GRAY = "#94a3b8"
NOT_DONE_BLUE = "#5cc8ff"

ROLES = [
    "Sales",
    "Engineering",
    "Marketing",
    "Production",
    "Committee Chair",
    "Customer Success",
    "Academic Solutions",
]

COLORS: Dict[str, str] = {
    "Sales": "#2563eb",
    "Engineering": "#16a34a",
    "Marketing": "#f97316",
    "Production": "#a855f7",
    "Committee Chair": "#e11d48",
    "Customer Success": "#0ea5a4",
    "Academic Solutions": "#eab308",
}

# =========================
# Data schema
# =========================
REQUIRED_COLUMNS = ["id", "name", "owner", "currentImpact", "futureImpact", "progress", "done"]

COLUMN_ALIASES = {
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
}

DEFAULT_SOURCE = Path(__file__).parent / "data"/"tasks.csv"


@dataclass
class Task:
    id: str
    name: str
    owner: str
    currentImpact: int
    futureImpact: int
    progress: int
    done: bool = False


# =========================
# Input layer
# Supports CSV / Excel / JSON / API JSON
# =========================
def to_bool(value) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y", "done", "completed"}


def normalize_owner(owner: str) -> str:
    owner = str(owner).strip()
    mapping = {
        "R&D": "Engineering",
        "Chairmans": "Committee Chair",
    }
    return mapping.get(owner, owner)


def detect_source_kind(source: str) -> str:
    source = str(source).strip()
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


def extract_json_records(payload) -> List[dict]:
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        for key in ("tasks", "data", "items", "results"):
            if isinstance(payload.get(key), list):
                return payload[key]

    raise ValueError("JSON source must be a list of task objects or a dict containing tasks/data/items/results.")


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

    response = requests.get(source, timeout=15)
    response.raise_for_status()
    return pd.DataFrame(extract_json_records(response.json()))


# =========================
# Processing layer
# Standardize, validate, clean
# =========================
def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for col in df.columns:
        clean = str(col).strip().replace("-", " ").replace("_", " ").lower()
        rename_map[col] = COLUMN_ALIASES.get(
            clean,
            COLUMN_ALIASES.get(clean.replace(" ", ""), str(col).strip())
        )
    return df.rename(columns=rename_map)


def validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df).copy()

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns after normalization: {', '.join(missing)}")

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
    df["done"] = df["done"].apply(to_bool) | (df["progress"] >= 100)

    df = df[df["name"].ne("")].copy()

    if df["id"].duplicated().any():
        seen = {}
        deduped = []
        for value in df["id"]:
            count = seen.get(value, 0) + 1
            seen[value] = count
            deduped.append(value if count == 1 else f"{value}-{count}")
        df["id"] = deduped

    return df[REQUIRED_COLUMNS]


def load_tasks(source: str) -> List[Task]:
    df = validate_and_clean(read_source_to_frame(source))
    return [
        Task(
            id=str(row["id"]),
            name=str(row["name"]),
            owner=str(row["owner"]),
            currentImpact=int(row["currentImpact"]),
            futureImpact=int(row["futureImpact"]),
            progress=int(row["progress"]),
            done=bool(row["done"]),
        )
        for _, row in df.iterrows()
    ]


# =========================
# Dashboard helpers
# =========================
def is_done(task: Task) -> bool:
    return task.done or task.progress >= 100


def owner_groups(tasks: List[Task]) -> Dict[str, List[Task]]:
    groups: Dict[str, List[Task]] = {role: [] for role in ROLES}
    for task in tasks:
        groups.setdefault(task.owner, []).append(task)
    return groups


def active_index_by_owner(tasks: List[Task]) -> Dict[str, Dict[str, int]]:
    index_map: Dict[str, Dict[str, int]] = {}
    for owner in owner_groups(tasks).keys():
        active = [task for task in tasks if task.owner == owner and not is_done(task)]
        index_map[owner] = {task.id: i + 1 for i, task in enumerate(active)}
    return index_map


def build_task_payload(tasks: List[Task]) -> List[Dict[str, object]]:
    groups = owner_groups(tasks)
    active_map = active_index_by_owner(tasks)
    active_ids = {
        owner: [task.id for task in items if not is_done(task)]
        for owner, items in groups.items()
    }

    payload: List[Dict[str, object]] = []
    for task in tasks:
        total_active = len(active_ids.get(task.owner, []))
        payload.append(
            {
                "id": task.id,
                "name": task.name,
                "owner": task.owner,
                "currentImpact": task.currentImpact,
                "futureImpact": task.futureImpact,
                "progress": task.progress,
                "done": is_done(task),
                "status": "done" if is_done(task) else "not_done",
                "pointColor": DONE_GRAY if is_done(task) else COLORS.get(task.owner, "#64748b"),
                "ownerColor": COLORS.get(task.owner, "#64748b"),
                "bubbleSize": 18,
                "activeIndex": active_map.get(task.owner, {}).get(task.id, 0),
                "activeTotal": total_active,
            }
        )
    return payload


def owner_cards_html(tasks: List[Task]) -> str:
    groups = owner_groups(tasks)
    ordered_roles = ROLES + [owner for owner in groups.keys() if owner not in ROLES]

    overall = {
        owner: round(sum(task.progress for task in groups[owner]) / len(groups[owner])) if groups[owner] else 0
        for owner in ordered_roles
    }
    active_ids = {
        owner: [task.id for task in groups[owner] if not is_done(task)]
        for owner in ordered_roles
    }

    def task_html(task: Task) -> str:
        safe_name = escape(task.name)
        color = DONE_GRAY if is_done(task) else COLORS.get(task.owner, "#64748b")
        ids = active_ids.get(task.owner, [])
        idx = ids.index(task.id) + 1 if task.id in ids else 0
        label = "Done" if is_done(task) else f"Active {idx}/{len(ids)}"
        return f"""
        <div class="task-card">
          <div class="task-row">
            <div>
              <div class="task-title">{safe_name}</div>
              <div class="task-sub">Current {task.currentImpact} · Future {task.futureImpact}</div>
            </div>
            <div class="task-progress">{task.progress}%</div>
          </div>
          <div class="progress-track"><i style="width:{task.progress}%;background:{color}"></i></div>
          <div class="task-status">{label}</div>
        </div>
        """

    panels = []
    for owner in ordered_roles:
        items = groups[owner]

        visible_items = items[:3]
        extra_items = items[3:]

        visible_cards = "\n".join(task_html(task) for task in visible_items) if visible_items else '<div class="empty-state">No tasks assigned</div>'

        extra_cards = "\n".join(task_html(task) for task in extra_items)

        more_html = ""
        if extra_items:
            more_html = f"""
            <details class="more-tasks">
              <summary class="more-summary">{len(extra_items)} more task{'s' if len(extra_items) != 1 else ''}</summary>
              <div class="more-tasks-wrap">
                {extra_cards}
              </div>
            </details>
        """
        safe_owner = escape(owner)
        task_word = "task" if len(items) == 1 else "tasks"

        panels.append(f"""
        <details class="owner-panel" open>
          <summary class="owner-summary">
            <div class="owner-left">
              <div class="owner-name">{safe_owner}</div>
            </div>
            <div class="owner-right">
              <div class="owner-overall">Overall: {overall[owner]}%</div>
              <div class="owner-count">{len(items)} {task_word}</div>
            </div>
          </summary>
          <div class="progress-track owner-track"><i style="width:{overall[owner]}%;background:{COLORS.get(owner, '#64748b')}"></i></div>
          <div class="owner-cards">
            {visible_cards}
            {more_html}
          </div>
        </details>
        """)

    return "\n".join(panels)


# =========================
# UI layer
# =========================
def build_dashboard_html(tasks: List[Task]) -> str:
    tasks_json = json.dumps(build_task_payload(tasks))
    owners_html = owner_cards_html(tasks)

    role_legend = "".join(
        f'<div class="role-chip"><span class="role-swatch" style="background:{COLORS[role]}"></span>{escape(role)}</div>'
        for role in ROLES
    )

    return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Strategic Task Management</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{
      margin: 0;
      background: {BG};
      color: {TEXT};
      font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
    }}
    .wrap {{
      max-width: 1500px;
      margin: 0 auto;
      padding: 22px;
      box-sizing: border-box;
    }}
    .panel {{
      background: {PANEL};
      border: 1px solid {BORDER};
      border-radius: 18px;
      padding: 18px;
      box-sizing: border-box;
    }}
    .legend-panel {{
      margin-bottom: 16px;
    }}
    .panel-title {{
      margin: 0 0 8px 0;
      font-size: 18px;
      font-weight: 700;
      color: {TEXT};
    }}
    .panel-subtitle {{
      margin: 0;
      font-size: 13px;
      color: {TEXT_MUTED};
    }}
    .role-legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 14px;
      margin-top: 12px;
      color: {TEXT_MUTED3};
      font-size: 13px;
    }}
    .role-chip {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }}
    .role-swatch {{
      width: 12px;
      height: 12px;
      border-radius: 4px;
      display: inline-block;
    }}
    .dashboard-grid {{
      display: grid;
      grid-template-columns: minmax(0, 2fr) minmax(340px, 0.95fr);
      gap: 16px;
      align-items: start;
    }}
    #impactChart {{
      height: 520px;
    }}
    #portfolioChart {{
      height: 180px;
    }}
    .chart-toolbar {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 12px;
      flex-wrap: wrap;
    }}
    .filter-pill {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      border: 1px solid {BORDER};
      background: {PANEL_2};
      color: {TEXT_MUTED3};
      font-size: 12px;
    }}
    .filter-pill strong {{
      color: {TEXT};
      font-weight: 700;
    }}
    .clear-btn {{
      border: 1px solid {BORDER};
      background: {PANEL_2};
      color: {TEXT};
      padding: 8px 12px;
      border-radius: 10px;
      cursor: pointer;
      font-size: 12px;
    }}
    .clear-btn:hover {{
      border-color: #475569;
    }}
    .status-row {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
      margin-top: 14px;
    }}
    .status-pill {{
      width: 100%;
      border: 1px solid {BORDER};
      background: {PANEL_2};
      color: {TEXT};
      border-radius: 14px;
      padding: 12px 14px;
      text-align: left;
      cursor: pointer;
      transition: border-color 0.2s ease, background 0.2s ease, transform 0.2s ease;
    }}
    .status-pill:hover {{
      border-color: #64748b;
      transform: translateY(-1px);
    }}
    .status-pill.active {{
      border-color: #cbd5e1;
      box-shadow: 0 0 0 1px rgba(255,255,255,0.08) inset;
    }}
    .status-label {{
      display: flex;
      align-items: center;
      gap: 8px;
      color: {TEXT_MUTED3};
      font-size: 12px;
      margin-bottom: 6px;
    }}
    .status-dot {{
      width: 10px;
      height: 10px;
      border-radius: 999px;
      display: inline-block;
    }}
    .status-value {{
      font-size: 15px;
      font-weight: 700;
      color: {TEXT};
    }}
    .section-title {{
      margin: 18px 0 10px 2px;
      font-size: 24px;
      font-weight: 700;
      color: {TEXT};
    }}
    .owners-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 16px;
    }}
    .owner-panel {{
      background: {PANEL};
      border: 1px solid {BORDER};
      border-radius: 16px;
      padding: 14px 14px 10px 14px;
    }}
    .owner-summary {{
      list-style: none;
      cursor: pointer;
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 12px;
      position: relative;
      padding-right: 20px;
    }}
    .owner-summary::-webkit-details-marker {{
      display: none;
    }}
    .owner-summary::after {{
      content: "▸";
      position: absolute;
      right: 0;
      top: 0;
      color: {TEXT_MUTED2};
      font-size: 14px;
      line-height: 1;
    }}
    details[open] > .owner-summary::after {{
      content: "▾";
    }}
    .owner-name {{
      font-size: 16px;
      font-weight: 700;
      color: {TEXT};
    }}
    .owner-right {{
      text-align: right;
      padding-right: 6px;
    }}
    .owner-overall {{
      font-size: 13px;
      font-weight: 700;
      color: {TEXT_MUTED3};
    }}
    .owner-count {{
      font-size: 12px;
      color: {TEXT_MUTED2};
      margin-top: 3px;
    }}
    .progress-track {{
      height: 8px;
      background: #29303a;
      border-radius: 999px;
      overflow: hidden;
      margin: 10px 0 12px 0;
    }}
    .progress-track > i {{
      display: block;
      height: 100%;
      border-radius: 999px;
    }}
    .owner-track {{
      margin-top: 12px;
    }}
    .task-card {{
      background: {PANEL_2};
      border: 1px solid {BORDER};
      border-radius: 14px;
      padding: 12px;
      margin-bottom: 12px;
    }}
    .task-row {{
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 8px;
    }}
    .task-title {{
      font-size: 15px;
      font-weight: 700;
      color: {TEXT};
    }}
    .task-sub {{
      font-size: 12px;
      color: {TEXT_MUTED2};
      margin-top: 2px;
    }}
    .task-progress {{
      font-size: 13px;
      font-weight: 700;
      color: {TEXT_MUTED3};
      white-space: nowrap;
    }}
    .task-status {{
      font-size: 11px;
      color: {TEXT_MUTED3};
      margin-top: 2px;
    }}
    .empty-state {{
      color: {TEXT_MUTED2};
      font-size: 12px;
      padding: 6px 0 2px 0;
    }}
    @media (max-width: 1100px) {{
      .dashboard-grid {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="panel legend-panel">
      <div class="panel-title">Current vs Future Impact Map</div>
      <p class="panel-subtitle">X-axis: Current Offering. Y-axis: Future Offering.</p>
      <div class="role-legend">
        {role_legend}
        <div class="role-chip"><span class="role-swatch" style="background:{DONE_GRAY}"></span>Done</div>
      </div>
    </div>

    <div class="dashboard-grid">
      <div class="panel">
        <div class="chart-toolbar">
          <div class="filter-pill">Filter: <strong id="activeFilterLabel">All tasks</strong></div>
          <button id="clearFilterBtn" class="clear-btn" type="button">Clear filter</button>
        </div>
        <div class="panel-title">Strategic Positioning</div>
        <div id="impactChart"></div>
      </div>

      <div class="panel">
        <div class="panel-title">Completion Distribution</div>
        <div id="portfolioChart"></div>
        <div class="status-row">
          <button id="donePill" class="status-pill" type="button">
            <div class="status-label"><span class="status-dot" style="background:{DONE_GRAY}"></span>Done</div>
            <div id="doneValue" class="status-value">-</div>
          </button>
          <button id="notDonePill" class="status-pill" type="button">
            <div class="status-label"><span class="status-dot" style="background:{NOT_DONE_BLUE}"></span>Not Finished</div>
            <div id="notDoneValue" class="status-value">-</div>
          </button>
        </div>
      </div>
    </div>

    <div class="section-title">Owner Views</div>
    <div class="owners-grid">
      {owners_html}
    </div>
  </div>

  <script>
    const TASKS = {tasks_json};

    let activeFilter = "all";

    const impactEl = document.getElementById("impactChart");
    const portfolioEl = document.getElementById("portfolioChart");
    const activeFilterLabelEl = document.getElementById("activeFilterLabel");
    const clearFilterBtn = document.getElementById("clearFilterBtn");
    const donePill = document.getElementById("donePill");
    const notDonePill = document.getElementById("notDonePill");
    const doneValue = document.getElementById("doneValue");
    const notDoneValue = document.getElementById("notDoneValue");

    function escapeHtml(value) {{
      return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }}

    function statusLabel(status) {{
      if (status === "done") return "Done";
      if (status === "not_done") return "Not Finished";
      return "All tasks";
    }}

    function getVisibleTasks() {{
      if (activeFilter === "all") return TASKS;
      return TASKS.filter(task => task.status === activeFilter);
    }}

    function toggleFilter(nextFilter) {{
      activeFilter = activeFilter === nextFilter ? "all" : nextFilter;
      renderDashboard();
    }}

    function updateStatusPills() {{
      const doneCount = TASKS.filter(task => task.status === "done").length;
      const notDoneCount = TASKS.filter(task => task.status === "not_done").length;
      const total = TASKS.length || 1;
      const donePct = Math.round((doneCount / total) * 100);
      const notDonePct = 100 - donePct;

      doneValue.textContent = `${{donePct}}% (${{doneCount}}/${{TASKS.length}})`;
      notDoneValue.textContent = `${{notDonePct}}% (${{notDoneCount}}/${{TASKS.length}})`;

      donePill.classList.toggle("active", activeFilter === "done");
      notDonePill.classList.toggle("active", activeFilter === "not_done");
    }}

    function buildHoverText(task) {{
      const statusLine = task.done ? "Done" : `Active ${{task.activeIndex}}/${{task.activeTotal}}`;
      return `<b>${{escapeHtml(task.name)}}</b><br>` +
             `Owner: ${{escapeHtml(task.owner)}}<br>` +
             `Current Impact: ${{task.currentImpact}}<br>` +
             `Future Impact: ${{task.futureImpact}}<br>` +
             `Progress: ${{task.progress}}%<br>` +
             statusLine;
    }}

    function renderImpactChart() {{
      const visibleTasks = getVisibleTasks();
      const activeTasks = visibleTasks.filter(task => !task.done);

      const haloTrace = {{
        x: activeTasks.map(task => task.currentImpact),
        y: activeTasks.map(task => task.futureImpact),
        mode: "markers",
        hoverinfo: "skip",
        showlegend: false,
        marker: {{
          size: activeTasks.map(task => task.bubbleSize + 8),
          color: activeTasks.map(task => task.ownerColor),
          opacity: 0.22,
          line: {{
            width: 2,
            color: activeTasks.map(task => task.ownerColor),
          }},
        }},
      }};

      const pointTrace = {{
        x: visibleTasks.map(task => task.currentImpact),
        y: visibleTasks.map(task => task.futureImpact),
        mode: "markers",
        text: visibleTasks.map(task => buildHoverText(task)),
        customdata: visibleTasks.map(task => task.status),
        hovertemplate: "%{{text}}<extra></extra>",
        showlegend: false,
        marker: {{
          size: visibleTasks.map(task => task.bubbleSize),
          color: visibleTasks.map(task => task.pointColor),
          line: {{ width: 0 }},
        }},
      }};

      const layout = {{
        template: "plotly_dark",
        paper_bgcolor: "{BG}",
        plot_bgcolor: "{PANEL_2}",
        font: {{ color: "{TEXT}" }},
        margin: {{ l: 60, r: 20, t: 10, b: 60 }},
        xaxis: {{
          title: "Impact on Current Offering (Sales Today)",
          range: [0, 100],
          gridcolor: "{BORDER}",
          zeroline: false,
        }},
        yaxis: {{
          title: "Impact on Future Offering (Next Packages & Growth)",
          range: [0, 100],
          gridcolor: "{BORDER}",
          zeroline: false,
        }},
        hoverlabel: {{
          bgcolor: "#111827",
          font: {{ color: "#f8fafc" }},
        }},
      }};

      Plotly.react(impactEl, [haloTrace, pointTrace], layout, {{
        displayModeBar: false,
        responsive: true,
      }});

      if (impactEl.removeAllListeners) impactEl.removeAllListeners("plotly_click");
      impactEl.on("plotly_click", function(event) {{
        const point = event.points && event.points[0];
        if (!point || !point.customdata) return;
        toggleFilter(point.customdata);
      }});
    }}

    function renderPortfolioChart() {{
      const doneCount = TASKS.filter(task => task.status === "done").length;
      const notDoneCount = TASKS.filter(task => task.status === "not_done").length;
      const total = TASKS.length || 1;
      const donePct = Math.round((doneCount / total) * 100);
      const notDonePct = 100 - donePct;

      const doneTrace = {{
        x: [donePct],
        y: [""],
        type: "bar",
        orientation: "h",
        marker: {{
          color: "{DONE_GRAY}",
          opacity: activeFilter === "not_done" ? 0.35 : 1,
        }},
        customdata: ["done"],
        hovertemplate: `Done: ${{donePct}}% (${{doneCount}}/${{TASKS.length}})<extra></extra>`,
        showlegend: false,
      }};

      const notDoneTrace = {{
        x: [notDonePct],
        y: [""],
        type: "bar",
        orientation: "h",
        marker: {{
          color: "{NOT_DONE_BLUE}",
          opacity: activeFilter === "done" ? 0.35 : 1,
        }},
        customdata: ["not_done"],
        hovertemplate: `Not Finished: ${{notDonePct}}% (${{notDoneCount}}/${{TASKS.length}})<extra></extra>`,
        showlegend: false,
      }};

      const layout = {{
        barmode: "stack",
        template: "plotly_dark",
        paper_bgcolor: "{BG}",
        plot_bgcolor: "{PANEL_2}",
        font: {{ color: "{TEXT}" }},
        margin: {{ l: 20, r: 20, t: 10, b: 20 }},
        xaxis: {{
          range: [0, 100],
          visible: false,
        }},
        yaxis: {{
          visible: false,
        }},
      }};

      Plotly.react(portfolioEl, [doneTrace, notDoneTrace], layout, {{
        displayModeBar: false,
        responsive: true,
      }});

      if (portfolioEl.removeAllListeners) portfolioEl.removeAllListeners("plotly_click");
      portfolioEl.on("plotly_click", function(event) {{
        const point = event.points && event.points[0];
        if (!point || !point.customdata) return;
        toggleFilter(point.customdata);
      }});
    }}

    function renderDashboard() {{
      activeFilterLabelEl.textContent = statusLabel(activeFilter);
      updateStatusPills();
      renderImpactChart();
      renderPortfolioChart();
    }}

    clearFilterBtn.addEventListener("click", function() {{
      activeFilter = "all";
      renderDashboard();
    }});

    donePill.addEventListener("click", function() {{
      toggleFilter("done");
    }});

    notDonePill.addEventListener("click", function() {{
      toggleFilter("not_done");
    }});

    renderDashboard();
  </script>
</body>
</html>
"""


# =========================
# Streamlit shell
# =========================
st.markdown(
    """
    <style>
      .stApp {
        background: #161a20;
      }
      .block-container {
        padding-top: 1rem;
        padding-bottom: 1.25rem;
        max-width: 1560px;
      }
      h1, .stApp h1 {
        color: #ffffff !important;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Strategic Task Management")

tasks = load_tasks(str(DEFAULT_SOURCE))
html = build_dashboard_html(tasks)
components.html(html, height=1900, scrolling=True)