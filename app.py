from __future__ import annotations

from html import escape
from pathlib import Path
from typing import Dict, List
import json
import os

import streamlit as st
import streamlit.components.v1 as components

from src.loader import load_tasks
from src.schema import Task, normalize_owner, task_status


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

DONE_GREEN = "#22c55e"
PAUSED_GRAY = "#94a3b8"
ACTIVE_BLUE = "#5cc8ff"

FALLBACK_OWNER_COLORS = [
    "#38bdf8",
    "#fb7185",
    "#34d399",
    "#f59e0b",
    "#818cf8",
    "#f472b6",
    "#2dd4bf",
    "#a78bfa",
    "#f87171",
    "#22c55e",
    "#eab308",
    "#60a5fa",
]

DEFAULT_SOURCE = Path(
    os.getenv(
        "TASKS_SOURCE",
        str(Path(__file__).resolve().parent / "data" / "tasks.csv"),
    )
)


def owner_color(owner: str) -> str:
    owner = normalize_owner(owner)
    return FALLBACK_OWNER_COLORS[
        sum(ord(ch) for ch in owner) % len(FALLBACK_OWNER_COLORS)
    ]


def bubble_size_for_progress(progress: int) -> float:
    min_size = 14
    max_size = 30
    progress = max(0, min(100, int(progress)))

    if progress >= 100:
        return min_size

    return round(max_size - ((progress / 100) * (max_size - min_size)), 1)


def owner_order(tasks: List[Task]) -> List[str]:
    seen: List[str] = []

    for task in tasks:
        owner = normalize_owner(task.owner)
        if owner not in seen:
            seen.append(owner)

    return seen


def owner_groups(tasks: List[Task]) -> Dict[str, List[Task]]:
    groups: Dict[str, List[Task]] = {owner: [] for owner in owner_order(tasks)}

    for task in tasks:
        groups.setdefault(normalize_owner(task.owner), []).append(task)

    return groups


def active_index_by_owner(tasks: List[Task]) -> Dict[str, Dict[str, int]]:
    index_map: Dict[str, Dict[str, int]] = {}

    for owner, items in owner_groups(tasks).items():
        active = [task for task in items if task_status(task) == "active"]
        index_map[owner] = {task.id: index + 1 for index, task in enumerate(active)}

    return index_map


def build_task_payload(tasks: List[Task]) -> List[Dict[str, object]]:
    groups = owner_groups(tasks)
    active_map = active_index_by_owner(tasks)
    active_ids = {
        owner: [task.id for task in items if task_status(task) == "active"]
        for owner, items in groups.items()
    }

    payload: List[Dict[str, object]] = []

    for task in tasks:
        owner = normalize_owner(task.owner)
        status = task_status(task)

        if status == "done":
            point_color = DONE_GREEN
        elif status == "paused":
            point_color = PAUSED_GRAY
        else:
            point_color = owner_color(owner)

        payload.append(
            {
                "id": task.id,
                "name": task.name,
                "owner": owner,
                "currentImpact": task.currentImpact,
                "futureImpact": task.futureImpact,
                "progress": task.progress,
                "done": status == "done",
                "paused": status == "paused",
                "status": status,
                "pointColor": point_color,
                "ownerColor": owner_color(owner),
                "bubbleSize": bubble_size_for_progress(task.progress),
                "activeIndex": active_map.get(owner, {}).get(task.id, 0),
                "activeTotal": len(active_ids.get(owner, [])),
            }
        )

    return payload


def owner_cards_html(tasks: List[Task]) -> str:
    groups = owner_groups(tasks)
    ordered_owners = list(groups.keys())

    overall = {
        owner: round(sum(task.progress for task in items) / len(items)) if items else 0
        for owner, items in groups.items()
    }
    active_ids = {
        owner: [task.id for task in groups[owner] if task_status(task) == "active"]
        for owner in ordered_owners
    }

    def render_task(task: Task) -> str:
        owner = normalize_owner(task.owner)
        status = task_status(task)
        safe_name = escape(task.name)

        if status == "done":
            color = DONE_GREEN
            label = "Done"
        elif status == "paused":
            color = PAUSED_GRAY
            label = "Paused"
        else:
            ids = active_ids.get(owner, [])
            index = ids.index(task.id) + 1 if task.id in ids else 0
            color = owner_color(owner)
            label = f"Active {index}/{len(ids)}"

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

    panels: List[str] = []

    for owner in ordered_owners:
        items = groups[owner]
        visible_items = items[:3]
        extra_items = items[3:]

        visible_cards = (
            "\n".join(render_task(task) for task in visible_items)
            if visible_items
            else '<div class="empty-state">No tasks assigned</div>'
        )
        extra_cards = "\n".join(render_task(task) for task in extra_items)

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

        panels.append(
            f"""
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
              <div class="progress-track owner-track"><i style="width:{overall[owner]}%;background:{owner_color(owner)}"></i></div>
              <div class="owner-cards">
                {visible_cards}
                {more_html}
              </div>
            </details>
            """
        )

    return "\n".join(panels)


def build_dashboard_html(tasks: List[Task]) -> str:
    tasks_json = json.dumps(build_task_payload(tasks))
    owners_html = owner_cards_html(tasks)

    role_legend = "".join(
        f'<div class="role-chip"><span class="role-swatch" style="background:{owner_color(owner)}"></span>{escape(owner)}</div>'
        for owner in owner_order(tasks)
    )
    role_legend += f'<div class="role-chip"><span class="role-swatch" style="background:{DONE_GREEN}"></span>Done</div>'
    role_legend += f'<div class="role-chip"><span class="role-swatch" style="background:{PAUSED_GRAY}"></span>Paused</div>'

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
      grid-template-columns: repeat(3, 1fr);
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
      margin: 32px 0 10px 2px;
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
    .more-tasks {{
      margin-top: 6px;
      border-top: 1px dashed {BORDER};
      padding-top: 8px;
    }}
    .more-summary {{
      cursor: pointer;
      color: {TEXT_MUTED3};
      font-size: 12px;
      margin-bottom: 10px;
    }}
    .more-tasks-wrap {{
      margin-top: 10px;
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
            <div class="status-label"><span class="status-dot" style="background:{DONE_GREEN}"></span>Done</div>
            <div id="doneValue" class="status-value">-</div>
          </button>
          <button id="pausedPill" class="status-pill" type="button">
            <div class="status-label"><span class="status-dot" style="background:{PAUSED_GRAY}"></span>Paused</div>
            <div id="pausedValue" class="status-value">-</div>
          </button>
          <button id="activePill" class="status-pill" type="button">
            <div class="status-label"><span class="status-dot" style="background:{ACTIVE_BLUE}"></span>Active</div>
            <div id="activeValue" class="status-value">-</div>
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
    const pausedPill = document.getElementById("pausedPill");
    const activePill = document.getElementById("activePill");

    const doneValue = document.getElementById("doneValue");
    const pausedValue = document.getElementById("pausedValue");
    const activeValue = document.getElementById("activeValue");

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
      if (status === "paused") return "Paused";
      if (status === "active") return "Active";
      return "All tasks";
    }}

    function getVisibleTasks() {{
      if (activeFilter === "all") return TASKS;
      return TASKS.filter(function(task) {{
        return task.status === activeFilter;
      }});
    }}

    function toggleFilter(nextFilter) {{
      activeFilter = activeFilter === nextFilter ? "all" : nextFilter;
      renderDashboard();
    }}

    function updateStatusPills() {{
      const doneCount = TASKS.filter(function(task) {{ return task.status === "done"; }}).length;
      const pausedCount = TASKS.filter(function(task) {{ return task.status === "paused"; }}).length;
      const activeCount = TASKS.filter(function(task) {{ return task.status === "active"; }}).length;

      const total = TASKS.length || 1;
      const donePct = Math.round((doneCount / total) * 100);
      const pausedPct = Math.round((pausedCount / total) * 100);
      const activePct = Math.round((activeCount / total) * 100);

      doneValue.textContent = donePct + "% (" + doneCount + "/" + TASKS.length + ")";
      pausedValue.textContent = pausedPct + "% (" + pausedCount + "/" + TASKS.length + ")";
      activeValue.textContent = activePct + "% (" + activeCount + "/" + TASKS.length + ")";

      donePill.classList.toggle("active", activeFilter === "done");
      pausedPill.classList.toggle("active", activeFilter === "paused");
      activePill.classList.toggle("active", activeFilter === "active");
    }}

    function buildHoverText(task) {{
      let statusLine = "Active";
      if (task.status === "done") {{
        statusLine = "Done";
      }} else if (task.status === "paused") {{
        statusLine = "Paused";
      }} else {{
        statusLine = "Active " + task.activeIndex + "/" + task.activeTotal;
      }}

      return "<b>" + escapeHtml(task.name) + "</b><br>" +
             "Owner: " + escapeHtml(task.owner) + "<br>" +
             "Current Impact: " + task.currentImpact + "<br>" +
             "Future Impact: " + task.futureImpact + "<br>" +
             "Progress: " + task.progress + "%<br>" +
             statusLine;
    }}

    function renderImpactChart() {{
      const visibleTasks = getVisibleTasks();
      const activeTasks = visibleTasks.filter(function(task) {{
        return task.status === "active";
      }});

      const haloTrace = {{
        x: activeTasks.map(function(task) {{ return task.currentImpact; }}),
        y: activeTasks.map(function(task) {{ return task.futureImpact; }}),
        mode: "markers",
        hoverinfo: "skip",
        showlegend: false,
        marker: {{
          size: activeTasks.map(function(task) {{ return task.bubbleSize + 8; }}),
          color: activeTasks.map(function(task) {{ return task.ownerColor; }}),
          opacity: 0.22,
          line: {{
            width: 2,
            color: activeTasks.map(function(task) {{ return task.ownerColor; }}),
          }},
        }},
      }};

      const pointTrace = {{
        x: visibleTasks.map(function(task) {{ return task.currentImpact; }}),
        y: visibleTasks.map(function(task) {{ return task.futureImpact; }}),
        mode: "markers",
        text: visibleTasks.map(function(task) {{ return buildHoverText(task); }}),
        customdata: visibleTasks.map(function(task) {{ return task.status; }}),
        hovertemplate: "%{{text}}<extra></extra>",
        showlegend: false,
        marker: {{
          size: visibleTasks.map(function(task) {{ return task.bubbleSize; }}),
          color: visibleTasks.map(function(task) {{ return task.pointColor; }}),
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
      const doneCount = TASKS.filter(function(task) {{ return task.status === "done"; }}).length;
      const pausedCount = TASKS.filter(function(task) {{ return task.status === "paused"; }}).length;
      const activeCount = TASKS.filter(function(task) {{ return task.status === "active"; }}).length;

      const total = TASKS.length || 1;
      const donePct = Math.round((doneCount / total) * 100);
      const pausedPct = Math.round((pausedCount / total) * 100);
      const activePct = Math.max(0, 100 - donePct - pausedPct);

      const doneTrace = {{
        x: [donePct],
        y: [""],
        type: "bar",
        orientation: "h",
        marker: {{
          color: "{DONE_GREEN}",
          opacity: activeFilter !== "all" && activeFilter !== "done" ? 0.35 : 1,
        }},
        customdata: ["done"],
        hovertemplate: "Done: " + donePct + "% (" + doneCount + "/" + TASKS.length + ")<extra></extra>",
        showlegend: false,
      }};

      const pausedTrace = {{
        x: [pausedPct],
        y: [""],
        type: "bar",
        orientation: "h",
        marker: {{
          color: "{PAUSED_GRAY}",
          opacity: activeFilter !== "all" && activeFilter !== "paused" ? 0.35 : 1,
        }},
        customdata: ["paused"],
        hovertemplate: "Paused: " + pausedPct + "% (" + pausedCount + "/" + TASKS.length + ")<extra></extra>",
        showlegend: false,
      }};

      const activeTrace = {{
        x: [activePct],
        y: [""],
        type: "bar",
        orientation: "h",
        marker: {{
          color: "{ACTIVE_BLUE}",
          opacity: activeFilter !== "all" && activeFilter !== "active" ? 0.35 : 1,
        }},
        customdata: ["active"],
        hovertemplate: "Active: " + activePct + "% (" + activeCount + "/" + TASKS.length + ")<extra></extra>",
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

      Plotly.react(portfolioEl, [doneTrace, pausedTrace, activeTrace], layout, {{
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

    pausedPill.addEventListener("click", function() {{
      toggleFilter("paused");
    }});

    activePill.addEventListener("click", function() {{
      toggleFilter("active");
    }});

    renderDashboard();
  </script>
</body>
</html>
"""

st.markdown(
    """
    <style>
      .stApp,
      [data-testid="stAppViewContainer"],
      [data-testid="stHeader"] {
        background: #161a20 !important;
      }

      .block-container {
        padding-top: 2.5rem;
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

try:
    tasks = load_tasks(str(DEFAULT_SOURCE))
except Exception as exc:
    st.error(f"Could not load tasks from: {DEFAULT_SOURCE}")
    st.exception(exc)
    st.stop()

html = build_dashboard_html(tasks)
components.html(html, height=1900, scrolling=True)
