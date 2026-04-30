from __future__ import annotations

import json
from datetime import datetime
from functools import cache, lru_cache
from html import escape
from pathlib import Path

from src.domain.tasks import Task, normalize_owner, owner_view_visible, task_status

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
OWNER_DONE_RETENTION_DAYS = 14
OWNER_CARD_VISIBLE_LIMIT = 3

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

STREAMLIT_CHROME_STYLE = """
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
"""


def owner_color(owner: str) -> str:
    owner = normalize_owner(owner)
    return FALLBACK_OWNER_COLORS[
        sum(ord(character) for character in owner) % len(FALLBACK_OWNER_COLORS)
    ]


def bubble_size_for_progress(progress: int) -> float:
    min_size = 14
    max_size = 30
    progress = max(0, min(100, int(progress)))

    if progress >= 100:
        return min_size

    return round(max_size - ((progress / 100) * (max_size - min_size)), 1)


def owner_order(tasks: list[Task]) -> list[str]:
    seen: list[str] = []

    for task in tasks:
        owner = normalize_owner(task.owner)
        if owner not in seen:
            seen.append(owner)

    return seen


def owner_groups(tasks: list[Task]) -> dict[str, list[Task]]:
    groups: dict[str, list[Task]] = {owner: [] for owner in owner_order(tasks)}

    for task in tasks:
        groups.setdefault(normalize_owner(task.owner), []).append(task)

    return groups


def owner_view_tasks(
    tasks: list[Task],
    *,
    now: datetime | None = None,
    done_retention_days: int = OWNER_DONE_RETENTION_DAYS,
) -> list[Task]:
    return [
        task
        for task in tasks
        if owner_view_visible(
            task,
            now=now,
            done_retention_days=done_retention_days,
        )
    ]


def active_index_by_owner(tasks: list[Task]) -> dict[str, dict[str, int]]:
    index_map: dict[str, dict[str, int]] = {}

    for owner, items in owner_groups(tasks).items():
        active = [task for task in items if task_status(task) == "active"]
        index_map[owner] = {task.id: index + 1 for index, task in enumerate(active)}

    return index_map


def build_task_payload(tasks: list[Task]) -> list[dict[str, object]]:
    groups = owner_groups(tasks)
    active_map = active_index_by_owner(tasks)
    active_ids = {
        owner: [task.id for task in items if task_status(task) == "active"]
        for owner, items in groups.items()
    }

    payload: list[dict[str, object]] = []

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
                "currentImpact": task.current_impact,
                "futureImpact": task.future_impact,
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


def owner_cards_html(
    tasks: list[Task],
    *,
    now: datetime | None = None,
    done_retention_days: int = OWNER_DONE_RETENTION_DAYS,
) -> str:
    visible_tasks = owner_view_tasks(
        tasks,
        now=now,
        done_retention_days=done_retention_days,
    )
    groups = owner_groups(visible_tasks)
    ordered_owners = list(groups.keys())
    overall_progress = {
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
              <div class="task-sub">Current {task.current_impact} · Future {task.future_impact}</div>
            </div>
            <div class="task-progress">{task.progress}%</div>
          </div>
          <div class="progress-track"><i style="width:{task.progress}%;background:{color}"></i></div>
          <div class="task-status">{label}</div>
        </div>
        """

    panels: list[str] = []

    for owner in ordered_owners:
        items = groups[owner]
        visible_items = items[:OWNER_CARD_VISIBLE_LIMIT]
        extra_items = items[OWNER_CARD_VISIBLE_LIMIT:]
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
                  <div class="owner-overall">Overall: {overall_progress[owner]}%</div>
                  <div class="owner-count">{len(items)} {task_word}</div>
                </div>
              </summary>
              <div class="progress-track owner-track"><i style="width:{overall_progress[owner]}%;background:{owner_color(owner)}"></i></div>
              <div class="owner-cards">
                {visible_cards}
                {more_html}
              </div>
            </details>
            """
        )

    return "\n".join(panels)


@lru_cache(maxsize=1)
def load_dashboard_template() -> str:
    template_path = Path(__file__).resolve().parent / "templates" / "dashboard.html"
    return template_path.read_text(encoding="utf-8")


@cache
def load_dashboard_asset(name: str) -> str:
    asset_path = Path(__file__).resolve().parent / "assets" / name
    return asset_path.read_text(encoding="utf-8")


def render_dashboard_template(replacements: dict[str, str]) -> str:
    html = load_dashboard_template()
    for key, value in replacements.items():
        html = html.replace(key, value)
    return html


def build_dashboard_html(
    tasks: list[Task],
    *,
    now: datetime | None = None,
    done_retention_days: int = OWNER_DONE_RETENTION_DAYS,
) -> str:
    tasks_json = json.dumps(build_task_payload(tasks))
    owners_html = owner_cards_html(
        tasks,
        now=now,
        done_retention_days=done_retention_days,
    )
    role_legend = "".join(
        f'<div class="role-chip"><span class="role-swatch" style="background:{owner_color(owner)}"></span>{escape(owner)}</div>'
        for owner in owner_order(tasks)
    )
    role_legend += f'<div class="role-chip"><span class="role-swatch" style="background:{DONE_GREEN}"></span>Done</div>'
    role_legend += f'<div class="role-chip"><span class="role-swatch" style="background:{PAUSED_GRAY}"></span>Paused</div>'

    return render_dashboard_template(
        {
            "__TASKS_JSON__": tasks_json,
            "__OWNERS_HTML__": owners_html,
            "__ROLE_LEGEND__": role_legend,
            "__DASHBOARD_CSS__": load_dashboard_asset("dashboard.css"),
            "__DASHBOARD_JS__": load_dashboard_asset("dashboard.js"),
            "__BG__": BG,
            "__PANEL__": PANEL,
            "__PANEL_2__": PANEL_2,
            "__BORDER__": BORDER,
            "__TEXT__": TEXT,
            "__TEXT_MUTED__": TEXT_MUTED,
            "__TEXT_MUTED2__": TEXT_MUTED2,
            "__TEXT_MUTED3__": TEXT_MUTED3,
            "__DONE_GREEN__": DONE_GREEN,
            "__PAUSED_GRAY__": PAUSED_GRAY,
            "__ACTIVE_BLUE__": ACTIVE_BLUE,
        }
    )
