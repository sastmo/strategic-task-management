from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import time
import unittest

import pandas as pd

from src.application.auto_sync import build_source_snapshot, determine_sync_reason
from src.application.task_workflow import load_task_batch
from src.domain.tasks import Task
from src.presentation.dashboard import build_dashboard_html, owner_cards_html


class ApplicationBehaviorTests(unittest.TestCase):
    def test_load_task_batch_supports_csv_and_multi_sheet_excel(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace = Path(temp_dir)
            csv_path = workspace / "ops.csv"
            excel_path = workspace / "planning.xlsx"

            pd.DataFrame(
                [
                    {
                        "id": "csv-1",
                        "name": "CSV Task",
                        "owner": "Operations",
                        "currentImpact": 40,
                        "futureImpact": 60,
                        "progress": 25,
                    }
                ]
            ).to_csv(csv_path, index=False)

            with pd.ExcelWriter(excel_path) as writer:
                pd.DataFrame(
                    [
                        {
                            "Task ID": "dup-1",
                            "Task Name": "Workbook Task A",
                            "Department": "Sales",
                            "Current Impact": 80,
                            "Future Impact": 90,
                            "Progress": 50,
                        }
                    ]
                ).to_excel(writer, sheet_name="SheetA", index=False)
                pd.DataFrame(
                    [
                        {
                            "Task ID": "dup-1",
                            "Task Name": "Workbook Task A Duplicate",
                            "Department": "Sales",
                            "Current Impact": 82,
                            "Future Impact": 91,
                            "Progress": 52,
                        }
                    ]
                ).to_excel(writer, sheet_name="SheetB", index=False)

            batch = load_task_batch(
                {
                    "sources": [
                        {"source": str(csv_path), "source_name": "ops_feed", "source_priority": 200},
                        {"source": str(excel_path), "source_name": "planning_book", "all_sheets": True},
                    ],
                    "union_mode": "union",
                }
            )

        self.assertEqual(batch.source_count, 2)
        self.assertEqual(batch.frame_count, 3)
        self.assertEqual(len(batch.staged_frame), 3)
        self.assertEqual(len(batch.current_frame), 2)

    def test_determine_sync_reason_covers_start_change_and_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace = Path(temp_dir)
            source_file = workspace / "tasks.csv"
            source_file.write_text(
                "name,currentImpact,futureImpact,progress\nTask A,10,20,30\n",
                encoding="utf-8",
            )
            first_snapshot = build_source_snapshot(str(workspace))

            self.assertEqual(
                determine_sync_reason(
                    snapshot=first_snapshot,
                    last_synced_fingerprint=None,
                    last_success_at=None,
                    last_attempt_at=None,
                    last_attempt_failed=False,
                    now=time.monotonic(),
                    refresh_seconds=1800,
                    retry_seconds=120,
                ),
                "startup",
            )

            source_file.write_text(
                "name,currentImpact,futureImpact,progress\nTask B,10,20,30\n",
                encoding="utf-8",
            )
            second_snapshot = build_source_snapshot(str(workspace))

        self.assertEqual(
            determine_sync_reason(
                snapshot=second_snapshot,
                last_synced_fingerprint=first_snapshot.fingerprint,
                last_success_at=time.monotonic(),
                last_attempt_at=time.monotonic(),
                last_attempt_failed=False,
                now=time.monotonic(),
                refresh_seconds=1800,
                retry_seconds=120,
            ),
            "source_change",
        )
        self.assertEqual(
            determine_sync_reason(
                snapshot=second_snapshot,
                last_synced_fingerprint=second_snapshot.fingerprint,
                last_success_at=time.monotonic() - 4000,
                last_attempt_at=time.monotonic(),
                last_attempt_failed=False,
                now=time.monotonic(),
                refresh_seconds=1800,
                retry_seconds=120,
            ),
            "scheduled_refresh",
        )

    def test_dashboard_template_renders_without_placeholders(self) -> None:
        html = build_dashboard_html(
            [
                Task(
                    id="a",
                    name="Task A",
                    owner="Ops",
                    current_impact=50,
                    future_impact=70,
                    progress=40,
                ),
                Task(
                    id="b",
                    name="Task B",
                    owner="Sales",
                    current_impact=80,
                    future_impact=90,
                    progress=100,
                    done=True,
                ),
            ]
        )

        self.assertIn("<!doctype html>", html)
        self.assertIn("Strategic Positioning", html)
        self.assertNotIn("__TASKS_JSON__", html)

    def test_owner_cards_hide_done_tasks_older_than_retention_window(self) -> None:
        now = datetime(2026, 4, 26, tzinfo=timezone.utc)
        html = owner_cards_html(
            [
                Task(
                    id="active-1",
                    name="Active Task",
                    owner="Ops",
                    current_impact=50,
                    future_impact=70,
                    progress=40,
                ),
                Task(
                    id="done-old",
                    name="Old Done Task",
                    owner="Ops",
                    current_impact=60,
                    future_impact=80,
                    progress=100,
                    done=True,
                    completed_at=now - timedelta(days=20),
                ),
                Task(
                    id="done-recent",
                    name="Recent Done Task",
                    owner="Ops",
                    current_impact=65,
                    future_impact=82,
                    progress=100,
                    done=True,
                    completed_at=now - timedelta(days=4),
                ),
            ],
            now=now,
        )

        self.assertIn("Active Task", html)
        self.assertIn("Recent Done Task", html)
        self.assertNotIn("Old Done Task", html)


if __name__ == "__main__":
    unittest.main()
