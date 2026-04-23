from __future__ import annotations

import unittest

import pandas as pd

from src.infrastructure.task_frames import normalize_task_frame, resolve_current_frame


class TaskFrameTests(unittest.TestCase):
    def test_normalize_task_frame_maps_aliases_and_builds_keys(self) -> None:
        raw_frame = pd.DataFrame(
            [
                {
                    "Task Name": "North Star Initiative",
                    "Department": "Strategy",
                    "Current Impact": "85",
                    "Future Impact": "95",
                    "Progress": "40",
                    "Status": "active",
                    "source_name": "master_plan",
                    "source_kind": "excel",
                    "source_sheet": "Sheet1",
                }
            ]
        )

        normalized = normalize_task_frame(raw_frame)
        row = normalized.iloc[0]

        self.assertEqual(row["owner"], "Strategy")
        self.assertEqual(row["source_task_id"], "north-star-initiative")
        self.assertEqual(row["business_key"], "master-plan::strategy::north-star-initiative")
        self.assertIn("record_hash", normalized.columns)

    def test_resolve_current_frame_union_prefers_higher_priority(self) -> None:
        staged = pd.DataFrame(
            [
                {
                    "business_key": "feed::ops::task-1",
                    "source_task_id": "task-1",
                    "name": "Legacy version",
                    "owner": "Ops",
                    "current_impact": 40,
                    "future_impact": 50,
                    "progress": 20,
                    "done": False,
                    "paused": False,
                    "record_hash": "aaa",
                    "source_name": "feed",
                    "source_kind": "csv",
                    "source_sheet": "",
                    "source_path": "/tmp/old.csv",
                    "source_priority": 100,
                    "source_order": 1,
                    "source_row_number": 1,
                },
                {
                    "business_key": "feed::ops::task-1",
                    "source_task_id": "task-1",
                    "name": "Preferred version",
                    "owner": "Ops",
                    "current_impact": 60,
                    "future_impact": 70,
                    "progress": 80,
                    "done": False,
                    "paused": False,
                    "record_hash": "bbb",
                    "source_name": "feed",
                    "source_kind": "excel",
                    "source_sheet": "Q1",
                    "source_path": "/tmp/new.xlsx",
                    "source_priority": 300,
                    "source_order": 2,
                    "source_row_number": 1,
                },
            ]
        )

        current = resolve_current_frame(staged, union_mode="union")

        self.assertEqual(len(current), 1)
        self.assertEqual(current.iloc[0]["record_id"], "feed::ops::task-1")
        self.assertEqual(current.iloc[0]["name"], "Preferred version")

    def test_resolve_current_frame_union_all_preserves_duplicates(self) -> None:
        staged = pd.DataFrame(
            [
                {
                    "business_key": "feed::ops::task-1",
                    "source_task_id": "task-1",
                    "name": "One",
                    "owner": "Ops",
                    "current_impact": 10,
                    "future_impact": 20,
                    "progress": 30,
                    "done": False,
                    "paused": False,
                    "record_hash": "aaa",
                    "source_name": "feed",
                    "source_kind": "csv",
                    "source_sheet": "",
                    "source_path": "/tmp/a.csv",
                    "source_priority": 100,
                    "source_order": 1,
                    "source_row_number": 1,
                },
                {
                    "business_key": "feed::ops::task-1",
                    "source_task_id": "task-1",
                    "name": "Two",
                    "owner": "Ops",
                    "current_impact": 11,
                    "future_impact": 21,
                    "progress": 31,
                    "done": False,
                    "paused": False,
                    "record_hash": "bbb",
                    "source_name": "feed",
                    "source_kind": "csv",
                    "source_sheet": "",
                    "source_path": "/tmp/b.csv",
                    "source_priority": 100,
                    "source_order": 2,
                    "source_row_number": 1,
                },
            ]
        )

        current = resolve_current_frame(staged, union_mode="union_all")

        self.assertEqual(len(current), 2)
        self.assertEqual(current.iloc[0]["record_id"], "feed::ops::task-1")
        self.assertEqual(current.iloc[1]["record_id"], "feed::ops::task-1::dup2")


if __name__ == "__main__":
    unittest.main()
