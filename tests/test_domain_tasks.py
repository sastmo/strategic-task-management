from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from src.domain.tasks import (
    Task,
    build_business_key,
    normalize_owner,
    normalize_union_mode,
    owner_view_visible,
    task_status,
)


class TaskDomainTests(unittest.TestCase):
    def test_normalize_owner_uses_unassigned_for_blank_values(self) -> None:
        self.assertEqual(normalize_owner(""), "Unassigned")
        self.assertEqual(normalize_owner(None), "Unassigned")

    def test_build_business_key_slugifies_all_parts(self) -> None:
        business_key = build_business_key("Main Source", "R&D Team", "Task 42/A")
        self.assertEqual(business_key, "main-source::r-d-team::task-42-a")

    def test_task_status_prefers_done_over_paused(self) -> None:
        task = Task(
            id="t-1",
            name="Finish migration",
            owner="Platform",
            current_impact=80,
            future_impact=90,
            progress=100,
            done=True,
            paused=True,
        )

        self.assertEqual(task_status(task), "done")

    def test_normalize_union_mode_rejects_unknown_modes(self) -> None:
        with self.assertRaises(ValueError):
            normalize_union_mode("merge")

    def test_owner_view_visible_done_with_no_completed_at_is_always_visible(self) -> None:
        task = Task(
            id="t-no-date",
            name="No completion date",
            owner="Ops",
            current_impact=50,
            future_impact=60,
            progress=100,
            done=True,
            completed_at=None,
        )
        self.assertTrue(owner_view_visible(task))

    def test_owner_view_visible_hides_old_done_tasks(self) -> None:
        now = datetime(2026, 4, 26, tzinfo=UTC)
        old_done_task = Task(
            id="t-2",
            name="Archive me",
            owner="Marketing",
            current_impact=20,
            future_impact=30,
            progress=100,
            done=True,
            completed_at=now - timedelta(days=15),
        )
        recent_done_task = Task(
            id="t-3",
            name="Keep me",
            owner="Marketing",
            current_impact=25,
            future_impact=35,
            progress=100,
            done=True,
            completed_at=now - timedelta(days=5),
        )

        self.assertFalse(owner_view_visible(old_done_task, now=now))
        self.assertTrue(owner_view_visible(recent_done_task, now=now))


if __name__ == "__main__":
    unittest.main()
