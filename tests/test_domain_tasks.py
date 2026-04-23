from __future__ import annotations

import unittest

from src.domain.tasks import (
    Task,
    build_business_key,
    normalize_owner,
    normalize_union_mode,
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


if __name__ == "__main__":
    unittest.main()
