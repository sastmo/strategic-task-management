from __future__ import annotations

from contextlib import closing
import os
import types
import unittest

import pandas as pd

try:
    import psycopg
except ImportError:  # pragma: no cover - optional local dependency
    psycopg = None

task_store_module: types.ModuleType | None
try:
    import src.infrastructure.task_store as task_store_module
except ImportError:  # pragma: no cover - optional local dependency
    task_store_module = None


TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL", "").strip()


def staged_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def current_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


class TaskStoreIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not TEST_DATABASE_URL:
            raise unittest.SkipTest("Set TEST_DATABASE_URL to run database integration tests.")
        if psycopg is None or task_store_module is None:
            raise unittest.SkipTest("psycopg is not available in this environment.")

    def setUp(self) -> None:
        assert psycopg is not None
        assert task_store_module is not None
        self.connection = psycopg.connect(TEST_DATABASE_URL)
        self.store = task_store_module.TaskWarehouseStore(self.connection)
        self._drop_test_schemas()
        self.store.ensure_database_objects()

    def tearDown(self) -> None:
        try:
            self._drop_test_schemas()
        finally:
            self.connection.close()

    def test_merge_tracks_updates_deletes_and_completion_dates(self) -> None:
        run_1 = self.store.create_ingestion_run(
            source_config={"sources": ["test"]},
            union_mode="union",
            source_count=1,
            frame_count=1,
            staged_row_count=2,
            current_row_count=2,
        )
        self.store.stage_task_data(
            run_id=run_1,
            staged_frame=staged_frame(
                [
                    self._task_row("feed::ops::t1", "ops::t1", "T1", "Task One", progress=25),
                    self._task_row("feed::ops::t2", "ops::t2", "T2", "Task Two", progress=40),
                ]
            ),
            current_frame=current_frame(
                [
                    self._task_row("feed::ops::t1", "ops::t1", "T1", "Task One", progress=25),
                    self._task_row("feed::ops::t2", "ops::t2", "T2", "Task Two", progress=40),
                ]
            ),
        )
        counts_1 = self.store.merge_staged_data(run_id=run_1, source_names=["feed"])
        self.store.finalize_ingestion_run(
            run_id=run_1,
            status="success",
            inserted_count=counts_1["inserted_count"],
            updated_count=counts_1["updated_count"],
            deleted_count=counts_1["deleted_count"],
            unchanged_count=counts_1["unchanged_count"],
        )
        self.connection.commit()

        self.assertEqual(counts_1["inserted_count"], 2)
        self.assertEqual(counts_1["updated_count"], 0)
        self.assertEqual(counts_1["deleted_count"], 0)

        run_2 = self.store.create_ingestion_run(
            source_config={"sources": ["test"]},
            union_mode="union",
            source_count=1,
            frame_count=1,
            staged_row_count=1,
            current_row_count=1,
        )
        self.store.stage_task_data(
            run_id=run_2,
            staged_frame=staged_frame(
                [
                    self._task_row(
                        "feed::ops::t1",
                        "ops::t1",
                        "T1",
                        "Task One",
                        progress=100,
                        done=True,
                        record_hash="hash-1-done",
                    ),
                ]
            ),
            current_frame=current_frame(
                [
                    self._task_row(
                        "feed::ops::t1",
                        "ops::t1",
                        "T1",
                        "Task One",
                        progress=100,
                        done=True,
                        record_hash="hash-1-done",
                    ),
                ]
            ),
        )
        counts_2 = self.store.merge_staged_data(run_id=run_2, source_names=["feed"])
        self.store.finalize_ingestion_run(
            run_id=run_2,
            status="success",
            inserted_count=counts_2["inserted_count"],
            updated_count=counts_2["updated_count"],
            deleted_count=counts_2["deleted_count"],
            unchanged_count=counts_2["unchanged_count"],
        )
        self.connection.commit()

        self.assertEqual(counts_2["inserted_count"], 0)
        self.assertEqual(counts_2["updated_count"], 1)
        self.assertEqual(counts_2["deleted_count"], 1)

        live_tasks = self.store.load_current_tasks()
        self.assertEqual(len(live_tasks), 1)
        self.assertEqual(live_tasks[0].id, "feed::ops::t1")
        self.assertTrue(live_tasks[0].done)
        self.assertIsNotNone(live_tasks[0].completed_at)

        with closing(self.connection.cursor()) as cursor:
            cursor.execute(
                """
                SELECT is_deleted, completed_at
                FROM warehouse.tasks_current
                WHERE record_id = %s
                """,
                ("feed::ops::t2",),
            )
            deleted_row = cursor.fetchone()

            cursor.execute("SELECT COUNT(*) FROM warehouse.task_history")
            history_count = cursor.fetchone()[0]

        self.assertIsNotNone(deleted_row)
        self.assertTrue(deleted_row[0])
        self.assertGreaterEqual(history_count, 3)

    def _drop_test_schemas(self) -> None:
        with closing(self.connection.cursor()) as cursor:
            cursor.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
            cursor.execute("DROP SCHEMA IF EXISTS staging CASCADE")
            cursor.execute("DROP SCHEMA IF EXISTS ops CASCADE")
            cursor.execute("DROP SCHEMA IF EXISTS app CASCADE")
        self.connection.commit()

    @staticmethod
    def _task_row(
        record_id: str,
        business_key: str,
        source_task_id: str,
        name: str,
        *,
        progress: int,
        done: bool = False,
        paused: bool = False,
        record_hash: str | None = None,
    ) -> dict[str, object]:
        return {
            "record_id": record_id,
            "business_key": business_key,
            "source_task_id": source_task_id,
            "name": name,
            "owner": "Ops",
            "current_impact": 60,
            "future_impact": 80,
            "progress": progress,
            "done": done,
            "paused": paused,
            "record_hash": record_hash or f"hash-{record_id}-{progress}-{done}-{paused}",
            "source_name": "feed",
            "source_kind": "csv",
            "source_sheet": "",
            "source_path": "/tmp/tasks.csv",
            "source_priority": 100,
            "source_order": 1,
            "source_row_number": 1,
        }


if __name__ == "__main__":
    unittest.main()
