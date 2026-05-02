from __future__ import annotations

import sys
import types
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Literal
from unittest.mock import MagicMock, patch

from src.application.auth_service import AuthorizationContext
from src.domain.identity import AuthenticatedUser, PermissionSet


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self._last_query = ""

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object | None,
    ) -> Literal[False]:
        return False

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> None:
        self._last_query = query
        self.connection.executed.append((query, params))

    def fetchone(self) -> tuple[bool] | None:
        if "pg_try_advisory_lock" in self._last_query:
            return (self.connection.lock_acquired,)
        if "pg_advisory_unlock" in self._last_query:
            return (True,)
        return None


class FakeConnection:
    def __init__(self, *, lock_acquired: bool = True) -> None:
        self.lock_acquired = lock_acquired
        self.executed: list[tuple[str, tuple[object, ...] | None]] = []
        self.commit_count = 0
        self.rollback_count = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1


class FakeTaskStore:
    def __init__(
        self,
        connection: FakeConnection,
        *,
        merge_stats: dict[str, int] | None = None,
        merge_error: Exception | None = None,
        run_id: int = 42,
    ) -> None:
        self.connection = connection
        self.merge_stats = merge_stats or {
            "inserted_count": 3,
            "updated_count": 2,
            "deleted_count": 1,
            "unchanged_count": 4,
        }
        self.merge_error = merge_error
        self.run_id = run_id
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def ensure_database_objects(self, *, allow_bootstrap: bool) -> None:
        self.calls.append(("ensure_database_objects", {"allow_bootstrap": allow_bootstrap}))

    def create_ingestion_run(self, **kwargs: Any) -> int:
        self.calls.append(("create_ingestion_run", kwargs))
        return self.run_id

    def log_event(self, **kwargs: Any) -> None:
        self.calls.append(("log_event", kwargs))

    def stage_task_data(self, **kwargs: Any) -> None:
        self.calls.append(("stage_task_data", kwargs))

    def merge_staged_data(self, **kwargs: Any) -> dict[str, int]:
        self.calls.append(("merge_staged_data", kwargs))
        if self.merge_error is not None:
            raise self.merge_error
        return self.merge_stats

    def finalize_ingestion_run(self, **kwargs: Any) -> None:
        self.calls.append(("finalize_ingestion_run", kwargs))

    def prune_old_staging_data(self) -> None:
        self.calls.append(("prune_old_staging_data", {}))


class TaskSyncFlowTests(unittest.TestCase):
    def _make_batch(self) -> SimpleNamespace:
        source_config = SimpleNamespace(union_mode="union")
        return SimpleNamespace(
            resolved_sources=[
                SimpleNamespace(source_name="beta"),
                SimpleNamespace(source_name="alpha"),
                SimpleNamespace(source_name="alpha"),
            ],
            source_config=source_config,
            source_count=2,
            frame_count=2,
            staged_frame=[{"id": 1}, {"id": 2}, {"id": 3}],
            current_frame=[{"id": 1}, {"id": 2}],
            source_config_payload=lambda: {"sources": ["alpha", "beta"], "union_mode": "union"},
        )

    @contextmanager
    def _pooled_connection(self, connection: FakeConnection) -> Iterator[FakeConnection]:
        yield connection

    def test_sync_to_database_returns_summary_for_successful_run(self) -> None:
        from src.application.task_sync import sync_to_database

        batch = self._make_batch()
        connection = FakeConnection(lock_acquired=True)
        store = FakeTaskStore(connection)

        with patch("src.application.task_sync.load_task_batch", return_value=batch):
            with patch(
                "src.infrastructure.db.pooled_connection",
                side_effect=lambda _url: self._pooled_connection(connection),
            ):
                with patch(
                    "src.infrastructure.db.database_schema_bootstrap_enabled",
                    return_value=True,
                ):
                    with patch(
                        "src.infrastructure.task_store.TaskWarehouseStore",
                        return_value=store,
                    ):
                        summary = sync_to_database("tasks.csv", "postgresql://fake/db")

        self.assertEqual(summary.run_id, 42)
        self.assertEqual(summary.inserted_count, 3)
        self.assertEqual(summary.updated_count, 2)
        self.assertEqual(summary.deleted_count, 1)
        self.assertEqual(summary.unchanged_count, 4)
        self.assertEqual(summary.source_count, 2)
        self.assertEqual(summary.frame_count, 2)
        self.assertGreaterEqual(connection.commit_count, 2)
        self.assertGreaterEqual(connection.rollback_count, 2)

        finalized = [
            payload
            for call_name, payload in store.calls
            if call_name == "finalize_ingestion_run"
        ]
        self.assertEqual(finalized[-1]["status"], "success")

        events = [
            payload["event_type"]
            for call_name, payload in store.calls
            if call_name == "log_event"
        ]
        self.assertEqual(events, ["task_sync.started", "task_sync.completed"])

    def test_sync_to_database_raises_conflict_when_lock_is_unavailable(self) -> None:
        from src.application.task_sync import SyncLockConflict, sync_to_database

        batch = self._make_batch()
        connection = FakeConnection(lock_acquired=False)
        store = FakeTaskStore(connection)

        with patch("src.application.task_sync.load_task_batch", return_value=batch):
            with patch(
                "src.infrastructure.db.pooled_connection",
                side_effect=lambda _url: self._pooled_connection(connection),
            ):
                with patch(
                    "src.infrastructure.db.database_schema_bootstrap_enabled",
                    return_value=False,
                ):
                    with patch(
                        "src.infrastructure.task_store.TaskWarehouseStore",
                        return_value=store,
                    ):
                        with self.assertRaises(SyncLockConflict):
                            sync_to_database("tasks.csv", "postgresql://fake/db")

        self.assertEqual(store.calls, [])
        executed_sql = [query for query, _params in connection.executed]
        self.assertTrue(any("pg_try_advisory_lock" in query for query in executed_sql))
        self.assertFalse(any("pg_advisory_unlock" in query for query in executed_sql))

    def test_sync_to_database_marks_run_failed_when_merge_raises(self) -> None:
        from src.application.task_sync import sync_to_database

        batch = self._make_batch()
        connection = FakeConnection(lock_acquired=True)
        store = FakeTaskStore(connection, merge_error=RuntimeError("merge failed"))

        with patch("src.application.task_sync.load_task_batch", return_value=batch):
            with patch(
                "src.infrastructure.db.pooled_connection",
                side_effect=lambda _url: self._pooled_connection(connection),
            ):
                with patch(
                    "src.infrastructure.db.database_schema_bootstrap_enabled",
                    return_value=True,
                ):
                    with patch(
                        "src.infrastructure.task_store.TaskWarehouseStore",
                        return_value=store,
                    ):
                        with self.assertRaisesRegex(RuntimeError, "merge failed"):
                            sync_to_database("tasks.csv", "postgresql://fake/db")

        finalized = [
            payload
            for call_name, payload in store.calls
            if call_name == "finalize_ingestion_run"
        ]
        self.assertEqual(finalized[-1]["status"], "failed")
        self.assertIn("merge failed", finalized[-1]["error_message"])

        events = [
            payload["event_type"]
            for call_name, payload in store.calls
            if call_name == "log_event"
        ]
        self.assertEqual(events, ["task_sync.started", "task_sync.failed"])

        executed_sql = [query for query, _params in connection.executed]
        self.assertTrue(any("pg_advisory_unlock" in query for query in executed_sql))


class AuthUiRenderingTests(unittest.TestCase):
    def _make_user(self) -> AuthenticatedUser:
        return AuthenticatedUser(
            user_key="email::person@example.com",
            email="person@example.com",
            display_name="Person Example",
            auth_source="entra",
        )

    def _make_context(
        self,
        *,
        state: str,
        roles: tuple[str, ...] = ("viewer",),
        sign_in_url: str | None = None,
        sign_out_url: str | None = None,
        diagnostics: tuple[str, ...] = (),
        user: AuthenticatedUser | None = None,
    ) -> AuthorizationContext:
        return AuthorizationContext(
            state=state,  # type: ignore[arg-type]
            user=user,
            permissions=PermissionSet(roles=roles),  # type: ignore[arg-type]
            message="Authorization message",
            auth_mode="app_service",
            sign_in_url=sign_in_url,
            sign_out_url=sign_out_url,
            diagnostics=diagnostics,
        )

    def test_render_authorization_gate_returns_true_for_authorized_user(self) -> None:
        import src.presentation.auth_ui as auth_ui

        auth_context = self._make_context(
            state="authorized",
            user=self._make_user(),
        )

        with patch.object(auth_ui.st, "warning") as warning:
            with patch.object(auth_ui.st, "error") as error:
                with patch.object(auth_ui.st, "link_button") as link_button:
                    allowed = auth_ui.render_authorization_gate(auth_context)

        self.assertTrue(allowed)
        warning.assert_not_called()
        error.assert_not_called()
        link_button.assert_not_called()

    def test_render_authorization_gate_shows_sign_in_for_authentication_required(self) -> None:
        import src.presentation.auth_ui as auth_ui

        auth_context = self._make_context(
            state="authentication_required",
            roles=(),
            sign_in_url="https://example.com/sign-in",
        )

        with patch.object(auth_ui.st, "warning") as warning:
            with patch.object(auth_ui.st, "error") as error:
                with patch.object(auth_ui.st, "link_button") as link_button:
                    allowed = auth_ui.render_authorization_gate(auth_context)

        self.assertFalse(allowed)
        warning.assert_called_once_with("Authorization message")
        error.assert_not_called()
        link_button.assert_called_once_with(
            "Sign in with Microsoft",
            "https://example.com/sign-in",
            type="primary",
        )

    def test_render_authorization_gate_shows_sign_out_and_diagnostics(self) -> None:
        import src.presentation.auth_ui as auth_ui

        auth_context = self._make_context(
            state="access_denied",
            roles=(),
            sign_out_url="https://example.com/sign-out",
            diagnostics=("missing-group",),
        )

        with patch.object(auth_ui.st, "error") as error:
            with patch.object(auth_ui.st, "link_button") as link_button:
                with patch.object(auth_ui.st, "caption") as caption:
                    allowed = auth_ui.render_authorization_gate(auth_context)

        self.assertFalse(allowed)
        error.assert_called_once_with("Authorization message")
        link_button.assert_called_once_with("Sign out", "https://example.com/sign-out")
        caption.assert_called_once_with("missing-group")

    def test_render_authorization_gate_offers_retry_when_only_sign_in_url_is_available(self) -> None:
        import src.presentation.auth_ui as auth_ui

        auth_context = self._make_context(
            state="access_denied",
            roles=(),
            sign_in_url="https://example.com/sign-in",
        )

        with patch.object(auth_ui.st, "error"):
            with patch.object(auth_ui.st, "link_button") as link_button:
                with patch.object(auth_ui.st, "caption") as caption:
                    auth_ui.render_authorization_gate(auth_context)

        link_button.assert_called_once_with(
            "Try sign in again",
            "https://example.com/sign-in",
        )
        caption.assert_not_called()

    def test_render_user_status_noops_when_panel_is_hidden(self) -> None:
        import src.presentation.auth_ui as auth_ui

        auth_context = self._make_context(
            state="authorized",
            user=self._make_user(),
        )

        with patch.object(auth_ui.st, "columns") as columns:
            auth_ui.render_user_status(auth_context, show_status_panel=False)

        columns.assert_not_called()

    def test_render_user_status_renders_label_and_sign_out_button(self) -> None:
        import src.presentation.auth_ui as auth_ui

        left_column = MagicMock()
        right_column = MagicMock()
        auth_context = self._make_context(
            state="authorized",
            roles=("admin",),
            sign_out_url="https://example.com/sign-out",
            user=self._make_user(),
        )

        with patch.object(auth_ui.st, "columns", return_value=(left_column, right_column)) as columns:
            auth_ui.render_user_status(auth_context)

        columns.assert_called_once_with([6, 1])
        left_column.caption.assert_called_once()
        rendered_label = left_column.caption.call_args.args[0]
        self.assertIn("Person Example", rendered_label)
        self.assertIn("person@example.com", rendered_label)
        self.assertIn("role: admin", rendered_label)
        self.assertIn("auth: entra", rendered_label)
        right_column.link_button.assert_called_once_with(
            "Sign out",
            "https://example.com/sign-out",
        )


class AzureHelperTests(unittest.TestCase):
    def test_azure_package_reexports_helper_functions(self) -> None:
        import src.infrastructure.azure as azure_package
        import src.infrastructure.azure.credentials as credentials

        self.assertIs(
            azure_package.has_azure_identity_support,
            credentials.has_azure_identity_support,
        )
        self.assertIs(
            azure_package.get_default_azure_credential,
            credentials.get_default_azure_credential,
        )

    def test_has_azure_identity_support_false_when_dependency_is_missing(self) -> None:
        import src.infrastructure.azure.credentials as credentials

        original_import = __import__

        def fake_import(
            name: str,
            globals_dict: dict[str, object] | None = None,
            locals_dict: dict[str, object] | None = None,
            fromlist: tuple[str, ...] = (),
            level: int = 0,
        ) -> Any:
            if name == "azure.identity":
                raise ImportError("missing azure.identity")
            return original_import(name, globals_dict, locals_dict, fromlist, level)

        with patch("builtins.__import__", side_effect=fake_import):
            self.assertFalse(credentials.has_azure_identity_support())

    def test_has_azure_identity_support_true_when_dependency_is_available(self) -> None:
        import src.infrastructure.azure.credentials as credentials

        azure_package = types.ModuleType("azure")
        azure_package.__path__ = []  # type: ignore[attr-defined]
        azure_identity = types.ModuleType("azure.identity")
        azure_package.identity = azure_identity  # type: ignore[attr-defined]

        with patch.dict(
            sys.modules,
            {"azure": azure_package, "azure.identity": azure_identity},
            clear=False,
        ):
            self.assertTrue(credentials.has_azure_identity_support())

    def test_get_default_azure_credential_raises_helpful_error_when_missing(self) -> None:
        import src.infrastructure.azure.credentials as credentials

        original_import = __import__

        def fake_import(
            name: str,
            globals_dict: dict[str, object] | None = None,
            locals_dict: dict[str, object] | None = None,
            fromlist: tuple[str, ...] = (),
            level: int = 0,
        ) -> Any:
            if name == "azure.identity":
                raise ImportError("missing azure.identity")
            return original_import(name, globals_dict, locals_dict, fromlist, level)

        with patch("builtins.__import__", side_effect=fake_import):
            with self.assertRaisesRegex(RuntimeError, "azure-identity is required"):
                credentials.get_default_azure_credential()

    def test_get_default_azure_credential_returns_configured_default_credential(self) -> None:
        import src.infrastructure.azure.credentials as credentials

        azure_package = types.ModuleType("azure")
        azure_package.__path__ = []  # type: ignore[attr-defined]
        azure_identity = types.ModuleType("azure.identity")
        azure_package.identity = azure_identity  # type: ignore[attr-defined]

        class FakeDefaultAzureCredential:
            def __init__(self, **kwargs: Any) -> None:
                self.kwargs = kwargs

        azure_identity.DefaultAzureCredential = FakeDefaultAzureCredential  # type: ignore[attr-defined]

        with patch.dict(
            sys.modules,
            {"azure": azure_package, "azure.identity": azure_identity},
            clear=False,
        ):
            credential = credentials.get_default_azure_credential()

        self.assertIsInstance(credential, FakeDefaultAzureCredential)
        self.assertEqual(
            credential.kwargs,
            {"exclude_interactive_browser_credential": False},
        )


class LazyExportTests(unittest.TestCase):
    def test_src_getattr_imports_known_export(self) -> None:
        import src

        fake_module = SimpleNamespace(Task="sentinel-task")
        with patch.object(src, "import_module", return_value=fake_module) as import_module:
            value = src.__getattr__("Task")

        self.assertEqual(value, "sentinel-task")
        import_module.assert_called_once_with("src.domain.tasks")

    def test_src_getattr_raises_for_unknown_export(self) -> None:
        import src

        with self.assertRaisesRegex(AttributeError, "missing_export"):
            src.__getattr__("missing_export")

    def test_infrastructure_getattr_imports_known_export(self) -> None:
        import src.infrastructure as infrastructure

        fake_module = SimpleNamespace(parse_source_config="sentinel-parser")
        with patch.object(
            infrastructure,
            "import_module",
            return_value=fake_module,
        ) as import_module:
            value = infrastructure.__getattr__("parse_source_config")

        self.assertEqual(value, "sentinel-parser")
        import_module.assert_called_once_with("src.infrastructure.sources")

    def test_infrastructure_getattr_raises_for_unknown_export(self) -> None:
        import src.infrastructure as infrastructure

        with self.assertRaisesRegex(AttributeError, "missing_export"):
            infrastructure.__getattr__("missing_export")


if __name__ == "__main__":
    unittest.main()
