"""Regression tests for the security and data-loss fixes applied in the review.

Each test class is self-contained and documents the exact issue it covers.
Tests that touch the database are gated on TEST_DATABASE_URL being set.
"""
from __future__ import annotations

import ipaddress
import json
import os
import time
import unittest
from contextlib import closing
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Auth write commit (Fix 1)
# ---------------------------------------------------------------------------

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover
    psycopg = None  # type: ignore[assignment]

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL", "").strip()


class AuthWriteCommitTests(unittest.TestCase):
    """Auth database writes must persist after open_user_access_repository exits."""

    @classmethod
    def setUpClass(cls) -> None:
        if not TEST_DATABASE_URL:
            raise unittest.SkipTest("Set TEST_DATABASE_URL to run database integration tests.")
        if psycopg is None:
            raise unittest.SkipTest("psycopg is not available.")

    def setUp(self) -> None:
        assert psycopg is not None
        self.connection = psycopg.connect(TEST_DATABASE_URL)
        self._drop_schemas()
        self.connection.commit()

    def tearDown(self) -> None:
        try:
            self._drop_schemas()
        finally:
            self.connection.close()

    def _drop_schemas(self) -> None:
        with closing(self.connection.cursor()) as cur:
            cur.execute("DROP SCHEMA IF EXISTS app CASCADE")
            cur.execute("DROP SCHEMA IF EXISTS ops CASCADE")
        self.connection.commit()

    def test_upsert_user_persists_after_context_exit(self) -> None:
        """upsert_user data must survive after open_user_access_repository exits."""
        import src.infrastructure.user_repository as repo_mod
        from src.domain.identity import AuthenticatedUser

        with patch.dict("os.environ", {"DB_BOOTSTRAP_SCHEMA": "1"}, clear=False):
            with repo_mod.open_user_access_repository(TEST_DATABASE_URL) as repo:
                assert repo is not None
                user = AuthenticatedUser(
                    user_key="email::test@example.com",
                    email="test@example.com",
                    display_name="Test User",
                    auth_source="local",
                )
                repo.upsert_user(user)

        # Open a fresh connection to verify the row actually committed.
        with psycopg.connect(TEST_DATABASE_URL) as verify_conn:
            with verify_conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT email FROM app.users WHERE user_key = %s",
                    ("email::test@example.com",),
                )
                row = cur.fetchone()

        self.assertIsNotNone(row, "upsert_user data was rolled back instead of committed")
        assert row is not None
        self.assertEqual(row["email"], "test@example.com")

    def test_log_event_persists_after_context_exit(self) -> None:
        """log_event data must survive after open_user_access_repository exits."""
        import src.infrastructure.user_repository as repo_mod

        with patch.dict("os.environ", {"DB_BOOTSTRAP_SCHEMA": "1"}, clear=False):
            with repo_mod.open_user_access_repository(TEST_DATABASE_URL) as repo:
                assert repo is not None
                repo.log_event(
                    event_type="test.event",
                    actor_type="system",
                    actor_id="test",
                    payload={"check": "committed"},
                )

        with psycopg.connect(TEST_DATABASE_URL) as verify_conn:
            with verify_conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT payload FROM app.event_log WHERE event_type = %s",
                    ("test.event",),
                )
                row = cur.fetchone()

        self.assertIsNotNone(row, "log_event data was rolled back instead of committed")
        assert row is not None
        self.assertEqual(row["payload"]["check"], "committed")

    def test_exception_in_context_does_not_commit(self) -> None:
        """An exception inside the with-block must NOT commit partial writes."""
        import src.infrastructure.user_repository as repo_mod
        from src.domain.identity import AuthenticatedUser

        with patch.dict("os.environ", {"DB_BOOTSTRAP_SCHEMA": "1"}, clear=False):
            # Bootstrap first so tables exist.
            with repo_mod.open_user_access_repository(TEST_DATABASE_URL):
                pass

        try:
            with repo_mod.open_user_access_repository(
                TEST_DATABASE_URL, ensure_objects=False
            ) as repo:
                assert repo is not None
                user = AuthenticatedUser(
                    user_key="email::rollback@example.com",
                    email="rollback@example.com",
                    display_name="Should Roll Back",
                    auth_source="local",
                )
                repo.upsert_user(user)
                raise RuntimeError("deliberate failure")
        except RuntimeError:
            pass

        with psycopg.connect(TEST_DATABASE_URL) as verify_conn:
            with verify_conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT email FROM app.users WHERE user_key = %s",
                    ("email::rollback@example.com",),
                )
                row = cur.fetchone()

        self.assertIsNone(row, "Write must be rolled back when an exception escapes the context")


# ---------------------------------------------------------------------------
# Connection pool timeout (Fix 2)
# ---------------------------------------------------------------------------


class ConnectionPoolTimeoutTests(unittest.TestCase):
    """Pool must raise TimeoutError instead of blocking forever when exhausted."""

    def _make_pool(self, max_size: int = 1, borrow_timeout: int = 1):
        from src.infrastructure.db import DatabaseConnectionPool

        pool = DatabaseConnectionPool(
            "postgresql://fake/fake",
            max_size=max_size,
            borrow_timeout=borrow_timeout,
        )
        return pool

    def test_borrow_raises_timeout_when_pool_exhausted(self) -> None:
        """Exhausted pool must raise TimeoutError, not block indefinitely."""
        from src.infrastructure.db import DatabaseConnectionPool

        pool = DatabaseConnectionPool(
            "postgresql://fake/fake",
            max_size=1,
            borrow_timeout=1,
        )

        # Manually fill the pool's capacity counter without an actual connection.
        fake_conn = MagicMock()
        fake_conn.closed = False
        # Simulate OK status so _is_usable returns True.
        try:
            from psycopg.pq import ConnStatus
            fake_conn.pgconn.status = ConnStatus.OK
        except Exception:
            pass

        pool._created = 1  # pretend max capacity is reached
        # _available queue is empty so the pool will try to wait.

        start = time.monotonic()
        with self.assertRaises(TimeoutError):
            pool._borrow_connection()
        elapsed = time.monotonic() - start

        # Must timeout within a couple of seconds (borrow_timeout=1).
        self.assertLess(elapsed, 5.0, "Pool wait exceeded expected timeout window")

    def test_pool_max_size_invalid_string_raises_helpful_error(self) -> None:
        """Non-numeric DB_POOL_MAX_SIZE must raise a descriptive ValueError."""
        with patch.dict("os.environ", {"DB_POOL_MAX_SIZE": "not-a-number"}, clear=False):
            with self.assertRaises(ValueError) as ctx:
                from src.infrastructure import db as db_mod
                db_mod.database_pool_max_size()
            self.assertIn("not-a-number", str(ctx.exception))

    def test_pool_max_size_zero_raises_helpful_error(self) -> None:
        """DB_POOL_MAX_SIZE=0 must raise a descriptive ValueError."""
        with patch.dict("os.environ", {"DB_POOL_MAX_SIZE": "0"}, clear=False):
            with self.assertRaises(ValueError) as ctx:
                from src.infrastructure import db as db_mod
                db_mod.database_pool_max_size()
            self.assertIn("positive integer", str(ctx.exception))


# ---------------------------------------------------------------------------
# XSS — safe JSON for HTML script context (Fix 3)
# ---------------------------------------------------------------------------


class SafeJsonForHtmlTests(unittest.TestCase):
    """Tasks JSON embedded in <script> must not allow </script> breakout."""

    def _safe(self, obj: object) -> str:
        from src.presentation.dashboard import safe_json_for_html
        return safe_json_for_html(obj)

    def test_closing_script_tag_is_escaped(self) -> None:
        payload = {"name": "</script><script>alert(1)</script>"}
        result = self._safe(payload)
        self.assertNotIn("</script>", result)
        self.assertIn("<\\/script>", result)

    def test_html_comment_open_is_escaped(self) -> None:
        payload = {"name": "<!--bad-->"}
        result = self._safe(payload)
        self.assertNotIn("<!--", result)

    def test_normal_task_names_are_preserved(self) -> None:
        payload = {"name": "Deploy to production", "progress": 42}
        result = self._safe(payload)
        parsed = json.loads(result)
        self.assertEqual(parsed["name"], "Deploy to production")
        self.assertEqual(parsed["progress"], 42)

    def test_dashboard_html_does_not_contain_raw_closing_script(self) -> None:
        from src.domain.tasks import Task
        from src.presentation.dashboard import build_dashboard_html

        task = Task(
            id="xss-1",
            name="</script><script>alert('xss')</script>",
            owner="Attacker",
            current_impact=50,
            future_impact=80,
            progress=10,
        )
        html = build_dashboard_html([task])
        # The raw breakout sequence must not appear anywhere in the output.
        self.assertNotIn("</script><script>", html)

    def test_single_pass_template_no_placeholder_collision(self) -> None:
        """A replacement value matching another placeholder must not be re-substituted."""
        from src.presentation.dashboard import render_dashboard_template

        # Patch load_dashboard_template to return a controlled template.
        with patch(
            "src.presentation.dashboard.load_dashboard_template",
            return_value="A=__KEY_A__ B=__KEY_B__",
        ):
            result = render_dashboard_template(
                {"__KEY_A__": "__KEY_B__", "__KEY_B__": "FINAL"}
            )
        # KEY_A was replaced with the literal string "__KEY_B__".
        # In a single-pass substitution that string must NOT be further replaced.
        self.assertEqual(result, "A=__KEY_B__ B=FINAL")


# ---------------------------------------------------------------------------
# SSRF — URL validation (Fix 4)
# ---------------------------------------------------------------------------


class SSRFValidationTests(unittest.TestCase):
    """validate_http_url must block all non-public address ranges."""

    def _validate(self, url: str) -> None:
        from src.infrastructure.sources import validate_http_url
        validate_http_url(url)

    def test_rejects_non_http_scheme(self) -> None:
        with self.assertRaises(ValueError, msg="file:// should be rejected"):
            self._validate("file:///etc/passwd")

    def test_rejects_ftp_scheme(self) -> None:
        with self.assertRaises(ValueError):
            self._validate("ftp://example.com/tasks.csv")

    def test_rejects_localhost_by_name(self) -> None:
        with self.assertRaises(ValueError):
            self._validate("http://localhost/tasks")

    def test_rejects_loopback_ip(self) -> None:
        with self.assertRaises(ValueError):
            self._validate("http://127.0.0.1/tasks")

    def test_rejects_rfc1918_10_block(self) -> None:
        with self.assertRaises(ValueError):
            self._validate("http://10.0.0.1/tasks")

    def test_rejects_rfc1918_172_block(self) -> None:
        with self.assertRaises(ValueError):
            self._validate("http://172.16.0.1/tasks")

    def test_rejects_rfc1918_192_block(self) -> None:
        with self.assertRaises(ValueError):
            self._validate("http://192.168.1.1/tasks")

    def test_rejects_link_local(self) -> None:
        with self.assertRaises(ValueError):
            self._validate("http://169.254.169.254/latest/meta-data/")

    def test_rejects_url_with_no_hostname(self) -> None:
        with self.assertRaises(ValueError):
            self._validate("http:///no-host")

    def test_accepts_public_https_url(self) -> None:
        # DNS resolution is mocked to a public IP so the test is deterministic.
        import socket

        public_ip = "93.184.216.34"  # example.com
        with patch.object(
            socket,
            "getaddrinfo",
            return_value=[(socket.AF_INET, socket.SOCK_STREAM, 0, "", (public_ip, 0))],
        ):
            # Must not raise.
            self._validate("https://example.com/tasks.json")

    def test_blocked_network_constants_are_valid(self) -> None:
        """All entries in _BLOCKED_NETWORKS must be valid ip_network objects."""
        from src.infrastructure.sources import _BLOCKED_NETWORKS
        for net in _BLOCKED_NETWORKS:
            self.assertIsInstance(net, (ipaddress.IPv4Network, ipaddress.IPv6Network))


# ---------------------------------------------------------------------------
# Assert replacement (Fix 6)
# ---------------------------------------------------------------------------


class AssertReplacementTests(unittest.TestCase):
    """Production assert guards must be replaced with explicit runtime errors."""

    def test_normalize_source_spec_raises_type_error_not_assertion(self) -> None:
        """is_graph_source_spec_dict returning True for a non-dict must raise TypeError."""
        from src.infrastructure.sources import normalize_source_spec

        # Monkey-patch is_graph_source_spec_dict to return True for a non-dict.
        with patch(
            "src.infrastructure.sources.is_graph_source_spec_dict",
            return_value=True,
        ):
            with self.assertRaises(TypeError) as ctx:
                normalize_source_spec("not-a-dict")
            self.assertIn("dict", str(ctx.exception).lower())

    def test_sync_raises_runtime_error_not_assertion_on_missing_run_id(self) -> None:
        """sync_to_database must raise RuntimeError (not AssertionError) for logic failures."""
        # We test the guard condition itself, not the full sync flow.
        # Verify the source code no longer contains raw `assert` statements.
        import inspect

        from src.application import task_sync
        source = inspect.getsource(task_sync.sync_to_database)
        self.assertNotIn("\n    assert run_id", source)
        self.assertNotIn("\n    assert merge_stats", source)


# ---------------------------------------------------------------------------
# Recursion depth guard (Fix 13)
# ---------------------------------------------------------------------------


class SourceConfigRecursionDepthTests(unittest.TestCase):
    """parse_source_config must refuse configs nested deeper than _SOURCE_CONFIG_MAX_DEPTH."""

    def test_deeply_nested_inline_json_raises_value_error(self) -> None:
        # Build a config that nests one level deeper than the allowed max.
        # Each level is {"sources": [inner_json_string]}.
        import json as _json

        from src.infrastructure.sources import (
            _SOURCE_CONFIG_MAX_DEPTH,
            parse_source_config,
        )

        inner: object = ["/tmp/tasks.csv"]
        for _ in range(_SOURCE_CONFIG_MAX_DEPTH + 1):
            inner = {"sources": [_json.dumps(inner)]}

        with self.assertRaises(ValueError) as ctx:
            parse_source_config(_json.dumps(inner))
        self.assertIn("nested", str(ctx.exception).lower())

    def test_shallow_config_is_accepted(self) -> None:
        from src.infrastructure.sources import parse_source_config

        config = parse_source_config(
            '{"sources": ["/tmp/a.csv", "/tmp/b.csv"], "union_mode": "union"}'
        )
        self.assertEqual(len(config.sources), 2)


# ---------------------------------------------------------------------------
# DB_POOL_MAX_SIZE error messages (Fix 2 / medium quality)
# ---------------------------------------------------------------------------


class PoolConfigErrorTests(unittest.TestCase):
    def test_non_numeric_max_size_includes_bad_value_in_message(self) -> None:
        with patch.dict("os.environ", {"DB_POOL_MAX_SIZE": "four"}, clear=False):
            # Force reload to bypass cache.
            import importlib

            import src.infrastructure.db as db_mod
            importlib.reload(db_mod)
            with self.assertRaises(ValueError) as ctx:
                db_mod.database_pool_max_size()
            self.assertIn("four", str(ctx.exception))

    def test_negative_max_size_includes_value_in_message(self) -> None:
        with patch.dict("os.environ", {"DB_POOL_MAX_SIZE": "-3"}, clear=False):
            import importlib

            import src.infrastructure.db as db_mod
            importlib.reload(db_mod)
            with self.assertRaises(ValueError) as ctx:
                db_mod.database_pool_max_size()
            self.assertIn("-3", str(ctx.exception))

    def test_borrow_timeout_default_is_positive(self) -> None:
        from src.infrastructure import db as db_mod
        with patch.dict("os.environ", {}, clear=False):
            timeout = db_mod.database_pool_borrow_timeout()
        self.assertGreater(timeout, 0)

    def test_borrow_timeout_non_numeric_raises_value_error(self) -> None:
        with patch.dict("os.environ", {"DB_POOL_BORROW_TIMEOUT": "fast"}, clear=False):
            import importlib

            import src.infrastructure.db as db_mod
            importlib.reload(db_mod)
            with self.assertRaises(ValueError) as ctx:
                db_mod.database_pool_borrow_timeout()
            self.assertIn("fast", str(ctx.exception))

    def test_borrow_timeout_zero_raises_value_error(self) -> None:
        with patch.dict("os.environ", {"DB_POOL_BORROW_TIMEOUT": "0"}, clear=False):
            import importlib

            import src.infrastructure.db as db_mod
            importlib.reload(db_mod)
            with self.assertRaises(ValueError) as ctx:
                db_mod.database_pool_borrow_timeout()
            self.assertIn("positive integer", str(ctx.exception))


# ---------------------------------------------------------------------------
# Auto sync pure-logic unit tests
# ---------------------------------------------------------------------------


class AutoSyncMonitorTests(unittest.TestCase):
    """AutoSyncMonitor unit tests using injected fakes — no real DB or file I/O."""

    def _make_settings(self, source_input: str = "/tmp/data"):
        from src.application.settings import AutoSyncSettings
        return AutoSyncSettings(
            source_input=source_input,
            database_url="postgresql://fake/fake",
            poll_seconds=5,
            refresh_seconds=1800,
            retry_seconds=60,
            verbose_idle=False,
        )

    def test_init_stores_settings_and_defaults(self) -> None:
        from src.application.auto_sync import AutoSyncMonitor
        settings = self._make_settings()
        monitor = AutoSyncMonitor(settings)
        self.assertIs(monitor.settings, settings)
        self.assertIsNone(monitor.last_synced_fingerprint)
        self.assertIsNone(monitor.last_success_at)
        self.assertIsNone(monitor.last_attempt_at)
        self.assertFalse(monitor.last_attempt_failed)

    def test_run_forever_exits_on_snapshot_error_then_succeeds(self) -> None:
        """run_forever retries after a snapshot scan failure and then stops when sync succeeds."""
        from src.application.auto_sync import AutoSyncMonitor, SourceSnapshot

        settings = self._make_settings()
        calls: list[str] = []

        good_snapshot = SourceSnapshot(
            fingerprint="abc",
            source_count=1,
            local_file_count=1,
            volatile_source_count=0,
            tracked_paths=["/tmp/data"],
            details={},
        )

        scan_call = [0]
        def fake_build_snapshot(_src: object) -> SourceSnapshot:
            scan_call[0] += 1
            if scan_call[0] == 1:
                raise RuntimeError("scan error")
            return good_snapshot

        def fake_sync(_src: object, _db: str) -> None:
            calls.append("synced")

        tick = [0.0]
        def fake_monotonic() -> float:
            tick[0] += 1.0
            return tick[0]

        sleep_call = [0]
        slept: list[float] = []
        def fake_sleep(s: float) -> None:
            slept.append(s)
            sleep_call[0] += 1
            if sleep_call[0] >= 2:
                raise StopIteration("stop loop")

        monitor = AutoSyncMonitor(
            settings,
            sync_function=fake_sync,
            monotonic=fake_monotonic,
            sleep=fake_sleep,
            printer=lambda _: None,
        )

        with self.assertRaises(StopIteration):
            with patch("src.application.auto_sync.build_source_snapshot", fake_build_snapshot):
                with patch("src.application.auto_sync._write_health_signal"):
                    monitor.run_forever()

        self.assertEqual(calls, ["synced"])
        self.assertEqual(slept[0], settings.retry_seconds)

    def test_run_forever_idle_path_does_not_sync(self) -> None:
        """When no sync is needed, run_forever sleeps and never calls sync_function."""
        from src.application.auto_sync import AutoSyncMonitor, SourceSnapshot

        settings = self._make_settings()
        synced: list[str] = []

        stable_snapshot = SourceSnapshot(
            fingerprint="stable",
            source_count=0,
            local_file_count=0,
            volatile_source_count=0,
            tracked_paths=[],
            details={},
        )

        def fake_build_snapshot(_src: object) -> SourceSnapshot:
            return stable_snapshot

        def fake_sync(_src: object, _db: str) -> None:
            synced.append("synced")

        tick = [1000.0]
        def fake_monotonic() -> float:
            tick[0] += 1.0
            return tick[0]

        sleep_call = [0]
        def fake_sleep(_s: float) -> None:
            sleep_call[0] += 1
            if sleep_call[0] >= 2:
                raise StopIteration("stop loop")

        monitor = AutoSyncMonitor(
            settings,
            sync_function=fake_sync,
            monotonic=fake_monotonic,
            sleep=fake_sleep,
            printer=lambda _: None,
        )
        # Prime the monitor so it's not in startup state.
        monitor.last_synced_fingerprint = "stable"
        monitor.last_success_at = 1000.0

        with self.assertRaises(StopIteration):
            with patch("src.application.auto_sync.build_source_snapshot", fake_build_snapshot):
                monitor.run_forever()

        self.assertEqual(synced, [], "Sync must not be called when fingerprint is unchanged")


class WriteHealthSignalTests(unittest.TestCase):
    """_write_health_signal must touch a file and swallow OSError."""

    def test_health_signal_written_to_path(self) -> None:
        import tempfile
        from pathlib import Path

        from src.application import auto_sync as mod

        with tempfile.TemporaryDirectory() as tmpdir:
            signal_path = Path(tmpdir) / "sync.ok"
            with patch.object(mod, "_HEALTH_SIGNAL_PATH", signal_path):
                mod._write_health_signal()
            self.assertTrue(signal_path.exists())

    def test_health_signal_swallows_os_error(self) -> None:
        from pathlib import Path

        from src.application import auto_sync as mod

        bad_path = Path("/nonexistent_root/sync.ok")
        with patch.object(mod, "_HEALTH_SIGNAL_PATH", bad_path):
            # Must not raise.
            mod._write_health_signal()


# ---------------------------------------------------------------------------
# SyncSummary formatting
# ---------------------------------------------------------------------------


class SyncSummaryTests(unittest.TestCase):
    def _make_summary(self):
        from src.application.task_sync import SyncSummary
        return SyncSummary(
            run_id=42,
            source_count=3,
            frame_count=5,
            staged_row_count=100,
            current_row_count=90,
            union_mode="union",
            inserted_count=10,
            updated_count=5,
            deleted_count=2,
            unchanged_count=73,
        )

    def test_as_dict_returns_all_fields(self) -> None:
        s = self._make_summary()
        d = s.as_dict()
        self.assertEqual(d["run_id"], 42)
        self.assertEqual(d["inserted_count"], 10)
        self.assertEqual(d["union_mode"], "union")

    def test_describe_includes_run_id_and_counts(self) -> None:
        s = self._make_summary()
        text = s.describe()
        self.assertIn("run_id=42", text)
        self.assertIn("inserted=10", text)
        self.assertIn("deleted=2", text)


if __name__ == "__main__":
    unittest.main()
