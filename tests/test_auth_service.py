from __future__ import annotations

import base64
import json
import unittest
from unittest.mock import patch

from src.application.auth_service import (
    AuthorizationContext,
    build_db_unavailable_context,
    resolve_auth_on_db_error,
    resolve_request_authorization,
)
from src.application.settings import AuthSettings, load_app_settings, load_auth_settings
from src.infrastructure.auth.app_service import parse_app_service_user


class FakeUserAccessRepository:
    def __init__(self, roles: tuple[str, ...] = ()) -> None:
        self.roles = roles
        self.last_upsert_user: object = None
        self.last_event_type: str | None = None
        self.last_activity_event: str | None = None

    def load_roles(self, user: object) -> tuple[str, ...]:
        return self.roles

    def upsert_user(self, user: object) -> None:
        self.last_upsert_user = user

    def log_event(self, *, event_type: str, **_kwargs: object) -> None:
        self.last_event_type = event_type

    def log_user_activity(self, *, event_name: str, **_kwargs: object) -> None:
        self.last_activity_event = event_name


def build_principal_header(payload: dict[str, object]) -> str:
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
    return encoded.rstrip("=")


class AuthServiceTests(unittest.TestCase):
    def test_parse_app_service_user_extracts_claims(self) -> None:
        user = parse_app_service_user(
            {
                "X-MS-CLIENT-PRINCIPAL": build_principal_header(
                    {
                        "auth_typ": "aad",
                        "claims": [
                            {"typ": "preferred_username", "val": "person@example.com"},
                            {"typ": "name", "val": "Person Example"},
                            {"typ": "tid", "val": "tenant-123"},
                            {"typ": "oid", "val": "user-456"},
                            {"typ": "groups", "val": "group-viewers"},
                            {"typ": "roles", "val": "editor"},
                        ],
                    }
                )
            }
        )

        self.assertIsNotNone(user)
        assert user is not None
        self.assertEqual(user.email, "person@example.com")
        self.assertEqual(user.display_name, "Person Example")
        self.assertEqual(user.tenant_id, "tenant-123")
        self.assertEqual(user.principal_id, "user-456")
        self.assertEqual(user.groups, ("group-viewers",))
        self.assertEqual(user.app_roles, ("editor",))

    def test_resolve_request_authorization_supports_local_mode(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "AUTH_MODE": "local",
                "AUTH_LOCAL_USER_EMAIL": "admin@example.com",
                "AUTH_LOCAL_USER_NAME": "Local Admin",
                "AUTH_LOCAL_USER_ROLES": "admin",
            },
            clear=False,
        ):
            settings = load_auth_settings()

        auth_context = resolve_request_authorization(headers={}, settings=settings)

        self.assertTrue(auth_context.is_authorized)
        self.assertEqual(auth_context.permissions.primary_role, "admin")
        self.assertIsNotNone(auth_context.user)
        assert auth_context.user is not None
        self.assertEqual(auth_context.user.email, "admin@example.com")

    def test_resolve_request_authorization_combines_group_and_database_roles(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "AUTH_MODE": "app_service",
                "AUTH_ADMIN_GROUP_IDS": "group-admins",
                "AUTH_EDITOR_GROUP_IDS": "group-editors",
                "AUTH_REQUIRE_EXPLICIT_ACCESS": "true",
                "AUTH_USE_DATABASE_ROLES": "true",
                "AUTH_ALLOW_UNVERIFIED_APP_SERVICE_PROXY": "1",
            },
            clear=False,
        ):
            settings = load_auth_settings()

        header = build_principal_header(
            {
                "auth_typ": "aad",
                "claims": [
                    {"typ": "preferred_username", "val": "person@example.com"},
                    {"typ": "groups", "val": "group-editors"},
                ],
            }
        )

        auth_context = resolve_request_authorization(
            headers={"X-MS-CLIENT-PRINCIPAL": header},
            settings=settings,
            repository=FakeUserAccessRepository(("admin",)),
        )

        self.assertTrue(auth_context.is_authorized)
        self.assertEqual(auth_context.permissions.primary_role, "admin")

    def test_app_service_mode_denies_when_no_proxy_secret_configured(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "AUTH_MODE": "app_service",
                "APP_TRUSTED_PROXY_SECRET": "",
                "AUTH_ALLOW_UNVERIFIED_APP_SERVICE_PROXY": "",
            },
            clear=False,
        ):
            settings = load_auth_settings()

        auth_context = resolve_request_authorization(headers={}, settings=settings)

        self.assertFalse(auth_context.is_authorized)
        self.assertEqual(auth_context.state, "access_denied")

    def test_app_service_mode_denies_request_with_wrong_proxy_secret(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "AUTH_MODE": "app_service",
                "APP_TRUSTED_PROXY_SECRET": "correct-secret",
                "APP_TRUSTED_PROXY_HEADER": "X-Proxy-Auth",
            },
            clear=False,
        ):
            settings = load_auth_settings()

        header = build_principal_header(
            {
                "auth_typ": "aad",
                "claims": [{"typ": "preferred_username", "val": "attacker@evil.com"}],
            }
        )
        auth_context = resolve_request_authorization(
            headers={"X-MS-CLIENT-PRINCIPAL": header, "X-Proxy-Auth": "wrong-secret"},
            settings=settings,
        )

        self.assertFalse(auth_context.is_authorized)
        self.assertEqual(auth_context.state, "access_denied")

    def test_app_service_mode_accepts_request_with_correct_proxy_secret(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "AUTH_MODE": "app_service",
                "APP_TRUSTED_PROXY_SECRET": "correct-secret",
                "APP_TRUSTED_PROXY_HEADER": "X-Proxy-Auth",
                "AUTH_REQUIRE_EXPLICIT_ACCESS": "false",
                "AUTH_DEFAULT_ROLE": "viewer",
            },
            clear=False,
        ):
            settings = load_auth_settings()

        header = build_principal_header(
            {
                "auth_typ": "aad",
                "claims": [{"typ": "preferred_username", "val": "user@example.com"}],
            }
        )
        auth_context = resolve_request_authorization(
            headers={"X-MS-CLIENT-PRINCIPAL": header, "X-Proxy-Auth": "correct-secret"},
            settings=settings,
        )

        self.assertTrue(auth_context.is_authorized)

    def test_app_service_mode_denies_unexpected_identity_provider(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "AUTH_MODE": "app_service",
                "APP_TRUSTED_PROXY_SECRET": "correct-secret",
                "APP_TRUSTED_PROXY_HEADER": "X-Proxy-Auth",
                "AUTH_APP_SERVICE_PROVIDER": "aad",
            },
            clear=False,
        ):
            settings = load_auth_settings()

        header = build_principal_header(
            {
                "auth_typ": "google",
                "claims": [{"typ": "preferred_username", "val": "user@example.com"}],
            }
        )
        auth_context = resolve_request_authorization(
            headers={
                "X-MS-CLIENT-PRINCIPAL": header,
                "X-MS-CLIENT-PRINCIPAL-IDP": "google",
                "X-Proxy-Auth": "correct-secret",
            },
            settings=settings,
        )

        self.assertFalse(auth_context.is_authorized)
        self.assertEqual(auth_context.state, "access_denied")

    def test_load_auth_settings_raises_for_local_mode_in_production(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "AUTH_MODE": "local",
                "ENVIRONMENT": "production",
                "ALLOW_LOCAL_AUTH_IN_PRODUCTION": "",
            },
            clear=False,
        ):
            with self.assertRaises(RuntimeError, msg="Should raise when local mode used in production"):
                load_auth_settings()

    def test_load_auth_settings_allows_local_mode_in_production_with_override(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "AUTH_MODE": "local",
                "ENVIRONMENT": "production",
                "ALLOW_LOCAL_AUTH_IN_PRODUCTION": "1",
            },
            clear=False,
        ):
            settings = load_auth_settings()

        self.assertEqual(settings.mode, "local")

    def test_resolve_request_authorization_denies_without_explicit_access(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "AUTH_MODE": "app_service",
                "AUTH_REQUIRE_EXPLICIT_ACCESS": "true",
                "AUTH_DEFAULT_ROLE": "",
            },
            clear=False,
        ):
            settings = load_auth_settings()

        header = build_principal_header(
            {
                "auth_typ": "aad",
                "claims": [
                    {"typ": "preferred_username", "val": "person@example.com"},
                ],
            }
        )

        auth_context = resolve_request_authorization(
            headers={"X-MS-CLIENT-PRINCIPAL": header},
            settings=settings,
            repository=FakeUserAccessRepository(),
        )

        self.assertFalse(auth_context.is_authorized)
        self.assertEqual(auth_context.state, "access_denied")


class RecordSessionAndViewTests(unittest.TestCase):
    """record_authorized_session and record_dashboard_view must call through to the repository."""

    def _authorized_context(self) -> AuthorizationContext:
        from src.domain.identity import AuthenticatedUser, PermissionSet

        user = AuthenticatedUser(
            user_key="email::user@example.com",
            email="user@example.com",
            display_name="User",
            auth_source="app_service",
        )
        return AuthorizationContext(
            state="authorized",
            user=user,
            permissions=PermissionSet(roles=("viewer",)),
            message="Access granted.",
            auth_mode="app_service",
        )

    def test_record_authorized_session_calls_repository(self) -> None:
        from src.application.auth_service import record_authorized_session

        repo = FakeUserAccessRepository()
        ctx = self._authorized_context()

        record_authorized_session(auth_context=ctx, repository=repo, session_id="sess-1")

        self.assertIsNotNone(repo.last_upsert_user)
        self.assertEqual(repo.last_event_type, "auth.session_started")

    def test_record_authorized_session_noops_when_repository_is_none(self) -> None:
        from src.application.auth_service import record_authorized_session

        ctx = self._authorized_context()
        record_authorized_session(auth_context=ctx, repository=None, session_id="sess-1")

    def test_record_dashboard_view_calls_repository(self) -> None:
        from src.application.auth_service import record_dashboard_view

        repo = FakeUserAccessRepository()
        ctx = self._authorized_context()

        record_dashboard_view(auth_context=ctx, repository=repo, session_id="sess-1", task_count=5)

        self.assertEqual(repo.last_activity_event, "dashboard.view_loaded")

    def test_record_dashboard_view_noops_when_repository_is_none(self) -> None:
        from src.application.auth_service import record_dashboard_view

        ctx = self._authorized_context()
        record_dashboard_view(auth_context=ctx, repository=None, session_id="sess-1", task_count=0)


class ProductionSettingsGuardTests(unittest.TestCase):
    """AUTH_MODE=disabled and unverified proxy must be rejected in production."""

    def test_disabled_mode_raises_in_production(self) -> None:
        with patch.dict(
            "os.environ",
            {"AUTH_MODE": "disabled", "ENVIRONMENT": "production", "ALLOW_DISABLED_AUTH_IN_PRODUCTION": ""},
            clear=False,
        ):
            with self.assertRaises(RuntimeError):
                load_auth_settings()

    def test_disabled_mode_allowed_in_production_with_override(self) -> None:
        with patch.dict(
            "os.environ",
            {"AUTH_MODE": "disabled", "ENVIRONMENT": "production", "ALLOW_DISABLED_AUTH_IN_PRODUCTION": "1"},
            clear=False,
        ):
            settings = load_auth_settings()
        self.assertEqual(settings.mode, "disabled")

    def test_disabled_mode_allowed_outside_production(self) -> None:
        with patch.dict(
            "os.environ",
            {"AUTH_MODE": "disabled", "ENVIRONMENT": "development"},
            clear=False,
        ):
            settings = load_auth_settings()
        self.assertEqual(settings.mode, "disabled")

    def test_unverified_proxy_raises_in_production(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "AUTH_MODE": "app_service",
                "ENVIRONMENT": "production",
                "AUTH_ALLOW_UNVERIFIED_APP_SERVICE_PROXY": "1",
            },
            clear=False,
        ):
            with self.assertRaises(RuntimeError):
                load_auth_settings()

    def test_app_service_requires_proxy_secret_in_production(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "AUTH_MODE": "app_service",
                "ENVIRONMENT": "production",
                "APP_TRUSTED_PROXY_SECRET": "",
            },
            clear=False,
        ):
            with self.assertRaises(RuntimeError):
                load_auth_settings()

    def test_unverified_proxy_allowed_outside_production(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "AUTH_MODE": "app_service",
                "ENVIRONMENT": "development",
                "AUTH_ALLOW_UNVERIFIED_APP_SERVICE_PROXY": "1",
            },
            clear=False,
        ):
            settings = load_auth_settings()
        self.assertTrue(settings.allow_unverified_proxy)


class ProductionAppSettingsGuardTests(unittest.TestCase):
    """Production app settings must point the dashboard at the warehouse."""

    def test_database_url_required_in_production(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "ENVIRONMENT": "production",
                "AUTH_MODE": "app_service",
                "APP_TRUSTED_PROXY_SECRET": "secret",
                "TASKS_SOURCE": "postgresql://user:pass@example/db",
                "DATABASE_URL": "",
            },
            clear=False,
        ):
            with self.assertRaises(RuntimeError):
                load_app_settings("data/tasks.csv")

    def test_tasks_source_must_be_database_url_in_production(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "ENVIRONMENT": "production",
                "AUTH_MODE": "app_service",
                "APP_TRUSTED_PROXY_SECRET": "secret",
                "DATABASE_URL": "postgresql://user:pass@example/db",
                "TASKS_SOURCE": "/app/data/tasks.csv",
            },
            clear=False,
        ):
            with self.assertRaises(RuntimeError):
                load_app_settings("data/tasks.csv")

    def test_database_source_allowed_in_production(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "ENVIRONMENT": "production",
                "AUTH_MODE": "app_service",
                "APP_TRUSTED_PROXY_SECRET": "secret",
                "DATABASE_URL": "postgresql://user:pass@example/db",
                "TASKS_SOURCE": "postgresql://user:pass@example/db",
            },
            clear=False,
        ):
            settings = load_app_settings("data/tasks.csv")

        self.assertEqual(settings.database_url, "postgresql://user:pass@example/db")


class FailClosedDbUnavailableTests(unittest.TestCase):
    """When the database is unavailable, access must be denied if roles come from DB."""

    def _app_service_settings(self, *, use_database_roles: bool) -> AuthSettings:
        env = {
            "AUTH_MODE": "app_service",
            "AUTH_USE_DATABASE_ROLES": "true" if use_database_roles else "false",
            "AUTH_ALLOW_UNVERIFIED_APP_SERVICE_PROXY": "1",
            "AUTH_DEFAULT_ROLE": "viewer",
            "AUTH_REQUIRE_EXPLICIT_ACCESS": "false",
        }
        with patch.dict("os.environ", env, clear=False):
            return load_auth_settings()

    def test_build_db_unavailable_context_returns_access_denied(self) -> None:
        settings = self._app_service_settings(use_database_roles=True)
        ctx = build_db_unavailable_context(settings)
        self.assertFalse(ctx.is_authorized)
        self.assertEqual(ctx.state, "access_denied")
        self.assertFalse(ctx.permissions.can_view)

    def test_resolve_auth_on_db_error_denies_when_roles_required(self) -> None:
        settings = self._app_service_settings(use_database_roles=True)
        header = build_principal_header(
            {"auth_typ": "aad", "claims": [{"typ": "preferred_username", "val": "user@example.com"}]}
        )
        ctx = resolve_auth_on_db_error(
            headers={"X-MS-CLIENT-PRINCIPAL": header, "X-Proxy-Auth": ""},
            settings=settings,
            exc=RuntimeError("db down"),
        )
        self.assertFalse(ctx.is_authorized)
        self.assertEqual(ctx.state, "access_denied")

    def test_resolve_auth_on_db_error_falls_through_when_roles_not_required(self) -> None:
        # Database used only for audit -- a failure should not block access.
        settings = self._app_service_settings(use_database_roles=False)
        header = build_principal_header(
            {"auth_typ": "aad", "claims": [{"typ": "preferred_username", "val": "user@example.com"}]}
        )
        ctx = resolve_auth_on_db_error(
            headers={"X-MS-CLIENT-PRINCIPAL": header, "X-Proxy-Auth": ""},
            settings=settings,
            exc=RuntimeError("audit db down"),
        )
        # User has default viewer role from token, so they should still get in.
        self.assertTrue(ctx.is_authorized)

    def test_db_unavailable_context_includes_sign_in_url_for_app_service(self) -> None:
        settings = self._app_service_settings(use_database_roles=True)
        ctx = build_db_unavailable_context(settings)
        self.assertIsNotNone(ctx.sign_in_url)


if __name__ == "__main__":
    unittest.main()
