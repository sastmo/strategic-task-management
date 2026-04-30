from __future__ import annotations

import base64
import json
import unittest
from unittest.mock import patch

from src.application.auth_service import resolve_request_authorization
from src.application.settings import load_auth_settings
from src.infrastructure.auth.app_service import parse_app_service_user


class FakeUserAccessRepository:
    def __init__(self, roles: tuple[str, ...] = ()) -> None:
        self.roles = roles

    def load_roles(self, user):  # noqa: ANN001
        return self.roles

    def upsert_user(self, user):  # noqa: ANN001
        return None

    def log_event(self, **kwargs):  # noqa: ANN003
        return None

    def log_user_activity(self, **kwargs):  # noqa: ANN003
        return None


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


if __name__ == "__main__":
    unittest.main()
