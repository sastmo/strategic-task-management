from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final, Literal

from src.domain.tasks import text_or_blank

AppRole = Literal["viewer", "editor", "admin"]
AuthState = Literal["authorized", "authentication_required", "access_denied"]

SUPPORTED_APP_ROLES: Final[tuple[AppRole, ...]] = ("viewer", "editor", "admin")
ROLE_PRIORITY: Final[dict[AppRole, int]] = {
    "viewer": 1,
    "editor": 2,
    "admin": 3,
}

ROLE_ALIASES: Final[dict[str, AppRole]] = {
    "viewer": "viewer",
    "view": "viewer",
    "read": "viewer",
    "reader": "viewer",
    "user": "viewer",
    "editor": "editor",
    "edit": "editor",
    "write": "editor",
    "writer": "editor",
    "contributor": "editor",
    "admin": "admin",
    "administrator": "admin",
    "owner": "admin",
    "superuser": "admin",
}


@dataclass(frozen=True, slots=True)
class AuthenticatedUser:
    user_key: str
    email: str
    display_name: str
    principal_id: str = ""
    tenant_id: str = ""
    identity_provider: str = ""
    auth_source: str = ""
    groups: tuple[str, ...] = ()
    app_roles: tuple[AppRole, ...] = ()
    is_authenticated: bool = True

    @property
    def label(self) -> str:
        return self.display_name or self.email or self.user_key


@dataclass(frozen=True, slots=True)
class PermissionSet:
    roles: tuple[AppRole, ...] = ()

    @property
    def primary_role(self) -> AppRole | None:
        return highest_role(self.roles)

    @property
    def can_view(self) -> bool:
        return bool(self.roles)

    @property
    def can_edit(self) -> bool:
        role = self.primary_role
        return role in {"editor", "admin"}

    @property
    def can_admin(self) -> bool:
        return self.primary_role == "admin"


def normalize_email(value: object) -> str:
    return text_or_blank(value).lower()


def normalize_app_role(value: object) -> AppRole | None:
    key = text_or_blank(value).lower()
    if not key:
        return None
    return ROLE_ALIASES.get(key)


def normalize_role_collection(values: Iterable[object]) -> tuple[AppRole, ...]:
    normalized = {
        role
        for role in (normalize_app_role(value) for value in values)
        if role is not None
    }
    return tuple(sorted(normalized, key=lambda role: ROLE_PRIORITY[role]))


def highest_role(values: Iterable[object]) -> AppRole | None:
    roles = normalize_role_collection(values)
    return roles[-1] if roles else None


def build_user_key(
    *,
    email: object = "",
    principal_id: object = "",
    tenant_id: object = "",
) -> str:
    normalized_email = normalize_email(email)
    normalized_principal_id = text_or_blank(principal_id).lower()
    normalized_tenant_id = text_or_blank(tenant_id).lower()

    if normalized_tenant_id and normalized_principal_id:
        return f"entra::{normalized_tenant_id}::{normalized_principal_id}"
    if normalized_principal_id:
        return f"principal::{normalized_principal_id}"
    if normalized_email:
        return f"email::{normalized_email}"
    return "anonymous"
