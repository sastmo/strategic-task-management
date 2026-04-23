from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any, Mapping, Protocol

from src.application.settings import AuthSettings
from src.domain.identity import (
    AuthState,
    AppRole,
    AuthenticatedUser,
    PermissionSet,
    build_user_key,
    normalize_email,
    normalize_role_collection,
)
from src.infrastructure.auth.app_service import (
    build_app_service_login_url,
    build_app_service_logout_url,
    parse_app_service_user,
)

_logger = logging.getLogger(__name__)


class UserAccessRepositoryLike(Protocol):
    def load_roles(self, user: AuthenticatedUser) -> tuple[str, ...]:
        ...

    def upsert_user(self, user: AuthenticatedUser) -> None:
        ...

    def log_event(
        self,
        *,
        event_type: str,
        actor_type: str,
        actor_id: str | None,
        payload: dict[str, Any],
    ) -> None:
        ...

    def log_user_activity(
        self,
        *,
        user_id: str | None,
        session_id: str,
        event_name: str,
        payload: dict[str, Any],
    ) -> None:
        ...


@dataclass(frozen=True, slots=True)
class AuthorizationContext:
    state: AuthState
    user: AuthenticatedUser | None
    permissions: PermissionSet
    message: str
    auth_mode: str
    sign_in_url: str | None = None
    sign_out_url: str | None = None
    diagnostics: tuple[str, ...] = ()

    @property
    def is_authorized(self) -> bool:
        return self.state == "authorized" and self.permissions.can_view


def build_local_user(settings: AuthSettings) -> AuthenticatedUser:
    return AuthenticatedUser(
        user_key=build_user_key(email=settings.local_user_email),
        email=normalize_email(settings.local_user_email),
        display_name=settings.local_user_name or settings.local_user_email,
        auth_source="local",
        identity_provider="local",
        app_roles=settings.local_user_roles,
        is_authenticated=True,
    )


def resolve_request_authorization(
    *,
    headers: Mapping[str, Any],
    settings: AuthSettings,
    repository: UserAccessRepositoryLike | None = None,
) -> AuthorizationContext:
    sign_in_url = (
        build_app_service_login_url(settings.app_service_provider)
        if settings.mode == "app_service"
        else None
    )
    sign_out_url = (
        build_app_service_logout_url()
        if settings.mode == "app_service"
        else None
    )

    if settings.mode == "disabled":
        roles = normalize_role_collection(
            [settings.default_authenticated_role] if settings.default_authenticated_role else []
        )
        return AuthorizationContext(
            state="authorized",
            user=None,
            permissions=PermissionSet(roles=roles),
            message="Authentication is disabled for this environment.",
            auth_mode=settings.mode,
            sign_in_url=sign_in_url,
            sign_out_url=sign_out_url,
        )

    if settings.mode == "local":
        user = build_local_user(settings)
    else:
        try:
            user = parse_app_service_user(headers)
        except Exception as exc:
            _logger.warning(
                "Azure identity header parsing failed (%s): %s",
                type(exc).__name__,
                exc,
                exc_info=True,
            )
            return AuthorizationContext(
                state="access_denied",
                user=None,
                permissions=PermissionSet(),
                message="The Azure identity headers could not be parsed for this request.",
                auth_mode=settings.mode,
                sign_in_url=sign_in_url,
                sign_out_url=sign_out_url,
                diagnostics=(str(exc),),
            )

    if user is None:
        if not settings.required:
            roles = normalize_role_collection(
                [settings.default_authenticated_role] if settings.default_authenticated_role else []
            )
            return AuthorizationContext(
                state="authorized" if roles else "access_denied",
                user=None,
                permissions=PermissionSet(roles=roles),
                message="Authentication is optional for this environment.",
                auth_mode=settings.mode,
                sign_in_url=sign_in_url,
                sign_out_url=sign_out_url,
            )

        return AuthorizationContext(
            state="authentication_required",
            user=None,
            permissions=PermissionSet(),
            message="Sign in with your organization account to continue.",
            auth_mode=settings.mode,
            sign_in_url=sign_in_url,
            sign_out_url=sign_out_url,
        )

    if settings.allowed_tenant_ids and user.tenant_id.lower() not in settings.allowed_tenant_ids:
        return AuthorizationContext(
            state="access_denied",
            user=user,
            permissions=PermissionSet(),
            message="Your account is valid, but it does not belong to an approved tenant.",
            auth_mode=settings.mode,
            sign_in_url=sign_in_url,
            sign_out_url=sign_out_url,
        )

    roles = resolve_roles(
        user=user,
        settings=settings,
        repository=repository,
    )

    if not roles:
        return AuthorizationContext(
            state="access_denied",
            user=user,
            permissions=PermissionSet(),
            message="You signed in successfully, but this account does not have access to the app.",
            auth_mode=settings.mode,
            sign_in_url=sign_in_url,
            sign_out_url=sign_out_url,
        )

    return AuthorizationContext(
        state="authorized",
        user=user,
        permissions=PermissionSet(roles=roles),
        message="Access granted.",
        auth_mode=settings.mode,
        sign_in_url=sign_in_url,
        sign_out_url=sign_out_url,
    )


def resolve_roles(
    *,
    user: AuthenticatedUser,
    settings: AuthSettings,
    repository: UserAccessRepositoryLike | None,
) -> tuple[AppRole, ...]:
    role_candidates: list[object] = list(user.app_roles)

    if set(user.groups) & set(settings.viewer_group_ids):
        role_candidates.append("viewer")
    if set(user.groups) & set(settings.editor_group_ids):
        role_candidates.append("editor")
    if set(user.groups) & set(settings.admin_group_ids):
        role_candidates.append("admin")

    if repository is not None and settings.use_database_roles:
        role_candidates.extend(repository.load_roles(user))

    roles = normalize_role_collection(role_candidates)
    if roles:
        return roles

    if settings.require_explicit_access:
        return ()

    if settings.default_authenticated_role:
        return normalize_role_collection((settings.default_authenticated_role,))

    return ()


def record_authorized_session(
    *,
    auth_context: AuthorizationContext,
    repository: UserAccessRepositoryLike | None,
    session_id: str,
) -> None:
    if repository is None or auth_context.user is None or not auth_context.is_authorized:
        return

    repository.upsert_user(auth_context.user)
    repository.log_event(
        event_type="auth.session_started",
        actor_type="user",
        actor_id=auth_context.user.user_key,
        payload=authorization_payload(auth_context),
    )
    repository.log_user_activity(
        user_id=auth_context.user.user_key,
        session_id=session_id,
        event_name="auth.session_started",
        payload=authorization_payload(auth_context),
    )


def record_dashboard_view(
    *,
    auth_context: AuthorizationContext,
    repository: UserAccessRepositoryLike | None,
    session_id: str,
    task_count: int,
) -> None:
    if repository is None or auth_context.user is None or not auth_context.is_authorized:
        return

    repository.log_user_activity(
        user_id=auth_context.user.user_key,
        session_id=session_id,
        event_name="dashboard.view_loaded",
        payload={
            "task_count": task_count,
            "role": auth_context.permissions.primary_role,
        },
    )


def authorization_payload(auth_context: AuthorizationContext) -> dict[str, Any]:
    user = auth_context.user
    return {
        "auth_mode": auth_context.auth_mode,
        "email": user.email if user else "",
        "display_name": user.display_name if user else "",
        "tenant_id": user.tenant_id if user else "",
        "user_key": user.user_key if user else "",
        "roles": list(auth_context.permissions.roles),
    }
