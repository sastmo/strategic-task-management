from __future__ import annotations

from dataclasses import dataclass
import os

from src.domain.identity import AppRole, normalize_role_collection


DEFAULT_LOCAL_USER_EMAIL = "local.admin@example.com"
SUPPORTED_AUTH_MODES = {"local", "app_service", "disabled"}


def env_flag(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y", "on"}


def load_database_url(*, required: bool = False) -> str:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if required and not database_url:
        raise RuntimeError(
            "DATABASE_URL is required for this runtime path. "
            "Set it in your environment or container configuration."
        )
    return database_url


def env_list(name: str, default: str = "") -> tuple[str, ...]:
    raw_value = os.getenv(name, default)
    values = [item.strip() for item in raw_value.split(",")]
    return tuple(item for item in values if item)


def env_role_list(name: str, default: str = "") -> tuple[AppRole, ...]:
    return normalize_role_collection(env_list(name, default))


def env_optional_role(name: str, default: str = "") -> AppRole | None:
    roles = env_role_list(name, default)
    return roles[-1] if roles else None


def normalize_auth_mode(value: str) -> str:
    mode = value.strip().lower() or "local"
    if mode not in SUPPORTED_AUTH_MODES:
        supported = ", ".join(sorted(SUPPORTED_AUTH_MODES))
        raise ValueError(f"Unsupported AUTH_MODE: {value}. Expected one of: {supported}")
    return mode


def load_source_input(
    *,
    config_env: str,
    source_env: str,
    default_source: str,
) -> str:
    return os.getenv(config_env, os.getenv(source_env, default_source))


def load_sync_source_input(default_source: str) -> str:
    return load_source_input(
        config_env="SYNC_SOURCE_CONFIG",
        source_env="SYNC_SOURCE",
        default_source=default_source,
    )


@dataclass(frozen=True, slots=True)
class AppSettings:
    tasks_source: str
    database_url: str
    refresh_ms: int
    dashboard_height: int
    auth: "AuthSettings"


@dataclass(frozen=True, slots=True)
class AuthSettings:
    mode: str
    required: bool
    require_explicit_access: bool
    default_authenticated_role: AppRole | None
    use_database_roles: bool
    audit_to_database: bool
    local_user_email: str
    local_user_name: str
    local_user_roles: tuple[AppRole, ...]
    allowed_tenant_ids: tuple[str, ...]
    viewer_group_ids: tuple[str, ...]
    editor_group_ids: tuple[str, ...]
    admin_group_ids: tuple[str, ...]
    app_service_provider: str
    show_status_panel: bool

    @property
    def uses_database(self) -> bool:
        return self.use_database_roles or self.audit_to_database


@dataclass(frozen=True, slots=True)
class AutoSyncSettings:
    source_input: str
    database_url: str
    poll_seconds: int
    refresh_seconds: int
    retry_seconds: int
    verbose_idle: bool


def load_auth_settings() -> AuthSettings:
    mode = normalize_auth_mode(os.getenv("AUTH_MODE", "local"))
    local_user_email = os.getenv("AUTH_LOCAL_USER_EMAIL", DEFAULT_LOCAL_USER_EMAIL).strip().lower()

    return AuthSettings(
        mode=mode,
        required=env_flag("AUTH_REQUIRED", "true"),
        require_explicit_access=env_flag("AUTH_REQUIRE_EXPLICIT_ACCESS", "false"),
        default_authenticated_role=env_optional_role("AUTH_DEFAULT_ROLE", "viewer"),
        use_database_roles=env_flag("AUTH_USE_DATABASE_ROLES", "false"),
        audit_to_database=env_flag("AUTH_AUDIT_TO_DATABASE", "false"),
        local_user_email=local_user_email,
        local_user_name=os.getenv("AUTH_LOCAL_USER_NAME", "Local Admin").strip() or local_user_email,
        local_user_roles=env_role_list("AUTH_LOCAL_USER_ROLES", "admin"),
        allowed_tenant_ids=tuple(item.lower() for item in env_list("AUTH_ALLOWED_TENANT_IDS")),
        viewer_group_ids=env_list("AUTH_VIEWER_GROUP_IDS"),
        editor_group_ids=env_list("AUTH_EDITOR_GROUP_IDS"),
        admin_group_ids=env_list("AUTH_ADMIN_GROUP_IDS"),
        app_service_provider=os.getenv("AUTH_APP_SERVICE_PROVIDER", "aad").strip() or "aad",
        show_status_panel=env_flag("APP_AUTH_SHOW_STATUS", "true"),
    )


def load_app_settings(default_source: str) -> AppSettings:
    return AppSettings(
        tasks_source=os.getenv("TASKS_SOURCE", default_source),
        database_url=load_database_url(),
        refresh_ms=int(os.getenv("APP_REFRESH_MS", "60000")),
        dashboard_height=int(os.getenv("APP_DASHBOARD_HEIGHT", "1900")),
        auth=load_auth_settings(),
    )


def load_auto_sync_settings(default_source: str) -> AutoSyncSettings:
    source_input = load_sync_source_input(default_source)
    database_url = load_database_url(required=True)
    poll_seconds = max(
        5,
        int(os.getenv("SYNC_POLL_SECONDS", os.getenv("SYNC_INTERVAL_SECONDS", "30"))),
    )
    refresh_seconds = max(
        poll_seconds,
        int(os.getenv("SYNC_REFRESH_SECONDS", "1800")),
    )
    retry_seconds = max(
        poll_seconds,
        int(os.getenv("SYNC_RETRY_SECONDS", "120")),
    )

    return AutoSyncSettings(
        source_input=source_input,
        database_url=database_url,
        poll_seconds=poll_seconds,
        refresh_seconds=refresh_seconds,
        retry_seconds=retry_seconds,
        verbose_idle=env_flag("SYNC_VERBOSE_IDLE", "false"),
    )
