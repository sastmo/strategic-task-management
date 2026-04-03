from __future__ import annotations

from dataclasses import dataclass
import os


DEFAULT_DATABASE_URL = "postgresql://stm_user:stm_password@db:5432/strategic_tasks"


def env_flag(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y", "on"}


def load_database_url() -> str:
    return os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)


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
    refresh_ms: int
    dashboard_height: int


@dataclass(frozen=True, slots=True)
class AutoSyncSettings:
    source_input: str
    database_url: str
    poll_seconds: int
    refresh_seconds: int
    retry_seconds: int
    verbose_idle: bool


def load_app_settings(default_source: str) -> AppSettings:
    return AppSettings(
        tasks_source=os.getenv("TASKS_SOURCE", default_source),
        refresh_ms=int(os.getenv("APP_REFRESH_MS", "60000")),
        dashboard_height=int(os.getenv("APP_DASHBOARD_HEIGHT", "1900")),
    )


def load_auto_sync_settings(default_source: str) -> AutoSyncSettings:
    source_input = load_sync_source_input(default_source)
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
        database_url=load_database_url(),
        poll_seconds=poll_seconds,
        refresh_seconds=refresh_seconds,
        retry_seconds=retry_seconds,
        verbose_idle=env_flag("SYNC_VERBOSE_IDLE", "false"),
    )
