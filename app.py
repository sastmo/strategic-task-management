from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext
import logging
from pathlib import Path
from uuid import uuid4

import streamlit as st
import streamlit.components.v1 as components
from streamlit_autorefresh import st_autorefresh

from src.application.auth_service import (
    AuthorizationContext,
    record_authorized_session,
    record_dashboard_view,
    resolve_request_authorization,
)
from src.application.settings import AppSettings, load_app_settings
from src.infrastructure.user_repository import UserAccessRepository, open_user_access_repository
from src.loader import load_tasks
from src.presentation import (
    STREAMLIT_CHROME_STYLE,
    build_dashboard_html,
    render_authorization_gate,
    render_user_status,
)

_logger = logging.getLogger(__name__)
AUTH_SCHEMA_READY_KEY = "auth_schema_ready"
AUTH_SESSION_RECORDED_KEY = "auth_session_recorded"
DASHBOARD_VIEW_RECORDED_KEY = "dashboard_view_recorded"


def session_id() -> str:
    if "session_id" not in st.session_state:
        st.session_state["session_id"] = uuid4().hex
    return str(st.session_state["session_id"])


def request_headers() -> dict[str, str]:
    return dict(st.context.headers.items())


def open_user_repository(
    *,
    database_url: str,
    enabled: bool,
) -> AbstractContextManager[UserAccessRepository | None]:
    auth_schema_ready = bool(st.session_state.get(AUTH_SCHEMA_READY_KEY))
    return (
        open_user_access_repository(
            database_url,
            ensure_objects=not auth_schema_ready,
        )
        if enabled
        else nullcontext(None)
    )


def mark_auth_schema_ready(user_repository: object | None) -> None:
    if user_repository is not None:
        st.session_state[AUTH_SCHEMA_READY_KEY] = True


def resolve_auth_context(settings: AppSettings) -> tuple[AuthorizationContext, Exception | None]:
    headers = request_headers()
    repository_error: Exception | None = None

    try:
        with open_user_repository(
            database_url=settings.database_url,
            enabled=settings.auth.uses_database,
        ) as user_repository:
            mark_auth_schema_ready(user_repository)
            auth_context = resolve_request_authorization(
                headers=headers,
                settings=settings.auth,
                repository=user_repository,
            )

            if not st.session_state.get(AUTH_SESSION_RECORDED_KEY):
                record_authorized_session(
                    auth_context=auth_context,
                    repository=user_repository,
                    session_id=session_id(),
                )
                st.session_state[AUTH_SESSION_RECORDED_KEY] = True
    except Exception as exc:
        repository_error = exc
        auth_context = resolve_request_authorization(
            headers=headers,
            settings=settings.auth,
            repository=None,
        )

    return auth_context, repository_error


def record_dashboard_view_once(
    settings: AppSettings,
    auth_context: AuthorizationContext,
    *,
    task_count: int,
) -> None:
    if st.session_state.get(DASHBOARD_VIEW_RECORDED_KEY):
        return

    try:
        with open_user_repository(
            database_url=settings.database_url,
            enabled=settings.auth.audit_to_database,
        ) as user_repository:
            mark_auth_schema_ready(user_repository)
            record_dashboard_view(
                auth_context=auth_context,
                repository=user_repository,
                session_id=session_id(),
                task_count=task_count,
            )
            st.session_state[DASHBOARD_VIEW_RECORDED_KEY] = True
    except Exception:
        _logger.exception("Failed to record dashboard view for session %s", session_id())


def main() -> None:
    settings = load_app_settings(
        str(Path(__file__).resolve().parent / "data" / "tasks.csv")
    )

    st.set_page_config(
        page_title="Strategic Task Management",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.markdown(STREAMLIT_CHROME_STYLE, unsafe_allow_html=True)
    st.title("Strategic Task Management")

    auth_context, repository_error = resolve_auth_context(settings)

    render_user_status(
        auth_context,
        show_status_panel=settings.auth.show_status_panel,
    )

    if settings.auth.uses_database and not settings.database_url:
        st.warning("Authorization database integration is enabled, but DATABASE_URL is not set.")
    elif repository_error is not None and settings.auth.uses_database:
        st.warning(f"Authorization database integration is unavailable: {repository_error}")

    if not render_authorization_gate(auth_context):
        st.stop()

    st_autorefresh(
        interval=settings.refresh_ms,
        key="tasks_refresh",
    )

    try:
        tasks = load_tasks(settings.tasks_source)
    except Exception as exc:
        st.error(f"Could not load tasks from: {settings.tasks_source}")
        st.exception(exc)
        st.stop()

    record_dashboard_view_once(settings, auth_context, task_count=len(tasks))

    components.html(
        build_dashboard_html(tasks),
        height=settings.dashboard_height,
        scrolling=True,
    )


if __name__ == "__main__":
    main()
