from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
from uuid import uuid4

import streamlit as st
import streamlit.components.v1 as components
from streamlit_autorefresh import st_autorefresh

from src.application.auth_service import (
    record_authorized_session,
    record_dashboard_view,
    resolve_request_authorization,
)
from src.application.settings import load_app_settings
from src.infrastructure.user_repository import open_user_access_repository
from src.loader import load_tasks
from src.presentation import (
    STREAMLIT_CHROME_STYLE,
    build_dashboard_html,
    render_authorization_gate,
    render_user_status,
)


def session_id() -> str:
    if "session_id" not in st.session_state:
        st.session_state["session_id"] = uuid4().hex
    return str(st.session_state["session_id"])


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

    auth_schema_ready = bool(st.session_state.get("auth_schema_ready"))
    repository_context = (
        open_user_access_repository(
            settings.database_url,
            ensure_objects=not auth_schema_ready,
        )
        if settings.auth.uses_database
        else nullcontext(None)
    )
    repository_error: Exception | None = None

    try:
        with repository_context as user_repository:
            if user_repository is not None:
                st.session_state["auth_schema_ready"] = True

            auth_context = resolve_request_authorization(
                headers=dict(st.context.headers.items()),
                settings=settings.auth,
                repository=user_repository,
            )

            if not st.session_state.get("auth_session_recorded"):
                record_authorized_session(
                    auth_context=auth_context,
                    repository=user_repository,
                    session_id=session_id(),
                )
                st.session_state["auth_session_recorded"] = True
    except Exception as exc:
        repository_error = exc
        auth_context = resolve_request_authorization(
            headers=dict(st.context.headers.items()),
            settings=settings.auth,
            repository=None,
        )

    render_user_status(
        auth_context,
        show_status_panel=settings.auth.show_status_panel,
    )

    if repository_error is not None and settings.auth.uses_database:
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

    if not st.session_state.get("dashboard_view_recorded"):
        try:
            with (
                open_user_access_repository(
                    settings.database_url,
                    ensure_objects=not bool(st.session_state.get("auth_schema_ready")),
                )
                if settings.auth.audit_to_database
                else nullcontext(None)
            ) as user_repository:
                if user_repository is not None:
                    st.session_state["auth_schema_ready"] = True

                record_dashboard_view(
                    auth_context=auth_context,
                    repository=user_repository,
                    session_id=session_id(),
                    task_count=len(tasks),
                )
                st.session_state["dashboard_view_recorded"] = True
        except Exception:
            pass

    components.html(
        build_dashboard_html(tasks),
        height=settings.dashboard_height,
        scrolling=True,
    )


if __name__ == "__main__":
    main()
