from __future__ import annotations

from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components
from streamlit_autorefresh import st_autorefresh

from src.application.settings import load_app_settings
from src.loader import load_tasks
from src.presentation.dashboard import STREAMLIT_CHROME_STYLE, build_dashboard_html


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

    components.html(
        build_dashboard_html(tasks),
        height=settings.dashboard_height,
        scrolling=True,
    )


if __name__ == "__main__":
    main()
