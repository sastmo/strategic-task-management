from __future__ import annotations

from pathlib import Path
import os

import streamlit as st
import streamlit.components.v1 as components
from streamlit_autorefresh import st_autorefresh

from src.dashboard import STREAMLIT_CHROME_STYLE, build_dashboard_html
from src.loader import load_tasks


DEFAULT_SOURCE = os.getenv(
    "TASKS_SOURCE",
    str(Path(__file__).resolve().parent / "data" / "tasks.csv"),
)
DASHBOARD_HEIGHT = int(os.getenv("APP_DASHBOARD_HEIGHT", "1900"))


def main() -> None:
    st.set_page_config(
        page_title="Strategic Task Management",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.markdown(STREAMLIT_CHROME_STYLE, unsafe_allow_html=True)
    st.title("Strategic Task Management")

    st_autorefresh(
        interval=int(os.getenv("APP_REFRESH_MS", "60000")),
        key="tasks_refresh",
    )

    try:
        tasks = load_tasks(DEFAULT_SOURCE)
    except Exception as exc:
        st.error(f"Could not load tasks from: {DEFAULT_SOURCE}")
        st.exception(exc)
        st.stop()

    components.html(
        build_dashboard_html(tasks),
        height=DASHBOARD_HEIGHT,
        scrolling=True,
    )


if __name__ == "__main__":
    main()
