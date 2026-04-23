from __future__ import annotations

import streamlit as st

from src.application.auth_service import AuthorizationContext


def render_authorization_gate(auth_context: AuthorizationContext) -> bool:
    if auth_context.is_authorized:
        return True

    if auth_context.state == "authentication_required":
        st.warning(auth_context.message)
        if auth_context.sign_in_url:
            st.link_button("Sign in with Microsoft", auth_context.sign_in_url, type="primary")
        return False

    st.error(auth_context.message)
    if auth_context.sign_out_url:
        st.link_button("Sign out", auth_context.sign_out_url)
    elif auth_context.sign_in_url:
        st.link_button("Try sign in again", auth_context.sign_in_url)

    if auth_context.diagnostics:
        st.caption(auth_context.diagnostics[0])

    return False


def render_user_status(auth_context: AuthorizationContext, *, show_status_panel: bool = True) -> None:
    if not show_status_panel or not auth_context.is_authorized or auth_context.user is None:
        return

    user = auth_context.user
    role = auth_context.permissions.primary_role or "viewer"
    label = f"{user.label} | {user.email or user.user_key} | role: {role} | auth: {user.auth_source}"

    left_column, right_column = st.columns([6, 1])
    left_column.caption(label)

    if auth_context.sign_out_url:
        right_column.link_button("Sign out", auth_context.sign_out_url)
