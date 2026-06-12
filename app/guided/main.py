"""
app/guided/main — Main entrypoint for the guided (novice) XNAT-Interact app.

Handles page config, auth gate, task routing.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import streamlit as st

from app import state
from app.guided import components, home, wizard_state
from app.guided.wizard_upload import render_upload_wizard
from app.logic.auth import attempt_login


def _ensure_sys_path() -> None:
    """Belt-and-suspenders: ensure repo root is on sys.path."""
    root = Path(__file__).resolve().parent.parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def _render_login() -> None:
    """Minimal login form."""
    st.title("XNAT-Interact — Guided")
    st.markdown("**Log in to get started**")

    with st.form("login_form"):
        username = st.text_input(
            "HawkID",
            placeholder="e.g., jsmith",
            key="login_username",
        )
        password = st.text_input(
            "Password",
            type="password",
            key="login_password",
        )
        submit = st.form_submit_button("Log In", type="primary")

    if submit:
        result = attempt_login(username, password)
        if result.ok:
            state.set_authenticated(result.username, result.server)
            st.rerun()
        else:
            if result.friendly:
                components.render_friendly_error(result.friendly)
            else:
                st.error("Login failed. Please try again.")


def _render_authenticated() -> None:
    """Render the authenticated app."""
    username = state.get_username() or "unknown"
    server = state.get_server()
    connected = server is not None

    # Header
    components.render_header(username, "XNAT-Interact", connected)

    # Logout button
    if st.button("🚪 Log Out", key="btn_logout"):
        state.clear_auth()
        wizard_state.reset_wizard()
        st.rerun()

    st.markdown("")

    # Route by task
    current_task = wizard_state.get_task()

    if current_task is None:
        home.render_home()
    elif current_task == "upload":
        # Load config; in a full integration, this would come from a config service
        try:
            from src.services.config import AppConfig
            config = AppConfig.load()
        except Exception:
            st.error("Could not load configuration. Please try again.")
            if st.button("← Home"):
                wizard_state.reset_wizard()
                st.rerun()
            return

        render_upload_wizard(config, server)
    elif current_task == "download":
        st.info("🔎 Download feature coming soon — use Advanced tools for now.")
        if st.button("← Home"):
            wizard_state.set_task(None)
            st.rerun()
    elif current_task == "annotations":
        st.info("🏷️ Annotations feature coming soon — use Advanced tools for now.")
        if st.button("← Home"):
            wizard_state.set_task(None)
            st.rerun()
    else:
        st.error(f"Unknown task: {current_task}")
        if st.button("← Home"):
            wizard_state.set_task(None)
            st.rerun()


def main() -> None:
    """Main app entrypoint."""
    _ensure_sys_path()

    st.set_page_config(
        page_title="XNAT-Interact — Guided",
        layout="centered",
    )

    # Auth gate
    if not state.is_authenticated():
        _render_login()
    else:
        _render_authenticated()


if __name__ == "__main__":
    main()
