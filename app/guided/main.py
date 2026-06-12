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
from app.guided import components, demo, home, wizard_state
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

    # Demo mode auto-login
    if demo.is_demo_mode() and not state.is_authenticated():
        st.warning(
            "🧪 **DEMO MODE** — Synthetic data, no real XNAT server. Nothing here is real."
        )
        result = demo.demo_login()
        if result.ok:
            state.set_authenticated(result.username, result.server)
            # Store demo config in session state for use in upload wizard
            st.session_state["demo_config"] = demo.DemoConfig()
            st.rerun()
        else:
            if result.friendly:
                components.render_friendly_error(result.friendly)
            else:
                st.error("Demo login failed. Please try again.")
        return

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

    # Header with resolved project name
    try:
        if demo.is_demo_mode():
            project_display = "DEMO_PROJECT"
        else:
            from src.services.config import AppConfig
            project_display = AppConfig.load().project_name
    except Exception:
        project_display = "XNAT"

    components.render_header(username, project_display, connected)

    # Persistent demo mode banner
    if demo.is_demo_mode():
        st.warning("🧪 DEMO MODE — synthetic data, no real XNAT server. Nothing here is real.")

    # Logout button
    if st.button("�eb46 Log Out", key="btn_logout"):
        state.clear_auth()
        wizard_state.reset_wizard()
        st.rerun()

    st.markdown("")

    # Route by task
    current_task = wizard_state.get_task()

    if current_task is None:
        home.render_home()
    elif current_task == "upload":
        # Load config; in demo mode, use cached DemoConfig; else load from file/env
        try:
            if demo.is_demo_mode():
                config = st.session_state.get("demo_config")
                if config is None:
                    config = demo.DemoConfig()
                    st.session_state["demo_config"] = config
            else:
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
        # Resolve project name for download/browse
        try:
            if demo.is_demo_mode():
                project_name = "DEMO_PROJECT"
            else:
                from src.services.config import AppConfig
                project_name = AppConfig.load().project_name
        except Exception:
            st.error("Could not load project name. Please try again.")
            if st.button("← Home"):
                wizard_state.set_task(None)
                st.rerun()
            return

        from app.guided.browse_view import render_browse
        render_browse(server, project_name)
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
