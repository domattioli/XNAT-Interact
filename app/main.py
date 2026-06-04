"""
app/main.py — Streamlit entrypoint for XNAT-Interact.

Launch:
    streamlit run app/main.py

Responsibilities
----------------
1. Set Streamlit page config (title, icon, layout).
2. Route to the correct page based on session state (app/state.py).
3. Guard Browse and Upload behind authentication.
4. Import st.* here; never in app/logic/*.py (offline-testability rule).
"""
from __future__ import annotations

import streamlit as st

from app import state
from app.pages import browse, login, upload

# ---------------------------------------------------------------------------
# Page configuration — must be the FIRST streamlit call
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="XNAT-Interact",
    page_icon="🔬",
    layout="centered",
    initial_sidebar_state="auto",
)


# ---------------------------------------------------------------------------
# Sidebar navigation (only shown when authenticated)
# ---------------------------------------------------------------------------

def _render_sidebar() -> None:
    """Render the navigation sidebar for authenticated users."""
    if not state.is_authenticated():
        return

    with st.sidebar:
        st.markdown(f"**Logged in as:** `{state.get_username()}`")
        st.markdown("---")

        current = state.get_current_page()

        if st.button(
            "Browse",
            key="nav_browse",
            type="primary" if current == state.PAGE_BROWSE else "secondary",
        ):
            state.set_current_page(state.PAGE_BROWSE)
            st.rerun()

        if st.button(
            "Upload",
            key="nav_upload",
            type="primary" if current == state.PAGE_UPLOAD else "secondary",
        ):
            state.set_current_page(state.PAGE_UPLOAD)
            st.rerun()

        st.markdown("---")
        if st.button("Log out", key="nav_logout"):
            state.clear_auth()
            st.rerun()


# ---------------------------------------------------------------------------
# Main router
# ---------------------------------------------------------------------------

def main() -> None:
    """Top-level router: dispatch to the current page."""
    _render_sidebar()

    current_page = state.get_current_page()

    if not state.is_authenticated():
        # Always show login when not authenticated, regardless of requested page.
        login.render()
        return

    # Authenticated — route to the requested page.
    if current_page == state.PAGE_BROWSE:
        browse.render()
    elif current_page == state.PAGE_UPLOAD:
        upload.render()
    else:
        # Unknown page — fall back to Browse.
        state.set_current_page(state.PAGE_BROWSE)
        browse.render()


main()
