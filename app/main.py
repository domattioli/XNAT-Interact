"""
app/main.py — Streamlit entrypoint for XNAT-Interact.

Launch:
    streamlit run app/main.py

Responsibilities
----------------
1. Set Streamlit page config (title, icon, layout).
2. Route to the correct page based on session state (app/state.py).
3. Guard all screens behind authentication (unauthenticated → login).
4. Import st.* here; never in app/logic/*.py (offline-testability rule).
"""
from __future__ import annotations

import streamlit as st

from app import state
from app.pages import browse, login, upload, batch, download, terminal

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

_NAV_PAGES = [
    (state.PAGE_BROWSE,   "Browse"),
    (state.PAGE_UPLOAD,   "Upload"),
    (state.PAGE_BATCH,    "Batch Upload"),
    (state.PAGE_DOWNLOAD, "Download"),
    (state.PAGE_LEARN,    "Learn (CLI ref)"),
]


def _render_sidebar() -> None:
    """Render the navigation sidebar for authenticated users."""
    if not state.is_authenticated():
        return

    with st.sidebar:
        st.markdown(f"**Logged in as:** `{state.get_username()}`")
        st.markdown("---")

        current = state.get_current_page()

        for page_id, label in _NAV_PAGES:
            if st.button(
                label,
                key=f"nav_{page_id}",
                type="primary" if current == page_id else "secondary",
            ):
                state.set_current_page(page_id)
                st.rerun()

        st.markdown("---")
        if st.button("Log out", key="nav_logout"):
            state.clear_auth()
            st.rerun()


# ---------------------------------------------------------------------------
# Main router
# ---------------------------------------------------------------------------

_PAGE_RENDERERS = {
    state.PAGE_BROWSE:   browse.render,
    state.PAGE_UPLOAD:   upload.render,
    state.PAGE_BATCH:    batch.render,
    state.PAGE_DOWNLOAD: download.render,
    state.PAGE_LEARN:    terminal.render,
}


def main() -> None:
    """Top-level router: dispatch to the current page."""
    _render_sidebar()

    if not state.is_authenticated():
        # Always show login when not authenticated, regardless of requested page.
        login.render()
        return

    current_page = state.get_current_page()
    renderer = _PAGE_RENDERERS.get(current_page)

    if renderer is not None:
        renderer()
    else:
        # Unknown page — fall back to Browse.
        state.set_current_page(state.PAGE_BROWSE)
        browse.render()


main()
