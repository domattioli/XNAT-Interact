"""
app/pages/login — Streamlit login screen.

Renders:
  - VPN/onboarding hint (static, Phase 4 will make it dynamic)
  - Username + password fields (password via secure input, NEVER echoed/logged)
  - "Connect" button
  - On failure: FriendlyError panel (title / message / recourse), never a traceback

Logic is in app/logic/auth.attempt_login (no streamlit there).
"""
from __future__ import annotations

import streamlit as st

from app.components.error_panel import render_friendly_error
from app.logic.auth import attempt_login
from app import state


# ---------------------------------------------------------------------------
# Onboarding hint (Phase 4 will add live checks; static placeholder for now)
# ---------------------------------------------------------------------------

_ONBOARDING_HINT = """
**Need access?**

Before you can log in you need three things:

① **An XNAT account** — request one from the Data Librarian (`dmattioli` / `stelong`).

② **Added to the project** — the Data Librarian must add your username to the
  project before your credentials will work.

③ **UIowa VPN** — you must be connected to the UIowa VPN for the app to reach the
  XNAT server.  Use the Cisco AnyConnect client and connect before clicking *Connect*.
"""


# ---------------------------------------------------------------------------
# Page renderer
# ---------------------------------------------------------------------------

def render() -> None:
    """Draw the login screen.  Called by app/main.py when not authenticated."""
    st.title("XNAT-Interact — Log In")

    # Onboarding / VPN hint (always visible)
    with st.expander("Need access? Setup checklist", expanded=False):
        st.markdown(_ONBOARDING_HINT)

    st.markdown("---")

    # Preflight VPN reminder — concise, always visible above the form
    st.info(
        "Make sure you are connected to the **UIowa VPN** before clicking Connect.",
        icon="🔒",
    )

    # Login form
    with st.form(key="login_form", clear_on_submit=False):
        username = st.text_input("XNAT Username", key="login_username")
        # st.text_input with type="password" — input is masked; value never echoed
        password = st.text_input(
            "XNAT Password",
            type="password",
            key="login_password",
            help="Your UIowa / XNAT password.  Never stored or logged.",
        )
        submitted = st.form_submit_button("Connect")

    if submitted:
        if not username.strip():
            st.warning("Please enter your XNAT username.")
            return
        if not password:
            st.warning("Please enter your password.")
            return

        with st.spinner("Connecting to XNAT server…"):
            # Delegate all logic to the pure, testable auth module.
            # Password is passed through but never stored in session state
            # or any returned object.
            result = attempt_login(username.strip(), password)

        if result.ok:
            # Store the authenticated handle — password is NOT stored.
            state.set_authenticated(
                username=result.username,
                server_handle=result.server,
            )
            st.rerun()
        else:
            # Render friendly error — no traceback, no raw exception.
            render_friendly_error(result.friendly)
